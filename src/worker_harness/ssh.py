"""SSH client for orchestrator → worker communication.

Wraps the `tailscale ssh` CLI for command execution, tmux management, and port forwarding.

Multi-agent reliability:
- Every call goes through WorkerLanes (per-worker SSH concurrency limit).
- Every call uses asyncio.create_subprocess_exec so we can kill the subprocess
  on cancellation, timeout, or exception. No orphaned SSH processes.
- Every call records its duration into metrics.ssh_call_ms, keyed by op name.

The lane is held ONLY during the SSH round-trip, not during long waits
(e.g., a 6-hour training job acquires the lane for ~200ms to spawn tmux,
then releases it).
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import re
import logging
import os
import shlex
import signal
import subprocess  # Popen return type for ssh_port_forward
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from .machines import Machine
from .models import RunnerResult, Worker

log = logging.getLogger("ssh-client")


# ── Module-level dependency injection ──────────────────────────────────

_lanes = None  # type: ignore[var-annotated]


def set_lanes(lanes) -> None:
    """Set the WorkerLanes instance used by all SSH calls. Called from
    heartbeat.py lifespan on server startup."""
    global _lanes
    _lanes = lanes


def _lanes_or_default():
    """Get the WorkerLanes instance, lazily creating a default if not set
    (so unit tests + CLI usage without a server still work)."""
    global _lanes
    if _lanes is None:
        from .lanes import WorkerLanes
        _lanes = WorkerLanes()
    return _lanes


# ── Result types ───────────────────────────────────────────────────────


@dataclass
class SSHResult:
    stdout: str
    stderr: str
    returncode: int


def _signal_process_group(pgid: int, sig: signal.Signals) -> bool:
    """Signal an owned process group. Return whether the group still existed."""
    try:
        os.killpg(pgid, sig)
    except ProcessLookupError:
        return False
    except PermissionError:
        log.warning("Permission denied signalling process group %s with %s", pgid, sig.name)
        return True
    return True


def _process_group_exists(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


async def _wait_for_async_process_group(proc: asyncio.subprocess.Process, timeout: float) -> bool:
    """Wait for the complete group to disappear while reaping its leader."""
    deadline = asyncio.get_running_loop().time() + timeout
    while _process_group_exists(proc.pid):
        if proc.returncode is None:
            # asyncio's child watcher normally reaps the leader. Yielding here
            # lets that callback run before checking whether descendants remain.
            await asyncio.sleep(0)
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            return False
        await asyncio.sleep(min(0.05, remaining))
    await proc.wait()
    return True


async def _terminate_async_process(proc: asyncio.subprocess.Process, grace_seconds: float = 2.0) -> None:
    """Terminate and reap the complete process group of an async SSH call.

    The direct process can exit before a ProxyCommand descendant. Its return
    code therefore never proves that the process group is gone.
    """
    _signal_process_group(proc.pid, signal.SIGTERM)
    group_gone = await _wait_for_async_process_group(proc, grace_seconds)
    if not group_gone:
        _signal_process_group(proc.pid, signal.SIGKILL)
        group_gone = await _wait_for_async_process_group(proc, 1.0)

    if proc.returncode is None:
        try:
            await asyncio.wait_for(proc.wait(), timeout=0.5)
        except asyncio.TimeoutError:
            log.error("SSH process %s could not be reaped after SIGKILL", proc.pid)
    if not group_gone:
        log.error("SSH process group %s survived SIGKILL", proc.pid)


def _wait_for_popen_process_group(proc: subprocess.Popen, timeout: float) -> bool:
    """Wait for a Popen group to disappear while polling/reaping its leader."""
    deadline = time.monotonic() + timeout
    while True:
        proc.poll()
        if not _process_group_exists(proc.pid):
            return True
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        time.sleep(min(0.05, remaining))


def _terminate_popen_process_group(proc: subprocess.Popen, grace_seconds: float = 2.0) -> None:
    """Synchronous sibling for persistent tunnel Popen cleanup."""
    _signal_process_group(proc.pid, signal.SIGTERM)
    group_gone = _wait_for_popen_process_group(proc, grace_seconds)
    if not group_gone:
        _signal_process_group(proc.pid, signal.SIGKILL)
        group_gone = _wait_for_popen_process_group(proc, 1.0)

    if proc.returncode is None:
        try:
            proc.wait(timeout=0.5)
        except subprocess.TimeoutExpired:
            log.error("Tunnel process %s could not be reaped after SIGKILL", proc.pid)
    if not group_gone:
        log.error("Tunnel process group %s survived SIGKILL", proc.pid)


# ── Argument helpers ───────────────────────────────────────────────────


def _ssh_target(worker: Worker) -> str:
    return f"{worker.ssh_user}@{worker.ssh_host}"


def _ssh_base_args(worker: Worker) -> list[str]:
    return ["tailscale", "ssh", _ssh_target(worker)]


def _worker_harness_dir(worker: Worker) -> str:
    return worker.harness_dir.rstrip("/") or "/harness"


def _worker_tmux_tmpdir(worker: Worker) -> str:
    if worker.slurm is not None:
        # tmux also uses AF_UNIX paths; shared allocation roots can be very long.
        digest = hashlib.sha256(worker.id.encode()).hexdigest()[:16]
        return f"/tmp/wh-tmux-{digest}"
    # Keep job sockets beneath the writable harness bind.  The historical
    # sibling `${WH_DIR}/tmux` can be shadowed by a read-only SIF overlay.
    return f"{_worker_harness_dir(worker)}/job-tmux"


def _tmux_env(worker: Worker) -> str:
    if worker.slurm is not None and worker.slurm.compute_kind == "cpu":
        return (
            f"env -u TMUX -u TMUX_PANE CUDA_VISIBLE_DEVICES='' "
            f"TMUX_TMPDIR={shlex.quote(_worker_tmux_tmpdir(worker))} tmux"
        )
    # Preserve the host-driver libraries that Apptainer binds under --nv, and
    # put CUDA tools on PATH for jobs launched through tmux.
    return (
        f"env -u TMUX -u TMUX_PANE "
        f"TMUX_TMPDIR='{_worker_tmux_tmpdir(worker)}' "
        f"LD_LIBRARY_PATH=/.singularity.d/libs${{LD_LIBRARY_PATH:+:${{LD_LIBRARY_PATH}}}} "
        f"PATH=/usr/local/cuda/bin:/usr/local/nvidia/bin:${{PATH}} "
        f"tmux"
    )


# ── Core executor: lane + kill-on-exit + metrics ───────────────────────


async def _exec_ssh_bytes(
    lane_key: str,
    args: Sequence[str],
    *,
    lane_timeout: float = 10.0,
    cmd_timeout: float = 30.0,
    op_name: str = "ssh",
    input_data: bytes | None = None,
) -> tuple[bytes, SSHResult]:
    """Run an SSH subprocess without decoding stdout.

    Returns raw stdout and result metadata (whose stdout is empty). Command
    timeouts return code -1; lane admission failures retain their own exception.

    Propagates CancelledError to the caller (the FastAPI handler will turn
    that into 499 / 503 as appropriate).
    """
    from .metrics import get_metrics
    metrics = get_metrics()

    started = time.monotonic()
    async with _lanes_or_default().acquire(lane_key, timeout=lane_timeout):
        try:
            proc = await asyncio.create_subprocess_exec(
                *args,
                stdin=asyncio.subprocess.PIPE if input_data is not None else asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=True,
            )
        except FileNotFoundError:
            metrics.observe_ssh(op_name, (time.monotonic() - started) * 1000)
            return b"", SSHResult(stdout="", stderr="ssh command not found", returncode=127)

        try:
            try:
                stdout_b, stderr_b = await asyncio.wait_for(
                    proc.communicate(input=input_data),
                    timeout=cmd_timeout,
                )
            except asyncio.TimeoutError:
                # Kill the complete process group, including wrapper children.
                await _terminate_async_process(proc)
                metrics.observe_ssh(op_name, (time.monotonic() - started) * 1000)
                return b"", SSHResult(
                    stdout="",
                    stderr=f"Command timed out after {cmd_timeout}s",
                    returncode=-1,
                )
            except asyncio.CancelledError:
                # Caller cancelled (e.g., FastAPI client disconnect): no child
                # may outlive this request.
                await _terminate_async_process(proc)
                raise
            else:
                metrics.observe_ssh(op_name, (time.monotonic() - started) * 1000)
                return stdout_b or b"", SSHResult(
                    stdout="",
                    stderr=(stderr_b or b"").decode(errors="replace"),
                    returncode=proc.returncode or 0,
                )
        finally:
            # Belt-and-suspenders: if anything escapes the above (e.g., an
            # exception in communicate's decode step), still ensure the
            # subprocess is dead so we don't leak.
            if proc.returncode is None:
                await _terminate_async_process(proc)


async def _exec_ssh(
    lane_key: str,
    args: Sequence[str],
    *,
    lane_timeout: float = 10.0,
    cmd_timeout: float = 30.0,
    op_name: str = "ssh",
    input_data: bytes | None = None,
) -> SSHResult:
    """Text adapter retaining replacement decoding for existing SSH callers."""
    stdout, result = await _exec_ssh_bytes(
        lane_key, args, lane_timeout=lane_timeout, cmd_timeout=cmd_timeout,
        op_name=op_name, input_data=input_data,
    )
    result.stdout = stdout.decode(errors="replace")
    return result


# ── Public SSH calls ───────────────────────────────────────────────────


async def async_ssh_run(worker: Worker, command: str, *, timeout: int = 30) -> SSHResult:
    """Run `command` over ssh on the worker, return result."""
    args = _ssh_base_args(worker) + [command]
    return await _exec_ssh(
        worker.id, args,
        lane_timeout=min(10.0, max(1.0, timeout / 3)),
        cmd_timeout=float(timeout),
        op_name="async_ssh_run",
    )


async def async_ssh_run_pty(worker: Worker, command: str, *, timeout: int = 60) -> SSHResult:
    """Run `command` over ssh with a pseudo-tty."""
    args = _ssh_base_args(worker) + ["-tt", command]
    return await _exec_ssh(
        worker.id, args,
        lane_timeout=min(10.0, max(1.0, timeout / 3)),
        cmd_timeout=float(timeout),
        op_name="async_ssh_run_pty",
    )


# ── Herdr machines (forced-command shim over plain OpenSSH) ────────────


def _machine_base_args(machine: Machine) -> list[str]:
    # Plain OpenSSH with the dedicated service key: the shim is bound to that
    # key's authorized_keys entry, so `tailscale ssh` (which authenticates by
    # tailnet identity, not key) would bypass the allowlist entirely.
    return [
        "ssh",
        "-i", machine.key_path,
        "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=yes",
        "-o", "IdentitiesOnly=yes",
        "-o", "ConnectTimeout=10",
        "-o", "RequestTTY=no",
        machine.ssh_target,
    ]


async def async_machine_run(
    machine: Machine,
    command: str,
    *,
    timeout: float = 30.0,
    input_data: bytes | None = None,
) -> SSHResult:
    """Run one allowlisted shim verb on a herdr machine.

    ``command`` becomes $SSH_ORIGINAL_COMMAND verbatim; the shim word-splits it
    and never evaluates it, so quoting here buys nothing — arguments containing
    whitespace travel as ``b64:`` tokens instead.
    """
    return await _exec_ssh(
        f"machine:{machine.name}",
        _machine_base_args(machine) + [command],
        lane_timeout=min(10.0, max(1.0, timeout / 3)),
        cmd_timeout=float(timeout),
        op_name="async_machine_run",
        input_data=input_data,
    )


async def async_machine_run_bytes(
    machine: Machine,
    command: str,
    *,
    timeout: float = 30.0,
    input_data: bytes | None = None,
) -> tuple[bytes, SSHResult]:
    """Run a shim verb preserving stdout bytes; metadata stdout remains empty."""
    return await _exec_ssh_bytes(
        f"machine:{machine.name}",
        _machine_base_args(machine) + [command],
        lane_timeout=min(10.0, max(1.0, timeout / 3)),
        cmd_timeout=float(timeout),
        op_name="async_machine_run_bytes",
        input_data=input_data,
    )


def _runner_stage_command(worker: Worker) -> str:
    """Atomically install the standalone module, including prebuilt images."""
    source = Path(__file__).with_name("job_runner.py").read_bytes()
    encoded = base64.b64encode(source).decode()
    harness = shlex.quote(_worker_harness_dir(worker))
    module = shlex.quote(f"{_worker_harness_dir(worker)}/.job_runner.py")
    tmux_dir = shlex.quote(_worker_tmux_tmpdir(worker))
    return (
        f"umask 077; mkdir -p {harness} && "
        f"mkdir -p -m 700 {tmux_dir} && test ! -L {tmux_dir} && test -O {tmux_dir} && "
        f"chmod 700 {tmux_dir} && "
        f"runner_tmp={module}.$$ && "
        f"printf %s {shlex.quote(encoded)} | base64 -d > \"$runner_tmp\" && "
        f"chmod 600 \"$runner_tmp\" && mv -f \"$runner_tmp\" {module}"
    )


async def ssh_stage_job_runner(worker: Worker) -> SSHResult:
    """Stage supervision through the same authenticated SSH readiness path."""
    return await _exec_ssh(
        worker.id, _ssh_base_args(worker) + [_runner_stage_command(worker)],
        lane_timeout=10.0, cmd_timeout=30.0, op_name="ssh_stage_job_runner",
    )


def _cluster_gpu_mask(worker: Worker, gpu_indices: list[int] | None) -> str:
    if worker.slurm.compute_kind == "cpu":
        if gpu_indices:
            raise ValueError("CPU allocations cannot launch GPU jobs")
        return ""
    ids = worker.gpu_device_ids
    if (len(ids) != worker.gpu_count or len(set(ids)) != len(ids)
            or any(not re.fullmatch(r"GPU-[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", value) for value in ids)):
        raise ValueError("Allocation requires an unambiguous physical GPU UUID map")
    indices = list(range(worker.gpu_count)) if gpu_indices is None else gpu_indices
    if (len(set(indices)) != len(indices)
            or any(type(index) is not int or not 0 <= index < len(ids) for index in indices)):
        raise ValueError("Invalid allocation GPU indices")
    return ",".join(ids[index] for index in indices)


def _build_cluster_job_command(
    worker: Worker, job_id: str, command: str,
    gpu_indices: list[int] | None, queue_managed: bool,
) -> str:
    directory = f"{_worker_harness_dir(worker)}/{job_id}"
    quoted = shlex.quote(directory)
    metadata = {"job_id": job_id, "worker_id": worker.id, "queue_managed": queue_managed}
    inputs = {
        "script.sh": "#!/bin/bash\n" + command + "\n",
        "gpu.env": f"CUDA_VISIBLE_DEVICES={_cluster_gpu_mask(worker, gpu_indices)}\n",
        "launch.json": json.dumps(metadata) + "\n",
    }
    writes = [
        f"printf %s {shlex.quote(base64.b64encode(value.encode()).decode())} | base64 -d "
        f"> {shlex.quote(directory + '/' + name)}"
        for name, value in inputs.items()
    ]
    launch = shlex.join([
        "python3", f"{_worker_harness_dir(worker)}/.job_runner.py",
        "run", "--job-dir", directory,
    ])
    return (
        _runner_stage_command(worker) + " && "
        f"mkdir -m 700 {quoted} && " + " && ".join(writes) + " && "
        f"{_tmux_env(worker)} new-session -d -s {shlex.quote('wh_' + job_id)} {shlex.quote(launch)} && "
        # Do not mark RUNNING merely because tmux accepted a process. A drain
        # rejection is definite; a lost/hanging startup remains unknown.
        f"for i in $(seq 1 100); do "
        f"if test -e {quoted}/runtime.json || test -e {quoted}/result.json; then echo started; exit 0; fi; "
        f"if test -e {quoted}/launch_error.json; then cat {quoted}/launch_error.json >&2; exit 1; fi; "
        f"sleep 0.05; done; exit 255"
    )


def _build_job_command(
    worker: Worker,
    job_id: str,
    command: str,
    gpu_indices: list[int] | None = None,
    queue_managed: bool = False,
) -> str:
    """Compose the legacy or structured allocation job launch."""
    if worker.slurm is not None:
        return _build_cluster_job_command(worker, job_id, command, gpu_indices, queue_managed)
    harness_dir = f"{_worker_harness_dir(worker)}/{job_id}"
    script_path = f"{harness_dir}/script.sh"
    log_path = f"{harness_dir}/output.log"
    gpu_export = (
        f"export CUDA_VISIBLE_DEVICES={','.join(str(index) for index in gpu_indices)}\n"
        if gpu_indices is not None
        else ""
    )

    script_content = (
        f"#!/bin/bash\n"
        f"exec >>{log_path} 2>&1\n"
        f"{gpu_export}"
        f"({command}); ec=$?\n"
        f"echo EXIT:$ec\n"
        f"sleep 60\n"
        f"tmux kill-session -t wh_{job_id} 2>/dev/null\n"
    )
    script_b64 = base64.b64encode(script_content.encode()).decode().rstrip()

    full_cmd = (
        f"mkdir -p {harness_dir} && "
        f"echo '{script_b64}' | base64 -d > {script_path} && "
        f"chmod +x {script_path} && "
        f"{_tmux_env(worker)} new-session -d -s wh_{job_id} 'bash {script_path}' && "
        f"echo 'started'"
    )
    return full_cmd


async def ssh_tmux_new(
    worker: Worker,
    job_id: str,
    command: str,
    pty_enabled: bool = True,
    gpu_indices: list[int] | None = None,
    queue_managed: bool = False,
) -> SSHResult:
    """Start a tmux job on the worker."""
    full_cmd = _build_job_command(worker, job_id, command, gpu_indices, queue_managed)
    args = _ssh_base_args(worker) + [full_cmd]
    # Note: pty_enabled is informational here (jobs always run via tmux).
    return await _exec_ssh(worker.id, args, lane_timeout=10.0, cmd_timeout=30.0, op_name="ssh_tmux_new")


async def ssh_tmux_kill(worker: Worker, job_id: str) -> SSHResult:
    if worker.slurm is not None:
        return await ssh_job_control(worker, job_id, action="stop", signal_name="TERM")
    session = f"wh_{job_id}"
    tmux = _tmux_env(worker)
    cmd = (
        f"{tmux} kill-session -t '{session}' 2>/dev/null || true; "
        f"{tmux} has-session -t '{session}' 2>/dev/null && echo still_running || echo stopped"
    )
    return await async_ssh_run(worker, cmd, timeout=10)

async def ssh_job_started(worker: Worker, job_id: str) -> bool | None:
    """Probe durable launch evidence, including jobs whose tmux session already exited."""
    if worker.slurm is not None:
        directory = f"{_worker_harness_dir(worker)}/{job_id}"
        probe = (
            "import json, pathlib, sys\n"
            "directory=pathlib.Path(sys.argv[1])\n"
            "records={}\n"
            "for name in ('runtime','result','launch_error'):\n"
            " try: records[name]=json.loads((directory/(name+'.json')).read_text())\n"
            " except (OSError,ValueError): pass\n"
            "print(json.dumps(records))\n"
        )
        result = await async_ssh_run(
            worker, shlex.join(["python3", "-c", probe, directory]), timeout=5,
        )
        if result.returncode != 0:
            return None
        try:
            records = json.loads(result.stdout)
            if "result" in records:
                RunnerResult.model_validate(records["result"])
                return True
            runtime = records.get("runtime")
            if isinstance(runtime, dict):
                if (runtime.get("job_id") == job_id
                        and type(runtime.get("runner_pid")) is int and runtime["runner_pid"] > 0
                        and type(runtime.get("start_ticks")) is int
                        and isinstance(runtime.get("boot_id"), str) and runtime["boot_id"]
                        and type(runtime.get("queue_managed")) is bool
                        and runtime.get("checkpoint_signal") in ("USR1", "TERM")):
                    return True
                return None
            error = records.get("launch_error")
            if isinstance(error, dict) and isinstance(error.get("error"), str) and error["error"]:
                return False
        except (ValueError, AttributeError, TypeError):
            pass
        return None
    session = f"wh_{job_id}"
    tmux = _tmux_env(worker)
    log_path = shlex.quote(f"{_worker_harness_dir(worker)}/{job_id}/output.log")
    result = await async_ssh_run(
        worker,
        f"if {tmux} has-session -t '={session}' 2>/dev/null; then echo exists; "
        f"elif test -e {log_path}; then echo exists; else echo absent; fi",
        timeout=5,
    )
    if result.returncode != 0:
        return None
    output = result.stdout.strip()
    if output == "exists":
        return True
    if output == "absent":
        return False
    return None


async def ssh_tmux_running(worker: Worker, job_id: str) -> bool | None:
    """Return remote running state, or None when the SSH probe failed."""
    log_path = f"{_worker_harness_dir(worker)}/{job_id}/output.log"
    if worker.slurm is not None:
        return False if await ssh_job_result(worker, job_id) is not None else None
    result = await async_ssh_run(
        worker,
        f"grep -q '^EXIT:' '{log_path}' 2>/dev/null && echo 'done' || echo 'running'",
        timeout=5,
    )
    if result.returncode != 0:
        log.warning(
            "Could not probe job %s on %s: %s",
            job_id,
            worker.name,
            result.stderr.strip() or f"exit {result.returncode}",
        )
        return None
    return result.stdout.strip() == "running"


async def ssh_tmux_capture(worker: Worker, job_id: str) -> str:
    session = f"wh_{job_id}"
    cmd = f"{_tmux_env(worker)} capture-pane -t '{session}' -p 2>/dev/null"
    result = await async_ssh_run(worker, cmd, timeout=5)
    return result.stdout


async def ssh_read_log(
    worker: Worker,
    job_id: str,
    tail: int | None = None,
    head: int | None = None,
    *,
    timeout: int = 10,
) -> str:
    log_path = f"{_worker_harness_dir(worker)}/{job_id}/output.log"
    if head is not None:
        cmd = f"head -n {head} '{log_path}' 2>/dev/null"
    elif tail is not None and tail > 0:
        cmd = f"tail -n {tail} '{log_path}' 2>/dev/null"
    elif tail == 0:
        cmd = f"grep -E '^EXIT:' '{log_path}' 2>/dev/null || echo 'still running'"
    else:
        cmd = f"tail -n 10 '{log_path}' 2>/dev/null"
    result = await async_ssh_run(worker, cmd, timeout=timeout)
    return result.stdout


async def ssh_get_exit_code(worker: Worker, job_id: str) -> int | None:
    if worker.slurm is not None:
        result = await ssh_job_result(worker, job_id)
        return result.exit_code if result is not None else None
    result = await async_ssh_run(
        worker,
        f"grep -E '^EXIT:' '{_worker_harness_dir(worker)}/{job_id}/output.log' 2>/dev/null | sed 's/EXIT://'",
        timeout=5,
    )
    if result.returncode == 0 and result.stdout.strip():
        try:
            return int(result.stdout.strip())
        except ValueError:
            pass
    return None



async def ssh_job_result(worker: Worker, job_id: str) -> RunnerResult | None:
    """Read validated descendant-exit proof; missing evidence is not completion."""
    path = shlex.quote(f"{_worker_harness_dir(worker)}/{job_id}/result.json")
    response = await async_ssh_run(worker, f"cat {path} 2>/dev/null", timeout=5)
    if response.returncode != 0:
        return None
    try:
        return RunnerResult.model_validate_json(response.stdout)
    except ValueError:
        return None


async def ssh_job_control(
    worker: Worker, job_id: str, *, action: str, signal_name: str,
) -> SSHResult:
    if action not in ("stop", "checkpoint") or signal_name not in ("USR1", "TERM"):
        raise ValueError("Invalid runner control request")
    command = shlex.join([
        "python3", f"{_worker_harness_dir(worker)}/.job_runner.py", "control",
        "--job-dir", f"{_worker_harness_dir(worker)}/{job_id}",
        "--action", action, "--signal", signal_name,
    ])
    return await async_ssh_run(worker, command, timeout=15)

async def ssh_upload_bytes(worker: Worker, content: bytes, remote_path: str, *, timeout: int = 60) -> SSHResult:
    """Upload raw bytes to a worker path."""
    remote_parent = shlex.quote(str(Path(remote_path).parent))
    remote_file = shlex.quote(remote_path)
    cmd = f"mkdir -p {remote_parent} && cat > {remote_file}"
    # Tailscale SSH preserves a single remote-command argument reliably, but
    # does not preserve a separate `sh`, `-lc`, command argv vector. Quote the
    # complete shell invocation so remote paths and stdin redirection survive.
    args = _ssh_base_args(worker) + [f"sh -lc {shlex.quote(cmd)}"]
    return await _exec_ssh(
        worker.id, args,
        lane_timeout=10.0,
        cmd_timeout=float(timeout),
        op_name="ssh_upload_bytes",
        input_data=content,
    )


async def ssh_download_bytes(
    worker: Worker,
    remote_path: str,
    max_bytes: int = 10 * 1024 * 1024,
    *,
    timeout: int = 30,
) -> tuple[bytes, SSHResult]:
    """Download a file from a worker. Returns (content, ssh_result)."""
    remote_file = shlex.quote(remote_path)
    cmd = f"cat {remote_file}"
    args = _ssh_base_args(worker) + [f"sh -lc {shlex.quote(cmd)}"]
    # We need bytes back, so use a custom path (not _exec_ssh which decodes).
    started = time.monotonic()
    from .metrics import get_metrics
    metrics = get_metrics()
    async with _lanes_or_default().acquire(worker.id, timeout=10.0):
        try:
            proc = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=True,
            )
        except FileNotFoundError:
            metrics.observe_ssh("ssh_download_bytes", (time.monotonic() - started) * 1000)
            return b"", SSHResult(stdout="", stderr="ssh command not found", returncode=127)
        try:
            try:
                stdout_b, stderr_b = await asyncio.wait_for(
                    proc.communicate(), timeout=float(timeout),
                )
            except asyncio.TimeoutError:
                await _terminate_async_process(proc)
                metrics.observe_ssh("ssh_download_bytes", (time.monotonic() - started) * 1000)
                return b"", SSHResult(
                    stdout="",
                    stderr=f"Command timed out after {timeout}s",
                    returncode=-1,
                )
            except asyncio.CancelledError:
                await _terminate_async_process(proc)
                raise
            else:
                metrics.observe_ssh("ssh_download_bytes", (time.monotonic() - started) * 1000)
                content = stdout_b or b""
                if len(content) > max_bytes:
                    content = content[:max_bytes]
                return content, SSHResult(
                    stdout="",
                    stderr=(stderr_b or b"").decode(errors="replace"),
                    returncode=proc.returncode or 0,
                )
        finally:
            if proc.returncode is None:
                await _terminate_async_process(proc)


async def ssh_port_forward(
    worker: Worker,
    local_port: int,
    remote_port: int,
    *,
    bind_host: str = "0.0.0.0",
) -> subprocess.Popen:
    """Start a persistent port-forward tunnel to the worker. Returns the
    Popen handle. The caller (heartbeat.py) registers it in TunnelRegistry.

    ``bind_host`` controls the orchestrator-side listener. Generic tunnels
    retain the legacy all-interface default; service primitives should bind
    explicitly to the orchestrator's Tailnet address.

    The per-worker SSH lane is held only during SSH connection setup; the
    returned Popen runs independently and is NOT in the lane.
    """
    from .metrics import get_metrics
    metrics = get_metrics()
    args = _ssh_base_args(worker) + [
        "-o", "ExitOnForwardFailure=yes",
        "-N", "-g",
        "-L", f"{bind_host}:{local_port}:localhost:{remote_port}",
    ]
    started = time.monotonic()
    async with _lanes_or_default().acquire(worker.id, timeout=10.0):
        proc = subprocess.Popen(
            args,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        try:
            # Brief wait to let SSH handshake complete and ExitOnForwardFailure
            # to take effect. The persistent Popen is returned after this and
            # deliberately no longer occupies the lane.
            for _ in range(10):
                if proc.poll() is not None:
                    break
                await asyncio.sleep(0.05)
            metrics.observe_ssh("ssh_port_forward", (time.monotonic() - started) * 1000)
            return proc
        except BaseException:
            # A cancellation while the handshake is in progress must not leak
            # a long-lived port-forward subprocess or ProxyCommand descendant.
            _terminate_popen_process_group(proc)
            raise