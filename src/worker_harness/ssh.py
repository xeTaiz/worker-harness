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
import logging
import os
import shlex
import signal
import subprocess  # Popen return type for ssh_port_forward
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from .models import Worker

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


async def _terminate_async_process(proc: asyncio.subprocess.Process, grace_seconds: float = 2.0) -> None:
    """Terminate the complete process group of an async SSH call.

    All subprocesses use ``start_new_session=True``. Killing their process
    group avoids the shell-wrapper leak where killing `tailscale` leaves an
    underlying ssh child alive and holding a connection.
    """
    if proc.returncode is not None:
        return
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        return
    try:
        await asyncio.wait_for(proc.wait(), timeout=grace_seconds)
        return
    except asyncio.TimeoutError:
        pass
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass
    try:
        await asyncio.wait_for(proc.wait(), timeout=1.0)
    except asyncio.TimeoutError:
        pass


def _terminate_popen_process_group(proc: subprocess.Popen, grace_seconds: float = 2.0) -> None:
    """Synchronous sibling for persistent tunnel Popen cleanup."""
    if proc.poll() is not None:
        return
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        return
    try:
        proc.wait(timeout=grace_seconds)
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass
    try:
        proc.wait(timeout=1.0)
    except subprocess.TimeoutExpired:
        pass


# ── Argument helpers ───────────────────────────────────────────────────


def _ssh_target(worker: Worker) -> str:
    return f"{worker.ssh_user}@{worker.ssh_host}"


def _ssh_base_args(worker: Worker) -> list[str]:
    return ["tailscale", "ssh", _ssh_target(worker)]


def _worker_harness_dir(worker: Worker) -> str:
    return worker.harness_dir.rstrip("/") or "/harness"


def _worker_tmux_tmpdir(worker: Worker) -> str:
    return f"{Path(_worker_harness_dir(worker)).parent}/tmux"


def _tmux_env(worker: Worker) -> str:
    # Prepend /usr/local/cuda/bin and /usr/local/nvidia/bin to PATH so jobs
    # can call `nvcc` / `nvidia-smi` directly.
    return (
        f"env -u TMUX -u TMUX_PANE "
        f"TMUX_TMPDIR='{_worker_tmux_tmpdir(worker)}' "
        f"PATH=/usr/local/cuda/bin:/usr/local/nvidia/bin:${{PATH}} "
        f"tmux"
    )


# ── Core executor: lane + kill-on-exit + metrics ───────────────────────


async def _exec_ssh(
    worker: Worker,
    args: Sequence[str],
    *,
    lane_timeout: float = 10.0,
    cmd_timeout: float = 30.0,
    op_name: str = "ssh",
    input_data: bytes | None = None,
) -> SSHResult:
    """Run an ssh subprocess inside the per-worker lane with hard kill
    on cancel / timeout / exception.

    Returns SSHResult. Never raises TimeoutError — returns SSHResult with
    returncode=-1 and a stderr message instead, so callers don't have to
    distinguish "subprocess timed out" from "subprocess exited with code X".

    Propagates CancelledError to the caller (the FastAPI handler will turn
    that into 499 / 503 as appropriate).
    """
    from .metrics import get_metrics
    metrics = get_metrics()

    started = time.monotonic()
    async with _lanes_or_default().acquire(worker.id, timeout=lane_timeout):
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
            return SSHResult(stdout="", stderr="ssh command not found", returncode=127)

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
                return SSHResult(
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
                return SSHResult(
                    stdout=(stdout_b or b"").decode(errors="replace"),
                    stderr=(stderr_b or b"").decode(errors="replace"),
                    returncode=proc.returncode or 0,
                )
        finally:
            # Belt-and-suspenders: if anything escapes the above (e.g., an
            # exception in communicate's decode step), still ensure the
            # subprocess is dead so we don't leak.
            if proc.returncode is None:
                await _terminate_async_process(proc)


# ── Public SSH calls ───────────────────────────────────────────────────


async def async_ssh_run(worker: Worker, command: str, *, timeout: int = 30) -> SSHResult:
    """Run `command` over ssh on the worker, return result."""
    args = _ssh_base_args(worker) + [command]
    return await _exec_ssh(
        worker, args,
        lane_timeout=min(10.0, max(1.0, timeout / 3)),
        cmd_timeout=float(timeout),
        op_name="async_ssh_run",
    )


async def async_ssh_run_pty(worker: Worker, command: str, *, timeout: int = 60) -> SSHResult:
    """Run `command` over ssh with a pseudo-tty."""
    args = _ssh_base_args(worker) + ["-tt", command]
    return await _exec_ssh(
        worker, args,
        lane_timeout=min(10.0, max(1.0, timeout / 3)),
        cmd_timeout=float(timeout),
        op_name="async_ssh_run_pty",
    )


def _build_job_command(worker: Worker, job_id: str, command: str) -> str:
    """Compose the remote job-launch command. Pure string assembly — no I/O."""
    harness_dir = f"{_worker_harness_dir(worker)}/{job_id}"
    script_path = f"{harness_dir}/script.sh"
    log_path = f"{harness_dir}/output.log"

    script_content = (
        f"#!/bin/bash\n"
        f"exec >>{log_path} 2>&1\n"
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


async def ssh_tmux_new(worker: Worker, job_id: str, command: str, pty_enabled: bool = True) -> SSHResult:
    """Start a tmux job on the worker."""
    full_cmd = _build_job_command(worker, job_id, command)
    args = _ssh_base_args(worker) + [full_cmd]
    # Note: pty_enabled is informational here (jobs always run via tmux).
    return await _exec_ssh(worker, args, lane_timeout=10.0, cmd_timeout=30.0, op_name="ssh_tmux_new")


async def ssh_tmux_kill(worker: Worker, job_id: str) -> SSHResult:
    session = f"wh_{job_id}"
    tmux = _tmux_env(worker)
    cmd = (
        f"{tmux} kill-session -t '{session}' 2>/dev/null || true; "
        f"{tmux} has-session -t '{session}' 2>/dev/null && echo still_running || echo stopped"
    )
    return await async_ssh_run(worker, cmd, timeout=10)


async def ssh_tmux_running(worker: Worker, job_id: str) -> bool:
    log_path = f"{_worker_harness_dir(worker)}/{job_id}/output.log"
    result = await async_ssh_run(
        worker,
        f"grep -q '^EXIT:' '{log_path}' 2>/dev/null && echo 'done' || echo 'running'",
        timeout=5,
    )
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
        worker, args,
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


async def ssh_port_forward(worker: Worker, local_port: int, remote_port: int) -> subprocess.Popen:
    """Start a persistent port-forward tunnel to the worker. Returns the
    Popen handle. The caller (heartbeat.py) registers it in TunnelRegistry.

    The per-worker SSH lane is held only during SSH connection setup; the
    returned Popen runs independently and is NOT in the lane.
    """
    from .metrics import get_metrics
    metrics = get_metrics()
    args = _ssh_base_args(worker) + [
        "-N", "-g",
        "-L", f"0.0.0.0:{local_port}:localhost:{remote_port}",
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
            # a long-lived port-forward subprocess.
            if proc.poll() is None:
                _terminate_popen_process_group(proc)
            raise