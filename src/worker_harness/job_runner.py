"""Standalone Linux subreaper for tmux-hosted allocation jobs.

Only result.json proves completion: neither shell exit, tmux disappearance nor
workload output proves that all descendants have exited. Staged over normal SSH.
"""
from __future__ import annotations

import argparse
import ctypes
import fcntl
import json
import math
import os
from pathlib import Path
import re
import select
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time


def _atomic_json(path: Path, value: dict) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(value, stream)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


def _process(pid: int) -> tuple[int, int, str] | None:
    """Parent PID/start ticks/state; /proc comm can contain spaces and ')'."""
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
        fields = stat[stat.rfind(")") + 2:].split()
        return int(fields[1]), int(fields[19]), fields[0]
    except (OSError, ValueError, IndexError):
        return None


def _is_descendant(pid: int, ancestor: int) -> bool:
    seen = set()
    while pid > 1 and pid not in seen:
        seen.add(pid)
        identity = _process(pid)
        if identity is None:
            return False
        pid = identity[0]
        if pid == ancestor:
            return True
    return False


def _descendants(runner_pid: int) -> dict[int, int]:
    processes = {}
    for entry in Path("/proc").iterdir():
        if entry.name.isdigit():
            identity = _process(int(entry.name))
            if identity is not None:
                processes[int(entry.name)] = identity
    descendants = {}
    for pid, (_, ticks, _) in processes.items():
        parent = pid
        seen = set()
        while parent in processes and parent not in seen:
            seen.add(parent)
            parent = processes[parent][0]
            if parent == runner_pid:
                descendants[pid] = ticks
                break
    return descendants


def _signal_descendant(pid: int, ticks: int, runner_pid: int, sig: int) -> bool:
    # Recheck identity and ancestry after pidfd_open; PID reuse cannot redirect
    # a signal once the descriptor pins the process.
    identity = _process(pid)
    if identity is None or identity[1] != ticks or not _is_descendant(pid, runner_pid):
        return False
    try:
        descriptor = os.pidfd_open(pid)
    except ProcessLookupError:
        return False
    try:
        identity = _process(pid)
        if identity is None or identity[1] != ticks or not _is_descendant(pid, runner_pid):
            return False
        try:
            signal.pidfd_send_signal(descriptor, sig)
            return True
        except ProcessLookupError:
            return False
    finally:
        os.close(descriptor)


def _preflight() -> None:
    if not sys.platform.startswith("linux"):
        raise RuntimeError("Job supervision requires Linux")
    if not hasattr(os, "pidfd_open") or not hasattr(signal, "pidfd_send_signal"):
        raise RuntimeError("Job supervision requires pidfds")
    descriptor = os.pidfd_open(os.getpid())
    try:
        signal.pidfd_send_signal(descriptor, 0)
    finally:
        os.close(descriptor)
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:  # PR_SET_CHILD_SUBREAPER
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))


def _read_result(directory: Path) -> dict | None:
    try:
        value = json.loads((directory / "result.json").read_text())
        if (value.get("descendants_exited") is True
                and type(value.get("exit_code")) is int
                and type(value.get("finished_at")) in (int, float)
                and math.isfinite(value["finished_at"]) and value["finished_at"] > 0
                and type(value.get("finished_monotonic")) in (int, float)
                and math.isfinite(value["finished_monotonic"]) and value["finished_monotonic"] >= 0
                and isinstance(value.get("boot_id"), str) and value["boot_id"]):
            started_at = value.get("started_at")
            started_monotonic = value.get("started_monotonic")
            if started_at is not None or started_monotonic is not None:
                if (type(started_at) not in (int, float)
                        or type(started_monotonic) not in (int, float)
                        or not math.isfinite(started_at) or not math.isfinite(started_monotonic)
                        or started_at <= 0
                        or not 0 <= started_monotonic <= value["finished_monotonic"]):
                    return None
            return value
    except (OSError, ValueError, AttributeError):
        pass
    return None


def run(directory: Path) -> int:
    os.umask(0o077)
    directory = directory.resolve()
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    directory.chmod(0o700)
    with (directory / "runner.lock").open("w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return 1
        with (directory / "output.log").open("ab", buffering=0) as output:
            listener = None
            socket_dir = None
            launch_lock = None
            root = None
            stop_at = None
            reason = None
            checkpoint_signal = None

            def request_stop() -> None:
                nonlocal stop_at, reason
                if stop_at is None:
                    stop_at = time.monotonic()
                    reason = "user_cancelled"

            def handle_signal(signum, frame) -> None:
                nonlocal checkpoint_signal
                if signum == signal.SIGUSR1:
                    if stop_at is None:
                        checkpoint_signal = signal.SIGUSR1
                else:
                    request_stop()

            for sig in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT, signal.SIGUSR1):
                signal.signal(sig, handle_signal)
            try:
                _preflight()
                if (directory / "runtime.json").exists() or (directory / "result.json").exists():
                    raise RuntimeError("Refusing to reuse an existing job runtime")
                script = directory / "script.sh"
                script.read_bytes()
                metadata = json.loads((directory / "launch.json").read_text())
                if "worker_id" in metadata:
                    settings = json.loads((directory.parent.parent.parent / "settings.json").read_text())
                    if settings.get("worker_id") != metadata["worker_id"]:
                        raise RuntimeError("Allocation settings identity mismatch")
                    metadata["checkpoint_signal"] = settings["checkpoint_signal"]
                if (metadata.get("job_id") != directory.name
                        or type(metadata.get("queue_managed")) is not bool
                        or metadata.get("checkpoint_signal") not in ("USR1", "TERM")):
                    raise RuntimeError("Invalid launch metadata")
                gpu_env = (directory / "gpu.env").read_text().removesuffix("\n")
                key, separator, value = gpu_env.partition("=")
                if (key != "CUDA_VISIBLE_DEVICES" or not separator
                        or (value and any(not re.fullmatch(r"GPU-[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", part)
                                          for part in value.split(",")))
                        or len(value.split(",")) != len(set(value.split(",")))):
                    raise RuntimeError("GPU mask must contain unique physical UUIDs or be empty")
                identity = _process(os.getpid())
                if identity is None:
                    raise RuntimeError("Cannot establish supervisor process identity")
                boot_id = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
                # Shared paths routinely exceed AF_UNIX's 108-byte pathname
                # limit. Keep the socket in an unpredictable private short dir.
                socket_dir = Path(tempfile.mkdtemp(prefix="wh-job-", dir="/tmp"))
                socket_path = socket_dir / "control.sock"
                listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                listener.bind(str(socket_path))
                socket_path.chmod(0o600)
                listener.listen(8)
                listener.setblocking(False)
                launch_lock = (directory.parent / ".launch.lock").open("a")
                fcntl.flock(launch_lock, fcntl.LOCK_EX)
                if (directory.parent / ".draining").exists():
                    raise RuntimeError("Allocation is draining; launch refused")
                if stop_at is not None:
                    raise RuntimeError("Launch was stopped before workload start")
                # A durable runtime is written under the fence before releasing
                # it. Daemon drain checks and delayed SSH launches cannot cross.
                environment = os.environ.copy()
                environment["CUDA_VISIBLE_DEVICES"] = value
                root = subprocess.Popen(
                    ["/bin/bash", str(script)], env=environment,
                    stdin=subprocess.DEVNULL, stdout=output, stderr=output,
                    start_new_session=True,
                )
                started_at = time.time()
                started_monotonic = time.monotonic()
                _atomic_json(directory / "runtime.json", {
                    "job_id": directory.name, "runner_pid": os.getpid(),
                    "start_ticks": identity[1], "boot_id": boot_id,
                    "started_at": started_at, "started_monotonic": started_monotonic,
                    "socket_path": str(socket_path),
                    "queue_managed": metadata["queue_managed"],
                    "checkpoint_signal": metadata["checkpoint_signal"],
                })
            except Exception as error:
                output.write(f"Job launch failed: {error}\n".encode())
                if root is None:
                    if not (directory / "runtime.json").exists():
                        _atomic_json(directory / "launch_error.json", {"error": str(error)})
                    if listener is not None:
                        listener.close()
                    if socket_dir is not None:
                        (socket_dir / "control.sock").unlink(missing_ok=True)
                        socket_dir.rmdir()
                    return 1
                # An initialization failure after fork must still reap every
                # descendant. A crash deliberately leaves no completion proof.
                request_stop()
            finally:
                if launch_lock is not None:
                    launch_lock.close()

            exit_code = None
            term_sent = set()
            try:
                while True:
                    no_children = False
                    while True:
                        try:
                            pid, status = os.waitpid(-1, os.WNOHANG)
                        except ChildProcessError:
                            no_children = True
                            break
                        if pid == 0:
                            break
                        if pid == root.pid:
                            code = os.waitstatus_to_exitcode(status)
                            root.returncode = code
                            exit_code = code if code >= 0 else 128 - code
                    if no_children:
                        if exit_code is None:
                            raise RuntimeError("Root shell exit status unavailable")
                        _atomic_json(directory / "result.json", {
                            "exit_code": exit_code, "termination_reason": reason,
                            "descendants_exited": True, "finished_at": time.time(),
                            "finished_monotonic": time.monotonic(), "boot_id": boot_id,
                            "started_at": started_at, "started_monotonic": started_monotonic,
                        })
                        return 0
                    if stop_at is not None:
                        checkpoint_signal = None  # Stop is sticky, including later USR1.
                        force = time.monotonic() - stop_at >= 2.0
                        for pid, ticks in _descendants(os.getpid()).items():
                            identity_key = (pid, ticks)
                            if force or identity_key not in term_sent:
                                if _signal_descendant(pid, ticks, os.getpid(),
                                                      signal.SIGKILL if force else signal.SIGTERM):
                                    term_sent.add(identity_key)
                    elif checkpoint_signal is not None:
                        for pid, ticks in _descendants(os.getpid()).items():
                            _signal_descendant(pid, ticks, os.getpid(), checkpoint_signal)
                        checkpoint_signal = None
                    ready, _, _ = select.select([listener], [], [], 0.05)
                    if not ready:
                        continue
                    try:
                        connection, _ = listener.accept()
                    except BlockingIOError:
                        continue
                    with connection:
                        connection.settimeout(0.2)
                        try:
                            _, uid, _ = struct.unpack("3i", connection.getsockopt(
                                socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i")))
                            if uid != os.getuid():
                                continue
                            payload = bytearray()
                            while b"\n" not in payload and len(payload) <= 4096:
                                chunk = connection.recv(4097 - len(payload))
                                if not chunk:
                                    break
                                payload.extend(chunk)
                            request = json.loads(payload)
                            action = request.get("action")
                            sig = request.get("signal")
                            if action not in ("stop", "checkpoint") or sig not in ("USR1", "TERM"):
                                continue
                            if action == "stop":
                                request_stop()
                            elif stop_at is None:
                                checkpoint_signal = getattr(signal, f"SIG{sig}")
                            connection.sendall(b'{"state":"requested"}\n')
                        except (OSError, ValueError, AttributeError):
                            pass
            finally:
                listener.close()
                (socket_dir / "control.sock").unlink(missing_ok=True)
                socket_dir.rmdir()


def control(directory: Path, action: str, requested_signal: str) -> int:
    directory = directory.resolve()
    result = _read_result(directory)
    if result is not None:
        print(json.dumps({"state": "finished", **result}), flush=True)
        return 0
    try:
        runtime = json.loads((directory / "runtime.json").read_text())
        identity = _process(runtime["runner_pid"])
        if (runtime["boot_id"] != Path("/proc/sys/kernel/random/boot_id").read_text().strip()
                or identity is None or identity[1] != runtime["start_ticks"]):
            raise ValueError("Supervisor identity no longer matches")
        path = Path(runtime["socket_path"])
        parent = path.parent.stat()
        if (path.name != "control.sock" or path.parent.parent != Path("/tmp")
                or not path.parent.name.startswith("wh-job-") or parent.st_uid != os.getuid()
                or parent.st_mode & 0o077):
            raise ValueError("Invalid private control socket")
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
            connection.settimeout(1.0)
            connection.connect(str(path))
            connection.sendall(json.dumps({"action": action, "signal": requested_signal}).encode() + b"\n")
            response = json.loads(connection.recv(4096))
            if response.get("state") != "requested":
                raise ValueError("Control request not accepted")
        if action == "checkpoint":
            print(json.dumps({"state": "requested"}), flush=True)
            return 0
        deadline = time.monotonic() + 10.0
        while result is None and time.monotonic() < deadline:
            result = _read_result(directory)
            if result is None:
                time.sleep(0.05)
    except (OSError, ValueError, KeyError, TypeError):
        result = _read_result(directory)
    if result is not None:
        print(json.dumps({"state": "finished", **result}), flush=True)
        return 0
    print(json.dumps({"state": "unknown"}), flush=True)
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    launch = commands.add_parser("run")
    launch.add_argument("--job-dir", type=Path, required=True)
    request = commands.add_parser("control")
    request.add_argument("--job-dir", type=Path, required=True)
    request.add_argument("--action", choices=("stop", "checkpoint"), required=True)
    request.add_argument("--signal", choices=("USR1", "TERM"), required=True)
    args = parser.parse_args()
    if args.command == "run":
        return run(args.job_dir)
    return control(args.job_dir, args.action, args.signal)


if __name__ == "__main__":
    raise SystemExit(main())
