"""Standalone Linux job supervisor, copied verbatim to each remote job directory.

Only the supervisor's durable result proves completion.  The root shell exiting,
its terminal disappearing, and text in the workload log are not completion.
"""
from __future__ import annotations

import argparse
import ctypes
import fcntl
import json
import os
from pathlib import Path
import select
import signal
import socket
import subprocess
import sys
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
    """Return parent PID, start ticks, state; comm may contain spaces and ')'."""
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
            pid = int(entry.name)
            identity = _process(pid)
            if identity is not None:
                processes[pid] = identity
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
    # Check ancestry before opening, then identity/ancestry again after opening.
    # The descriptor pins the process, so exit/PID reuse cannot redirect a signal.
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
        raise RuntimeError("Job supervision requires pidfd_open and pidfd_send_signal")
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
        if value.get("descendants_exited") is True and type(value.get("exit_code")) is int:
            return value
    except (OSError, ValueError, AttributeError):
        pass
    return None


def run(directory: Path) -> int:
    os.umask(0o077)
    directory = directory.resolve()
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    directory.chmod(0o700)
    log_fd = os.open(directory / "output.log", os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    os.fchmod(log_fd, 0o600)
    with os.fdopen(log_fd, "ab", buffering=0) as output:
        lock_fd = os.open(directory / "runner.lock", os.O_WRONLY | os.O_CREAT, 0o600)
        with os.fdopen(lock_fd, "w") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                output.write(b"A job supervisor is already running\n")
                return 1
            try:
                _preflight()
                if (directory / "runtime.json").exists() or (directory / "result.json").exists():
                    raise RuntimeError("Refusing to reuse an existing job runtime")
                script = directory / "script.sh"
                script.read_bytes()  # Fail before starting if either launch input is missing.
                gpu_env = (directory / "gpu.env").read_text().removesuffix("\n")
                key, separator, value = gpu_env.partition("=")
                if key != "CUDA_VISIBLE_DEVICES" or not separator or "\n" in value:
                    raise RuntimeError("Invalid gpu.env")
                if value and any(not part.isdecimal() for part in value.split(",")):
                    raise RuntimeError("Invalid GPU indices")
                identity = _process(os.getpid())
                if identity is None:
                    raise RuntimeError("Cannot establish supervisor process identity")
                boot_id = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
                listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                try:
                    listener.bind(str(directory / "control.sock"))
                    os.chmod(directory / "control.sock", 0o600)
                    listener.listen(8)
                    listener.setblocking(False)
                except BaseException:
                    listener.close()
                    raise
            except Exception as error:
                output.write(f"Job launch failed: {error}\n".encode())
                # This marker is only written before any workload can exist.
                if not (directory / "runtime.json").exists():
                    _atomic_json(directory / "launch_error.json", {"error": str(error)})
                return 1

            reason = None
            stop_at = None

            def request_stop(requested_reason: str) -> None:
                nonlocal reason, stop_at
                if stop_at is None:
                    reason = requested_reason
                    stop_at = time.monotonic()

            def handle_signal(signum, frame) -> None:
                request_stop("user_cancelled")

            signal.signal(signal.SIGTERM, handle_signal)
            signal.signal(signal.SIGHUP, handle_signal)
            root = None
            try:
                environment = os.environ.copy()
                environment["CUDA_VISIBLE_DEVICES"] = value
                # Allocated indices are nvidia-smi (PCI bus) indices; CUDA's
                # default FASTEST_FIRST order differs on mixed-GPU hosts.
                environment["CUDA_DEVICE_ORDER"] = "PCI_BUS_ID"
                root = subprocess.Popen(
                    ["bash", str(script)], env=environment,
                    stdin=subprocess.DEVNULL, stdout=output, stderr=output,
                    start_new_session=True,
                )
            except Exception as error:
                output.write(f"Job launch failed: {error}\n".encode())
                _atomic_json(directory / "launch_error.json", {"error": str(error)})
                listener.close()
                (directory / "control.sock").unlink(missing_ok=True)
                return 1

            exit_code = None
            term_sent = set()
            try:
                try:
                    _atomic_json(directory / "runtime.json", {
                        "job_id": directory.name,
                        "runner_pid": os.getpid(), "start_ticks": identity[1],
                        "boot_id": boot_id, "started_at": time.time(),
                    })
                    print("started", flush=True)
                except Exception as error:
                    # Never leave a launched workload behind after an ordinary
                    # initialization failure. A crash still deliberately has no proof.
                    output.write(f"Job runtime initialization failed: {error}\n".encode())
                    request_stop("user_cancelled")

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
                        })
                        return 0
                    if stop_at is not None:
                        force = time.monotonic() - stop_at >= 2.0
                        for pid, ticks in _descendants(os.getpid()).items():
                            identity_key = (pid, ticks)
                            if force or identity_key not in term_sent:
                                if _signal_descendant(
                                    pid, ticks, os.getpid(), signal.SIGKILL if force else signal.SIGTERM,
                                ):
                                    term_sent.add(identity_key)
                    ready, _, _ = select.select([listener], [], [], 0.05)
                    if ready:
                        try:
                            connection, _ = listener.accept()
                        except BlockingIOError:
                            continue
                        with connection:
                            connection.settimeout(0.1)
                            try:
                                payload = bytearray()
                                while b"\n" not in payload and len(payload) <= 4096:
                                    chunk = connection.recv(4096 - len(payload) + 1)
                                    if not chunk:
                                        break
                                    payload.extend(chunk)
                                request = json.loads(payload)
                                requested_reason = request.get("reason")
                                if isinstance(requested_reason, str) and 0 < len(requested_reason) <= 100:
                                    request_stop(requested_reason)
                            except (OSError, ValueError, AttributeError):
                                pass
            finally:
                listener.close()
                (directory / "control.sock").unlink(missing_ok=True)


def stop(directory: Path, reason: str) -> int:
    deadline = time.monotonic() + 10.0
    result = _read_result(directory)
    if result is None:
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
                connection.settimeout(1.0)
                connection.connect(str(directory / "control.sock"))
                connection.sendall(json.dumps({"reason": reason}).encode() + b"\n")
        except OSError:
            result = _read_result(directory)
            if result is None:
                print(json.dumps({"state": "unknown"}), flush=True)
                return 1
        while result is None and time.monotonic() < deadline:
            time.sleep(0.05)
            result = _read_result(directory)
    if result is not None:
        print(json.dumps({"state": "finished", **result}), flush=True)
        return 0
    print(json.dumps({"state": "unknown"}), flush=True)
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    launch = commands.add_parser("run")
    launch.add_argument("--job-dir", type=Path, required=True)
    terminate = commands.add_parser("stop")
    terminate.add_argument("--job-dir", type=Path, required=True)
    terminate.add_argument("--reason", required=True)
    args = parser.parse_args()
    if args.action == "run":
        return run(args.job_dir)
    return stop(args.job_dir, args.reason)


if __name__ == "__main__":
    raise SystemExit(main())
