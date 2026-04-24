"""SSH client for orchestrator → worker communication.

Wraps the `ssh` CLI for command execution, tmux management, and port forwarding.
All operations are async (run in thread pool to not block the event loop).
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import subprocess
import tempfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated

import typer

from .models import Worker

log = logging.getLogger("ssh-client")

SSH_USER = "root"  # Workers run SSH as root by default in the container
SSH_KEY_PATH = os.environ.get("SSH_KEY_PATH", os.path.expanduser("~/.ssh/id_rsa"))

# Thread pool for blocking SSH operations
_executor = ThreadPoolExecutor(thread_name_prefix="ssh-")


@dataclass
class SSHResult:
    """Result of an SSH command execution."""
    stdout: str
    stderr: str
    returncode: int


def _ssh_base_args(worker: Worker) -> list[str]:
    return [
        "ssh",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "ConnectTimeout=10",
        "-p", str(worker.ssh_port),
        "-i", SSH_KEY_PATH,
        f"{SSH_USER}@{worker.zerotier_ip}",
    ]


def _run_ssh_sync(args: list[str], input_data: str | None = None, timeout: int = 30) -> SSHResult:
    """Run an SSH command synchronously. Use via async_ssh_run only."""
    try:
        result = subprocess.run(
            args,
            input=input_data,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return SSHResult(stdout=result.stdout, stderr=result.stderr, returncode=result.returncode)
    except subprocess.TimeoutExpired:
        return SSHResult(stdout="", stderr=f"Command timed out after {timeout}s", returncode=-1)
    except FileNotFoundError:
        return SSHResult(stdout="", stderr="ssh command not found", returncode=127)


async def async_ssh_run(
    worker: Worker,
    command: str,
    timeout: int = 30,
) -> SSHResult:
    """
    Execute a command on a worker via SSH.
    Returns (stdout, stderr, returncode).
    """
    args = _ssh_base_args(worker) + [command]
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, _run_ssh_sync, args, None, timeout)


async def async_ssh_run_pty(
    worker: Worker,
    command: str,
    timeout: int = 60,
) -> SSHResult:
    """
    Execute a command with a PTY (-tt) for interactive/TUI programs.
    Captures output but allocates a PTY so programs like tqdm work.
    """
    args = _ssh_base_args(worker) + ["-tt", command]
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, _run_ssh_sync, args, None, timeout)


async def ssh_tmux_new(
    worker: Worker,
    job_id: str,
    command: str,
    pty_enabled: bool = True,
) -> SSHResult:
    """
    Start a new tmux session for a job.
    Session name: wh_<job_id>
    Output is tee'd to /tmp/wh_<job_id>.log and EXIT code appended on finish.
    """
    session_name = f"wh_{job_id}"
    # Quote the command so it runs inside tmux correctly
    escaped_cmd = command.replace("'", "'\\''")
    tmux_cmd = (
        f"tmux new -d -s '{session_name}' "
        f"'{escaped_cmd} 2>&1 | tee /tmp/wh_{job_id}.log; "
        f"echo EXIT:$? >> /tmp/wh_{job_id}.log'"
    )
    if pty_enabled:
        args = _ssh_base_args(worker) + ["-tt", tmux_cmd]
    else:
        args = _ssh_base_args(worker) + [tmux_cmd]

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, _run_ssh_sync, args, None, 60)


async def ssh_tmux_kill(worker: Worker, job_id: str) -> SSHResult:
    session_name = f"wh_{job_id}"
    cmd = f"tmux kill-session -t '{session_name}'"
    return await async_ssh_run(worker, cmd, timeout=10)


async def ssh_tmux_running(worker: Worker, job_id: str) -> bool:
    """Check if a tmux session for a job is still running."""
    session_name = f"wh_{job_id}"
    result = await async_ssh_run(worker, f"tmux has-session -t '{session_name}' 2>&1", timeout=5)
    return result.returncode == 0


async def ssh_tmux_capture(worker: Worker, job_id: str) -> str:
    """
    Capture the current visible pane content of a tmux session.
    Used for live log streaming (like tail -f).
    """
    session_name = f"wh_{job_id}"
    cmd = f"tmux capture-pane -t '{session_name}' -p 2>/dev/null"
    result = await async_ssh_run(worker, cmd, timeout=5)
    return result.stdout


async def ssh_read_log(
    worker: Worker,
    job_id: str,
    tail: int | None = None,
    head: int | None = None,
    timeout: int = 10,
) -> str:
    """
    Read the job log file from the worker.

    Args:
        worker: target worker
        job_id: job identifier
        tail: show last N lines (None = 10 by convention, use 0 for none)
        head: show first N lines (mutually exclusive with tail)
    """
    log_path = f"/tmp/wh_{job_id}.log"

    if head is not None:
        cmd = f"head -n {head} '{log_path}' 2>/dev/null"
    elif tail is not None and tail > 0:
        cmd = f"tail -n {tail} '{log_path}' 2>/dev/null"
    elif tail == 0:
        # Just show exit status line
        cmd = f"grep -E '^EXIT:' '{log_path}' 2>/dev/null || echo 'still running'"
    else:
        # Default: tail -n 10
        cmd = f"tail -n 10 '{log_path}' 2>/dev/null"

    result = await async_ssh_run(worker, cmd, timeout=timeout)
    return result.stdout


async def ssh_get_exit_code(worker: Worker, job_id: str) -> int | None:
    """
    Read the EXIT line from the job log to get the job's exit code.
    Returns None if job is still running or file not found.
    """
    result = await async_ssh_run(
        worker,
        f"grep -E '^EXIT:' '/tmp/wh_{job_id}.log' 2>/dev/null | sed 's/EXIT://'",
        timeout=5,
    )
    if result.returncode == 0 and result.stdout.strip():
        try:
            return int(result.stdout.strip())
        except ValueError:
            pass
    return None


async def ssh_copy_file(
    worker: Worker,
    local_path: str | Path,
    remote_path: str,
    timeout: int = 60,
) -> SSHResult:
    """Copy a local file to a worker via scp."""
    loop = asyncio.get_event_loop()
    args = [
        "scp",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "ConnectTimeout=10",
        "-P", str(worker.ssh_port),
        "-i", SSH_KEY_PATH,
        str(local_path),
        f"{SSH_USER}@{worker.zerotier_ip}:{remote_path}",
    ]
    result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    return SSHResult(stdout=result.stdout, stderr=result.stderr, returncode=result.returncode)


async def ssh_port_forward(
    worker: Worker,
    local_port: int,
    remote_port: int,
) -> subprocess.Popen:
    """
    Start an SSH tunnel: localhost:local_port → worker:remote_port.
    Returns the Popen process. Caller is responsible for terminating it.
    """
    args = [
        "ssh",
        "-N",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "ServerAliveInterval=30",
        "-L", f"{local_port}:localhost:{remote_port}",
        "-p", str(worker.ssh_port),
        "-i", SSH_KEY_PATH,
        f"{SSH_USER}@{worker.zerotier_ip}",
    ]
    proc = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return proc
