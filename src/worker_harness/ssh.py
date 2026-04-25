"""SSH client for orchestrator → worker communication.

Wraps the `ssh` CLI for command execution, tmux management, and port forwarding.
All operations are async (run in thread pool to not block the event loop).
"""

from __future__ import annotations

import asyncio
import base64
import logging
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

from .models import Worker

log = logging.getLogger("ssh-client")

SSH_USER = "root"
SSH_KEY_PATH = os.environ.get("SSH_KEY_PATH", os.path.expanduser("~/.ssh/id_rsa"))

_executor = ThreadPoolExecutor(thread_name_prefix="ssh-")


@dataclass
class SSHResult:
    stdout: str
    stderr: str
    returncode: int


def _ssh_base_args(worker: Worker) -> list[str]:
    return [
        "ssh",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "ConnectTimeout=10",
        "-o", "ServerAliveInterval=30",
        "-p", str(worker.ssh_port),
        "-i", SSH_KEY_PATH,
        f"{SSH_USER}@{worker.zerotier_ip}",
    ]


def _run_ssh_sync(args: list[str], input_data: str | None = None, timeout: int = 30) -> SSHResult:
    try:
        result = subprocess.run(args, input=input_data, capture_output=True,
                               text=True, timeout=timeout)
        return SSHResult(stdout=result.stdout, stderr=result.stderr, returncode=result.returncode)
    except subprocess.TimeoutExpired:
        return SSHResult(stdout="", stderr=f"Command timed out after {timeout}s", returncode=-1)
    except FileNotFoundError:
        return SSHResult(stdout="", stderr="ssh command not found", returncode=127)


async def async_ssh_run(worker: Worker, command: str, timeout: int = 30) -> SSHResult:
    args = _ssh_base_args(worker) + [command]
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, _run_ssh_sync, args, None, timeout)


async def async_ssh_run_pty(worker: Worker, command: str, timeout: int = 60) -> SSHResult:
    args = _ssh_base_args(worker) + ["-tt", command]
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, _run_ssh_sync, args, None, timeout)


def _ssh_job_sync(worker: Worker, job_id: str, command: str) -> SSHResult:
    """
    Start a job on a worker via base64-encoded script file.

    Strategy:
    1. Base64-encode the bash script (job command + EXIT marker + tmux cleanup)
    2. Single SSH call: mkdir /harness dir, write script, chmod, run in tmux
    3. tmux session stays alive for interactive inspection, auto-closes after 60s
    4. Log path: /harness/<job_id>/output.log
    """
    harness_dir = f"/harness/{job_id}"
    script_path = f"{harness_dir}/script.sh"
    log_path = f"{harness_dir}/output.log"

    # Build script: redirect all output to log, wrap command in subshell
    # (so 'exit N' doesn't terminate the script), capture exit code, cleanup tmux
    script_content = (
        f"#!/bin/bash\n"
        f"exec >>{log_path} 2>&1\n"
        f"({command}); ec=$?\n"
        f"echo EXIT:$ec\n"
        f"sleep 60\n"
        f"tmux kill-session -t wh_{job_id} 2>/dev/null\n"
    )
    script_b64 = base64.b64encode(script_content.encode()).decode().rstrip()

    # Single SSH command: write and run script via base64
    full_cmd = (
        f"mkdir -p {harness_dir} && "
        f"echo '{script_b64}' | base64 -d > {script_path} && "
        f"chmod +x {script_path} && "
        f"tmux new-session -d -s wh_{job_id} 'bash {script_path}' && "
        f"echo 'started'"
    )
    args = _ssh_base_args(worker) + [full_cmd]
    return _run_ssh_sync(args, None, 30)


async def ssh_tmux_new(worker: Worker, job_id: str, command: str, pty_enabled: bool = True) -> SSHResult:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, _ssh_job_sync, worker, job_id, command)


async def ssh_tmux_kill(worker: Worker, job_id: str) -> SSHResult:
    session = f"wh_{job_id}"
    cmd = (
        f"tmux kill-session -t '{session}' 2>/dev/null; "
        f"pkill -f '/harness/{job_id}/script.sh' 2>/dev/null; "
        f"echo done"
    )
    return await async_ssh_run(worker, cmd, timeout=10)


async def ssh_tmux_running(worker: Worker, job_id: str) -> bool:
    """Check if a job is still running by looking for the EXIT marker in the log."""
    log_path = f"/harness/{job_id}/output.log"
    result = await async_ssh_run(
        worker,
        f"grep -q '^EXIT:' '{log_path}' 2>/dev/null && echo 'done' || echo 'running'",
        timeout=5,
    )
    return result.stdout.strip() == "running"


async def ssh_tmux_capture(worker: Worker, job_id: str) -> str:
    session = f"wh_{job_id}"
    cmd = f"tmux capture-pane -t '{session}' -p 2>/dev/null"
    result = await async_ssh_run(worker, cmd, timeout=5)
    return result.stdout


async def ssh_read_log(
    worker: Worker,
    job_id: str,
    tail: int | None = None,
    head: int | None = None,
    timeout: int = 10,
) -> str:
    log_path = f"/harness/{job_id}/output.log"

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
        f"grep -E '^EXIT:' '/harness/{job_id}/output.log' 2>/dev/null | sed 's/EXIT://'",
        timeout=5,
    )
    if result.returncode == 0 and result.stdout.strip():
        try:
            return int(result.stdout.strip())
        except ValueError:
            pass
    return None


async def ssh_copy_file(worker: Worker, local_path: str | Path, remote_path: str, timeout: int = 60) -> SSHResult:
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


async def ssh_port_forward(worker: Worker, local_port: int, remote_port: int) -> subprocess.Popen:
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
