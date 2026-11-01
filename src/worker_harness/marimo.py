"""Marimo service primitives for worker-harness.

A marimo session is a token-protected worker-local process plus a loopback
SSH forward, exposed only through the owner-authorized harness proxy.
"""

from __future__ import annotations

import asyncio
import os
import re
import shlex
import socket
import subprocess
from collections.abc import Collection
from ipaddress import ip_address, ip_network
from pathlib import Path, PurePosixPath

import httpx

from .models import Worker
from .ssh import SSHResult, async_ssh_run, ssh_upload_bytes, tailscale_cli

MARIMO_LAUNCHER = "/usr/local/lib/worker-harness/wh-marimo-launch"


def validate_absolute_path(value: str, label: str) -> str:
    if not value or "\n" in value or "\r" in value or "\x00" in value:
        raise ValueError(f"{label} must be a non-empty single-line absolute path")
    path = PurePosixPath(value)
    if not path.is_absolute() or ".." in path.parts:
        raise ValueError(f"{label} must be an absolute normalized path")
    return str(path)


def build_launch_command(
    *, notebook_path: str, environment: str, port: int, token_file: str, base_url: str
) -> str:
    notebook = validate_absolute_path(notebook_path, "notebook_path")
    env = validate_absolute_path(environment, "environment")
    token_path = validate_absolute_path(token_file, "token_file")
    if token_path != token_file:
        raise ValueError("token_file must be an absolute normalized path")
    _validate_base_path(base_url)
    if not 1 <= port <= 65535:
        raise ValueError("port must be between 1 and 65535")
    return " ".join(
        shlex.quote(part)
        for part in (
            MARIMO_LAUNCHER,
            "--notebook",
            notebook,
            "--environment",
            env,
            "--port",
            str(port),
            "--token-file",
            token_path,
            "--base-url",
            base_url,
        )
    )


async def allocate_worker_port(worker: Worker) -> int:
    command = (
        "python3 -c "
        + shlex.quote(
            "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); "
            "print(s.getsockname()[1]); s.close()"
        )
    )
    result = await async_ssh_run(worker, command, timeout=10)
    if result.returncode != 0:
        raise RuntimeError(f"failed to allocate worker port: {result.stderr.strip()}")
    try:
        port = int(result.stdout.strip().splitlines()[-1])
    except (ValueError, IndexError) as exc:
        raise RuntimeError("worker returned an invalid port") from exc
    if not 1 <= port <= 65535:
        raise RuntimeError("worker returned an out-of-range port")
    return port


def tailnet_bind_host() -> str:
    configured = os.environ.get("WH_TAILNET_BIND_HOST", "").strip()
    if configured:
        try:
            socket.inet_aton(configured)
        except OSError as exc:
            raise RuntimeError("WH_TAILNET_BIND_HOST must be an IPv4 address") from exc
        if ip_address(configured) not in ip_network("100.64.0.0/10"):
            raise RuntimeError("WH_TAILNET_BIND_HOST must be a Tailnet IPv4 address")
        return configured

    try:
        output = subprocess.check_output(
            tailscale_cli("ip", "-4"), text=True, stderr=subprocess.DEVNULL, timeout=5
        )
    except (FileNotFoundError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError("cannot determine orchestrator Tailnet IPv4 address") from exc
    host = next((line.strip() for line in output.splitlines() if line.strip()), "")
    try:
        socket.inet_aton(host)
    except OSError as exc:
        raise RuntimeError("tailscale returned an invalid IPv4 address") from exc
    if ip_address(host) not in ip_network("100.64.0.0/10"):
        raise RuntimeError("tailscale did not return a Tailnet IPv4 address")
    return host


def allocate_local_port(bind_host: str, *, reserved: Collection[int] = ()) -> int:
    """Reserve a free port from the ACL-scoped marimo tunnel range.

    ``reserved`` are listener ports already recorded in the database (for
    example a dead forward still owned by a notebook record); they are
    skipped even when nothing currently listens on them.
    """
    first = int(os.environ.get("WH_MARIMO_PORT_MIN", "18000"))
    last = int(os.environ.get("WH_MARIMO_PORT_MAX", "18999"))
    if not (1 <= first <= last <= 65535):
        raise RuntimeError("invalid WH_MARIMO_PORT_MIN/WH_MARIMO_PORT_MAX range")
    for port in range(first, last + 1):
        if port in reserved:
            continue
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.bind((bind_host, port))
            except OSError:
                continue
            return port
    raise RuntimeError("no free marimo tunnel port")


async def wait_until_ready(
    bind_host: str, port: int, timeout: float = 30.0, *, base_url: str, token: str
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    _validate_base_path(base_url)
    # Marimo 0.23.16 serves /health without authentication; /api/status
    # requires the native token, so readiness also proves token enforcement.
    url = f"http://{bind_host}:{port}{base_url}/api/status"
    async with httpx.AsyncClient(
        trust_env=False, timeout=1.5, auth=httpx.BasicAuth("wh", token)
    ) as client:
        while asyncio.get_running_loop().time() < deadline:
            try:
                response = await client.get(url)
                if response.status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            await asyncio.sleep(0.25)
    raise TimeoutError(f"marimo did not become ready within {timeout:g}s")


def _validate_base_path(base_url: str) -> None:
    if not re.fullmatch(r"/api/v1/marimo/[A-Za-z0-9-]+/proxy", base_url):
        raise ValueError("invalid Marimo proxy base path")


def _session_id(session_id: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9-]+", session_id):
        raise ValueError("invalid Marimo session ID")
    return session_id


def proxy_base_path(session_id: str) -> str:
    return f"/api/v1/marimo/{_session_id(session_id)}/proxy"


def local_token_path(db_path: Path, session_id: str) -> Path:
    return db_path.parent / "marimo-secrets" / _session_id(session_id) / "native.token"


def remote_token_path(worker: Worker, session_id: str) -> str:
    root = validate_absolute_path(worker.harness_dir, "harness_dir")
    return str(PurePosixPath(root) / ".marimo" / _session_id(session_id) / "native.token")


def write_local_token(path: Path, token: str) -> None:
    # Do not change permissions on the database directory itself.
    for directory in (path.parent.parent, path.parent):
        directory.mkdir(mode=0o700, exist_ok=True)
        if directory.is_symlink() or not directory.is_dir():
            raise ValueError("token directory must not be a symlink")
        directory.chmod(0o700)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as output:
        os.fchmod(output.fileno(), 0o600)
        output.write(token.encode("utf-8"))


def delete_local_token(path: Path) -> None:
    path.unlink(missing_ok=True)
    try:
        path.parent.rmdir()
    except OSError:
        pass


async def upload_remote_token(worker: Worker, session_id: str, token: str) -> SSHResult:
    path = remote_token_path(worker, session_id)
    parent = str(PurePosixPath(path).parent)
    root = str(PurePosixPath(parent).parent)
    # Precreate with noclobber, before the upload helper opens the file for
    # writing. Secret bytes travel only through SSH stdin, never the command.
    command = (
        f"umask 077; mkdir -p -- {shlex.quote(root)} && "
        f"test ! -L {shlex.quote(root)} && chmod 700 -- {shlex.quote(root)} && "
        f"mkdir -m 700 -- {shlex.quote(parent)} && "
        f"(set -C; : > {shlex.quote(path)}) && chmod 600 -- {shlex.quote(path)}"
    )
    result = await async_ssh_run(worker, command, timeout=10)
    if result.returncode != 0:
        return result
    return await ssh_upload_bytes(worker, token.encode("utf-8"), path)


async def delete_remote_token(worker: Worker, session_id: str) -> SSHResult:
    path = remote_token_path(worker, session_id)
    parent = str(PurePosixPath(path).parent)
    return await async_ssh_run(
        worker, f"rm -f -- {shlex.quote(path)} && rmdir -- {shlex.quote(parent)}",
        timeout=10,
    )
