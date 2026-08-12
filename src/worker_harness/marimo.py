"""Marimo service primitives for worker-harness.

A marimo session is a worker-local process bound to loopback plus an
orchestrator-side SSH forward bound only to the orchestrator's Tailnet IP.
"""

from __future__ import annotations

import asyncio
import os
import shlex
import socket
import subprocess
from ipaddress import ip_address, ip_network
from pathlib import PurePosixPath

import httpx

from .models import Worker
from .ssh import async_ssh_run

MARIMO_LAUNCHER = "/usr/local/lib/worker-harness/wh-marimo-launch"


def validate_absolute_path(value: str, label: str) -> str:
    if not value or "\n" in value or "\r" in value or "\x00" in value:
        raise ValueError(f"{label} must be a non-empty single-line absolute path")
    path = PurePosixPath(value)
    if not path.is_absolute() or ".." in path.parts:
        raise ValueError(f"{label} must be an absolute normalized path")
    return str(path)


def build_launch_command(*, notebook_path: str, environment: str, port: int) -> str:
    notebook = validate_absolute_path(notebook_path, "notebook_path")
    env = validate_absolute_path(environment, "environment")
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
            ["tailscale", "ip", "-4"], text=True, stderr=subprocess.DEVNULL, timeout=5
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


def allocate_local_port(bind_host: str) -> int:
    """Reserve a free port from the ACL-scoped marimo tunnel range."""
    first = int(os.environ.get("WH_MARIMO_PORT_MIN", "18000"))
    last = int(os.environ.get("WH_MARIMO_PORT_MAX", "18999"))
    if not (1 <= first <= last <= 65535):
        raise RuntimeError("invalid WH_MARIMO_PORT_MIN/WH_MARIMO_PORT_MAX range")
    for port in range(first, last + 1):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.bind((bind_host, port))
            except OSError:
                continue
            return port
    raise RuntimeError("no free marimo tunnel port")


async def wait_until_ready(bind_host: str, port: int, timeout: float = 30.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    url = f"http://{bind_host}:{port}/health"
    async with httpx.AsyncClient(trust_env=False, timeout=1.5) as client:
        while asyncio.get_running_loop().time() < deadline:
            try:
                response = await client.get(url)
                if response.status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            await asyncio.sleep(0.25)
    raise TimeoutError(f"marimo did not become ready within {timeout:g}s")
