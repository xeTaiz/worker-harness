#!/usr/bin/env python3
"""
Worker Daemon — runs inside each worker container.

Responsibilities:
  - Send initial registration + periodic heartbeats to the orchestrator
  - Expose a Unix socket for in-container queries (optional, future use)
  - Do nothing else. Keep it minimal.
"""

import asyncio
import getpass
import json
import logging
import os
import socket
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx

# ── Configuration from env ──────────────────────────────────────────
ORCHESTRATOR_HOST: str = os.environ.get("ORCHESTRATOR_HOST", "")
ORCHESTRATOR_PORT: int = int(os.environ.get("ORCHESTRATOR_PORT", "12888"))
HEARTBEAT_INTERVAL: int = int(os.environ.get("HEARTBEAT_INTERVAL", "60"))
WORKER_NAME: str = os.environ.get("WORKER_NAME", socket.gethostname())
WH_DIR: Path = Path(os.environ.get("WH_DIR", os.path.join(Path.home(), ".local", "worker-harness"))).expanduser()
TS_SOCKET: str = str(WH_DIR / "tailscale" / "run" / "tailscaled.sock")
HARNESS_DIR: Path = WH_DIR / "harness"
WORKER_ID_FILE: Path = WH_DIR / "worker-daemon" / "id"
WH_PROXY: str = os.environ.get("WH_PROXY", "").strip()
def _detect_ssh_user() -> str:
    for key in (
        "SSH_USER",
        "SINGULARITY_USER",
        "APPTAINER_USER",
        "SUDO_USER",
        "LOGNAME",
        "USER",
    ):
        value = os.environ.get(key, "").strip()
        if value and value != "root":
            return value

    home = os.environ.get("HOME", "").strip()
    if home.startswith("/home/"):
        candidate = home.split("/", 2)[-1].strip()
        if candidate and candidate != "root":
            return candidate

    try:
        return getpass.getuser() or "root"
    except Exception:
        return "root"


SSH_USER: str = _detect_ssh_user()

logging.basicConfig(
    level=logging.INFO,
    format="[worker-daemon] %(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("worker-daemon")


# ── System info collection ──────────────────────────────────────────

def _run(cmd: list[str], timeout: int = 5) -> str:
    try:
        return subprocess.check_output(cmd, stderr=subprocess.DEVNULL, timeout=timeout).decode()
    except Exception:
        return ""


def get_gpu_info() -> dict[str, Any]:
    """Collect GPU info via nvidia-smi. Falls back to empty if unavailable."""
    try:
        # nvidia-smi may not be on PATH; try common locations
        for cmd in ["nvidia-smi", "/usr/bin/nvidia-smi", "/usr/local/nvidia/bin/nvidia-smi"]:
            try:
                out = subprocess.check_output(
                    [cmd, "--query-gpu=index,name,memory.total,memory.used",
                     "--format=csv,noheader,nounits"],
                    stderr=subprocess.DEVNULL, timeout=5,
                ).decode()
                break
            except (FileNotFoundError, subprocess.CalledProcessError):
                continue
        else:
            log.debug("nvidia-smi not found in any known location")
            return {"count": 0, "gpus": []}

        gpus = []
        for line in out.strip().splitlines():
            if not line:
                continue
            idx, name, total_mb, used_mb = line.split(", ")
            gpus.append({
                "index": int(idx.strip()),
                "name": name.strip(),
                "vram_total_gb": round(int(total_mb.strip()) / 1024, 1),
                "vram_used_gb": round(int(used_mb.strip()) / 1024, 1),
            })
        return {"gpu_count": len(gpus), "gpus": gpus}
    except Exception as e:
        log.debug(f"nvidia-smi not available: {e}")
        return {"count": 0, "gpus": []}


def get_tailscale_identity() -> tuple[str, str]:
    """Get the Tailscale IPv4 address and MagicDNS hostname."""
    commands = [
        ["tailscale", f"--socket={TS_SOCKET}", "status", "--json"],
        ["tailscale", "status", "--json"],
    ]

    for cmd in commands:
        try:
            out = subprocess.check_output(cmd, stderr=subprocess.DEVNULL, timeout=5).decode().strip()
            if not out:
                continue
            data = json.loads(out)
            self_info = data.get("Self") or data.get("self") or {}
            ips = self_info.get("TailscaleIPs") or []
            ip = next((ip.strip() for ip in ips if "." in str(ip)), (ips[0].strip() if ips else ""))
            dns_name = (self_info.get("DNSName") or self_info.get("HostName") or "").rstrip(".").strip()
            if ip or dns_name:
                return ip, dns_name
        except Exception:
            continue
    return "", ""


def get_system_info() -> dict[str, Any]:
    """Collect CPU, RAM, disk info. Uses /proc when psutil isn't available."""
    import psutil

    mem = psutil.virtual_memory()
    disk = psutil.disk_usage("/")

    return {
        "cpu_cores": psutil.cpu_count(logical=False) or psutil.cpu_count(),
        "total_ram_gb": round(mem.total / (1024**3), 1),
        "used_ram_gb": round(mem.used / (1024**3), 1),
        "total_disk_gb": round(disk.total / (1024**3), 1),
        "used_disk_gb": round(disk.used / (1024**3), 1),
    }


def get_active_jobs() -> list[dict[str, Any]]:
    """Query tmux for active worker-harness sessions."""
    try:
        out = subprocess.check_output(
            ["tmux", "list-sessions", "-F", "#{session_name} #{session_created}"],
            stderr=subprocess.DEVNULL,
        ).decode()
        jobs = []
        for line in out.strip().splitlines():
            parts = line.split()
            if not parts:
                continue
            session_name = parts[0]
            if session_name.startswith("wh_"):
                job_id = session_name[3:]  # strip "wh_" prefix
                jobs.append({
                    "job_id": job_id,
                    "tmux_session": session_name,
                    "status": "running",
                })
        return jobs
    except Exception:
        return []


def get_data_paths() -> list[str]:
    """Return immediate shareable directories below configured bind roots.

    The host launcher writes bind destinations from ``WH_EXTRA_BINDS`` to the
    manifest.  Each destination is a collection root, not itself an advertised
    dataset: enumerate its direct, non-symlink directory children only.  This
    deliberately avoids recursive indexing, file metadata, and host paths.
    """
    manifest = WH_DIR / "data" / "bind-paths.json"
    try:
        payload = json.loads(manifest.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return []

    roots = payload.get("paths", []) if isinstance(payload, dict) else []
    shareable: set[str] = set()
    for value in roots:
        if (
            not isinstance(value, str)
            or not value.startswith("/")
            or value == "/"
            or ".." in value.split("/")
        ):
            continue
        try:
            children = Path(value.rstrip("/")).iterdir()
            for child in children:
                # Do not advertise symlinks: an advertised path must stay in
                # the configured bind tree rather than resolving elsewhere.
                if child.is_symlink() or not child.is_dir():
                    continue
                shareable.add(str(child))
        except OSError:
            # A missing/unreadable mount is simply absent from this heartbeat.
            continue
    return sorted(shareable)


def get_active_ports() -> list[dict[str, Any]]:
    """Query SSH tunnels via ps to find active port forwards."""
    try:
        out = subprocess.check_output(
            ["ps", "aux"],
            stderr=subprocess.DEVNULL,
        ).decode()
        ports = []
        for line in out.splitlines():
            if "ssh" in line and "-L " in line:
                # Parse: ssh -N -L local:remote ... worker_ip
                # We store what we know from env vars for now; a more robust
                # approach would parse the command line.
                pass
        return ports
    except Exception:
        return []


# ── Worker identity ──────────────────────────────────────────────────

def get_worker_id() -> str:
    """Load or create a stable worker ID persisted on the host volume."""
    WORKER_ID_FILE.parent.mkdir(parents=True, exist_ok=True)
    if WORKER_ID_FILE.exists():
        return WORKER_ID_FILE.read_text().strip()
    worker_id = str(uuid.uuid4())
    WORKER_ID_FILE.write_text(worker_id)
    log.info(f"Generated new worker ID: {worker_id}")
    return worker_id


# ── Heartbeat ────────────────────────────────────────────────────────

def _validate_proxy(proxy: str) -> str:
    parsed = urlparse(proxy)
    if parsed.scheme not in {"socks5", "socks5h", "http", "https"}:
        raise ValueError("WH_PROXY must use socks5/socks5h/http/https scheme")
    if not parsed.hostname:
        raise ValueError("WH_PROXY must include a host")
    if parsed.port is None:
        raise ValueError("WH_PROXY must include a port")
    return proxy


def build_http_client() -> httpx.AsyncClient:
    kwargs: dict[str, Any] = {"trust_env": False}
    if WH_PROXY:
        kwargs["proxy"] = _validate_proxy(WH_PROXY)
    return httpx.AsyncClient(**kwargs)


def build_payload(worker_id: str, tailscale_ip: str, dns_name: str) -> dict[str, Any]:
    gpu_info = get_gpu_info()
    sys_info = get_system_info()
    return {
        "worker_id": worker_id,
        "name": WORKER_NAME,
        "worker_ip": tailscale_ip,
        "dns_name": dns_name,
        "ssh_user": SSH_USER,
        "harness_dir": str(HARNESS_DIR),
        "gpu_count": gpu_info.get("gpu_count", 0),
        "gpus": gpu_info.get("gpus", []),
        "cpu_cores": sys_info.get("cpu_cores", 0),
        "total_ram_gb": sys_info.get("total_ram_gb", 0.0),
        "used_ram_gb": sys_info.get("used_ram_gb", 0.0),
        "total_disk_gb": sys_info.get("total_disk_gb", 0.0),
        "used_disk_gb": sys_info.get("used_disk_gb", 0.0),
        "active_jobs": get_active_jobs(),
        "active_ports": [],
        "data_paths": get_data_paths(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


async def send_heartbeat(worker_id: str, tailscale_ip: str, dns_name: str, client: httpx.AsyncClient) -> bool:
    payload = build_payload(worker_id, tailscale_ip, dns_name)
    url = f"http://{ORCHESTRATOR_HOST}:{ORCHESTRATOR_PORT}/register"
    try:
        resp = await client.post(url, json=payload, timeout=10.0)
        if resp.status_code in (200, 201):
            log.info(f"Heartbeat OK → orchestrator ({resp.status_code})")
            return True
        else:
            log.warning(f"Heartbeat failed: {resp.status_code} {resp.text}")
            return False
    except httpx.ConnectError:
        log.warning(f"Cannot reach orchestrator at {url}")
        return False
    except Exception as e:
        log.error(f"Heartbeat error: {e}")
        return False


# ── Main loop ────────────────────────────────────────────────────────

async def main() -> None:
    if not ORCHESTRATOR_HOST:
        log.error("ORCHESTRATOR_HOST is not set — cannot register. Exiting.")
        sys.exit(1)

    worker_id = get_worker_id()
    proxy_mode = "enabled" if WH_PROXY else "disabled"
    log.info(
        "Worker daemon starting. ID=%s, name=%s, ssh_user=%s, wh_dir=%s, proxy=%s, orchestrator=%s:%s",
        worker_id,
        WORKER_NAME,
        SSH_USER,
        WH_DIR,
        proxy_mode,
        ORCHESTRATOR_HOST,
        ORCHESTRATOR_PORT,
    )

    try:
        client = build_http_client()
    except ValueError as e:
        log.error(f"Invalid WH_PROXY: {e}")
        sys.exit(1)

    async with client:
        # Initial registration
        tailscale_ip, dns_name = get_tailscale_identity()
        log.info(f"Tailscale IP: {tailscale_ip} DNS: {dns_name or '(none)'}")
        await send_heartbeat(worker_id, tailscale_ip, dns_name, client)

        # Periodic heartbeats
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            tailscale_ip, dns_name = get_tailscale_identity()
            await send_heartbeat(worker_id, tailscale_ip, dns_name, client)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Worker daemon shutting down.")
