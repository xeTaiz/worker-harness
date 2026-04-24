#!/usr/bin/env python3
"""
Worker Daemon — runs inside each worker container.

Responsibilities:
  - Send initial registration + periodic heartbeats to the orchestrator
  - Expose a Unix socket for in-container queries (optional, future use)
  - Do nothing else. Keep it minimal.
"""

import asyncio
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

import httpx

# ── Configuration from env ──────────────────────────────────────────
ORCHESTRATOR_HOST: str = os.environ.get("ORCHESTRATOR_HOST", "")
ORCHESTRATOR_PORT: int = int(os.environ.get("ORCHESTRATOR_PORT", "12888"))
HEARTBEAT_INTERVAL: int = int(os.environ.get("HEARTBEAT_INTERVAL", "60"))
WORKER_NAME: str = os.environ.get("WORKER_NAME", socket.gethostname())
WORKER_SSH_PORT: int = int(os.environ.get("WORKER_SSH_PORT", "22"))
WORKER_ID_FILE: Path = Path("/run/worker-daemon/id")

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


def get_zerotier_ip() -> str:
    """Get the ZeroTier IPv4 address (prefer) or IPv6 from zerotier-cli -j."""
    try:
        out = subprocess.check_output(
            ["zerotier-cli", "-j", "listnetworks"], stderr=subprocess.DEVNULL, timeout=5
        ).decode()
        networks = json.loads(out)
        for net in networks:
            for addr in net.get("assignedAddresses", []):
                # Prefer IPv4 (no colon), fall back to IPv6
                if ":" not in addr:
                    return addr.split("/")[0]
        return ""
    except Exception:
        return ""


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

def build_payload(worker_id: str, zerotier_ip: str, ssh_port: int) -> dict[str, Any]:
    gpu_info = get_gpu_info()
    sys_info = get_system_info()
    return {
        "worker_id": worker_id,
        "name": WORKER_NAME,
        "zerotier_ip": zerotier_ip,
        "ssh_port": ssh_port,
        "gpu_count": gpu_info.get("gpu_count", 0),
        "gpus": gpu_info.get("gpus", []),
        "cpu_cores": sys_info.get("cpu_cores", 0),
        "total_ram_gb": sys_info.get("total_ram_gb", 0.0),
        "used_ram_gb": sys_info.get("used_ram_gb", 0.0),
        "total_disk_gb": sys_info.get("total_disk_gb", 0.0),
        "used_disk_gb": sys_info.get("used_disk_gb", 0.0),
        "active_jobs": get_active_jobs(),
        "active_ports": [],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


async def send_heartbeat(worker_id: str, zerotier_ip: str, ssh_port: int, client: httpx.AsyncClient) -> bool:
    payload = build_payload(worker_id, zerotier_ip, ssh_port)
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
    log.info(f"Worker daemon starting. ID={worker_id}, name={WORKER_NAME}")

    async with httpx.AsyncClient() as client:
        # Initial registration
        zerotier_ip = get_zerotier_ip()
        log.info(f"ZeroTier IP: {zerotier_ip}")
        await send_heartbeat(worker_id, zerotier_ip, WORKER_SSH_PORT, client)

        # Periodic heartbeats
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            zerotier_ip = get_zerotier_ip()
            await send_heartbeat(worker_id, zerotier_ip, WORKER_SSH_PORT, client)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Worker daemon shutting down.")
