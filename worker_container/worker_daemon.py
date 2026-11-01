#!/usr/bin/env python3
"""
Worker Daemon — runs inside each worker container.

Responsibilities:
  - Send initial registration + periodic heartbeats to the orchestrator
  - Keep job, GPU, and data responsibilities available to the fleet
"""

import asyncio
import getpass
import json
import logging
import os
import signal
import socket
import subprocess
import sys
import time
import uuid
import xml.etree.ElementTree as ET
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
JOB_TMUX_DIR: Path = HARNESS_DIR / "job-tmux"
WORKER_ID_FILE: Path = WH_DIR / "worker-daemon" / "id"
WH_PROXY: str = os.environ.get("WH_PROXY", "").strip()
DAEMON_INSTANCE_ID: str = str(uuid.uuid4())
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


GPU_PROCESS_IGNORE_MB = 512
GPU_MOSTLY_FREE_FRACTION = 0.90
IGNORED_GPU_PROCESS_NAMES = {
    "xorg",
    "xwayland",
    "gnome-shell",
    "kwin_wayland",
}


def _required_text(node: ET.Element, path: str) -> str:
    value = (node.findtext(path) or "").strip()
    if not value or value.lower() in {
        "n/a", "[not supported]", "not supported", "unknown",
        "insufficient permissions", "[insufficient permissions]", "permission denied",
    }:
        raise ValueError(f"Missing or unsupported GPU field: {path}")
    return value


def _memory_bytes(node: ET.Element, path: str) -> int:
    value = _required_text(node, path)
    amount, unit = value.split()
    scale = {"KiB": 1024, "MiB": 1024**2, "GiB": 1024**3}[unit]
    number = int(amount)
    if number < 0:
        raise ValueError("Negative GPU memory")
    return number * scale


def _parse_gpu(node: ET.Element) -> dict[str, Any]:
    """Keep availability thresholds separate from fail-closed idle telemetry."""
    gpu: dict[str, Any] = {
        "index": 0, "uuid": "", "name": "", "vram_total_gb": 0.0,
        "vram_used_gb": 0.0, "busy": None, "utilization_percent": None,
        "memory_used_bytes": None, "process_count": None,
        "workload_process_count": None, "workload_memory_bytes": None,
        "display_only": False, "activity_valid": False,
    }
    try:
        index_path = "index" if node.find("index") is not None else "minor_number"
        gpu["index"] = int(_required_text(node, index_path))
        if gpu["index"] < 0:
            raise ValueError("Negative GPU index")
        gpu["uuid"] = _required_text(node, "uuid")
        gpu["name"] = _required_text(node, "product_name")
        total_bytes = _memory_bytes(node, "fb_memory_usage/total")
        gpu["vram_total_gb"] = round(total_bytes / 1024**3, 1)
        used_bytes = _memory_bytes(node, "fb_memory_usage/used")
        gpu["memory_used_bytes"] = used_bytes
        gpu["vram_used_gb"] = round(used_bytes / 1024**3, 1)

        # These are the existing availability rules, including the 512 MiB
        # exception. They do NOT determine workload activity for idle stops.
        total_mb, used_mb = total_bytes // 1024**2, used_bytes // 1024**2
        mostly_free_limit_mb = max(
            GPU_PROCESS_IGNORE_MB,
            round(total_mb * (1.0 - GPU_MOSTLY_FREE_FRACTION)),
        )
        gpu["busy"] = used_mb > mostly_free_limit_mb
        utilization = _required_text(node, "utilization/gpu_util").split()
        if len(utilization) != 2 or utilization[1] != "%":
            raise ValueError("Invalid GPU utilization")
        percent = float(utilization[0])
        if not 0 <= percent <= 100:
            raise ValueError("Invalid GPU utilization")
        gpu["utilization_percent"] = percent

        # MIG-capable GPUs report Disabled/Enabled; GPUs without MIG support
        # (e.g. GeForce) report N/A, which is equally whole-GPU telemetry.
        mig = node.find("mig_mode")
        if mig is not None and (mig.findtext("current_mig") or "").strip() not in {"Disabled", "N/A"}:
            raise ValueError("MIG telemetry is not complete physical-GPU telemetry")
        processes = node.find("processes")
        if processes is None or (processes.text or "").strip():
            raise ValueError("Missing or unavailable GPU process inventory")
        ignored_mb = 0
        significant = False
        workload_count = 0
        workload_bytes = 0
        entries = list(processes)
        for process in entries:
            if process.tag != "process_info":
                raise ValueError("Unrecognized GPU process inventory")
            if int(_required_text(process, "pid")) <= 0:
                raise ValueError("Invalid GPU process PID")
            executable = Path(_required_text(process, "process_name")).name.lower()
            process_type = _required_text(process, "type")
            memory = _memory_bytes(process, "used_memory")
            memory_mb = memory // 1024**2
            if memory_mb < GPU_PROCESS_IGNORE_MB or executable in IGNORED_GPU_PROCESS_NAMES:
                ignored_mb += memory_mb
            else:
                significant = True
            display = process_type == "G" and executable in IGNORED_GPU_PROCESS_NAMES
            if not display:
                workload_count += 1
                workload_bytes += memory
        gpu["busy"] = significant or max(0, used_mb - ignored_mb) > mostly_free_limit_mb
        gpu["process_count"] = len(entries)
        gpu["workload_process_count"] = workload_count
        gpu["workload_memory_bytes"] = workload_bytes
        gpu["display_only"] = bool(entries) and workload_count == 0
        gpu["activity_valid"] = True
    except (ValueError, KeyError, OverflowError):
        # Preserve any known capacity fields, but never turn unknown activity
        # into a successful empty inventory.
        pass
    return gpu


def get_gpu_info() -> dict[str, Any]:
    """Collect complete GPU/process XML in one coherent nvidia-smi sample."""
    for command in ("nvidia-smi", "/usr/bin/nvidia-smi", "/usr/local/nvidia/bin/nvidia-smi"):
        try:
            out = subprocess.check_output(
                [command, "-q", "-x"], stderr=subprocess.DEVNULL, timeout=5,
            )
            root = ET.fromstring(out)
            if root.tag != "nvidia_smi_log":
                raise ValueError("Unexpected nvidia-smi XML")
            gpus = [_parse_gpu(node) for node in root.findall("gpu")]
            # Duplicate devices or a truncated inventory cannot establish a
            # complete assignment, even when the XML itself is well-formed.
            if (
                len({gpu["index"] for gpu in gpus}) != len(gpus)
                or len({gpu["uuid"] for gpu in gpus}) != len(gpus)
                or int(_required_text(root, "attached_gpus")) != len(gpus)
            ):
                for gpu in gpus:
                    gpu["activity_valid"] = False
                    gpu["display_only"] = False
            return {"gpu_count": len(gpus), "gpus": gpus}
        except (OSError, subprocess.SubprocessError, ET.ParseError, ValueError) as exc:
            log.debug("GPU telemetry unavailable via %s: %s", command, exc)
    return {"gpu_count": 0, "gpus": []}


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
    """Query job-plane tmux sessions."""
    try:
        env = os.environ.copy()
        env["TMUX_TMPDIR"] = str(JOB_TMUX_DIR)
        out = subprocess.check_output(
            ["tmux", "list-sessions", "-F", "#{session_name} #{session_created}"],
            stderr=subprocess.DEVNULL,
            env=env,
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

    The host launcher writes effective container bind destinations to the
    manifest. Each destination is a collection root, not itself an advertised
    dataset: enumerate its direct, non-symlink directory children only. This
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


def build_payload(
    worker_id: str,
    tailscale_ip: str,
    dns_name: str,
) -> dict[str, Any]:
    gpu_info = get_gpu_info()
    sample_ns = time.monotonic_ns()
    try:
        boot_id = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    except OSError:
        boot_id = ""
    sys_info = get_system_info()
    return {
        "worker_id": worker_id, "name": WORKER_NAME, "worker_ip": tailscale_ip,
        "dns_name": dns_name, "ssh_user": SSH_USER, "harness_dir": str(HARNESS_DIR),
        "gpu_count": gpu_info.get("gpu_count", 0), "gpus": gpu_info.get("gpus", []),
        "gpu_sample_id": str(uuid.uuid4()), "gpu_sample_monotonic_ns": sample_ns,
        "worker_boot_id": boot_id, "daemon_instance_id": DAEMON_INSTANCE_ID,
        "cpu_cores": sys_info.get("cpu_cores", 0), "total_ram_gb": sys_info.get("total_ram_gb", 0.0),
        "used_ram_gb": sys_info.get("used_ram_gb", 0.0), "total_disk_gb": sys_info.get("total_disk_gb", 0.0),
        "used_disk_gb": sys_info.get("used_disk_gb", 0.0), "active_jobs": get_active_jobs(),
        "active_ports": [], "data_paths": get_data_paths(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


async def send_heartbeat(
    worker_id: str,
    tailscale_ip: str,
    dns_name: str,
    client: httpx.AsyncClient,
) -> bool:
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
        worker_id, WORKER_NAME, SSH_USER, WH_DIR, proxy_mode, ORCHESTRATOR_HOST, ORCHESTRATOR_PORT,
    )

    try:
        client = build_http_client()
    except ValueError as e:
        log.error(f"Invalid WH_PROXY: {e}")
        sys.exit(1)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signal_number in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signal_number, stop_event.set)
        except (NotImplementedError, RuntimeError):
            # The worker runs on Linux, but retain normal asyncio portability.
            signal.signal(signal_number, lambda *_: stop_event.set())

    async with client:
        # Initial registration
        tailscale_ip, dns_name = get_tailscale_identity()
        log.info(f"Tailscale IP: {tailscale_ip} DNS: {dns_name or '(none)'}")
        await send_heartbeat(
            worker_id,
            tailscale_ip,
            dns_name,
            client,
        )

        # Periodic heartbeats. Signal handlers wake this wait so the
        # finally block removes the persistent Serve rule immediately.
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=HEARTBEAT_INTERVAL)
            except TimeoutError:
                pass
            if stop_event.is_set():
                break
            tailscale_ip, dns_name = get_tailscale_identity()
            await send_heartbeat(
                worker_id,
                tailscale_ip,
                dns_name,
                client,
            )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Worker daemon shutting down.")
