#!/usr/bin/env python3
"""
Worker Daemon — runs inside each worker container.

Responsibilities:
  - Send initial registration + periodic heartbeats to the orchestrator
  - Run the loopback-only Pi-session relay and publish it through Tailscale
    Serve for trusted non-worker Tailnet members
  - Keep Pi process launch/PTY ownership out of this first transport slice
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
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx

# worker_daemon.py and pi_relay.py are copied together to / in the worker
# image.  This also makes path-based test imports resolve the sibling module.
MODULE_DIR = Path(__file__).resolve().parent
if str(MODULE_DIR) not in sys.path:
    sys.path.insert(0, str(MODULE_DIR))
from pi_job_server import PiJobServer, PiJobService
from pi_relay import PROTOCOL_VERSION, RelayServer

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
PI_RELAY_PORT: int = int(os.environ.get("WH_PI_RELAY_PORT", "27888"))
# Private child job/state service. This must remain a Unix-domain socket under
# the worker's writable harness bind; Apptainer/userspace Tailscale can expose
# host loopback TCP listeners to other Tailnet peers.
PI_JOB_SOCKET: Path = Path(
    os.environ.get("WH_PI_JOB_SOCKET") or (HARNESS_DIR / "pi-job" / "socket")
).expanduser()
PI_SESSIONS_DIR: Path = WH_DIR / "pi" / "sessions"
PI_AGENT_CONFIG_DIR: Path = WH_DIR / "pi" / "current" / "agent-config"
# Releases provide this path atomically. Operators may override it for a
# canary/runtime migration without changing worker daemon code.
# Empty-string env values must fall back to the default: start-wh.sh passes
# the variable through even when unset on the host.
PI_COMMAND: str = os.environ.get("WH_PI_COMMAND") or str(WH_DIR / "pi" / "current" / "bin" / "pi-worker")
# Optional orchestrator ingest target. The worker relays state transitions
# here so the durable projection stays truthful. Workers do not authenticate;
# Tailnet membership is the trust boundary (spec §7.2).
PI_INGEST_BASE_URL: str = os.environ.get("WH_PI_INGEST_BASE_URL", "").strip() or (
    f"http://{ORCHESTRATOR_HOST}:{ORCHESTRATOR_PORT}" if ORCHESTRATOR_HOST else ""
)
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


def _gpu_process_usage(command: str) -> tuple[set[str], dict[str, int]]:
    """Return GPU UUIDs with significant compute and ignored memory by UUID."""
    try:
        out = subprocess.check_output(
            [
                command,
                "--query-compute-apps=gpu_uuid,process_name,used_gpu_memory",
                "--format=csv,noheader,nounits",
            ],
            stderr=subprocess.DEVNULL,
            timeout=5,
        ).decode()
    except (FileNotFoundError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return set(), {}

    significant: set[str] = set()
    ignored_memory: dict[str, int] = {}
    for line in out.strip().splitlines():
        if not line:
            continue
        parts = [part.strip() for part in line.split(",", 2)]
        if len(parts) != 3:
            continue
        gpu_uuid, process_name, used_mb_text = parts
        try:
            used_mb = int(used_mb_text)
        except ValueError:
            continue
        executable = Path(process_name).name.lower()
        if used_mb < GPU_PROCESS_IGNORE_MB or executable in IGNORED_GPU_PROCESS_NAMES:
            ignored_memory[gpu_uuid] = ignored_memory.get(gpu_uuid, 0) + used_mb
        else:
            significant.add(gpu_uuid)
    return significant, ignored_memory


def get_gpu_info() -> dict[str, Any]:
    """Collect GPU capacity and workload state via nvidia-smi."""
    try:
        # nvidia-smi may not be on PATH; try common locations.
        for command in ("nvidia-smi", "/usr/bin/nvidia-smi", "/usr/local/nvidia/bin/nvidia-smi"):
            try:
                out = subprocess.check_output(
                    [
                        command,
                        "--query-gpu=index,uuid,name,memory.total,memory.used",
                        "--format=csv,noheader,nounits",
                    ],
                    stderr=subprocess.DEVNULL,
                    timeout=5,
                ).decode()
                break
            except (FileNotFoundError, subprocess.CalledProcessError):
                continue
        else:
            log.debug("nvidia-smi not found in any known location")
            return {"gpu_count": 0, "gpus": []}

        significant_processes, ignored_memory = _gpu_process_usage(command)
        gpus = []
        for line in out.strip().splitlines():
            if not line:
                continue
            idx, gpu_uuid, name, total_mb_text, used_mb_text = line.split(", ", 4)
            total_mb = int(total_mb_text.strip())
            used_mb = int(used_mb_text.strip())
            relevant_used_mb = max(0, used_mb - ignored_memory.get(gpu_uuid.strip(), 0))
            mostly_free_limit_mb = max(
                GPU_PROCESS_IGNORE_MB,
                round(total_mb * (1.0 - GPU_MOSTLY_FREE_FRACTION)),
            )
            gpus.append({
                "index": int(idx.strip()),
                "name": name.strip(),
                "vram_total_gb": round(total_mb / 1024, 1),
                "vram_used_gb": round(used_mb / 1024, 1),
                "busy": (
                    gpu_uuid.strip() in significant_processes
                    or relevant_used_mb > mostly_free_limit_mb
                ),
            })
        return {"gpu_count": len(gpus), "gpus": gpus}
    except Exception as e:
        log.debug(f"nvidia-smi not available: {e}")
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
    """Query job-plane tmux only; Pi relay sessions use a separate socket."""
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
            # wh_pi_* belongs to the separate delegated-Pi terminal relay,
            # not the observable bare-job plane.
            if session_name.startswith("wh_") and not session_name.startswith("wh_pi_"):
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


def publish_pi_relay(port: int) -> bool:
    """Publish the loopback relay without changing any other Serve rule."""
    command = [
        "tailscale",
        f"--socket={TS_SOCKET}",
        "serve",
        "--bg",
        "--yes",
        f"--tcp={port}",
        f"tcp://127.0.0.1:{port}",
    ]
    try:
        result = subprocess.run(command, text=True, capture_output=True, timeout=10)
    except (OSError, subprocess.SubprocessError) as exc:
        log.error("Pi relay publication failed: %s", exc)
        return False
    if result.returncode != 0:
        log.error("Pi relay publication failed: %s", result.stderr.strip())
        return False
    log.info("Pi relay published on Tailnet TCP port %s", port)
    return True


def is_pi_relay_published(port: int) -> bool:
    """Check the exact Serve target without disturbing unrelated rules."""
    command = ["tailscale", f"--socket={TS_SOCKET}", "serve", "status"]
    try:
        result = subprocess.run(command, text=True, capture_output=True, timeout=10)
    except (OSError, subprocess.SubprocessError) as exc:
        log.warning("Pi relay Serve status check failed: %s", exc)
        return False
    return result.returncode == 0 and f"tcp://127.0.0.1:{port}" in result.stdout


def unpublish_pi_relay(port: int) -> None:
    """Remove only this relay's TCP Serve rule, never global Serve state."""
    command = [
        "tailscale",
        f"--socket={TS_SOCKET}",
        "serve",
        f"--tcp={port}",
        "off",
    ]
    try:
        result = subprocess.run(command, text=True, capture_output=True, timeout=10)
    except (OSError, subprocess.SubprocessError) as exc:
        log.warning("Pi relay unpublish failed: %s", exc)
        return
    if result.returncode != 0:
        log.warning("Pi relay unpublish failed: %s", result.stderr.strip())
    else:
        log.info("Pi relay unpublished from Tailnet TCP port %s", port)


def reconcile_pi_relay(relay: RelayServer) -> bool:
    """Return an accurate advertised capability and repair a lost Serve rule."""
    if not relay.is_running:
        log.error("Pi relay task is no longer running; withholding direct-attach capability")
        return False
    if is_pi_relay_published(PI_RELAY_PORT):
        return True
    log.warning("Pi relay Serve rule is missing; attempting republish")
    return publish_pi_relay(PI_RELAY_PORT)


def build_payload(
    worker_id: str,
    tailscale_ip: str,
    dns_name: str,
    *,
    pi_relay_available: bool = False,
) -> dict[str, Any]:
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
        "pi_relay_port": PI_RELAY_PORT,
        "pi_relay_available": pi_relay_available,
        "pi_relay_protocol_version": PROTOCOL_VERSION,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


async def send_heartbeat(
    worker_id: str,
    tailscale_ip: str,
    dns_name: str,
    client: httpx.AsyncClient,
    *,
    pi_relay_available: bool = False,
) -> bool:
    payload = build_payload(
        worker_id,
        tailscale_ip,
        dns_name,
        pi_relay_available=pi_relay_available,
    )
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
    if not 1 <= PI_RELAY_PORT <= 65535:
        log.error("WH_PI_RELAY_PORT must be in range 1..65535; got %s", PI_RELAY_PORT)
        sys.exit(1)
    try:
        pi_job_socket = PI_JOB_SOCKET.resolve()
        relative_socket = pi_job_socket.relative_to(HARNESS_DIR.resolve())
        if len(relative_socket.parts) < 2:
            raise ValueError("socket must be inside a private harness subdirectory")
        if len(os.fsencode(pi_job_socket)) >= 100:
            raise ValueError("path is too long")
    except ValueError as exc:
        log.error("WH_PI_JOB_SOCKET must be a short path beneath %s: %s", HARNESS_DIR, exc)
        sys.exit(1)

    worker_id = get_worker_id()
    proxy_mode = "enabled" if WH_PROXY else "disabled"
    log.info(
        "Worker daemon starting. ID=%s, name=%s, ssh_user=%s, wh_dir=%s, proxy=%s, orchestrator=%s:%s, pi_relay_port=%s, pi_job_socket=%s",
        worker_id,
        WORKER_NAME,
        SSH_USER,
        WH_DIR,
        proxy_mode,
        ORCHESTRATOR_HOST,
        ORCHESTRATOR_PORT,
        PI_RELAY_PORT,
        pi_job_socket,
    )

    try:
        client = build_http_client()
    except ValueError as e:
        log.error(f"Invalid WH_PROXY: {e}")
        sys.exit(1)

    relay = RelayServer(
        PI_RELAY_PORT,
        sessions_root=PI_SESSIONS_DIR,
        pi_command=PI_COMMAND,
        default_cwd=Path.home(),
        tmux_tmpdir=HARNESS_DIR / "pi-tmux",
        agent_config=PI_AGENT_CONFIG_DIR,
        orchestrator_url=PI_INGEST_BASE_URL or None,
        worker_id=worker_id,
        proxy=WH_PROXY or None,
        job_socket=str(pi_job_socket),
    )
    jobs = PiJobServer(
        pi_job_socket,
        PiJobService(
            sessions=relay.state,
            sessions_root=PI_SESSIONS_DIR,
            harness_dir=HARNESS_DIR,
            tmux_tmpdir=JOB_TMUX_DIR,
            orchestrator_url=PI_INGEST_BASE_URL or None,
            worker_id=worker_id,
            proxy=WH_PROXY or None,
        ),
    )
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signal_number in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signal_number, stop_event.set)
        except (NotImplementedError, RuntimeError):
            # The worker runs on Linux, but retain normal asyncio portability.
            signal.signal(signal_number, lambda *_: stop_event.set())

    try:
        # Bind both loopback services before publishing only the terminal
        # relay. The job service intentionally remains private to the worker.
        await relay.start()
        await jobs.start()
        if not publish_pi_relay(PI_RELAY_PORT):
            log.error("Pi relay remains loopback-only; heartbeat will advertise it as unavailable")

        async with client:
            # Initial registration
            tailscale_ip, dns_name = get_tailscale_identity()
            relay_available = reconcile_pi_relay(relay)
            log.info(f"Tailscale IP: {tailscale_ip} DNS: {dns_name or '(none)'}")
            await send_heartbeat(
                worker_id,
                tailscale_ip,
                dns_name,
                client,
                pi_relay_available=relay_available,
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
                relay_available = reconcile_pi_relay(relay)
                await send_heartbeat(
                    worker_id,
                    tailscale_ip,
                    dns_name,
                    client,
                    pi_relay_available=relay_available,
                )
    finally:
        # This removes only the reserved relay port, not data-export or other
        # independent Serve rules that may coexist on the worker.
        unpublish_pi_relay(PI_RELAY_PORT)
        await jobs.stop()
        await relay.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Worker daemon shutting down.")
