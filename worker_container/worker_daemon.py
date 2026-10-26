#!/usr/bin/env python3
"""
Worker Daemon — runs inside each worker container.

Responsibilities:
  - Send initial registration + periodic heartbeats to the orchestrator
  - Keep job, GPU, and data responsibilities available to the fleet
"""

import asyncio
import ctypes
import fcntl
import getpass
import json
import logging
import os
import signal
import socket
import subprocess
import sys
import math
import time
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
TS_SOCKET: str = os.environ.get("TS_SOCKET", str(WH_DIR / "tailscale" / "run" / "tailscaled.sock"))
HARNESS_DIR: Path = WH_DIR / "harness"
JOB_TMUX_DIR: Path = Path(os.environ.get("JOB_TMUX_DIR", str(HARNESS_DIR / "job-tmux")))
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
                "uuid": gpu_uuid.strip(),
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
    ]
    if not os.environ.get("WH_SLURM_WORKER_ID"):
        commands.append(["tailscale", "status", "--json"])

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
    reserved = os.environ.get("WH_SLURM_WORKER_ID", "").strip()
    if reserved:
        WORKER_ID_FILE.parent.mkdir(parents=True, exist_ok=True)
        if WORKER_ID_FILE.exists() and WORKER_ID_FILE.read_text().strip() != reserved:
            raise ValueError("persisted worker identity differs from reserved allocation")
        WORKER_ID_FILE.write_text(reserved)
        return reserved
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
    sys_info = get_system_info()
    return {
        "worker_id": worker_id, "name": WORKER_NAME, "worker_ip": tailscale_ip,
        "dns_name": dns_name, "ssh_user": SSH_USER, "harness_dir": str(HARNESS_DIR),
        "gpu_count": gpu_info.get("gpu_count", 0), "gpus": gpu_info.get("gpus", []),
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

def atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary.open("w") as stream:
        os.chmod(temporary, 0o600)
        json.dump(value, stream, allow_nan=False)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text())
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def timestamp(value: Any, default: float) -> float:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        except ValueError:
            return default
    return float(value) if type(value) in (int, float) and math.isfinite(value) else default


def cuda_device_uuids(expected: int) -> list[str]:
    """Resolve CUDA's Slurm-visible logical order, never host NVIDIA indices."""
    visibility = os.environ.get("WH_SLURM_CUDA_VISIBLE_DEVICES")
    if not visibility or "MIG" in visibility.upper():
        raise ValueError("missing or unsupported MIG Slurm CUDA visibility")
    os.environ["CUDA_VISIBLE_DEVICES"] = visibility
    driver = ctypes.CDLL("libcuda.so.1")

    def checked(name: str, *args: Any) -> None:
        if getattr(driver, name)(*args) != 0:
            raise ValueError(f"CUDA driver mapping failed: {name}")

    checked("cuInit", 0)
    count = ctypes.c_int()
    checked("cuDeviceGetCount", ctypes.byref(count))
    if count.value != expected:
        raise ValueError("CUDA visible GPU count differs from reserved allocation")
    mapped = []
    for index in range(count.value):
        device = ctypes.c_int()
        checked("cuDeviceGet", ctypes.byref(device), index)
        raw = (ctypes.c_ubyte * 16)()
        if not hasattr(driver, "cuDeviceGetUuid_v2"):
            raise ValueError("CUDA driver lacks unambiguous UUID v2 mapping (MIG unsupported)")
        name = "cuDeviceGetUuid_v2"
        checked(name, ctypes.byref(raw), device)
        mapped.append("GPU-" + str(uuid.UUID(bytes=bytes(raw))))
    if len(set(mapped)) != expected:
        raise ValueError("ambiguous CUDA GPU UUID mapping")
    return mapped


def allocation_gpu_info(device_ids: list[str]) -> dict[str, Any]:
    if not device_ids:
        return {"gpu_count": 0, "gpus": []}
    observed = get_gpu_info()["gpus"]
    mapped: dict[str, dict[str, Any]] = {}
    for gpu in observed:
        key = gpu["uuid"]
        if key in mapped:
            raise ValueError("ambiguous NVIDIA GPU UUID telemetry")
        mapped[key] = gpu
    if any(device not in mapped for device in device_ids):
        raise ValueError("CUDA mapping absent from physical NVIDIA telemetry (MIG unsupported)")
    return {"gpu_count": len(device_ids), "gpus": [
        dict(mapped[device], index=index) for index, device in enumerate(device_ids)
    ]}


def valid_result(value: dict[str, Any]) -> dict[str, Any] | None:
    """Only runner descendant-exit proof, not shell output, releases a claim."""
    if type(value.get("exit_code")) is not int or value.get("descendants_exited") is not True:
        return None
    for key, minimum in (("finished_at", 0), ("finished_monotonic", -1)):
        number = value.get(key)
        if type(number) not in (int, float) or not math.isfinite(number) or number <= minimum:
            return None
    if not isinstance(value.get("boot_id"), str) or not 0 < len(value["boot_id"]) <= 128:
        return None
    result = {key: value[key] for key in (
        "exit_code", "descendants_exited", "finished_at", "finished_monotonic", "boot_id",
    )}
    if "started_at" in value or "started_monotonic" in value:
        for key, minimum in (("started_at", 0), ("started_monotonic", -1)):
            number = value.get(key)
            if type(number) not in (int, float) or not math.isfinite(number) or number <= minimum:
                return None
            result[key] = number
        if result["started_monotonic"] > result["finished_monotonic"]:
            return None
    reason = value.get("termination_reason")
    if reason is not None:
        if not isinstance(reason, str) or len(reason) > 1024:
            return None
        result["termination_reason"] = reason
    return result


def clock_after(left: dict[str, Any], left_prefix: str, right: dict[str, Any], right_prefix: str) -> bool:
    if left.get("boot_id") and left.get("boot_id") == right.get("boot_id"):
        return left[f"{left_prefix}_monotonic"] > right[f"{right_prefix}_monotonic"]
    return left[f"{left_prefix}_at"] > right[f"{right_prefix}_at"]


class SlurmRuntime:
    """Finite mode of the ordinary daemon; execution remains in the runner."""

    def __init__(self, settings: dict[str, Any], worker_id: str, token: str):
        self.settings, self.worker_id, self.token = settings, worker_id, token
        self.state_path = WH_DIR / "observations.json"
        self.state = read_json(self.state_path) or {
            "seen_results": {}, "pending_results": {}, "latest_start_clock": None,
            "failure_deadline": None, "failure_clock": None,
        }
        if self.state.get("worker_id", worker_id) != worker_id:
            raise ValueError("observation identity differs from reservation")
        self.state["worker_id"] = worker_id
        self.boot_id = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        self.active: dict[str, dict[str, Any]] = {}
        self.device_ids: list[str] = []
        self.ready = False
        self.control_action = ""
        self.control_deadline: float | None = None
        self.controlled: set[tuple[str, str, str]] = set()
        self.startup_error: str | None = None

    def persist(self) -> None:
        atomic_json(self.state_path, self.state)

    def fence(self, *, idle_only: bool = False) -> bool:
        HARNESS_DIR.mkdir(parents=True, exist_ok=True)
        with (HARNESS_DIR / ".launch.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            if idle_only:
                self.scan()
                if self.useful_active():
                    return False
            (HARNESS_DIR / ".draining").touch(mode=0o600, exist_ok=True)
            return True

    def scan(self) -> bool:
        before = json.dumps(self.state, sort_keys=True)
        active, records = {}, []
        for directory in HARNESS_DIR.iterdir() if HARNESS_DIR.exists() else ():
            if not directory.is_dir() or directory.is_symlink():
                continue
            started_runtime = read_json(directory / "runtime.json")
            runtime = started_runtime or read_json(directory / "launch.json")
            if not runtime or runtime.get("job_id") != directory.name:
                continue
            result = valid_result(read_json(directory / "result.json"))
            if result and "started_at" in result and not started_runtime:
                runtime = dict(runtime, **{key: result[key] for key in (
                    "started_at", "started_monotonic", "boot_id",
                )})
            records.append((directory.name, runtime, result))
            if result is None and started_runtime:
                active[directory.name] = runtime
            if runtime.get("queue_managed") is True:
                started, monotonic = runtime.get("started_at"), runtime.get("started_monotonic")
                if (type(started) in (int, float) and math.isfinite(started) and started > 0
                        and type(monotonic) in (int, float) and math.isfinite(monotonic) and monotonic >= 0
                        and isinstance(runtime.get("boot_id"), str) and runtime["boot_id"]):
                    previous = self.state.get("latest_start_clock")
                    if previous is None or clock_after(runtime, "started", previous, "started"):
                        self.state["latest_start_clock"] = {key: runtime[key] for key in (
                            "started_at", "started_monotonic", "boot_id",
                        )}
                        clock = self.state.get("failure_clock")
                        if clock and clock_after(runtime, "started", clock, "finished"):
                            self.state["failure_deadline"] = None
                            self.state["failure_clock"] = None
        self.active = active
        for job_id, runtime, result in sorted(records, key=lambda item: (item[2] or {}).get("finished_at", 0)):
            if result is None or job_id in self.state["seen_results"]:
                continue
            self.state["seen_results"][job_id] = result
            self.state["pending_results"][job_id] = result
            if (runtime.get("queue_managed") is True and result["exit_code"] != 0
                    and (self.state["latest_start_clock"] is None or not clock_after(
                        self.state["latest_start_clock"], "started", result, "finished"))
                    and self.state["failure_deadline"] is None):
                self.state["failure_deadline"] = result["finished_at"] + self.settings["failure_grace_seconds"]
                self.state["failure_clock"] = result
        changed = before != json.dumps(self.state, sort_keys=True)
        if changed:
            self.persist()
        return changed

    def grace_remaining(self) -> float | None:
        deadline = self.state.get("failure_deadline")
        if deadline is None:
            return None
        remaining = deadline - time.time()
        clock = self.state.get("failure_clock")
        if clock and clock["boot_id"] == self.boot_id:
            remaining = min(remaining, clock["finished_monotonic"]
                            + self.settings["failure_grace_seconds"] - time.monotonic())
        return remaining

    def useful_active(self) -> bool:
        return any(runtime.get("queue_managed") is True for runtime in self.active.values())

    def payload(self, ip: str, dns: str, drain: bool = False) -> dict[str, Any]:
        # CPU mode never calls GPU/CUDA readiness or telemetry.
        gpu = ({"gpu_count": self.settings["gpu_count"], "gpus": []}
               if self.startup_error else allocation_gpu_info(self.device_ids))
        system = get_system_info()
        system["used_ram_gb"] = min(system.get("used_ram_gb", 0), self.settings["memory_mb"] / 1024)
        return {
            "worker_id": self.worker_id, "name": self.settings["worker_name"], "worker_ip": ip,
            "dns_name": dns, "ssh_user": SSH_USER, "harness_dir": str(HARNESS_DIR),
            **gpu, **system,
            "cpu_cores": self.settings["cpus"], "total_ram_gb": self.settings["memory_mb"] / 1024,
            "active_jobs": [{
                "job_id": job_id, "status": "running",
                **{key: record[key] for key in (
                    "queue_managed", "started_at", "started_monotonic", "boot_id",
                ) if key in record},
            } for job_id, record in self.active.items()],
            "active_ports": [], "data_paths": get_data_paths(),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "slurm": {
                "results": [{"job_id": job_id, "result": result} for job_id, result
                            in list(self.state["pending_results"].items())[:100]],
                "failure_deadline": self.state.get("failure_deadline"),
                "checkpoint_requested": bool(self.control_action), "draining": drain,
                "startup_error": self.startup_error,
            },
        }

    async def heartbeat(self, client: httpx.AsyncClient, drain: bool = False) -> dict[str, Any] | None:
        try:
            ip, dns = await asyncio.to_thread(get_tailscale_identity)
            payload = await asyncio.to_thread(self.payload, ip, dns, drain)
            response = await client.post(
                f"http://{ORCHESTRATOR_HOST}:{ORCHESTRATOR_PORT}/register",
                json=payload, headers={"Authorization": f"Bearer {self.token}"}, timeout=12,
            )
            if response.status_code != 200:
                log.warning("Scoped heartbeat refused: HTTP %s", response.status_code)
                return None
            body = response.json()
            if body.get("worker_id") != self.worker_id or body.get("worker_status") not in {
                "offline", "online", "draining",
            }:
                log.error("Scoped registration returned invalid identity/readiness")
                return None
            for report in payload["slurm"]["results"]:
                self.state["pending_results"].pop(report["job_id"], None)
            self.persist()
            if body["worker_status"] == "online" and not self.ready:
                atomic_json(WH_DIR / "ready", {"worker_id": self.worker_id, "ready_at": time.time()})
                self.ready = True
            return body
        except (httpx.HTTPError, ValueError):
            log.warning("Scoped heartbeat unavailable", exc_info=True)
            return None

    async def runner_control(self, job_id: str, action: str, requested_signal: str) -> None:
        key = (job_id, action, requested_signal)
        if key in self.controlled:
            return
        try:
            process = await asyncio.create_subprocess_exec(
                sys.executable, str(HARNESS_DIR / ".job_runner.py"), "control",
                "--job-dir", str(HARNESS_DIR / job_id), "--action", action, "--signal", requested_signal,
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
            )
            try:
                code = await asyncio.wait_for(process.wait(), timeout=5)
            except asyncio.CancelledError:
                if process.returncode is None:
                    process.kill()
                await process.wait()
                raise
            except TimeoutError:
                process.kill()
                await process.wait()
                return
            if code == 0:
                self.controlled.add(key)
        except OSError:
            log.exception("Runner control failed for %s", job_id)

    async def controls(self) -> None:
        request = read_json(WH_DIR / "control.json")
        action = request.get("action")
        if action in {"stop", "checkpoint"} and self.control_action != "stop":
            self.control_action = action
            self.fence()
            self.scan()
            if action == "stop" and self.control_deadline is None:
                requested_at = timestamp(request.get("requested_at"), time.time())
                remaining = max(0, min(self.settings["termination_grace_seconds"],
                                      requested_at + self.settings["termination_grace_seconds"] - time.time()))
                self.control_deadline = time.monotonic() + remaining
        if not self.control_action:
            return
        stopping = self.control_action == "stop" and time.monotonic() >= self.control_deadline
        await asyncio.gather(*(self.runner_control(
            job_id, "stop" if stopping else "checkpoint",
            "TERM" if self.control_action == "stop" else
            self.active[job_id].get("checkpoint_signal", self.settings.get("checkpoint_signal", "USR1")),
        ) for job_id in self.active))

    async def stop_all(self, *, idle_only: bool = False) -> bool:
        if not self.fence(idle_only=idle_only):
            return False
        self.scan()
        await asyncio.gather(*(self.runner_control(job_id, "stop", "TERM") for job_id in self.active))
        return True


async def slurm_main() -> None:
    worker_id = get_worker_id()
    settings = read_json(Path(os.environ.get("WH_SLURM_SETTINGS_FILE", "")))
    if settings.get("worker_id") != worker_id or settings.get("compute_kind") not in {"cpu", "gpu"}:
        raise ValueError("invalid allocation settings or reserved identity")
    if not isinstance(settings.get("worker_name"), str) or not settings["worker_name"]:
        raise ValueError("allocation settings require the reserved worker name")
    if settings.get("checkpoint_signal") not in {"USR1", "TERM"}:
        raise ValueError("invalid workload checkpoint signal")
    (WH_DIR / "ready").unlink(missing_ok=True)
    for key in ("cpus", "memory_mb", "gpu_count", "failure_grace_seconds", "termination_grace_seconds"):
        minimum = 1 if key in {"cpus", "memory_mb"} else 0
        if type(settings.get(key)) is not int or settings[key] < minimum:
            raise ValueError(f"invalid allocation setting {key}")
    if (settings["compute_kind"] == "cpu") != (settings["gpu_count"] == 0):
        raise ValueError("allocation GPU capacity does not match compute kind")
    if not ORCHESTRATOR_HOST:
        raise ValueError("ORCHESTRATOR_HOST required")
    token = Path(os.environ["WH_WORKER_TOKEN_FILE"]).read_text().strip()
    if not token or any(character.isspace() for character in token):
        raise ValueError("invalid scoped worker token file")
    runtime = SlurmRuntime(settings, worker_id, token)
    if settings["compute_kind"] == "cpu":
        os.environ["CUDA_VISIBLE_DEVICES"] = ""
    else:
        try:
            runtime.device_ids = cuda_device_uuids(settings["gpu_count"])
            allocation_gpu_info(runtime.device_ids)
        except Exception as error:
            log.error("Allocation GPU startup failed: %s", error)
            runtime.startup_error = str(error)[:2048]
            atomic_json(WH_DIR / "startup-error.json", {"error": runtime.startup_error})
            runtime.fence()
            async with build_http_client() as client:
                await runtime.heartbeat(client)
            raise
    loop = asyncio.get_running_loop()

    def requested(signum: int) -> None:
        with (WH_DIR / "control.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            old = read_json(WH_DIR / "control.json")
            if old.get("action") != "stop":
                atomic_json(WH_DIR / "control.json", {
                    "action": "checkpoint" if signum == signal.SIGUSR1 else "stop",
                    "signal": settings["checkpoint_signal"] if signum == signal.SIGUSR1 else "TERM",
                    "requested_at": time.time(),
                })

    for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGUSR1):
        loop.add_signal_handler(signum, requested, signum)
    startup = read_json(WH_DIR / "startup.json")
    deadline = timestamp(startup.get("deadline_epoch", startup.get("deadline", startup.get("deadline_at"))),
                         time.time() + 180)
    startup_deadline = time.monotonic() + min(180, max(0, deadline - time.time()))
    task: asyncio.Task | None = None
    control_task: asyncio.Task | None = None
    connected, exit_code = False, 0
    async with build_http_client() as client:
        try:
            while True:
                runtime.scan()
                if control_task is not None and control_task.done():
                    control_task.result()
                    control_task = None
                if control_task is None:
                    control_task = asyncio.create_task(runtime.controls())
                if task is not None and task.done():
                    body = task.result()
                    task = None
                    connected = body is not None
                    if body and body.get("draining"):
                        runtime.fence()
                        if not runtime.useful_active():
                            await runtime.stop_all()
                            exit_code = 1 if runtime.grace_remaining() is not None or runtime.control_action == "stop" else 0
                            break
                remaining = runtime.grace_remaining()
                if remaining is not None and remaining <= 0 and not runtime.useful_active():
                    if await runtime.stop_all(idle_only=True):
                        log.warning("Allocation failure grace expired at %s; releasing idle resources",
                                    runtime.state["failure_deadline"])
                        exit_code = 1
                        break
                if runtime.control_action and not runtime.active:
                    exit_code = 1 if runtime.control_action == "stop" else 0
                    break
                if not runtime.ready and time.monotonic() >= startup_deadline:
                    runtime.startup_error = "allocation startup timed out awaiting authenticated SSH readiness"
                    atomic_json(WH_DIR / "startup-error.json", {"error": runtime.startup_error})
                    log.error(runtime.startup_error)
                    await runtime.stop_all()
                    exit_code = 1
                    break
                if runtime.ready and not connected and task is None and not runtime.useful_active():
                    if remaining is None or remaining <= 0:
                        if await runtime.stop_all(idle_only=True):
                            log.warning("Control unavailable after local useful work ended; releasing allocation")
                            exit_code = 1
                            break
                if task is None:
                    drain = (runtime.ready and not runtime.useful_active() and remaining is None
                             and not runtime.control_action)
                    task = asyncio.create_task(runtime.heartbeat(client, drain=drain))
                await asyncio.sleep(1)
        finally:
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            if control_task is not None:
                control_task.cancel()
                await asyncio.gather(control_task, return_exceptions=True)
            runtime.scan()
            runtime.persist()
            if runtime.state["pending_results"] or runtime.control_action or runtime.startup_error:
                try:
                    await asyncio.wait_for(runtime.heartbeat(client, drain=not runtime.useful_active()), timeout=1)
                except TimeoutError:
                    pass
    if exit_code:
        raise SystemExit(exit_code)


async def main() -> None:
    if os.environ.get("WH_SLURM_WORKER_ID"):
        await slurm_main()
        return
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
