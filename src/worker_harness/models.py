"""Pydantic models for worker-harness data structures."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from pydantic import AliasChoices, BaseModel, Field


# ── Enums ────────────────────────────────────────────────────────────

class WorkerStatus(str, Enum):
    ONLINE = "online"
    OFFLINE = "offline"
    DRAINING = "draining"


class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"


# ── GPU ───────────────────────────────────────────────────────────────

class GPUInfo(BaseModel):
    index: int
    name: str
    vram_total_gb: float
    vram_used_gb: float


# ── Resources ─────────────────────────────────────────────────────────

class SystemResources(BaseModel):
    cpu_cores: int
    total_ram_gb: float
    used_ram_gb: float
    total_disk_gb: float
    used_disk_gb: float


# ── Worker ─────────────────────────────────────────────────────────────

class WorkerRegistration(BaseModel):
    """Payload sent by worker daemon on registration and heartbeat."""

    worker_id: str
    name: str
    worker_ip: str = Field(validation_alias=AliasChoices("worker_ip", "zerotier_ip"))
    ssh_port: int = 22
    gpu_count: int = 0
    gpus: list[GPUInfo] = Field(default_factory=list)
    cpu_cores: int = 0
    total_ram_gb: float = 0.0
    used_ram_gb: float = 0.0
    total_disk_gb: float = 0.0
    used_disk_gb: float = 0.0
    active_jobs: list[dict[str, Any]] = Field(default_factory=list)
    active_ports: list[dict[str, Any]] = Field(default_factory=list)
    timestamp: str = ""


class Worker(BaseModel):
    """Full worker record stored in the database."""

    id: str
    name: str
    worker_ip: str
    ssh_port: int = 22
    gpu_count: int = 0
    gpu_names: list[str] = Field(default_factory=list)
    gpu_vram_gb: list[float] = Field(default_factory=list)
    gpu_used_vram_gb: list[float] = Field(default_factory=list)
    cpu_cores: int = 0
    total_ram_gb: float = 0.0
    used_ram_gb: float = 0.0
    total_disk_gb: float = 0.0
    used_disk_gb: float = 0.0
    status: WorkerStatus = WorkerStatus.OFFLINE
    last_heartbeat_ts: int = 0
    created_at: int = 0

    @classmethod
    def from_registration(cls, reg: WorkerRegistration) -> Worker:
        now = int(datetime.now(timezone.utc).timestamp())
        return cls(
            id=reg.worker_id,
            name=reg.name,
            worker_ip=reg.worker_ip,
            ssh_port=reg.ssh_port,
            gpu_count=reg.gpu_count,
            gpu_names=[g.name for g in reg.gpus],
            gpu_vram_gb=[g.vram_total_gb for g in reg.gpus],
            gpu_used_vram_gb=[g.vram_used_gb for g in reg.gpus],
            cpu_cores=reg.cpu_cores,
            total_ram_gb=reg.total_ram_gb,
            used_ram_gb=reg.used_ram_gb,
            total_disk_gb=reg.total_disk_gb,
            used_disk_gb=reg.used_disk_gb,
            status=WorkerStatus.ONLINE,
            last_heartbeat_ts=now,
            created_at=now,
        )

    def update_from_registration(self, reg: WorkerRegistration) -> None:
        self.name = reg.name
        self.worker_ip = reg.worker_ip
        self.ssh_port = reg.ssh_port
        self.gpu_count = reg.gpu_count
        self.gpu_names = [g.name for g in reg.gpus]
        self.gpu_vram_gb = [g.vram_total_gb for g in reg.gpus]
        self.gpu_used_vram_gb = [g.vram_used_gb for g in reg.gpus]
        self.cpu_cores = reg.cpu_cores
        self.total_ram_gb = reg.total_ram_gb
        self.used_ram_gb = reg.used_ram_gb
        self.total_disk_gb = reg.total_disk_gb
        self.used_disk_gb = reg.used_disk_gb
        self.status = WorkerStatus.ONLINE
        self.last_heartbeat_ts = int(datetime.now(timezone.utc).timestamp())


# ── Job ────────────────────────────────────────────────────────────────

class Job(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    worker_id: str | None = None
    tmux_session: str = ""
    command: str = ""
    status: JobStatus = JobStatus.PENDING
    exit_code: int | None = None
    pty_enabled: bool = True
    started_at: int = 0
    finished_at: int = 0


# ── Port Forward ──────────────────────────────────────────────────────

class PortForward(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    worker_id: str
    local_port: int
    remote_port: int
    service_name: str = ""
    pid: int = 0
    created_at: int = 0


# ── Failure ───────────────────────────────────────────────────────────

class Failure(BaseModel):
    id: int = 0
    job_id: str
    worker_id: str
    exit_code: int
    timestamp: int = 0
    summary: str = ""


# ── CLI output models ─────────────────────────────────────────────────

class WorkerSummary(BaseModel):
    """Compact worker info for CLI/agent output."""

    id: str
    name: str
    worker_ip: str
    ssh_port: int
    gpu_count: int
    gpu_names: list[str]
    cpu_cores: int
    total_ram_gb: float
    used_ram_gb: float
    total_disk_gb: float
    used_disk_gb: float
    status: WorkerStatus
    last_heartbeat_ts: int
    running_job_count: int = 0


class AgentWorkersResponse(BaseModel):
    """Output model for `worker-harness agent workers`."""

    workers: list[WorkerSummary]
    total_online: int
    total_offline: int


class FreeGPUEntry(BaseModel):
    worker_id: str
    worker_name: str
    gpu_index: int
    gpu_name: str
    vram_total_gb: float
    vram_used_gb: float


class AgentFreeGPUsResponse(BaseModel):
    gpus: list[FreeGPUEntry]
    total_free_gb: float
