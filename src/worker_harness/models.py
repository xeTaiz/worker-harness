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
    STARTING = "starting"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"


class JobKind(str, Enum):
    SSH = "ssh"
    DELEGATED = "delegated"


class JobPriority(str, Enum):
    """Advisory urgency shown on the shared board; it never reorders FIFO."""

    FLEXIBLE = "flexible"
    NORMAL = "normal"
    BLOCKING = "blocking"
    TIME_CRITICAL = "time_critical"


class ExecutionState(str, Enum):
    """Supervisor lifecycle refinement of a starting/running job."""

    ACTIVE = "active"
    STOPPING = "stopping"
    TERMINATION_UNKNOWN = "termination_unknown"


class TerminationReason(str, Enum):
    USER_CANCELLED = "user_cancelled"
    GPU_IDLE_TIMEOUT = "gpu_idle_timeout"
    USER_DISABLED = "user_disabled"


class PiSessionType(str, Enum):
    INTERACTIVE = "interactive"
    GLOBAL_ROUTER = "global-router"

    @property
    def bridge_backed(self) -> bool:
        return self is PiSessionType.INTERACTIVE or self is PiSessionType.GLOBAL_ROUTER


class PiSessionState(str, Enum):
    QUEUED = "queued"
    STARTING = "starting"
    WORKING = "working"
    BLOCKED = "blocked"
    IDLE = "idle"
    STOPPED = "stopped"
    FAILED = "failed"
    # Timed out without the worker acknowledging the terminal state (spec §8.1).
    TERMINATION_UNKNOWN = "termination_unknown"


# ── GPU ───────────────────────────────────────────────────────────────

class GPUInfo(BaseModel):
    index: int
    name: str
    vram_total_gb: float
    vram_used_gb: float
    busy: bool | None = None
    # Raw activity telemetry for idle detection. These are deliberately
    # separate from the display-rounded VRAM values and availability flag.
    uuid: str = ""
    utilization_percent: float | None = None
    memory_used_bytes: int | None = None
    process_count: int | None = None
    # Counts/memory of processes that are NOT recognized display-server
    # graphics processes (xorg, xwayland, gnome-shell, kwin_wayland as type G).
    workload_process_count: int | None = None
    workload_memory_bytes: int | None = None
    # True only for a nonempty complete inventory of exempt display processes.
    display_only: bool = False
    # False whenever telemetry is failed, incomplete, unsupported or unknown.
    activity_valid: bool = False


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
    dns_name: str = ""
    ssh_user: str = "root"
    harness_dir: str = "/harness"
    gpu_count: int = 0
    gpus: list[GPUInfo] = Field(default_factory=list)
    cpu_cores: int = 0
    total_ram_gb: float = 0.0
    used_ram_gb: float = 0.0
    total_disk_gb: float = 0.0
    used_disk_gb: float = 0.0
    active_jobs: list[dict[str, Any]] = Field(default_factory=list)
    active_ports: list[dict[str, Any]] = Field(default_factory=list)
    # Immediate non-symlink directory children below effective container bind
    # destinations. This is a shallow discovery hint, not a recursive inventory.
    data_paths: list[str] = Field(default_factory=list)
    timestamp: str = ""
    # GPU sample identity. A new random sample ID accompanies each GPU query;
    # monotonic time is the daemon's clock, boot/daemon IDs scope it.
    gpu_sample_id: str = ""
    gpu_sample_monotonic_ns: int = 0
    worker_boot_id: str = ""
    daemon_instance_id: str = ""


class Worker(BaseModel):
    """Full worker record stored in the database."""

    id: str
    name: str
    worker_ip: str
    dns_name: str = ""
    ssh_user: str = "root"
    harness_dir: str = "/harness"
    gpu_count: int = 0
    gpu_names: list[str] = Field(default_factory=list)
    gpu_vram_gb: list[float] = Field(default_factory=list)
    gpu_used_vram_gb: list[float] = Field(default_factory=list)
    gpu_busy: list[bool | None] = Field(default_factory=list)
    cpu_cores: int = 0
    total_ram_gb: float = 0.0
    used_ram_gb: float = 0.0
    total_disk_gb: float = 0.0
    used_disk_gb: float = 0.0
    data_paths: list[str] = Field(default_factory=list)
    status: WorkerStatus = WorkerStatus.OFFLINE
    last_heartbeat_ts: int = 0
    created_at: int = 0
    # Latest raw GPU sample (complete GPUInfo incl. activity telemetry) and
    # its identity. Received fields are stamped by the control process with
    # its monotonic clock and per-process instance ID.
    gpu_telemetry: list[GPUInfo] = Field(default_factory=list)
    gpu_sample_id: str = ""
    gpu_sample_monotonic_ns: int = 0
    worker_boot_id: str = ""
    daemon_instance_id: str = ""
    gpu_sample_received_ns: int = 0
    gpu_sample_server_instance: str = ""

    @property
    def ssh_host(self) -> str:
        return self.dns_name or self.worker_ip

    @classmethod
    def from_registration(cls, reg: WorkerRegistration) -> Worker:
        now = int(datetime.now(timezone.utc).timestamp())
        return cls(
            id=reg.worker_id,
            name=reg.name,
            worker_ip=reg.worker_ip,
            dns_name=reg.dns_name,
            ssh_user=reg.ssh_user,
            harness_dir=reg.harness_dir,
            gpu_count=reg.gpu_count,
            gpu_names=[g.name for g in reg.gpus],
            gpu_vram_gb=[g.vram_total_gb for g in reg.gpus],
            gpu_used_vram_gb=[g.vram_used_gb for g in reg.gpus],
            gpu_busy=[g.busy for g in reg.gpus],
            cpu_cores=reg.cpu_cores,
            total_ram_gb=reg.total_ram_gb,
            used_ram_gb=reg.used_ram_gb,
            total_disk_gb=reg.total_disk_gb,
            used_disk_gb=reg.used_disk_gb,
            data_paths=reg.data_paths,
            status=WorkerStatus.ONLINE,
            last_heartbeat_ts=now,
            created_at=now,
            gpu_telemetry=list(reg.gpus),
            gpu_sample_id=reg.gpu_sample_id,
            gpu_sample_monotonic_ns=reg.gpu_sample_monotonic_ns,
            worker_boot_id=reg.worker_boot_id,
            daemon_instance_id=reg.daemon_instance_id,
        )

    def update_from_registration(self, reg: WorkerRegistration) -> None:
        self.name = reg.name
        self.worker_ip = reg.worker_ip
        self.dns_name = reg.dns_name
        self.ssh_user = reg.ssh_user
        self.harness_dir = reg.harness_dir
        self.gpu_count = reg.gpu_count
        self.gpu_names = [g.name for g in reg.gpus]
        self.gpu_vram_gb = [g.vram_total_gb for g in reg.gpus]
        self.gpu_used_vram_gb = [g.vram_used_gb for g in reg.gpus]
        self.gpu_busy = [g.busy for g in reg.gpus]
        self.cpu_cores = reg.cpu_cores
        self.total_ram_gb = reg.total_ram_gb
        self.used_ram_gb = reg.used_ram_gb
        self.total_disk_gb = reg.total_disk_gb
        self.used_disk_gb = reg.used_disk_gb
        self.data_paths = reg.data_paths
        self.status = WorkerStatus.ONLINE
        self.last_heartbeat_ts = int(datetime.now(timezone.utc).timestamp())
        self.gpu_telemetry = list(reg.gpus)
        self.gpu_sample_id = reg.gpu_sample_id
        self.gpu_sample_monotonic_ns = reg.gpu_sample_monotonic_ns
        self.worker_boot_id = reg.worker_boot_id
        self.daemon_instance_id = reg.daemon_instance_id


# ── Pi sessions ───────────────────────────────────────────────────────

class PiSession(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    worker_id: str | None = None
    parent_session_id: str | None = None
    session_type: PiSessionType = PiSessionType.INTERACTIVE
    state: PiSessionState = PiSessionState.QUEUED
    task: str = ""
    cwd: str = ""
    tmux_session: str = ""
    detail: str = ""
    role: str = ""
    token_hash: str = Field(default="", exclude=True, repr=False)
    question: str = ""
    meta: dict[str, Any] = Field(default_factory=dict)
    # Interactive bridge and host-relay metadata.
    name: str = ""
    host: str = ""
    agent: str = "pi"
    bridge_incarnation: str | None = None
    terminal_attachable: bool = False
    terminal_host: str = ""
    terminal_port: int = 0
    terminal_protocol_version: int = 0
    has_pending_messages: bool = False
    last_seen: int = 0
    created_at: int = 0
    updated_at: int = 0
    # Human owner (always the bootstrap administrator for fleet sessions).
    owner_user_id: str = ""


class PiSessionEvent(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    session_id: str
    event_type: str
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: int = 0
    # Durable, per-session replay cursor assigned by SQLite. Producers omit it.
    sequence: int = 0


class PiIngestEvent(BaseModel):
    """Event supplied during bridge registration or an event batch."""

    id: str | None = Field(default=None, max_length=128)
    event_type: str = Field(min_length=1, max_length=64)
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: int = 0




class PiBridgeRegister(BaseModel):
    """Registration from an ordinary non-worker Pi extension."""

    session_id: str = Field(min_length=1, max_length=128)
    incarnation: str = Field(min_length=1, max_length=128)
    cwd: str = Field(default="", max_length=4096)
    resume_path: str = Field(default="", max_length=4096)
    name: str = Field(default="", max_length=256)
    host: str = Field(default="", max_length=256)
    agent: str = Field(default="pi", max_length=16, pattern=r"^[a-z][a-z0-9_-]*$")
    terminal_attachable: bool = False
    terminal_host: str = Field(default="", max_length=256, pattern=r"^[A-Za-z0-9.-]*$")
    terminal_port: int = Field(default=0, ge=0, le=65535)
    terminal_protocol_version: int = Field(default=0, ge=0, le=100)
    has_pending_messages: bool = False
    # A newly attached bridge may include the latest completed exchange so the
    # durable transcript is useful immediately. The database accepts only
    # message start/end events from this bounded registration snapshot.
    initial_events: list[PiIngestEvent] = Field(default_factory=list, max_length=4)


class PiBridgeEventBatch(BaseModel):
    incarnation: str = Field(min_length=1, max_length=128)
    state: PiSessionState | None = None
    detail: str = Field(default="", max_length=4096)
    has_pending_messages: bool | None = None
    events: list[PiIngestEvent] = Field(default_factory=list)


class PiSessionCommand(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    session_id: str
    kind: str = "prompt"
    message: str = ""
    deliver_as: str = "followUp"
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: int = 0
    claimed_at: int = 0
    claimed_by: str = ""
    delivered_at: int = 0




# ── Job ────────────────────────────────────────────────────────────────

class Job(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    worker_id: str | None = None
    name: str = ""
    tmux_session: str = ""
    command: str = ""
    status: JobStatus = JobStatus.PENDING
    queue_managed: bool = False
    exit_code: int | None = None
    pty_enabled: bool = True
    # SSH-created jobs keep the legacy default.  A delegated Pi child reports
    # a linked job with its relay session as the immutable origin.
    kind: JobKind = JobKind.SSH
    origin_session_id: str | None = None
    report_revision: int = 0
    expected_seconds: int = 0
    gpu_count: int = 0
    gpu_indices: list[int] = Field(default_factory=list)
    queue_order: int = 0
    started_at: int = 0
    finished_at: int = 0
    # ── Multi-user ownership and shared-board metadata ──
    owner_user_id: str = ""
    submitted_by_actor_id: str = ""
    submitted_at: int = 0
    submission_sequence: int = 0
    priority: JobPriority = JobPriority.NORMAL
    priority_note: str = ""
    deadline_at: int | None = None
    termination_reason: TerminationReason | None = None
    worker_name_at_submission: str = ""
    # ── Supervisor lifecycle ──
    execution_state: ExecutionState = ExecutionState.ACTIVE
    stop_requested_at: int = 0
    stop_reason: TerminationReason | None = None
    # Runner confirmed all descendants exited; allocation may still be held
    # pending the post-stop GPU sample (idle-timeout handoff).
    stop_confirmed_at: int = 0


class WorkerJobReport(BaseModel):
    """A revisioned delegated-job projection reported by a worker relay."""

    id: str = Field(min_length=1, max_length=128)
    origin_session_id: str = Field(min_length=1, max_length=128)
    tmux_session: str = Field(min_length=1, max_length=128)
    command: str = Field(min_length=1, max_length=100_000)
    status: JobStatus
    exit_code: int | None = None
    pty_enabled: bool = True
    started_at: int = 0
    finished_at: int = 0
    report_revision: int = Field(ge=1)


class WorkerJobReportBatch(BaseModel):
    jobs: list[WorkerJobReport] = Field(default_factory=list, max_length=100)


# ── Port Forward ──────────────────────────────────────────────────────

class PortForward(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    worker_id: str
    local_port: int
    remote_port: int
    service_name: str = ""
    pid: int = 0
    created_at: int = 0
    owner_user_id: str = ""
    created_by_actor_id: str = ""
    bind_host: str = ""
    # Set for the internal forward owned by a managed Marimo session.
    marimo_session_id: str | None = None


# ── Marimo Service ────────────────────────────────────────────────────

class MarimoSession(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    worker_id: str
    notebook_path: str
    environment: str
    job_id: str
    tunnel_id: str
    local_port: int
    remote_port: int
    bind_host: str
    url: str
    status: str = "ready"
    created_at: int = 0
    owner_user_id: str = ""
    created_by_actor_id: str = ""
    # Private orchestrator-side path of the native Marimo token file.
    upstream_token_path: str = Field(default="", exclude=True, repr=False)


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
    dns_name: str = ""
    ssh_user: str
    harness_dir: str
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

    @property
    def ssh_host(self) -> str:
        return self.dns_name or self.worker_ip


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
