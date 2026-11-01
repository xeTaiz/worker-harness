"""FastAPI-based HTTP server for worker heartbeats and orchestration API."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import secrets
import shlex
import time
from collections import Counter
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, AsyncIterator, Awaitable, Callable, Literal, Self, TypeVar
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit
from uuid import uuid4

from fastapi import Depends, FastAPI, HTTPException, Query, Request, WebSocket, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator
from starlette.requests import HTTPConnection
from websockets.asyncio.client import connect as websocket_connect
from websockets.exceptions import ConnectionClosed, WebSocketException

from .auth import (
    MARIMO_PROXY_PATH,
    SESSION_COOKIE,
    LoginRateLimiter,
    load_proxy_trust,
    PasswordService,
    Principal,
    enforce_browser_mutation,
    enforce_capability,
    load_web_origin,
    origin_matches,
    principal_still_valid,
    require_bootstrap_web_user,
    require_principal,
    resolve_principal,
)
from .auth_routes import create_auth_router
from .cache import TTLCache
from .data import (
    DataPathError,
    destination_copy_command,
    is_advertised_data_path,
    reverse_data_paths,
    source_cleanup_command,
    source_export_command,
    validate_data_path,
    with_worker_dir,
)
from .db import Database, ListenerConflict, QueueConflict, QueueValidationError
from .job import JobManager
from .machines import load_machines
from .orchestration import Orchestrator, WorktreeError
from .pr import PrRejected, open_pr
from .projects import load_projects
from .lanes import LaneTimeout, WorkerLanes
from .metrics import Metrics, set_global_metrics
from .marimo import (
    allocate_local_port,
    allocate_worker_port,
    build_launch_command,
    delete_local_token,
    delete_remote_token,
    local_token_path,
    proxy_base_path,
    remote_token_path,
    tailnet_bind_host,
    upload_remote_token,
    wait_until_ready,
    write_local_token,
)
from .marimo_proxy import ProxyTarget, create_marimo_proxy_router
from .models import (
    Job,
    JobKind,
    JobPriority,
    JobStatus,
    PiBridgeEventBatch,
    PiBridgeRegister,
    PiSession,
    PiSessionCommand,
    PiSessionEvent,
    PiSessionState,
    PortForward,
    MarimoSession,
    TerminationReason,
    WorkerJobReportBatch,
    WorkerRegistration,
    WorkerStatus,
)
from .queue_board import forecast_worker, owned_job, public_queue_job, public_worker
from .ratelimit import AgentRateLimiter, RateLimited, resolve_agent_name
from .reaper import reap_loop, release_orphan_forwards
from .ssh import async_ssh_run, set_lanes, ssh_download_bytes, ssh_port_forward, ssh_upload_bytes
from .tunnel_registry import TunnelProcess, TunnelRegistry

log = logging.getLogger("heartbeat-server")

T = TypeVar("T")

# GET routes that reach workers over SSH and therefore stay rate-limited.
SSH_BACKED_READ = re.compile(
    r"^/api/v1/(jobs/[^/]+/logs(/stream)?|marimo/[^/]+|workers/[^/]+/files)$"
)


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


def _nonblank(value: str | None, label: str) -> str | None:
    if value is not None and not value.strip():
        raise ValueError(f"{label} must not be blank")
    return value


class PriorityFields(_Strict):
    priority: JobPriority = JobPriority.NORMAL
    priority_note: str = Field(default="", max_length=1000)
    deadline_at: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def _time_critical_note(self) -> Self:
        if self.priority == JobPriority.TIME_CRITICAL and not self.priority_note.strip():
            raise ValueError("time_critical priority requires a priority_note")
        return self


class JobCreateRequest(PriorityFields):
    """CPU/setup work launched immediately; it never takes a GPU slot."""

    worker_id: str
    command: str = Field(min_length=1)
    name: str = Field(min_length=1, max_length=200)
    expected_seconds: int = Field(ge=1)
    no_pty: bool = False
    sync: bool = False       # block until command finishes, return stdout
    sync_timeout: int = Field(default=120, ge=1, le=3600)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        return _nonblank(value, "name")


class JobEnqueueRequest(PriorityFields):
    worker_id: str
    command: str = Field(min_length=1)
    name: str = Field(min_length=1, max_length=200)
    expected_seconds: int = Field(ge=1)
    gpu_count: int = Field(ge=1)
    no_pty: bool = False

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        return _nonblank(value, "name")


class _MetadataUpdate(_Strict):
    expected_seconds: int | None = Field(default=None, ge=1)
    priority: JobPriority | None = None
    priority_note: str | None = Field(default=None, max_length=1000)
    deadline_at: int | None = Field(default=None, ge=0)

    def changes(self) -> dict:
        # deadline_at may be explicitly cleared with null.
        return {key: getattr(self, key) for key in self.model_fields_set}

    @model_validator(mode="after")
    def validate_update(self) -> Self:
        if not self.model_fields_set:
            raise ValueError("at least one field must be supplied")
        for key in self.model_fields_set - {"deadline_at"}:
            if getattr(self, key) is None:
                raise ValueError(f"{key} cannot be null")
        return self


class RunningJobUpdateRequest(_MetadataUpdate):
    pass


class QueuedJobUpdateRequest(_MetadataUpdate):
    worker_id: str | None = None
    name: str | None = Field(default=None, max_length=200)
    gpu_count: int | None = Field(default=None, ge=1)

    @field_validator("name")
    @classmethod
    def validate_optional_name(cls, value: str | None) -> str | None:
        return _nonblank(value, "name")

    def changes(self) -> dict:
        return {key: getattr(self, key) for key in self.model_fields_set if key != "worker_id"}


class QueuePreviewRequest(_Strict):
    before_job_id: str | None
    expected_revision: int = Field(ge=0)
    reason: str

    @field_validator("reason")
    @classmethod
    def validate_reason(cls, value: str) -> str:
        value = value.strip()
        if not 1 <= len(value) <= 1000:
            raise ValueError("reason must be 1-1000 characters")
        return value


class QueueMoveRequest(_Strict):
    preview_id: str = Field(min_length=1)
    expected_revision: int = Field(ge=0)


class TunnelCreateRequest(_Strict):
    worker_id: str
    local_port: int = Field(ge=1024, le=65535)
    remote_port: int = Field(ge=1, le=65535)
    name: str = Field(default="", max_length=200)


class MarimoCreateRequest(_Strict):
    worker_id: str
    notebook_path: str
    environment: str
    ready_timeout: float = 45.0

# 10 MB — larger transfers should use direct rsync over tailnet SSH
MAX_FILE_TRANSFER_BYTES = 10 * 1024 * 1024


class FileUploadRequest(BaseModel):
    path: str
    content_b64: str  # base64-encoded file content


class FileDownloadRequest(BaseModel):
    path: str
    max_bytes: int = MAX_FILE_TRANSFER_BYTES


class DataCopyRequest(BaseModel):
    src_worker: str
    src_path: str
    dst_worker: str
    dst_path: str
    ttl_seconds: int = 6 * 60 * 60


class PiMessageRequest(BaseModel):
    message: str = Field(min_length=1)


class PiQuestionRequest(BaseModel):
    question: str = Field(min_length=1, max_length=4000)


class PiNoteRequest(BaseModel):
    note: str = Field(min_length=1)


class PiTaskLaunchRequest(BaseModel):
    branch: str = Field(min_length=1)
    briefing: str = Field(min_length=1)


class PiConfigureRequest(BaseModel):
    provider: str | None = None
    model: str | None = None
    thinking_level: Literal[
        "off", "minimal", "low", "medium", "high", "xhigh", "max"
    ] | None = None


class PiTeardownRequest(BaseModel):
    force: bool = False


class PiSubmitPrRequest(BaseModel):
    summary: str = Field(min_length=1)


class PiCommandAck(BaseModel):
    incarnation: str




async def reconcile_active_ssh_jobs(
    db: Database,
    job_manager: JobManager,
    *,
    max_concurrent: int = 4,
) -> None:
    """Refresh persisted SSH job state without blocking read endpoints."""
    jobs = [job for job in await db.list_active_jobs() if job.status == JobStatus.RUNNING]
    if not jobs:
        return

    workers = {worker.id: worker for worker in await db.list_workers()}
    pending = iter(jobs)

    async def reconcile_worker() -> None:
        for job in pending:
            worker = workers.get(job.worker_id or "")
            if worker is None:
                continue
            try:
                await job_manager.refresh_job_status(worker, job)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Failed to reconcile job %s", job.id)

    worker_count = min(max(1, max_concurrent), len(jobs))
    await asyncio.gather(*(reconcile_worker() for _ in range(worker_count)))

async def reconcile_queued_jobs(db: Database, job_manager: JobManager) -> None:
    """Recover claimed queue jobs, then advance every online worker queue."""
    workers = {
        worker.id: worker
        for worker in await db.list_workers()
        if worker.status == WorkerStatus.ONLINE
    }
    starting_jobs = [
        job
        for job in await db.list_jobs(status=JobStatus.STARTING)
        if job.kind == JobKind.SSH and job.worker_id in workers
    ]
    # Recovery prepends missing launches; process newest first to preserve FIFO.
    for job in sorted(starting_jobs, key=lambda item: (item.started_at, item.queue_order, item.id), reverse=True):
        try:
            await job_manager.reconcile_starting_job(workers[job.worker_id], job)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Failed to reconcile queued job %s", job.id)
    for worker in workers.values():
        try:
            await job_manager.enforce_gpu_idle(worker)
            await job_manager.dispatch_worker(worker)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Failed to dispatch queue for worker %s", worker.id)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start reliability background work and deterministically clean up.

    Database ownership remains with the caller (`serve`), but every persistent
    tunnel and every queued lane waiter belongs to this FastAPI app.
    """
    # Forward processes never survive this process; rows left by a restart
    # or crash would otherwise reserve their listener ports forever.
    await release_orphan_forwards(app)
    app.state.reaper_task = asyncio.create_task(reap_loop(app))
    background_tasks = [app.state.reaper_task]
    reconciler = getattr(app.state, "job_reconciler", None)
    if reconciler is not None:
        app.state.job_reconciler_task = asyncio.create_task(reconciler())
        background_tasks.append(app.state.job_reconciler_task)
    if app.state.machines:
        async def poll_fleet() -> None:
            while True:
                await asyncio.sleep(5)
                try:
                    await app.state.orchestrator.poll()
                except Exception:
                    log.exception("Fleet projection failed")
        background_tasks.append(asyncio.create_task(poll_fleet(), name="pi-fleet-poll"))
    try:
        yield
    finally:
        for task in background_tasks:
            task.cancel()
        await asyncio.gather(*background_tasks, return_exceptions=True)
        reaped = app.state.tunnels.shutdown()
        app.state.metrics.reaped_tunnels_total.inc(reaped)
        await app.state.lanes.shutdown()


def create_registration_app(db: Database) -> FastAPI:
    """Worker-only registration service, intentionally separate from control."""
    app = FastAPI(title="Worker Harness Registration API")

    @app.post("/register")
    async def register(reg: WorkerRegistration):
        try:
            worker = await db.upsert_worker(reg)
            log.info(
                "Worker registered/updated: %s (id=%s, ip=%s)",
                worker.name, worker.id, worker.worker_ip,
            )
            return {"status": "ok", "worker_id": worker.id}
        except ValidationError as exc:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc))
        except Exception as exc:
            log.error("Registration failed: %s", exc)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))

    @app.get("/health")
    async def health():
        return {"status": "healthy", "ts": datetime.now(timezone.utc).isoformat()}


    @app.post("/pi/worker/{worker_id}/jobs")
    async def worker_pi_jobs(worker_id: str, payload: WorkerJobReportBatch):
        worker = await db.get_worker(worker_id)
        if not worker:
            raise HTTPException(status_code=404, detail="worker not found")
        applied = 0
        try:
            for report in payload.jobs:
                _job, changed = await db.upsert_reported_worker_job(worker_id, report)
                applied += int(changed)
        except KeyError as exc:
            raise HTTPException(status_code=410, detail="origin session projection is gone") from exc
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"jobs_received": len(payload.jobs), "jobs_applied": applied}

    return app


async def stream_pi_session_events(
    request: Request, db: Database, session_id: str, after: int = 0,
    *, still_valid: Callable[[], Awaitable[bool]],
) -> AsyncIterator[str]:
    """Replay and tail one session's durable event log as SSE.

    Credentials are rechecked at least every five seconds; revocation or a
    disabled account ends the stream.
    """
    cursor = max(0, after)
    heartbeat_at = asyncio.get_running_loop().time() + 15.0
    recheck_at = asyncio.get_running_loop().time() + 5.0
    while True:
        if await request.is_disconnected():
            return
        if asyncio.get_running_loop().time() >= recheck_at:
            if not await still_valid():
                return
            recheck_at = asyncio.get_running_loop().time() + 5.0
        events = await db.list_pi_session_events(session_id, after=cursor, limit=500)
        if events:
            for event in events:
                cursor = event.sequence
                data = json.dumps(event.model_dump(mode="json"), separators=(",", ":"))
                yield f"id: {cursor}\nevent: pi-event\ndata: {data}\n\n"
            heartbeat_at = asyncio.get_running_loop().time() + 15.0
            continue
        now = asyncio.get_running_loop().time()
        if now >= heartbeat_at:
            yield ": keep-alive\n\n"
            heartbeat_at = now + 15.0
        await asyncio.sleep(0.25)


def _positive_int_env(name: str, default: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except ValueError:
        return default
    return value if value > 0 else default


def _terminal_dimension(value: str | None, fallback: int) -> int:
    try:
        parsed = int(value or fallback)
    except (TypeError, ValueError):
        parsed = fallback
    return max(1, min(1000, parsed))


def _terminal_url_with_dimensions(url: str, rows: int, cols: int) -> str:
    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query.update({"rows": str(rows), "cols": str(cols)})
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


@dataclass
class _GatewayAttachment:
    id: str
    client: WebSocket
    last_activity: float
    rows: int
    cols: int
    evict: asyncio.Event = field(default_factory=asyncio.Event)
    released: asyncio.Event = field(default_factory=asyncio.Event)

    def note_client_frame(self, data: bytes | str) -> None:
        """Track input and changed resize frames without altering the payload."""

        if isinstance(data, bytes):
            self.last_activity = asyncio.get_running_loop().time()
            return
        try:
            frame = json.loads(data)
        except (json.JSONDecodeError, TypeError):
            self.last_activity = asyncio.get_running_loop().time()
            return
        if not isinstance(frame, dict):
            self.last_activity = asyncio.get_running_loop().time()
            return
        if frame.get("type") == "resize":
            rows = _terminal_dimension(frame.get("rows"), self.rows)
            cols = _terminal_dimension(frame.get("cols"), self.cols)
            if (rows, cols) != (self.rows, self.cols):
                self.rows, self.cols = rows, cols
                self.last_activity = asyncio.get_running_loop().time()
            return
        self.last_activity = asyncio.get_running_loop().time()


async def _reserve_gateway_attachment(
    app: FastAPI, session_id: str, attachment: _GatewayAttachment
) -> _GatewayAttachment | None:
    """Atomically admit an attachment and signal the longest-idle victim."""

    victim = None
    async with app.state.pi_gateway_lock:
        active = app.state.pi_gateway_attachments.setdefault(session_id, {})
        if len(active) >= app.state.pi_gateway_max_per_session:
            victim = min(
                active.values(),
                key=lambda candidate: (candidate.last_activity, candidate.id),
            )
            active.pop(victim.id, None)
            app.state.pi_gateway_evictions_total += 1
        active[attachment.id] = attachment
    if victim is not None:
        victim.evict.set()
    return victim


async def _release_gateway_attachment(
    app: FastAPI, session_id: str, attachment: _GatewayAttachment
) -> None:
    """Release exactly this attachment; delayed victim cleanup is harmless."""

    async with app.state.pi_gateway_lock:
        active = app.state.pi_gateway_attachments.get(session_id)
        if active is not None and active.get(attachment.id) is attachment:
            active.pop(attachment.id, None)
            if not active:
                app.state.pi_gateway_attachments.pop(session_id, None)
    attachment.released.set()


async def _pump_terminal_gateway(
    client: WebSocket,
    upstream: Any,
    *,
    send_timeout: float = 10.0,
    attachment: _GatewayAttachment | None = None,
    still_valid: Callable[[], Awaitable[bool]] | None = None,
) -> str:
    """Pump terminal frames without buffering or touching persistent state."""

    async def client_to_upstream() -> None:
        while True:
            message = await client.receive()
            if message.get("type") == "websocket.disconnect":
                return
            data = message.get("bytes")
            if data is None:
                data = message.get("text")
            if data is not None:
                if attachment is not None:
                    attachment.note_client_frame(data)
                await asyncio.wait_for(upstream.send(data), timeout=send_timeout)

    async def upstream_to_client() -> None:
        async for data in upstream:
            if isinstance(data, bytes):
                await asyncio.wait_for(client.send_bytes(data), timeout=send_timeout)
            else:
                await asyncio.wait_for(client.send_text(data), timeout=send_timeout)

    tasks = {
        asyncio.create_task(client_to_upstream(), name="pi-gateway-client-upstream"),
        asyncio.create_task(upstream_to_client(), name="pi-gateway-upstream-client"),
    }
    eviction_task = None
    if attachment is not None:
        eviction_task = asyncio.create_task(attachment.evict.wait(), name="pi-gateway-eviction")
        tasks.add(eviction_task)
    revocation_task = None
    if still_valid is not None:
        async def watch_credentials() -> None:
            while await still_valid():
                await asyncio.sleep(5.0)

        revocation_task = asyncio.create_task(watch_credentials(), name="pi-gateway-revocation")
        tasks.add(revocation_task)
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    for task in done:
        if task is not eviction_task:
            task.result()
    if revocation_task in done:
        return "revoked"
    return "replaced" if eviction_task in done else "completed"


def origin_session_of(principal: Principal) -> str | None:
    """Internal Pi credentials attribute work to their session, never payloads."""
    return principal.pi_session.id if principal.pi_session is not None else None


async def api_gate(request: HTTPConnection) -> None:
    """Global HTTP authentication and capability boundary.

    Login, health and static assets are public. WebSockets authenticate in
    their handlers (before attachment), never through this dependency.
    """
    if request.scope["type"] == "websocket":
        return
    path = request.url.path
    if not path.startswith("/api/") or path == "/api/v1/auth/login":
        return
    principal = await require_principal(request)
    enforce_browser_mutation(request, principal, request.app.state.web_origin)
    enforce_capability(principal, request.scope.get("method", "GET"), path)


def pi_access(*roles: str):
    """Pi workflow access: the listed internal roles, or the bootstrap browser user."""

    async def resolve(request: HTTPConnection) -> Principal:
        principal = await require_principal(request)
        if principal.credential_kind == "pi_session":
            if principal.pi_session.role not in roles:
                raise HTTPException(status_code=403, detail="caller role is not allowed")
        elif not principal.is_bootstrap_web:
            raise HTTPException(status_code=403, detail="Pi workflow belongs to the bootstrap administrator")
        return principal

    return resolve


def pi_role(*roles: str):
    """Internal-session-only Pi access for the listed roles."""

    async def resolve(request: HTTPConnection) -> Principal:
        principal = await require_principal(request)
        if principal.credential_kind != "pi_session" or principal.pi_session.role not in roles:
            raise HTTPException(status_code=403, detail="caller role is not allowed")
        return principal

    return resolve


def create_app(db: Database) -> FastAPI:
    """Create the authenticated control API (kept as the public test factory)."""
    app = FastAPI(
        title="Worker Harness Control API", lifespan=lifespan,
        dependencies=[Depends(api_gate)],
    )
    app.state.db = db
    app.state.web_origin = load_web_origin()
    app.state.passwords = PasswordService()
    app.state.login_limiter = LoginRateLimiter()
    app.state.proxy_trust = load_proxy_trust()
    app.state.machines = load_machines()
    app.state.projects = load_projects()
    app.state.orchestrator = Orchestrator(
        db,
        app.state.machines,
        app.state.projects,
    )

    async def on_job_terminal(job: Job) -> None:
        # An owner stopping a managed notebook's job cleans the session up.
        session = await db.get_marimo_session_for_job(job.id)
        if session is not None:
            await release_marimo_resources(session)

    jm = JobManager(db, on_job_terminal=on_job_terminal)
    app.state.job_manager = jm

    # Shared reliability services. They are attached before lifespan starts so
    # handlers, reaper, and /api/v1/_stats all see one coherent state.
    app.state.cache = TTLCache()
    app.state.lanes = WorkerLanes(max_concurrent=4, max_queue=32)
    app.state.rate_limiter = AgentRateLimiter(capacity=50, refill_rate=1.0)
    app.state.metrics = Metrics()
    app.state.tunnels = TunnelRegistry()
    app.state.pi_gateway_lock = asyncio.Lock()
    app.state.pi_gateway_attachments = {}
    app.state.pi_gateway_refused_total = 0
    app.state.pi_gateway_evictions_total = 0
    app.state.pi_gateway_close_reasons = Counter()
    app.state.pi_gateway_max_per_session = _positive_int_env(
        "WH_PI_MAX_ATTACHMENTS", 8
    )
    set_global_metrics(app.state.metrics)
    set_lanes(app.state.lanes)

    @app.get("/health")
    async def health():
        """Public control-plane liveness endpoint."""
        return {"status": "healthy", "ts": datetime.now(timezone.utc).isoformat()}

    @app.exception_handler(RequestValidationError)
    async def validation_handler(_request: Request, exc: RequestValidationError):
        # Never echo submitted values (passwords, keys, commands).
        errors = [
            {"loc": list(error.get("loc", ())), "msg": error.get("msg", ""), "type": error.get("type", "")}
            for error in exc.errors()
        ]
        return JSONResponse(status_code=422, content={"detail": errors}, headers={"Cache-Control": "no-store"})

    @app.middleware("http")
    async def no_store_middleware(request: Request, call_next):
        response = await call_next(request)
        if request.url.path.startswith("/api/"):
            response.headers["Cache-Control"] = "no-store"
        return response

    @app.middleware("http")
    async def metrics_middleware(request: Request, call_next):
        metrics = app.state.metrics
        metrics.requests_total.inc()
        metrics.in_flight_requests.inc()
        try:
            return await call_next(request)
        finally:
            metrics.in_flight_requests.dec()

    @app.middleware("http")
    async def rate_limit_middleware(request: Request, call_next):
        # Bridge registration, lifecycle uploads, transcript batches, and
        # command long-polls are infrastructure traffic. Several Pi sessions
        # legitimately share one Tailnet source IP, and one streaming turn can
        # upload more than the operator API's 60 req/min budget. Bridge routes
        # validate the active session incarnation instead of consuming the
        # peer-IP agent bucket; expensive operator routes remain rate-limited.
        # Browsers all reach the control API through one web edge, so the
        # bucket is keyed by the presented credential, never the shared peer
        # address. Cheap database-only reads (the board, lists, usage) that
        # the UI polls are not throttled; SSH-backed reads and every mutation
        # still are.
        path = request.url.path
        exempt = (
            path.startswith("/api/v1/pi/bridge/")
            or MARIMO_PROXY_PATH.match(path)
            or (request.method in ("GET", "HEAD") and not SSH_BACKED_READ.match(path))
        )
        if path.startswith("/api/v1/") and not exempt:
            # A presented credential always selects the bucket: a caller-chosen
            # X-Agent-Name must not mint fresh buckets for an authenticated client.
            credential = request.headers.get("authorization") or request.cookies.get(SESSION_COOKIE)
            if credential:
                agent = "credential:" + hashlib.sha256(credential.encode()).hexdigest()[:16]
            else:
                peer_ip = request.client.host if request.client else "unknown"
                agent = resolve_agent_name(dict(request.headers), peer_ip)
            try:
                app.state.rate_limiter.check(agent)
            except RateLimited as e:
                return JSONResponse(
                    status_code=429,
                    content={"error": {"code": "RATE_LIMITED", "message": str(e)}},
                    headers={"Retry-After": str(max(1, int(e.retry_after_seconds + 0.999)))},
                )
        return await call_next(request)

    @app.exception_handler(LaneTimeout)
    async def lane_timeout_handler(_request: Request, exc: LaneTimeout):
        return JSONResponse(
            status_code=503,
            content={"error": {"code": "WORKER_LANE_BUSY", "message": str(exc), "worker_id": exc.worker_id}},
            headers={"Retry-After": "2"},
        )

    async def cached(key: str, ttl_seconds: float, loader: Callable[[], Awaitable[T]]) -> T:
        """Best-effort cache helper for read-heavy, eventually-consistent routes."""
        value = await app.state.cache.get(key)
        if value is not None:
            return value
        value = await loader()
        await app.state.cache.set(key, value, ttl_seconds)
        return value

    async def invalidate_workers_cache() -> None:
        await app.state.cache.invalidate("workers:public")
        await app.state.cache.invalidate("workers:summary")
        await app.state.cache.invalidate("data:paths:False")
        await app.state.cache.invalidate("data:paths:True")

    async def resolve_worker(worker_id_or_name: str):
        worker = await db.get_worker(worker_id_or_name)
        if worker:
            return worker
        workers = await db.list_workers()
        # Case-insensitive name match: worker hostnames register in varying cases
        # (e.g. "KW60898" from the OS hostname vs "kw60898" from DNS) and callers
        # naturally pass the lowercase form. Matching case-insensitively avoids
        # spurious 404s that only affect single-worker/exec routes.
        return next(
            (w for w in workers if w.name.lower() == worker_id_or_name.lower()),
            None,
        )


    # ── Orchestration API (/api/v1) ──────────────────────────────────────────

    @app.get("/api/v1/workers")
    async def workers_list():
        async def load():
            return [public_worker(worker) for worker in await db.list_workers()]
        return await cached("workers:public", 5.0, load)

    @app.get("/api/v1/workers/summary")
    async def workers_summary():
        async def load():
            workers = await db.list_workers()
            status_counts = Counter(w.status.value for w in workers)
            return {
                "total": len(workers),
                "online": status_counts.get("online", 0),
                "offline": status_counts.get("offline", 0),
                "draining": status_counts.get("draining", 0),
            }
        return await cached("workers:summary", 2.0, load)

    @app.delete("/api/v1/workers/prune")
    async def workers_prune(
        minutes: int = Query(5, ge=0),
        _principal: Principal = Depends(require_bootstrap_web_user),
    ):
        cutoff = int(time.time()) - (minutes * 60)
        removed = await db.prune_workers(cutoff)
        await invalidate_workers_cache()
        return {"removed": removed, "minutes": minutes}

    @app.post("/api/v1/pi/bridge/register")
    async def pi_bridge_register(
        payload: PiBridgeRegister,
        principal: Principal = Depends(pi_access("orchestrator", "pm", "task")),
    ):
        caller = principal.pi_session
        if caller is not None:
            # Internal sessions only register their own, already-owned row.
            if caller.id != payload.session_id:
                raise HTTPException(status_code=403, detail="session token does not match bridge session")
            owner_user_id = caller.owner_user_id
        else:
            owner_user_id = principal.user_id
        if payload.resume_path and caller:
            machine = app.state.machines.get(caller.meta.get("machine") or caller.host)
            path = PurePosixPath(payload.resume_path)
            if (
                machine is None or ".." in path.parts or path.suffix != ".jsonl"
                or not path.is_relative_to(PurePosixPath(machine.home) / ".omp/agent/sessions")
            ):
                raise HTTPException(status_code=403, detail="transcript path is outside the agent session directory")
        now = int(datetime.now(timezone.utc).timestamp())
        try:
            session = await db.register_interactive_pi_session(
                payload, owner_user_id=owner_user_id, now=now,
            )
        except (ValueError, PermissionError) as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        await db.insert_pi_session_event(PiSessionEvent(
            session_id=session.id,
            event_type="bridge-registered",
            payload={"incarnation": payload.incarnation, "host": payload.host},
            created_at=now,
        ))
        return session.model_dump(mode="json")

    @app.post("/api/v1/pi/bridge/{session_id}/events")
    async def pi_bridge_events(
        session_id: str,
        payload: PiBridgeEventBatch,
        principal: Principal = Depends(pi_access("orchestrator", "pm", "task")),
    ):
        caller = principal.pi_session
        if caller and caller.id != session_id:
            raise HTTPException(status_code=403, detail="session token does not match bridge session")
        try:
            session, persisted = await db.apply_interactive_pi_events(session_id, payload)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="interactive Pi session not found") from exc
        except PermissionError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {
            "session_id": session.id,
            "state": session.state.value,
            "events_persisted": len(persisted),
        }

    @app.get("/api/v1/pi/bridge/{session_id}/commands")
    async def pi_bridge_commands(
        session_id: str,
        incarnation: str,
        wait_seconds: float = Query(20.0, ge=0.0, le=30.0),
        principal: Principal = Depends(pi_access("orchestrator", "pm", "task")),
    ):
        caller = principal.pi_session
        if caller and caller.id != session_id:
            raise HTTPException(status_code=403, detail="session token does not match bridge session")
        deadline = asyncio.get_running_loop().time() + wait_seconds
        while True:
            try:
                commands = await db.claim_pi_session_commands(session_id, incarnation)
            except KeyError as exc:
                raise HTTPException(status_code=404, detail="interactive Pi session not found") from exc
            except PermissionError as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from exc
            if commands or asyncio.get_running_loop().time() >= deadline:
                return [command.model_dump(mode="json") for command in commands]
            await asyncio.sleep(min(0.5, max(0.01, deadline - asyncio.get_running_loop().time())))

    @app.post("/api/v1/pi/bridge/{session_id}/commands/{command_id}:ack")
    async def pi_bridge_command_ack(
        session_id: str,
        command_id: str,
        payload: PiCommandAck,
        principal: Principal = Depends(pi_access("orchestrator", "pm", "task")),
    ):
        caller = principal.pi_session
        if caller and caller.id != session_id:
            raise HTTPException(status_code=403, detail="session token does not match bridge session")
        try:
            acknowledged = await db.ack_pi_session_command(session_id, command_id, payload.incarnation)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="interactive Pi session not found") from exc
        except PermissionError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        if not acknowledged:
            raise HTTPException(status_code=404, detail="pending command not found")
        return {"acknowledged": True, "command_id": command_id}

    async def enqueue_prompt(
        target: PiSession,
        message: str,
        *,
        deliver_as: str,
        caller: PiSession | None,
    ) -> PiSessionCommand:
        message = message.strip()
        if not message:
            raise HTTPException(status_code=422, detail="message must not be empty")
        now = int(datetime.now(timezone.utc).timestamp())
        command = PiSessionCommand(
            session_id=target.id,
            kind="prompt",
            message=message,
            deliver_as=deliver_as,
            payload={"sender_session_id": caller.id if caller else ""},
            created_at=now,
        )
        await db.enqueue_pi_session_command(command)
        await db.insert_pi_session_event(PiSessionEvent(
            session_id=target.id,
            event_type="prompt-queued",
            payload={
                "command_id": command.id,
                "deliver_as": deliver_as,
                "sender_session_id": caller.id if caller else "",
            },
            created_at=now,
        ))
        return command

    @app.get("/api/v1/pi/orchestrator")
    async def pi_orchestrator_get(
        _principal: Principal = Depends(pi_access("orchestrator", "pm")),
    ):
        sessions = await db.list_pi_sessions_by_role("orchestrator")
        for session in sessions:
            if session.state not in {PiSessionState.STOPPED, PiSessionState.FAILED}:
                return session.model_dump(mode="json")
        raise HTTPException(status_code=404, detail="no live orchestrator session")

    @app.post("/api/v1/pi/orchestrator:send")
    async def pi_orchestrator_send(
        payload: PiMessageRequest,
        principal: Principal = Depends(pi_access("pm")),
    ):
        caller = principal.pi_session
        try:
            session = await app.state.orchestrator.ensure_orchestrator()
        except Exception as exc:
            log.error("Could not launch orchestrator session: %s", exc)
            raise HTTPException(
                status_code=503,
                detail=f"could not launch orchestrator session: {exc}",
            ) from exc
        command = await enqueue_prompt(
            session,
            payload.message,
            deliver_as="steer",
            caller=caller,
        )
        return {
            "session": session.model_dump(mode="json"),
            "command_id": command.id,
        }

    @app.get("/api/v1/pi/sessions")
    async def pi_sessions_list(
        request: Request,
        worker_id: str | None = None,
        include_attach_info: bool = False,
        principal: Principal = Depends(pi_access("orchestrator", "pm")),
    ):
        caller = principal.pi_session
        if include_attach_info and caller:
            raise HTTPException(status_code=403, detail="terminal attachment is operator-only")
        sessions = await db.list_pi_sessions(worker_id)
        if caller and caller.role == "pm":
            sessions = [
                session
                for session in sessions
                if (
                    session.id == caller.id
                    or (
                        session.role == "task"
                        and session.parent_session_id == caller.id
                        and bool(caller.meta.get("project"))
                        and session.meta.get("project") == caller.meta.get("project")
                    )
                    or session.role == "orchestrator"
                )
            ]
        if not include_attach_info:
            return [session.model_dump(mode="json") for session in sessions]
        return [
            {
                **session.model_dump(mode="json"),
                "attach_info": attach_info_with_gateway(session, request),
            }
            for session in sessions
        ]

    @app.post("/api/v1/pi/sessions/{session_id}:send")
    async def pi_session_send(
        session_id: str,
        payload: PiMessageRequest,
        principal: Principal = Depends(pi_access("orchestrator", "pm")),
    ):
        caller = principal.pi_session
        target = await db.get_pi_session(session_id)
        if target is None:
            raise HTTPException(status_code=404, detail="Pi session not found")
        if caller and caller.role == "orchestrator":
            if target.role != "pm":
                raise HTTPException(status_code=403, detail="orchestrator may only message project managers")
        elif caller and caller.role == "pm":
            is_own_task = target.role == "task" and target.parent_session_id == caller.id
            is_orchestrator = target.role == "orchestrator"
            if not is_own_task and not is_orchestrator:
                raise HTTPException(
                    status_code=403,
                    detail="project manager may only message its own tasks or the orchestrator",
                )
            if is_own_task and (
                not caller.meta.get("project")
                or target.meta.get("project") != caller.meta.get("project")
            ):
                raise HTTPException(status_code=403, detail="cross-project target")
        if target.role == "pm":
            async with app.state.orchestrator.pm_session(target.meta["project"]) as manager:
                command = await enqueue_prompt(
                    manager, payload.message, deliver_as="steer", caller=caller,
                )
        else:
            command = await enqueue_prompt(
                target, payload.message, deliver_as="steer", caller=caller,
            )
        return {"command_id": command.id, "queued": True}

    @app.post("/api/v1/pi/sessions/{session_id}:interrupt")
    async def pi_session_interrupt(
        session_id: str,
        principal: Principal = Depends(pi_access("orchestrator", "pm")),
    ):
        caller = principal.pi_session
        target = await db.get_pi_session(session_id)
        if target is None:
            raise HTTPException(status_code=404, detail="Pi session not found")
        if caller and caller.role == "orchestrator" and target.role != "pm":
            raise HTTPException(
                status_code=403,
                detail="orchestrator may only interrupt project managers",
            )
        if caller and caller.role == "pm":
            if target.role != "task" or target.parent_session_id != caller.id:
                raise HTTPException(
                    status_code=403,
                    detail="project manager may only interrupt its own tasks",
                )
            if not caller.meta.get("project") or target.meta.get("project") != caller.meta.get("project"):
                raise HTTPException(status_code=403, detail="cross-project target")
        if (
            not target.session_type.bridge_backed
            or not target.bridge_incarnation
            or target.state not in {PiSessionState.WORKING, PiSessionState.IDLE, PiSessionState.BLOCKED}
        ):
            raise HTTPException(status_code=409, detail="interactive Pi bridge is not active")
        now = int(datetime.now(timezone.utc).timestamp())
        command = PiSessionCommand(
            session_id=target.id,
            kind="interrupt",
            created_at=now,
        )
        await db.enqueue_pi_session_command(command)
        await db.insert_pi_session_event(PiSessionEvent(
            session_id=target.id,
            event_type="interrupt-queued",
            payload={
                "command_id": command.id,
                "sender_session_id": caller.id if caller else "",
            },
            created_at=now,
        ))
        return {"command_id": command.id, "queued": True}

    @app.post("/api/v1/pi/sessions/{session_id}:configure")
    async def pi_session_configure(
        session_id: str,
        payload: PiConfigureRequest,
        _principal: Principal = Depends(pi_access()),
    ):
        target = await db.get_pi_session(session_id)
        if (
            target is None
            or not target.session_type.bridge_backed
            or not target.bridge_incarnation
            or target.state not in {PiSessionState.WORKING, PiSessionState.IDLE, PiSessionState.BLOCKED}
        ):
            raise HTTPException(status_code=409, detail="interactive Pi bridge is not active")
        if bool(payload.provider) != bool(payload.model):
            raise HTTPException(status_code=422, detail="provider and model must be set together")
        if not payload.provider and not payload.thinking_level:
            raise HTTPException(status_code=422, detail="model or thinking_level is required")
        for value, label in ((payload.provider, "provider"), (payload.model, "model")):
            if value and (len(value) > 256 or not value.strip()):
                raise HTTPException(status_code=422, detail=f"invalid {label}")
        command_payload = {
            **(
                {
                    "provider": payload.provider.strip(),
                    "model": payload.model.strip(),
                }
                if payload.provider and payload.model
                else {}
            ),
            **(
                {"thinking_level": payload.thinking_level}
                if payload.thinking_level
                else {}
            ),
        }
        now = int(datetime.now(timezone.utc).timestamp())
        command = PiSessionCommand(
            session_id=target.id,
            kind="configure",
            payload=command_payload,
            created_at=now,
        )
        await db.enqueue_pi_session_command(command)
        await db.insert_pi_session_event(PiSessionEvent(
            session_id=target.id,
            event_type="configure-queued",
            payload={"command_id": command.id, **command_payload},
            created_at=now,
        ))
        return {"command_id": command.id, "queued": True}

    @app.post("/api/v1/pi/sessions/{session_id}:ask-pm")
    async def pi_session_ask_pm(
        session_id: str,
        payload: PiQuestionRequest,
        principal: Principal = Depends(pi_role("task")),
    ):
        caller = principal.pi_session
        if caller.id != session_id:
            raise HTTPException(status_code=403, detail="task may only block its own session")
        parent = await db.get_pi_session(caller.parent_session_id or "")
        if parent is None or parent.role != "pm":
            raise HTTPException(status_code=403, detail="task has no project manager")
        if not caller.meta.get("project") or parent.meta.get("project") != caller.meta.get("project"):
            raise HTTPException(status_code=403, detail="cross-project parent")
        question = payload.question.strip()
        if not question:
            raise HTTPException(status_code=422, detail="question must not be empty")
        try:
            await db.set_pi_session_blocked(caller.id, question)
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        command = await enqueue_prompt(
            parent,
            f"Task {caller.id} asks: {question}",
            deliver_as="followUp",
            caller=caller,
        )
        return {"command_id": command.id, "state": PiSessionState.BLOCKED.value}

    @app.post("/api/v1/pi/sessions/{session_id}:notify-pm")
    async def pi_session_notify_pm(
        session_id: str,
        payload: PiNoteRequest,
        principal: Principal = Depends(pi_role("task")),
    ):
        caller = principal.pi_session
        if caller.id != session_id:
            raise HTTPException(status_code=403, detail="task may only notify from its own session")
        parent = await db.get_pi_session(caller.parent_session_id or "")
        if parent is None or parent.role != "pm":
            raise HTTPException(status_code=403, detail="task has no project manager")
        if not caller.meta.get("project") or parent.meta.get("project") != caller.meta.get("project"):
            raise HTTPException(status_code=403, detail="cross-project parent")
        note = payload.note.strip()
        if not note:
            raise HTTPException(status_code=422, detail="note must not be empty")
        command = await enqueue_prompt(
            parent,
            f"Task {caller.id} reports: {note}",
            deliver_as="followUp",
            caller=caller,
        )
        return {"command_id": command.id, "queued": True}

    @app.get("/api/v1/pi/projects")
    async def pi_projects_list(
        _principal: Principal = Depends(pi_access("orchestrator")),
    ):
        return [
            {
                "name": project.name,
                "machine": project.machine,
                "repo": project.repo,
                "remote": project.remote,
                "base_branch": project.base_branch,
            }
            for project in app.state.projects.values()
        ]

    @app.post("/api/v1/pi/projects/{project}:send")
    async def pi_project_send(
        project: str,
        payload: PiMessageRequest,
        principal: Principal = Depends(pi_access("orchestrator")),
    ):
        caller = principal.pi_session
        if project not in app.state.projects:
            raise HTTPException(status_code=404, detail="project not found")
        try:
            async with app.state.orchestrator.pm_session(project) as manager:
                command = await enqueue_prompt(
                    manager, payload.message, deliver_as="steer", caller=caller,
                )
        except Exception as exc:
            log.error("Could not launch project manager for %s: %s", project, exc)
            raise HTTPException(
                status_code=503,
                detail=f"could not launch project manager for {project}: {exc}",
            ) from exc
        return {
            "session": manager.model_dump(mode="json"),
            "command_id": command.id,
            "queued": True,
        }

    @app.post("/api/v1/pi/projects/{project}/tasks", status_code=201)
    async def pi_project_task_create(
        project: str,
        payload: PiTaskLaunchRequest,
        principal: Principal = Depends(pi_role("pm")),
    ):
        caller = principal.pi_session
        if caller.meta.get("project") != project:
            raise HTTPException(status_code=403, detail="cross-project task launch")
        project_config = app.state.projects.get(project)
        if project_config is None:
            raise HTTPException(status_code=404, detail="project not found")
        branch = payload.branch.strip()
        briefing = payload.briefing.strip()
        if not branch or not briefing:
            raise HTTPException(status_code=422, detail="branch and briefing must not be empty")
        session = await app.state.orchestrator.launch_task(
            project_config,
            branch=branch,
            briefing=briefing,
            parent_session_id=caller.id,
        )
        return session.model_dump(mode="json")

    def own_task(caller: PiSession, task: PiSession) -> None:
        if task.role != "task" or task.parent_session_id != caller.id:
            raise HTTPException(status_code=403, detail="project manager does not own task")
        if not caller.meta.get("project") or task.meta.get("project") != caller.meta.get("project"):
            raise HTTPException(status_code=403, detail="cross-project task")

    @app.post("/api/v1/pi/sessions/{session_id}:teardown")
    async def pi_session_teardown(
        session_id: str,
        payload: PiTeardownRequest,
        principal: Principal = Depends(pi_role("pm")),
    ):
        caller = principal.pi_session
        task = await db.get_pi_session(session_id)
        if task is None:
            raise HTTPException(status_code=404, detail="Pi session not found")
        own_task(caller, task)
        try:
            await app.state.orchestrator.teardown_task(task, force=payload.force)
        except WorktreeError as exc:
            # A refusal is a policy answer the PM must read, not a server fault:
            # tearing down a worktree with unpushed commits destroys the work.
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {"session_id": task.id, "torn_down": True}

    @app.post("/api/v1/pi/sessions/{session_id}:submit-pr")
    async def pi_session_submit_pr(
        session_id: str,
        payload: PiSubmitPrRequest,
        principal: Principal = Depends(pi_role("pm")),
    ):
        caller = principal.pi_session
        task = await db.get_pi_session(session_id)
        if task is None:
            raise HTTPException(status_code=404, detail="Pi session not found")
        own_task(caller, task)
        project_name = str(task.meta.get("project") or "")
        project = app.state.projects.get(project_name)
        machine_name = str(task.meta.get("machine") or (project.machine if project else ""))
        machine = app.state.machines.get(machine_name)
        if project is None or machine is None:
            raise HTTPException(status_code=409, detail="task launch metadata is incomplete")
        try:
            url = await open_pr(
                machine,
                project,
                branch=str(task.meta.get("branch") or ""),
                worktree=str(task.meta.get("worktree") or ""),
                summary=payload.summary,
                session_id=task.id,
            )
        except PrRejected as exc:
            raise HTTPException(
                status_code=422,
                detail={"message": str(exc), "missing": exc.missing},
            ) from exc
        await db.update_pi_session_meta(task.id, {"pr_url": url})
        return {"session_id": task.id, "pr_url": url}

    async def readable_session(
        session_id: str,
        principal: Principal = Depends(pi_access("orchestrator", "pm", "task")),
    ) -> PiSession:
        caller = principal.pi_session
        session = await db.get_pi_session(session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Pi session not found")
        if caller and caller.role == "task" and session.id != caller.id:
            raise HTTPException(status_code=403, detail="task may only read its own session")
        if caller and caller.role == "pm" and session.id != caller.id and session.role != "orchestrator":
            own_task(caller, session)
        return session

    @app.get("/api/v1/pi/sessions/{session_id}")
    async def pi_session_get(session: PiSession = Depends(readable_session)):
        return session.model_dump(mode="json")

    def build_pi_attach_info(session: PiSession) -> dict[str, Any]:
        if session.state not in {PiSessionState.WORKING, PiSessionState.IDLE, PiSessionState.BLOCKED}:
            return {
                "session_id": session.id,
                "attachable": False,
                "reason": f"Session is {session.state.value}",
            }
        if not session.session_type.bridge_backed:
            return {
                "session_id": session.id,
                "attachable": False,
                "reason": "Session has no terminal transport",
            }
        if not session.terminal_attachable or not session.terminal_host or not session.terminal_port:
            return {
                "session_id": session.id,
                "attachable": False,
                "reason": "Interactive session host relay is unavailable",
            }
        direct_url = (
            f"ws://{session.terminal_host}:{session.terminal_port}"
            f"/v1/sessions/{quote(session.id, safe='')}/attach"
        )
        return {
            "session_id": session.id,
            "attachable": True,
            "transport": "direct-interactive-websocket",
            "protocol_version": session.terminal_protocol_version,
            "websocket_url": direct_url,
            "direct_websocket_url": direct_url,
        }

    def attach_info_with_gateway(
        session: PiSession,
        request: Request,
    ) -> dict[str, Any]:
        info = build_pi_attach_info(session)
        if info.get("attachable"):
            forwarded_proto = request.headers.get("x-forwarded-proto", "").split(",", 1)[0].strip()
            scheme = "wss" if (forwarded_proto or request.url.scheme) == "https" else "ws"
            info["gateway_websocket_url"] = (
                f"{scheme}://{request.url.netloc}/api/v1/pi/sessions/"
                f"{quote(session.id, safe='')}/attach-gateway"
            )
        return info

    async def resolve_pi_attach_info(session_id: str) -> dict[str, Any]:
        session = await db.get_pi_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Pi session not found")
        return build_pi_attach_info(session)

    @app.get("/api/v1/pi/sessions/{session_id}/attach-info")
    async def pi_session_attach_info(
        session_id: str, request: Request,
        _principal: Principal = Depends(pi_access()),
        session: PiSession = Depends(readable_session),
    ):
        return attach_info_with_gateway(session, request)

    @app.websocket("/api/v1/pi/sessions/{session_id}/attach-gateway")
    async def pi_session_attach_gateway(websocket: WebSocket, session_id: str) -> None:
        await websocket.accept()
        try:
            principal = await resolve_principal(websocket)
            if not principal.is_bootstrap_web:
                raise HTTPException(status_code=403, detail="terminal attachment is bootstrap-only")
            if not origin_matches(websocket, app.state.web_origin):
                raise HTTPException(status_code=403, detail="origin not allowed")
        except HTTPException as exc:
            await websocket.close(code=4400 + exc.status_code % 100, reason=str(exc.detail))
            return
        try:
            info = await resolve_pi_attach_info(session_id)
        except HTTPException as exc:
            await websocket.send_json({"type": "error", "code": "session_not_found", "detail": exc.detail})
            await websocket.close(code=4404, reason="session not found")
            return
        if not info.get("attachable"):
            await websocket.send_json({"type": "error", "code": "session_not_attachable", "detail": info.get("reason")})
            await websocket.close(code=4409, reason=str(info.get("reason") or "session not attachable")[:120])
            return

        rows = _terminal_dimension(websocket.query_params.get("rows"), 24)
        cols = _terminal_dimension(websocket.query_params.get("cols"), 80)
        attachment = _GatewayAttachment(
            id=str(uuid4()),
            client=websocket,
            last_activity=asyncio.get_running_loop().time(),
            rows=rows,
            cols=cols,
        )
        victim = await _reserve_gateway_attachment(app, session_id, attachment)
        if victim is not None:
            try:
                await asyncio.wait_for(victim.released.wait(), timeout=3.0)
            except asyncio.TimeoutError:
                log.warning("timed out waiting for evicted Pi gateway attachment cleanup")

        upstream_url = _terminal_url_with_dimensions(info["direct_websocket_url"], rows, cols)
        close_reason = "completed"
        try:
            if attachment.evict.is_set():
                close_reason = "replaced"
                await websocket.send_json({
                    "type": "status",
                    "state": "replaced",
                    "reason": "attachment capacity reclaimed by a newer client",
                })
                await asyncio.sleep(0)
                await websocket.close(code=4410, reason="replaced by newer attachment")
                return
            async with websocket_connect(
                upstream_url,
                max_size=None,
                max_queue=4,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=2,
            ) as upstream:
                outcome = await _pump_terminal_gateway(
                    websocket, upstream, attachment=attachment,
                    still_valid=lambda: principal_still_valid(db, principal),
                )
                if outcome == "revoked":
                    close_reason = "revoked"
                    await websocket.close(code=4401, reason="credentials revoked")
                if outcome == "replaced":
                    close_reason = "replaced"
                    await websocket.send_json({
                        "type": "status",
                        "state": "replaced",
                        "reason": "attachment capacity reclaimed by a newer client",
                    })
                    await asyncio.sleep(0)
                    await websocket.close(code=4410, reason="replaced by newer attachment")
        except ConnectionClosed as exc:
            close_reason = f"upstream-{exc.code}"
            try:
                await websocket.close(
                    code=exc.code if 1000 <= exc.code <= 4999 else 1011,
                    reason=(exc.reason or "upstream terminal closed")[:120],
                )
            except RuntimeError:
                pass
        except (OSError, WebSocketException, asyncio.TimeoutError, ValueError) as exc:
            close_reason = "upstream-unavailable"
            try:
                await websocket.send_json({
                    "type": "error",
                    "code": "upstream_unavailable",
                    "detail": str(exc),
                })
                await websocket.close(code=1011, reason="upstream terminal unavailable")
            except RuntimeError:
                pass
        finally:
            async with app.state.pi_gateway_lock:
                app.state.pi_gateway_close_reasons[close_reason] += 1
            await _release_gateway_attachment(app, session_id, attachment)

    @app.get("/api/v1/pi/sessions/{session_id}/events")
    async def pi_session_events(
        session_id: str,
        after: int = Query(0, ge=0),
        limit: int = Query(500, ge=1, le=1000),
        _session: PiSession = Depends(readable_session),
    ):
        # Session ownership is checked before exposing the transcript.
        events = await db.list_pi_session_events(session_id, after=after, limit=limit)
        return [event.model_dump(mode="json") for event in events]

    @app.get("/api/v1/pi/sessions/{session_id}/stream")
    async def pi_session_stream(
        request: Request,
        session_id: str,
        after: int = Query(0, ge=0),
        _session: PiSession = Depends(readable_session),
    ):
        # Authorize before opening an indefinitely streaming response.
        last_event_id = request.headers.get("last-event-id", "")
        if last_event_id.isdigit():
            after = max(after, int(last_event_id))
        return StreamingResponse(
            stream_pi_session_events(
                request, db, session_id, after,
                still_valid=lambda: principal_still_valid(db, request.state.principal),
            ),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache, no-transform",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )


    @app.get("/api/v1/data")
    async def data_list(include_offline: bool = False):
        async def load():
            return reverse_data_paths(
                await db.list_workers(), include_offline=include_offline
            )
        return await cached(f"data:paths:{include_offline}", 2.0, load)

    @app.post("/api/v1/data/copy")
    async def data_copy(payload: DataCopyRequest, request: Request):
        principal: Principal = request.state.principal
        try:
            src_path = validate_data_path(payload.src_path)
            dst_path = validate_data_path(payload.dst_path)
        except DataPathError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        if not 60 <= payload.ttl_seconds <= 24 * 60 * 60:
            raise HTTPException(status_code=400, detail="ttl_seconds must be between 60 and 86400")

        source = await resolve_worker(payload.src_worker)
        destination = await resolve_worker(payload.dst_worker)
        if not source:
            raise HTTPException(status_code=404, detail=f"Source worker not found: {payload.src_worker}")
        if not destination:
            raise HTTPException(status_code=404, detail=f"Destination worker not found: {payload.dst_worker}")
        if source.id == destination.id:
            raise HTTPException(status_code=400, detail="source and destination workers must differ")
        if source.status.value != "online" or destination.status.value != "online":
            raise HTTPException(status_code=409, detail="source and destination workers must be online")
        if not is_advertised_data_path(src_path, source.data_paths):
            raise HTTPException(
                status_code=400,
                detail="source path is outside the source worker's advertised data directories",
            )

        transfer_id = str(uuid4())
        exported = await async_ssh_run(
            source,
            with_worker_dir(source, source_export_command(src_path, transfer_id, payload.ttl_seconds)),
            timeout=30,
        )
        if exported.returncode != 0:
            raise HTTPException(status_code=502, detail=f"source export failed: {exported.stderr or 'unknown error'}")
        try:
            endpoint = json.loads(exported.stdout.strip().splitlines()[-1])
            port = int(endpoint["port"])
            username = str(endpoint["username"])
            password = str(endpoint["password"])
            if not 22000 <= port <= 22999 or not username or not password:
                raise ValueError("invalid endpoint")
        except (IndexError, KeyError, TypeError, ValueError, json.JSONDecodeError):
            await async_ssh_run(source, with_worker_dir(source, source_cleanup_command(transfer_id)), timeout=15)
            raise HTTPException(status_code=502, detail="source export returned invalid metadata")

        secret_path = f"{destination.harness_dir.rstrip('/')}/data-transfer-{transfer_id}.secret"
        uploaded = await ssh_upload_bytes(destination, password.encode(), secret_path)
        if uploaded.returncode != 0:
            await async_ssh_run(source, with_worker_dir(source, source_cleanup_command(transfer_id)), timeout=15)
            raise HTTPException(status_code=502, detail=f"destination credential upload failed: {uploaded.stderr or 'unknown error'}")

        command = with_worker_dir(
            destination,
            destination_copy_command(source.ssh_host, port, dst_path, username, secret_path),
        )
        try:
            job = await jm.start_job(
                destination, command, owner_user_id=principal.user_id,
                actor_id=principal.actor_id, origin_session_id=origin_session_of(principal),
                pty_enabled=False,
            )
        except Exception as exc:
            await async_ssh_run(source, with_worker_dir(source, source_cleanup_command(transfer_id)), timeout=15)
            raise HTTPException(status_code=502, detail=f"destination copy job failed to start: {exc}")

        async def cleanup_after_copy() -> None:
            """End the source export promptly; TTL is the crash-safe fallback."""
            deadline = asyncio.get_running_loop().time() + payload.ttl_seconds
            try:
                current = job
                while asyncio.get_running_loop().time() < deadline:
                    current = await jm.refresh_job_status(destination, current)
                    if current.status in (JobStatus.DONE, JobStatus.FAILED):
                        break
                    await asyncio.sleep(5)
            except Exception:
                log.exception("Could not monitor data copy %s", transfer_id)
            finally:
                await async_ssh_run(
                    source,
                    with_worker_dir(source, source_cleanup_command(transfer_id)),
                    timeout=15,
                )

        asyncio.create_task(cleanup_after_copy(), name=f"data-copy-cleanup-{transfer_id}")
        return {
            "transfer_id": transfer_id,
            "job_id": job.id,
            "source_worker": source.id,
            "source_path": src_path,
            "destination_worker": destination.id,
            "destination_path": dst_path,
            "expires_in_seconds": payload.ttl_seconds,
        }

    @app.get("/api/v1/workers/{worker_id}")
    async def workers_get(worker_id: str):
        worker = await resolve_worker(worker_id)
        if not worker:
            raise HTTPException(status_code=404, detail=f"Worker not found: {worker_id}")
        return public_worker(worker)

    # ── Jobs: owner-private details, shared public board ────────────────────

    def conflict(code: str, message: str, status_code: int = 409) -> HTTPException:
        return HTTPException(status_code=status_code, detail={"code": code, "message": message})

    async def owned_job_or_404(principal: Principal, job_id: str) -> Job:
        job = await db.get_job(job_id)
        # Another owner's private job is indistinguishable from a missing one.
        if job is None or job.owner_user_id != principal.user_id:
            raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")
        return job

    async def owned_job_dto(job: Job, *, with_history: bool = False) -> dict:
        names = await db.user_display_names({job.owner_user_id})
        worker = await db.get_worker(job.worker_id or "")
        item = owned_job(
            job, owner_display_name=names.get(job.owner_user_id, ""),
            worker_name=worker.name if worker else None,
        )
        if with_history:
            history = await db.delay_history([job.id])
            item["delay_history"] = history.get(job.id, [])
        return item

    async def worker_or_404(worker_id: str):
        worker = await resolve_worker(worker_id)
        if not worker:
            raise HTTPException(status_code=404, detail=f"Worker not found: {worker_id}")
        return worker

    @app.post("/api/v1/jobs/queue")
    async def jobs_queue_create(payload: JobEnqueueRequest, request: Request):
        principal: Principal = request.state.principal
        worker = await worker_or_404(payload.worker_id)
        if payload.gpu_count > worker.gpu_count:
            raise HTTPException(
                status_code=422,
                detail=f"gpu_count exceeds worker capacity ({worker.gpu_count})",
            )
        job = await jm.enqueue_job(
            worker,
            payload.command,
            owner_user_id=principal.user_id,
            actor_id=principal.actor_id,
            name=payload.name,
            expected_seconds=payload.expected_seconds,
            gpu_count=payload.gpu_count,
            priority=payload.priority,
            priority_note=payload.priority_note,
            deadline_at=payload.deadline_at,
            origin_session_id=origin_session_of(principal),
            pty_enabled=not payload.no_pty,
        )
        return await owned_job_dto(job)

    async def build_board(worker_id: str | None) -> dict:
        now = int(time.time())
        workers = {worker.id: worker for worker in await db.list_workers()}
        if worker_id is not None:
            selected = await resolve_worker(worker_id)
            worker_ids = [selected.id] if selected else [worker_id]
        else:
            worker_ids = list(workers)
        queued = [job for job in await db.list_queued_jobs() if job.worker_id in worker_ids or worker_id is None]
        cpu_jobs = [job for job in await db.list_active_cpu_jobs() if job.worker_id in worker_ids or worker_id is None]
        for job in (*queued, *cpu_jobs):
            if job.worker_id and job.worker_id not in worker_ids:
                worker_ids.append(job.worker_id)
        revisions = await db.queue_revisions()
        names = await db.user_display_names({job.owner_user_id for job in (*queued, *cpu_jobs)})
        history = await db.delay_history([job.id for job in queued], limit_per_job=5)
        entries = []
        for current_id in worker_ids:
            worker = workers.get(current_id)
            worker_jobs = [job for job in queued if job.worker_id == current_id]
            estimates = forecast_worker(worker, worker_jobs, now)
            snapshot = worker.name if worker else next(
                (job.worker_name_at_submission for job in worker_jobs if job.worker_name_at_submission), None,
            )
            rows = []
            position = 0
            for job in worker_jobs:
                if job.status == JobStatus.PENDING:
                    position += 1
                rows.append(public_queue_job(
                    job, owner_display_name=names.get(job.owner_user_id, ""), worker_name=snapshot,
                    position=position if job.status == JobStatus.PENDING else 0,
                    estimate=estimates.get(job.id), delay_history=history.get(job.id, []),
                ))
            cpu_rows = [
                public_queue_job(
                    job, owner_display_name=names.get(job.owner_user_id, ""), worker_name=snapshot,
                    position=0, estimate=None, delay_history=[],
                )
                for job in cpu_jobs if job.worker_id == current_id
            ]
            entries.append({
                "worker_id": current_id,
                "worker_name": snapshot,
                "revision": revisions.get(current_id, 0),
                "jobs": rows,
                "cpu_jobs": cpu_rows,
            })
        entries.sort(key=lambda item: ((item["worker_name"] or "").lower(), item["worker_id"]))
        return {"workers": entries, "as_of": now}

    @app.get("/api/v1/jobs/queue")
    async def jobs_queue_list(worker_id: str | None = None):
        return await build_board(worker_id)

    @app.get("/api/v1/jobs/queue/history")
    async def jobs_queue_history(
        worker_id: str | None = None,
        job_id: str | None = None,
        before_event_id: int | None = Query(None, ge=1),
        limit: int = Query(50, ge=1, le=200),
    ):
        events = await db.list_queue_events(
            worker_id=worker_id, job_id=job_id, before_event_id=before_event_id, limit=limit,
        )
        return {
            "events": events,
            "next_before_event_id": events[-1]["id"] if len(events) == limit else None,
        }

    @app.post("/api/v1/jobs/{job_id}/queue/preview")
    async def jobs_queue_preview(job_id: str, payload: QueuePreviewRequest, request: Request):
        principal: Principal = request.state.principal
        try:
            return await db.create_queue_preview(
                job_id, before_job_id=payload.before_job_id,
                expected_revision=payload.expected_revision, reason=payload.reason,
                actor=principal.audit_actor(), now=int(time.time()),
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=f"Job not found: {job_id}") from exc
        except QueueValidationError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except QueueConflict as exc:
            raise conflict(exc.code, exc.message) from exc

    @app.post("/api/v1/jobs/{job_id}/queue/move")
    async def jobs_queue_move(job_id: str, payload: QueueMoveRequest, request: Request):
        principal: Principal = request.state.principal
        try:
            result = await db.commit_queue_move(
                job_id, preview_id=payload.preview_id, expected_revision=payload.expected_revision,
                actor=principal.audit_actor(), now=int(time.time()),
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="preview not found") from exc
        except QueueConflict as exc:
            raise conflict(exc.code, exc.message) from exc
        worker = await db.get_worker(result["event"]["worker_id"])
        if worker is not None:
            await jm.dispatch_worker(worker)
        return result

    @app.patch("/api/v1/jobs/{job_id}/queue")
    async def jobs_queue_update(job_id: str, payload: QueuedJobUpdateRequest, request: Request):
        principal: Principal = request.state.principal
        job = await owned_job_or_404(principal, job_id)
        destination = None
        if payload.worker_id is not None:
            destination = await worker_or_404(payload.worker_id)
        try:
            updated = await db.update_job_metadata(
                job_id, owner_user_id=principal.user_id, actor=principal.audit_actor(),
                changes=payload.changes(), pending=True,
                destination_worker_id=destination.id if destination else None, now=int(time.time()),
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=f"Job not found: {job_id}") from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        for worker_ref in {job.worker_id, updated.worker_id}:
            worker = await db.get_worker(worker_ref or "")
            if worker is not None:
                await jm.dispatch_worker(worker)
        return await owned_job_dto(await db.get_job(job_id) or updated)

    @app.patch("/api/v1/jobs/{job_id}")
    async def jobs_update(job_id: str, payload: RunningJobUpdateRequest, request: Request):
        principal: Principal = request.state.principal
        await owned_job_or_404(principal, job_id)
        try:
            updated = await db.update_job_metadata(
                job_id, owner_user_id=principal.user_id, actor=principal.audit_actor(),
                changes=payload.changes(), pending=False, now=int(time.time()),
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=f"Job not found: {job_id}") from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return await owned_job_dto(updated)

    @app.post("/api/v1/jobs")
    async def jobs_create(payload: JobCreateRequest, request: Request):
        principal: Principal = request.state.principal
        worker = await worker_or_404(payload.worker_id)
        if worker.status != WorkerStatus.ONLINE:
            raise conflict("WORKER_OFFLINE", "worker is offline; CPU/setup jobs launch immediately")
        job = await jm.start_job(
            worker,
            payload.command,
            owner_user_id=principal.user_id,
            actor_id=principal.actor_id,
            name=payload.name,
            expected_seconds=payload.expected_seconds,
            priority=payload.priority,
            priority_note=payload.priority_note,
            deadline_at=payload.deadline_at,
            origin_session_id=origin_session_of(principal),
            pty_enabled=not payload.no_pty,
        )
        if not payload.sync:
            return await owned_job_dto(job)

        # Sync mode: poll until the supervisor confirms completion or timeout.
        deadline = time.monotonic() + payload.sync_timeout
        while time.monotonic() < deadline:
            job = await jm.refresh_job_status(worker, job)
            if job.status not in (JobStatus.STARTING, JobStatus.RUNNING):
                break
            await asyncio.sleep(0.5)
        log_path = f"{worker.harness_dir.rstrip('/')}/{job.id}/output.log"
        log_result = await async_ssh_run(worker, f"cat {shlex.quote(log_path)} 2>/dev/null", timeout=10)
        result = await owned_job_dto(job)
        result["stdout"] = log_result.stdout.rstrip("\n")
        return result

    @app.get("/api/v1/jobs")
    async def jobs_list(
        request: Request,
        worker_id: str | None = None,
        status_value: str | None = Query(None, alias="status"),
    ):
        principal: Principal = request.state.principal
        job_status = None
        if status_value:
            try:
                job_status = JobStatus(status_value)
            except ValueError:
                raise HTTPException(status_code=400, detail=f"Invalid status: {status_value}")
        # Ownership is applied in SQL regardless of other filters.
        jobs = await db.list_jobs(worker_id=worker_id, status=job_status, owner_user_id=principal.user_id)
        workers = {w.id: w for w in await db.list_workers()}
        names = await db.user_display_names({principal.user_id})
        return [
            owned_job(
                job, owner_display_name=names.get(principal.user_id, ""),
                worker_name=workers[job.worker_id].name if job.worker_id in workers else None,
            )
            for job in jobs
        ]

    @app.get("/api/v1/jobs/{job_id}")
    async def jobs_get(job_id: str, request: Request):
        job = await owned_job_or_404(request.state.principal, job_id)
        return await owned_job_dto(job, with_history=True)

    async def _job_reconciler() -> None:
        interval = _positive_int_env("WH_JOB_RECONCILE_INTERVAL_SECONDS", 10)
        while True:
            await asyncio.sleep(interval)
            try:
                await reconcile_active_ssh_jobs(db, jm)
                await reconcile_queued_jobs(db, jm)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Job reconciliation iteration failed")

    app.state.job_reconciler = _job_reconciler

    @app.get("/api/v1/jobs/{job_id}/logs")
    async def jobs_logs(
        job_id: str,
        request: Request,
        tail: int | None = Query(None, ge=0),
        head: int | None = Query(None, ge=0),
    ):
        if tail is not None and head is not None:
            raise HTTPException(status_code=400, detail="tail and head are mutually exclusive")
        job = await owned_job_or_404(request.state.principal, job_id)
        worker = await db.get_worker(job.worker_id or "")
        if not worker:
            raise HTTPException(status_code=404, detail=f"Worker not found: {job.worker_id}")
        resolved_tail = tail if tail is not None else (None if head is not None else 10)
        logs = await jm.get_logs(worker, job_id, tail=resolved_tail, head=head)
        return {"job_id": job_id, "tail": resolved_tail, "head": head, "logs": logs}

    @app.get("/api/v1/jobs/{job_id}/logs/stream")
    async def jobs_logs_stream(
        job_id: str,
        request: Request,
        poll_seconds: float = Query(1.0, gt=0, le=10),
        tail: int = Query(50, ge=1, le=10000),
    ):
        principal: Principal = request.state.principal
        job = await owned_job_or_404(principal, job_id)
        worker = await db.get_worker(job.worker_id or "")
        if not worker:
            raise HTTPException(status_code=404, detail=f"Worker not found: {job.worker_id}")

        async def stream():
            offset: int | None = None
            partial = b""
            while True:
                # Revalidate credentials before every batch.
                if not await principal_still_valid(db, principal):
                    return
                chunk = await jm.get_log_range(worker, job_id, offset)
                if chunk is not None:
                    start, end, data = chunk
                    if offset is not None and start != offset:
                        partial = b""  # the log was truncated or replaced
                    complete, newline, partial = (partial + data).rpartition(b"\n")
                    lines = (complete + newline).decode(errors="replace").splitlines(keepends=True)
                    if offset is None:
                        # The initial window may begin mid-line; then every new line.
                        lines = (lines[1:] if start > 0 else lines)[-tail:]
                    for line in lines:
                        yield line
                    offset = end
                await asyncio.sleep(poll_seconds)

        return StreamingResponse(stream(), media_type="text/plain", headers={"X-Accel-Buffering": "no"})

    @app.delete("/api/v1/jobs/{job_id}")
    async def jobs_delete(job_id: str, request: Request):
        job = await owned_job_or_404(request.state.principal, job_id)
        if job.status in (JobStatus.DONE, JobStatus.FAILED):
            return {
                "job_id": job_id, "stopped": True, "already_terminal": True,
                "status": job.status.value, "execution_state": job.execution_state.value,
            }
        worker = await db.get_worker(job.worker_id or "")
        if not worker:
            raise conflict("STOP_UNCONFIRMED", "worker is unknown; the job keeps its allocation", 503)
        outcome = await jm.stop_job(worker, job_id, reason=TerminationReason.USER_CANCELLED)
        if outcome.state == "not_found":
            raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")
        if outcome.state == "unconfirmed":
            raise conflict(
                "STOP_UNCONFIRMED",
                "stop requested but termination is not confirmed; the job keeps its allocation",
                503,
            )
        updated = outcome.job or await db.get_job(job_id)
        return {
            "job_id": job_id,
            "stopped": True,
            "already_terminal": outcome.state == "already_terminal",
            "status": updated.status.value if updated else None,
            "execution_state": updated.execution_state.value if updated else None,
        }

    @app.get("/api/v1/usage")
    async def usage(worker_id: str | None = None, user_id: str | None = None):
        now = int(time.time())
        rows = await db.usage(now=now, worker_id=worker_id, user_id=user_id)
        return {"rows": rows, "as_of": now, "provisional": any(row["provisional"] for row in rows)}

    # ── Managed Marimo (owner-scoped) ────────────────────────────────────────

    def marimo_dto(session: MarimoSession, *, worker_name: str | None, ready: bool) -> dict:
        return {
            "id": session.id,
            "worker_id": session.worker_id,
            "worker_name": worker_name,
            "notebook_path": session.notebook_path,
            "environment": session.environment,
            "job_id": session.job_id,
            "status": "ready" if ready else "stopped",
            "url": session.url,
            "owner_user_id": session.owner_user_id,
            "created_at": session.created_at,
        }

    async def stop_tunnel(tunnel_id: str) -> None:
        entry = app.state.tunnels.remove(tunnel_id)
        if entry:
            # The registry kills the complete process group off the event loop.
            await asyncio.to_thread(TunnelRegistry.stop, entry)
        await db.delete_port_forward(tunnel_id)

    async def release_marimo_resources(session: MarimoSession) -> None:
        """Delete forward, tokens and record once the notebook job is confirmed gone."""
        await stop_tunnel(session.tunnel_id)
        if session.upstream_token_path:
            delete_local_token(Path(session.upstream_token_path))
        worker = await db.get_worker(session.worker_id)
        if worker is not None:
            await delete_remote_token(worker, session.id)
        await db.delete_marimo_session(session.id)

    async def owned_marimo(principal: Principal, session_id: str) -> tuple[MarimoSession, Job, PortForward]:
        session = await db.get_marimo_session(session_id)
        if session is None or session.owner_user_id != principal.user_id:
            raise HTTPException(status_code=404, detail=f"Marimo session not found: {session_id}")
        job = await db.get_job(session.job_id)
        forward = await db.get_port_forward(session.tunnel_id)
        if (
            job is None or forward is None
            or job.owner_user_id != session.owner_user_id
            or forward.owner_user_id != session.owner_user_id
        ):
            raise HTTPException(status_code=409, detail="managed notebook resources are inconsistent")
        return session, job, forward

    def marimo_ready(job: Job | None, tunnel_id: str) -> bool:
        entry = app.state.tunnels.get(tunnel_id)
        return bool(job and job.status == JobStatus.RUNNING and entry and entry.proc.poll() is None)

    @app.post("/api/v1/marimo", status_code=201)
    async def marimo_create(payload: MarimoCreateRequest, request: Request):
        principal: Principal = request.state.principal
        worker = await worker_or_404(payload.worker_id)
        if not 1 <= payload.ready_timeout <= 300:
            raise HTTPException(status_code=422, detail="ready_timeout must be between 1 and 300 seconds")
        session_id = str(uuid4())
        base_url = proxy_base_path(session_id)
        bind_host = "127.0.0.1"
        try:
            remote_port = await allocate_worker_port(worker)
            reserved = {pf.local_port for pf in await db.list_port_forwards() if pf.bind_host == bind_host}
            local_port = allocate_local_port(bind_host, reserved=reserved)
            command = build_launch_command(
                notebook_path=payload.notebook_path,
                environment=payload.environment,
                port=remote_port,
                token_file=remote_token_path(worker, session_id),
                base_url=base_url,
            )
        except (ValueError, RuntimeError, OSError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        token = secrets.token_urlsafe(32)
        token_path = local_token_path(db.path, session_id)
        write_local_token(token_path, token)
        uploaded = await upload_remote_token(worker, session_id, token)
        if uploaded.returncode != 0:
            delete_local_token(token_path)
            await delete_remote_token(worker, session_id)
            raise HTTPException(status_code=502, detail="could not provision the notebook token")

        # Managed notebooks are CPU-only; GPU notebooks use ordinary queued commands.
        job = await jm.start_job(
            worker, command, owner_user_id=principal.user_id, actor_id=principal.actor_id,
            name="Marimo session", origin_session_id=origin_session_of(principal), pty_enabled=False,
        )
        if job.status == JobStatus.FAILED:
            delete_local_token(token_path)
            await delete_remote_token(worker, session_id)
            raise HTTPException(status_code=502, detail="failed to start marimo worker process")

        now = int(datetime.now(timezone.utc).timestamp())
        pf = PortForward(
            worker_id=worker.id,
            local_port=local_port,
            remote_port=remote_port,
            service_name="Marimo session",
            created_at=now,
            owner_user_id=principal.user_id,
            created_by_actor_id=principal.actor_id,
            bind_host=bind_host,
            marimo_session_id=session_id,
        )
        proc = None
        reserved = False
        registered = False
        try:
            await db.insert_port_forward(pf)
            reserved = True
            proc = await ssh_port_forward(worker, local_port, remote_port, bind_host=bind_host)
            if proc.poll() is not None:
                raise RuntimeError(f"SSH tunnel exited immediately (code={proc.returncode})")
            pf.pid = proc.pid
            await db.update_port_forward_pid(pf.id, proc.pid)
            app.state.tunnels.add(TunnelProcess(
                id=pf.id, worker_id=worker.id, local_port=local_port, remote_port=remote_port,
                proc=proc, created_at=now, owner_user_id=principal.user_id,
            ))
            registered = True
            await wait_until_ready(bind_host, local_port, payload.ready_timeout, base_url=base_url, token=token)
            session = MarimoSession(
                id=session_id,
                worker_id=worker.id,
                notebook_path=payload.notebook_path,
                environment=payload.environment,
                job_id=job.id,
                tunnel_id=pf.id,
                local_port=local_port,
                remote_port=remote_port,
                bind_host=bind_host,
                url=f"{app.state.web_origin}{base_url}/",
                status="ready",
                created_at=now,
                owner_user_id=principal.user_id,
                created_by_actor_id=principal.actor_id,
                upstream_token_path=str(token_path),
            )
            await db.insert_marimo_session(session)
        except BaseException as exc:
            async def cleanup_failed_start() -> None:
                # Remove only resources allocated by this request.
                entry = app.state.tunnels.remove(pf.id) if registered else None
                if entry:
                    await asyncio.to_thread(TunnelRegistry.stop, entry)
                elif proc is not None:
                    transient = TunnelProcess(
                        id=pf.id, worker_id=worker.id, local_port=local_port,
                        remote_port=remote_port, proc=proc, created_at=now,
                        owner_user_id=principal.user_id,
                    )
                    await asyncio.to_thread(TunnelRegistry.stop, transient)
                if reserved:
                    try:
                        await db.delete_port_forward(pf.id)
                    except Exception:
                        log.exception("Failed to delete marimo tunnel row %s during rollback", pf.id)
                try:
                    outcome = await jm.stop_job(worker, job.id, reason=TerminationReason.USER_CANCELLED)
                except Exception:
                    log.exception("Failed to stop marimo job %s during rollback", job.id)
                    return
                if outcome.state in ("stopped", "already_terminal"):
                    delete_local_token(token_path)
                    await delete_remote_token(worker, session_id)
                else:
                    log.error("Marimo job %s stop unconfirmed; retaining its token for reconciliation", job.id)

            cleanup_task = asyncio.create_task(cleanup_failed_start())
            try:
                await asyncio.shield(cleanup_task)
            except asyncio.CancelledError:
                await cleanup_task
            if isinstance(exc, asyncio.CancelledError):
                raise
            if isinstance(exc, ListenerConflict):
                raise HTTPException(status_code=409, detail="listener port unavailable; retry") from exc
            raise HTTPException(status_code=502, detail=f"marimo startup failed: {exc}") from exc

        return marimo_dto(session, worker_name=worker.name, ready=True)

    @app.get("/api/v1/marimo")
    async def marimo_list(request: Request, worker_id: str | None = None):
        principal: Principal = request.state.principal
        sessions = await db.list_marimo_sessions(worker_id=worker_id, owner_user_id=principal.user_id)
        workers = {worker.id: worker for worker in await db.list_workers()}
        result = []
        for session in sessions:
            job = await db.get_job(session.job_id)
            worker = workers.get(session.worker_id)
            result.append(marimo_dto(
                session, worker_name=worker.name if worker else None,
                ready=marimo_ready(job, session.tunnel_id),
            ))
        return result

    @app.get("/api/v1/marimo/{session_id}")
    async def marimo_get(session_id: str, request: Request):
        session, job, _forward = await owned_marimo(request.state.principal, session_id)
        worker = await db.get_worker(session.worker_id)
        if worker and job.status == JobStatus.RUNNING:
            job = await jm.refresh_job_status(worker, job)
        return marimo_dto(
            session, worker_name=worker.name if worker else None,
            ready=marimo_ready(job, session.tunnel_id),
        )

    @app.delete("/api/v1/marimo/{session_id}")
    async def marimo_delete(session_id: str, request: Request):
        session, job, _forward = await owned_marimo(request.state.principal, session_id)
        if job.status not in (JobStatus.DONE, JobStatus.FAILED):
            worker = await db.get_worker(session.worker_id)
            if worker is None:
                raise conflict("STOP_UNCONFIRMED", "worker is unknown; notebook kept", 503)
            outcome = await jm.stop_job(worker, job.id, reason=TerminationReason.USER_CANCELLED)
            if outcome.state == "unconfirmed":
                raise conflict("STOP_UNCONFIRMED", "notebook stop is not confirmed; resources kept", 503)
        # The terminal-job hook normally released everything; finish if not.
        remaining = await db.get_marimo_session(session_id)
        if remaining is not None:
            await release_marimo_resources(remaining)
        return {"session_id": session_id, "removed": True}

    async def authorize_marimo(connection: HTTPConnection, session_id: str) -> ProxyTarget:
        principal = await resolve_principal(connection)
        if principal.credential_kind == "pi_session" and principal.pi_session.role == "orchestrator":
            raise HTTPException(status_code=403, detail="caller role is not allowed")
        session, job, forward = await owned_marimo(principal, session_id)
        if job.status != JobStatus.RUNNING:
            raise HTTPException(status_code=409, detail="notebook is not running")
        token_path = Path(session.upstream_token_path) if session.upstream_token_path else None
        try:
            token = token_path.read_text(encoding="utf8").strip() if token_path else ""
        except OSError:
            token = ""
        if not token:
            # Legacy unauthenticated listeners are never adopted.
            raise HTTPException(status_code=409, detail="notebook has no native token; recreate it")
        return ProxyTarget(
            upstream=f"http://127.0.0.1:{forward.local_port}",
            base_path=proxy_base_path(session.id),
            token=token,
            credential_kind=principal.credential_kind,
        )

    app.include_router(create_marimo_proxy_router(authorize=authorize_marimo, web_origin=app.state.web_origin))

    # ── Generic tunnels (owner-scoped raw TCP on the trusted Tailnet) ────────

    def tunnel_dto(pf: PortForward, worker_name: str | None) -> dict:
        return {
            "id": pf.id,
            "worker_id": pf.worker_id,
            "worker_name": worker_name,
            "local_port": pf.local_port,
            "remote_port": pf.remote_port,
            "service_name": pf.service_name,
            "bind_host": pf.bind_host,
            "owner_user_id": pf.owner_user_id,
            "created_at": pf.created_at,
        }

    @app.post("/api/v1/tunnels")
    async def tunnels_create(payload: TunnelCreateRequest, request: Request):
        principal: Principal = request.state.principal
        worker = await worker_or_404(payload.worker_id)
        try:
            bind_host = tailnet_bind_host()
        except RuntimeError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        pf = PortForward(
            worker_id=worker.id,
            local_port=payload.local_port,
            remote_port=payload.remote_port,
            service_name=payload.name or f"port-{payload.remote_port}",
            created_at=int(datetime.now(timezone.utc).timestamp()),
            owner_user_id=principal.user_id,
            created_by_actor_id=principal.actor_id,
            bind_host=bind_host,
        )
        try:
            await db.insert_port_forward(pf)
        except ListenerConflict as exc:
            raise HTTPException(status_code=409, detail="local port unavailable") from exc

        proc = None
        registered = False
        try:
            proc = await ssh_port_forward(worker, payload.local_port, payload.remote_port, bind_host=bind_host)
            if proc.poll() is not None:
                raise HTTPException(
                    status_code=502,
                    detail=f"SSH tunnel setup exited immediately (code={proc.returncode})",
                )
            pf.pid = proc.pid
            await db.update_port_forward_pid(pf.id, proc.pid)
            app.state.tunnels.add(TunnelProcess(
                id=pf.id,
                worker_id=worker.id,
                local_port=payload.local_port,
                remote_port=payload.remote_port,
                proc=proc,
                created_at=pf.created_at,
                owner_user_id=principal.user_id,
            ))
            registered = True
        except BaseException:
            async def cleanup_failed_tunnel() -> None:
                entry = app.state.tunnels.remove(pf.id) if registered else None
                if entry:
                    await asyncio.to_thread(TunnelRegistry.stop, entry)
                elif proc is not None:
                    transient = TunnelProcess(
                        id=pf.id, worker_id=worker.id,
                        local_port=payload.local_port, remote_port=payload.remote_port,
                        proc=proc, created_at=pf.created_at, owner_user_id=principal.user_id,
                    )
                    await asyncio.to_thread(TunnelRegistry.stop, transient)
                try:
                    await db.delete_port_forward(pf.id)
                except Exception:
                    log.exception("Failed to delete tunnel row %s during rollback", pf.id)

            cleanup_task = asyncio.create_task(cleanup_failed_tunnel())
            try:
                await asyncio.shield(cleanup_task)
            except asyncio.CancelledError:
                await cleanup_task
            raise
        finally:
            await app.state.cache.invalidate(f"tunnels:{principal.user_id}")
        return tunnel_dto(pf, worker.name)

    @app.get("/api/v1/tunnels")
    async def tunnels_list(request: Request):
        principal: Principal = request.state.principal

        async def load():
            tunnels = await db.list_port_forwards(owner_user_id=principal.user_id)
            workers = {w.id: w for w in await db.list_workers()}
            return [
                tunnel_dto(t, getattr(workers.get(t.worker_id), "name", None))
                for t in tunnels if t.marimo_session_id is None
            ]
        return await cached(f"tunnels:{principal.user_id}", 10.0, load)

    @app.delete("/api/v1/tunnels/{tunnel_id}")
    async def tunnels_delete(tunnel_id: str, request: Request):
        principal: Principal = request.state.principal
        pf = await db.get_port_forward(tunnel_id)
        if pf is None or pf.owner_user_id != principal.user_id:
            raise HTTPException(status_code=404, detail=f"Tunnel not found: {tunnel_id}")
        if pf.marimo_session_id is not None:
            raise conflict("MANAGED_BY_MARIMO", "stop the managed notebook instead")
        await stop_tunnel(pf.id)
        await app.state.cache.invalidate(f"tunnels:{principal.user_id}")
        return {"tunnel_id": pf.id, "removed": True}

    async def on_user_disabled(user_id: str) -> None:
        """Cancel pending work and close generic listeners; running jobs continue."""
        cancelled = await db.cancel_pending_jobs_for_user(user_id, finished_at=int(time.time()))
        if cancelled:
            for worker in await db.list_workers():
                await jm.dispatch_worker(worker)
        for pf in await db.list_port_forwards(owner_user_id=user_id):
            if pf.marimo_session_id is None:
                await stop_tunnel(pf.id)
        await app.state.cache.invalidate(f"tunnels:{user_id}")

    app.include_router(create_auth_router(on_user_disabled=on_user_disabled))

    @app.post("/api/v1/workers/{worker_id}/files")
    async def worker_file_upload(worker_id: str, payload: FileUploadRequest):
        import base64

        worker = await resolve_worker(worker_id)
        if not worker:
            raise HTTPException(status_code=404, detail=f"Worker not found: {worker_id}")

        try:
            content = base64.b64decode(payload.content_b64, validate=True)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid base64 content")

        if len(content) > MAX_FILE_TRANSFER_BYTES:
            raise HTTPException(
                status_code=413,
                detail=(
                    f"File too large: {len(content)} bytes > {MAX_FILE_TRANSFER_BYTES} limit. "
                    "Use rsync over tailnet SSH for large files: "
                    "rsync -e 'tailscale ssh' <local> {worker.ssh_user}@{host}:{path}".format(
                        worker=worker, host=worker.ssh_host, path=payload.path
                    )
                ),
            )

        result = await ssh_upload_bytes(worker, content, payload.path)
        if result.returncode != 0:
            raise HTTPException(
                status_code=502,
                detail=f"SSH upload failed: {result.stderr or 'unknown error'}",
            )

        return {
            "worker_id": worker.id,
            "path": payload.path,
            "size": len(content),
        }

    @app.get("/api/v1/workers/{worker_id}/files")
    async def worker_file_download(
        worker_id: str,
        path: str = Query(..., description="Remote file path to download"),
        max_bytes: int = Query(MAX_FILE_TRANSFER_BYTES, ge=1, le=MAX_FILE_TRANSFER_BYTES),
    ):
        import base64

        worker = await resolve_worker(worker_id)
        if not worker:
            raise HTTPException(status_code=404, detail=f"Worker not found: {worker_id}")

        content, result = await ssh_download_bytes(worker, path, max_bytes=max_bytes)
        if result.returncode != 0:
            raise HTTPException(
                status_code=502,
                detail=f"SSH download failed: {result.stderr or 'unknown error'}",
            )

        if len(content) > max_bytes:
            raise HTTPException(
                status_code=413,
                detail=(
                    f"File too large: {len(content)} bytes > {max_bytes} limit. "
                    "Use rsync over tailnet SSH for large files."
                ),
            )

        return {
            "worker_id": worker.id,
            "path": path,
            "size": len(content),
            "content_b64": base64.b64encode(content).decode(),
        }

    @app.get("/api/v1/_stats")
    async def reliability_stats():
        """Live reliability/queue/cache diagnostics for operators and agents."""
        snapshot = app.state.metrics.snapshot()
        # These components own their detailed state; compose it rather than
        # duplicating counters in every hot request path.
        snapshot["cache"] = app.state.cache.stats()
        snapshot["lanes"]["workers"] = app.state.lanes.stats()
        snapshot["rate_limit"]["agents"] = app.state.rate_limiter.stats()
        snapshot["tunnels"] = app.state.tunnels.stats()
        return snapshot

    @app.get("/api/v1/events")
    async def events_list(limit: int = Query(50, ge=1, le=1000)):
        failures = await db.list_failures(limit=limit)
        return [
            {
                "type": "job_failure",
                "id": f.id,
                "job_id": f.job_id,
                "worker_id": f.worker_id,
                "exit_code": f.exit_code,
                "timestamp": f.timestamp,
                "summary": f.summary,
            }
            for f in failures
        ]

    web_default = Path(__file__).resolve().parents[2] / "web"
    web_dir = Path(os.environ.get("WH_WEB_DIR", str(web_default)))
    if web_dir.is_dir():
        app.mount("/", StaticFiles(directory=web_dir, html=True), name="web")

    return app


async def _run_server(app: FastAPI, host: str, port: int) -> None:
    import uvicorn

    config = uvicorn.Config(app, host=host, port=port, log_level="warning")
    await uvicorn.Server(config).serve()


async def run_registration_server(db: Database, host: str = "0.0.0.0", port: int = 12888) -> None:
    """Run the worker-only registration server."""
    await _run_server(create_registration_app(db), host, port)


async def run_control_server(db: Database, host: str = "0.0.0.0", port: int = 12889) -> None:
    """Run the privileged operator/control server."""
    await _run_server(create_app(db), host, port)

