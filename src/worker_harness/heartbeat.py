"""FastAPI-based HTTP server for worker heartbeats and orchestration API."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import secrets
from collections import Counter
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import AsyncIterator, Awaitable, Callable, Literal, Self, TypeVar
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit
from uuid import uuid4

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, WebSocket, status
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, ValidationError
from starlette.requests import HTTPConnection
from websockets.asyncio.client import connect as websocket_connect
from websockets.exceptions import ConnectionClosed, WebSocketException

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
from .db import Database
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
    tailnet_bind_host,
    wait_until_ready,
)
from .models import (
    JobKind,
    JobStatus,
    PiBridgeEventBatch,
    PiBridgeRegister,
    PiSession,
    PiSessionCommand,
    PiSessionEvent,
    PiSessionState,
    PortForward,
    MarimoSession,
    WorkerJobReportBatch,
    WorkerRegistration,
    WorkerStatus,
)
from .ratelimit import AgentRateLimiter, RateLimited, resolve_agent_name
from .reaper import reap_loop
from .ssh import async_ssh_run, set_lanes, ssh_download_bytes, ssh_port_forward, ssh_upload_bytes
from .tunnel_registry import TunnelProcess, TunnelRegistry

log = logging.getLogger("heartbeat-server")

T = TypeVar("T")


class JobCreateRequest(BaseModel):
    worker_id: str
    command: str
    name: str | None = None
    no_pty: bool = False
    sync: bool = False       # block until command finishes, return stdout
    sync_timeout: int = 120  # seconds to wait in sync mode


class TunnelCreateRequest(BaseModel):
    worker_id: str
    local_port: int
    remote_port: int
    name: str = ""


class MarimoCreateRequest(BaseModel):
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
    jobs = [
        job
        for job in await db.list_jobs()
        if job.kind == JobKind.SSH
        and job.status in (JobStatus.RUNNING, JobStatus.PENDING)
    ]
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start reliability background work and deterministically clean up.

    Database ownership remains with the caller (`serve`), but every persistent
    tunnel and every queued lane waiter belongs to this FastAPI app.
    """
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
) -> AsyncIterator[str]:
    """Replay and tail one session's durable event log as SSE."""
    cursor = max(0, after)
    heartbeat_at = asyncio.get_running_loop().time() + 15.0
    while True:
        if await request.is_disconnected():
            return
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
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    for task in done:
        if task is not eviction_task:
            task.result()
    return "replaced" if eviction_task in done else "completed"


def _operator_token() -> str:
    token = os.environ.get("WH_OPERATOR_TOKEN", "").strip()
    token_file = os.environ.get("WH_OPERATOR_TOKEN_FILE", "").strip()
    if not token and token_file:
        token = Path(token_file).expanduser().read_text(encoding="utf8").strip()
    if token or token_file:
        if len(token) < 43 or any(
            character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
            for character in token
        ):
            raise ValueError("operator token must be at least 43 base64url characters")
    return token


def require_role(*allowed: str):
    """Resolve an optional fleet bearer token and enforce the caller role."""

    async def resolve(
        request: HTTPConnection,
        authorization: str | None = Header(default=None),
    ) -> PiSession | None:
        operator_token = request.app.state.operator_token
        if authorization is None:
            if operator_token or request.app.state.machines:
                raise HTTPException(status_code=401, detail="operator or session bearer token required")
            if "operator" in allowed:
                return None
            raise HTTPException(status_code=403, detail="operator is not allowed")
        scheme, separator, token = authorization.partition(" ")
        if separator != " " or scheme.lower() != "bearer" or not token.strip() or not token.isascii():
            raise HTTPException(status_code=401, detail="invalid bearer token")
        if operator_token and secrets.compare_digest(token.strip(), operator_token):
            if "operator" in allowed:
                return None
            raise HTTPException(status_code=403, detail="operator is not allowed")
        caller = await request.app.state.db.get_pi_session_by_token_hash(
            hashlib.sha256(token.strip().encode()).hexdigest()
        )
        if caller is None:
            raise HTTPException(status_code=401, detail="invalid bearer token")
        if caller.state in {
            PiSessionState.STOPPED, PiSessionState.FAILED, PiSessionState.TERMINATION_UNKNOWN,
        }:
            raise HTTPException(status_code=401, detail="session token is no longer active")
        if caller.role not in allowed:
            raise HTTPException(status_code=403, detail="caller role is not allowed")
        return caller

    return resolve


async def require_api_caller(request: HTTPConnection) -> None:
    if request.url.path == "/health":
        return
    if request.scope["type"] == "websocket":
        return
    allowed = ("operator", "pm", "task")
    if request.url.path.startswith("/api/v1/pi/"):
        allowed += ("orchestrator",)
    await require_role(*allowed)(request, request.headers.get("authorization"))


def create_app(db: Database) -> FastAPI:
    """Create the privileged control API (kept as the public test factory)."""
    app = FastAPI(
        title="Worker Harness Control API", lifespan=lifespan,
        dependencies=[Depends(require_api_caller)],
    )
    jm = JobManager(db)
    app.state.db = db
    app.state.operator_token = _operator_token()
    app.state.machines = load_machines()
    app.state.projects = load_projects()
    app.state.orchestrator = Orchestrator(
        db,
        app.state.machines,
        app.state.projects,
    )

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
        """Control-plane liveness endpoint (separate from registration)."""
        async with app.state.pi_gateway_lock:
            gateway_by_session = {
                session_id: len(active)
                for session_id, active in app.state.pi_gateway_attachments.items()
                if active
            }
            gateway_close_reasons = dict(app.state.pi_gateway_close_reasons)
            gateway_refused_total = app.state.pi_gateway_refused_total
            gateway_evictions_total = app.state.pi_gateway_evictions_total
        return {
            "status": "healthy",
            "ts": datetime.now(timezone.utc).isoformat(),
            "pi_gateway_attachment_count": sum(gateway_by_session.values()),
            "pi_gateway_attachments_by_session": gateway_by_session,
            "pi_gateway_max_per_session": app.state.pi_gateway_max_per_session,
            "pi_gateway_refused_total": gateway_refused_total,
            "pi_gateway_evictions_total": gateway_evictions_total,
            "pi_gateway_close_reasons": gateway_close_reasons,
        }

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
        path = request.url.path
        is_pi_bridge = path.startswith("/api/v1/pi/bridge/")
        if path.startswith("/api/v1/") and not is_pi_bridge:
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
        await app.state.cache.invalidate("workers:list")
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


    # ── Privileged orchestration API (/api/v1) ───────────────────────────────

    @app.get("/api/v1/workers")
    async def workers_list():
        async def load():
            workers = await db.list_workers()
            return [w.model_dump(mode="json") for w in workers]
        return await cached("workers:list", 5.0, load)

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
        _caller: PiSession | None = Depends(require_role("operator")),
    ):
        import time as _time

        cutoff = int(_time.time()) - (minutes * 60)
        removed = await db.prune_workers(cutoff)
        await invalidate_workers_cache()
        return {"removed": removed, "minutes": minutes}

    @app.post("/api/v1/pi/bridge/register")
    async def pi_bridge_register(
        payload: PiBridgeRegister,
        caller: PiSession | None = Depends(
            require_role("operator", "orchestrator", "pm", "task")
        ),
    ):
        if caller and caller.id != payload.session_id:
            raise HTTPException(status_code=403, detail="session token does not match bridge session")
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
            session = await db.register_interactive_pi_session(payload, now=now)
        except ValueError as exc:
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
        caller: PiSession | None = Depends(
            require_role("operator", "orchestrator", "pm", "task")
        ),
    ):
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
        caller: PiSession | None = Depends(
            require_role("operator", "orchestrator", "pm", "task")
        ),
    ):
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
        caller: PiSession | None = Depends(
            require_role("operator", "orchestrator", "pm", "task")
        ),
    ):
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
        _caller: PiSession | None = Depends(require_role("operator", "orchestrator", "pm")),
    ):
        sessions = await db.list_pi_sessions_by_role("orchestrator")
        for session in sessions:
            if session.state not in {PiSessionState.STOPPED, PiSessionState.FAILED}:
                return session.model_dump(mode="json")
        raise HTTPException(status_code=404, detail="no live orchestrator session")

    @app.post("/api/v1/pi/orchestrator:send")
    async def pi_orchestrator_send(
        payload: PiMessageRequest,
        caller: PiSession | None = Depends(require_role("operator", "pm")),
    ):
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
        caller: PiSession | None = Depends(require_role("operator", "orchestrator", "pm")),
    ):
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
        caller: PiSession | None = Depends(require_role("operator", "orchestrator", "pm")),
    ):
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
        caller: PiSession | None = Depends(require_role("operator", "orchestrator", "pm")),
    ):
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
        _caller: PiSession | None = Depends(require_role("operator")),
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
        caller: PiSession = Depends(require_role("task")),
    ):
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
        caller: PiSession = Depends(require_role("task")),
    ):
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
        _caller: PiSession | None = Depends(require_role("operator", "orchestrator")),
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
        caller: PiSession | None = Depends(require_role("operator", "orchestrator")),
    ):
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
        caller: PiSession = Depends(require_role("pm")),
    ):
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
        caller: PiSession = Depends(require_role("pm")),
    ):
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
        caller: PiSession = Depends(require_role("pm")),
    ):
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
        caller: PiSession | None = Depends(require_role("operator", "orchestrator", "pm", "task")),
    ) -> PiSession:
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
        _caller: PiSession | None = Depends(require_role("operator")),
        session: PiSession = Depends(readable_session),
    ):
        return attach_info_with_gateway(session, request)

    @app.websocket("/api/v1/pi/sessions/{session_id}/attach-gateway")
    async def pi_session_attach_gateway(websocket: WebSocket, session_id: str) -> None:
        await websocket.accept()
        try:
            await require_role("operator")(websocket, websocket.headers.get("authorization"))
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
                    websocket, upstream, attachment=attachment
                )
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
            stream_pi_session_events(request, db, session_id, after),
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
    async def data_copy(payload: DataCopyRequest):
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
            job = await jm.start_job(destination, command, name=f"data-copy-{transfer_id}", pty_enabled=False)
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
        return worker.model_dump(mode="json")

    @app.post("/api/v1/jobs")
    async def jobs_create(payload: JobCreateRequest):
        worker = await resolve_worker(payload.worker_id)
        if not worker:
            raise HTTPException(
                status_code=404,
                detail=f"Worker not found: {payload.worker_id}",
            )

        job = await jm.start_job(
            worker,
            payload.command,
            name=payload.name,
            pty_enabled=not payload.no_pty,
        )

        if not payload.sync:
            return job.model_dump(mode="json")

        # Sync mode: poll until the job finishes or sync_timeout expires.
        import time as _time

        deadline = _time.monotonic() + payload.sync_timeout
        while _time.monotonic() < deadline:
            job = await jm.refresh_job_status(worker, job)
            if job.status not in (JobStatus.RUNNING, JobStatus.PENDING):
                break
            await asyncio.sleep(0.5)

        # Read the full log (stdout+stderr merged by the tmux script)
        log_path = f"{worker.harness_dir.rstrip('/')}/{job.id}/output.log"
        log_result = await async_ssh_run(worker, f"cat '{log_path}' 2>/dev/null", timeout=10)
        # Strip the EXIT marker line
        output_lines = [
            line for line in log_result.stdout.splitlines() if not line.startswith("EXIT:")
        ]
        output = "\n".join(output_lines)

        result = job.model_dump(mode="json")
        result["stdout"] = output
        return result

    @app.get("/api/v1/jobs")
    async def jobs_list(
        worker_id: str | None = None,
        status_value: str | None = Query(None, alias="status"),
        origin_session_id: str | None = None,
    ):
        job_status = None
        if status_value:
            try:
                job_status = JobStatus(status_value)
            except ValueError:
                raise HTTPException(status_code=400, detail=f"Invalid status: {status_value}")

        jobs = await db.list_jobs(
            worker_id=worker_id,
            status=job_status,
            origin_session_id=origin_session_id,
        )
        workers = {w.id: w for w in await db.list_workers()}

        refreshed = []
        for job in jobs:
            item = job.model_dump(mode="json")
            worker_ref = workers.get(job.worker_id or "")
            item["worker_name"] = worker_ref.name if worker_ref else None
            refreshed.append(item)

        return refreshed

    async def _job_reconciler() -> None:
        interval = _positive_int_env("WH_JOB_RECONCILE_INTERVAL_SECONDS", 10)
        while True:
            await asyncio.sleep(interval)
            try:
                await reconcile_active_ssh_jobs(db, jm)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Job reconciliation iteration failed")

    app.state.job_reconciler = _job_reconciler

    @app.get("/api/v1/jobs/{job_id}/logs")
    async def jobs_logs(
        job_id: str,
        tail: int | None = Query(None, ge=0),
        head: int | None = Query(None, ge=0),
    ):
        if tail is not None and head is not None:
            raise HTTPException(status_code=400, detail="tail and head are mutually exclusive")

        job = await db.get_job(job_id)
        if not job or not job.worker_id:
            raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")

        worker = await db.get_worker(job.worker_id)
        if not worker:
            raise HTTPException(status_code=404, detail=f"Worker not found: {job.worker_id}")

        resolved_tail = tail if tail is not None else (None if head is not None else 10)
        logs = await jm.get_logs(worker, job_id, tail=resolved_tail, head=head)
        return {
            "job_id": job_id,
            "tail": resolved_tail,
            "head": head,
            "logs": logs,
        }

    @app.get("/api/v1/jobs/{job_id}/logs/stream")
    async def jobs_logs_stream(
        job_id: str,
        poll_seconds: float = Query(1.0, gt=0, le=10),
        tail: int = Query(50, ge=1, le=10000),
    ):
        job = await db.get_job(job_id)
        if not job or not job.worker_id:
            raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")

        worker = await db.get_worker(job.worker_id)
        if not worker:
            raise HTTPException(status_code=404, detail=f"Worker not found: {job.worker_id}")

        async def stream():
            last_len = 0
            while True:
                logs = await jm.get_logs(worker, job_id, tail=tail)
                lines = logs.splitlines(keepends=True)
                for line in lines[last_len:]:
                    yield line
                last_len = len(lines)
                await asyncio.sleep(poll_seconds)

        return StreamingResponse(stream(), media_type="text/plain")

    @app.delete("/api/v1/jobs/{job_id}")
    async def jobs_delete(job_id: str):
        job = await db.get_job(job_id)
        if not job or not job.worker_id:
            raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")

        if job.status in (JobStatus.DONE, JobStatus.FAILED):
            return {
                "job_id": job_id,
                "stopped": True,
                "already_terminal": True,
                "status": job.status.value,
            }

        worker = await db.get_worker(job.worker_id)
        if not worker:
            raise HTTPException(status_code=404, detail=f"Worker not found: {job.worker_id}")

        stopped = await jm.stop_job(worker, job_id)
        if not stopped:
            raise HTTPException(
                status_code=500,
                detail={
                    "message": f"Failed to stop job: {job_id}",
                    "hint": "Job may have already exited; refresh job status and retry.",
                },
            )

        updated = await db.get_job(job_id)
        return {
            "job_id": job_id,
            "stopped": True,
            "already_terminal": False,
            "status": updated.status.value if updated else None,
        }

    async def stop_marimo_session(session: MarimoSession) -> None:
        job = await db.get_job(session.job_id)
        worker = await db.get_worker(session.worker_id)
        if job and worker and job.status in (JobStatus.RUNNING, JobStatus.PENDING):
            if not await jm.stop_job(worker, job.id):
                raise RuntimeError(f"failed to stop marimo job {job.id}")

        entry = app.state.tunnels.remove(session.tunnel_id)
        if entry:
            await asyncio.to_thread(TunnelRegistry.stop, entry)
        await db.delete_port_forward(session.tunnel_id)
        await db.delete_marimo_session(session.id)
        await app.state.cache.invalidate("tunnels:list")

    @app.post("/api/v1/marimo", status_code=201)
    async def marimo_create(payload: MarimoCreateRequest):
        worker = await resolve_worker(payload.worker_id)
        if not worker:
            raise HTTPException(status_code=404, detail=f"Worker not found: {payload.worker_id}")
        if not 1 <= payload.ready_timeout <= 300:
            raise HTTPException(status_code=422, detail="ready_timeout must be between 1 and 300 seconds")

        try:
            remote_port = await allocate_worker_port(worker)
            bind_host = tailnet_bind_host()
            local_port = allocate_local_port(bind_host)
            command = build_launch_command(
                notebook_path=payload.notebook_path,
                environment=payload.environment,
                port=remote_port,
            )
        except (ValueError, RuntimeError, OSError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        job = await jm.start_job(worker, command, name=f"marimo-{uuid4().hex[:12]}", pty_enabled=False)
        if job.status == JobStatus.FAILED:
            raise HTTPException(status_code=502, detail="failed to start marimo worker process")

        pf = PortForward(
            worker_id=worker.id,
            local_port=local_port,
            remote_port=remote_port,
            service_name=f"marimo:{payload.notebook_path}",
            created_at=int(datetime.now(timezone.utc).timestamp()),
        )
        proc = None
        registered = False
        try:
            proc = await ssh_port_forward(
                worker, local_port, remote_port, bind_host=bind_host
            )
            if proc.poll() is not None:
                raise RuntimeError(f"SSH tunnel exited immediately (code={proc.returncode})")
            pf.pid = proc.pid
            await db.insert_port_forward(pf)
            app.state.tunnels.add(TunnelProcess(
                id=pf.id,
                worker_id=worker.id,
                local_port=local_port,
                remote_port=remote_port,
                proc=proc,
                created_at=pf.created_at,
            ))
            registered = True
            await wait_until_ready(bind_host, local_port, payload.ready_timeout)
            session = MarimoSession(
                worker_id=worker.id,
                notebook_path=payload.notebook_path,
                environment=payload.environment,
                job_id=job.id,
                tunnel_id=pf.id,
                local_port=local_port,
                remote_port=remote_port,
                bind_host=bind_host,
                url=f"http://{bind_host}:{local_port}",
                status="ready",
                created_at=pf.created_at,
            )
            await db.insert_marimo_session(session)
        except BaseException as exc:
            async def cleanup_failed_start() -> None:
                entry = app.state.tunnels.remove(pf.id) if registered else None
                if entry:
                    await asyncio.to_thread(TunnelRegistry.stop, entry)
                elif proc is not None:
                    transient = TunnelProcess(
                        id=pf.id, worker_id=worker.id, local_port=local_port,
                        remote_port=remote_port, proc=proc, created_at=pf.created_at,
                    )
                    await asyncio.to_thread(TunnelRegistry.stop, transient)
                try:
                    await db.delete_port_forward(pf.id)
                except Exception:
                    log.exception("Failed to delete marimo tunnel row %s during rollback", pf.id)
                try:
                    await jm.stop_job(worker, job.id)
                except Exception:
                    log.exception("Failed to stop marimo job %s during rollback", job.id)

            cleanup_task = asyncio.create_task(cleanup_failed_start())
            try:
                await asyncio.shield(cleanup_task)
            except asyncio.CancelledError:
                await cleanup_task
            if isinstance(exc, asyncio.CancelledError):
                raise
            raise HTTPException(status_code=502, detail=f"marimo startup failed: {exc}") from exc

        await app.state.cache.invalidate("tunnels:list")
        return {**session.model_dump(mode="json"), "worker_name": worker.name}

    @app.get("/api/v1/marimo")
    async def marimo_list(worker_id: str | None = None):
        sessions = await db.list_marimo_sessions(worker_id=worker_id)
        workers = {worker.id: worker for worker in await db.list_workers()}
        result = []
        for session in sessions:
            job = await db.get_job(session.job_id)
            worker = workers.get(session.worker_id)
            if job and worker and job.status in (JobStatus.RUNNING, JobStatus.PENDING):
                job = await jm.refresh_job_status(worker, job)
            entry = app.state.tunnels.get(session.tunnel_id)
            tunnel_live = bool(entry and entry.proc.poll() is None)
            item = session.model_dump(mode="json")
            item["status"] = "ready" if job and job.status == JobStatus.RUNNING and tunnel_live else "stopped"
            item["worker_name"] = worker.name if worker else None
            result.append(item)
        return result

    @app.get("/api/v1/marimo/{session_id}")
    async def marimo_get(session_id: str):
        session = await db.get_marimo_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail=f"Marimo session not found: {session_id}")
        worker = await db.get_worker(session.worker_id)
        job = await db.get_job(session.job_id)
        if job and worker and job.status in (JobStatus.RUNNING, JobStatus.PENDING):
            job = await jm.refresh_job_status(worker, job)
        entry = app.state.tunnels.get(session.tunnel_id)
        tunnel_live = bool(entry and entry.proc.poll() is None)
        item = session.model_dump(mode="json")
        item["status"] = "ready" if job and job.status == JobStatus.RUNNING and tunnel_live else "stopped"
        item["worker_name"] = worker.name if worker else None
        return item

    @app.delete("/api/v1/marimo/{session_id}")
    async def marimo_delete(session_id: str):
        session = await db.get_marimo_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail=f"Marimo session not found: {session_id}")
        try:
            await stop_marimo_session(session)
        except RuntimeError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        return {"session_id": session_id, "removed": True}

    @app.post("/api/v1/tunnels")
    async def tunnels_create(payload: TunnelCreateRequest):
        worker = await resolve_worker(payload.worker_id)
        if not worker:
            raise HTTPException(
                status_code=404,
                detail=f"Worker not found: {payload.worker_id}",
            )

        existing = await db.list_port_forwards()
        conflict = next((p for p in existing if p.local_port == payload.local_port), None)
        if conflict:
            raise HTTPException(
                status_code=409,
                detail=f"Local port {payload.local_port} already forwarded",
            )

        pf = PortForward(
            worker_id=worker.id,
            local_port=payload.local_port,
            remote_port=payload.remote_port,
            service_name=payload.name or f"port-{payload.remote_port}",
            created_at=int(datetime.now(timezone.utc).timestamp()),
        )

        proc = None
        registered = False
        try:
            proc = await ssh_port_forward(worker, payload.local_port, payload.remote_port)
            if proc.poll() is not None:
                raise HTTPException(
                    status_code=502,
                    detail=f"SSH tunnel setup exited immediately (code={proc.returncode})",
                )
            pf.pid = proc.pid
            await db.insert_port_forward(pf)
            app.state.tunnels.add(TunnelProcess(
                id=pf.id,
                worker_id=worker.id,
                local_port=payload.local_port,
                remote_port=payload.remote_port,
                proc=proc,
                created_at=pf.created_at,
            ))
            registered = True
            await app.state.cache.invalidate("tunnels:list")
        except BaseException:
            async def cleanup_failed_tunnel() -> None:
                entry = app.state.tunnels.remove(pf.id) if registered else None
                if entry:
                    await asyncio.to_thread(TunnelRegistry.stop, entry)
                elif proc is not None:
                    transient = TunnelProcess(
                        id=pf.id, worker_id=worker.id,
                        local_port=payload.local_port, remote_port=payload.remote_port,
                        proc=proc, created_at=pf.created_at,
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

        return {
            **pf.model_dump(mode="json"),
            "worker_name": worker.name,
        }

    @app.get("/api/v1/tunnels")
    async def tunnels_list():
        async def load():
            tunnels = await db.list_port_forwards()
            workers = {w.id: w for w in await db.list_workers()}
            return [
                {
                    **t.model_dump(mode="json"),
                    "worker_name": getattr(workers.get(t.worker_id), "name", None),
                }
                for t in tunnels
            ]
        return await cached("tunnels:list", 10.0, load)

    @app.delete("/api/v1/tunnels/{tunnel_id}")
    async def tunnels_delete(tunnel_id: str):
        tunnels = await db.list_port_forwards()
        pf = next((t for t in tunnels if t.id == tunnel_id), None)
        if not pf:
            raise HTTPException(status_code=404, detail=f"Tunnel not found: {tunnel_id}")

        entry = app.state.tunnels.remove(pf.id)
        if entry:
            # Tunnel registry kills the complete process group off the event
            # loop, so tunnel teardown never blocks unrelated HTTP calls.
            await asyncio.to_thread(TunnelRegistry.stop, entry)

        await db.delete_port_forward(pf.id)
        await app.state.cache.invalidate("tunnels:list")
        return {"tunnel_id": pf.id, "removed": True}

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

