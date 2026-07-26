"""FastAPI-based HTTP server for worker heartbeats and orchestration API."""

from __future__ import annotations

import asyncio
import json
import logging
from collections import Counter
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Awaitable, Callable, TypeVar
from uuid import uuid4

import httpx
from fastapi import FastAPI, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, ValidationError

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
from .lanes import LaneTimeout, WorkerLanes
from .metrics import Metrics, set_global_metrics
from .models import (
    JobStatus,
    PiDelegation,
    PiIngestPayload,
    PiSession,
    PiSessionEvent,
    PiSessionState,
    PiSessionType,
    PortForward,
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


class PiDelegationCreateRequest(BaseModel):
    task: str
    worker_id: str | None = None
    parent_session_id: str | None = None
    cwd: str = ""
    # 0 disables the timeout gate (spec §8.1: duration is the only session-level
    # policy knob; unacknowledged expiry becomes termination_unknown).
    timeout_seconds: int = 0
    # sync=true blocks until the child reaches a settled/terminal state or the
    # wait cap elapses; it never fabricates completion (spec §8.1).
    sync: bool = False


class PiPromptRequest(BaseModel):
    message: str


# Timeout gate (spec §8.1): an expired delegation is cancelled through the
# worker relay; without an acknowledgement the terminal state is unknown,
# never a fabricated cancellation.
PI_DELEGATION_TERMINAL_STATES = {
    PiSessionState.STOPPED,
    PiSessionState.FAILED,
    PiSessionState.TERMINATION_UNKNOWN,
}


async def sweep_expired_pi_delegations(db: Database, relay_request_fn, now: int | None = None) -> None:
    """Expire delegations past their timeout gate.

    ``relay_request_fn`` matches the orchestrator's worker relay client
    signature ``(worker, method, path, payload=None) -> dict``.
    """
    now = now if now is not None else int(datetime.now(timezone.utc).timestamp())
    for delegation in await db.list_pi_delegations():
        if (
            delegation.timeout_seconds <= 0
            or delegation.state in PI_DELEGATION_TERMINAL_STATES
            or now - delegation.created_at < delegation.timeout_seconds
        ):
            continue
        session = await db.get_pi_session(delegation.child_session_id)
        if not session:
            continue
        worker = await db.get_worker(delegation.worker_id)
        ack = False
        if worker:
            try:
                await relay_request_fn(worker, "POST", f"/v1/sessions/{session.id}:cancel")
                ack = True
            except Exception as exc:
                log.warning("Pi delegation %s timeout cancel was not acknowledged: %s", delegation.id, exc)
        if ack:
            session.state = PiSessionState.STOPPED
            session.detail = "delegation timed out"
            event_type = "timeout"
            delegation.state = PiSessionState.STOPPED
        else:
            session.state = PiSessionState.TERMINATION_UNKNOWN
            session.detail = "delegation timed out; worker unreachable"
            event_type = "timeout_unknown"
            delegation.state = PiSessionState.TERMINATION_UNKNOWN
        session.updated_at = now
        delegation.completed_at = now
        await db.update_pi_session(session)
        await db.update_pi_delegation(delegation)
        await db.insert_pi_session_event(PiSessionEvent(
            session_id=session.id, event_type=event_type, payload={"delegation_id": delegation.id}, created_at=now
        ))


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start reliability background work and deterministically clean up.

    Database ownership remains with the caller (`serve`), but every persistent
    tunnel and every queued lane waiter belongs to this FastAPI app.
    """
    app.state.reaper_task = asyncio.create_task(reap_loop(app))
    sweeper = getattr(app.state, "pi_delegation_sweeper", None)
    if sweeper is not None:
        app.state.pi_sweeper_task = asyncio.create_task(sweeper())
    try:
        yield
    finally:
        app.state.reaper_task.cancel()
        if sweeper is not None:
            app.state.pi_sweeper_task.cancel()
            try:
                await app.state.pi_sweeper_task
            except asyncio.CancelledError:
                pass
        try:
            await app.state.reaper_task
        except asyncio.CancelledError:
            pass
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

    @app.post("/pi/worker/{worker_id}/sessions/{session_id}/events")
    async def worker_pi_session_events(worker_id: str, session_id: str, payload: PiIngestPayload):
        # Workers may only upload events for sessions they own. The orchestrator's
        # session table is the single writer of the durable projection; the
        # reported state is layered on top so the projection stays truthful even
        # when nothing else is happening on the wire.
        if payload.session_id != session_id:
            raise HTTPException(status_code=422, detail="session_id mismatch between path and payload")
        worker = await db.get_worker(worker_id)
        if not worker:
            raise HTTPException(status_code=404, detail="worker not found")
        session = await db.get_pi_session(session_id)
        if not session or session.worker_id != worker_id:
            raise HTTPException(status_code=404, detail="session not found for worker")
        try:
            persisted = await db.apply_pi_ingest(worker_id, payload)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {
            "session_id": session_id,
            "events_persisted": len(persisted),
            "state": payload.state.value if payload.state else None,
        }

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
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"jobs_received": len(payload.jobs), "jobs_applied": applied}

    return app


def create_app(db: Database) -> FastAPI:
    """Create the privileged control API (kept as the public test factory)."""
    app = FastAPI(title="Worker Harness Control API", lifespan=lifespan)
    jm = JobManager(db)

    # Shared reliability services. They are attached before lifespan starts so
    # handlers, reaper, and /api/v1/_stats all see one coherent state.
    app.state.cache = TTLCache()
    app.state.lanes = WorkerLanes(max_concurrent=4, max_queue=32)
    app.state.rate_limiter = AgentRateLimiter(capacity=10, refill_rate=1.0)
    app.state.metrics = Metrics()
    app.state.tunnels = TunnelRegistry()
    set_global_metrics(app.state.metrics)
    set_lanes(app.state.lanes)

    @app.get("/health")
    async def health():
        """Control-plane liveness endpoint (separate from registration)."""
        return {"status": "healthy", "ts": datetime.now(timezone.utc).isoformat()}

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
        # Worker heartbeat registration and /health are infrastructure traffic,
        # not agent traffic. Rate-limit only the orchestration API.
        if request.url.path.startswith("/api/v1/"):
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

    async def worker_relay_request(worker, method: str, path: str, payload: dict | None = None) -> dict:
        if worker.status != WorkerStatus.ONLINE:
            raise HTTPException(status_code=409, detail=f"worker is {worker.status.value}")
        if not worker.pi_relay_available or not worker.pi_relay_port:
            raise HTTPException(status_code=409, detail="worker does not advertise a Pi relay")
        # Userspace Tailscale Serve exposes the worker's Tailnet IP; prefer it
        # over MagicDNS because an operator's resolver may not have MagicDNS.
        url = f"http://{worker.worker_ip}:{worker.pi_relay_port}{path}"
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.request(method, url, json=payload)
        except httpx.HTTPError as exc:
            raise HTTPException(status_code=502, detail=f"worker Pi relay unavailable: {exc}") from exc
        if response.status_code >= 400:
            raise HTTPException(status_code=response.status_code, detail=response.text)
        return response.json()

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
    async def workers_prune(minutes: int = Query(5, ge=0)):
        import time as _time

        cutoff = int(_time.time()) - (minutes * 60)
        removed = await db.prune_workers(cutoff)
        await invalidate_workers_cache()
        return {"removed": removed, "minutes": minutes}

    @app.get("/api/v1/pi/sessions")
    async def pi_sessions_list(worker_id: str | None = None):
        return [session.model_dump(mode="json") for session in await db.list_pi_sessions(worker_id)]

    @app.get("/api/v1/pi/sessions/{session_id}")
    async def pi_session_get(session_id: str):
        session = await db.get_pi_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Pi session not found")
        return session.model_dump(mode="json")

    @app.get("/api/v1/pi/sessions/{session_id}/events")
    async def pi_session_events(session_id: str):
        if not await db.get_pi_session(session_id):
            raise HTTPException(status_code=404, detail="Pi session not found")
        return [event.model_dump(mode="json") for event in await db.list_pi_session_events(session_id)]

    @app.post("/api/v1/pi/delegations", status_code=201)
    async def pi_delegations_create(payload: PiDelegationCreateRequest):
        if not payload.task.strip():
            raise HTTPException(status_code=422, detail="task must not be empty")
        if payload.worker_id:
            worker = await resolve_worker(payload.worker_id)
        else:
            workers = await db.list_workers()
            worker = next(
                (item for item in workers if item.status == WorkerStatus.ONLINE and item.pi_relay_available),
                None,
            )
        if not worker:
            raise HTTPException(status_code=404, detail="no Pi-capable online worker found")

        now = int(datetime.now(timezone.utc).timestamp())
        session = PiSession(
            worker_id=worker.id,
            parent_session_id=payload.parent_session_id,
            session_type=PiSessionType.DELEGATED,
            state=PiSessionState.STARTING,
            task=payload.task,
            cwd=payload.cwd,
            created_at=now,
            updated_at=now,
        )
        delegation = PiDelegation(
            worker_id=worker.id,
            parent_session_id=payload.parent_session_id,
            child_session_id=session.id,
            task=payload.task,
            state=PiSessionState.STARTING,
            timeout_seconds=max(0, payload.timeout_seconds),
            created_at=now,
        )
        await db.insert_pi_session(session)
        await db.insert_pi_delegation(delegation)
        await db.insert_pi_session_event(PiSessionEvent(
            session_id=session.id, event_type="starting", payload={"worker_id": worker.id}, created_at=now
        ))
        try:
            remote = await worker_relay_request(
                worker,
                "POST",
                "/v1/sessions",
                {"session_id": session.id, "parent_session_id": payload.parent_session_id, "task": payload.task,
                 "cwd": payload.cwd or None},
            )
        except HTTPException as exc:
            session.state = PiSessionState.FAILED
            session.detail = str(exc.detail)
            session.updated_at = int(datetime.now(timezone.utc).timestamp())
            delegation.state = PiSessionState.FAILED
            delegation.completed_at = session.updated_at
            await db.update_pi_session(session)
            await db.update_pi_delegation(delegation)
            await db.insert_pi_session_event(PiSessionEvent(
                session_id=session.id, event_type="failed", payload={"detail": session.detail}, created_at=session.updated_at
            ))
            raise
        session.state = PiSessionState(remote["state"])
        session.tmux_session = remote.get("tmux_session", "")
        session.detail = remote.get("detail", "")
        session.updated_at = remote.get("updated_at", int(datetime.now(timezone.utc).timestamp()))
        delegation.state = session.state
        await db.update_pi_session(session)
        await db.update_pi_delegation(delegation)
        await db.insert_pi_session_event(PiSessionEvent(
            session_id=session.id, event_type=session.state.value, payload=remote, created_at=session.updated_at
        ))
        if payload.sync:
            # A zero duration disables the desired-state timeout, but a sync
            # HTTP request still needs a bounded wait.  Its 10-minute cap is
            # reported as unsettled rather than inventing a child result.
            wait_cap = payload.timeout_seconds if payload.timeout_seconds > 0 else 600
            settled = await _wait_for_pi_session(session.id, wait_cap)
            # The periodic sweeper has a deliberately coarse cadence.  A sync
            # request owns an exact duration promise, so apply the same timeout
            # gate before returning when its requested deadline elapsed.
            if not settled.settled and payload.timeout_seconds > 0:
                await sweep_expired_pi_delegations(db, worker_relay_request)
                settled = await _read_pi_wait_result(session.id)
            delegation = await db.get_pi_delegation(delegation.id)
            return {
                "delegation_id": delegation.id,
                "child_session_id": session.id,
                "state": settled.session.state.value if settled.session else "unknown",
                "settled": settled.settled,
                "session": settled.session.model_dump(mode="json") if settled.session else None,
                "delegation": delegation.model_dump(mode="json") if delegation else None,
                "events": [event.model_dump(mode="json") for event in settled.events],
                "status_url": f"/api/v1/pi/delegations/{delegation.id}",
            }
        return {"delegation_id": delegation.id, "child_session_id": session.id, "state": session.state.value,
                "status_url": f"/api/v1/pi/delegations/{delegation.id}"}

    class _PiWaitResult:
        def __init__(self, session, events, settled: bool):
            self.session = session
            self.events = events
            self.settled = settled

    async def _read_pi_wait_result(session_id: str) -> "_PiWaitResult":
        session = await db.get_pi_session(session_id)
        events = await db.list_pi_session_events(session_id)
        # termination_unknown is a terminal projection but not a known child
        # completion: surface it immediately without claiming a result.
        settled = bool(session and session.state in {
            PiSessionState.IDLE,
            PiSessionState.STOPPED,
            PiSessionState.FAILED,
        })
        return _PiWaitResult(session, events, settled=settled)

    async def _wait_for_pi_session(session_id: str, wait_cap_seconds: int) -> "_PiWaitResult":
        loop = asyncio.get_running_loop()
        deadline = loop.time() + max(1, wait_cap_seconds)
        while loop.time() < deadline:
            result = await _read_pi_wait_result(session_id)
            if result.settled or (
                result.session and result.session.state == PiSessionState.TERMINATION_UNKNOWN
            ):
                return result
            await asyncio.sleep(min(2, max(0.01, deadline - loop.time())))
        return await _read_pi_wait_result(session_id)

    async def _pi_delegation_sweeper() -> None:
        while True:
            try:
                await sweep_expired_pi_delegations(db, worker_relay_request)
            except Exception:
                log.exception("Pi delegation sweeper iteration failed")
            await asyncio.sleep(30)

    app.state.pi_delegation_sweeper = _pi_delegation_sweeper

    @app.get("/api/v1/pi/delegations/{delegation_id}")
    async def pi_delegation_get(delegation_id: str):
        delegation = await db.get_pi_delegation(delegation_id)
        if not delegation:
            raise HTTPException(status_code=404, detail="Pi delegation not found")
        return delegation.model_dump(mode="json")

    @app.post("/api/v1/pi/sessions/{session_id}:prompt")
    async def pi_session_prompt(session_id: str, payload: PiPromptRequest):
        session = await db.get_pi_session(session_id)
        if not session or not session.worker_id:
            raise HTTPException(status_code=404, detail="Pi session not found")
        worker = await db.get_worker(session.worker_id)
        if not worker:
            raise HTTPException(status_code=404, detail="worker not found")
        remote = await worker_relay_request(worker, "POST", f"/v1/sessions/{session_id}:prompt", {"message": payload.message})
        session.state = PiSessionState(remote["state"])
        session.detail = remote.get("detail", "")
        session.updated_at = remote.get("updated_at", int(datetime.now(timezone.utc).timestamp()))
        await db.update_pi_session(session)
        await db.insert_pi_session_event(PiSessionEvent(
            session_id=session.id, event_type="prompt", payload={"message": payload.message}, created_at=session.updated_at
        ))
        return session.model_dump(mode="json")

    @app.post("/api/v1/pi/sessions/{session_id}:cancel")
    async def pi_session_cancel(session_id: str):
        session = await db.get_pi_session(session_id)
        if not session or not session.worker_id:
            raise HTTPException(status_code=404, detail="Pi session not found")
        worker = await db.get_worker(session.worker_id)
        if not worker:
            raise HTTPException(status_code=404, detail="worker not found")
        remote = await worker_relay_request(worker, "POST", f"/v1/sessions/{session_id}:cancel")
        session.state = PiSessionState(remote["state"])
        session.detail = remote.get("detail", "")
        session.updated_at = remote.get("updated_at", int(datetime.now(timezone.utc).timestamp()))
        await db.update_pi_session(session)
        await db.insert_pi_session_event(PiSessionEvent(
            session_id=session.id, event_type="cancelled", payload=remote, created_at=session.updated_at
        ))
        return session.model_dump(mode="json")

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
            if job.status in (JobStatus.RUNNING, JobStatus.PENDING):
                worker = workers.get(job.worker_id or "")
                if worker:
                    job = await jm.refresh_job_status(worker, job)
            item = job.model_dump(mode="json")
            worker_ref = workers.get(job.worker_id or "")
            item["worker_name"] = worker_ref.name if worker_ref else None
            refreshed.append(item)

        return refreshed

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
        await app.state.cache.invalidate("tunnels:list")

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


# Compatibility alias for callers that previously started the combined server.
# It now intentionally starts registration only.
run_heartbeat_server = run_registration_server
