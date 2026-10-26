"""Reserved allocation control and durable ordinary-job notifications.

Recipients are routing identifiers under the current single-user trust model,
not an ownership or privacy boundary. Worker-scoped tokens never grant control.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Callable

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from .db import Database
from .models import SlurmObservation, SlurmReservationRequest


def slurm_http_error(exc: ValueError) -> HTTPException:
    """Preserve machine-readable conflicts on ordinary admission surfaces too."""
    code = getattr(exc, "code", None)
    if code:
        detail = {"code": code, "message": str(exc)}
        if code == "SLURM_WORKER_FINISHED":
            detail["guidance"] = "Submit a fresh allocation with ordinary enqueue."
        return HTTPException(status_code=409, detail=detail)
    return HTTPException(status_code=422, detail=str(exc))


async def stream_job_events(
    request: Request, db: Database, recipient_id: str, after: int = 0,
) -> AsyncIterator[str]:
    """Replay persisted events, then tail without a consumer/acknowledgement API."""
    cursor = after
    heartbeat_at = asyncio.get_running_loop().time() + 15.0
    while not await request.is_disconnected():
        events = await db.list_job_events(recipient_id, after=cursor, limit=500)
        if events:
            for event in events:
                cursor = event.sequence
                data = json.dumps(event.model_dump(mode="json"), separators=(",", ":"))
                yield f"id: {cursor}\nevent: job-event\ndata: {data}\n\n"
            heartbeat_at = asyncio.get_running_loop().time() + 15.0
            continue
        now = asyncio.get_running_loop().time()
        if now >= heartbeat_at:
            yield ": keep-alive\n\n"
            heartbeat_at = now + 15.0
        await asyncio.sleep(0.25)


def mount_slurm_routes(app: FastAPI, db: Database, require_control: Callable) -> None:
    """Attach narrow allocation controls to the existing authenticated API."""
    dependencies = [Depends(require_control)]

    @app.post("/api/v1/workers/reservations", dependencies=dependencies)
    async def reserve_worker(payload: SlurmReservationRequest):
        try:
            worker = await db.reserve_slurm_worker(payload)
        except ValueError as exc:
            raise slurm_http_error(exc) from exc
        for key in ("workers:list", "workers:summary"):
            await app.state.cache.invalidate(key)
        return {"worker_id": worker.id}

    @app.patch("/api/v1/workers/{worker_id}/slurm", dependencies=dependencies)
    async def observe_worker(worker_id: str, payload: SlurmObservation):
        try:
            worker = await db.observe_slurm_worker(worker_id, payload)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="Slurm worker not found") from exc
        except ValueError as exc:
            raise slurm_http_error(exc) from exc
        for key in ("workers:list", "workers:summary"):
            await app.state.cache.invalidate(key)
        return worker.model_dump(mode="json")

    @app.get("/api/v1/job-events", dependencies=dependencies)
    async def job_events(
        request: Request,
        recipient_id: str = Query(min_length=1, max_length=256),
        after: int = Query(default=0, ge=0),
    ):
        return StreamingResponse(
            stream_job_events(request, db, recipient_id, after),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )
