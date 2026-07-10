"""FastAPI-based HTTP server for worker heartbeats and orchestration API."""

from __future__ import annotations

import asyncio
import logging
import subprocess
from collections import Counter
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ValidationError

from .db import Database
from .job import JobManager
from .models import JobStatus, PortForward, WorkerRegistration
from .ssh import async_ssh_run, ssh_port_forward, ssh_upload_bytes, ssh_download_bytes

log = logging.getLogger("heartbeat-server")

# In-memory tunnel process handles for the current server process.
_tunnel_processes: dict[str, subprocess.Popen] = {}


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


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: DB is already connected by the caller.
    yield
    # Shutdown: caller handles db.close().


def create_app(db: Database) -> FastAPI:
    app = FastAPI(title="Worker Harness Heartbeat API", lifespan=lifespan)
    jm = JobManager(db)

    async def resolve_worker(worker_id_or_name: str):
        worker = await db.get_worker(worker_id_or_name)
        if worker:
            return worker
        workers = await db.list_workers()
        return next((w for w in workers if w.name == worker_id_or_name), None)

    # ── Heartbeat/registration endpoints (existing behavior) ─────────────────

    @app.post("/register")
    async def register(reg: WorkerRegistration):
        """
        Full registration or heartbeat from a worker.
        Workers send this on startup and every N seconds thereafter.
        """
        try:
            worker = await db.upsert_worker(reg)
            log.info(
                f"Worker registered/updated: {worker.name} "
                f"(id={worker.id}, ip={worker.worker_ip}, dns={getattr(worker, 'dns_name', '')}, gpus={worker.gpu_count})"
            )
            return {"status": "ok", "worker_id": worker.id}
        except ValidationError as e:
            log.error(f"Invalid registration payload: {e}")
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=str(e),
            )
        except Exception as e:
            log.error(f"Registration failed: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=str(e),
            )

    @app.get("/health")
    async def health():
        return {"status": "healthy", "ts": datetime.now(timezone.utc).isoformat()}

    # ── Orchestration API (/api/v1) ──────────────────────────────────────────

    @app.get("/api/v1/workers")
    async def workers_list():
        workers = await db.list_workers()
        return [w.model_dump(mode="json") for w in workers]

    @app.get("/api/v1/workers/summary")
    async def workers_summary():
        workers = await db.list_workers()
        status_counts = Counter(w.status.value for w in workers)
        return {
            "total": len(workers),
            "online": status_counts.get("online", 0),
            "offline": status_counts.get("offline", 0),
            "draining": status_counts.get("draining", 0),
        }

    @app.delete("/api/v1/workers/prune")
    async def workers_prune(minutes: int = Query(5, ge=0)):
        import time as _time

        cutoff = int(_time.time()) - (minutes * 60)
        removed = await db.prune_workers(cutoff)
        return {"removed": removed, "minutes": minutes}

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
    ):
        job_status = None
        if status_value:
            try:
                job_status = JobStatus(status_value)
            except ValueError:
                raise HTTPException(status_code=400, detail=f"Invalid status: {status_value}")

        jobs = await db.list_jobs(worker_id=worker_id, status=job_status)
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
        pf.pid = proc.pid
        await db.insert_port_forward(pf)
        _tunnel_processes[pf.id] = proc

        return {
            **pf.model_dump(mode="json"),
            "worker_name": worker.name,
        }

    @app.get("/api/v1/tunnels")
    async def tunnels_list():
        tunnels = await db.list_port_forwards()
        workers = {w.id: w for w in await db.list_workers()}
        return [
            {
                **t.model_dump(mode="json"),
                "worker_name": getattr(workers.get(t.worker_id), "name", None),
            }
            for t in tunnels
        ]

    @app.delete("/api/v1/tunnels/{tunnel_id}")
    async def tunnels_delete(tunnel_id: str):
        tunnels = await db.list_port_forwards()
        pf = next((t for t in tunnels if t.id == tunnel_id), None)
        if not pf:
            raise HTTPException(status_code=404, detail=f"Tunnel not found: {tunnel_id}")

        proc = _tunnel_processes.pop(pf.id, None)
        if proc:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()

        await db.delete_port_forward(pf.id)
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


async def run_heartbeat_server(
    db: Database,
    host: str = "0.0.0.0",
    port: int = 12888,
) -> None:
    """Run the HTTP server using uvicorn."""
    import uvicorn

    app = create_app(db)
    config = uvicorn.Config(app, host=host, port=port, log_level="warning")
    server = uvicorn.Server(config)
    await server.serve()
