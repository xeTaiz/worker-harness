"""Job lifecycle management — start, monitor, stop, retrieve logs."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from .db import Database
from .models import Failure, Job, JobKind, JobStatus, Worker, WorkerStatus
from .ssh import (
    ssh_get_exit_code,
    ssh_read_log,
    ssh_tmux_exists,
    ssh_tmux_kill,
    ssh_tmux_new,
    ssh_tmux_running,
)

log = logging.getLogger("job-manager")


class JobManager:
    """Manages job lifecycle across workers."""

    def __init__(self, db: Database) -> None:
        self.db = db
        self._dispatch_locks: dict[str, asyncio.Lock] = {}
    async def start_job(
        self,
        worker: Worker,
        command: str,
        name: str | None = None,
        pty_enabled: bool = True,
    ) -> Job:
        """Start a job immediately on a worker."""
        job = Job(
            worker_id=worker.id,
            name=name or "",
            command=command,
            status=JobStatus.STARTING,
            pty_enabled=pty_enabled,
            started_at=int(datetime.now(timezone.utc).timestamp()),
        )
        job.tmux_session = f"wh_{job.id}"
        await self.db.insert_job(job)
        return await self._launch_job(worker, job)

    async def enqueue_job(
        self,
        worker: Worker,
        command: str,
        name: str,
        expected_seconds: int,
        gpu_count: int = 1,
        pty_enabled: bool = True,
    ) -> Job:
        job = Job(
            worker_id=worker.id,
            name=name,
            command=command,
            status=JobStatus.PENDING,
            queue_managed=True,
            expected_seconds=expected_seconds,
            gpu_count=gpu_count,
            pty_enabled=pty_enabled,
        )
        job.tmux_session = f"wh_{job.id}"
        await self.db.insert_queued_job(job)
        await self.dispatch_worker(worker)
        persisted = await self.db.get_job(job.id)
        if persisted is None:
            raise RuntimeError("queued job disappeared after enqueue")
        return persisted

    async def _launch_job(self, worker: Worker, job: Job) -> Job:
        if job.queue_managed:
            result = await ssh_tmux_new(
                worker,
                job.id,
                job.command,
                pty_enabled=job.pty_enabled,
                gpu_indices=job.gpu_indices,
            )
        else:
            result = await ssh_tmux_new(
                worker,
                job.id,
                job.command,
                pty_enabled=job.pty_enabled,
            )
        if result.returncode != 0:
            job.status = JobStatus.FAILED
            job.exit_code = -1
            job.finished_at = int(datetime.now(timezone.utc).timestamp())
            await self.db.update_job(job)
            log.error("Failed to start job %s on %s: %s", job.id, worker.name, result.stderr)
        else:
            job.status = JobStatus.RUNNING
            await self.db.update_job(job)
            log.info("Started job %s on %s: %s...", job.id, worker.name, job.command[:60])
        return job

    def free_gpu_indices(self, worker: Worker) -> list[int]:
        if worker.status != WorkerStatus.ONLINE:
            return []
        free: list[int] = []
        for index in range(worker.gpu_count):
            explicit_busy = worker.gpu_busy[index] if index < len(worker.gpu_busy) else None
            if explicit_busy is not None:
                if not explicit_busy:
                    free.append(index)
                continue
            if index >= len(worker.gpu_vram_gb) or index >= len(worker.gpu_used_vram_gb):
                continue
            total = worker.gpu_vram_gb[index]
            used = worker.gpu_used_vram_gb[index]
            if used <= max(0.5, total * 0.1):
                free.append(index)
        return free

    def _dispatch_lock(self, worker_id: str) -> asyncio.Lock:
        return self._dispatch_locks.setdefault(worker_id, asyncio.Lock())

    async def dispatch_worker(self, worker: Worker) -> list[Job]:
        if worker.status != WorkerStatus.ONLINE:
            return []
        launched: list[Job] = []
        async with self._dispatch_lock(worker.id):
            free = self.free_gpu_indices(worker)
            claimed_indices = await self.db.running_queue_gpu_indices(worker.id)
            free = [index for index in free if index not in claimed_indices]
            while True:
                jobs = await self.db.list_queued_jobs(worker.id)
                head = next((job for job in jobs if job.status == JobStatus.PENDING), None)
                if head is None or head.gpu_count > len(free):
                    break
                assigned = free[:head.gpu_count]
                claimed = await self.db.claim_queued_job(
                    head.id,
                    assigned,
                    int(datetime.now(timezone.utc).timestamp()),
                )
                if claimed is None:
                    continue
                result = await self._launch_job(worker, claimed)
                launched.append(result)
                if result.status == JobStatus.RUNNING:
                    free = free[head.gpu_count:]
        return launched

    async def reconcile_starting_job(self, worker: Worker, job: Job) -> Job:
        async with self._dispatch_lock(worker.id):
            persisted = await self.db.get_job(job.id)
            if persisted is None or persisted.status != JobStatus.STARTING:
                return persisted or job
            exists = await ssh_tmux_exists(worker, job.id)
            if exists is None:
                return persisted
            reconciled = await self.db.reconcile_starting_job(
                job.id,
                session_exists=exists,
            )
        return reconciled or persisted

    async def stop_job(self, worker: Worker, job_id: str) -> bool:
        """Stop a job and release any queue-managed GPU assignment."""
        should_dispatch = False
        async with self._dispatch_lock(worker.id):
            job = await self.db.get_job(job_id)
            if job and job.status in (JobStatus.DONE, JobStatus.FAILED):
                return True
            if job and job.queue_managed and job.status == JobStatus.PENDING:
                stopped = await self.db.fail_pending_job(
                    job_id,
                    int(datetime.now(timezone.utc).timestamp()),
                )
                should_dispatch = stopped is not None
                success = should_dispatch
            else:
                result = await ssh_tmux_kill(worker, job_id)
                output = (result.stdout or "").strip()
                success = result.returncode == 0 and output in ("", "stopped")
                if success:
                    log.info("Stopped job %s on %s", job_id, worker.name)
                    job = await self.db.get_job(job_id)
                    if job and job.status in (
                        JobStatus.STARTING,
                        JobStatus.RUNNING,
                        JobStatus.PENDING,
                    ):
                        job.status = JobStatus.FAILED
                        job.exit_code = -1
                        job.finished_at = int(datetime.now(timezone.utc).timestamp())
                        await self.db.update_job(job)
                        should_dispatch = job.queue_managed
                else:
                    log.warning(
                        "Failed to stop job %s on %s: rc=%s, stdout=%r, stderr=%r",
                        job_id,
                        worker.name,
                        result.returncode,
                        result.stdout,
                        result.stderr,
                    )
        if should_dispatch:
            await self.dispatch_worker(worker)
        return success

    async def refresh_job_status(self, worker: Worker, job: Job) -> Job:
        """
        Check if a running job has finished and update the DB accordingly.
        Called periodically or on demand.
        """
        # The worker-local relay is authoritative for delegated-child job
        # state. SSH probing is retained only for SSH-created jobs.
        if job.kind != JobKind.SSH or job.status not in (JobStatus.RUNNING, JobStatus.PENDING):
            return job
        if job.queue_managed and job.status == JobStatus.PENDING:
            return job

        is_running = await ssh_tmux_running(worker, job.id)
        if is_running is None or is_running:
            return job

        exit_code = await ssh_get_exit_code(worker, job.id)
        if exit_code is None:
            log.warning(
                "Job %s on %s reported completion but its exit code is unavailable",
                job.id,
                worker.name,
            )
            return job

        job.status = JobStatus.FAILED if exit_code != 0 else JobStatus.DONE
        job.exit_code = exit_code
        job.finished_at = int(datetime.now(timezone.utc).timestamp())
        await self.db.update_job(job)

        if job.status == JobStatus.FAILED:
            await self._record_failure(worker, job)
        if job.queue_managed:
            await self.dispatch_worker(worker)

        log.info("Job %s finished with exit code %s", job.id, exit_code)
        return job

    async def get_logs(
        self,
        worker: Worker,
        job_id: str,
        tail: int | None = 10,
        head: int | None = None,
    ) -> str:
        """Retrieve job logs with tail/head slicing."""
        return await ssh_read_log(worker, job_id, tail=tail, head=head)

    async def _record_failure(self, worker: Worker, job: Job) -> None:
        """Record a failed job in the failures table."""
        # Grab a one-line summary from the end of the log
        summary = await ssh_read_log(worker, job.id, tail=1)
        summary = summary.strip().replace("\n", " ")[:200]

        failure = Failure(
            job_id=job.id,
            worker_id=worker.id,
            exit_code=job.exit_code or -1,
            timestamp=int(datetime.now(timezone.utc).timestamp()),
            summary=summary,
        )
        await self.db.insert_failure(failure)