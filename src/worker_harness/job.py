"""Job lifecycle management — start, monitor, stop, retrieve logs."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from .db import Database
from .models import Failure, Job, JobStatus, Worker
from .ssh import (
    async_ssh_run,
    ssh_get_exit_code,
    ssh_read_log,
    ssh_tmux_kill,
    ssh_tmux_new,
    ssh_tmux_running,
)

log = logging.getLogger("job-manager")


class JobManager:
    """Manages job lifecycle across workers."""

    def __init__(self, db: Database) -> None:
        self.db = db

    async def start_job(
        self,
        worker: Worker,
        command: str,
        name: str | None = None,
        pty_enabled: bool = True,
    ) -> Job:
        """Start a job on a worker. Returns the Job record."""
        job = Job(
            command=command,
            worker_id=worker.id,
            status=JobStatus.RUNNING,
            pty_enabled=pty_enabled,
            started_at=int(datetime.now(timezone.utc).timestamp()),
        )
        job.tmux_session = f"wh_{name or job.id}"
        await self.db.insert_job(job)

        # Create the tmux session on the worker
        result = await ssh_tmux_new(worker, job.id, command, pty_enabled=pty_enabled)
        if result.returncode != 0:
            job.status = JobStatus.FAILED
            job.exit_code = -1
            job.finished_at = int(datetime.now(timezone.utc).timestamp())
            await self.db.update_job(job)
            log.error(f"Failed to start job {job.id} on {worker.name}: {result.stderr}")
        else:
            log.info(f"Started job {job.id} on {worker.name}: {command[:60]}...")

        return job

    async def stop_job(self, worker: Worker, job_id: str) -> bool:
        """Kill a running job's tmux session. Returns True if successful."""
        result = await ssh_tmux_kill(worker, job_id)
        output = (result.stdout or "").strip()

        # Treat idempotent/already-gone session as success.
        if result.returncode == 0 and output in ("", "stopped"):
            log.info(f"Stopped job {job_id} on {worker.name}")
            # Refresh status — mark as stopped if still running
            job = await self.db.get_job(job_id)
            if job and job.status in (JobStatus.RUNNING, JobStatus.PENDING):
                job.status = JobStatus.FAILED
                job.exit_code = -1
                job.finished_at = int(datetime.now(timezone.utc).timestamp())
                await self.db.update_job(job)
            return True

        log.warning(
            f"Failed to stop job {job_id} on {worker.name}: "
            f"rc={result.returncode}, stdout={result.stdout!r}, stderr={result.stderr!r}"
        )
        return False

    async def refresh_job_status(self, worker: Worker, job: Job) -> Job:
        """
        Check if a running job has finished and update the DB accordingly.
        Called periodically or on demand.
        """
        if job.status not in (JobStatus.RUNNING, JobStatus.PENDING):
            return job

        is_running = await ssh_tmux_running(worker, job.id)
        if not is_running:
            exit_code = await ssh_get_exit_code(worker, job.id)
            # None means retrieval failed; treat as unknown but don't falsely mark as DONE
            if exit_code is None:
                job.status = JobStatus.FAILED
            else:
                job.status = JobStatus.FAILED if exit_code != 0 else JobStatus.DONE
            job.exit_code = exit_code
            job.finished_at = int(datetime.now(timezone.utc).timestamp())
            await self.db.update_job(job)

            if job.status == JobStatus.FAILED:
                await self._record_failure(worker, job)

            log.info(f"Job {job.id} finished with exit code {exit_code}")
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