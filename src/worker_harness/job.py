"""Job lifecycle management — start, monitor, stop, retrieve logs."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from weakref import WeakKeyDictionary

from .db import Database, SlurmError
from .models import Failure, Job, JobStatus, Worker, WorkerStatus
from .ssh import (
    ssh_get_exit_code,
    ssh_read_log,
    ssh_job_started,
    ssh_job_result,
    ssh_tmux_kill,
    ssh_tmux_new,
    ssh_tmux_running,
)

log = logging.getLogger("job-manager")


class JobManager:
    """Manages job lifecycle across workers."""

    _database_dispatch_locks: WeakKeyDictionary[Database, dict[str, asyncio.Lock]] = WeakKeyDictionary()

    def __init__(self, db: Database) -> None:
        self.db = db
        self._dispatch_locks = self._database_dispatch_locks.setdefault(db, {})

    async def start_job(
        self,
        worker: Worker,
        command: str,
        name: str | None = None,
        pty_enabled: bool = True,
        notify_recipient_id: str | None = None,
    ) -> Job:
        """Start a diagnostic job immediately, without extending failure grace."""
        async with self._dispatch_lock(worker.id):
            worker = await self.db.assert_worker_accepts_jobs(worker.id)
            if worker.status != WorkerStatus.ONLINE:
                raise ValueError("worker is offline")
            job = Job(
                worker_id=worker.id,
                name=name or "",
                command=command,
                status=JobStatus.STARTING,
                gpu_count=0 if self._is_cpu_worker(worker) else 1,
                pty_enabled=pty_enabled,
                notify_recipient_id=notify_recipient_id,
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
        notify_recipient_id: str | None = None,
        submission_key: str | None = None,
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
            notify_recipient_id=notify_recipient_id,
            submission_key=submission_key,
        )
        job.tmux_session = f"wh_{job.id}"
        async with self._dispatch_lock(worker.id):
            # The insert transaction validates admission for new jobs, but returns
            # an existing same-payload submission even after its allocation ends.
            job = await self.db.insert_queued_job(job)
            await self._dispatch_worker_locked(worker.id)
            persisted = await self.db.get_job(job.id)
            if persisted is None:
                raise RuntimeError("queued job disappeared after enqueue")
            return persisted

    @staticmethod
    def _is_cpu_worker(worker: Worker) -> bool:
        return worker.slurm is not None and worker.slurm.compute_kind == "cpu"

    async def _launch_job(self, worker: Worker, job: Job) -> Job:
        if job.queue_managed or self._is_cpu_worker(worker):
            result = await ssh_tmux_new(
                worker,
                job.id,
                job.command,
                pty_enabled=job.pty_enabled,
                gpu_indices=job.gpu_indices if job.queue_managed else [],
                **({"queue_managed": job.queue_managed} if worker.slurm is not None else {}),
            )
        else:
            result = await ssh_tmux_new(
                worker,
                job.id,
                job.command,
                pty_enabled=job.pty_enabled,
                **({"queue_managed": job.queue_managed} if worker.slurm is not None else {}),
            )
        if result.returncode < 0 or result.returncode == 255:
            log.warning(
                "Job %s launch outcome on %s is unknown; retaining its claim: %s",
                job.id, worker.name, result.stderr,
            )
            return await self.db.get_job(job.id) or job
        persisted = await self.db.get_job(job.id)
        if persisted is None or persisted.status != JobStatus.STARTING:
            return persisted or job
        job = persisted
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
        return await self.db.get_job(job.id) or job

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
        async with self._dispatch_lock(worker.id):
            return await self._dispatch_worker_locked(worker.id)

    async def _dispatch_worker_locked(self, worker_id: str) -> list[Job]:
        launched: list[Job] = []
        while True:
            # Caller snapshots may predate a drain, scheduler observation or
            # heartbeat; admission always uses the persisted worker under lock.
            try:
                worker = await self.db.assert_worker_accepts_jobs(worker_id)
            except SlurmError as exc:
                if exc.code == "SLURM_WORKER_FINISHED":
                    break
                raise
            if worker.status != WorkerStatus.ONLINE:
                break
            jobs = await self.db.list_queued_jobs(worker_id)
            if self._is_cpu_worker(worker) and any(
                job.status in (JobStatus.STARTING, JobStatus.RUNNING) for job in jobs
            ):
                break
            claimed_indices = await self.db.running_queue_gpu_indices(worker_id)
            free = [
                index for index in self.free_gpu_indices(worker)
                if index not in claimed_indices
            ]
            head = next((job for job in jobs if job.status == JobStatus.PENDING), None)
            if head is None or head.gpu_count > len(free):
                break
            claimed = await self.db.claim_queued_job(
                head.id,
                free[:head.gpu_count],
                int(datetime.now(timezone.utc).timestamp()),
                worker_id=worker_id,
            )
            if claimed is None:
                # A concurrent DB observation/reassignment invalidated this
                # claim. A later normal reconciliation retries fresh state.
                break
            launched.append(await self._launch_job(worker, claimed))
        return launched

    async def reconcile_starting_job(self, worker: Worker, job: Job) -> Job:
        async with self._dispatch_lock(worker.id):
            persisted = await self.db.get_job(job.id)
            if persisted is None or persisted.status != JobStatus.STARTING:
                return persisted or job
            if worker.slurm is not None:
                result = await ssh_job_result(worker, job.id)
                if result is not None:
                    return await self.db.apply_job_runner_result(worker.id, job.id, result)
            exists = await ssh_job_started(worker, job.id)
            if exists is None:
                return persisted
            reconciled = await self.db.reconcile_starting_job(
                job.id,
                launch_observed=exists,
            )
        return reconciled or persisted

    async def stop_job(self, worker: Worker, job_id: str) -> bool:
        """Stop a job and release any queue-managed GPU assignment."""
        should_dispatch = False
        async with self._dispatch_lock(worker.id):
            job = await self.db.get_job(job_id)
            if job and job.worker_id != worker.id:
                return False
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
                if worker.slurm is not None:
                    # A successful control request is not completion. Only
                    # reaped-descendant proof can release a cluster claim.
                    proof = await ssh_job_result(worker, job_id)
                    success = proof is not None
                    if success and job is not None:
                        job = await self.db.apply_job_runner_result(worker.id, job_id, proof)
                        should_dispatch = job.queue_managed
                else:
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
                if not success:
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
        async with self._dispatch_lock(worker.id):
            persisted = await self.db.get_job(job.id)
            if persisted is None:
                return job
            job = persisted
            if job.worker_id != worker.id:
                return job
            if job.status not in (JobStatus.RUNNING, JobStatus.PENDING):
                return job
            if job.queue_managed and job.status == JobStatus.PENDING:
                return job

            if worker.slurm is not None:
                proof = await ssh_job_result(worker, job.id)
                if proof is None:
                    return job
                job = await self.db.apply_job_runner_result(worker.id, job.id, proof)
                exit_code = proof.exit_code
            else:
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
                job = await self.db.get_job(job.id) or job

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