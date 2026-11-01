"""Job lifecycle management — start, monitor, stop, idle handoff and logs.

Termination is established only by the per-job supervisor's durable result
(``result.json`` with every descendant reaped). Allocations are released in
the same database transaction that records confirmed termination; an
unreachable worker or a vanished supervisor keeps the reservation.
"""

from __future__ import annotations

import asyncio
import logging
from time import monotonic_ns
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Awaitable, Callable, Literal

from .db import Database
from .idle import IDLE_TIMEOUT_SECONDS, GpuIdleTracker
from .models import (
    ExecutionState,
    Failure,
    Job,
    JobPriority,
    JobStatus,
    TerminationReason,
    Worker,
    WorkerStatus,
)
from .queue_board import available_gpu_indices
from .ssh import (
    JobProbe,
    ssh_job_launch,
    ssh_job_probe,
    ssh_job_stop,
    ssh_read_log,
    ssh_read_log_range,
)

log = logging.getLogger("job-manager")

SETUP_JOB_NAME = "Setup job"


def _now() -> int:
    return int(datetime.now(timezone.utc).timestamp())


@dataclass(frozen=True)
class StopOutcome:
    state: Literal["stopped", "already_terminal", "unconfirmed", "not_found"]
    job: Job | None


class JobManager:
    """Manages job lifecycle across workers."""

    def __init__(
        self,
        db: Database,
        *,
        idle_tracker: GpuIdleTracker | None = None,
        on_job_terminal: Callable[[Job], Awaitable[None]] | None = None,
    ) -> None:
        self.db = db
        self.idle_tracker = idle_tracker or GpuIdleTracker(db.instance_id)
        self.on_job_terminal = on_job_terminal
        self._dispatch_locks: dict[str, asyncio.Lock] = {}

    async def start_job(
        self,
        worker: Worker,
        command: str,
        *,
        owner_user_id: str,
        actor_id: str,
        name: str = SETUP_JOB_NAME,
        expected_seconds: int = 0,
        priority: JobPriority = JobPriority.NORMAL,
        priority_note: str = "",
        deadline_at: int | None = None,
        origin_session_id: str | None = None,
        pty_enabled: bool = True,
    ) -> Job:
        """Start a CPU/setup job immediately; it never takes a GPU slot."""
        job = Job(
            worker_id=worker.id,
            name=name,
            command=command,
            status=JobStatus.STARTING,
            pty_enabled=pty_enabled,
            started_at=_now(),
            owner_user_id=owner_user_id,
            submitted_by_actor_id=actor_id,
            expected_seconds=expected_seconds,
            priority=priority,
            priority_note=priority_note,
            deadline_at=deadline_at,
            origin_session_id=origin_session_id,
            gpu_count=0,
        )
        job.tmux_session = f"wh_{job.id}"
        async with self._dispatch_lock(worker.id):
            await self.db.insert_job(job)
            return await self._launch_job(worker, job)

    async def enqueue_job(
        self,
        worker: Worker,
        command: str,
        *,
        owner_user_id: str,
        actor_id: str,
        name: str,
        expected_seconds: int,
        gpu_count: int = 1,
        priority: JobPriority = JobPriority.NORMAL,
        priority_note: str = "",
        deadline_at: int | None = None,
        origin_session_id: str | None = None,
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
            owner_user_id=owner_user_id,
            submitted_by_actor_id=actor_id,
            priority=priority,
            priority_note=priority_note,
            deadline_at=deadline_at,
            origin_session_id=origin_session_id,
        )
        job.tmux_session = f"wh_{job.id}"
        await self.db.insert_queued_job(job)
        await self.dispatch_worker(worker)
        persisted = await self.db.get_job(job.id)
        if persisted is None:
            raise RuntimeError("queued job disappeared after enqueue")
        return persisted

    async def _launch_job(self, worker: Worker, job: Job) -> Job:
        # CPU jobs receive an explicitly empty CUDA_VISIBLE_DEVICES.
        result = await ssh_job_launch(worker, job.id, job.command, gpu_indices=list(job.gpu_indices))
        if result.returncode < 0 or result.returncode == 255:
            log.warning(
                "Job %s launch outcome on %s is unknown; retaining its claim: %s",
                job.id, worker.name, result.stderr,
            )
            return job
        if result.returncode != 0:
            log.error("Failed to start job %s on %s: %s", job.id, worker.name, result.stderr)
            failed = await self.db.fail_job_launch(job.id, finished_at=_now())
            return failed or job
        running = await self.db.mark_job_running(job.id)
        log.info("Started job %s on %s", job.id, worker.name)
        return running or job

    def free_gpu_indices(self, worker: Worker) -> list[int]:
        return available_gpu_indices(worker)

    def _dispatch_lock(self, worker_id: str) -> asyncio.Lock:
        return self._dispatch_locks.setdefault(worker_id, asyncio.Lock())

    async def dispatch_worker(self, worker: Worker) -> list[Job]:
        if worker.status != WorkerStatus.ONLINE:
            return []
        if await self.db.bootstrap_user_id() is None:
            return []
        launched: list[Job] = []
        async with self._dispatch_lock(worker.id):
            available = self.free_gpu_indices(worker)
            while True:
                claimed_indices = await self.db.running_queue_gpu_indices(worker.id)
                free = [index for index in available if index not in claimed_indices]
                jobs = await self.db.list_queued_jobs(worker.id)
                head = next((job for job in jobs if job.status == JobStatus.PENDING), None)
                if head is None or head.gpu_count > len(free):
                    break
                assigned = free[:head.gpu_count]
                claimed = await self.db.claim_queued_job(
                    head.id, assigned, _now(), worker_id=worker.id,
                )
                if claimed is None:
                    continue
                launched.append(await self._launch_job(worker, claimed))
        return launched

    async def reconcile_starting_job(self, worker: Worker, job: Job) -> Job:
        released = None
        async with self._dispatch_lock(worker.id):
            persisted = await self.db.get_job(job.id)
            if persisted is None or persisted.status != JobStatus.STARTING:
                return persisted or job
            probe = await ssh_job_probe(worker, job.id)
            if probe is None or probe.state == "starting":
                return persisted
            if probe.state == "launch_failed" or (probe.state == "absent" and persisted.stop_requested_at):
                # The supervisor proved no workload ever started, or a stop
                # was requested for a launch that never happened: release
                # instead of holding (or re-queueing) the claim.
                released = await self.db.finish_job(
                    job.id, exit_code=-1, termination_reason=persisted.stop_reason, finished_at=_now(),
                )
                reconciled = released
            else:
                reconciled = await self.db.reconcile_starting_job(
                    job.id, launch_observed=probe.state != "absent",
                )
        if released is not None:
            await self._after_terminal(worker, released)
        return reconciled or persisted

    async def stop_job(self, worker: Worker, job_id: str, *, reason: TerminationReason) -> StopOutcome:
        """Request a stop; report success only after confirmed termination."""
        async with self._dispatch_lock(worker.id):
            job = await self.db.get_job(job_id)
            if job is None or job.worker_id != worker.id:
                return StopOutcome("not_found", job)
            if job.status in (JobStatus.DONE, JobStatus.FAILED):
                return StopOutcome("already_terminal", job)
            if job.status == JobStatus.PENDING:
                cancelled = await self.db.cancel_pending_job(job_id, reason=reason, finished_at=_now())
                if cancelled is None:
                    job = await self.db.get_job(job_id)
                    if job and job.status in (JobStatus.DONE, JobStatus.FAILED):
                        return StopOutcome("already_terminal", job)
                    return StopOutcome("unconfirmed", job)
                outcome = StopOutcome("stopped", cancelled)
            else:
                job = await self.db.request_job_stop(job_id, reason=reason, now=_now())
                if job is None or job.status in (JobStatus.DONE, JobStatus.FAILED):
                    return StopOutcome("already_terminal", job)
                probe = await ssh_job_stop(worker, job_id, (job.stop_reason or reason).value)
                job = await self._apply_probe(worker, job, probe)
                if job.status in (JobStatus.DONE, JobStatus.FAILED):
                    outcome = StopOutcome("stopped", job)
                elif job.stop_confirmed_at:
                    # Idle handoff: termination confirmed, allocation retained
                    # until a fresh post-stop GPU sample.
                    return StopOutcome("stopped", job)
                else:
                    return StopOutcome("unconfirmed", job)
        await self._after_terminal(worker, outcome.job)
        return outcome

    async def _apply_probe(self, worker: Worker, job: Job, probe: JobProbe | None) -> Job:
        """Persist what a supervisor probe proves; never guess termination."""
        if probe is None or probe.state in ("running", "starting"):
            return job
        if probe.state == "lost" or (probe.state == "absent" and job.status == JobStatus.RUNNING):
            return await self.db.mark_termination_unknown(job.id) or job
        if probe.state == "absent":
            # A starting job whose directory is absent may still be launching
            # (uncertain transport); only reconciliation decides it never ran.
            return job
        if probe.state == "launch_failed":
            # Proven pre-workload supervisor failure: nothing is running.
            finished = await self.db.finish_job(
                job.id, exit_code=-1, termination_reason=job.stop_reason, finished_at=_now(),
            )
            return finished or job
        exit_code = probe.exit_code if probe.exit_code is not None else -1
        if job.stop_reason == TerminationReason.GPU_IDLE_TIMEOUT and job.gpu_indices:
            confirmed = await self.db.record_stop_confirmed(job.id, exit_code=exit_code, confirmed_at=_now())
            self.idle_tracker.mark_stop_confirmed(job.id, monotonic_ns())
            return confirmed or job
        finished = await self.db.finish_job(
            job.id, exit_code=exit_code, termination_reason=job.stop_reason, finished_at=_now(),
        )
        return finished or job

    async def _after_terminal(self, worker: Worker, job: Job | None) -> None:
        if job is None or job.status not in (JobStatus.DONE, JobStatus.FAILED):
            return
        self.idle_tracker.forget(job.id)
        if job.status == JobStatus.FAILED and job.started_at and job.termination_reason is None:
            await self._record_failure(worker, job)
        if self.on_job_terminal is not None:
            try:
                await self.on_job_terminal(job)
            except Exception:
                log.exception("Terminal-job hook failed for %s", job.id)
        if job.queue_managed:
            await self.dispatch_worker(worker)

    async def refresh_job_status(self, worker: Worker, job: Job) -> Job:
        """Reconcile one active job with its supervisor's durable state."""
        async with self._dispatch_lock(worker.id):
            persisted = await self.db.get_job(job.id)
            if persisted is None:
                return job
            job = persisted
            if job.worker_id != worker.id or job.status not in (JobStatus.RUNNING, JobStatus.STARTING):
                return job
            if job.status == JobStatus.STARTING or job.stop_confirmed_at:
                return job
            if job.stop_requested_at and job.execution_state == ExecutionState.STOPPING:
                # Resume a durably requested stop (e.g. after a restart).
                probe = await ssh_job_stop(worker, job.id, (job.stop_reason or TerminationReason.USER_CANCELLED).value)
            else:
                probe = await ssh_job_probe(worker, job.id)
            job = await self._apply_probe(worker, job, probe)
        await self._after_terminal(worker, job)
        return job

    async def enforce_gpu_idle(self, worker: Worker) -> None:
        """Ten-minute idle handoff, only while GPU work waits on this worker."""
        now_ns = monotonic_ns()
        jobs = await self.db.list_queued_jobs(worker.id)
        running = [
            job for job in jobs
            if job.status == JobStatus.RUNNING and job.gpu_indices and not job.stop_confirmed_at
            and job.execution_state == ExecutionState.ACTIVE
        ]
        spans = {
            job.id: self.idle_tracker.observe(job.id, worker, list(job.gpu_indices), now_ns=now_ns)
            for job in running
        }
        for job in jobs:
            if job.status in (JobStatus.STARTING, JobStatus.RUNNING) and job.id not in spans and not job.stop_confirmed_at:
                self.idle_tracker.forget(job.id)
        # Release confirmed idle stops once a fresh post-stop sample is idle.
        for job in jobs:
            if (
                job.status == JobStatus.RUNNING and job.stop_confirmed_at
                and job.stop_reason == TerminationReason.GPU_IDLE_TIMEOUT
                and self.idle_tracker.post_stop_sample_ok(job.id, worker, list(job.gpu_indices))
            ):
                async with self._dispatch_lock(worker.id):
                    released = await self.db.finish_job(
                        job.id, exit_code=job.exit_code if job.exit_code is not None else -1,
                        termination_reason=TerminationReason.GPU_IDLE_TIMEOUT, finished_at=_now(),
                    )
                await self._after_terminal(worker, released)
        candidates = sorted(
            (job for job in running if spans[job.id] >= IDLE_TIMEOUT_SECONDS),
            key=lambda job: -spans[job.id],
        )
        if not candidates or worker.status != WorkerStatus.ONLINE:
            return
        for candidate in candidates:
            current = await self.db.list_queued_jobs(worker.id)
            head = next((job for job in current if job.status == JobStatus.PENDING), None)
            if head is None:
                return
            claimed = {index for job in current if job.status != JobStatus.PENDING for index in job.gpu_indices}
            free = [index for index in self.free_gpu_indices(worker) if index not in claimed]
            # GPUs of idle stops already requested (confirmed or not) are
            # about to be handed off; never stop more than the head needs.
            releasing = {
                index for job in current
                if job.status in (JobStatus.STARTING, JobStatus.RUNNING)
                and job.stop_requested_at and job.stop_reason == TerminationReason.GPU_IDLE_TIMEOUT
                for index in job.gpu_indices
            }
            if len(free) + len(releasing) >= head.gpu_count:
                return
            persisted = next((job for job in current if job.id == candidate.id), None)
            if persisted is None or persisted.status != JobStatus.RUNNING or persisted.stop_requested_at:
                continue
            log.info("Stopping idle GPU job %s on %s for queued work", candidate.id, worker.name)
            await self.stop_job(worker, candidate.id, reason=TerminationReason.GPU_IDLE_TIMEOUT)

    async def get_logs(
        self,
        worker: Worker,
        job_id: str,
        tail: int | None = 10,
        head: int | None = None,
    ) -> str:
        """Retrieve job logs with tail/head slicing."""
        return await ssh_read_log(worker, job_id, tail=tail, head=head)

    async def get_log_range(
        self, worker: Worker, job_id: str, start: int | None,
    ) -> tuple[int, int, bytes] | None:
        """Log bytes from ``start`` (None: the recent tail window) for streaming."""
        return await ssh_read_log_range(worker, job_id, start)

    async def _record_failure(self, worker: Worker, job: Job) -> None:
        """Record a failed job in the failures table."""
        summary = await ssh_read_log(worker, job.id, tail=1)
        summary = summary.strip().replace("\n", " ")[:200]
        failure = Failure(
            job_id=job.id,
            worker_id=worker.id,
            exit_code=job.exit_code if job.exit_code is not None else -1,
            timestamp=_now(),
            summary=summary,
        )
        await self.db.insert_failure(failure)
