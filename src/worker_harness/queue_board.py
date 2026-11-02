"""Pure projections for the shared GPU board, owned job views and forecasts.

Shared board rows are whitelisted public DTOs: they never contain commands,
runtime paths, logs, environment or session metadata, even for the owner.
"""

from __future__ import annotations

from typing import Any, Sequence

from .models import ExecutionState, Job, JobStatus, Worker, WorkerStatus
from .scheduling import Estimate, PendingRequest, RunningClaim, WorkerResources, estimate_worker

ACTIVE_STATUSES = (JobStatus.STARTING, JobStatus.RUNNING)


def available_gpu_indices(worker: Worker) -> list[int]:
    """GPU indices the worker reports as not busy (harness claims not applied)."""
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


def forecast_worker(
    worker: Worker | None,
    jobs: Sequence[Job],
    now: int,
    *,
    pending_order: Sequence[str] | None = None,
) -> dict[str, Estimate]:
    """Forecast one worker's queue-managed GPU jobs.

    ``jobs`` are that worker's starting/running/pending queue jobs.
    ``pending_order`` optionally overrides the pending order (reorder preview).
    """
    active = [job for job in jobs if job.status in ACTIVE_STATUSES]
    pending_by_id = {job.id: job for job in jobs if job.status == JobStatus.PENDING}
    if pending_order is None:
        ordered = sorted(pending_by_id.values(), key=lambda job: (job.queue_order, job.id))
    else:
        ordered = [pending_by_id[job_id] for job_id in pending_order if job_id in pending_by_id]
    running = [
        RunningClaim(
            job_id=job.id,
            gpu_indices=tuple(job.gpu_indices),
            started_at=job.started_at,
            expected_seconds=job.expected_seconds,
            release_known=(
                job.status == JobStatus.RUNNING
                and job.execution_state == ExecutionState.ACTIVE
            ),
        )
        for job in active
    ]
    if worker is None:
        resources = WorkerResources(worker_id="", online=False, gpu_count=0)
    else:
        claimed = {index for job in active for index in job.gpu_indices}
        free = set(available_gpu_indices(worker))
        external = frozenset(
            index for index in range(worker.gpu_count)
            if index not in free and index not in claimed
        )
        resources = WorkerResources(
            worker_id=worker.id,
            online=worker.status == WorkerStatus.ONLINE,
            gpu_count=worker.gpu_count,
            external_busy=external,
        )
    pending = [
        PendingRequest(job_id=job.id, gpu_count=job.gpu_count, expected_seconds=job.expected_seconds)
        for job in ordered
    ]
    labels = {job.id: job.name for job in (*active, *ordered) if job.name}
    return estimate_worker(now, resources, running, pending, labels=labels)


def public_worker(worker: Worker) -> dict[str, Any]:
    free = set(available_gpu_indices(worker))
    utilization: list[float | None] = []
    by_index = {gpu.index: gpu for gpu in worker.gpu_telemetry}
    for index in range(worker.gpu_count):
        gpu = by_index.get(index)
        utilization.append(gpu.utilization_percent if gpu and gpu.activity_valid else None)
    available: list[bool | None] = []
    for index in range(worker.gpu_count):
        if worker.status != WorkerStatus.ONLINE:
            available.append(None)
        else:
            available.append(index in free)
    return {
        "id": worker.id,
        "name": worker.name,
        "status": worker.status.value,
        "gpu_count": worker.gpu_count,
        "gpu_names": list(worker.gpu_names),
        "gpu_vram_gb": list(worker.gpu_vram_gb),
        "gpu_used_vram_gb": list(worker.gpu_used_vram_gb),
        "gpu_utilization_percent": utilization,
        "gpu_available": available,
        "cpu_cores": worker.cpu_cores,
        "data_paths": list(worker.data_paths),
    }


def _enum(value: Any) -> Any:
    return getattr(value, "value", value)


def public_queue_job(
    job: Job,
    *,
    owner_display_name: str,
    worker_name: str | None,
    position: int,
    estimate: Estimate | None,
    delay_history: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "id": job.id,
        "name": job.name,
        "owner_user_id": job.owner_user_id,
        "owner_display_name": owner_display_name,
        "worker_id": job.worker_id,
        "worker_name": worker_name,
        "status": job.status.value,
        "execution_state": job.execution_state.value,
        "position": position,
        "submitted_at": job.submitted_at,
        "submission_sequence": job.submission_sequence,
        "priority": job.priority.value,
        "priority_note": job.priority_note,
        "deadline_at": job.deadline_at,
        "expected_seconds": job.expected_seconds,
        "gpu_count": job.gpu_count,
        # 0 means "not yet"; clients expect null rather than the 1970 epoch.
        "started_at": job.started_at or None,
        "termination_reason": _enum(job.termination_reason),
        "stop_requested_at": job.stop_requested_at,
        "estimated_start_at": estimate.estimated_start_at if estimate else None,
        "estimated_finish_at": estimate.estimated_finish_at if estimate else None,
        "estimate_status": estimate.status if estimate else "unknown",
        "estimate_explanation": (
            estimate.explanation if estimate else "CPU/setup work does not wait for GPU slots"
        ),
        "delay_history": delay_history,
    }


def owned_job(job: Job, *, owner_display_name: str, worker_name: str | None) -> dict[str, Any]:
    return {
        "id": job.id,
        "worker_id": job.worker_id,
        "worker_name": worker_name if worker_name is not None else (job.worker_name_at_submission or None),
        "name": job.name,
        "command": job.command,
        "status": job.status.value,
        "execution_state": job.execution_state.value,
        "exit_code": job.exit_code,
        "kind": job.kind.value,
        "owner_user_id": job.owner_user_id,
        "owner_display_name": owner_display_name,
        "submitted_by_actor_id": job.submitted_by_actor_id,
        "submitted_at": job.submitted_at,
        "submission_sequence": job.submission_sequence,
        "priority": job.priority.value,
        "priority_note": job.priority_note,
        "deadline_at": job.deadline_at,
        "expected_seconds": job.expected_seconds,
        "gpu_count": job.gpu_count,
        "gpu_indices": list(job.gpu_indices),
        "queue_managed": job.queue_managed,
        "started_at": job.started_at or None,
        "finished_at": job.finished_at or None,
        "termination_reason": _enum(job.termination_reason),
        "stop_requested_at": job.stop_requested_at,
        "origin_session_id": job.origin_session_id,
    }
