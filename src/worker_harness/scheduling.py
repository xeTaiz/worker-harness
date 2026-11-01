"""Deterministic per-worker FIFO forecasts, without I/O or idle-stop speculation.

Inputs are a scheduling snapshot: ``pending`` is already in queue order and
``external_busy`` excludes harness claims. Unknown release times never imply
free capacity. A known start with an unknown duration has an unknown finish,
but can still be used to attribute changes in its admission time.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Literal, Sequence


@dataclass(frozen=True)
class RunningClaim:
    job_id: str
    gpu_indices: tuple[int, ...]
    started_at: int
    expected_seconds: int
    release_known: bool


@dataclass(frozen=True)
class PendingRequest:
    job_id: str
    gpu_count: int
    expected_seconds: int


@dataclass(frozen=True)
class WorkerResources:
    worker_id: str
    online: bool
    gpu_count: int
    external_busy: frozenset[int] = frozenset()


@dataclass(frozen=True)
class Estimate:
    job_id: str
    estimated_start_at: int | None
    estimated_finish_at: int | None
    status: Literal["known", "unknown"]
    explanation: str


def estimate_worker(
    now: int,
    resources: WorkerResources,
    running: Sequence[RunningClaim],
    pending: Sequence[PendingRequest],
) -> dict[str, Estimate]:
    """Simulate strict FIFO admission using the lowest available GPU indices.

    Later requests cannot start before an earlier head is admitted. Once a
    head's admission is unknown, every later request is unknown, even if it
    would fit otherwise. Releasing exactly at ``now`` is known; an overrun is
    not treated as an immediate release.
    """
    estimates: dict[str, Estimate] = {}
    releases: list[int | None] = [now] * resources.gpu_count
    explanations = [""] * resources.gpu_count
    owners: list[str | None] = [None] * resources.gpu_count

    for claim in running:
        finish = claim.started_at + claim.expected_seconds
        if not claim.release_known:
            finish = None
            explanation = f"unknown: job {claim.job_id} release is unconfirmed"
        elif claim.expected_seconds <= 0:
            finish = None
            explanation = f"unknown: job {claim.job_id} has no runtime estimate"
        elif finish < now:
            finish = None
            explanation = f"unknown: job {claim.job_id} has run past its estimate"
        else:
            explanation = f"job {claim.job_id} estimated to finish at {finish}"
        estimates[claim.job_id] = Estimate(
            claim.job_id,
            claim.started_at,
            finish,
            "known" if finish is not None else "unknown",
            explanation,
        )
        for index in claim.gpu_indices:
            if 0 <= index < resources.gpu_count:
                releases[index] = finish
                explanations[index] = explanation
                owners[index] = claim.job_id

    for index in sorted(resources.external_busy):
        if 0 <= index < resources.gpu_count:
            releases[index] = None
            explanations[index] = f"unknown: GPU {index} is busy outside harness jobs"
            owners[index] = None

    admission_time = now
    blocked_by: str | None = None
    for request in pending:
        if not resources.online:
            explanation = "unknown: worker offline"
        elif blocked_by is not None:
            explanation = f"unknown: earlier queued job {blocked_by} has unknown start"
        elif request.gpu_count > resources.gpu_count:
            explanation = (
                f"unknown: job {request.job_id} requests more GPUs than the worker has"
            )
        else:
            known_releases = sorted(time for time in releases if time is not None)
            if request.gpu_count > len(known_releases):
                explanation = next(
                    reason
                    for time, reason in zip(releases, explanations)
                    if time is None
                )
            else:
                start = max(
                    admission_time,
                    known_releases[request.gpu_count - 1] if request.gpu_count else now,
                )
                indices = [
                    index
                    for index, time in enumerate(releases)
                    if time is not None and time <= start
                ][:request.gpu_count]
                waits = [
                    f"GPU {index} released by job {owners[index]} at {releases[index]}"
                    for index in indices
                    if owners[index] is not None and releases[index] > now
                ]
                explanation = "waits for " + "; ".join(waits) if waits else "GPUs available"
                if start > now and not waits:
                    explanation = f"waits for earlier queued admission at {start}"
                finish = start + request.expected_seconds if request.expected_seconds > 0 else None
                if finish is None:
                    explanation += f"; unknown: job {request.job_id} has no runtime estimate"
                estimates[request.job_id] = Estimate(
                    request.job_id,
                    start,
                    finish,
                    "known" if finish is not None else "unknown",
                    explanation,
                )
                for index in indices:
                    releases[index] = finish
                    owners[index] = request.job_id
                    explanations[index] = (
                        f"unknown: job {request.job_id} has no runtime estimate"
                    )
                admission_time = start
                continue

        estimates[request.job_id] = Estimate(
            request.job_id, None, None, "unknown", explanation
        )
        if blocked_by is None:
            blocked_by = request.job_id

    return estimates


def delays(
    before: dict[str, Estimate],
    after: dict[str, Estimate],
    job_ids: Iterable[str],
) -> dict[str, int | None]:
    """Return signed admission-time changes, or None for unknown starts."""
    result: dict[str, int | None] = {}
    for job_id in job_ids:
        before_start = before[job_id].estimated_start_at
        after_start = after[job_id].estimated_start_at
        result[job_id] = (
            after_start - before_start
            if before_start is not None and after_start is not None
            else None
        )
    return result
