"""Fail-closed, sample-driven observations for cooperative GPU idle handoff."""

from __future__ import annotations

from time import monotonic_ns
from dataclasses import dataclass

from worker_harness.models import GPUInfo, Worker, WorkerStatus

IDLE_TIMEOUT_SECONDS = 600
MAX_SAMPLE_GAP_SECONDS = 120
_NS_PER_SECOND = 1_000_000_000


def gpu_is_idle(gpu: GPUInfo) -> bool:
    return (
        gpu.activity_valid
        and gpu.workload_process_count == 0
        and gpu.workload_memory_bytes == 0
        and (gpu.utilization_percent == 0 or gpu.display_only)
    )


@dataclass
class _Observation:
    incarnation: tuple[str, str, str]
    assignment: tuple[tuple[int, str], ...]
    sample_id: str = ""
    monotonic_ns: int = 0
    received_ns: int = 0
    first_idle_ns: int | None = None

    def span(self) -> float:
        if self.first_idle_ns is None:
            return 0.0
        return (self.received_ns - self.first_idle_ns) / _NS_PER_SECOND


@dataclass(frozen=True)
class _Stop:
    confirmed_ns: int
    incarnation: tuple[str, str, str] | None
    sample_id: str
    monotonic_ns: int


class GpuIdleTracker:
    def __init__(self, server_instance_id: str):
        self.server_instance_id = server_instance_id
        self._observations: dict[str, _Observation] = {}
        self._stops: dict[str, _Stop] = {}

    def _assigned(
        self, worker: Worker, gpu_indices: list[int],
    ) -> tuple[tuple[tuple[int, str], ...], bool] | None:
        if (
            worker.status != WorkerStatus.ONLINE
            or worker.gpu_sample_server_instance != self.server_instance_id
            or not self.server_instance_id
            or not worker.gpu_sample_id
            or worker.gpu_sample_monotonic_ns <= 0
            or worker.gpu_sample_received_ns <= 0
            or not worker.daemon_instance_id
            or not worker.worker_boot_id
            or not gpu_indices
            or len(set(gpu_indices)) != len(gpu_indices)
        ):
            return None
        by_index = {gpu.index: gpu for gpu in worker.gpu_telemetry}
        if len(by_index) != len(worker.gpu_telemetry):
            return None
        if any(index not in by_index or not by_index[index].uuid for index in gpu_indices):
            return None
        assignment = tuple((index, by_index[index].uuid) for index in sorted(gpu_indices))
        return assignment, all(gpu_is_idle(by_index[index]) for index in gpu_indices)

    def observe(self, job_id: str, worker: Worker, gpu_indices: list[int], *, now_ns: int) -> float:
        assigned = self._assigned(worker, gpu_indices)
        previous = self._observations.get(job_id)
        if (
            assigned is None
            or worker.gpu_sample_received_ns > now_ns
            or now_ns - worker.gpu_sample_received_ns > MAX_SAMPLE_GAP_SECONDS * _NS_PER_SECOND
        ):
            if previous is not None:
                previous.first_idle_ns = None
            return 0.0
        assignment, idle = assigned
        incarnation = (worker.id, worker.daemon_instance_id, worker.worker_boot_id)
        if previous is None or previous.incarnation != incarnation:
            previous = _Observation(incarnation, assignment)
            self._observations[job_id] = previous
        if previous.assignment != assignment:
            previous.assignment = assignment
            previous.first_idle_ns = None
        if not idle:
            previous.first_idle_ns = None
        if worker.gpu_sample_id == previous.sample_id:
            # Polling the same received sample cannot extend the interval.
            if (
                worker.gpu_sample_monotonic_ns != previous.monotonic_ns
                or worker.gpu_sample_received_ns != previous.received_ns
            ):
                previous.first_idle_ns = None
            return previous.span()
        if (
            worker.gpu_sample_monotonic_ns <= previous.monotonic_ns
            or worker.gpu_sample_received_ns <= previous.received_ns
        ):
            previous.first_idle_ns = None
            return 0.0
        if (
            worker.gpu_sample_received_ns - previous.received_ns
            > MAX_SAMPLE_GAP_SECONDS * _NS_PER_SECOND
        ):
            previous.first_idle_ns = None
        previous.sample_id = worker.gpu_sample_id
        previous.monotonic_ns = worker.gpu_sample_monotonic_ns
        previous.received_ns = worker.gpu_sample_received_ns
        if idle and previous.first_idle_ns is None:
            previous.first_idle_ns = previous.received_ns
        return previous.span()

    def forget(self, job_id: str) -> None:
        self._observations.pop(job_id, None)
        self._stops.pop(job_id, None)

    def mark_stop_confirmed(self, job_id: str, now_ns: int) -> None:
        # A repeated reconciliation of the same result must not move the bar.
        previous = self._observations.get(job_id)
        self._stops.setdefault(job_id, _Stop(
            now_ns,
            previous.incarnation if previous is not None else None,
            previous.sample_id if previous is not None else "",
            previous.monotonic_ns if previous is not None else 0,
        ))

    def post_stop_sample_ok(self, job_id: str, worker: Worker, gpu_indices: list[int]) -> bool:
        stop = self._stops.get(job_id)
        if stop is None:
            self.mark_stop_confirmed(job_id, monotonic_ns())
            return False
        assigned = self._assigned(worker, gpu_indices)
        if assigned is None or not assigned[1] or worker.gpu_sample_received_ns <= stop.confirmed_ns:
            return False
        incarnation = (worker.id, worker.daemon_instance_id, worker.worker_boot_id)
        if incarnation == stop.incarnation and (
            worker.gpu_sample_id == stop.sample_id
            or worker.gpu_sample_monotonic_ns <= stop.monotonic_ns
        ):
            return False
        return True
