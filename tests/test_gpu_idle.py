from __future__ import annotations

from unittest.mock import patch

import pytest

from worker_harness.idle import GpuIdleTracker, IDLE_TIMEOUT_SECONDS, gpu_is_idle
from worker_harness.models import GPUInfo, Worker, WorkerStatus

NS = 1_000_000_000


def gpu(index: int = 0, **changes) -> GPUInfo:
    return GPUInfo(**{
        "index": index, "uuid": f"GPU-{index}", "name": "V100",
        "vram_total_gb": 16, "vram_used_gb": 0,
        "memory_used_bytes": 0, "process_count": 0,
        "workload_process_count": 0, "workload_memory_bytes": 0,
        "utilization_percent": 0, "activity_valid": True,
        **changes,
    })


def worker(second: int = 0, **changes) -> Worker:
    return Worker(**{
        "id": "worker", "name": "worker", "worker_ip": "127.0.0.1",
        "status": WorkerStatus.ONLINE, "gpu_count": 2,
        "gpu_telemetry": [gpu(0), gpu(1)],
        "gpu_sample_id": f"sample-{second}",
        "gpu_sample_monotonic_ns": (100 + second) * NS,
        "gpu_sample_received_ns": (1000 + second) * NS,
        "gpu_sample_server_instance": "server", "daemon_instance_id": "daemon",
        "worker_boot_id": "boot", **changes,
    })


def observe(tracker: GpuIdleTracker, sample: Worker, indices: list[int] | None = None) -> float:
    return tracker.observe("job", sample, [0] if indices is None else indices,
                           now_ns=sample.gpu_sample_received_ns)


def interval(tracker: GpuIdleTracker, **changes) -> float:
    for second in range(0, 541, 60):
        span = observe(tracker, worker(second, **changes))
    return span


def test_ten_minutes_requires_received_sample_span_not_polling():
    tracker = GpuIdleTracker("server")
    assert interval(tracker) == 540
    assert observe(tracker, worker(599)) == 599
    assert observe(tracker, worker(600)) == IDLE_TIMEOUT_SECONDS
    assert tracker.observe("job", worker(600), [0], now_ns=1690 * NS) == 600
    assert observe(tracker, worker(660)) == 660


def test_same_sample_repeated_cannot_accumulate_and_eventually_goes_stale():
    tracker = GpuIdleTracker("server")
    assert observe(tracker, worker(0)) == 0
    assert observe(tracker, worker(60)) == 60
    assert tracker.observe("job", worker(60), [0], now_ns=1180 * NS) == 60
    assert tracker.observe("job", worker(60), [0], now_ns=1181 * NS) == 0
    assert observe(tracker, worker(181)) == 0


def test_out_of_order_sample_resets_and_does_not_lower_high_watermark():
    tracker = GpuIdleTracker("server")
    observe(tracker, worker(0))
    assert observe(tracker, worker(60)) == 60
    assert observe(tracker, worker(30)) == 0
    assert observe(tracker, worker(45)) == 0
    assert observe(tracker, worker(120)) == 0
    assert observe(tracker, worker(180)) == 60


@pytest.mark.parametrize("change", [
    {"gpu_sample_id": "sample-60"},
    {"gpu_sample_monotonic_ns": 160 * NS},
    {"gpu_sample_received_ns": 1060 * NS},
])
def test_new_sample_requires_new_id_and_strictly_increasing_clocks(change):
    tracker = GpuIdleTracker("server")
    observe(tracker, worker(0))
    observe(tracker, worker(60))
    assert observe(tracker, worker(120, **change)) == 0


def test_gap_exactly_120_seconds_is_allowed_but_larger_gap_resets():
    tracker = GpuIdleTracker("server")
    observe(tracker, worker(0))
    assert observe(tracker, worker(120)) == 120
    assert observe(tracker, worker(241)) == 0
    assert observe(tracker, worker(301)) == 60


@pytest.mark.parametrize("change", [
    {"daemon_instance_id": "restarted-daemon"},
    {"worker_boot_id": "rebooted-worker"},
    {"id": "replacement-worker"},
])
def test_restart_at_590_seconds_resets_even_without_a_telemetry_gap(change):
    tracker = GpuIdleTracker("server")
    interval(tracker)
    assert observe(tracker, worker(590)) == 590
    assert observe(tracker, worker(600, **change)) == 0
    assert observe(tracker, worker(660, **change)) == 60


def test_daemon_restart_accepts_a_new_scoped_monotonic_clock():
    tracker = GpuIdleTracker("server")
    interval(tracker)
    assert observe(tracker, worker(590, daemon_instance_id="new", gpu_sample_monotonic_ns=NS)) == 0
    assert observe(tracker, worker(650, daemon_instance_id="new", gpu_sample_monotonic_ns=61 * NS)) == 60


@pytest.mark.parametrize("change", [
    {"gpu_sample_server_instance": "old-server"},
    {"gpu_sample_server_instance": ""},
    {"gpu_sample_id": ""},
    {"gpu_sample_monotonic_ns": 0},
    {"gpu_sample_received_ns": 0},
    {"daemon_instance_id": ""},
    {"worker_boot_id": ""},
    {"status": WorkerStatus.OFFLINE},
    {"status": WorkerStatus.DRAINING},
    {"gpu_telemetry": []},
    {"gpu_telemetry": [gpu(uuid="")]},
    {"gpu_telemetry": [gpu(), gpu()]},
])
def test_invalid_identity_offline_or_missing_assignment_resets(change):
    tracker = GpuIdleTracker("server")
    assert interval(tracker) == 540
    assert observe(tracker, worker(600, **change)) == 0
    assert observe(tracker, worker(660)) == 0
    assert observe(tracker, worker(720)) == 60


def test_invalid_and_active_gpu_samples_reset_continuity():
    for telemetry in (
        gpu(activity_valid=False), gpu(workload_process_count=1),
        gpu(workload_memory_bytes=1), gpu(utilization_percent=1),
        gpu(utilization_percent=None), gpu(workload_process_count=None),
    ):
        tracker = GpuIdleTracker("server")
        interval(tracker)
        assert observe(tracker, worker(600, gpu_telemetry=[telemetry])) == 0
        assert observe(tracker, worker(660)) == 0
        assert observe(tracker, worker(720)) == 60


def test_display_only_positive_memory_and_utilization_are_idle():
    display = gpu(display_only=True, process_count=1, memory_used_bytes=2 * 1024**3,
                  vram_used_gb=2, utilization_percent=85)
    assert gpu_is_idle(display)
    tracker = GpuIdleTracker("server")
    assert interval(tracker, gpu_telemetry=[display]) == 540
    assert observe(tracker, worker(600, gpu_telemetry=[display])) == 600
    assert not gpu_is_idle(display.model_copy(update={"workload_process_count": 1}))
    assert not gpu_is_idle(display.model_copy(update={"workload_memory_bytes": 1}))
    assert not gpu_is_idle(display.model_copy(update={"activity_valid": False}))


def test_all_assigned_gpus_must_be_idle_and_uuid_or_assignment_changes_reset():
    tracker = GpuIdleTracker("server")
    for second in range(0, 601, 60):
        assert observe(tracker, worker(second), [0, 1]) == second
    assert observe(tracker, worker(660, gpu_telemetry=[gpu(0), gpu(1, workload_process_count=1)]), [0, 1]) == 0
    assert observe(tracker, worker(720), [0, 1]) == 0
    assert observe(tracker, worker(780), [0, 1]) == 60
    assert observe(tracker, worker(840, gpu_telemetry=[gpu(0), gpu(1, uuid="replacement")]), [0, 1]) == 0
    assert observe(tracker, worker(900), [0]) == 0
    assert observe(tracker, worker(960), [0]) == 60
    assert observe(tracker, worker(1020), []) == 0


def test_assignment_order_is_irrelevant_and_nonassigned_activity_does_not_reset():
    tracker = GpuIdleTracker("server")
    observe(tracker, worker(0), [0, 1])
    assert observe(tracker, worker(60), [1, 0]) == 60
    other_busy = worker(120, gpu_telemetry=[gpu(0), gpu(1, workload_process_count=1)])
    assert observe(tracker, other_busy, [0]) == 0
    assert observe(tracker, worker(180, gpu_telemetry=other_busy.gpu_telemetry), [0]) == 60


def test_server_receive_clock_not_daemon_elapsed_time_measures_span():
    tracker = GpuIdleTracker("server")
    observe(tracker, worker(0))
    assert observe(tracker, worker(60, gpu_sample_monotonic_ns=10000 * NS)) == 60


def test_future_receive_time_fails_closed():
    tracker = GpuIdleTracker("server")
    interval(tracker)
    assert tracker.observe("job", worker(600), [0], now_ns=1599 * NS) == 0


def test_forget_discards_interval_and_stop_confirmation():
    tracker = GpuIdleTracker("server")
    interval(tracker)
    tracker.mark_stop_confirmed("job", 1550 * NS)
    tracker.forget("job")
    assert observe(tracker, worker(600)) == 0
    with patch("worker_harness.idle.monotonic_ns", return_value=1700 * NS):
        assert not tracker.post_stop_sample_ok("job", worker(660), [0])


def test_post_stop_requires_new_valid_idle_sample_received_strictly_after_confirmation():
    tracker = GpuIdleTracker("server")
    interval(tracker)
    tracker.mark_stop_confirmed("job", 1600 * NS)
    assert not tracker.post_stop_sample_ok("job", worker(540), [0])
    assert not tracker.post_stop_sample_ok("job", worker(600), [0])
    assert not tracker.post_stop_sample_ok("job", worker(660, gpu_sample_server_instance="old"), [0])
    assert not tracker.post_stop_sample_ok("job", worker(660, gpu_telemetry=[gpu(workload_process_count=1)]), [0])
    assert not tracker.post_stop_sample_ok("job", worker(660, gpu_sample_monotonic_ns=640 * NS), [0])
    assert not tracker.post_stop_sample_ok("job", worker(660, gpu_sample_id="sample-540"), [0])
    assert tracker.post_stop_sample_ok("job", worker(660), [0])
    # Reconciliation may observe the fresh sample before checking the handoff.
    observe(tracker, worker(660))
    assert tracker.post_stop_sample_ok("job", worker(660), [0])
    tracker.mark_stop_confirmed("job", 1700 * NS)
    assert tracker.post_stop_sample_ok("job", worker(660), [0])


def test_post_stop_without_mark_records_now_and_waits_for_a_subsequent_sample():
    tracker = GpuIdleTracker("server")
    with patch("worker_harness.idle.monotonic_ns", return_value=1600 * NS):
        assert not tracker.post_stop_sample_ok("job", worker(600), [0])
    assert not tracker.post_stop_sample_ok("job", worker(600), [0])
    assert tracker.post_stop_sample_ok("job", worker(660), [0])
