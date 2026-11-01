"""Ten-minute GPU idle handoff through JobManager with controlled samples."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from worker_harness.db import Database
from worker_harness.job import JobManager
from worker_harness.models import GPUInfo, JobStatus, TerminationReason, WorkerRegistration
from worker_harness.ssh import JobProbe, SSHResult

from identity_support import bootstrap

SECOND = 1_000_000_000


class Clock:
    def __init__(self) -> None:
        self.ns = 10_000 * SECOND

    def __call__(self) -> int:
        return self.ns

    def advance(self, seconds: float) -> None:
        self.ns += int(seconds * SECOND)


class IdleHandoffTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.tmp.name) / "db.sqlite")
        await self.db.connect()
        self.owner = (await bootstrap(self.db))["id"]
        self.clock = Clock()
        for target in ("worker_harness.db.monotonic_ns", "worker_harness.job.monotonic_ns",
                       "worker_harness.idle.monotonic_ns"):
            patcher = patch(target, new=self.clock)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.launch = AsyncMock(return_value=SSHResult(stdout="started", stderr="", returncode=0))
        self.stop = AsyncMock(return_value=JobProbe(state="finished", exit_code=143, termination_reason="gpu_idle_timeout"))
        for target, mock in (("worker_harness.job.ssh_job_launch", self.launch),
                             ("worker_harness.job.ssh_job_stop", self.stop),
                             ("worker_harness.job.ssh_read_log", AsyncMock(return_value=""))):
            patcher = patch(target, new=mock)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.daemon = "daemon-1"
        self.boot = "boot-1"
        self.worker = await self.sample()
        self.manager = JobManager(self.db)

    async def asyncTearDown(self) -> None:
        await self.db.close()
        self.tmp.cleanup()

    async def sample(self, *, advance: float = 0, workload: int = 0, workload_bytes: int = 0,
                     utilization: float = 0, display_only: bool = False, valid: bool = True,
                     processes: int | None = None):
        self.clock.advance(advance)
        gpus = [
            GPUInfo(
                index=index, name="GPU", vram_total_gb=24, vram_used_gb=0.4, busy=False, uuid=f"GPU-{index}",
                utilization_percent=utilization, memory_used_bytes=400 * 2**20,
                process_count=processes if processes is not None else workload + int(display_only),
                workload_process_count=workload, workload_memory_bytes=workload_bytes,
                display_only=display_only, activity_valid=valid,
            )
            for index in range(getattr(self, "gpu_count", 1))
        ]
        self.worker = await self.db.upsert_worker(WorkerRegistration(
            worker_id="w1", name="worker-w1", worker_ip="100.64.0.2", gpu_count=len(gpus), gpus=gpus,
            gpu_sample_id=str(uuid4()), gpu_sample_monotonic_ns=self.clock.ns,
            worker_boot_id=self.boot, daemon_instance_id=self.daemon,
        ))
        return self.worker

    async def enqueue(self, name: str):
        return await self.manager.enqueue_job(
            self.worker, f"run {name}", owner_user_id=self.owner, actor_id="api_key:t",
            name=name, expected_seconds=3600, gpu_count=1,
        )

    async def tick(self, **kwargs) -> None:
        await self.sample(advance=60, **kwargs)
        await self.manager.enforce_gpu_idle(self.worker)

    async def idle_for(self, seconds: int, **kwargs) -> None:
        await self.manager.enforce_gpu_idle(await self.sample(**kwargs))
        for _ in range(seconds // 60):
            await self.tick(**kwargs)

    async def test_idle_job_is_stopped_only_at_600s_and_released_after_fresh_sample(self) -> None:
        holder = await self.enqueue("holder")
        waiting = await self.enqueue("waiting")
        self.assertEqual(holder.status, JobStatus.RUNNING)
        await self.idle_for(540)
        self.stop.assert_not_awaited()
        await self.tick()  # 600 s of continuous idle samples
        self.stop.assert_awaited_once()
        self.assertEqual(self.stop.await_args.args[2], "gpu_idle_timeout")
        stopped = await self.db.get_job(holder.id)
        self.assertEqual(stopped.status, JobStatus.RUNNING)
        self.assertTrue(stopped.stop_confirmed_at)
        # Confirmed exit alone does not release the GPU.
        self.assertEqual(await self.db.running_queue_gpu_indices("w1"), {0})
        self.assertEqual((await self.db.get_job(waiting.id)).status, JobStatus.PENDING)
        await self.manager.enforce_gpu_idle(self.worker)  # same old sample: still held
        self.assertEqual((await self.db.get_job(waiting.id)).status, JobStatus.PENDING)
        await self.tick()  # fresh post-stop idle sample
        released = await self.db.get_job(holder.id)
        self.assertEqual((released.status, released.termination_reason.value), (JobStatus.FAILED, "gpu_idle_timeout"))
        self.assertEqual((await self.db.get_job(waiting.id)).status, JobStatus.RUNNING)

    async def test_display_only_activity_does_not_keep_a_job_alive(self) -> None:
        holder = await self.enqueue("holder")
        await self.enqueue("waiting")
        await self.idle_for(600, display_only=True, utilization=35)
        self.stop.assert_awaited_once()
        await self.tick(display_only=True, utilization=40)
        self.assertEqual((await self.db.get_job(holder.id)).status, JobStatus.FAILED)

    async def test_no_waiting_work_means_no_idle_stop(self) -> None:
        await self.enqueue("holder")
        await self.idle_for(900)
        self.stop.assert_not_awaited()

    async def test_any_workload_process_prevents_idle(self) -> None:
        await self.enqueue("holder")
        await self.enqueue("waiting")
        await self.idle_for(900, workload=1, workload_bytes=64 * 2**20)
        self.stop.assert_not_awaited()

    async def test_nonzero_utilization_without_processes_is_not_idle(self) -> None:
        await self.enqueue("holder")
        await self.enqueue("waiting")
        await self.idle_for(900, utilization=12, processes=0)
        self.stop.assert_not_awaited()

    async def test_invalid_samples_and_gaps_and_restarts_reset_the_interval(self) -> None:
        await self.enqueue("holder")
        await self.enqueue("waiting")

        async def ticks(count: int) -> None:
            for _ in range(count):
                await self.tick()

        await ticks(10)  # 540 s of idle samples
        await self.tick(valid=False)  # invalid telemetry resets
        await ticks(10)  # 540 s again
        await self.sample(advance=121)  # telemetry gap > 120 s resets
        await self.manager.enforce_gpu_idle(self.worker)
        await ticks(9)  # 540 s again
        self.daemon = "daemon-2"  # same-boot daemon restart at 540 s resets
        await self.tick()
        await ticks(9)
        self.stop.assert_not_awaited()
        await self.tick()  # 600 s after the restart
        self.stop.assert_awaited_once()

    async def test_unconfirmed_stop_keeps_the_reservation(self) -> None:
        holder = await self.enqueue("holder")
        waiting = await self.enqueue("waiting")
        self.stop.return_value = None
        await self.idle_for(600)
        self.stop.assert_awaited()
        held = await self.db.get_job(holder.id)
        self.assertEqual((held.status, held.execution_state.value), (JobStatus.RUNNING, "stopping"))
        await self.tick()
        self.assertEqual((await self.db.get_job(waiting.id)).status, JobStatus.PENDING)
        self.assertEqual(await self.db.running_queue_gpu_indices("w1"), {0})

    async def test_starting_and_cpu_jobs_are_never_idle_candidates(self) -> None:
        self.launch.return_value = SSHResult(stdout="", stderr="timeout", returncode=-1)
        starting = await self.enqueue("unknown-launch")
        self.assertEqual(starting.status, JobStatus.STARTING)
        await self.enqueue("waiting")
        self.launch.return_value = SSHResult(stdout="started", stderr="", returncode=0)
        cpu = await self.manager.start_job(
            self.worker, "pip install x", owner_user_id=self.owner, actor_id="api_key:t", name="setup",
        )
        self.assertEqual((cpu.status, cpu.gpu_indices), (JobStatus.RUNNING, []))
        await self.idle_for(900)
        self.stop.assert_not_awaited()

    async def test_an_unreleased_idle_stop_counts_toward_the_waiting_head(self) -> None:
        self.gpu_count = 2
        await self.sample()
        first = await self.enqueue("first")
        second = await self.enqueue("second")
        waiting = await self.enqueue("waiting")  # needs one GPU
        self.assertEqual((first.status, second.status, waiting.status),
                         (JobStatus.RUNNING, JobStatus.RUNNING, JobStatus.PENDING))
        await self.idle_for(600)
        self.stop.assert_awaited_once()
        # Same sample again: the confirmed stop is not released yet, but its
        # GPU is already promised to the head, so nothing else is stopped.
        await self.manager.enforce_gpu_idle(self.worker)
        await self.manager.enforce_gpu_idle(self.worker)
        self.stop.assert_awaited_once()
        await self.tick()  # fresh post-stop sample hands the GPU to the head
        self.assertEqual((await self.db.get_job(waiting.id)).status, JobStatus.RUNNING)
        stopped = [job for job in (await self.db.get_job(first.id), await self.db.get_job(second.id))
                   if job.status == JobStatus.FAILED]
        self.assertEqual(len(stopped), 1)

    async def test_proven_pre_workload_launch_failure_releases_the_claim(self) -> None:
        self.launch.return_value = SSHResult(stdout="", stderr="timeout", returncode=255)
        uncertain = await self.enqueue("uncertain")
        self.assertEqual(uncertain.status, JobStatus.STARTING)
        self.launch.return_value = SSHResult(stdout="started", stderr="", returncode=0)
        waiting = await self.enqueue("waiting")
        self.assertEqual(waiting.status, JobStatus.PENDING)
        probe = AsyncMock(return_value=JobProbe(state="launch_failed"))
        with patch("worker_harness.job.ssh_job_probe", new=probe):
            await self.manager.reconcile_starting_job(self.worker, uncertain)
        released = await self.db.get_job(uncertain.id)
        self.assertEqual((released.status, released.exit_code), (JobStatus.FAILED, -1))
        self.assertEqual((await self.db.get_job(waiting.id)).status, JobStatus.RUNNING)

    async def test_stopping_a_launch_that_provably_failed_is_confirmed(self) -> None:
        self.launch.return_value = SSHResult(stdout="", stderr="timeout", returncode=255)
        uncertain = await self.enqueue("uncertain")
        self.stop.return_value = JobProbe(state="launch_failed")
        outcome = await self.manager.stop_job(self.worker, uncertain.id, reason=TerminationReason.USER_CANCELLED)
        self.assertEqual(outcome.state, "stopped")
        self.assertEqual(outcome.job.termination_reason, TerminationReason.USER_CANCELLED)
        self.assertEqual(await self.db.running_queue_gpu_indices("w1"), set())


if __name__ == "__main__":
    unittest.main()
