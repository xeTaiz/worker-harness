"""Durable per-worker FIFO GPU queue behavior."""

import asyncio
import base64
import re
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from worker_harness.db import Database, SlurmError
from worker_harness.heartbeat import create_app, reconcile_queued_jobs
from worker_harness.job import JobManager
from worker_harness.models import (
    GPUInfo,
    Job,
    JobStatus,
    RunnerResult,
    SlurmObservation,
    SlurmReservationRequest,
    WorkerRegistration,
    Worker,
    WorkerStatus,
)
from worker_harness.ssh import SSHResult, _build_job_command


def registration(worker_id: str, gpu_count: int = 4, *, busy: list[bool | None] | None = None) -> WorkerRegistration:
    reported_busy = busy or [False] * gpu_count
    return WorkerRegistration(
        worker_id=worker_id,
        name=f"worker-{worker_id}",
        worker_ip="100.64.0.2",
        gpu_count=gpu_count,
        gpus=[
            GPUInfo(
                index=index,
                name="NVIDIA RTX 4090",
                vram_total_gb=24,
                vram_used_gb=0,
                busy=reported_busy[index],
            )
            for index in range(gpu_count)
        ],
        cpu_cores=8,
        total_ram_gb=64,
        used_ram_gb=8,
        total_disk_gb=500,
        used_disk_gb=100,
    )


def launch_success(*_args, **_kwargs) -> SSHResult:
    return SSHResult(stdout="started", stderr="", returncode=0)


def cpu_reservation(request_id: str = "cpu-reservation") -> SlurmReservationRequest:
    return SlurmReservationRequest(
        request_id=request_id,
        name="CPU preparation",
        gpu_count=0,
        recipient_id="queue-tests",
        registration_token_hash="0" * 64,
        slurm={
            "cluster_id": "test-cpu",
            "storage_id": "test",
            "compute_kind": "cpu",
            "cpus": 1,
            "memory_mb": 4096,
            "walltime_seconds": 600,
            "failure_grace_seconds": 120,
            "warning_seconds": 30,
            "termination_grace_seconds": 10,
        },
    )


def reserved_registration(worker: Worker) -> WorkerRegistration:
    return registration(worker.id, 0).model_copy(update={
        "name": worker.name,
        "cpu_cores": worker.slurm.cpus,
        "total_ram_gb": worker.slurm.memory_mb / 1024,
    })


class QueueSchedulerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        tmp = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
        tmp.close()
        self.path = Path(tmp.name)
        self.db = Database(self.path)
        await self.db.connect()
        self.worker = await self.db.upsert_worker(registration("w1"))
        self.manager = JobManager(self.db)

    async def asyncTearDown(self) -> None:
        await self.db.close()
        self.path.unlink(missing_ok=True)

    async def test_concurrent_enqueue_claims_fifo_once(self) -> None:
        launched: list[str] = []

        async def launch(_worker, _job_id, command, **_kwargs):
            launched.append(command)
            await asyncio.sleep(0)
            return launch_success()

        with patch("worker_harness.job.ssh_tmux_new", new=launch):
            jobs = await asyncio.gather(*(
                self.manager.enqueue_job(self.worker, f"command-{index}", f"job-{index}", 60)
                for index in range(3)
            ))

        self.assertEqual(launched, ["command-0", "command-1", "command-2"])
        self.assertEqual(len({job.id for job in jobs}), 3)
        persisted = await self.db.list_queued_jobs("w1")
        self.assertEqual([job.status for job in persisted], [JobStatus.RUNNING] * 3)
        self.assertEqual(
            {job.command: job.gpu_indices for job in persisted},
            {"command-0": [0], "command-1": [1], "command-2": [2]},
        )

    async def test_workers_dispatch_independently(self) -> None:
        worker2 = await self.db.upsert_worker(registration("w2"))
        active = 0
        peak = 0

        async def launch(*_args, **_kwargs):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            try:
                await asyncio.sleep(0.02)
                return launch_success()
            finally:
                active -= 1

        with patch("worker_harness.job.ssh_tmux_new", new=launch):
            await asyncio.gather(
                self.manager.enqueue_job(self.worker, "one", "one", 60),
                self.manager.enqueue_job(worker2, "two", "two", 60),
            )

        self.assertEqual(peak, 2)

    async def test_four_gpus_fill_and_multi_gpu_head_blocks_backfill(self) -> None:
        with patch("worker_harness.job.ssh_tmux_new", new=AsyncMock(side_effect=launch_success)):
            running = [
                await self.manager.enqueue_job(self.worker, f"run-{index}", f"run-{index}", 60)
                for index in range(4)
            ]
            head = await self.manager.enqueue_job(self.worker, "head", "head", 120, gpu_count=2)
            later = await self.manager.enqueue_job(self.worker, "later", "later", 30)

            self.assertEqual(head.status, JobStatus.PENDING)
            self.assertEqual(later.status, JobStatus.PENDING)

            running[0].status = JobStatus.DONE
            await self.db.update_job(running[0])
            await self.manager.dispatch_worker(self.worker)
            self.assertEqual((await self.db.get_job(head.id)).status, JobStatus.PENDING)
            self.assertEqual((await self.db.get_job(later.id)).status, JobStatus.PENDING)

            running[1].status = JobStatus.DONE
            await self.db.update_job(running[1])
            await self.manager.dispatch_worker(self.worker)

        claimed_head = await self.db.get_job(head.id)
        self.assertEqual(claimed_head.status, JobStatus.RUNNING)
        self.assertEqual(claimed_head.gpu_indices, [0, 1])
        self.assertEqual((await self.db.get_job(later.id)).status, JobStatus.PENDING)

    async def test_launch_failure_advances_to_next_head(self) -> None:
        first = Job(worker_id="w1", name="first", command="false", queue_managed=True, expected_seconds=10, gpu_count=1)
        second = Job(worker_id="w1", name="second", command="true", queue_managed=True, expected_seconds=10, gpu_count=1)
        await self.db.insert_queued_job(first)
        await self.db.insert_queued_job(second)
        results = [
            SSHResult(stdout="", stderr="launch failed", returncode=1),
            launch_success(),
        ]
        with patch("worker_harness.job.ssh_tmux_new", new=AsyncMock(side_effect=results)):
            await self.manager.dispatch_worker(self.worker)

        self.assertEqual((await self.db.get_job(first.id)).status, JobStatus.FAILED)
        self.assertEqual((await self.db.get_job(second.id)).status, JobStatus.RUNNING)
        self.assertEqual((await self.db.get_job(second.id)).gpu_indices, [0])

    async def test_completion_stop_and_pending_cancel_release_queue(self) -> None:
        one_gpu = await self.db.upsert_worker(registration("single", 1))
        manager = JobManager(self.db)
        launcher = AsyncMock(side_effect=launch_success)
        with patch("worker_harness.job.ssh_tmux_new", new=launcher):
            first = await manager.enqueue_job(one_gpu, "first", "first", 10)
            second = await manager.enqueue_job(one_gpu, "second", "second", 10)
            third = await manager.enqueue_job(one_gpu, "third", "third", 10)

            with patch("worker_harness.job.ssh_tmux_running", new=AsyncMock(return_value=False)), patch(
                "worker_harness.job.ssh_get_exit_code", new=AsyncMock(return_value=0)
            ):
                await manager.refresh_job_status(one_gpu, first)
            self.assertEqual((await self.db.get_job(second.id)).status, JobStatus.RUNNING)

            kill = AsyncMock(return_value=SSHResult(stdout="stopped\n", stderr="", returncode=0))
            with patch("worker_harness.job.ssh_tmux_kill", new=kill):
                self.assertTrue(await manager.stop_job(one_gpu, second.id))
            self.assertEqual((await self.db.get_job(third.id)).status, JobStatus.RUNNING)

            pending = await manager.enqueue_job(one_gpu, "pending", "pending", 10)
            no_kill = AsyncMock(side_effect=AssertionError("pending cancellation must not use SSH"))
            with patch("worker_harness.job.ssh_tmux_kill", new=no_kill):
                self.assertTrue(await manager.stop_job(one_gpu, pending.id))
            no_kill.assert_not_awaited()
            self.assertEqual((await self.db.get_job(pending.id)).status, JobStatus.FAILED)

    async def test_move_reorder_and_update_renumber_queues(self) -> None:
        worker2 = await self.db.upsert_worker(registration("w2"))
        await self.db.set_worker_status("w1", WorkerStatus.OFFLINE)
        await self.db.set_worker_status("w2", WorkerStatus.OFFLINE)
        jobs = [
            Job(worker_id="w1", name=name, command=name, queue_managed=True, expected_seconds=10, gpu_count=1)
            for name in ("a", "b", "c")
        ]
        other = Job(worker_id="w2", name="d", command="d", queue_managed=True, expected_seconds=10, gpu_count=1)
        for job in [*jobs, other]:
            await self.db.insert_queued_job(job)

        await self.db.update_pending_job(
            jobs[2].id,
            worker_id=None,
            position=1,
            name=None,
            expected_seconds=None,
            gpu_count=None,
        )
        self.assertEqual(
            [job.name for job in await self.db.list_queued_jobs("w1")],
            ["c", "a", "b"],
        )

        await self.db.update_pending_job(
            jobs[0].id,
            worker_id=worker2.id,
            position=1,
            name="renamed",
            expected_seconds=99,
            gpu_count=2,
        )
        source = await self.db.list_queued_jobs("w1")
        destination = await self.db.list_queued_jobs("w2")
        self.assertEqual([(job.name, job.queue_order) for job in source], [("c", 1), ("b", 2)])
        self.assertEqual([(job.name, job.queue_order) for job in destination], [("renamed", 1), ("d", 2)])
        self.assertEqual(destination[0].expected_seconds, 99)
        self.assertEqual(destination[0].gpu_count, 2)

    async def test_starting_recovery_existing_absent_and_unreachable(self) -> None:
        queued = [
            Job(worker_id="w1", name=name, command=name, queue_managed=True, expected_seconds=10, gpu_count=1)
            for name in ("existing", "absent", "unreachable")
        ]
        for index, job in enumerate(queued):
            await self.db.insert_queued_job(job)
            await self.db.claim_queued_job(job.id, [index], 100 + index, worker_id="w1")

        await self.db.close()
        self.db = Database(self.path)
        await self.db.connect()
        manager = JobManager(self.db)
        worker = await self.db.get_worker("w1")
        probe_results = {queued[0].id: True, queued[1].id: False, queued[2].id: None}

        async def probe(_worker, job_id):
            return probe_results[job_id]

        with patch("worker_harness.job.ssh_job_started", new=probe):
            for job in queued:
                await manager.reconcile_starting_job(worker, await self.db.get_job(job.id))

        existing = await self.db.get_job(queued[0].id)
        absent = await self.db.get_job(queued[1].id)
        unreachable = await self.db.get_job(queued[2].id)
        self.assertEqual(existing.status, JobStatus.RUNNING)
        self.assertEqual(absent.status, JobStatus.PENDING)
        self.assertEqual(absent.gpu_indices, [])
        self.assertEqual(absent.started_at, 0)
        self.assertEqual(unreachable.status, JobStatus.STARTING)

    async def test_claim_rejects_stale_worker_size_and_fifo_head(self) -> None:
        other = await self.db.upsert_worker(registration("other"))
        first = Job(worker_id="w1", command="first", gpu_count=1)
        second = Job(worker_id="w1", command="second", gpu_count=1)
        await self.db.insert_queued_job(first)
        await self.db.insert_queued_job(second)
        self.assertIsNone(await self.db.claim_queued_job(second.id, [0], 100, worker_id="w1"))
        await self.db.update_pending_job(
            first.id, worker_id=other.id, position=None, name=None,
            expected_seconds=None, gpu_count=2,
        )
        self.assertIsNone(await self.db.claim_queued_job(first.id, [0], 100, worker_id="w1"))
        self.assertIsNone(await self.db.claim_queued_job(first.id, [0], 100, worker_id=other.id))
        claimed = await self.db.claim_queued_job(first.id, [0, 1], 100, worker_id=other.id)
        self.assertEqual((claimed.worker_id, claimed.gpu_indices), (other.id, [0, 1]))
        self.assertEqual((await self.db.get_job(second.id)).status, JobStatus.PENDING)

    async def test_unknown_launch_keeps_gpu_reserved_until_recovery(self) -> None:
        worker = await self.db.upsert_worker(registration("single", 1))
        launcher = AsyncMock(return_value=SSHResult(stdout="", stderr="connection lost", returncode=255))
        with patch("worker_harness.job.ssh_tmux_new", new=launcher):
            first = await self.manager.enqueue_job(worker, "first", "first", 10)
            second = await self.manager.enqueue_job(worker, "second", "second", 10)
        self.assertEqual(first.status, JobStatus.STARTING)
        self.assertEqual(second.status, JobStatus.PENDING)
        self.assertEqual(await self.db.running_queue_gpu_indices(worker.id), {0})
        launcher.assert_awaited_once()

    async def test_recovery_does_not_rerun_completed_job_without_tmux(self) -> None:
        job = Job(worker_id="w1", command="finished", gpu_count=1)
        await self.db.insert_queued_job(job)
        job = await self.db.claim_queued_job(job.id, [0], 100, worker_id="w1")
        with tempfile.TemporaryDirectory() as harness_dir:
            self.worker.harness_dir = harness_dir
            output_dir = Path(harness_dir) / job.id
            output_dir.mkdir()
            (output_dir / "output.log").write_text("result\nEXIT:0\n")

            async def local_probe(_worker, command, **_kwargs):
                proc = await asyncio.create_subprocess_exec(
                    "bash", "-c", command,
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                )
                stdout, stderr = await proc.communicate()
                return SSHResult(stdout.decode(), stderr.decode(), proc.returncode)

            with patch("worker_harness.ssh._tmux_env", return_value="false"), patch(
                "worker_harness.ssh.async_ssh_run", new=local_probe,
            ), patch("worker_harness.job.ssh_tmux_new", new=AsyncMock()) as launch:
                recovered = await self.manager.reconcile_starting_job(self.worker, job)
                completed = await self.manager.refresh_job_status(self.worker, recovered)
            self.assertEqual((completed.status, completed.exit_code), (JobStatus.DONE, 0))
            launch.assert_not_awaited()

    async def test_reconciler_recovers_immediate_starting_job(self) -> None:
        job = Job(worker_id="w1", command="immediate", status=JobStatus.STARTING)
        await self.db.insert_job(job)
        with patch("worker_harness.job.ssh_job_started", new=AsyncMock(return_value=True)):
            await reconcile_queued_jobs(self.db, self.manager)
        self.assertEqual((await self.db.get_job(job.id)).status, JobStatus.RUNNING)

    async def test_recovery_preserves_fifo_for_same_second_claims(self) -> None:
        jobs = [
            Job(id=job_id, worker_id="w1", command=job_id, gpu_count=1)
            for job_id in ("z-first", "a-second")
        ]
        for index, job in enumerate(jobs):
            await self.db.insert_queued_job(job)
            await self.db.claim_queued_job(job.id, [index], 100, worker_id="w1")
        launch = AsyncMock(side_effect=launch_success)
        with patch("worker_harness.job.ssh_job_started", new=AsyncMock(return_value=False)), patch(
            "worker_harness.job.ssh_tmux_new", new=launch,
        ):
            await reconcile_queued_jobs(self.db, self.manager)
        self.assertEqual([call.args[1] for call in launch.await_args_list], ["z-first", "a-second"])

    async def test_stale_refresh_does_not_overwrite_stopped_job(self) -> None:
        with patch("worker_harness.job.ssh_tmux_new", new=AsyncMock(side_effect=launch_success)):
            job = await self.manager.enqueue_job(self.worker, "work", "work", 10)
        with patch("worker_harness.job.ssh_tmux_kill", new=AsyncMock(
            return_value=SSHResult(stdout="stopped", stderr="", returncode=0),
        )):
            self.assertTrue(await self.manager.stop_job(self.worker, job.id))
        with patch("worker_harness.job.ssh_tmux_running", new=AsyncMock(return_value=False)), patch(
            "worker_harness.job.ssh_get_exit_code", new=AsyncMock(return_value=0),
        ):
            refreshed = await self.manager.refresh_job_status(self.worker, job)
        self.assertEqual((refreshed.status, refreshed.exit_code), (JobStatus.FAILED, -1))

    async def test_cpu_reservation_waits_offline_then_dispatches_serially(self) -> None:
        worker = await self.db.reserve_slurm_worker(cpu_reservation())
        launch = AsyncMock(side_effect=launch_success)
        with patch("worker_harness.job.ssh_tmux_new", new=launch):
            first = await self.manager.enqueue_job(
                worker, "prepare", "prepare", 30, gpu_count=0,
                submission_key="cpu-reservation",
            )
            second = await self.manager.enqueue_job(worker, "next", "next", 30, gpu_count=0)
            self.assertEqual((first.status, second.status), (JobStatus.PENDING, JobStatus.PENDING))
            launch.assert_not_awaited()
            reserved = await self.db.get_worker(worker.id)
            self.assertEqual(reserved.slurm.initial_job_id, first.id)
            self.assertEqual(first.notify_recipient_id, "queue-tests")

            await self.db.register_slurm_worker(reserved_registration(worker), ready=True)
            # Even the original OFFLINE snapshot dispatches against fresh state.
            await self.manager.dispatch_worker(worker)
            self.assertEqual((await self.db.get_job(first.id)).status, JobStatus.RUNNING)
            self.assertEqual((await self.db.get_job(second.id)).status, JobStatus.PENDING)
            launch.assert_awaited_once()
            self.assertEqual(launch.await_args.kwargs["gpu_indices"], [])

            with patch("worker_harness.job.ssh_job_result", new=AsyncMock(return_value=RunnerResult(
                exit_code=0, descendants_exited=True, finished_at=time.time(),
                finished_monotonic=time.monotonic(), boot_id="queue-test",
            ))):
                await self.manager.refresh_job_status(worker, first)
        self.assertEqual((await self.db.get_job(second.id)).status, JobStatus.RUNNING)
        self.assertEqual(launch.await_count, 2)
        self.assertTrue(all(call.kwargs["gpu_indices"] == [] for call in launch.await_args_list))

    async def test_cpu_unknown_launch_blocks_next_transactional_claim(self) -> None:
        worker = await self.db.reserve_slurm_worker(cpu_reservation())
        worker = await self.db.register_slurm_worker(reserved_registration(worker), ready=True)
        launch = AsyncMock(return_value=SSHResult("", "connection lost", 255))
        other_manager = JobManager(self.db)
        with patch("worker_harness.job.ssh_tmux_new", new=launch):
            first, second = await asyncio.gather(
                self.manager.enqueue_job(worker, "one", "one", 30, gpu_count=0),
                other_manager.enqueue_job(worker, "two", "two", 30, gpu_count=0),
            )
            await other_manager.dispatch_worker(worker)
        self.assertEqual((first.status, second.status), (JobStatus.STARTING, JobStatus.PENDING))
        self.assertIsNone(await self.db.claim_queued_job(second.id, [], 100, worker_id=worker.id))
        self.assertEqual((await self.db.get_job(first.id)).gpu_indices, [])
        launch.assert_awaited_once()

    async def test_zero_gpu_admission_only_allows_cpu_allocations(self) -> None:
        with self.assertRaises(ValueError):
            await self.manager.enqueue_job(self.worker, "cpu", "cpu", 30, gpu_count=0)
        cpu = await self.db.reserve_slurm_worker(cpu_reservation())
        with self.assertRaises(ValueError):
            await self.manager.enqueue_job(cpu, "gpu", "gpu", 30, gpu_count=1)
        gpu_request = cpu_reservation("gpu-reservation").model_dump()
        gpu_request["gpu_count"] = 1
        gpu_request["slurm"]["compute_kind"] = "gpu"
        gpu = await self.db.reserve_slurm_worker(SlurmReservationRequest(**gpu_request))
        with self.assertRaises(ValueError):
            await self.manager.enqueue_job(gpu, "cpu", "cpu", 30, gpu_count=0)

    async def test_enqueue_submission_key_replays_without_duplicate_launch(self) -> None:
        launch = AsyncMock(side_effect=launch_success)
        with patch("worker_harness.job.ssh_tmux_new", new=launch):
            first, repeated = await asyncio.gather(*(
                self.manager.enqueue_job(
                    self.worker, "once", "once", 30, submission_key="stable-key",
                    notify_recipient_id="recipient",
                )
                for _ in range(2)
            ))
            self.assertEqual(first.id, repeated.id)
            self.assertEqual(first.notify_recipient_id, "recipient")
            with self.assertRaises(SlurmError) as conflict:
                await self.manager.enqueue_job(
                    self.worker, "changed", "once", 30, submission_key="stable-key",
                    notify_recipient_id="recipient",
                )
        self.assertEqual(conflict.exception.code, "IDEMPOTENCY_CONFLICT")
        self.assertEqual(len(await self.db.list_jobs(self.worker.id)), 1)
        launch.assert_awaited_once()

    async def test_terminal_allocation_replays_initial_job_but_rejects_new_work(self) -> None:
        worker = await self.db.reserve_slurm_worker(cpu_reservation())
        first = await self.manager.enqueue_job(
            worker, "once", "once", 30, gpu_count=0, submission_key="cpu-reservation",
        )
        await self.db.observe_slurm_worker(
            worker.id, SlurmObservation(scheduler_state="FAILED", observed_at=time.time()),
        )
        launch = AsyncMock(side_effect=AssertionError("finished allocations must not launch"))
        with patch("worker_harness.job.ssh_tmux_new", new=launch):
            repeated = await self.manager.enqueue_job(
                worker, "once", "once", 30, gpu_count=0, submission_key="cpu-reservation",
            )
            self.assertEqual(repeated.id, first.id)
            self.assertEqual(repeated.status, JobStatus.FAILED)
            for action in (
                self.manager.enqueue_job(worker, "new", "new", 30, gpu_count=0),
                self.manager.start_job(worker, "diagnose"),
            ):
                with self.assertRaises(SlurmError) as finished:
                    await action
                self.assertEqual(finished.exception.code, "SLURM_WORKER_FINISHED")
                self.assertIn("fresh", str(finished.exception).lower())
            self.assertEqual(await self.manager.dispatch_worker(worker), [])
        launch.assert_not_awaited()

    async def test_draining_worker_rejects_stale_online_start_and_enqueue(self) -> None:
        await self.db.set_worker_status(self.worker.id, WorkerStatus.DRAINING)
        launch = AsyncMock()
        with patch("worker_harness.job.ssh_tmux_new", new=launch):
            with self.assertRaises(SlurmError):
                await self.manager.start_job(self.worker, "diagnose")
            with self.assertRaises(SlurmError):
                await self.manager.enqueue_job(self.worker, "later", "later", 30)
            self.assertEqual(await self.manager.dispatch_worker(self.worker), [])
        self.assertEqual(await self.db.list_jobs(self.worker.id), [])
        launch.assert_not_awaited()

    async def test_stale_online_snapshot_cannot_start_or_dispatch_offline_worker(self) -> None:
        await self.db.set_worker_status(self.worker.id, WorkerStatus.OFFLINE)
        launch = AsyncMock()
        with patch("worker_harness.job.ssh_tmux_new", new=launch):
            queued = await self.manager.enqueue_job(self.worker, "later", "later", 30)
            self.assertEqual(queued.status, JobStatus.PENDING)
            with self.assertRaisesRegex(ValueError, "offline"):
                await self.manager.start_job(self.worker, "now")
            self.assertEqual(await self.manager.dispatch_worker(self.worker), [])
        launch.assert_not_awaited()
        self.assertEqual(len(await self.db.list_jobs(self.worker.id)), 1)

    async def test_job_managers_share_start_and_enqueue_dispatch_lock(self) -> None:
        other_manager = JobManager(self.db)
        self.assertIs(self.manager._dispatch_lock("w1"), other_manager._dispatch_lock("w1"))
        active = 0
        peak = 0

        async def launch(*_args, **_kwargs):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0)
            active -= 1
            return launch_success()

        with patch("worker_harness.job.ssh_tmux_new", new=launch):
            await asyncio.gather(
                self.manager.start_job(self.worker, "diagnostic"),
                other_manager.enqueue_job(self.worker, "useful", "useful", 30),
            )
        self.assertEqual(peak, 1)

    async def test_successful_ssh_reply_cannot_reopen_confirmed_failed_job(self) -> None:
        async def launch(_worker, job_id, *_args, **_kwargs):
            confirmed = await self.db.get_job(job_id)
            confirmed.status = JobStatus.FAILED
            confirmed.exit_code = 7
            confirmed.finished_at = int(time.time())
            await self.db.update_job(confirmed)
            return launch_success()

        with patch("worker_harness.job.ssh_tmux_new", new=launch):
            job = await self.manager.start_job(
                self.worker, "failed", notify_recipient_id="failure-recipient",
            )
        self.assertEqual((job.status, job.exit_code), (JobStatus.FAILED, 7))
        self.assertEqual((await self.db.get_job(job.id)).status, JobStatus.FAILED)

    async def test_cpu_diagnostics_keep_grace_until_useful_job_actually_starts(self) -> None:
        worker = await self.db.reserve_slurm_worker(cpu_reservation())
        worker = await self.db.register_slurm_worker(reserved_registration(worker), ready=True)
        launch = AsyncMock(side_effect=launch_success)
        with patch("worker_harness.job.ssh_tmux_new", new=launch):
            failed = await self.manager.enqueue_job(worker, "fail", "fail", 30, gpu_count=0)
            failed.status = JobStatus.FAILED
            failed.exit_code = 1
            failed.finished_at = int(time.time())
            await self.db.update_job(failed)
            deadline = (await self.db.get_worker(worker.id)).slurm.failure_deadline
            self.assertIsNotNone(deadline)
            diagnostic = await self.manager.start_job(worker, "diagnose")
            self.assertEqual(diagnostic.status, JobStatus.RUNNING)
            self.assertEqual(launch.await_args.kwargs["gpu_indices"], [])
            self.assertEqual((await self.db.get_worker(worker.id)).slurm.failure_deadline, deadline)

        unknown = AsyncMock(return_value=SSHResult("", "lost launch reply", 255))
        with patch("worker_harness.job.ssh_tmux_new", new=unknown):
            useful = await self.manager.enqueue_job(worker, "rerun", "rerun", 30, gpu_count=0)
        self.assertEqual(useful.status, JobStatus.STARTING)
        self.assertEqual((await self.db.get_worker(worker.id)).slurm.failure_deadline, deadline)
        with patch("worker_harness.job.ssh_job_started", new=AsyncMock(return_value=True)), patch(
            "worker_harness.job.ssh_job_result", new=AsyncMock(return_value=None),
        ):
            await self.manager.reconcile_starting_job(worker, useful)
        self.assertIsNone((await self.db.get_worker(worker.id)).slurm.failure_deadline)
        self.assertEqual(unknown.await_args.kwargs["gpu_indices"], [])
        self.assertTrue(unknown.await_args.kwargs["queue_managed"])
        self.assertFalse(launch.await_args.kwargs["queue_managed"])

    async def test_job_script_exports_only_queued_gpu_indices(self) -> None:
        with tempfile.TemporaryDirectory() as harness_dir:
            self.worker.harness_dir = harness_dir
            for job_id, indices, expected in (
                ("queued", [3, 1], "3,1"),
                ("cpu", [], ""),
                ("immediate", None, "inherited"),
            ):
                output_dir = Path(harness_dir) / job_id
                output_dir.mkdir()
                command = _build_job_command(
                    self.worker, job_id, 'printf "gpu=%s\\n" "$CUDA_VISIBLE_DEVICES"', indices,
                )
                encoded = re.search(r"echo '([^']+)' \| base64", command).group(1)
                script = base64.b64decode(encoded).decode()
                proc = await asyncio.create_subprocess_exec(
                    "bash", "-c",
                    "export CUDA_VISIBLE_DEVICES=inherited; sleep() { :; }; tmux() { :; }; " + script,
                )
                self.assertEqual(await proc.wait(), 0)
                self.assertEqual((output_dir / "output.log").read_text(), f"gpu={expected}\nEXIT:0\n")


class QueueApiTests(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
        tmp.close()
        self.path = Path(tmp.name)
        self.db = Database(self.path)
        asyncio.run(self.db.connect())
        asyncio.run(self.db.upsert_worker(registration("api", 1)))
        self.client = TestClient(create_app(self.db))

    def tearDown(self) -> None:
        self.client.close()
        asyncio.run(self.db.close())
        self.path.unlink(missing_ok=True)

    def test_queue_api_positions_capacity_and_non_pending_conflict(self) -> None:
        with patch("worker_harness.job.ssh_tmux_new", new=AsyncMock(side_effect=launch_success)):
            first = self.client.post(
                "/api/v1/jobs/queue",
                json={"worker_id": "api", "command": "first", "name": "first", "expected_seconds": 60},
            )
            second = self.client.post(
                "/api/v1/jobs/queue",
                json={"worker_id": "api", "command": "second", "name": "second", "expected_seconds": 30},
            )

        self.assertEqual(first.status_code, 200, first.text)
        self.assertEqual(first.json()["status"], "running")
        self.assertEqual(second.status_code, 200, second.text)
        self.assertEqual(second.json()["status"], "pending")
        queue = self.client.get("/api/v1/jobs/queue?worker_id=api").json()
        self.assertEqual([(item["status"], item["position"]) for item in queue], [("running", 0), ("pending", 1)])

        conflict = self.client.patch(
            f"/api/v1/jobs/{first.json()['id']}/queue",
            json={"name": "cannot-change"},
        )
        self.assertEqual(conflict.status_code, 409)
        too_large = self.client.post(
            "/api/v1/jobs/queue",
            json={
                "worker_id": "api",
                "command": "large",
                "name": "large",
                "expected_seconds": 60,
                "gpu_count": 2,
            },
        )
        self.assertEqual(too_large.status_code, 422)
        self.assertEqual(self.client.patch(f"/api/v1/jobs/{second.json()['id']}/queue", json={}).status_code, 422)

    def test_offline_enqueue_dispatches_after_worker_returns(self) -> None:
        asyncio.run(self.db.set_worker_status("api", WorkerStatus.OFFLINE))
        launch = AsyncMock(side_effect=launch_success)
        with patch("worker_harness.job.ssh_tmux_new", new=launch):
            response = self.client.post(
                "/api/v1/jobs/queue",
                json={"worker_id": "api", "command": "later", "name": "later", "expected_seconds": 60},
            )
            self.assertEqual(response.json()["status"], "pending")
            asyncio.run(self.db.upsert_worker(registration("api", 1)))
            asyncio.run(reconcile_queued_jobs(self.db, JobManager(self.db)))

        self.assertEqual(asyncio.run(self.db.get_job(response.json()["id"])).status, JobStatus.RUNNING)
        launch.assert_awaited_once()

    def test_terminal_allocation_returns_conflict_with_fresh_allocation_guidance(self) -> None:
        worker = asyncio.run(self.db.reserve_slurm_worker(cpu_reservation()))
        payload = {
            "worker_id": worker.id,
            "command": "prepare",
            "name": "prepare",
            "expected_seconds": 30,
            "gpu_count": 0,
            "submission_key": "cpu-reservation",
        }
        initial = self.client.post("/api/v1/jobs/queue", json=payload)
        self.assertEqual(initial.status_code, 200, initial.text)
        asyncio.run(self.db.observe_slurm_worker(
            worker.id, SlurmObservation(scheduler_state="COMPLETED", observed_at=time.time()),
        ))
        replay = self.client.post("/api/v1/jobs/queue", json=payload)
        self.assertEqual(replay.status_code, 200, replay.text)
        self.assertEqual(replay.json()["id"], initial.json()["id"])
        fresh = self.client.post(
            "/api/v1/jobs/queue", json={**payload, "submission_key": "new-work"},
        )
        self.assertEqual(fresh.status_code, 409, fresh.text)
        self.assertIn("SLURM_WORKER_FINISHED", fresh.text)
        self.assertIn("fresh allocation", fresh.text.lower())

    def test_end_to_end_completion_advances_next_job(self) -> None:
        launch = AsyncMock(side_effect=launch_success)
        with patch("worker_harness.job.ssh_tmux_new", new=launch):
            first = self.client.post(
                "/api/v1/jobs/queue",
                json={
                    "worker_id": "api",
                    "command": "python first.py",
                    "name": "first experiment",
                    "expected_seconds": 120,
                },
            ).json()
            second = self.client.post(
                "/api/v1/jobs/queue",
                json={
                    "worker_id": "api",
                    "command": "python second.py",
                    "name": "second experiment",
                    "expected_seconds": 60,
                },
            ).json()
            initial = self.client.get("/api/v1/jobs/queue?worker_id=api").json()
            self.assertEqual(
                [(job["id"], job["status"], job["position"]) for job in initial],
                [(first["id"], "running", 0), (second["id"], "pending", 1)],
            )

            manager = JobManager(self.db)
            worker = asyncio.run(self.db.get_worker("api"))
            first_job = asyncio.run(self.db.get_job(first["id"]))
            with patch(
                "worker_harness.job.ssh_tmux_running",
                new=AsyncMock(return_value=False),
            ), patch(
                "worker_harness.job.ssh_get_exit_code",
                new=AsyncMock(return_value=0),
            ):
                asyncio.run(manager.refresh_job_status(worker, first_job))

        advanced = self.client.get("/api/v1/jobs/queue?worker_id=api").json()
        self.assertEqual(
            [(job["id"], job["status"], job["position"]) for job in advanced],
            [(second["id"], "running", 0)],
        )
        history = self.client.get("/api/v1/jobs?worker_id=api").json()
        self.assertEqual(
            {job["command"] for job in history},
            {"python first.py", "python second.py"},
        )
