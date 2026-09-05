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

from worker_harness.db import Database
from worker_harness.heartbeat import create_app, reconcile_queued_jobs
from worker_harness.job import JobManager
from worker_harness.models import GPUInfo, Job, JobStatus, WorkerRegistration, WorkerStatus
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
            await self.db.claim_queued_job(job.id, [index], 100 + index)

        await self.db.close()
        self.db = Database(self.path)
        await self.db.connect()
        manager = JobManager(self.db)
        worker = await self.db.get_worker("w1")
        probe_results = {queued[0].id: True, queued[1].id: False, queued[2].id: None}

        async def probe(_worker, job_id):
            return probe_results[job_id]

        with patch("worker_harness.job.ssh_tmux_exists", new=probe):
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

    async def test_job_script_exports_only_queued_gpu_indices(self) -> None:
        queued_command = _build_job_command(self.worker, "queued", "python train.py", [3, 1])
        immediate_command = _build_job_command(self.worker, "immediate", "echo hi")

        def decode_script(command: str) -> str:
            encoded = re.search(r"echo '([^']+)' \| base64", command).group(1)
            return base64.b64decode(encoded).decode()

        self.assertIn("export CUDA_VISIBLE_DEVICES=3,1\n", decode_script(queued_command))
        self.assertNotIn("CUDA_VISIBLE_DEVICES", decode_script(immediate_command))


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
