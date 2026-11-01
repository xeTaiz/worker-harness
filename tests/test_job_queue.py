"""Durable per-worker FIFO GPU queue behavior."""

import asyncio
import base64
import shlex
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from identity_support import bootstrap, login
from worker_harness.db import AuditActor, Database
from worker_harness.heartbeat import create_app, reconcile_queued_jobs
from worker_harness.job import JobManager
from worker_harness.models import GPUInfo, Job, JobStatus, TerminationReason, WorkerRegistration, WorkerStatus
from worker_harness.ssh import JobProbe, SSHResult, _build_job_command


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
        self.owner = await bootstrap(self.db)
        self.actor_id = f"user:{self.owner['id']}"
        self.actor = AuditActor(self.owner["id"], self.owner["display_name"], self.actor_id)
        log_patch = patch("worker_harness.job.ssh_read_log", new=AsyncMock(return_value=""))
        log_patch.start()
        self.addCleanup(log_patch.stop)
        self.worker = await self.db.upsert_worker(registration("w1"))
        self.manager = JobManager(self.db)

    async def asyncTearDown(self) -> None:
        await self.db.close()
        self.path.unlink(missing_ok=True)

    def job(self, **fields) -> Job:
        return Job(
            owner_user_id=self.owner["id"], submitted_by_actor_id=self.actor_id, **fields,
        )

    async def enqueue(self, worker, command, name, expected_seconds, **fields):
        return await self.manager.enqueue_job(
            worker, command, owner_user_id=self.owner["id"], actor_id=self.actor_id,
            name=name, expected_seconds=expected_seconds, **fields,
        )

    async def test_concurrent_enqueue_claims_fifo_once(self) -> None:
        launched: list[str] = []

        async def launch(_worker, _job_id, command, **_kwargs):
            launched.append(command)
            await asyncio.sleep(0)
            return launch_success()

        with patch("worker_harness.job.ssh_job_launch", new=launch):
            jobs = await asyncio.gather(*(
                self.enqueue(self.worker, f"command-{index}", f"job-{index}", 60)
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

        with patch("worker_harness.job.ssh_job_launch", new=launch):
            await asyncio.gather(
                self.enqueue(self.worker, "one", "one", 60),
                self.enqueue(worker2, "two", "two", 60),
            )

        self.assertEqual(peak, 2)

    async def test_four_gpus_fill_and_multi_gpu_head_blocks_backfill(self) -> None:
        with patch("worker_harness.job.ssh_job_launch", new=AsyncMock(side_effect=launch_success)):
            running = [
                await self.enqueue(self.worker, f"run-{index}", f"run-{index}", 60)
                for index in range(4)
            ]
            head = await self.enqueue(self.worker, "head", "head", 120, gpu_count=2)
            later = await self.enqueue(self.worker, "later", "later", 30)

            self.assertEqual(head.status, JobStatus.PENDING)
            self.assertEqual(later.status, JobStatus.PENDING)

            await self.db.finish_job(running[0].id, exit_code=0, termination_reason=None, finished_at=int(time.time()))
            await self.manager.dispatch_worker(self.worker)
            self.assertEqual((await self.db.get_job(head.id)).status, JobStatus.PENDING)
            self.assertEqual((await self.db.get_job(later.id)).status, JobStatus.PENDING)

            await self.db.finish_job(running[1].id, exit_code=0, termination_reason=None, finished_at=int(time.time()))
            await self.manager.dispatch_worker(self.worker)

        claimed_head = await self.db.get_job(head.id)
        self.assertEqual(claimed_head.status, JobStatus.RUNNING)
        self.assertEqual(claimed_head.gpu_indices, [0, 1])
        self.assertEqual((await self.db.get_job(later.id)).status, JobStatus.PENDING)

    async def test_launch_failure_advances_to_next_head(self) -> None:
        first = self.job(worker_id="w1", name="first", command="false", queue_managed=True, expected_seconds=10, gpu_count=1)
        second = self.job(worker_id="w1", name="second", command="true", queue_managed=True, expected_seconds=10, gpu_count=1)
        await self.db.insert_queued_job(first)
        await self.db.insert_queued_job(second)
        results = [
            SSHResult(stdout="", stderr="launch failed", returncode=1),
            launch_success(),
        ]
        with patch("worker_harness.job.ssh_job_launch", new=AsyncMock(side_effect=results)):
            await self.manager.dispatch_worker(self.worker)

        self.assertEqual((await self.db.get_job(first.id)).status, JobStatus.FAILED)
        self.assertEqual((await self.db.get_job(second.id)).status, JobStatus.RUNNING)
        self.assertEqual((await self.db.get_job(second.id)).gpu_indices, [0])

    async def test_completion_stop_and_pending_cancel_release_queue(self) -> None:
        one_gpu = await self.db.upsert_worker(registration("single", 1))
        launcher = AsyncMock(side_effect=launch_success)
        with patch("worker_harness.job.ssh_job_launch", new=launcher):
            first = await self.enqueue(one_gpu, "first", "first", 10)
            second = await self.enqueue(one_gpu, "second", "second", 10)
            third = await self.enqueue(one_gpu, "third", "third", 10)

            with patch("worker_harness.job.ssh_job_probe", new=AsyncMock(return_value=JobProbe("finished", exit_code=0))):
                await self.manager.refresh_job_status(one_gpu, first)
            self.assertEqual((await self.db.get_job(second.id)).status, JobStatus.RUNNING)

            stop = AsyncMock(return_value=JobProbe("finished", exit_code=-1))
            with patch("worker_harness.job.ssh_job_stop", new=stop):
                outcome = await self.manager.stop_job(one_gpu, second.id, reason=TerminationReason.USER_CANCELLED)
                self.assertEqual(outcome.state, "stopped")
            self.assertEqual((await self.db.get_job(third.id)).status, JobStatus.RUNNING)

            pending = await self.enqueue(one_gpu, "pending", "pending", 10)
            no_stop = AsyncMock(side_effect=AssertionError("pending cancellation must not use SSH"))
            with patch("worker_harness.job.ssh_job_stop", new=no_stop):
                outcome = await self.manager.stop_job(one_gpu, pending.id, reason=TerminationReason.USER_CANCELLED)
                self.assertEqual(outcome.state, "stopped")
            no_stop.assert_not_awaited()
            self.assertEqual((await self.db.get_job(pending.id)).status, JobStatus.FAILED)

    async def test_worker_move_and_update_append_and_renumber_queues(self) -> None:
        worker2 = await self.db.upsert_worker(registration("w2"))
        await self.db.set_worker_status("w1", WorkerStatus.OFFLINE)
        await self.db.set_worker_status("w2", WorkerStatus.OFFLINE)
        jobs = [
            self.job(worker_id="w1", name=name, command=name, expected_seconds=10, gpu_count=1)
            for name in ("a", "b", "c")
        ]
        other = self.job(worker_id="w2", name="d", command="d", expected_seconds=10, gpu_count=1)
        for job in [*jobs, other]:
            await self.db.insert_queued_job(job)

        await self.db.update_job_metadata(
            jobs[0].id, owner_user_id=self.owner["id"], actor=self.actor,
            changes={"name": "renamed", "expected_seconds": 99, "gpu_count": 2},
            pending=True, destination_worker_id=worker2.id, now=int(time.time()),
        )
        source = await self.db.list_queued_jobs("w1")
        destination = await self.db.list_queued_jobs("w2")
        self.assertEqual([(job.name, job.queue_order) for job in source], [("b", 1), ("c", 2)])
        self.assertEqual([(job.name, job.queue_order) for job in destination], [("d", 1), ("renamed", 2)])
        self.assertEqual(destination[1].expected_seconds, 99)
        self.assertEqual(destination[1].gpu_count, 2)

    async def test_starting_recovery_existing_absent_and_unreachable(self) -> None:
        queued = [
            self.job(worker_id="w1", name=name, command=name, queue_managed=True, expected_seconds=10, gpu_count=1)
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
        probe_results = {queued[0].id: JobProbe("running"), queued[1].id: JobProbe("absent"), queued[2].id: None}

        async def probe(_worker, job_id):
            return probe_results[job_id]

        with patch("worker_harness.job.ssh_job_probe", new=probe):
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
        first = self.job(worker_id="w1", command="first", gpu_count=1)
        second = self.job(worker_id="w1", command="second", gpu_count=1)
        await self.db.insert_queued_job(first)
        await self.db.insert_queued_job(second)
        self.assertIsNone(await self.db.claim_queued_job(second.id, [0], 100, worker_id="w1"))
        await self.db.update_job_metadata(
            first.id, owner_user_id=self.owner["id"], actor=self.actor,
            changes={"gpu_count": 2}, pending=True, destination_worker_id=other.id,
            now=int(time.time()),
        )
        self.assertIsNone(await self.db.claim_queued_job(first.id, [0], 100, worker_id="w1"))
        self.assertIsNone(await self.db.claim_queued_job(first.id, [0], 100, worker_id=other.id))
        claimed = await self.db.claim_queued_job(first.id, [0, 1], 100, worker_id=other.id)
        self.assertEqual((claimed.worker_id, claimed.gpu_indices), (other.id, [0, 1]))
        self.assertEqual((await self.db.get_job(second.id)).status, JobStatus.PENDING)

    async def test_unknown_launch_keeps_gpu_reserved_until_recovery(self) -> None:
        worker = await self.db.upsert_worker(registration("single", 1))
        launcher = AsyncMock(return_value=SSHResult(stdout="", stderr="connection lost", returncode=255))
        with patch("worker_harness.job.ssh_job_launch", new=launcher):
            first = await self.enqueue(worker, "first", "first", 10)
            second = await self.enqueue(worker, "second", "second", 10)
        self.assertEqual(first.status, JobStatus.STARTING)
        self.assertEqual(second.status, JobStatus.PENDING)
        self.assertEqual(await self.db.running_queue_gpu_indices(worker.id), {0})
        launcher.assert_awaited_once()

    async def test_recovery_does_not_rerun_completed_supervised_job(self) -> None:
        job = self.job(worker_id="w1", command="finished", gpu_count=1)
        await self.db.insert_queued_job(job)
        job = await self.db.claim_queued_job(job.id, [0], 100, worker_id="w1")
        with patch("worker_harness.job.ssh_job_probe", new=AsyncMock(return_value=JobProbe("finished", exit_code=0))), patch(
            "worker_harness.job.ssh_job_launch", new=AsyncMock(),
        ) as launch:
            recovered = await self.manager.reconcile_starting_job(self.worker, job)
            completed = await self.manager.refresh_job_status(self.worker, recovered)
        self.assertEqual((completed.status, completed.exit_code), (JobStatus.DONE, 0))
        launch.assert_not_awaited()

    async def test_reconciler_recovers_immediate_starting_job(self) -> None:
        job = self.job(worker_id="w1", command="immediate", status=JobStatus.STARTING)
        await self.db.insert_job(job)
        with patch("worker_harness.job.ssh_job_probe", new=AsyncMock(return_value=JobProbe("running"))):
            await reconcile_queued_jobs(self.db, self.manager)
        self.assertEqual((await self.db.get_job(job.id)).status, JobStatus.RUNNING)

    async def test_recovery_preserves_fifo_for_same_second_claims(self) -> None:
        jobs = [
            self.job(id=job_id, worker_id="w1", command=job_id, gpu_count=1)
            for job_id in ("z-first", "a-second")
        ]
        for index, job in enumerate(jobs):
            await self.db.insert_queued_job(job)
            await self.db.claim_queued_job(job.id, [index], 100, worker_id="w1")
        launch = AsyncMock(side_effect=launch_success)
        with patch("worker_harness.job.ssh_job_probe", new=AsyncMock(return_value=JobProbe("absent"))), patch(
            "worker_harness.job.ssh_job_launch", new=launch,
        ):
            await reconcile_queued_jobs(self.db, self.manager)
        self.assertEqual([call.args[1] for call in launch.await_args_list], ["z-first", "a-second"])

    async def test_stale_refresh_does_not_overwrite_stopped_job(self) -> None:
        with patch("worker_harness.job.ssh_job_launch", new=AsyncMock(side_effect=launch_success)):
            job = await self.enqueue(self.worker, "work", "work", 10)
        with patch("worker_harness.job.ssh_job_stop", new=AsyncMock(return_value=JobProbe("finished", exit_code=-1))):
            outcome = await self.manager.stop_job(self.worker, job.id, reason=TerminationReason.USER_CANCELLED)
            self.assertEqual(outcome.state, "stopped")
        with patch("worker_harness.job.ssh_job_probe", new=AsyncMock(side_effect=AssertionError("terminal job must not probe"))):
            refreshed = await self.manager.refresh_job_status(self.worker, job)
        self.assertEqual((refreshed.status, refreshed.exit_code), (JobStatus.FAILED, -1))

    async def test_job_script_exports_only_assigned_gpu_indices(self) -> None:
        for job_id, indices, expected in (
            ("queued", [3, 1], "CUDA_VISIBLE_DEVICES=3,1\n"),
            ("immediate", [], "CUDA_VISIBLE_DEVICES=\n"),
        ):
            with self.subTest(job_id=job_id):
                command = _build_job_command(
                    self.worker, job_id, 'printf "gpu=%s\\n" "$CUDA_VISIBLE_DEVICES"', indices,
                )
                words = shlex.split(command)
                payloads = [
                    base64.b64decode(words[index + 2]).decode()
                    for index, word in enumerate(words)
                    if word == "printf" and words[index + 1] == "%s"
                ]
                self.assertIn(expected, payloads)
                self.assertIn('#!/bin/bash\nprintf "gpu=%s\\n" "$CUDA_VISIBLE_DEVICES"\n', payloads)


class QueueApiTests(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
        tmp.close()
        self.path = Path(tmp.name)
        self.db = Database(self.path)
        asyncio.run(self.db.connect())
        self.owner = asyncio.run(bootstrap(self.db))
        asyncio.run(self.db.upsert_worker(registration("api", 1)))
        self.client = TestClient(create_app(self.db))
        self.client.headers.update(login(self.client, "admin"))

    def tearDown(self) -> None:
        self.client.close()
        asyncio.run(self.db.close())
        self.path.unlink(missing_ok=True)

    def test_queue_api_positions_capacity_and_non_pending_conflict(self) -> None:
        with patch("worker_harness.job.ssh_job_launch", new=AsyncMock(side_effect=launch_success)):
            first = self.client.post(
                "/api/v1/jobs/queue",
                json={"worker_id": "api", "command": "first", "name": "first", "expected_seconds": 60, "gpu_count": 1},
            )
            second = self.client.post(
                "/api/v1/jobs/queue",
                json={"worker_id": "api", "command": "second", "name": "second", "expected_seconds": 30, "gpu_count": 1},
            )

        self.assertEqual(first.status_code, 200, first.text)
        self.assertEqual(first.json()["status"], "running")
        self.assertEqual(second.status_code, 200, second.text)
        self.assertEqual(second.json()["status"], "pending")
        board = self.client.get("/api/v1/jobs/queue?worker_id=api").json()
        self.assertIsInstance(board["as_of"], int)
        self.assertEqual(board["workers"][0]["worker_id"], "api")
        queue = board["workers"][0]["jobs"]
        self.assertEqual([(item["status"], item["position"]) for item in queue], [("running", 0), ("pending", 1)])
        self.assertEqual(first.json()["owner_user_id"], self.owner["id"])
        self.assertNotIn("tmux_session", first.json())
        self.assertNotIn("command", queue[0])

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
        removed_position = self.client.patch(
            f"/api/v1/jobs/{second.json()['id']}/queue", json={"position": 1},
        )
        self.assertEqual(removed_position.status_code, 422)

    def test_offline_enqueue_dispatches_after_worker_returns(self) -> None:
        asyncio.run(self.db.set_worker_status("api", WorkerStatus.OFFLINE))
        launch = AsyncMock(side_effect=launch_success)
        with patch("worker_harness.job.ssh_job_launch", new=launch):
            response = self.client.post(
                "/api/v1/jobs/queue",
                json={"worker_id": "api", "command": "later", "name": "later", "expected_seconds": 60, "gpu_count": 1},
            )
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()["status"], "pending")
            asyncio.run(self.db.upsert_worker(registration("api", 1)))
            asyncio.run(reconcile_queued_jobs(self.db, JobManager(self.db)))

        self.assertEqual(asyncio.run(self.db.get_job(response.json()["id"])).status, JobStatus.RUNNING)
        launch.assert_awaited_once()

    def test_end_to_end_completion_advances_next_job(self) -> None:
        launch = AsyncMock(side_effect=launch_success)
        with patch("worker_harness.job.ssh_job_launch", new=launch):
            first = self.client.post(
                "/api/v1/jobs/queue",
                json={
                    "worker_id": "api",
                    "command": "python first.py",
                    "name": "first experiment",
                    "expected_seconds": 120,
                    "gpu_count": 1,
                },
            ).json()
            second = self.client.post(
                "/api/v1/jobs/queue",
                json={
                    "worker_id": "api",
                    "command": "python second.py",
                    "name": "second experiment",
                    "expected_seconds": 60,
                    "gpu_count": 1,
                },
            ).json()
            initial = self.client.get("/api/v1/jobs/queue?worker_id=api").json()["workers"][0]["jobs"]
            self.assertEqual(
                [(job["id"], job["status"], job["position"]) for job in initial],
                [(first["id"], "running", 0), (second["id"], "pending", 1)],
            )

            manager = JobManager(self.db)
            worker = asyncio.run(self.db.get_worker("api"))
            first_job = asyncio.run(self.db.get_job(first["id"]))
            with patch(
                "worker_harness.job.ssh_job_probe",
                new=AsyncMock(return_value=JobProbe("finished", exit_code=0)),
            ):
                asyncio.run(manager.refresh_job_status(worker, first_job))

        advanced = self.client.get("/api/v1/jobs/queue?worker_id=api").json()["workers"][0]["jobs"]
        self.assertEqual(
            [(job["id"], job["status"], job["position"]) for job in advanced],
            [(second["id"], "running", 0)],
        )
        history = self.client.get("/api/v1/jobs?worker_id=api").json()
        self.assertEqual(
            {job["command"] for job in history},
            {"python first.py", "python second.py"},
        )

    def test_preview_move_reorders_and_worker_transfer_appends(self) -> None:
        asyncio.run(self.db.set_worker_status("api", WorkerStatus.OFFLINE))
        asyncio.run(self.db.upsert_worker(registration("destination", 2)))
        asyncio.run(self.db.set_worker_status("destination", WorkerStatus.OFFLINE))
        jobs = []
        for worker_id, name in (("api", "a"), ("api", "b"), ("api", "c"), ("destination", "d")):
            response = self.client.post(
                "/api/v1/jobs/queue",
                json={"worker_id": worker_id, "name": name, "command": name, "expected_seconds": 10, "gpu_count": 1},
            )
            self.assertEqual(response.status_code, 200, response.text)
            jobs.append(response.json())
        board = self.client.get("/api/v1/jobs/queue?worker_id=api").json()["workers"][0]
        preview = self.client.post(
            f"/api/v1/jobs/{jobs[2]['id']}/queue/preview",
            json={"before_job_id": jobs[0]["id"], "expected_revision": board["revision"], "reason": "unblock the experiment"},
        )
        self.assertEqual(preview.status_code, 200, preview.text)
        self.assertEqual(preview.json()["before_order"], [job["id"] for job in jobs[:3]])
        self.assertEqual(preview.json()["after_order"], [jobs[2]["id"], jobs[0]["id"], jobs[1]["id"]])
        moved = self.client.post(
            f"/api/v1/jobs/{jobs[2]['id']}/queue/move",
            json={"preview_id": preview.json()["preview_id"], "expected_revision": board["revision"]},
        )
        self.assertEqual(moved.status_code, 200, moved.text)
        source = self.client.get("/api/v1/jobs/queue?worker_id=api").json()["workers"][0]["jobs"]
        self.assertEqual([(job["name"], job["position"]) for job in source], [("c", 1), ("a", 2), ("b", 3)])
        transferred = self.client.patch(
            f"/api/v1/jobs/{jobs[0]['id']}/queue",
            json={"worker_id": "destination", "name": "renamed", "expected_seconds": 99, "gpu_count": 2},
        )
        self.assertEqual(transferred.status_code, 200, transferred.text)
        source = self.client.get("/api/v1/jobs/queue?worker_id=api").json()["workers"][0]["jobs"]
        destination = self.client.get("/api/v1/jobs/queue?worker_id=destination").json()["workers"][0]["jobs"]
        self.assertEqual([(job["name"], job["position"]) for job in source], [("c", 1), ("b", 2)])
        self.assertEqual([(job["name"], job["position"]) for job in destination], [("d", 1), ("renamed", 2)])
        self.assertEqual(destination[1]["expected_seconds"], 99)
        self.assertEqual(destination[1]["gpu_count"], 2)
