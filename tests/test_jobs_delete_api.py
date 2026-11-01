import asyncio
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from identity_support import bootstrap, login
from worker_harness.db import Database
from worker_harness.heartbeat import create_app
from worker_harness.models import ExecutionState, GPUInfo, Job, JobStatus, TerminationReason, WorkerRegistration
from worker_harness.ssh import JobProbe, SSHResult


class JobsDeleteApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
        self.tmp.close()
        self.db = Database(self.tmp.name)
        asyncio.run(self.db.connect())
        self.owner = asyncio.run(bootstrap(self.db))
        self.actor_id = f"user:{self.owner['id']}"
        reg = WorkerRegistration(
            worker_id="w-test", name="worker-test", worker_ip="100.64.0.9", ssh_user="root",
            gpu_count=1,
            gpus=[GPUInfo(index=0, name="GPU0", vram_total_gb=24, vram_used_gb=0, busy=False)],
            cpu_cores=8, total_ram_gb=64, used_ram_gb=8, total_disk_gb=500, used_disk_gb=100,
        )
        asyncio.run(self.db.upsert_worker(reg))
        now = int(time.time())
        self.done_job_id = "job-done-1"
        self.running_job_id = "job-run-1"
        asyncio.run(self.db.insert_job(self.job(
            id=self.done_job_id, command="echo done", status=JobStatus.DONE,
            exit_code=0, started_at=now - 10, finished_at=now - 5,
        )))
        asyncio.run(self.db.insert_job(self.job(
            id=self.running_job_id, command="sleep 999", status=JobStatus.RUNNING, started_at=now - 3,
        )))
        self.app = create_app(self.db)
        self.client = TestClient(self.app)
        self.client.headers.update(login(self.client, "admin"))

    def tearDown(self) -> None:
        self.client.close()
        asyncio.run(self.db.close())
        Path(self.tmp.name).unlink(missing_ok=True)

    def job(self, **fields) -> Job:
        return Job(
            worker_id="w-test", owner_user_id=self.owner["id"],
            submitted_by_actor_id=self.actor_id, **fields,
        )

    def queued_pair(self) -> tuple[Job, Job]:
        running = self.job(id="queued-running", name="running", command="sleep 10", expected_seconds=10, gpu_count=1)
        pending = self.job(id="queued-next", name="next", command="echo next", expected_seconds=10, gpu_count=1)
        asyncio.run(self.db.insert_queued_job(running))
        asyncio.run(self.db.claim_queued_job(running.id, [0], int(time.time()), worker_id="w-test"))
        running = asyncio.run(self.db.mark_job_running(running.id))
        asyncio.run(self.db.insert_queued_job(pending))
        return running, pending

    def test_delete_done_job_is_idempotent_success(self):
        with patch("worker_harness.job.JobManager.stop_job", new=AsyncMock(side_effect=AssertionError("terminal jobs do not stop"))) as stop_mock:
            resp = self.client.delete(f"/api/v1/jobs/{self.done_job_id}")
        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        self.assertEqual(body["job_id"], self.done_job_id)
        self.assertTrue(body["stopped"])
        self.assertTrue(body["already_terminal"])
        self.assertEqual(body["status"], "done")
        stop_mock.assert_not_awaited()

    def test_delete_running_job_calls_stop_and_succeeds(self):
        stop = AsyncMock(return_value=JobProbe("finished", exit_code=-1, termination_reason="user_cancelled"))
        with patch("worker_harness.job.ssh_job_stop", new=stop):
            resp = self.client.delete(f"/api/v1/jobs/{self.running_job_id}")
        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        self.assertEqual(body["job_id"], self.running_job_id)
        self.assertTrue(body["stopped"])
        self.assertFalse(body["already_terminal"])
        self.assertEqual(body["status"], "failed")
        self.assertEqual(stop.await_args.args[1:], (self.running_job_id, "user_cancelled"))
        persisted = asyncio.run(self.db.get_job(self.running_job_id))
        self.assertEqual(persisted.termination_reason, TerminationReason.USER_CANCELLED)

    def test_delete_pending_queue_job_skips_ssh(self):
        job = self.job(id="queued-pending", name="pending", command="sleep 10", expected_seconds=10, gpu_count=1)
        asyncio.run(self.db.insert_queued_job(job))
        stop = AsyncMock(side_effect=AssertionError("pending cancellation must not use SSH"))
        with patch("worker_harness.job.ssh_job_stop", new=stop):
            resp = self.client.delete(f"/api/v1/jobs/{job.id}")
        self.assertEqual(resp.status_code, 200, resp.text)
        stop.assert_not_awaited()
        self.assertEqual(asyncio.run(self.db.get_job(job.id)).status, JobStatus.FAILED)

    def test_delete_running_queue_job_releases_gpu_and_advances(self):
        running, pending = self.queued_pair()
        with patch("worker_harness.job.ssh_job_stop", new=AsyncMock(return_value=JobProbe("finished", exit_code=-1))), patch(
            "worker_harness.job.ssh_job_launch", new=AsyncMock(return_value=SSHResult("started", "", 0)),
        ):
            resp = self.client.delete(f"/api/v1/jobs/{running.id}")
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertEqual(asyncio.run(self.db.get_job(running.id)).status, JobStatus.FAILED)
        self.assertEqual(asyncio.run(self.db.get_job(pending.id)).status, JobStatus.RUNNING)

    def test_delete_unconfirmed_stop_retains_allocation_and_queue(self):
        running, pending = self.queued_pair()
        for probe in (None, JobProbe("starting"), JobProbe("running"), JobProbe("lost"), JobProbe("absent")):
            with self.subTest(probe=probe):
                stop = AsyncMock(return_value=probe)
                launch = AsyncMock(side_effect=AssertionError("unconfirmed stop must not release the GPU"))
                with patch("worker_harness.job.ssh_job_stop", new=stop), patch("worker_harness.job.ssh_job_launch", new=launch):
                    resp = self.client.delete(f"/api/v1/jobs/{running.id}")
                self.assertEqual(resp.status_code, 503, resp.text)
                self.assertEqual(resp.json()["detail"]["code"], "STOP_UNCONFIRMED")
                persisted = asyncio.run(self.db.get_job(running.id))
                self.assertEqual(persisted.status, JobStatus.RUNNING)
                self.assertIn(persisted.execution_state, (ExecutionState.STOPPING, ExecutionState.TERMINATION_UNKNOWN))
                self.assertEqual(persisted.gpu_indices, [0])
                self.assertEqual(asyncio.run(self.db.running_queue_gpu_indices("w-test")), {0})
                self.assertEqual(asyncio.run(self.db.get_job(pending.id)).status, JobStatus.PENDING)
                self.assertEqual(persisted.finished_at, 0)
                launch.assert_not_awaited()

    def test_delete_starting_absent_probe_is_not_confirmed_termination(self):
        starting = self.job(id="queued-starting", name="starting", command="sleep 10", expected_seconds=10, gpu_count=1)
        asyncio.run(self.db.insert_queued_job(starting))
        asyncio.run(self.db.claim_queued_job(starting.id, [0], int(time.time()), worker_id="w-test"))
        stop = AsyncMock(return_value=JobProbe("absent"))
        with patch("worker_harness.job.ssh_job_stop", new=stop):
            resp = self.client.delete(f"/api/v1/jobs/{starting.id}")
        self.assertEqual(resp.status_code, 503, resp.text)
        self.assertEqual(resp.json()["detail"]["code"], "STOP_UNCONFIRMED")
        persisted = asyncio.run(self.db.get_job(starting.id))
        self.assertEqual(persisted.status, JobStatus.STARTING)
        self.assertEqual(persisted.gpu_indices, [0])
        self.assertEqual(persisted.finished_at, 0)
        self.assertEqual(asyncio.run(self.db.running_queue_gpu_indices("w-test")), {0})


if __name__ == "__main__":
    unittest.main()
