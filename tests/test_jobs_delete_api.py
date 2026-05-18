import asyncio
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from worker_harness.db import Database
from worker_harness.heartbeat import create_app
from worker_harness.models import GPUInfo, Job, JobStatus, WorkerRegistration


class JobsDeleteApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
        self.tmp.close()
        self.db = Database(self.tmp.name)
        asyncio.run(self.db.connect())

        reg = WorkerRegistration(
            worker_id="w-test",
            name="worker-test",
            worker_ip="10.0.0.9",
            ssh_port=22,
            gpu_count=1,
            gpus=[GPUInfo(index=0, name="GPU0", vram_total_gb=24, vram_used_gb=0)],
            cpu_cores=8,
            total_ram_gb=64,
            used_ram_gb=8,
            total_disk_gb=500,
            used_disk_gb=100,
        )
        asyncio.run(self.db.upsert_worker(reg))

        now = int(time.time())
        self.done_job_id = "job-done-1"
        self.running_job_id = "job-run-1"

        asyncio.run(
            self.db.insert_job(
                Job(
                    id=self.done_job_id,
                    worker_id="w-test",
                    tmux_session=f"wh_{self.done_job_id}",
                    command="echo done",
                    status=JobStatus.DONE,
                    exit_code=0,
                    started_at=now - 10,
                    finished_at=now - 5,
                )
            )
        )
        asyncio.run(
            self.db.insert_job(
                Job(
                    id=self.running_job_id,
                    worker_id="w-test",
                    tmux_session=f"wh_{self.running_job_id}",
                    command="sleep 999",
                    status=JobStatus.RUNNING,
                    started_at=now - 3,
                )
            )
        )

        self.app = create_app(self.db)
        self.client = TestClient(self.app)

    def tearDown(self) -> None:
        self.client.close()
        asyncio.run(self.db.close())
        Path(self.tmp.name).unlink(missing_ok=True)

    def test_delete_done_job_is_idempotent_success(self):
        with patch("worker_harness.job.JobManager.stop_job", new=AsyncMock(return_value=True)) as stop_mock:
            resp = self.client.delete(f"/api/v1/jobs/{self.done_job_id}")

        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["job_id"], self.done_job_id)
        self.assertTrue(body["stopped"])
        self.assertTrue(body["already_terminal"])
        self.assertEqual(body["status"], "done")
        stop_mock.assert_not_awaited()

    def test_delete_running_job_calls_stop_and_succeeds(self):
        async def _fake_stop(jm_self, worker, job_id):
            job = await jm_self.db.get_job(job_id)
            job.status = JobStatus.FAILED
            job.exit_code = -1
            job.finished_at = int(time.time())
            await jm_self.db.update_job(job)
            return True

        with patch("worker_harness.job.JobManager.stop_job", new=_fake_stop):
            resp = self.client.delete(f"/api/v1/jobs/{self.running_job_id}")

        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["job_id"], self.running_job_id)
        self.assertTrue(body["stopped"])
        self.assertFalse(body["already_terminal"])
        self.assertEqual(body["status"], "failed")


if __name__ == "__main__":
    unittest.main()
