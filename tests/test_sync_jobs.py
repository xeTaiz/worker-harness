"""Sync execution uses supervisor probes and returns the unmarked job log."""

import asyncio
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from identity_support import bootstrap, login
from worker_harness.db import Database
from worker_harness.heartbeat import create_app, reconcile_active_ssh_jobs
from worker_harness.job import JobManager
from worker_harness.models import ExecutionState, GPUInfo, Job, JobStatus, WorkerRegistration
from worker_harness.ssh import JobProbe, SSHResult


class SyncJobsApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
        self.tmp.close()
        self.db = Database(self.tmp.name)
        asyncio.run(self.db.connect())
        self.owner = asyncio.run(bootstrap(self.db))
        self.actor_id = f"user:{self.owner['id']}"
        reg = WorkerRegistration(
            worker_id="w-test", name="worker-test", worker_ip="100.64.0.2",
            dns_name="worker-test.tailnet", ssh_user="testuser",
            harness_dir="/var/lib/worker-harness/harness", gpu_count=1,
            gpus=[GPUInfo(index=0, name="GPU0", vram_total_gb=24, vram_used_gb=0)],
            cpu_cores=8, total_ram_gb=64, used_ram_gb=8,
            total_disk_gb=500, used_disk_gb=100,
        )
        asyncio.run(self.db.upsert_worker(reg))
        self.app = create_app(self.db)
        self.client = TestClient(self.app)
        self.client.headers.update(login(self.client, "admin"))
        log_patch = patch("worker_harness.job.ssh_read_log", new=AsyncMock(return_value=""))
        log_patch.start()
        self.addCleanup(log_patch.stop)

    def tearDown(self) -> None:
        self.client.close()
        asyncio.run(self.db.close())
        Path(self.tmp.name).unlink(missing_ok=True)

    def job(self, **fields) -> Job:
        return Job(
            owner_user_id=self.owner["id"], submitted_by_actor_id=self.actor_id,
            worker_id="w-test", **fields,
        )

    def payload(self, command="echo hi", **fields) -> dict:
        return {
            "worker_id": "w-test", "command": command, "name": "test command",
            "expected_seconds": 10, **fields,
        }

    def test_jobs_list_returns_cached_state_without_ssh(self):
        job = self.job(id="cached-job", status=JobStatus.RUNNING)
        asyncio.run(self.db.insert_job(job))
        refresh = AsyncMock(side_effect=AssertionError("listing must not probe SSH"))
        with patch("worker_harness.heartbeat.JobManager.refresh_job_status", new=refresh):
            response = self.client.get("/api/v1/jobs")
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()[0]["id"], job.id)
        self.assertEqual(response.json()[0]["status"], "running")
        self.assertEqual(response.json()[0]["owner_user_id"], self.owner["id"])
        self.assertNotIn("tmux_session", response.json()[0])
        refresh.assert_not_awaited()

    def test_refresh_transport_failure_preserves_running_job(self):
        worker = asyncio.run(self.db.get_worker("w-test"))
        job = self.job(id="unreachable-job", status=JobStatus.RUNNING)
        asyncio.run(self.db.insert_job(job))
        with patch("worker_harness.job.ssh_job_probe", new=AsyncMock(return_value=None)):
            refreshed = asyncio.run(JobManager(self.db).refresh_job_status(worker, job))
        self.assertEqual(refreshed.status, JobStatus.RUNNING)
        self.assertEqual(asyncio.run(self.db.get_job(job.id)).status, JobStatus.RUNNING)

    def test_lost_supervisor_preserves_running_job_and_allocation(self):
        worker = asyncio.run(self.db.get_worker("w-test"))
        job = self.job(id="lost-supervisor", gpu_count=1)
        asyncio.run(self.db.insert_queued_job(job))
        asyncio.run(self.db.claim_queued_job(job.id, [0], int(time.time()), worker_id="w-test"))
        job = asyncio.run(self.db.mark_job_running(job.id))
        with patch("worker_harness.job.ssh_job_probe", new=AsyncMock(return_value=JobProbe("lost"))):
            refreshed = asyncio.run(JobManager(self.db).refresh_job_status(worker, job))
        self.assertEqual(refreshed.status, JobStatus.RUNNING)
        self.assertEqual(refreshed.execution_state, ExecutionState.TERMINATION_UNKNOWN)
        self.assertEqual(refreshed.finished_at, 0)
        self.assertEqual(asyncio.run(self.db.running_queue_gpu_indices("w-test")), {0})

    def test_reconciler_bounds_concurrent_status_probes(self):
        for index in range(6):
            asyncio.run(self.db.insert_job(self.job(id=f"running-{index}", status=JobStatus.RUNNING)))
        active = 0
        peak = 0
        reconciled: list[str] = []

        async def refresh(_manager, _worker, job):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            try:
                await asyncio.sleep(0.01)
                reconciled.append(job.id)
                return job
            finally:
                active -= 1

        with patch.object(JobManager, "refresh_job_status", new=refresh):
            asyncio.run(reconcile_active_ssh_jobs(self.db, JobManager(self.db), max_concurrent=2))
        self.assertEqual(peak, 2)
        self.assertEqual(len(reconciled), 6)

    def test_sync_job_returns_stdout(self):
        with patch("worker_harness.job.ssh_job_launch", new=AsyncMock(return_value=SSHResult("started", "", 0))), patch(
            "worker_harness.job.ssh_job_probe", new=AsyncMock(return_value=JobProbe("finished", exit_code=0)),
        ), patch("worker_harness.heartbeat.async_ssh_run", new=AsyncMock(return_value=SSHResult("hello world\n", "", 0))):
            resp = self.client.post("/api/v1/jobs", json=self.payload("echo hello world", sync=True, sync_timeout=10, no_pty=True))
        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        self.assertEqual(body["status"], "done")
        self.assertEqual(body["exit_code"], 0)
        self.assertEqual(body["stdout"], "hello world")

    def test_sync_job_preserves_job_id(self):
        with patch("worker_harness.job.ssh_job_launch", new=AsyncMock(return_value=SSHResult("started", "", 0))), patch(
            "worker_harness.job.ssh_job_probe", new=AsyncMock(return_value=JobProbe("finished", exit_code=0)),
        ), patch("worker_harness.heartbeat.async_ssh_run", new=AsyncMock(return_value=SSHResult("ok\n", "", 0))):
            resp = self.client.post("/api/v1/jobs", json=self.payload("echo ok", sync=True))
        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        persisted = asyncio.run(self.db.get_job(body["id"]))
        self.assertEqual(persisted.id, body["id"])
        self.assertEqual(body["status"], "done")

    def test_sync_job_failure_returns_exit_code_and_output(self):
        with patch("worker_harness.job.ssh_job_launch", new=AsyncMock(return_value=SSHResult("started", "", 0))), patch(
            "worker_harness.job.ssh_job_probe", new=AsyncMock(return_value=JobProbe("finished", exit_code=1)),
        ), patch("worker_harness.heartbeat.async_ssh_run", new=AsyncMock(return_value=SSHResult("cat: /nonexistent: No such file\n", "", 0))):
            resp = self.client.post("/api/v1/jobs", json=self.payload("cat /nonexistent", sync=True))
        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        self.assertEqual(body["status"], "failed")
        self.assertEqual(body["exit_code"], 1)
        self.assertEqual(body["stdout"], "cat: /nonexistent: No such file")

    def test_async_job_unchanged_without_sync(self):
        with patch("worker_harness.job.ssh_job_launch", new=AsyncMock(return_value=SSHResult("started", "", 0))):
            resp = self.client.post("/api/v1/jobs", json=self.payload())
        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        self.assertEqual(body["status"], "running")
        self.assertEqual(body["gpu_count"], 0)
        self.assertNotIn("stdout", body)

    def test_async_job_persists_display_name_and_id_keyed_launch(self):
        launch = AsyncMock(return_value=SSHResult("started", "", 0))
        with patch("worker_harness.job.ssh_job_launch", new=launch):
            resp = self.client.post("/api/v1/jobs", json=self.payload(name="mutable label"))
        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        persisted = asyncio.run(self.db.get_job(body["id"]))
        self.assertEqual(persisted.name, "mutable label")
        self.assertEqual(persisted.owner_user_id, self.owner["id"])
        self.assertEqual(launch.await_args.args[1], persisted.id)
        self.assertEqual(launch.await_args.kwargs["gpu_indices"], [])
        self.assertNotIn("tmux_session", body)

    def test_sync_job_unknown_worker_returns_404(self):
        resp = self.client.post("/api/v1/jobs", json=self.payload(worker_id="nonexistent", sync=True))
        self.assertEqual(resp.status_code, 404)

    def test_sync_job_start_failure_returns_failed_status(self):
        with patch("worker_harness.job.ssh_job_launch", new=AsyncMock(return_value=SSHResult("", "launch failed", 1))), patch(
            "worker_harness.heartbeat.async_ssh_run", new=AsyncMock(return_value=SSHResult("", "", 0)),
        ):
            resp = self.client.post("/api/v1/jobs", json=self.payload(sync=True))
        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        self.assertEqual(body["status"], "failed")
        self.assertEqual(body["exit_code"], -1)

    def test_sync_job_timeout_returns_running_status(self):
        with patch("worker_harness.job.ssh_job_launch", new=AsyncMock(return_value=SSHResult("started", "", 0))), patch(
            "worker_harness.job.ssh_job_probe", new=AsyncMock(return_value=JobProbe("running")),
        ), patch("worker_harness.heartbeat.async_ssh_run", new=AsyncMock(return_value=SSHResult("partial output\n", "", 0))):
            resp = self.client.post("/api/v1/jobs", json=self.payload("sleep 999", sync=True, sync_timeout=1))
        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        self.assertEqual(body["status"], "running")
        self.assertEqual(body["stdout"], "partial output")


if __name__ == "__main__":
    unittest.main()
