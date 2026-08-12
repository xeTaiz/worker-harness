import asyncio
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from worker_harness.db import Database
from worker_harness.heartbeat import create_app
from worker_harness.marimo import allocate_local_port, build_launch_command, validate_absolute_path
from worker_harness.models import GPUInfo, Job, JobStatus, WorkerRegistration
from worker_harness.ssh import SSHResult


class FakeProcess:
    pid = 4242
    returncode = None

    def poll(self):
        return self.returncode


class MarimoHelpersTests(unittest.TestCase):
    def test_launch_command_is_quoted_and_loopback_helper_receives_values(self):
        command = build_launch_command(
            notebook_path="/code/project with spaces/demo.py",
            environment="/code/project with spaces/.venv",
            port=12345,
        )
        self.assertIn("wh-marimo-launch", command)
        self.assertIn("'/code/project with spaces/demo.py'", command)
        self.assertIn("'/code/project with spaces/.venv'", command)
        self.assertIn("--port 12345", command)

    def test_paths_must_be_normalized_absolute_paths(self):
        for value in ("relative.py", "/code/../secret", "/code/demo\n.py"):
            with self.assertRaises(ValueError):
                validate_absolute_path(value, "path")

    def test_local_port_is_allocated_from_acl_scoped_range(self):
        with patch.dict("os.environ", {"WH_MARIMO_PORT_MIN": "28000", "WH_MARIMO_PORT_MAX": "28010"}):
            port = allocate_local_port("127.0.0.1")
        self.assertGreaterEqual(port, 28000)
        self.assertLessEqual(port, 28010)


class MarimoApiTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
        self.tmp.close()
        self.db = Database(self.tmp.name)
        asyncio.run(self.db.connect())
        asyncio.run(self.db.upsert_worker(WorkerRegistration(
            worker_id="w-test",
            name="worker-test",
            worker_ip="100.64.0.9",
            dns_name="worker-test.tailnet",
            ssh_user="testuser",
            harness_dir="/var/lib/worker-harness/harness",
            gpu_count=1,
            gpus=[GPUInfo(index=0, name="GPU0", vram_total_gb=24, vram_used_gb=0)],
        )))
        self.client = TestClient(create_app(self.db))

    def tearDown(self):
        self.client.close()
        asyncio.run(self.db.close())
        Path(self.tmp.name).unlink(missing_ok=True)

    def test_create_and_delete_marimo_session(self):
        job = Job(
            id="job-marimo",
            worker_id="w-test",
            tmux_session="wh_marimo",
            command="launch",
            status=JobStatus.RUNNING,
            started_at=int(time.time()),
        )
        fake_proc = FakeProcess()
        with patch("worker_harness.heartbeat.allocate_worker_port", new=AsyncMock(return_value=2718)), \
             patch("worker_harness.heartbeat.tailnet_bind_host", return_value="100.64.0.4"), \
             patch("worker_harness.heartbeat.allocate_local_port", return_value=18123), \
             patch("worker_harness.heartbeat.JobManager.start_job", new=AsyncMock(return_value=job)) as start, \
             patch("worker_harness.heartbeat.ssh_port_forward", new=AsyncMock(return_value=fake_proc)) as forward, \
             patch("worker_harness.heartbeat.wait_until_ready", new=AsyncMock()), \
             patch("worker_harness.heartbeat.JobManager.refresh_job_status", new=AsyncMock(return_value=job)), \
             patch("worker_harness.heartbeat.JobManager.stop_job", new=AsyncMock(return_value=True)) as stop, \
             patch("worker_harness.heartbeat.TunnelRegistry.stop", return_value=True):
            asyncio.run(self.db.insert_job(job))
            response = self.client.post("/api/v1/marimo", json={
                "worker_id": "worker-test",
                "notebook_path": "/code/project/demo.py",
                "environment": "/code/project/.venv",
            })
            self.assertEqual(response.status_code, 201, response.text)
            body = response.json()
            self.assertEqual(body["url"], "http://100.64.0.4:18123")
            self.assertEqual(body["remote_port"], 2718)
            self.assertEqual(body["status"], "ready")
            start.assert_awaited_once()
            self.assertIn("wh-marimo-launch", start.await_args.args[1])
            forward.assert_awaited_once()
            self.assertEqual(forward.await_args.kwargs["bind_host"], "100.64.0.4")

            listed = self.client.get("/api/v1/marimo").json()
            self.assertEqual(len(listed), 1)
            self.assertNotIn("token", body)

            deleted = self.client.delete(f"/api/v1/marimo/{body['id']}")
            self.assertEqual(deleted.status_code, 200, deleted.text)
            stop.assert_awaited_once()
            self.assertEqual(self.client.get("/api/v1/marimo").json(), [])

    def test_delete_preserves_session_when_job_cannot_be_stopped(self):
        job = Job(id="job-stop-fail", worker_id="w-test", status=JobStatus.RUNNING)
        asyncio.run(self.db.insert_job(job))
        from worker_harness.models import MarimoSession
        session = MarimoSession(
            id="session-stop-fail", worker_id="w-test", notebook_path="/code/demo.py",
            environment="/code/.venv", job_id=job.id, tunnel_id="tunnel-missing",
            local_port=18125, remote_port=2718, bind_host="100.64.0.4",
            url="http://100.64.0.4:18125",
        )
        asyncio.run(self.db.insert_marimo_session(session))
        with patch("worker_harness.heartbeat.JobManager.stop_job", new=AsyncMock(return_value=False)):
            response = self.client.delete(f"/api/v1/marimo/{session.id}")
        self.assertEqual(response.status_code, 502, response.text)
        self.assertIsNotNone(asyncio.run(self.db.get_marimo_session(session.id)))

    def test_startup_failure_stops_job_and_removes_tunnel(self):
        job = Job(id="job-fail", worker_id="w-test", status=JobStatus.RUNNING)
        fake_proc = FakeProcess()
        with patch("worker_harness.heartbeat.allocate_worker_port", new=AsyncMock(return_value=2718)), \
             patch("worker_harness.heartbeat.tailnet_bind_host", return_value="100.64.0.4"), \
             patch("worker_harness.heartbeat.allocate_local_port", return_value=18124), \
             patch("worker_harness.heartbeat.JobManager.start_job", new=AsyncMock(return_value=job)), \
             patch("worker_harness.heartbeat.ssh_port_forward", new=AsyncMock(return_value=fake_proc)), \
             patch("worker_harness.heartbeat.wait_until_ready", new=AsyncMock(side_effect=TimeoutError("no health"))), \
             patch("worker_harness.heartbeat.JobManager.stop_job", new=AsyncMock(return_value=True)) as stop, \
             patch("worker_harness.heartbeat.TunnelRegistry.stop", return_value=True) as tunnel_stop:
            response = self.client.post("/api/v1/marimo", json={
                "worker_id": "w-test",
                "notebook_path": "/code/project/demo.py",
                "environment": "/code/project/.venv",
            })
        self.assertEqual(response.status_code, 502, response.text)
        stop.assert_awaited_once()
        tunnel_stop.assert_called_once()
        self.assertEqual(asyncio.run(self.db.list_marimo_sessions()), [])
        self.assertEqual(asyncio.run(self.db.list_port_forwards()), [])


if __name__ == "__main__":
    unittest.main()
