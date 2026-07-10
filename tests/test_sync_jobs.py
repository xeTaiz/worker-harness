"""Test sync job execution — commands that block and return stdout.

The sync mechanism goes through the same tmux + log file path as async jobs,
so the output is fully logged. These tests mock the SSH layer to verify the
orchestrator's polling and response shaping.
"""

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
from worker_harness.ssh import SSHResult


class SyncJobsApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
        self.tmp.close()
        self.db = Database(self.tmp.name)
        asyncio.run(self.db.connect())

        reg = WorkerRegistration(
            worker_id="w-test",
            name="worker-test",
            worker_ip="100.64.0.2",
            dns_name="worker-test.tailnet",
            ssh_user="testuser",
            harness_dir="/var/lib/worker-harness/harness",
            gpu_count=1,
            gpus=[GPUInfo(index=0, name="GPU0", vram_total_gb=24, vram_used_gb=0)],
            cpu_cores=8,
            total_ram_gb=64,
            used_ram_gb=8,
            total_disk_gb=500,
            used_disk_gb=100,
        )
        asyncio.run(self.db.upsert_worker(reg))

        self.app = create_app(self.db)
        self.client = TestClient(self.app)

    def tearDown(self) -> None:
        self.client.close()
        asyncio.run(self.db.close())
        Path(self.tmp.name).unlink(missing_ok=True)

    def test_sync_job_returns_stdout(self):
        """Sync mode blocks and returns the command output."""
        from worker_harness.ssh import SSHResult

        # Mock ssh_tmux_new to succeed (job starts)
        async def mock_tmux_new(worker, job_id, command, pty_enabled=True):
            return SSHResult(stdout="started", stderr="", returncode=0)

        # Mock ssh_tmux_running to return False (job is done immediately)
        async def mock_tmux_running(worker, job_id):
            return False

        # Mock ssh_get_exit_code to return 0
        async def mock_get_exit_code(worker, job_id):
            return 0

        # Mock async_ssh_run for the log cat — return the log content
        async def mock_ssh_run(worker, command, timeout=30):
            if "cat '" in command:
                return SSHResult(stdout="hello world\nEXIT:0\n", stderr="", returncode=0)
            return SSHResult(stdout="", stderr="", returncode=0)

        with patch("worker_harness.job.ssh_tmux_new", new=mock_tmux_new), \
             patch("worker_harness.job.ssh_tmux_running", new=mock_tmux_running), \
             patch("worker_harness.job.ssh_get_exit_code", new=mock_get_exit_code), \
             patch("worker_harness.heartbeat.async_ssh_run", new=mock_ssh_run):

            resp = self.client.post(
                "/api/v1/jobs",
                json={
                    "worker_id": "w-test",
                    "command": "echo hello world",
                    "sync": True,
                    "sync_timeout": 10,
                    "no_pty": True,
                },
            )

        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["status"], "done")
        self.assertEqual(body["exit_code"], 0)
        self.assertEqual(body["stdout"], "hello world")
        # The EXIT marker should be stripped
        self.assertNotIn("EXIT:", body["stdout"])

    def test_sync_job_preserves_job_id(self):
        """Sync mode still creates a job record with an ID."""
        from worker_harness.ssh import SSHResult

        async def mock_tmux_new(worker, job_id, command, pty_enabled=True):
            return SSHResult(stdout="started", stderr="", returncode=0)

        async def mock_tmux_running(worker, job_id):
            return False

        async def mock_get_exit_code(worker, job_id):
            return 0

        async def mock_ssh_run(worker, command, timeout=30):
            return SSHResult(stdout="ok\nEXIT:0\n", stderr="", returncode=0)

        with patch("worker_harness.job.ssh_tmux_new", new=mock_tmux_new), \
             patch("worker_harness.job.ssh_tmux_running", new=mock_tmux_running), \
             patch("worker_harness.job.ssh_get_exit_code", new=mock_get_exit_code), \
             patch("worker_harness.heartbeat.async_ssh_run", new=mock_ssh_run):

            resp = self.client.post(
                "/api/v1/jobs",
                json={"worker_id": "w-test", "command": "echo ok", "sync": True},
            )

        body = resp.json()
        self.assertTrue(body["id"])
        self.assertEqual(body["status"], "done")

    def test_sync_job_failure_returns_exit_code_and_output(self):
        """Failed sync command returns non-zero exit code and stderr in stdout."""
        from worker_harness.ssh import SSHResult

        async def mock_tmux_new(worker, job_id, command, pty_enabled=True):
            return SSHResult(stdout="started", stderr="", returncode=0)

        async def mock_tmux_running(worker, job_id):
            return False

        async def mock_get_exit_code(worker, job_id):
            return 1

        async def mock_ssh_run(worker, command, timeout=30):
            return SSHResult(stdout="cat: /nonexistent: No such file\nEXIT:1\n", stderr="", returncode=0)

        with patch("worker_harness.job.ssh_tmux_new", new=mock_tmux_new), \
             patch("worker_harness.job.ssh_tmux_running", new=mock_tmux_running), \
             patch("worker_harness.job.ssh_get_exit_code", new=mock_get_exit_code), \
             patch("worker_harness.heartbeat.async_ssh_run", new=mock_ssh_run):

            resp = self.client.post(
                "/api/v1/jobs",
                json={"worker_id": "w-test", "command": "cat /nonexistent", "sync": True},
            )

        body = resp.json()
        self.assertEqual(body["status"], "failed")
        self.assertEqual(body["exit_code"], 1)
        self.assertIn("No such file", body["stdout"])

    def test_async_job_unchanged_without_sync(self):
        """Without sync=true, behavior is unchanged — returns job immediately."""
        from worker_harness.ssh import SSHResult

        async def mock_tmux_new(worker, job_id, command, pty_enabled=True):
            return SSHResult(stdout="started", stderr="", returncode=0)

        with patch("worker_harness.job.ssh_tmux_new", new=mock_tmux_new):
            resp = self.client.post(
                "/api/v1/jobs",
                json={"worker_id": "w-test", "command": "echo hi"},
            )

        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["status"], "running")
        # No stdout field in async mode
        self.assertNotIn("stdout", body)

    def test_sync_job_unknown_worker_returns_404(self):
        resp = self.client.post(
            "/api/v1/jobs",
            json={"worker_id": "nonexistent", "command": "echo hi", "sync": True},
        )
        self.assertEqual(resp.status_code, 404)

    def test_sync_job_start_failure_returns_failed_status(self):
        """If the tmux session fails to start, sync returns failed status."""
        from worker_harness.ssh import SSHResult

        async def mock_tmux_new(worker, job_id, command, pty_enabled=True):
            return SSHResult(stdout="", stderr="connection refused", returncode=1)

        async def mock_ssh_run(worker, command, timeout=30):
            return SSHResult(stdout="", stderr="", returncode=0)

        with patch("worker_harness.job.ssh_tmux_new", new=mock_tmux_new), \
             patch("worker_harness.heartbeat.async_ssh_run", new=mock_ssh_run):

            resp = self.client.post(
                "/api/v1/jobs",
                json={"worker_id": "w-test", "command": "echo hi", "sync": True},
            )

        body = resp.json()
        self.assertEqual(body["status"], "failed")
        self.assertEqual(body["exit_code"], -1)

    def test_sync_job_timeout_returns_running_status(self):
        """If sync_timeout expires, job status remains running with partial output."""
        from worker_harness.ssh import SSHResult

        async def mock_tmux_new(worker, job_id, command, pty_enabled=True):
            return SSHResult(stdout="started", stderr="", returncode=0)

        # Job never finishes — always running
        async def mock_tmux_running(worker, job_id):
            return True

        async def mock_ssh_run(worker, command, timeout=30):
            return SSHResult(stdout="partial output\nEXIT:0\n", stderr="", returncode=0)

        with patch("worker_harness.job.ssh_tmux_new", new=mock_tmux_new), \
             patch("worker_harness.job.ssh_tmux_running", new=mock_tmux_running), \
             patch("worker_harness.heartbeat.async_ssh_run", new=mock_ssh_run):

            resp = self.client.post(
                "/api/v1/jobs",
                json={
                    "worker_id": "w-test",
                    "command": "sleep 999",
                    "sync": True,
                    "sync_timeout": 1,  # 1 second timeout
                },
            )

        body = resp.json()
        # Job is still running — sync timed out
        self.assertEqual(body["status"], "running")
        # stdout still has the partial log content
        self.assertIn("partial output", body["stdout"])


if __name__ == "__main__":
    unittest.main()
