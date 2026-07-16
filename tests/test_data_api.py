"""Tests for bind-path discovery and rsync copy orchestration."""

import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from worker_harness.data import DataPathError, reverse_data_paths, validate_data_path
from worker_harness.db import Database
from worker_harness.heartbeat import create_app, create_registration_app
from worker_harness.models import WorkerRegistration, WorkerStatus
from worker_harness.ssh import SSHResult


class DataApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
        self.tmp.close()
        self.db = Database(self.tmp.name)
        asyncio.run(self.db.connect())
        for worker_id, name, paths in (
            ("w-one", "worker-one", ["/data/imagenet", "/code/project"]),
            ("w-two", "worker-two", ["/data/cache"]),
        ):
            asyncio.run(self.db.upsert_worker(WorkerRegistration(
                worker_id=worker_id,
                name=name,
                worker_ip=f"100.64.0.{1 if worker_id == 'w-one' else 2}",
                dns_name=f"{name}.tailnet",
                ssh_user="testuser",
                harness_dir="/var/lib/worker-harness/harness",
                data_paths=paths,
            )))
        self.client = TestClient(create_app(self.db))

    def tearDown(self) -> None:
        self.client.close()
        asyncio.run(self.db.close())
        Path(self.tmp.name).unlink(missing_ok=True)

    def test_data_list_returns_exact_advertised_paths(self):
        response = self.client.get("/api/v1/data")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {
            "/code/project": [{"worker_id": "w-one", "worker_name": "worker-one"}],
            "/data/cache": [{"worker_id": "w-two", "worker_name": "worker-two"}],
            "/data/imagenet": [{"worker_id": "w-one", "worker_name": "worker-one"}],
        })

    def test_data_list_excludes_offline_by_default(self):
        asyncio.run(self.db.set_worker_status("w-two", WorkerStatus.OFFLINE))
        self.assertNotIn("/data/cache", self.client.get("/api/v1/data").json())
        self.assertIn("/data/cache", self.client.get("/api/v1/data", params={"include_offline": True}).json())

    def test_copy_exports_then_starts_destination_job_without_peer_ssh(self):
        endpoint = json.dumps({"port": 22003, "module": "transfer", "username": "transfer", "password": "secret"})
        with patch(
            "worker_harness.heartbeat.async_ssh_run",
            new=AsyncMock(return_value=SSHResult(stdout=endpoint + "\n", stderr="", returncode=0)),
        ) as ssh, patch(
            "worker_harness.heartbeat.ssh_upload_bytes",
            new=AsyncMock(return_value=SSHResult(stdout="", stderr="", returncode=0)),
        ), patch("worker_harness.heartbeat.JobManager.start_job", new=AsyncMock()) as start_job:
            start_job.return_value.id = "job-copy"
            response = self.client.post("/api/v1/data/copy", json={
                "src_worker": "w-one",
                "src_path": "/data/imagenet",
                "dst_worker": "w-two",
                "dst_path": "/data/cache/imagenet",
            })

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["job_id"], "job-copy")
        self.assertIn("wh-data-export", ssh.await_args.args[1])
        command = start_job.await_args.args[1]
        self.assertIn("wh-data-import", command)
        self.assertNotIn("tailscale ssh", command)

    def test_copy_rejects_unadvertised_source(self):
        response = self.client.post("/api/v1/data/copy", json={
            "src_worker": "w-one", "src_path": "/data/not-advertised",
            "dst_worker": "w-two", "dst_path": "/data/cache/x",
        })
        self.assertEqual(response.status_code, 400)


class RegistrationIsolationTests(unittest.TestCase):
    def test_registration_app_has_no_control_routes(self):
        tmp = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
        tmp.close()
        db = Database(tmp.name)
        asyncio.run(db.connect())
        client = TestClient(create_registration_app(db))
        try:
            self.assertEqual(client.get("/api/v1/workers").status_code, 404)
            self.assertEqual(client.get("/health").status_code, 200)
        finally:
            client.close()
            asyncio.run(db.close())
            Path(tmp.name).unlink(missing_ok=True)


class DataUtilityTests(unittest.TestCase):
    def test_validate_data_path(self):
        self.assertEqual(validate_data_path("/data/imagenet"), "/data/imagenet")
        with self.assertRaises(DataPathError):
            validate_data_path("/data/../secret")
        with self.assertRaises(DataPathError):
            validate_data_path("relative/path")


if __name__ == "__main__":
    unittest.main()
