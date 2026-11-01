"""Tests for bind-path discovery and rsync copy orchestration."""

import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from worker_harness.data import DataPathError, is_advertised_data_path, reverse_data_paths, validate_data_path
from worker_harness.db import Database
from worker_harness.heartbeat import create_app, create_registration_app
from worker_harness.models import Job, JobStatus, WorkerRegistration, WorkerStatus
from worker_harness.ssh import SSHResult

from identity_support import bearer, bootstrap, issue_key, login


class DataApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
        self.tmp.close()
        self.db = Database(self.tmp.name)
        asyncio.run(self.db.connect())
        self.owner = asyncio.run(bootstrap(self.db))
        # The endpoint's detached export-cleanup monitor must never probe SSH.
        refresh = patch(
            "worker_harness.heartbeat.JobManager.refresh_job_status",
            new=AsyncMock(return_value=Job(
                id="completed-copy", worker_id="w-two", status=JobStatus.DONE,
                owner_user_id=self.owner["id"],
                submitted_by_actor_id=f"migration:{self.owner['id']}",
            )),
        )
        refresh.start()
        self.addCleanup(refresh.stop)
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
        self.client.headers.update(login(self.client, "admin"))

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

    def test_api_key_can_discover_shared_roots_but_cannot_start_data_copy(self):
        headers = bearer(issue_key(self.client, dict(self.client.headers)))
        response = self.client.get("/api/v1/data", headers=headers)
        self.assertEqual(response.status_code, 200, response.text)
        self.assertIn("/data/imagenet", response.json())
        with patch("worker_harness.heartbeat.async_ssh_run", new=AsyncMock()) as ssh:
            response = self.client.post("/api/v1/data/copy", headers=headers, json={
                "src_worker": "w-one", "src_path": "/data/imagenet",
                "dst_worker": "w-two", "dst_path": "/data/cache/imagenet",
            })
        self.assertEqual(response.status_code, 403, response.text)
        ssh.assert_not_awaited()

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
        self.assertTrue(any("wh-data-export" in call.args[1] for call in ssh.await_args_list))
        command = start_job.await_args.args[1]
        self.assertIn("wh-data-import", command)
        self.assertIn("--host worker-one.tailnet", command)
        self.assertNotIn("tailscale ssh", command)
        self.assertEqual(start_job.await_args.kwargs["owner_user_id"], self.owner["id"])
        self.assertTrue(start_job.await_args.kwargs["actor_id"].startswith("web_session:"))

    def test_copy_accepts_child_of_advertised_bind_root(self):
        with patch(
            "worker_harness.heartbeat.async_ssh_run",
            new=AsyncMock(return_value=SSHResult(
                stdout=json.dumps({"port": 22003, "username": "transfer", "password": "secret"}) + "\n",
                stderr="", returncode=0,
            ))), patch(
                "worker_harness.heartbeat.ssh_upload_bytes",
                new=AsyncMock(return_value=SSHResult(stdout="", stderr="", returncode=0)),
            ), patch("worker_harness.heartbeat.JobManager.start_job", new=AsyncMock()) as start_job:
            start_job.return_value.id = "job-child-copy"
            response = self.client.post("/api/v1/data/copy", json={
                "src_worker": "w-one", "src_path": "/data/imagenet/train",
                "dst_worker": "w-two", "dst_path": "/data/cache/train",
            })
        self.assertEqual(response.status_code, 200, response.text)

    def test_copy_rejects_source_outside_advertised_bind_roots(self):
        response = self.client.post("/api/v1/data/copy", json={
            "src_worker": "w-one", "src_path": "/share/not-advertised",
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
        self.assertTrue(is_advertised_data_path("/data/imagenet/train", ["/data/imagenet"]))
        self.assertFalse(is_advertised_data_path("/data", ["/data/imagenet"]))
        self.assertFalse(is_advertised_data_path("/data-other/train", ["/data/imagenet"]))
        with self.assertRaises(DataPathError):
            validate_data_path("/data/../secret")
        with self.assertRaises(DataPathError):
            validate_data_path("relative/path")


if __name__ == "__main__":
    unittest.main()
