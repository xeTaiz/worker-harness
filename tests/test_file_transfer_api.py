"""Test file transfer API endpoints (upload/download via orchestrator).

Tests the HTTP API layer with mocked SSH functions so they run without
a real worker or Tailscale network.
"""

import asyncio
import base64
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from worker_harness.db import Database
from worker_harness.heartbeat import create_app
from worker_harness.models import GPUInfo, JobStatus, WorkerRegistration


class FileTransferApiTests(unittest.TestCase):
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

    # ── Upload tests ──────────────────────────────────────────────────

    def test_upload_small_file_succeeds(self):
        """Valid base64 content uploads to a known worker."""
        content = b"#!/bin/bash\necho hello\n"
        payload = {
            "path": "/var/lib/worker-harness/harness/test.sh",
            "content_b64": base64.b64encode(content).decode(),
        }

        with patch(
            "worker_harness.heartbeat.ssh_upload_bytes",
            new=AsyncMock(return_value=type("R", (), {"returncode": 0, "stderr": ""})()),
        ) as mock_upload:
            resp = self.client.post("/api/v1/workers/w-test/files", json=payload)

        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["worker_id"], "w-test")
        self.assertEqual(body["path"], payload["path"])
        self.assertEqual(body["size"], len(content))

        # Verify the SSH function was called with the right args
        mock_upload.assert_awaited_once()
        call_args = mock_upload.call_args
        self.assertEqual(call_args.args[2], payload["path"])  # remote_path
        self.assertEqual(call_args.args[1], content)  # content bytes

    def test_upload_rejects_invalid_base64(self):
        """Malformed base64 returns 400, not 502."""
        payload = {"path": "/tmp/test.sh", "content_b64": "not!!valid!!base64!!!"}
        resp = self.client.post("/api/v1/workers/w-test/files", json=payload)
        self.assertEqual(resp.status_code, 400)
        self.assertIn("base64", resp.json()["detail"].lower())

    def test_upload_rejects_oversized_file(self):
        """Files exceeding the 10MB cap return 413."""
        # 11 MB of zeros
        big_content = b"\x00" * (10 * 1024 * 1024 + 1)
        payload = {
            "path": "/tmp/big.bin",
            "content_b64": base64.b64encode(big_content).decode(),
        }
        resp = self.client.post("/api/v1/workers/w-test/files", json=payload)
        self.assertEqual(resp.status_code, 413)
        self.assertIn("rsync", resp.json()["detail"].lower())

    def test_upload_unknown_worker_returns_404(self):
        payload = {"path": "/tmp/test.sh", "content_b64": base64.b64encode(b"hi").decode()}
        resp = self.client.post("/api/v1/workers/nonexistent/files", json=payload)
        self.assertEqual(resp.status_code, 404)

    def test_upload_ssh_failure_returns_502(self):
        """When SSH upload fails, API returns 502 with stderr."""
        from worker_harness.ssh import SSHResult

        payload = {
            "path": "/tmp/test.sh",
            "content_b64": base64.b64encode(b"hi").decode(),
        }
        with patch(
            "worker_harness.heartbeat.ssh_upload_bytes",
            new=AsyncMock(return_value=SSHResult(stdout="", stderr="Permission denied", returncode=1)),
        ):
            resp = self.client.post("/api/v1/workers/w-test/files", json=payload)

        self.assertEqual(resp.status_code, 502)
        self.assertIn("Permission denied", resp.json()["detail"])

    # ── Download tests ────────────────────────────────────────────────

    def test_download_file_succeeds(self):
        """Valid download returns base64-encoded content."""
        content = b"line1\nline2\nline3\n"
        from worker_harness.ssh import SSHResult

        with patch(
            "worker_harness.heartbeat.ssh_download_bytes",
            new=AsyncMock(return_value=(content, SSHResult(stdout="", stderr="", returncode=0))),
        ):
            resp = self.client.get(
                "/api/v1/workers/w-test/files",
                params={"path": "/var/lib/worker-harness/harness/output.log"},
            )

        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["size"], len(content))
        self.assertEqual(base64.b64decode(body["content_b64"]), content)

    def test_download_unknown_worker_returns_404(self):
        resp = self.client.get(
            "/api/v1/workers/nonexistent/files",
            params={"path": "/tmp/test.sh"},
        )
        self.assertEqual(resp.status_code, 404)

    def test_download_ssh_failure_returns_502(self):
        from worker_harness.ssh import SSHResult

        with patch(
            "worker_harness.heartbeat.ssh_download_bytes",
            new=AsyncMock(return_value=(b"", SSHResult(stdout="", stderr="No such file", returncode=1))),
        ):
            resp = self.client.get(
                "/api/v1/workers/w-test/files",
                params={"path": "/nonexistent/file"},
            )

        self.assertEqual(resp.status_code, 502)
        self.assertIn("No such file", resp.json()["detail"])

    def test_download_missing_path_param_returns_422(self):
        """path is a required query parameter."""
        resp = self.client.get("/api/v1/workers/w-test/files")
        self.assertEqual(resp.status_code, 422)  # FastAPI validation error

    # ── Round-trip test ───────────────────────────────────────────────

    def test_upload_then_download_roundtrip(self):
        """Upload content and download it back (mocked SSH preserves bytes)."""
        original = b"import torch\nprint('hello world')\n# some config\n"
        from worker_harness.ssh import SSHResult

        stored_content: list[bytes] = []

        async def mock_upload(worker, content, remote_path, timeout=60):
            stored_content.append(content)
            return SSHResult(stdout="", stderr="", returncode=0)

        async def mock_download(worker, remote_path, max_bytes=10485760, timeout=30):
            return (stored_content[-1] if stored_content else b"", SSHResult(stdout="", stderr="", returncode=0))

        with patch("worker_harness.heartbeat.ssh_upload_bytes", new=mock_upload), \
             patch("worker_harness.heartbeat.ssh_download_bytes", new=mock_download):

            # Upload
            upload_resp = self.client.post(
                "/api/v1/workers/w-test/files",
                json={
                    "path": "/var/lib/worker-harness/harness/config.py",
                    "content_b64": base64.b64encode(original).decode(),
                },
            )
            self.assertEqual(upload_resp.status_code, 200)

            # Download
            download_resp = self.client.get(
                "/api/v1/workers/w-test/files",
                params={"path": "/var/lib/worker-harness/harness/config.py"},
            )
            self.assertEqual(download_resp.status_code, 200)

            downloaded = base64.b64decode(download_resp.json()["content_b64"])
            self.assertEqual(downloaded, original)


if __name__ == "__main__":
    unittest.main()
