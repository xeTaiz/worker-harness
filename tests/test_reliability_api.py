import asyncio
import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from worker_harness.db import Database
from worker_harness.heartbeat import create_app
from worker_harness.models import GPUInfo, WorkerRegistration


class ReliabilityApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
        self.tmp.close()
        self.db = Database(self.tmp.name)
        asyncio.run(self.db.connect())
        asyncio.run(self.db.upsert_worker(WorkerRegistration(
            worker_id="worker-a",
            name="worker-a",
            worker_ip="100.64.0.10",
            ssh_user="root",
            gpu_count=1,
            gpus=[GPUInfo(index=0, name="GPU0", vram_total_gb=24, vram_used_gb=0)],
            cpu_cores=8,
            total_ram_gb=64,
            used_ram_gb=8,
            total_disk_gb=500,
            used_disk_gb=100,
        )))
        self.app = create_app(self.db)

    def tearDown(self) -> None:
        asyncio.run(self.db.close())
        Path(self.tmp.name).unlink(missing_ok=True)
        Path(self.tmp.name + "-wal").unlink(missing_ok=True)
        Path(self.tmp.name + "-shm").unlink(missing_ok=True)

    def test_cache_rate_limit_stats_and_lifespan_cleanup(self):
        # Context manager starts/shuts down lifespan, exercising reaper/lane
        # cleanup as well as normal request handlers.
        with TestClient(self.app) as client:
            headers = {"X-Agent-Name": "cache-agent"}
            self.assertEqual(client.get("/api/v1/workers", headers=headers).status_code, 200)
            self.assertEqual(client.get("/api/v1/workers", headers=headers).status_code, 200)

            # A per-agent bucket permits its burst then responds 429, without
            # impacting another agent's bucket.
            limited_headers = {"X-Agent-Name": "limited-agent"}
            for _ in range(10):
                self.assertEqual(client.get("/api/v1/workers", headers=limited_headers).status_code, 200)
            limited = client.get("/api/v1/workers", headers=limited_headers)
            self.assertEqual(limited.status_code, 429)
            self.assertIn("Retry-After", limited.headers)

            stats = client.get("/api/v1/_stats", headers={"X-Agent-Name": "operator"})
            self.assertEqual(stats.status_code, 200)
            body = stats.json()
            self.assertGreaterEqual(body["cache"]["hits"], 1)
            self.assertGreaterEqual(body["cache"]["misses"], 1)
            self.assertIn("workers", body["lanes"])
            self.assertIn("agents", body["rate_limit"])
            self.assertIn("tunnels", body)
            self.assertEqual(body["http"]["in_flight"], 1)  # this stats request itself

        # Lifespan shutdown clears lanes and stops the reaper task.
        self.assertEqual(self.app.state.lanes.stats(), {})
