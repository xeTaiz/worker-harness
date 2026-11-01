import asyncio
import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from worker_harness.db import Database
from worker_harness.heartbeat import create_app
from worker_harness.models import GPUInfo, WorkerRegistration

from identity_support import bootstrap, login


class ReliabilityApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
        self.tmp.close()
        self.db = Database(self.tmp.name)
        asyncio.run(self.db.connect())
        asyncio.run(bootstrap(self.db))
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
            client.headers.update(login(client, "admin"))
            other_key = client.post("/api/v1/auth/keys", json={"label": "other"}).json()["token"]
            headers = {"X-Agent-Name": "cache-agent"}
            self.assertEqual(client.get("/api/v1/workers", headers=headers).status_code, 200)
            self.assertEqual(client.get("/api/v1/workers", headers=headers).status_code, 200)

            # A per-credential bucket permits its burst then responds 429.
            # Only SSH-backed reads and mutations consume tokens; polled
            # database reads never do.
            for _ in range(60):
                self.assertEqual(client.get("/api/v1/workers").status_code, 200)
            for _ in range(49):
                self.assertEqual(client.get("/api/v1/jobs/missing/logs").status_code, 404)
            limited = client.get("/api/v1/jobs/missing/logs")
            self.assertEqual(limited.status_code, 429)
            self.assertIn("Retry-After", limited.headers)
            # A caller-chosen agent name cannot mint a fresh bucket...
            evading = client.get("/api/v1/jobs/missing/logs", headers={"X-Agent-Name": "fresh-agent-123"})
            self.assertEqual(evading.status_code, 429)
            # ...while a different credential from the same peer is unaffected.
            other = TestClient(self.app).get(
                "/api/v1/jobs/missing/logs", headers={"Authorization": f"Bearer {other_key}"},
            )
            self.assertEqual(other.status_code, 404)

            # Pi bridge traffic is incarnation-scoped infrastructure, not an
            # operator/agent request: it never consumes the limited bucket.
            for _ in range(20):
                bridge = client.get(
                    "/api/v1/pi/bridge/missing/commands",
                    params={"incarnation": "inc", "wait_seconds": 0},
                )
                self.assertEqual(bridge.status_code, 404, bridge.text)

            stats = client.get("/api/v1/_stats")
            self.assertEqual(stats.status_code, 200)
            body = stats.json()
            self.assertGreaterEqual(body["cache"]["hits"], 1)
            self.assertGreaterEqual(body["cache"]["misses"], 1)
            self.assertIn("workers", body["lanes"])
            agents = body["rate_limit"]["agents"]
            self.assertNotIn("fresh-agent-123", agents)
            # Two credential buckets; the credential-less login request is keyed by peer.
            self.assertEqual(len([name for name in agents if name.startswith("credential:")]), 2)
            self.assertEqual({agent["capacity"] for agent in agents.values()}, {50})
            self.assertIn("tunnels", body)
            self.assertEqual(body["http"]["in_flight"], 1)  # this stats request itself

        # Lifespan shutdown clears lanes and stops the reaper task.
        self.assertEqual(self.app.state.lanes.stats(), {})
