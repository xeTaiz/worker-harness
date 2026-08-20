import asyncio
import os
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from worker_harness.db import Database
from worker_harness.heartbeat import create_app
from worker_harness.models import WorkerRegistration
from worker_harness.tunnel_registry import TunnelProcess, TunnelRegistry


class TunnelRegistryTests(unittest.TestCase):
    def _entry(self, tunnel_id: str) -> TunnelProcess:
        proc = subprocess.Popen(["sleep", "30"], start_new_session=True)
        return TunnelProcess(
            id=tunnel_id,
            worker_id="worker-a",
            local_port=18000,
            remote_port=8000,
            proc=proc,
            created_at=int(time.time()),
        )

    def test_shutdown_kills_complete_tunnel_process_group(self):
        registry = TunnelRegistry()
        entry = self._entry("tunnel-1")
        registry.add(entry)
        self.assertEqual(registry.stats()["live"], 1)
        self.assertEqual(registry.shutdown(grace_seconds=0.1), 1)
        self.assertIsNotNone(entry.proc.poll())
        self.assertEqual(registry.stats()["live"], 0)

    def test_stop_kills_descendant_after_tunnel_leader_exits(self):
        with tempfile.TemporaryDirectory() as tmp:
            child_file = Path(tmp) / "child.pid"
            proc = subprocess.Popen(
                ["sh", "-c", f"sleep 30 & echo $! > {child_file}; exit 0"],
                start_new_session=True,
            )
            proc.wait(timeout=1)
            child_pid = int(child_file.read_text().strip())
            entry = TunnelProcess(
                id="tunnel-orphan",
                worker_id="worker-a",
                local_port=18001,
                remote_port=8001,
                proc=proc,
                created_at=int(time.time()),
            )

            self.assertTrue(TunnelRegistry.stop(entry, grace_seconds=0.1))
            with self.assertRaises(ProcessLookupError):
                os.kill(child_pid, 0)

    def test_reap_dead_removes_stale_registry_entry(self):
        registry = TunnelRegistry()
        entry = self._entry("tunnel-2")
        registry.add(entry)
        os.killpg(entry.proc.pid, 15)
        entry.proc.wait(timeout=1)
        self.assertEqual(registry.reap_dead(), 1)
        self.assertEqual(registry.stats()["live"], 0)


class TunnelApiCleanupTests(unittest.TestCase):
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
            gpu_count=0,
            gpus=[],
        )))
        self.client = TestClient(create_app(self.db), raise_server_exceptions=False)

    def tearDown(self):
        self.client.close()
        asyncio.run(self.db.close())
        Path(self.tmp.name).unlink(missing_ok=True)

    def test_failed_tunnel_insert_terminates_unregistered_process(self):
        proc = subprocess.Popen(["sleep", "30"], start_new_session=True)
        try:
            with patch(
                "worker_harness.heartbeat.ssh_port_forward",
                new=AsyncMock(return_value=proc),
            ), patch.object(
                self.db,
                "insert_port_forward",
                new=AsyncMock(side_effect=RuntimeError("database unavailable")),
            ):
                response = self.client.post("/api/v1/tunnels", json={
                    "worker_id": "worker-test",
                    "local_port": 18002,
                    "remote_port": 8002,
                })

            self.assertEqual(response.status_code, 500)
            self.assertIsNotNone(proc.poll())
            self.assertEqual(asyncio.run(self.db.list_port_forwards()), [])
        finally:
            if proc.poll() is None:
                os.killpg(proc.pid, 9)
                proc.wait(timeout=1)
