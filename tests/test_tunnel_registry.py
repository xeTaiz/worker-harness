import os
import subprocess
import time
import unittest

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

    def test_reap_dead_removes_stale_registry_entry(self):
        registry = TunnelRegistry()
        entry = self._entry("tunnel-2")
        registry.add(entry)
        os.killpg(entry.proc.pid, 15)
        entry.proc.wait(timeout=1)
        self.assertEqual(registry.reap_dead(), 1)
        self.assertEqual(registry.stats()["live"], 0)
