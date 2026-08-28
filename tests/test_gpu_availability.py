from __future__ import annotations

import asyncio
import importlib.util
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from worker_harness.db import Database
from worker_harness.models import GPUInfo, WorkerRegistration


DAEMON_PATH = Path(__file__).parents[1] / "worker_container" / "worker_daemon.py"


def load_daemon_module():
    spec = importlib.util.spec_from_file_location("worker_daemon_for_gpu_test", DAEMON_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class GPUAvailabilityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.daemon = load_daemon_module()

    def test_ignores_small_and_display_processes_but_requires_free_vram(self):
        gpu_output = "\n".join((
            "0, GPU-a, RTX 6000 Ada, 49140, 1024",
            "1, GPU-b, RTX 6000 Ada, 49140, 700",
            "2, GPU-c, V100, 16384, 5000",
            "3, GPU-d, V100, 16384, 2500",
        ))
        process_output = "\n".join((
            "GPU-a, python, 1024",
            "GPU-b, python, 200",
            "GPU-d, /usr/lib/Xorg, 2048",
        ))

        def nvidia_smi(args, **_kwargs):
            if any(arg.startswith("--query-gpu=") for arg in args):
                return gpu_output.encode()
            if any(arg.startswith("--query-compute-apps=") for arg in args):
                return process_output.encode()
            raise subprocess.CalledProcessError(1, args)

        with patch.object(self.daemon.subprocess, "check_output", side_effect=nvidia_smi):
            result = self.daemon.get_gpu_info()

        self.assertEqual([gpu["busy"] for gpu in result["gpus"]], [True, False, True, False])

    def test_persists_reported_busy_state(self):
        async def check() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                db = Database(Path(tmp) / "worker-harness.db")
                await db.connect()
                try:
                    await db.upsert_worker(WorkerRegistration(
                        worker_id="worker-1",
                        name="worker-1",
                        worker_ip="100.64.0.1",
                        gpu_count=2,
                        gpus=[
                            GPUInfo(index=0, name="V100", vram_total_gb=16, vram_used_gb=1, busy=False),
                            GPUInfo(index=1, name="V100", vram_total_gb=16, vram_used_gb=12, busy=True),
                        ],
                    ))
                    worker = await db.get_worker("worker-1")
                    self.assertIsNotNone(worker)
                    self.assertEqual(worker.gpu_busy, [False, True])
                finally:
                    await db.close()

        asyncio.run(check())


if __name__ == "__main__":
    unittest.main()
