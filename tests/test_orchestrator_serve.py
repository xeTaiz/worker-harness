"""`serve` must exit when one of its HTTP servers cannot run."""
from __future__ import annotations

import asyncio
import os
import socket
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from worker_harness.config import Config, ControlConfig, HeartbeatConfig
from worker_harness.orchestrator import serve


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class ServeFailureTests(unittest.IsolatedAsyncioTestCase):
    async def test_invalid_control_configuration_stops_the_process(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = Config(
                db_path=Path(tmp) / "db.sqlite",
                heartbeat=HeartbeatConfig(host="127.0.0.1", port=free_port()),
                control=ControlConfig(host="127.0.0.1", port=free_port()),
            )
            with patch.dict(os.environ, {"WH_TRUSTED_PROXIES": "172.31.250.11/32,PROXY_SUBNET"}):
                with self.assertRaisesRegex(RuntimeError, "control server stopped") as raised:
                    await asyncio.wait_for(serve(config), timeout=20)
            self.assertIn("WH_TRUSTED_PROXIES", str(raised.exception.__cause__))


if __name__ == "__main__":
    unittest.main()
