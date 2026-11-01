"""WH_TAILSCALE_SOCKET selects which local tailscaled the orchestrator uses.

A fake `tailscale` executable stands in for a host running two daemons: the
default socket belongs to one tailnet, `--socket=<headscale>` to another.
Worker commands and the Tailnet bind address must come from the configured
daemon, never silently from the default one.
"""
from __future__ import annotations

import os
import tempfile
import textwrap
import unittest
from pathlib import Path
from unittest.mock import patch

from worker_harness.marimo import tailnet_bind_host
from worker_harness.models import Worker
from worker_harness.ssh import async_ssh_run

FAKE_TAILSCALE = textwrap.dedent(
    """\
    #!/bin/bash
    daemon=default ip=100.93.180.99
    if [ "$1" = "--socket=$HEADSCALE_SOCKET" ]; then
      daemon=headscale ip=100.64.0.17
      shift
    elif [[ "$1" == --socket=* ]]; then
      echo "unknown socket $1" >&2
      exit 1
    fi
    case "$1" in
      ip) echo "$ip" ;;
      ssh) shift 2; TS_DAEMON=$daemon exec bash -c "$1" ;;
      *) exit 2 ;;
    esac
    """
)


class TailscaleSocketTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        fake = root / "tailscale"
        fake.write_text(FAKE_TAILSCALE)
        fake.chmod(0o755)
        self.socket = str(root / "headscale.sock")
        self.env = patch.dict(os.environ, {
            "PATH": f"{root}:{os.environ['PATH']}",
            "HEADSCALE_SOCKET": self.socket,
        })
        self.env.start()
        os.environ.pop("WH_TAILSCALE_SOCKET", None)
        os.environ.pop("WH_TAILNET_BIND_HOST", None)
        self.worker = Worker(id="w", name="w", worker_ip="100.64.0.4", ssh_user="dominik")

    def tearDown(self):
        self.env.stop()
        self.tmp.cleanup()

    async def test_configured_socket_routes_worker_ssh_and_bind_address(self):
        os.environ["WH_TAILSCALE_SOCKET"] = self.socket
        result = await async_ssh_run(self.worker, "echo $TS_DAEMON")
        self.assertEqual((result.returncode, result.stdout.strip()), (0, "headscale"))
        self.assertEqual(tailnet_bind_host(), "100.64.0.17")

    async def test_unset_socket_uses_cli_default_daemon(self):
        result = await async_ssh_run(self.worker, "echo $TS_DAEMON")
        self.assertEqual((result.returncode, result.stdout.strip()), (0, "default"))
        self.assertEqual(tailnet_bind_host(), "100.93.180.99")


if __name__ == "__main__":
    unittest.main()
