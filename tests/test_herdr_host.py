import asyncio
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from worker_harness import herdr_host, ssh
from worker_harness.lanes import WorkerLanes
from worker_harness.machines import Machine
from worker_harness.ssh import SSHResult


SHIM = Path(__file__).resolve().parents[1] / "scripts" / "wh-remote-shim"


def server_error(code, returncode=1):
    return SSHResult("", json.dumps({"error": {"code": code, "message": code}}), returncode)


class HerdrTransportTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.machine = Machine(name="test", ssh_target="user@test", home="/home/test")

    async def test_raw_stdout_preserved_and_text_callers_still_replace_invalid_utf8(self):
        command = [sys.executable, "-c", "import os; os.write(1, b'\\xe2\\x82\\xff'); os.write(2, b'error\\xff')"]
        with patch.object(ssh, "_machine_base_args", return_value=command), \
                patch.object(ssh, "_lanes", WorkerLanes(max_concurrent=1, max_queue=1)):
            raw, metadata = await ssh.async_machine_run_bytes(self.machine, "ignored")
            text = await ssh.async_machine_run(self.machine, "ignored")
        self.assertEqual(raw, b"\xe2\x82\xff")
        self.assertEqual(text.stdout, raw.decode(errors="replace"))
        self.assertEqual(metadata.stderr, "error\ufffd")
        self.assertEqual(text.stderr, metadata.stderr)
        self.assertEqual(metadata.returncode, 0)

    async def test_transcript_offsets_survive_split_and_invalid_utf8(self):
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp)
            transcript = home / "transcript space.jsonl"
            transcript.write_bytes(b"A\xe2\x82")
            machine = Machine(name="test", ssh_target="user@test", home=tmp)
            launcher = [
                sys.executable, "-c",
                "import os, sys; os.environ['SSH_ORIGINAL_COMMAND'] = sys.argv[1]; "
                f"os.execv('/bin/bash', ['bash', {str(SHIM)!r}])",
            ]
            with patch.object(ssh, "_machine_base_args", return_value=launcher), \
                    patch.object(ssh, "_lanes", WorkerLanes(max_concurrent=1, max_queue=1)), \
                    patch.dict(os.environ, {"HOME": tmp}):
                first, offset = await herdr_host.tail_transcript(machine, str(transcript), 1)
                with transcript.open("ab") as stream:
                    stream.write(b"\xac\xff\n")
                second, next_offset = await herdr_host.tail_transcript(machine, str(transcript), offset)
                final, final_offset = await herdr_host.tail_transcript(machine, str(transcript), next_offset)
            self.assertEqual(first, b"A\xe2\x82")
            self.assertEqual(offset, 4)
            self.assertEqual(second, b"\xac\xff\n")
            self.assertEqual(first + second, transcript.read_bytes())
            self.assertEqual(next_offset, 7)
            self.assertEqual((final, final_offset), (b"", 7))

    async def test_failed_tail_does_not_advance_offset(self):
        with patch.object(herdr_host, "async_machine_run_bytes", new=AsyncMock(
            return_value=(b"partial", SSHResult("", "failed", 1)),
        )):
            self.assertEqual(await herdr_host.tail_transcript(self.machine, "/home/test/log", 9), (b"", 9))

    async def test_cold_start_discriminates_structured_error_not_exit_code(self):
        responses = [
            server_error("server_not_running", returncode=2),
            SSHResult("", "", 0),  # systemctl start
            SSHResult('{"result": {}}', "", 0),  # readiness
            SSHResult('{"result": {"pane_id": "p1"}}', "", 0),
        ]
        with patch.object(herdr_host, "async_machine_run", new=AsyncMock(side_effect=responses)), \
                patch.object(herdr_host, "_COLD_START_POLL_SECONDS", 0):
            result = await herdr_host.herdr(self.machine, "workspace", "create")
        self.assertEqual(result, {"pane_id": "p1"})

    async def test_cold_start_reports_failed_systemctl_without_polling(self):
        responses = [server_error("server_not_running"), SSHResult("", "unit failed", 1)]
        with patch.object(herdr_host, "async_machine_run", new=AsyncMock(side_effect=responses)):
            with self.assertRaises(herdr_host.HerdrUnavailable) as raised:
                await herdr_host.herdr(self.machine, "workspace", "create")
        self.assertIsInstance(raised.exception.__cause__, herdr_host.ShimError)
        self.assertEqual(raised.exception.__cause__.stderr, "unit failed")

    async def test_readiness_error_is_not_hidden_by_retrying_original_command(self):
        responses = [
            server_error("server_not_running"), SSHResult("", "", 0),
            server_error("permission_denied"),
        ]
        with patch.object(herdr_host, "async_machine_run", new=AsyncMock(side_effect=responses)), \
                patch.object(herdr_host, "_COLD_START_POLL_SECONDS", 0):
            with self.assertRaises(herdr_host.HerdrError) as raised:
                await herdr_host.herdr(self.machine, "workspace", "create")
        self.assertEqual(raised.exception.code, "permission_denied")

    async def test_readiness_deadline_cancels_a_stalled_probe(self):
        probe_cancelled = asyncio.Event()

        async def run(machine, command, **kwargs):
            if command == "herdr-up":
                return SSHResult("", "", 0)
            if command == "herdr api snapshot":
                try:
                    await asyncio.sleep(60)
                finally:
                    probe_cancelled.set()
            return server_error("server_not_running")

        with patch.object(herdr_host, "async_machine_run", new=run), \
                patch.object(herdr_host, "_COLD_START_POLL_SECONDS", 0), \
                patch.object(herdr_host, "_COLD_START_TIMEOUT_SECONDS", 0.02):
            with self.assertRaises(herdr_host.HerdrUnavailable):
                await herdr_host.herdr(self.machine, "workspace", "create")
        self.assertTrue(probe_cancelled.is_set())

    async def test_remove_file_does_not_hide_cleanup_failure(self):
        with patch.object(herdr_host, "async_machine_run", new=AsyncMock(
            return_value=SSHResult("", "permission denied", 1),
        )):
            with self.assertRaises(herdr_host.ShimError):
                await herdr_host.remove_file(self.machine, "/home/test/.cache/worker-harness/session")
