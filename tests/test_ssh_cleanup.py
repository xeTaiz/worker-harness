import asyncio
import os
import stat
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from worker_harness.lanes import WorkerLanes
from worker_harness.models import GPUInfo, Worker, WorkerRegistration
from worker_harness.ssh import SSHResult, async_ssh_run, set_lanes, ssh_upload_bytes


class SshCleanupTests(unittest.TestCase):
    def _worker(self) -> Worker:
        return Worker.from_registration(WorkerRegistration(
            worker_id="fake-worker",
            name="fake-worker",
            worker_ip="100.64.0.99",
            ssh_user="root",
            gpu_count=0,
            gpus=[],
            cpu_cores=1,
            total_ram_gb=1,
            used_ram_gb=0,
            total_disk_gb=1,
            used_disk_gb=0,
        ))

    def _assert_pid_gone(self, pid: int) -> None:
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline:
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                return
            time.sleep(0.02)
        self.fail(f"pid {pid} still exists after process-group cleanup")

    def test_upload_uses_one_quoted_remote_shell_command(self):
        async def run():
            worker = self._worker()
            result = SSHResult(stdout="", stderr="", returncode=0)
            with patch("worker_harness.ssh._exec_ssh", new=AsyncMock(return_value=result)) as exec_mock:
                received = await ssh_upload_bytes(worker, b"payload", "/tmp/with space.txt")

            self.assertIs(received, result)
            args = exec_mock.await_args.args[1]
            self.assertEqual(args[:3], ["tailscale", "ssh", "root@100.64.0.99"])
            # Tailscale SSH must receive exactly one remote command; passing
            # `sh`, `-lc`, command as separate argv broke live upload_file.
            self.assertEqual(len(args), 4)
            self.assertTrue(args[-1].startswith("sh -lc "))
            self.assertIn("mkdir -p /tmp", args[-1])
            self.assertIn("with space.txt", args[-1])
            self.assertEqual(exec_mock.await_args.kwargs["input_data"], b"payload")

        asyncio.run(run())

    def test_timeout_kills_complete_ssh_process_group(self):
        async def run():
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                parent_file = root / "parent.pid"
                child_file = root / "child.pid"
                fake_tailscale = root / "tailscale"
                fake_tailscale.write_text(
                    "#!/bin/sh\n"
                    "echo $$ > \"$FAKE_PARENT_PID\"\n"
                    "sleep 30 &\n"
                    "echo $! > \"$FAKE_CHILD_PID\"\n"
                    "wait\n"
                )
                fake_tailscale.chmod(fake_tailscale.stat().st_mode | stat.S_IXUSR)
                env = {
                    "PATH": f"{root}:{os.environ['PATH']}",
                    "FAKE_PARENT_PID": str(parent_file),
                    "FAKE_CHILD_PID": str(child_file),
                }
                set_lanes(WorkerLanes(max_concurrent=1, max_queue=1))
                with patch.dict(os.environ, env, clear=False):
                    result = await async_ssh_run(self._worker(), "ignored", timeout=0.1)

                self.assertEqual(result.returncode, -1)
                self.assertIn("timed out", result.stderr)
                self.assertTrue(parent_file.exists())
                self.assertTrue(child_file.exists())
                self._assert_pid_gone(int(parent_file.read_text().strip()))
                self._assert_pid_gone(int(child_file.read_text().strip()))

        asyncio.run(run())

    def test_cancellation_kills_complete_ssh_process_group(self):
        async def run():
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                child_file = root / "child.pid"
                fake_tailscale = root / "tailscale"
                fake_tailscale.write_text(
                    "#!/bin/sh\n"
                    "sleep 30 &\n"
                    "echo $! > \"$FAKE_CHILD_PID\"\n"
                    "wait\n"
                )
                fake_tailscale.chmod(fake_tailscale.stat().st_mode | stat.S_IXUSR)
                set_lanes(WorkerLanes(max_concurrent=1, max_queue=1))
                with patch.dict(os.environ, {
                    "PATH": f"{root}:{os.environ['PATH']}",
                    "FAKE_CHILD_PID": str(child_file),
                }, clear=False):
                    task = asyncio.create_task(async_ssh_run(self._worker(), "ignored", timeout=30))
                    deadline = time.monotonic() + 1
                    while not child_file.exists() and time.monotonic() < deadline:
                        await asyncio.sleep(0.01)
                    self.assertTrue(child_file.exists())
                    task.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await task

                self._assert_pid_gone(int(child_file.read_text().strip()))

        asyncio.run(run())
