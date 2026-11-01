"""Real local processes exercise supervision and the remote shell transport."""
import asyncio
import ctypes
import json
import os
from pathlib import Path
import shlex
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from worker_harness import job_runner
from worker_harness.models import Worker, WorkerRegistration
from worker_harness.ssh import (
    _build_job_command, _job_probe_command, _job_stop_command,
    SSHResult, ssh_job_launch, ssh_job_probe, ssh_job_stop, ssh_read_log,
)


class JobRunnerTests(unittest.TestCase):
    def setUp(self):
        # Short paths also keep the Unix-domain control socket below Linux's limit.
        self.temporary = tempfile.TemporaryDirectory(prefix="wh-run-", dir="/tmp")
        self.root = Path(self.temporary.name)
        self.processes = []
        self.directories = []
        self.worker = Worker.from_registration(WorkerRegistration(
            worker_id="runner-local", name="runner-local", worker_ip="127.0.0.1",
            ssh_user="local", gpu_count=0, gpus=[], cpu_cores=1,
            total_ram_gb=1, used_ram_gb=0, total_disk_gb=1, used_disk_gb=0,
            harness_dir=str(self.root / "harness"),
        ))

    def tearDown(self):
        for directory in self.directories:
            if (directory / "control.sock").exists():
                self._stop(directory)
        for process in self.processes:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=2)
            if process.stdout is not None:
                process.stdout.close()
        subprocess.run(
            ["tmux", "kill-server"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            env={**os.environ, "TMUX_TMPDIR": str(self.root / "harness" / "job-tmux"), "TMUX": ""},
            timeout=3,
        )
        self.temporary.cleanup()

    def _wait(self, predicate, timeout=5):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = predicate()
            if value:
                return value
            time.sleep(0.02)
        logs = [
            (directory / "output.log").read_text()
            for directory in self.directories if (directory / "output.log").exists()
        ]
        self.fail(f"Timed out waiting for runner: {logs}")

    def _prepare(self, command, gpu_indices=""):
        directory = self.root / f"job-{len(self.directories)}"
        directory.mkdir(mode=0o700)
        (directory / "runner.py").write_bytes(Path(job_runner.__file__).read_bytes())
        (directory / "script.sh").write_text("#!/bin/bash\n" + command + "\n")
        (directory / "gpu.env").write_text(f"CUDA_VISIBLE_DEVICES={gpu_indices}\n")
        for file in directory.iterdir():
            file.chmod(0o600)
        self.directories.append(directory)
        return directory

    def _start(self, directory):
        process = subprocess.Popen(
            [sys.executable, str(directory / "runner.py"), "run", "--job-dir", str(directory)],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        self.processes.append(process)
        self._wait(lambda: (directory / "runtime.json").exists())
        return process

    def _stop(self, directory, reason="user_cancelled"):
        return subprocess.run(
            [sys.executable, str(directory / "runner.py"), "stop", "--job-dir", str(directory),
             "--reason", reason], capture_output=True, text=True, timeout=12,
        )

    def _result(self, directory):
        self._wait(lambda: (directory / "result.json").exists())
        return json.loads((directory / "result.json").read_text())

    def _pid(self, path):
        self._wait(path.exists)
        return int(path.read_text())

    def _assert_gone(self, pid):
        def absent():
            try:
                os.kill(pid, 0)
                return False
            except ProcessLookupError:
                return True
        self._wait(absent)

    def _shell(self, command):
        return subprocess.run(["bash", "-c", command], capture_output=True, text=True, timeout=15)

    def _probe(self, job_id):
        process = self._shell(_job_probe_command(self.worker, job_id))
        self.assertEqual(process.returncode, 0, process.stderr)
        return json.loads(process.stdout)

    def test_foreground_exit_status_gpu_environment_and_permissions(self):
        directory = self._prepare('printf "gpu=%s order=%s\\n" "$CUDA_VISIBLE_DEVICES" "$CUDA_DEVICE_ORDER"; exit 7', "1,3")
        process = self._start(directory)
        result = self._result(directory)
        self.assertEqual(result["exit_code"], 7)
        self.assertTrue(result["descendants_exited"])
        self.assertIsNone(result["termination_reason"])
        self.assertIn("gpu=1,3 order=PCI_BUS_ID", (directory / "output.log").read_text())
        self.assertEqual(process.wait(timeout=2), 0)
        runtime = json.loads((directory / "runtime.json").read_text())
        self.assertEqual(runtime["job_id"], directory.name)
        self.assertEqual(runtime["runner_pid"], process.pid)
        self.assertGreater(runtime["start_ticks"], 0)
        self.assertEqual(directory.stat().st_mode & 0o777, 0o700)
        for name in ("runtime.json", "result.json", "output.log"):
            self.assertEqual((directory / name).stat().st_mode & 0o777, 0o600)

    def test_background_child_outlives_shell(self):
        pid_file = self.root / "background.pid"
        directory = self._prepare(f"sleep 0.8 & echo $! > {shlex.quote(str(pid_file))}; exit 4")
        process = self._start(directory)
        child = self._pid(pid_file)
        time.sleep(0.15)
        self.assertIsNone(process.poll())
        self.assertFalse((directory / "result.json").exists())
        self.assertEqual(self._result(directory)["exit_code"], 4)
        self._assert_gone(child)

    def test_detached_setsid_descendant_is_reaped(self):
        pid_file = self.root / "detached.pid"
        child_script = f"echo $$ > {shlex.quote(str(pid_file))}; sleep 0.8"
        directory = self._prepare(f"setsid bash -c {shlex.quote(child_script)} & exit 0")
        process = self._start(directory)
        child = self._pid(pid_file)
        time.sleep(0.15)
        self.assertFalse((directory / "result.json").exists())
        self.assertIsNone(process.poll())
        self.assertEqual(self._result(directory)["exit_code"], 0)
        self._assert_gone(child)

    def test_term_ignoring_workload_is_killed_and_reaped(self):
        pid_file = self.root / "ignore.pid"
        directory = self._prepare(
            f"trap '' TERM; echo $$ > {shlex.quote(str(pid_file))}; while :; do sleep 0.1; done"
        )
        self._start(directory)
        child = self._pid(pid_file)
        began = time.monotonic()
        stopped = self._stop(directory, "gpu_idle_timeout")
        self.assertEqual(stopped.returncode, 0, stopped.stderr)
        self.assertGreaterEqual(time.monotonic() - began, 1.8)
        result = self._result(directory)
        self.assertEqual(result["termination_reason"], "gpu_idle_timeout")
        self.assertEqual(result["exit_code"], 137)
        self._assert_gone(child)

    def test_nested_descendants_are_all_stopped(self):
        root_pid = self.root / "root.pid"
        middle_pid = self.root / "middle.pid"
        child_pid = self.root / "leaf.pid"
        middle = (
            f"echo $$ > {shlex.quote(str(middle_pid))}; "
            f"sleep 30 & echo $! > {shlex.quote(str(child_pid))}; wait"
        )
        directory = self._prepare(
            f"echo $$ > {shlex.quote(str(root_pid))}; bash -c {shlex.quote(middle)} & wait"
        )
        self._start(directory)
        pids = [self._pid(path) for path in (root_pid, middle_pid, child_pid)]
        self.assertFalse((directory / "result.json").exists())
        stopped = self._stop(directory)
        self.assertEqual(stopped.returncode, 0, stopped.stderr)
        self.assertTrue(self._result(directory)["descendants_exited"])
        for pid in pids:
            self._assert_gone(pid)

    def test_repeated_stop_preserves_original_result(self):
        directory = self._prepare("sleep 30")
        self._start(directory)
        first = self._stop(directory, "gpu_idle_timeout")
        second = self._stop(directory, "user_cancelled")
        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertEqual(json.loads(first.stdout), json.loads(second.stdout))
        self.assertEqual(json.loads(second.stdout)["termination_reason"], "gpu_idle_timeout")

    def test_output_exit_marker_does_not_complete_workload(self):
        directory = self._prepare("echo EXIT:0; sleep 30")
        process = self._start(directory)
        self._wait(lambda: "EXIT:0" in (directory / "output.log").read_text())
        self.assertFalse((directory / "result.json").exists())
        self.assertIsNone(process.poll())
        self.assertEqual(self._stop(directory).returncode, 0)

    def test_runner_sigterm_and_sighup_confirm_descendant_shutdown(self):
        for sig in (signal.SIGTERM, signal.SIGHUP):
            with self.subTest(signal=sig):
                pid_file = self.root / f"signal-{sig}.pid"
                directory = self._prepare(f"echo $$ > {shlex.quote(str(pid_file))}; exec sleep 30")
                process = self._start(directory)
                child = self._pid(pid_file)
                process.send_signal(sig)
                self.assertEqual(self._result(directory)["termination_reason"], "user_cancelled")
                self._assert_gone(child)

    def test_runner_crash_has_no_completion_and_probe_is_lost(self):
        # Adopt the deliberately orphaned child in this test, rather than leave
        # it to an unrelated PID 1 (which may not reap in a CI container).
        libc = ctypes.CDLL(None, use_errno=True)
        previous = ctypes.c_int()
        self.assertEqual(libc.prctl(37, ctypes.byref(previous), 0, 0, 0), 0)
        self.assertEqual(libc.prctl(36, 1, 0, 0, 0), 0)
        child = None
        try:
            pid_file = self.root / "crash-child.pid"
            directory = self._prepare(f"echo $$ > {shlex.quote(str(pid_file))}; exec sleep 30")
            process = self._start(directory)
            child = self._pid(pid_file)
            process.kill()
            process.wait(timeout=2)
            self.assertFalse((directory / "result.json").exists())
            self.worker.harness_dir = str(self.root)
            self.assertEqual(self._probe(directory.name)["state"], "lost")
            stopped = self._stop(directory)
            self.assertNotEqual(stopped.returncode, 0)
            self.assertEqual(json.loads(stopped.stdout)["state"], "unknown")
            self.assertFalse((directory / "result.json").exists())
        finally:
            if child is not None:
                os.kill(child, signal.SIGKILL)
                os.waitpid(child, 0)
                self._assert_gone(child)
            libc.prctl(36, previous.value, 0, 0, 0)
            # A dead supervisor leaves a stale socket; do not repeat its stop in teardown.
            if self.directories:
                (self.directories[-1] / "control.sock").unlink(missing_ok=True)

    def test_identity_mismatch_and_boot_change_are_lost(self):
        directory = self._prepare("sleep 30")
        self.worker.harness_dir = str(self.root)
        self._start(directory)
        self.assertEqual(self._probe(directory.name)["state"], "running")
        runtime_path = directory / "runtime.json"
        original = json.loads(runtime_path.read_text())
        for modification in ({"start_ticks": original["start_ticks"] + 1}, {"boot_id": "different-boot"}):
            runtime_path.write_text(json.dumps({**original, **modification}))
            self.assertEqual(self._probe(directory.name)["state"], "lost")
        runtime_path.write_text(json.dumps(original))
        self.assertEqual(self._stop(directory).returncode, 0)

    def test_pidfd_signaling_rejects_reused_identity_and_non_descendants(self):
        child = subprocess.Popen(["sleep", "30"])
        self.processes.append(child)
        ticks = job_runner._process(child.pid)[1]
        self.assertFalse(job_runner._signal_descendant(
            child.pid, ticks + 1, os.getpid(), signal.SIGKILL,
        ))
        self.assertIsNone(child.poll())
        self.assertFalse(job_runner._signal_descendant(
            child.pid, ticks, child.pid, signal.SIGKILL,
        ))
        self.assertIsNone(child.poll())
        child.terminate()
        child.wait(timeout=2)
        self._assert_gone(child.pid)

    def test_preflight_failure_does_not_start_workload(self):
        marker = self.root / "should-not-run"
        directory = self._prepare(f"touch {shlex.quote(str(marker))}")
        code = (
            "from pathlib import Path; import os; "
            "from worker_harness.job_runner import run; "
            "del os.pidfd_open; "
            f"raise SystemExit(run(Path({str(directory)!r})))"
        )
        result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=5)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(marker.exists())
        self.assertFalse((directory / "runtime.json").exists())
        self.assertFalse((directory / "result.json").exists())
        self.assertIn("pidfd", (directory / "output.log").read_text())

    def test_remote_launch_probe_and_stop_through_real_local_shell(self):
        job_id = "transport"
        pid_file = self.root / "transport.pid"
        command = f"echo $$ > {shlex.quote(str(pid_file))}; printf 'gpu=%s\\n' \"$CUDA_VISIBLE_DEVICES\"; exec sleep 30"
        launched = self._shell(_build_job_command(self.worker, job_id, command, []))
        directory = Path(self.worker.harness_dir) / job_id
        self.directories.append(directory)
        self.assertEqual(launched.returncode, 0, launched.stderr)
        self.assertEqual(launched.stdout.strip(), "started")
        child = self._pid(pid_file)
        self.assertEqual(self._probe(job_id)["state"], "running")
        self.assertIn("gpu=\n", (directory / "output.log").read_text())
        stopped = self._shell(_job_stop_command(self.worker, job_id, "user_cancelled"))
        self.assertEqual(stopped.returncode, 0, stopped.stderr)
        self.assertEqual(self._probe(job_id), {
            "state": "finished", "exit_code": 143, "termination_reason": "user_cancelled",
        })
        self._assert_gone(child)
        repeated = self._shell(_job_stop_command(self.worker, job_id, "user_cancelled"))
        self.assertEqual(json.loads(repeated.stdout), json.loads(stopped.stdout))

    def test_remote_probe_absent_starting_and_invalid_result(self):
        self.assertEqual(self._probe("missing"), {"state": "absent"})
        directory = Path(self.worker.harness_dir) / "starting"
        directory.mkdir(parents=True)
        self.assertEqual(self._probe("starting"), {"state": "starting"})
        (directory / "result.json").write_text(json.dumps({"exit_code": 0, "descendants_exited": False}))
        result = self._shell(_job_probe_command(self.worker, "starting"))
        self.assertNotEqual(result.returncode, 0)

    def test_probe_reports_proven_pre_workload_launch_failure(self):
        marker = self.root / "never-runs"
        directory = Path(self.worker.harness_dir) / "bad-launch"
        directory.mkdir(parents=True, mode=0o700)
        self.directories.append(directory)
        (directory / "runner.py").write_bytes(Path(job_runner.__file__).read_bytes())
        (directory / "script.sh").write_text(f"#!/bin/bash\ntouch {shlex.quote(str(marker))}\n")
        (directory / "gpu.env").write_text("CUDA_VISIBLE_DEVICES=not-a-gpu\n")
        failed = subprocess.run(
            [sys.executable, str(directory / "runner.py"), "run", "--job-dir", str(directory)],
            capture_output=True, text=True, timeout=10,
        )
        self.assertNotEqual(failed.returncode, 0)
        self.assertTrue((directory / "launch_error.json").exists())
        self.assertFalse(marker.exists())
        self.assertEqual(self._probe("bad-launch"), {"state": "launch_failed"})

    def test_log_range_reads_exact_bytes_from_an_offset(self):
        from worker_harness.ssh import _log_range_command

        directory = Path(self.worker.harness_dir) / "logs"
        directory.mkdir(parents=True)
        self.directories.append(directory)
        (directory / "output.log").write_bytes(b"".join(b"line %d\n" % index for index in range(200)))
        size = (directory / "output.log").stat().st_size

        def read(start, limit):
            output = subprocess.run(
                ["bash", "-c", _log_range_command(self.worker, "logs", start, limit)],
                capture_output=True, timeout=10,
            ).stdout
            header, _, content = output.partition(b"\n")
            return [int(value) for value in header.split()], content

        (first, end), content = read(None, 64)
        self.assertEqual((first, end), (size - 64, size))
        self.assertTrue(content.endswith(b"line 199\n"))
        with (directory / "output.log").open("ab") as log:
            log.write(b"appended\n")
        (first, end), content = read(end, 1 << 20)
        self.assertEqual((first, content), (size, b"appended\n"))
        # An offset beyond the file (truncated/replaced log) is clamped to its end.
        (first, end), content = read(end + 50, 1 << 20)
        self.assertEqual((first, end, content), (size + 9, size + 9, b""))
        # Reads are bounded by the byte limit and never exceed the sampled size.
        (first, end), content = read(0, 100)
        self.assertEqual((first, end, len(content)), (0, 100, 100))
        (first, end), content = read(None, 10)
        self.assertEqual((first, end, content), (size - 1, size + 9, b"\nappended\n"))
        missing = subprocess.run(
            ["bash", "-c", _log_range_command(self.worker, "absent-job", None, 64)],
            capture_output=True, timeout=10,
        ).stdout
        self.assertEqual(missing, b"0 0\n")

    def test_ssh_public_functions_use_real_shell_and_return_durable_probe(self):
        # Only transport is replaced. Launch, supervisor, commands, probes,
        # stop and process termination are the production implementation.
        async def local_transport(lane_key, args, **kwargs):
            self.assertEqual(lane_key, self.worker.id)
            process = await asyncio.create_subprocess_exec(
                "bash", "-c", args[-1], stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
            out, err = await process.communicate()
            return SSHResult(stdout=out.decode(), stderr=err.decode(), returncode=process.returncode)

        async def scenario():
            with patch("worker_harness.ssh._exec_ssh", new=local_transport):
                launched = await ssh_job_launch(self.worker, "api", "sleep 30", gpu_indices=[])
                directory = Path(self.worker.harness_dir) / "api"
                self.directories.append(directory)
                self.assertEqual(launched.returncode, 0, launched.stderr)
                self.assertEqual((await ssh_job_probe(self.worker, "api")).state, "running")
                stopped = await ssh_job_stop(self.worker, "api", "gpu_idle_timeout")
                self.assertEqual(stopped.state, "finished")
                self.assertEqual(stopped.termination_reason, "gpu_idle_timeout")
                self.assertEqual(await ssh_read_log(self.worker, "api", tail=0), "")
        asyncio.run(scenario())

    def test_log_range_survives_text_transport_with_non_utf8_bytes(self):
        from worker_harness.ssh import ssh_read_log_range

        directory = Path(self.worker.harness_dir) / "binary"
        directory.mkdir(parents=True)
        self.directories.append(directory)
        payload = b"ok\n\xff\xfe broken utf8\n\x00tail\n"
        (directory / "output.log").write_bytes(payload)

        async def local_transport(lane_key, args, **kwargs):
            # The production text adapter decodes with replacement characters.
            process = await asyncio.create_subprocess_exec(
                "bash", "-c", args[-1], stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
            out, err = await process.communicate()
            return SSHResult(stdout=out.decode(errors="replace"), stderr=err.decode(), returncode=process.returncode)

        async def scenario():
            with patch("worker_harness.ssh._exec_ssh", new=local_transport):
                self.assertEqual(await ssh_read_log_range(self.worker, "binary", None), (0, len(payload), payload))
                self.assertEqual(await ssh_read_log_range(self.worker, "binary", 3), (3, len(payload), payload[3:]))
                self.assertEqual(await ssh_read_log_range(self.worker, "missing", None), (0, 0, b""))
        asyncio.run(scenario())
