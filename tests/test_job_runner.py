"""Local Linux execution tests: supervision is real, not mocked subprocesses."""
import fcntl
import json
import os
from pathlib import Path
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
from unittest.mock import AsyncMock, patch

from worker_harness import job_runner
from worker_harness.db import Database
from worker_harness.job import JobManager
from worker_harness.models import Job, JobStatus, RunnerResult, SlurmReservationRequest, Worker
from worker_harness.ssh import (
    SSHResult, _build_job_command, _cluster_gpu_mask,
    ssh_get_exit_code, ssh_job_result, ssh_job_started, ssh_tmux_running,
)


MODULE = str(Path(job_runner.__file__).resolve())


def cluster_worker(kind="gpu", harness="/harness"):
    return Worker(
        id="allocation", name="allocation", worker_ip="127.0.0.1", harness_dir=harness,
        gpu_count=2 if kind == "gpu" else 0,
        gpu_device_ids=["GPU-11111111-1111-1111-1111-111111111111", "GPU-22222222-2222-2222-2222-222222222222"] if kind == "gpu" else [],
        slurm={
            "request_id": "request", "recipient_id": "tests", "cluster_id": "test",
            "storage_id": "test", "compute_kind": kind, "cpus": 2, "memory_mb": 4096,
            "walltime_seconds": 600, "failure_grace_seconds": 60,
            "warning_seconds": 30, "termination_grace_seconds": 10,
        },
    )


def proof(exit_code=0):
    return RunnerResult(
        exit_code=exit_code, descendants_exited=True, finished_at=time.time(),
        finished_monotonic=time.monotonic(), boot_id="test-boot",
    )


@unittest.skipUnless(sys.platform.startswith("linux") and hasattr(os, "pidfd_open"), "Linux pidfds required")
class RealRunnerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.processes = []

    def tearDown(self):
        for process, directory in self.processes:
            if process.poll() is None:
                subprocess.run(
                    [sys.executable, MODULE, "control", "--job-dir", str(directory), "--action", "stop", "--signal", "TERM"],
                    capture_output=True, timeout=12,
                )
                try:
                    process.wait(timeout=4)
                except subprocess.TimeoutExpired:
                    process.terminate()
                    process.wait(timeout=5)
            process.communicate()
        self.tmp.cleanup()

    def wait_for(self, predicate, timeout=8):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = predicate()
            if value:
                return value
            time.sleep(0.02)
        self.fail("Timed out awaiting runner observation")

    def prepare(self, code, *, directory=None, gpu_mask="", queue_managed=True):
        directory = directory or self.root / "harness" / "job"
        directory.mkdir(parents=True)
        (directory / "script.sh").write_text("#!/bin/bash\nexec " + shlex.join([sys.executable, "-c", code]) + "\n")
        (directory / "gpu.env").write_text(f"CUDA_VISIBLE_DEVICES={gpu_mask}\n")
        (directory / "launch.json").write_text(json.dumps({
            "job_id": directory.name, "queue_managed": queue_managed, "checkpoint_signal": "USR1",
        }))
        return directory

    def start(self, directory):
        process = subprocess.Popen(
            [sys.executable, MODULE, "run", "--job-dir", str(directory)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        self.processes.append((process, directory))
        return process

    def control(self, directory, action="checkpoint", sig="USR1"):
        return subprocess.run(
            [sys.executable, MODULE, "control", "--job-dir", str(directory), "--action", action, "--signal", sig],
            capture_output=True, text=True, timeout=12,
        )

    def test_setsid_orphan_holds_completion_until_reaped(self):
        ready = self.root / "child-ready"
        release = self.root / "release"
        child_pid = self.root / "child-pid"
        directory = self.prepare(f"""
import os, time
if os.fork():
    os._exit(0)
os.setsid()
open({str(child_pid)!r}, 'w').write(str(os.getpid()))
open({str(ready)!r}, 'w').close()
while not os.path.exists({str(release)!r}): time.sleep(.02)
os._exit(0)
""")
        process = self.start(directory)
        self.wait_for(ready.exists)
        self.assertFalse((directory / "result.json").exists())
        self.assertIsNone(process.poll())
        release.touch()
        self.assertEqual(process.wait(timeout=5), 0)
        result = RunnerResult.model_validate_json((directory / "result.json").read_text())
        self.assertEqual(result.exit_code, 0)
        self.assertGreater(result.finished_at, 0)
        self.assertGreater(result.finished_monotonic, 0)
        self.assertEqual(result.boot_id, Path("/proc/sys/kernel/random/boot_id").read_text().strip())
        runtime = json.loads((directory / "runtime.json").read_text())
        self.assertEqual(result.started_at, runtime["started_at"])
        self.assertEqual(result.started_monotonic, runtime["started_monotonic"])
        self.assertLessEqual(result.started_at, result.finished_at)
        self.assertLessEqual(result.started_monotonic, result.finished_monotonic)
        self.assertIsNone(job_runner._process(int(child_pid.read_text())))

    def test_checkpoint_reaches_setsid_descendants_and_stop_is_sticky(self):
        ready = self.root / "ready"
        signals = self.root / "signals"
        directory = self.prepare(f"""
import os, signal, time
path={str(signals)!r}
def record(sig, frame):
    fd=os.open(path, os.O_WRONLY|os.O_CREAT|os.O_APPEND, 0o600)
    os.write(fd, (str(os.getpid())+':'+str(sig)+'\\n').encode()); os.close(fd)
signal.signal(signal.SIGUSR1, record)
signal.signal(signal.SIGTERM, record)
child=os.fork()
if child == 0:
    os.setsid()
    open({str(ready)!r}+'.child','w').close()
else:
    open({str(ready)!r},'w').close()
while True: time.sleep(.05)
""")
        process = self.start(directory)
        self.wait_for(ready.exists)
        self.wait_for(Path(str(ready) + ".child").exists)
        self.wait_for((directory / "runtime.json").exists)
        response = self.control(directory)
        self.assertEqual(response.returncode, 0, response.stderr)
        def usr1_lines():
            return [line for line in signals.read_text().splitlines() if line.endswith(":" + str(signal.SIGUSR1))] if signals.exists() else []
        self.wait_for(lambda: len(usr1_lines()) == 2)
        before = len(usr1_lines())
        started = time.monotonic()
        stop = subprocess.Popen(
            [sys.executable, MODULE, "control", "--job-dir", str(directory), "--action", "stop", "--signal", "TERM"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        try:
            self.wait_for(lambda: signals.exists() and ":15" in signals.read_text())
            self.assertEqual(self.control(directory).returncode, 0)
            stdout, stderr = stop.communicate(timeout=6)
            self.assertEqual(stop.returncode, 0, stderr)
            self.assertEqual(json.loads(stdout)["state"], "finished")
        finally:
            if stop.poll() is None:
                stop.kill()
                stop.communicate()
        self.assertGreaterEqual(time.monotonic() - started, 1.8)
        self.assertLess(time.monotonic() - started, 6)
        self.assertEqual(process.wait(timeout=3), 0)
        self.assertEqual(len(usr1_lines()), before)
        result = json.loads((directory / "result.json").read_text())
        self.assertEqual(result["termination_reason"], "user_cancelled")
        self.assertTrue(result["descendants_exited"])

    def test_spoofed_exit_output_is_not_completion(self):
        release = self.root / "release"
        directory = self.prepare(f"import os,time; print('EXIT:0',flush=True)\nwhile not os.path.exists({str(release)!r}): time.sleep(.02)")
        process = self.start(directory)
        self.wait_for(lambda: (directory / "output.log").exists() and "EXIT:0" in (directory / "output.log").read_text())
        self.assertIsNone(job_runner._read_result(directory))
        self.assertIsNone(process.poll())
        release.touch()
        self.assertEqual(process.wait(timeout=3), 0)

    def test_long_shared_path_uses_private_short_socket_and_records_metadata(self):
        directory = self.root / ("x" * 80) / ("y" * 80) / "job"
        directory = self.prepare("import time;time.sleep(30)", directory=directory, queue_managed=False)
        process = self.start(directory)
        self.wait_for((directory / "runtime.json").exists)
        runtime = json.loads((directory / "runtime.json").read_text())
        self.assertLess(len(runtime["socket_path"].encode()), 108)
        self.assertEqual(Path(runtime["socket_path"]).parent.stat().st_mode & 0o777, 0o700)
        self.assertFalse(runtime["queue_managed"])
        self.assertEqual(runtime["checkpoint_signal"], "USR1")
        self.assertEqual(self.control(directory, "stop", "TERM").returncode, 0)
        self.assertEqual(process.wait(timeout=3), 0)

    def test_launch_flock_and_drain_marker_fence_delayed_launch(self):
        ran = self.root / "ran"
        directory = self.prepare(f"open({str(ran)!r},'w').close()")
        with (directory.parent / ".launch.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            process = self.start(directory)
            time.sleep(.15)
            self.assertFalse((directory / "runtime.json").exists())
            self.assertFalse(ran.exists())
            (directory.parent / ".draining").touch()
        self.assertEqual(process.wait(timeout=3), 1)
        self.assertFalse(ran.exists())
        self.assertFalse((directory / "result.json").exists())
        self.assertIn("draining", (directory / "launch_error.json").read_text())

    def test_unknown_or_malformed_result_does_not_claim_completion(self):
        directory = self.root / "unknown"
        directory.mkdir()
        (directory / "output.log").write_text("EXIT:0\n")
        for value in (
            {"exit_code": 0, "descendants_exited": True},
            {**proof().model_dump(), "descendants_exited": False},
            {**proof().model_dump(), "started_at": time.time()},
            {**proof().model_dump(), "started_at": 1, "started_monotonic": time.monotonic() + 100},
            {**proof().model_dump(), "started_at": 1, "started_monotonic": float("inf")},
        ):
            (directory / "result.json").write_text(json.dumps(value))
            self.assertIsNone(job_runner._read_result(directory))
            response = self.control(directory, "stop", "TERM")
            self.assertNotEqual(response.returncode, 0)
            self.assertEqual(json.loads(response.stdout), {"state": "unknown"})

    def test_wall_clock_reversal_accepts_ordered_monotonic_proof(self):
        directory = self.root / "clock-step"
        directory.mkdir()
        value = {
            **proof().model_dump(),
            "started_at": time.time() + 100, "started_monotonic": 0,
        }
        (directory / "result.json").write_text(json.dumps(value))
        self.assertEqual(job_runner._read_result(directory), value)
        self.assertEqual(RunnerResult.model_validate(value).started_at, value["started_at"])

    def test_uuid_and_cpu_masks_reach_real_workload(self):
        for index, mask in enumerate(("GPU-11111111-1111-1111-1111-111111111111", "")):
            directory = self.prepare("import os;print(os.environ['CUDA_VISIBLE_DEVICES'])", directory=self.root / "harness" / str(index), gpu_mask=mask)
            process = self.start(directory)
            self.assertEqual(process.wait(timeout=3), 0)
            self.assertEqual((directory / "output.log").read_text(), mask + "\n")

    def test_checkpoint_term_is_not_supervisor_stop(self):
        ready = self.root / "ready"
        got = self.root / "checkpoint"
        directory = self.prepare(f"""
import signal,time
signal.signal(signal.SIGTERM, lambda *_: open({str(got)!r},'w').close())
open({str(ready)!r},'w').close()
while True: time.sleep(.05)
""")
        process = self.start(directory)
        self.wait_for(ready.exists)
        self.assertEqual(self.control(directory, sig="TERM").returncode, 0)
        self.wait_for(got.exists)
        self.assertIsNone(process.poll())
        self.assertFalse((directory / "result.json").exists())
        self.assertEqual(self.control(directory, "stop", "TERM").returncode, 0)


    @unittest.skipUnless(shutil.which("tmux"), "tmux required")
    def test_real_tmux_launch_preserves_project_environment_wrapper(self):
        harness = self.root / ("shared-" + "x" * 70) / "runtime" / "harness"
        harness.mkdir(parents=True)
        worker = cluster_worker("cpu", str(harness))
        worker.id = str(uuid.uuid4())
        (harness.parent.parent / "settings.json").write_text(json.dumps({
            "worker_id": worker.id, "checkpoint_signal": "TERM",
        }))
        project = self.root / "project with spaces"
        project.mkdir()
        (project / "project_local.py").write_text("VALUE='project import worked'\n")
        environment = self.root / "selected environment" / "bin"
        environment.mkdir(parents=True)
        interpreter = environment / "python"
        interpreter.write_text("#!/bin/sh\nexec " + shlex.quote(sys.executable) + " \"$@\"\n")
        interpreter.chmod(0o700)
        original = "python -c " + shlex.quote(
            "import os,project_local;print(project_local.VALUE);print('mask='+os.environ['CUDA_VISIBLE_DEVICES'])"
        )
        command = (
            f"cd {shlex.quote(str(project))} && "
            f"PATH={shlex.quote(str(environment))}:\"$PATH\" exec /bin/bash -c {shlex.quote(original)}"
        )
        assembled = _build_job_command(worker, "job", command, [], queue_managed=True)
        launched = subprocess.run(["/bin/bash", "-c", assembled], cwd="/", capture_output=True, text=True, timeout=12)
        self.assertEqual(launched.returncode, 0, launched.stderr)
        directory = harness / "job"
        self.wait_for((directory / "result.json").exists)
        self.assertEqual(RunnerResult.model_validate_json((directory / "result.json").read_text()).exit_code, 0)
        self.assertEqual((directory / "output.log").read_text(), "project import worked\nmask=\n")
        runtime = json.loads((directory / "runtime.json").read_text())
        self.assertTrue(runtime["queue_managed"])
        self.assertEqual(runtime["checkpoint_signal"], "TERM")


class ClusterSSHTests(unittest.IsolatedAsyncioTestCase):
    async def test_structured_result_only_ignores_exit_spoof_and_unknown(self):
        worker = cluster_worker()
        for response in (SSHResult("EXIT:0\n", "", 0), SSHResult("", "missing", 1), SSHResult('{"exit_code":0}', "", 0)):
            with patch("worker_harness.ssh.async_ssh_run", new=AsyncMock(return_value=response)) as call:
                self.assertIsNone(await ssh_job_result(worker, "job"))
                self.assertIsNone(await ssh_get_exit_code(worker, "job"))
                self.assertIsNone(await ssh_tmux_running(worker, "job"))
                self.assertNotIn("EXIT:", call.call_args.args[1])
        with patch("worker_harness.ssh.async_ssh_run", new=AsyncMock(return_value=SSHResult(proof().model_dump_json(), "", 0))):
            self.assertFalse(await ssh_tmux_running(worker, "job"))
            self.assertEqual(await ssh_get_exit_code(worker, "job"), 0)

    async def test_absent_cluster_runtime_is_unknown_until_proven_launch_failure(self):
        worker = cluster_worker()
        for records, expected in (
            ({"result": proof().model_dump()}, True),
            ({"launch_error": {"error": "allocation draining"}}, False),
            ({}, None),
            ({"runtime": {"job_id": "other"}}, None),
            ({"result": {"exit_code": 0}}, None),
        ):
            output = json.dumps(records)
            with patch("worker_harness.ssh.async_ssh_run", new=AsyncMock(return_value=SSHResult(output, "", 0))):
                self.assertIs(await ssh_job_started(worker, "job"), expected)

    def test_gpu_uuid_mapping_cpu_empty_and_invalid_mapping(self):
        worker = cluster_worker()
        self.assertEqual(_cluster_gpu_mask(worker, [1]), worker.gpu_device_ids[1])
        self.assertEqual(_cluster_gpu_mask(cluster_worker("cpu"), None), "")
        for ids in ([], ["MIG-abc", "GPU-123"], ["GPU-123", "GPU-123"]):
            with self.assertRaises(ValueError):
                _cluster_gpu_mask(worker.model_copy(update={"gpu_device_ids": ids}), [0])
        with self.assertRaises(ValueError):
            _cluster_gpu_mask(worker, [2])

    def test_cluster_launch_preserves_command_wrapper_and_metadata(self):
        worker = cluster_worker()
        command = "cd '/data/shared/test/project' && PATH='/data/shared/test/venv/bin':\"$PATH\" exec /bin/bash -c 'python train.py'"
        full = _build_job_command(worker, "job", command, [1], queue_managed=True)
        self.assertIn(".job_runner.py", full)
        self.assertIn("run --job-dir /harness/job", full)
        import base64
        self.assertIn(base64.b64encode(("#!/bin/bash\n" + command + "\n").encode()).decode(), full)
        self.assertNotIn("echo EXIT:", full)
        self.assertIn(base64.b64encode(json.dumps({"job_id": "job", "worker_id": worker.id, "queue_managed": True}).encode() + b"\n").decode(), full)


class ClusterJobProofTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.tmp.name) / "test.db")
        await self.db.connect()
        request = SlurmReservationRequest(
            request_id="request", name="allocation", recipient_id="tests", gpu_count=0,
            registration_token_hash="0" * 64,
            slurm=cluster_worker("cpu").slurm.model_dump(include={
                "cluster_id", "storage_id", "compute_kind", "cpus", "memory_mb", "walltime_seconds",
                "failure_grace_seconds", "warning_seconds", "termination_grace_seconds",
            }),
        )
        self.worker = await self.db.reserve_slurm_worker(request)
        self.manager = JobManager(self.db)
        self.job = Job(worker_id=self.worker.id, command="sleep 30", queue_managed=True, gpu_count=0, status=JobStatus.RUNNING)
        await self.db.insert_job(self.job)

    async def asyncTearDown(self):
        await self.db.close()
        self.tmp.cleanup()

    async def test_unknown_stop_never_releases_claim_or_kills_tmux(self):
        with patch("worker_harness.job.ssh_tmux_kill", new=AsyncMock(return_value=SSHResult('{"state":"unknown"}', "", 1))), patch("worker_harness.job.ssh_job_result", new=AsyncMock(return_value=None)):
            self.assertFalse(await self.manager.stop_job(self.worker, self.job.id))
        self.assertEqual((await self.db.get_job(self.job.id)).status, JobStatus.RUNNING)

    async def test_stop_and_reconciliation_persist_real_runner_evidence(self):
        evidence = proof(143)
        with patch("worker_harness.job.ssh_tmux_kill", new=AsyncMock(return_value=SSHResult('{"state":"finished"}', "", 0))), patch("worker_harness.job.ssh_job_result", new=AsyncMock(return_value=evidence)):
            self.assertTrue(await self.manager.stop_job(self.worker, self.job.id))
        persisted = await self.db.get_job(self.job.id)
        self.assertEqual(persisted.status, JobStatus.FAILED)
        self.assertEqual(persisted.exit_code, 143)
        self.assertEqual(persisted.runner_result, evidence)
        self.assertEqual(persisted.finished_at, evidence.finished_at)

    async def test_unknown_refresh_retains_running_claim(self):
        with patch("worker_harness.job.ssh_job_result", new=AsyncMock(return_value=None)):
            result = await self.manager.refresh_job_status(self.worker, self.job)
        self.assertEqual(result.status, JobStatus.RUNNING)
