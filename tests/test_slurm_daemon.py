"""Allocation daemon behavior with real durable files and local processes."""
import asyncio
import ctypes
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import AsyncMock, patch

import httpx


ROOT = Path(__file__).resolve().parents[1]
DAEMON = ROOT / "worker_container" / "worker_daemon.py"
RUNNER = ROOT / "src" / "worker_harness" / "job_runner.py"
spec = importlib.util.spec_from_file_location("allocation_daemon", DAEMON)
daemon = importlib.util.module_from_spec(spec)
spec.loader.exec_module(daemon)


def settings(worker_id="reserved-one", grace=60):
    return {"worker_id": worker_id, "worker_name": "reserved-name", "compute_kind": "cpu",
            "gpu_count": 0, "cpus": 2, "memory_mb": 4096,
            "failure_grace_seconds": grace, "termination_grace_seconds": 0,
            "checkpoint_signal": "USR1"}


def result(code=1, *, ago=0):
    return {"exit_code": code, "descendants_exited": True,
            "finished_at": time.time() - ago, "finished_monotonic": time.monotonic() - ago,
            "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip()}


def wait_until(predicate, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.025)
    raise AssertionError("timed out waiting for process observation")


class RuntimeFiles(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.harness = self.root / "harness"
        self.harness.mkdir()
        self.paths = patch.multiple(daemon, WH_DIR=self.root, HARNESS_DIR=self.harness,
                                    WORKER_ID_FILE=self.root / "worker-daemon" / "id",
                                    ORCHESTRATOR_HOST="127.0.0.1")
        self.paths.start()
        self.addCleanup(self.paths.stop)
        self.addCleanup(self.tmp.cleanup)
        self.runtime = daemon.SlurmRuntime(settings(), "reserved-one", "scoped-secret")

    def record(self, job_id, *, useful=True, finished=None, started=None):
        directory = self.harness / job_id
        directory.mkdir(exist_ok=True)
        start = started if started is not None else time.time() - 10
        value = {"job_id": job_id, "queue_managed": useful, "started_at": start,
                 "started_monotonic": time.monotonic() - (time.time() - start),
                 "boot_id": self.runtime.boot_id, "checkpoint_signal": "USR1"}
        daemon.atomic_json(directory / "runtime.json", value)
        if finished:
            daemon.atomic_json(directory / "result.json", finished)
        return directory

    def test_reserved_identity_persists_and_rejects_other_reservation(self):
        with patch.dict(os.environ, {"WH_SLURM_WORKER_ID": "reserved-one"}):
            self.assertEqual(daemon.get_worker_id(), "reserved-one")
        with patch.dict(os.environ, {"WH_SLURM_WORKER_ID": "other"}):
            with self.assertRaisesRegex(ValueError, "persisted worker identity"):
                daemon.get_worker_id()

    def test_result_requires_descendant_proof_and_strips_non_schema_fields(self):
        value = dict(result(), queue_managed=True)
        self.assertNotIn("queue_managed", daemon.valid_result(value))
        for invalid in (dict(value, descendants_exited=False), dict(value, exit_code=True),
                        dict(value, finished_at=float("nan")), dict(value, boot_id="")):
            self.assertIsNone(daemon.valid_result(invalid))
        observed = dict(value, started_at=value["finished_at"] - 1,
                        started_monotonic=value["finished_monotonic"] - 1)
        self.assertEqual(daemon.valid_result(observed)["started_at"], observed["started_at"])
        self.assertIsNone(daemon.valid_result(dict(observed, started_monotonic=None)))

    def test_unproven_exit_keeps_claim_despite_forged_exit_text(self):
        directory = self.record("unknown")
        (directory / "output.log").write_text("EXIT:0\n")
        daemon.atomic_json(directory / "result.json", dict(result(0), descendants_exited=False))
        self.runtime.scan()
        self.assertTrue(self.runtime.useful_active())
        self.assertEqual(self.runtime.state["pending_results"], {})

    def test_failure_deadline_persisted_before_report_and_reconnect_does_not_extend(self):
        finished = result(7, ago=8)
        self.record("failed", finished=finished)
        self.runtime.scan()
        deadline = finished["finished_at"] + 60
        self.assertEqual(self.runtime.state["failure_deadline"], deadline)
        reconnected = daemon.SlurmRuntime(settings(), "reserved-one", "scoped-secret")
        reconnected.scan()
        self.assertEqual(reconnected.state["failure_deadline"], deadline)
        self.assertEqual(reconnected.state["pending_results"]["failed"], finished)
        self.assertLessEqual((self.root / "observations.json").stat().st_mode & 0o777, 0o600)

    def test_diagnostic_start_does_not_clear_but_queued_rerun_does_permanently(self):
        self.record("failed", finished=result(2, ago=5))
        self.runtime.scan()
        self.record("diagnosis", useful=False, started=time.time())
        self.runtime.scan()
        self.assertIsNotNone(self.runtime.grace_remaining())
        self.record("rerun", started=time.time())
        self.runtime.scan()
        self.assertIsNone(self.runtime.grace_remaining())
        reconnected = daemon.SlurmRuntime(settings(), "reserved-one", "scoped-secret")
        reconnected.scan()
        self.assertIsNone(reconnected.grace_remaining())
        self.assertIn("failed", reconnected.state["pending_results"])

    def test_reconnect_discovers_failed_then_successful_rerun_without_resurrecting_grace(self):
        self.record("failed", finished=result(2, ago=8), started=time.time() - 20)
        self.record("rerun", finished=result(0, ago=1), started=time.time() - 5)
        self.runtime.scan()
        self.assertIsNone(self.runtime.grace_remaining())
        self.assertEqual(set(self.runtime.state["pending_results"]), {"failed", "rerun"})

    def test_launch_failure_without_runtime_is_reported(self):
        directory = self.harness / "never-started"
        directory.mkdir()
        daemon.atomic_json(directory / "launch.json", {"job_id": directory.name, "queue_managed": True})
        daemon.atomic_json(directory / "result.json", result(1))
        self.runtime.scan()
        self.assertFalse(self.runtime.useful_active())
        self.assertIn(directory.name, self.runtime.state["pending_results"])
        self.assertIsNotNone(self.runtime.grace_remaining())

    def test_monotonic_clock_prevents_wall_clock_rollback_extending_grace(self):
        finished = result(1, ago=5)
        self.record("failed", finished=finished)
        self.runtime.scan()
        with patch.object(daemon.time, "time", return_value=finished["finished_at"] - 1000):
            self.assertLessEqual(self.runtime.grace_remaining(), 55)

    def test_monotonic_start_clears_grace_even_when_wall_clock_moves_backwards(self):
        finished = result(1)
        self.record("failed", finished=finished)
        self.runtime.scan()
        directory = self.record("rerun", started=finished["finished_at"] - 100)
        record = daemon.read_json(directory / "runtime.json")
        record["started_monotonic"] = finished["finished_monotonic"] + 0.1
        daemon.atomic_json(directory / "runtime.json", record)
        self.runtime.scan()
        self.assertIsNone(self.runtime.grace_remaining())
        reconnected = daemon.SlurmRuntime(settings(), "reserved-one", "scoped-secret")
        reconnected.scan()
        self.assertIsNone(reconnected.grace_remaining())

    def test_idle_release_rechecks_actual_launch_under_shared_fence(self):
        self.runtime.scan()
        # A real launch may complete after the loop's previous scan but before
        # it takes the admission lock. It must continue rather than be killed.
        self.record("just-started", started=time.time())
        control = AsyncMock()
        with patch.object(self.runtime, "runner_control", control):
            self.assertFalse(asyncio.run(self.runtime.stop_all(idle_only=True)))
        control.assert_not_awaited()
        self.assertFalse((self.harness / ".draining").exists())

    def test_cpu_payload_never_probes_cuda_or_nvidia_and_uses_reserved_limits(self):
        with patch.object(daemon, "get_gpu_info", side_effect=AssertionError("NVIDIA called")), \
             patch.object(daemon, "cuda_device_uuids", side_effect=AssertionError("CUDA called")), \
             patch.object(daemon, "get_system_info", return_value={"cpu_cores": 128,
                          "total_ram_gb": 1024, "used_ram_gb": 100}), \
             patch.object(daemon, "get_data_paths", return_value=[]):
            payload = self.runtime.payload("127.0.0.1", "local")
        self.assertEqual((payload["cpu_cores"], payload["total_ram_gb"]), (2, 4))
        self.assertLessEqual(payload["used_ram_gb"], 4)
        self.assertEqual((payload["gpu_count"], payload["gpus"]), (0, []))
        self.assertEqual(payload["name"], "reserved-name")

    def test_gpu_telemetry_filtered_and_ordered_by_driver_uuid(self):
        observed = [{"index": index, "uuid": device, "name": device}
                    for index, device in enumerate(("GPU-third", "GPU-second", "GPU-first"))]
        with patch.object(daemon, "get_gpu_info", return_value={"gpus": observed}):
            mapped = daemon.allocation_gpu_info(["GPU-first", "GPU-second"])
            self.assertEqual([gpu["index"] for gpu in mapped["gpus"]], [0, 1])
            self.assertEqual([gpu["uuid"] for gpu in mapped["gpus"]], ["GPU-first", "GPU-second"])
            with self.assertRaisesRegex(ValueError, "absent"):
                daemon.allocation_gpu_info(["GPU-missing"])
        with patch.object(daemon, "get_gpu_info", return_value={"gpus": [observed[0], observed[0]]}):
            with self.assertRaisesRegex(ValueError, "ambiguous"):
                daemon.allocation_gpu_info(["GPU-third"])

    def test_cuda_driver_mapping_respects_original_order_and_rejects_bad_count(self):
        identities = [uuid.uuid4(), uuid.uuid4()]

        class Driver:
            def cuInit(self, flags):
                return 0

            def cuDeviceGetCount(self, pointer):
                ctypes.cast(pointer, ctypes.POINTER(ctypes.c_int))[0] = len(identities)
                return 0

            def cuDeviceGet(self, pointer, index):
                ctypes.cast(pointer, ctypes.POINTER(ctypes.c_int))[0] = index
                return 0

            def cuDeviceGetUuid_v2(self, pointer, device):
                ctypes.memmove(pointer, identities[device.value].bytes, 16)
                return 0

        with patch.dict(os.environ, {"WH_SLURM_CUDA_VISIBLE_DEVICES": "3,1"}), \
             patch.object(daemon.ctypes, "CDLL", return_value=Driver()):
            self.assertEqual(daemon.cuda_device_uuids(2), ["GPU-" + str(value) for value in identities])
            self.assertEqual(os.environ["CUDA_VISIBLE_DEVICES"], "3,1")
            with self.assertRaisesRegex(ValueError, "count"):
                daemon.cuda_device_uuids(1)
            identities[1] = identities[0]
            with self.assertRaisesRegex(ValueError, "ambiguous"):
                daemon.cuda_device_uuids(2)
        for visibility in ("", "MIG-GPU-slice"):
            with patch.dict(os.environ, {"WH_SLURM_CUDA_VISIBLE_DEVICES": visibility}), \
                 patch.object(daemon.ctypes, "CDLL", side_effect=AssertionError("must reject before driver")):
                with self.assertRaises(ValueError):
                    daemon.cuda_device_uuids(1)

    def test_scoped_readiness_and_report_acknowledgement(self):
        self.record("failed", finished=result())
        self.runtime.scan()
        replies = iter(["offline", "online"])
        payloads = []

        def transport(request):
            self.assertEqual(request.headers["Authorization"], "Bearer scoped-secret")
            payloads.append(json.loads(request.content))
            return httpx.Response(200, json={"worker_id": "reserved-one", "worker_status": next(replies)})

        async def scenario():
            async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
                await self.runtime.heartbeat(client)
                self.assertFalse((self.root / "ready").exists())
                await self.runtime.heartbeat(client)
                self.assertTrue((self.root / "ready").exists())

        with patch.object(daemon, "get_tailscale_identity", return_value=("127.0.0.1", "local")), \
             patch.object(daemon, "get_system_info", return_value={}), \
             patch.object(daemon, "get_data_paths", return_value=[]):
            asyncio.run(scenario())
        self.assertEqual(payloads[0]["slurm"]["results"][0]["job_id"], "failed")
        self.assertEqual(self.runtime.state["pending_results"], {})

    def test_unavailable_registration_retains_durable_results(self):
        self.record("failed", finished=result())
        self.runtime.scan()

        async def scenario():
            async with httpx.AsyncClient(transport=httpx.MockTransport(lambda req: httpx.Response(503))) as client:
                self.assertIsNone(await self.runtime.heartbeat(client))

        with patch.object(daemon, "get_tailscale_identity", return_value=("127.0.0.1", "local")), \
             patch.object(daemon, "get_system_info", return_value={}), \
             patch.object(daemon, "get_data_paths", return_value=[]):
            asyncio.run(scenario())
        self.assertIn("failed", daemon.read_json(self.root / "observations.json")["pending_results"])
        self.assertFalse((self.root / "ready").exists())

    def test_stop_is_sticky_and_checkpoint_uses_workload_signal(self):
        self.record("work")
        self.runtime.scan()
        control = AsyncMock()

        async def scenario():
            daemon.atomic_json(self.root / "control.json", {"action": "checkpoint", "requested_at": time.time()})
            await self.runtime.controls()
            control.assert_awaited_with("work", "checkpoint", "USR1")
            daemon.atomic_json(self.root / "control.json", {"action": "stop", "requested_at": time.time()})
            await self.runtime.controls()
            control.assert_awaited_with("work", "stop", "TERM")
            daemon.atomic_json(self.root / "control.json", {"action": "checkpoint", "requested_at": time.time()})
            await self.runtime.controls()
            control.assert_awaited_with("work", "stop", "TERM")

        with patch.object(self.runtime, "runner_control", control):
            asyncio.run(scenario())
        self.assertTrue((self.harness / ".draining").exists())

    def test_checkpoint_uses_declared_term_and_old_iso_stop_deadline_is_not_restarted(self):
        directory = self.record("work")
        record = daemon.read_json(directory / "runtime.json")
        record["checkpoint_signal"] = "TERM"
        daemon.atomic_json(directory / "runtime.json", record)
        self.runtime.scan()
        self.runtime.settings["termination_grace_seconds"] = 10
        control = AsyncMock()

        async def scenario():
            daemon.atomic_json(self.root / "control.json", {"action": "checkpoint"})
            await self.runtime.controls()
            control.assert_awaited_with("work", "checkpoint", "TERM")
            earlier = daemon.datetime.fromtimestamp(time.time() - 30, daemon.timezone.utc).isoformat()
            daemon.atomic_json(self.root / "control.json", {"action": "stop", "requested_at": earlier})
            await self.runtime.controls()
            control.assert_awaited_with("work", "stop", "TERM")

        with patch.object(self.runtime, "runner_control", control):
            asyncio.run(scenario())
        self.assertLessEqual(self.runtime.control_deadline, time.monotonic())

    @unittest.skipUnless(sys.platform.startswith("linux") and hasattr(os, "pidfd_open"), "requires Linux pidfds")
    def test_control_stops_real_runner_descendants_and_fences_delayed_launch(self):
        (self.harness / ".job_runner.py").symlink_to(RUNNER)
        directory = self.harness / "work"
        directory.mkdir()
        daemon.atomic_json(directory / "launch.json", {"job_id": "work", "queue_managed": True,
                                                       "checkpoint_signal": "USR1"})
        (directory / "gpu.env").write_text("CUDA_VISIBLE_DEVICES=\n")
        (directory / "script.sh").write_text("#!/bin/bash\ntrap '' TERM\nsleep 60 &\nwait\n")
        process = subprocess.Popen([sys.executable, str(RUNNER), "run", "--job-dir", str(directory)],
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            wait_until(lambda: (directory / "runtime.json").exists() or process.poll() is not None)
            self.assertIsNone(process.poll(), daemon.read_json(directory / "result.json"))
            self.runtime.scan()
            daemon.atomic_json(self.root / "control.json", {"action": "stop", "requested_at": time.time()})
            asyncio.run(self.runtime.controls())
            process.wait(timeout=8)
            proof = daemon.valid_result(daemon.read_json(directory / "result.json"))
            self.assertIsNotNone(proof)
            self.assertTrue(proof["descendants_exited"])
            self.assertTrue((self.harness / ".draining").exists())
            self.runtime.scan()
            self.assertFalse(self.runtime.useful_active())
            delayed = self.harness / "delayed"
            delayed.mkdir()
            daemon.atomic_json(delayed / "launch.json", {"job_id": "delayed", "queue_managed": True,
                                                          "checkpoint_signal": "USR1"})
            (delayed / "gpu.env").write_text("CUDA_VISIBLE_DEVICES=\n")
            (delayed / "script.sh").write_text("touch should-not-exist\n")
            denied = subprocess.run([sys.executable, str(RUNNER), "run", "--job-dir", str(delayed)],
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5)
            self.assertNotEqual(denied.returncode, 0)
            self.assertFalse((delayed / "runtime.json").exists())
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)


class DaemonProcess(unittest.TestCase):
    def test_real_cpu_daemon_waits_for_authenticated_ready_and_transactional_drain(self):
        self._run_cpu_daemon()

    def test_real_cpu_daemon_honors_persisted_failure_deadline_during_outage(self):
        self._run_cpu_daemon(failed=True, outage=True)

    def _run_cpu_daemon(self, *, failed=False, outage=False):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            runtime = root / "runtime"
            runtime.mkdir()
            config = settings(grace=4 if failed else 60)
            daemon.atomic_json(root / "settings.json", config)
            (root / "token").write_text("scoped-secret")
            tools = root / "bin"
            tools.mkdir()
            tailscale = tools / "tailscale"
            tailscale.write_text("#!/bin/sh\nprintf '%s\\n' '{\"Self\":{\"TailscaleIPs\":[\"127.0.0.1\"],\"DNSName\":\"local\"}}'\n")
            tailscale.chmod(0o700)
            # These boundaries must never be called by a CPU allocation.
            nvidia = tools / "nvidia-smi"
            nvidia.write_text(f"#!/bin/sh\ntouch '{root / 'nvidia-called'}'\nexit 1\n")
            nvidia.chmod(0o700)
            seen = []
            errors = []

            class Handler(BaseHTTPRequestHandler):
                def log_message(self, *_args):
                    pass

                def do_POST(self):
                    payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                    seen.append(payload)
                    if self.headers.get("Authorization") != "Bearer scoped-secret":
                        errors.append("unscoped heartbeat")
                    if len(seen) == 1 and (runtime / "ready").exists():
                        errors.append("ready before server authentication")
                    if not failed and len(seen) <= 3 and (runtime / "harness" / ".draining").exists():
                        errors.append("local admission fenced before transactional drain accepted")
                    if failed and len(seen) == 1:
                        job_dir = runtime / "harness" / "failed"
                        job_dir.mkdir(parents=True)
                        daemon.atomic_json(job_dir / "runtime.json", {
                            "job_id": "failed", "queue_managed": True,
                            "started_at": time.time() - 1, "started_monotonic": time.monotonic() - 1,
                            "boot_id": result()["boot_id"], "checkpoint_signal": "USR1",
                        })
                        daemon.atomic_json(job_dir / "result.json", result(9))
                    draining = not failed and len(seen) >= 3 and payload["slurm"]["draining"]
                    body = {"worker_id": "reserved-one", "worker_status": "draining" if draining else "online",
                            "draining": draining, "queued_jobs": [], "running_jobs": [], "failure_deadline": None}
                    encoded = json.dumps(body).encode()
                    self.send_response(503 if outage and len(seen) > 1 else 200)
                    self.send_header("Content-Length", str(len(encoded)))
                    self.end_headers()
                    self.wfile.write(encoded)

            server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            environment = dict(os.environ, WH_DIR=str(runtime), WH_SLURM_WORKER_ID="reserved-one",
                               WH_SLURM_SETTINGS_FILE=str(root / "settings.json"),
                               WH_WORKER_TOKEN_FILE=str(root / "token"), ORCHESTRATOR_HOST="127.0.0.1",
                               ORCHESTRATOR_PORT=str(server.server_port), PATH=str(tools) + os.pathsep + os.environ["PATH"])
            environment.pop("WH_PROXY", None)
            with (root / "daemon.log").open("w+") as output:
                process = subprocess.Popen([sys.executable, str(DAEMON)], env=environment,
                                           stdout=output, stderr=output)
                try:
                    process.wait(timeout=15)
                    output.seek(0)
                    self.assertEqual(process.returncode, 1 if failed else 0, output.read())
                    self.assertGreaterEqual(len(seen), 2 if failed else 3)
                    if failed:
                        observations = daemon.read_json(runtime / "observations.json")
                        finished = daemon.read_json(runtime / "harness" / "failed" / "result.json")
                        self.assertEqual(observations["failure_deadline"], finished["finished_at"] + 4)
                        self.assertGreaterEqual(time.time(), observations["failure_deadline"])
                        self.assertIn("failed", observations["pending_results"])
                    self.assertFalse(errors, errors)
                    self.assertTrue((runtime / "ready").exists())
                    self.assertTrue((runtime / "harness" / ".draining").exists())
                    self.assertFalse((root / "nvidia-called").exists())
                    self.assertTrue(all(item["gpu_count"] == 0 and item["gpus"] == [] for item in seen))
                    self.assertTrue(all(item["name"] == "reserved-name" for item in seen))
                finally:
                    if process.poll() is None:
                        process.kill()
                        process.wait(timeout=5)
                    server.shutdown()
                    server.server_close()
                    thread.join(timeout=5)
