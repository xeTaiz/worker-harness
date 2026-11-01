"""Finite batch lifecycle exercised with real child processes and signals."""

import fcntl
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest


ROOT = Path(__file__).resolve().parents[1]
WRAPPER = ROOT / "slurm/wh-allocation.sh"


class SlurmAllocationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.run = self.root / ".wh-slurm/test-worker"
        self.run.mkdir(parents=True)
        self.runtime = self.run / "runtime"
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.processes = []
        self.settings = {
            "version": 1,
            "worker_id": "test-worker",
            "compute_kind": "cpu",
            "gpu_count": 0,
            "cpus": 1,
            "memory_mb": 4096,
            "failure_grace_seconds": 0,
            "termination_grace_seconds": 1,
            "warning_seconds": 120,
            "checkpoint_signal": "USR1",
            "host_run_path": str(self.run),
            "run_path": "/data/shared/test/.wh-slurm/test-worker",
            "shared_root": str(self.root),
            "storage_id": "test",
            "sif_path": str(self.root / "worker.sif"),
            "runtime": "apptainer",
            "tailnet_key_file": str(self.run / "tailnet.key"),
            "worker_token_file": str(self.run / "worker.token"),
            "tailnet_login_server": "https://tailnet.invalid",
            "orchestrator_host": "orchestrator.invalid",
            "orchestrator_registration_port": 12888,
        }
        self.settings_path = self.run / "settings.json"
        self.write_settings()
        for name in ("tailnet.key", "worker.token"):
            (self.run / name).write_text("test-only-secret\n")
        shutil.copyfile(WRAPPER, self.run / "wh-allocation.sh")
        (self.run / "start-wh.sh").write_text('#!/bin/bash\nexec python3 "$FAKE_CHILD"\n')
        self.fake_child = self.root / "child.py"
        self.fake_child.write_text(
            "import json, os, pathlib, subprocess, sys, time\n"
            "runtime = pathlib.Path(os.environ['WH_DIR'])\n"
            "mode = os.environ.get('CHILD_MODE', 'cooperate')\n"
            "(runtime / 'environment.json').write_text(json.dumps(dict(os.environ)))\n"
            "(runtime / 'child-started').touch()\n"
            "if mode != 'unready': (runtime / 'ready').touch()\n"
            "if mode == 'success':\n"
            "    (runtime / 'job-result.json').write_text('{\"exit_code\":0}')\n"
            "    print('retained workload log', flush=True)\n"
            "    sys.exit(0)\n"
            "if mode == 'failure': sys.exit(17)\n"
            "if mode == 'stubborn':\n"
            "    child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(300)'])\n"
            "    (runtime / 'descendant.pid').write_text(str(child.pid))\n"
            "while True:\n"
            "    control = runtime / 'control.json'\n"
            "    if control.exists():\n"
            "        value = json.loads(control.read_text())\n"
            "        (runtime / 'observed-control.json').write_text(json.dumps(value))\n"
            "        if value['action'] == 'stop' and mode not in ('stubborn', 'unready'):\n"
            "            sys.exit(0)\n"
            "    time.sleep(0.02)\n"
        )
        # If a wrapper ever submits or requeues, this fake boundary records it.
        for command in ("sbatch", "scontrol"):
            path = self.bin / command
            path.write_text(f'#!/bin/sh\necho "$@" >> "{self.root}/unexpected-submission"\nexit 99\n')
            path.chmod(0o755)
        self.env = dict(os.environ, PATH=f"{self.bin}:{os.environ['PATH']}", FAKE_CHILD=str(self.fake_child))
        self.env.pop("SLURM_RESTART_COUNT", None)

    def tearDown(self):
        for process in self.processes:
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
                try:
                    process.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.communicate(timeout=5)
        self.temporary.cleanup()

    def write_settings(self):
        self.settings_path.write_text(json.dumps(self.settings))

    def start(self, mode="cooperate", **environment):
        env = dict(self.env, CHILD_MODE=mode, **environment)
        process = subprocess.Popen(
            ["bash", str(self.run / "wh-allocation.sh"), str(self.settings_path)],
            cwd=self.root,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.processes.append(process)
        return process

    def wait_for(self, path, predicate=None):
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            if path.exists():
                if predicate is None:
                    return
                try:
                    if predicate(json.loads(path.read_text())):
                        return
                except (json.JSONDecodeError, FileNotFoundError):
                    pass
            time.sleep(0.02)
        self.fail(f"Timed out waiting for {path}")

    def assert_finished(self, process, code):
        stdout, stderr = process.communicate(timeout=8)
        self.assertEqual(process.returncode, code, (stdout, stderr))
        self.assertFalse((self.run / "tailnet.key").exists())
        self.assertFalse((self.run / "worker.token").exists())
        self.assertFalse((self.root / "unexpected-submission").exists())
        self.assertEqual(json.loads((self.runtime / "wrapper-result.json").read_text())["exit_code"], code)

    def test_success_preserves_results_logs_and_unique_one_shot_configuration(self):
        process = self.start("success")
        stdout, _ = process.communicate(timeout=8)
        self.assert_finished(process, 0)
        self.assertIn("retained workload log", stdout)
        self.assertTrue((self.runtime / "job-result.json").exists())
        environment = json.loads((self.runtime / "environment.json").read_text())
        self.assertEqual(environment["WH_DIR"], str(self.runtime))
        self.assertEqual(environment["WH_LAUNCH_MODE"], "run")
        self.assertEqual(environment["WH_SLURM_WORKER_ID"], "test-worker")
        self.assertEqual(environment["WH_SLURM_SETTINGS_FILE"], self.settings["run_path"] + "/settings.json")
        self.assertEqual(environment["TS_AUTHKEY_FILE"], self.settings["run_path"] + "/tailnet.key")
        self.assertEqual(environment["WH_WORKER_TOKEN_FILE"], self.settings["run_path"] + "/worker.token")
        self.assertEqual(json.loads(environment["WH_SHARED_ROOTS_JSON"]), {"test": str(self.root)})
        startup = json.loads((self.runtime / "startup.json").read_text())
        self.assertEqual(startup["deadline_epoch"] - startup["started_at_epoch"], 180)

    def test_multiple_shared_roots_reach_real_launcher_binds_and_manifest(self):
        projects = self.root / "project files"
        environments = projects / "environments"
        environments.mkdir(parents=True)
        alias = self.root / "project-alias"
        alias.symlink_to(projects, target_is_directory=True)
        self.settings["additional_shared_roots"] = [
            {"storage_id": "projects", "shared_root": str(alias)},
            {"storage_id": "envs", "shared_root": str(environments)},
        ]
        self.settings["worker_name"] = "whc-mapping-test"
        self.write_settings()
        shutil.copyfile(ROOT / "start-wh.sh", self.run / "start-wh.sh")
        runtime = self.bin / "apptainer"
        runtime.write_text(
            "#!/usr/bin/env python3\n"
            "import json, os, pathlib, sys\n"
            "args = sys.argv[1:]\n"
            "if args[0] == 'exec':\n"
            "    print('root:x:0:0:root:/root:/bin/bash' if args[-1] == '/etc/passwd' else 'root:x:0:')\n"
            "elif args[0] == 'run':\n"
            "    state = pathlib.Path(os.environ['WH_DIR'])\n"
            "    (state / 'ready').touch()\n"
            "    (state / 'runtime-args.json').write_text(json.dumps(args))\n"
            "    (state / 'environment.json').write_text(json.dumps(dict(os.environ)))\n"
            "else:\n"
            "    sys.exit(1)\n"
        )
        runtime.chmod(0o755)
        self.assert_finished(self.start("success"), 0)
        args = json.loads((self.runtime / "runtime-args.json").read_text())
        binds = [args[index + 1] for index, value in enumerate(args) if value == "--bind"]
        expected = {
            "test": str(self.root),
            "projects": str(projects),
            "envs": str(environments),
        }
        for storage_id, host_root in expected.items():
            self.assertIn(f"{host_root}:/data/shared/{storage_id}", binds)
        manifest = json.loads((self.runtime / "data/bind-paths.json").read_text())
        self.assertEqual(manifest["paths"], ["/data/shared/envs", "/data/shared/projects", "/data/shared/test"])
        environment = json.loads((self.runtime / "environment.json").read_text())
        self.assertEqual(json.loads(environment["WH_SHARED_ROOTS_JSON"]), expected)
        self.assertEqual(environment["WH_SLURM_SETTINGS_FILE"], "/data/shared/test/.wh-slurm/test-worker/settings.json")

    def test_invalid_additional_roots_fail_before_launch(self):
        regular_file = self.root / "regular-file"
        regular_file.touch()
        unsafe = self.root / "unsafe;root"
        unsafe.mkdir()
        alias = self.root / "unsafe-alias"
        alias.symlink_to(unsafe, target_is_directory=True)
        root_alias = self.root / "filesystem-root-alias"
        root_alias.symlink_to("/", target_is_directory=True)
        invalid = [
            None,
            {},
            [None],
            [{"storage_id": "a", "shared_root": str(self.root), "unknown": True}],
            [{"storage_id": "test", "shared_root": str(self.root)}],
            [{"storage_id": "../a", "shared_root": str(self.root)}],
            [{"storage_id": "a", "shared_root": str(self.root)}, {"storage_id": "a", "shared_root": str(self.root)}],
        ]
        invalid.extend(
            [{"storage_id": "a", "shared_root": value}]
            for value in ["/", "relative", str(self.root / "missing"), str(regular_file),
                          str(alias), str(root_alias), str(self.root) + "/../escape",
                          str(self.root) + "/bad\nroot", str(self.root) + "/bad:root"]
        )
        for mappings in invalid:
            with self.subTest(mappings=mappings):
                self.settings["additional_shared_roots"] = mappings
                self.write_settings()
                process = self.start("success")
                _, stderr = process.communicate(timeout=8)
                self.assertNotEqual(process.returncode, 0, stderr)
                self.assertFalse((self.runtime / "launch-started").exists())
                self.assertFalse((self.runtime / "child-started").exists())
                self.assertTrue((self.run / "worker.token").exists())

    def test_additional_mapping_cannot_replace_primary_run_identity(self):
        self.settings["additional_shared_roots"] = [{"storage_id": "archive", "shared_root": str(self.root)}]
        self.settings["run_path"] = "/data/shared/archive/.wh-slurm/test-worker"
        self.write_settings()
        process = self.start("success")
        _, stderr = process.communicate(timeout=8)
        self.assertNotEqual(process.returncode, 0)
        self.assertIn("run_path does not match shared storage mapping", stderr)
        self.assertFalse((self.runtime / "launch-started").exists())

    def test_child_failure_propagates_without_successor(self):
        self.assert_finished(self.start("failure"), 17)

    def test_usr1_persists_workload_checkpoint_then_term_is_sticky(self):
        process = self.start("stubborn")
        self.wait_for(self.runtime / "ready")
        process.send_signal(signal.SIGUSR1)
        self.wait_for(self.runtime / "observed-control.json", lambda value: value["action"] == "checkpoint")
        control = json.loads((self.runtime / "control.json").read_text())
        self.assertEqual(control["signal"], "USR1")
        process.send_signal(signal.SIGTERM)
        self.wait_for(self.runtime / "control.json", lambda value: value["action"] == "stop")
        process.send_signal(signal.SIGUSR1)
        self.assert_finished(process, 143)
        self.assertEqual(json.loads((self.runtime / "control.json").read_text())["action"], "stop")
        descendant = int((self.runtime / "descendant.pid").read_text())
        stat = Path(f"/proc/{descendant}/stat")
        deadline = time.monotonic() + 3
        while stat.exists() and stat.read_text().split()[2] != "Z" and time.monotonic() < deadline:
            time.sleep(0.02)
        self.assertTrue(not stat.exists() or stat.read_text().split()[2] == "Z")

    def test_checkpoint_cannot_overwrite_concurrent_daemon_stop(self):
        process = self.start("stubborn")
        self.wait_for(self.runtime / "ready")
        control = self.runtime / "control.json"
        with (self.runtime / "control.lock").open("a") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            process.send_signal(signal.SIGUSR1)
            time.sleep(0.2)
            self.assertFalse(control.exists(), "wrapper must wait for daemon's control lock")
            temporary = self.runtime / "daemon-control.tmp"
            temporary.write_text(json.dumps({"action": "stop", "signal": "TERM"}))
            temporary.replace(control)
        self.wait_for(self.runtime / "observed-control.json", lambda value: value["action"] == "stop")
        process.send_signal(signal.SIGTERM)
        self.assert_finished(process, 143)
        self.assertEqual(json.loads(control.read_text())["action"], "stop")

    def test_checkpoint_signal_term_is_not_an_allocation_stop(self):
        self.settings["checkpoint_signal"] = "TERM"
        self.write_settings()
        process = self.start()
        self.wait_for(self.runtime / "ready")
        process.send_signal(signal.SIGUSR1)
        self.wait_for(self.runtime / "observed-control.json", lambda value: value["action"] == "checkpoint")
        self.assertEqual(json.loads((self.runtime / "control.json").read_text())["signal"], "TERM")
        self.assertIsNone(process.poll())
        process.send_signal(signal.SIGTERM)
        self.assert_finished(process, 143)

    def test_stop_during_enrollment_is_persisted_and_bounded(self):
        process = self.start("unready")
        self.wait_for(self.runtime / "child-started")
        start = time.monotonic()
        process.send_signal(signal.SIGTERM)
        self.assert_finished(process, 143)
        self.assertLess(time.monotonic() - start, 4)
        self.assertFalse((self.runtime / "ready").exists())
        self.assertEqual(json.loads((self.runtime / "control.json").read_text())["action"], "stop")

    def test_startup_deadline_covers_hanging_enrollment(self):
        # Fake only the clock binary, not the wrapper lifecycle or its child.
        # Once enrollment has started, advance beyond the real 180s policy.
        date = self.bin / "date"
        date.write_text(
            f'#!{sys.executable}\n'
            'import pathlib, time\n'
            f'path = pathlib.Path({str(self.runtime / "child-started")!r})\n'
            'print(int(time.time()) + (181 if path.exists() else 0))\n'
        )
        date.chmod(0o755)
        process = self.start("unready")
        self.assert_finished(process, 124)
        self.assertEqual(json.loads((self.runtime / "control.json").read_text())["action"], "stop")

    def test_duplicate_launch_does_not_disturb_live_credentials_or_child(self):
        first = self.start()
        self.wait_for(self.runtime / "ready")
        second = self.start()
        _, stderr = second.communicate(timeout=8)
        self.assertEqual(second.returncode, 1)
        self.assertIn("duplicate allocation launch", stderr)
        self.assertIsNone(first.poll())
        self.assertTrue((self.run / "tailnet.key").exists())
        self.assertTrue((self.run / "worker.token").exists())
        first.send_signal(signal.SIGTERM)
        self.assert_finished(first, 143)

    def test_requeued_batch_refuses_to_launch(self):
        process = self.start(SLURM_RESTART_COUNT="1")
        _, stderr = process.communicate(timeout=8)
        self.assert_finished(process, 1)
        self.assertIn("Refusing a requeued allocation", stderr)
        self.assertFalse((self.runtime / "child-started").exists())
        self.assertFalse((self.root / "unexpected-submission").exists())

    def test_persisted_prelaunch_stop_never_enrolls(self):
        self.runtime.mkdir()
        (self.runtime / "control.json").write_text(json.dumps({"action": "stop", "signal": "TERM"}))
        self.assert_finished(self.start(), 143)
        self.assertFalse((self.runtime / "child-started").exists())

    def test_packaged_assets_are_deterministic_exact_root_originals(self):
        output = self.root / "assets.json"
        command = [sys.executable, str(ROOT / "scripts/package-slurm-client.py"), "--output", str(output)]
        subprocess.run(command, check=True, capture_output=True, timeout=8)
        first = output.read_bytes()
        subprocess.run(command, check=True, capture_output=True, timeout=8)
        self.assertEqual(output.read_bytes(), first)
        assets = json.loads(first)
        self.assertEqual(assets["version"], 1)
        self.assertEqual(set(assets["assets"]), {"start-wh.sh", "wh-allocation.sh"})
        self.assertEqual(assets["assets"]["start-wh.sh"], (ROOT / "start-wh.sh").read_text())
        self.assertEqual(assets["assets"]["wh-allocation.sh"], WRAPPER.read_text())


if __name__ == "__main__":
    unittest.main()
