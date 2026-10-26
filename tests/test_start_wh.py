"""Launcher bind configuration exercised without starting a real container."""

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path


LAUNCHER = Path(__file__).resolve().parents[1] / "start-wh.sh"


class LauncherTestCase(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.home = self.root / "home"
        self.home.mkdir()
        self.bin_dir = self.root / "bin"
        self.bin_dir.mkdir()
        self.argv_file = self.root / "runtime-argv.json"
        runtime = self.bin_dir / "fake-runtime"
        runtime.write_text(
            "#!/usr/bin/env python3\n"
            "import json, os, sys\n"
            "args = sys.argv[1:]\n"
            "if args[0] == 'exec':\n"
            "    if args[-1] == '/etc/passwd':\n"
            "        print('root:x:0:0:root:/root:/bin/bash')\n"
            "    else:\n"
            "        print('root:x:0:')\n"
            "elif args[0] == 'run':\n"
            "    with open(os.environ['RUNTIME_ARGV_FILE'], 'w') as output:\n"
            "        json.dump(args, output)\n"
            "else:\n"
            "    sys.exit(1)\n"
        )
        runtime.chmod(0o755)
        findmnt = self.bin_dir / "findmnt"
        findmnt.write_text("#!/bin/sh\nexit 0\n")
        findmnt.chmod(0o755)
        overlay = self.root / "overlay.ext3"
        overlay.touch()
        env_file = self.root / "empty.env"
        env_file.touch()
        self.env = {
            "PATH": f"{self.bin_dir}:{os.environ['PATH']}",
            "HOME": str(self.home),
            "WH_RUNTIME": str(runtime),
            "WH_ENV_FILE": str(env_file),
            "WH_LAUNCH_MODE": "run",
            "WH_DIR": str(self.root / "state"),
            "WH_IMAGE": str(self.root / "worker.sif"),
            "WH_OVERLAY": str(overlay),
            "WH_IBEX_USER_ROOT": "",
            "WH_IBEX_PROJECTS": "",
            "TS_AUTHKEY": "test-only",
            "ORCHESTRATOR_HOST": "test-only.invalid",
            "RUNTIME_ARGV_FILE": str(self.argv_file),
        }

    def tearDown(self):
        self.tempdir.cleanup()

    def run_launcher(self):
        return subprocess.run(
            ["bash", str(LAUNCHER)],
            cwd=self.root,
            env=self.env,
            capture_output=True,
            text=True,
            timeout=15,
        )

    def binds(self):
        argv = json.loads(self.argv_file.read_text())
        return [argv[index + 1] for index, value in enumerate(argv) if value == "--bind"]


class SharedStorageLauncherTests(LauncherTestCase):
    def test_shared_mapping_reuses_manifest_and_preserves_unrelated_binds(self):
        shared = self.root / "shared files"
        shared.mkdir()
        alias = self.root / "shared-alias"
        alias.symlink_to(shared, target_is_directory=True)
        unrelated = self.root / "other"
        unrelated.mkdir()
        self.env["WH_SHARED_ROOTS_JSON"] = json.dumps({"ibex": str(alias), "archive": str(shared)})
        self.env["WH_EXTRA_BINDS"] = f"{shared}:/data/shared/ibex;{unrelated}:/custom:ro"

        result = self.run_launcher()

        self.assertEqual(result.returncode, 0, result.stderr)
        binds = self.binds()
        self.assertEqual(binds.count(f"{shared}:/data/shared/ibex"), 1)
        self.assertIn(f"{shared}:/data/shared/archive", binds)
        self.assertIn(f"{unrelated}:/custom:ro", binds)
        manifest = json.loads((self.root / "state/data/bind-paths.json").read_text())
        self.assertEqual(manifest["paths"], ["/custom", "/data/shared/archive", "/data/shared/ibex"])

    def test_mapping_overrides_same_legacy_storage_id_only(self):
        configured = self.root / "configured"
        legacy = self.root / "legacy"
        projects = self.root / "projects"
        for directory in (configured, legacy, projects / "c2324"):
            directory.mkdir(parents=True)
        self.env.update(
            WH_SHARED_ROOTS_JSON=json.dumps({"ibex": str(configured)}),
            WH_IBEX_USER_ROOT=str(legacy),
            WH_IBEX_PROJECT_ROOT=str(projects),
            WH_IBEX_PROJECTS="c2324",
        )

        result = self.run_launcher()

        self.assertEqual(result.returncode, 0, result.stderr)
        binds = self.binds()
        self.assertIn(f"{configured}:/data/shared/ibex", binds)
        self.assertNotIn(f"{legacy}:/data/shared/ibex", binds)
        self.assertIn(f"{legacy}:/code", binds)
        self.assertIn(f"{projects / 'c2324'}:/data/shared/ibex_c2324", binds)

    def test_unset_or_empty_mapping_preserves_existing_extra_binds(self):
        self.env["WH_EXTRA_BINDS"] = f"{self.root}:/data/shared/existing"

        for raw in (None, "", "{}"):
            with self.subTest(raw=raw):
                if raw is None:
                    self.env.pop("WH_SHARED_ROOTS_JSON", None)
                else:
                    self.env["WH_SHARED_ROOTS_JSON"] = raw
                result = self.run_launcher()

                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn(f"{self.root}:/data/shared/existing", self.binds())

    def test_invalid_mapping_stops_before_container_run(self):
        regular_file = self.root / "not-a-directory"
        regular_file.touch()
        invalid = [
            "{broken",
            "[]",
            "null",
            json.dumps({"../escape": str(self.root)}),
            json.dumps({"bad.id": str(self.root)}),
            json.dumps({"ibex": None}),
            json.dumps({"ibex": "relative"}),
            json.dumps({"ibex": str(self.root / "missing")}),
            json.dumps({"ibex": str(regular_file)}),
            json.dumps({"ibex": str(self.root / "bad;path")}),
            json.dumps({"ibex": str(self.root / "bad:path")}),
            json.dumps({"ibex": str(self.root / "bad,path")}),
            json.dumps({"ibex": str(self.root / "bad\npath")}),
            '{"ibex":"/tmp","ibex":"/tmp"}',
        ]
        for raw in invalid:
            with self.subTest(raw=raw):
                self.env["WH_SHARED_ROOTS_JSON"] = raw
                result = self.run_launcher()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("WH_SHARED_ROOTS_JSON", result.stderr)
                self.assertFalse(self.argv_file.exists())

    def test_rejects_symlink_target_with_bind_delimiter(self):
        target = self.root / "bad;target"
        target.mkdir()
        alias = self.root / "alias"
        alias.symlink_to(target, target_is_directory=True)
        self.env["WH_SHARED_ROOTS_JSON"] = json.dumps({"ibex": str(alias)})

        result = self.run_launcher()

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("WH_SHARED_ROOTS_JSON", result.stderr)
        self.assertFalse(self.argv_file.exists())


class SlurmLauncherTests(LauncherTestCase):
    def configure_allocation(self, kind="cpu", worker_id="reserved-worker"):
        shared = self.root / "shared"
        run = shared / ".wh-slurm" / worker_id
        runtime = run / "runtime"
        runtime.mkdir(parents=True, exist_ok=True)
        container_run = f"/data/shared/test/.wh-slurm/{worker_id}"
        (run / "settings.json").write_text(json.dumps({
            "worker_id": worker_id,
            "worker_name": "whc-profile-request",
            "compute_kind": kind,
            "host_run_path": str(run),
            "run_path": container_run,
        }))
        (run / "tailnet.key").write_text("secret-tailnet-credential")
        (run / "worker.token").write_text("secret-worker-credential")
        self.env.update(
            WH_SLURM_WORKER_ID=worker_id,
            WH_SLURM_SETTINGS_FILE=f"{container_run}/settings.json",
            WH_DIR=str(runtime),
            WH_SHARED_ROOTS_JSON=json.dumps({"test": str(shared)}),
            WH_LAUNCH_MODE="instance",
            TS_AUTHKEY="ambient-secret-must-not-be-used",
        )
        return run, container_run

    def runtime_env(self):
        argv = json.loads(self.argv_file.read_text())
        return dict(argv[index + 1].split("=", 1) for index, value in enumerate(argv) if value == "--env")

    def test_cpu_isolated_launch_ignores_ambient_environment_and_overlay(self):
        run, container_run = self.configure_allocation()
        ambient_marker = self.root / "ambient-was-sourced"
        Path(self.env["WH_ENV_FILE"]).write_text(f"touch '{ambient_marker}'\n")
        self.env["CUDA_VISIBLE_DEVICES"] = "0,1"
        extra = self.root / "extra"
        extra.mkdir()
        self.env["WH_EXTRA_BINDS"] = f"{extra}:/custom:ro"
        self.env["WH_OVERLAY"] = str(self.root / "not-created.ext3")

        result = self.run_launcher()

        self.assertEqual(result.returncode, 0, result.stderr)
        argv = json.loads(self.argv_file.read_text())
        environment = self.runtime_env()
        self.assertEqual(argv[0], "run")
        self.assertIn("--cleanenv", argv)
        self.assertIn("--contain", argv)
        self.assertIn("--no-mount=cwd,hostfs,bind-paths", argv)
        self.assertNotIn("--nv", argv)
        self.assertNotIn("--overlay", argv)
        self.assertFalse(ambient_marker.exists())
        self.assertFalse(Path(self.env["WH_OVERLAY"]).exists())
        self.assertEqual(environment["CUDA_VISIBLE_DEVICES"], "")
        self.assertEqual(environment["WH_SLURM_CUDA_VISIBLE_DEVICES"], "")
        self.assertEqual(environment["NVIDIA_VISIBLE_DEVICES"], "void")
        self.assertEqual(environment["WH_DIR"], f"{container_run}/runtime")
        self.assertEqual(environment["TS_HOSTNAME"], "whc-reserved-worker")
        self.assertEqual(environment["WORKER_NAME"], "whc-profile-request")
        self.assertNotIn("TS_AUTHKEY", environment)
        self.assertIn(f"{run / 'tailnet.key'}:{container_run}/tailnet.key:ro", self.binds())
        self.assertIn(f"{run / 'worker.token'}:{container_run}/worker.token:ro", self.binds())
        self.assertIn(f"{run.parents[1]}:/data/shared/test", self.binds())
        self.assertIn(f"{extra}:/custom:ro", self.binds())
        self.assertNotIn("/code", [bind.split(":")[1] for bind in self.binds()])
        for secret in ("secret-tailnet-credential", "secret-worker-credential", "ambient-secret-must-not-be-used"):
            self.assertNotIn(secret, self.argv_file.read_text() + result.stdout + result.stderr)

    def test_gpu_preserves_assigned_visibility_through_cleanenv(self):
        self.configure_allocation("gpu")
        self.env["CUDA_VISIBLE_DEVICES"] = "GPU-1234,GPU-5678"

        result = self.run_launcher()

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("--nv", json.loads(self.argv_file.read_text()))
        environment = self.runtime_env()
        self.assertEqual(environment["CUDA_VISIBLE_DEVICES"], "GPU-1234,GPU-5678")
        self.assertEqual(environment["WH_SLURM_CUDA_VISIBLE_DEVICES"], "GPU-1234,GPU-5678")

    def test_gpu_rejects_missing_assignment_instead_of_exposing_host(self):
        self.configure_allocation("gpu")
        result = self.run_launcher()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("original assigned CUDA visibility", result.stderr)
        self.assertFalse(self.argv_file.exists())

    def test_missing_secret_stops_before_launch(self):
        run, _ = self.configure_allocation()
        (run / "worker.token").unlink()
        result = self.run_launcher()
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.argv_file.exists())

    def test_colocated_allocations_have_distinct_identity_and_runtime_binds(self):
        self.configure_allocation(worker_id="first")
        first = self.run_launcher()
        self.assertEqual(first.returncode, 0, first.stderr)
        first_env, first_binds = self.runtime_env(), self.binds()

        self.configure_allocation(worker_id="second")
        second = self.run_launcher()
        self.assertEqual(second.returncode, 0, second.stderr)
        second_env, second_binds = self.runtime_env(), self.binds()
        self.assertNotEqual(first_env["WH_DIR"], second_env["WH_DIR"])
        self.assertNotEqual(first_env["TS_HOSTNAME"], second_env["TS_HOSTNAME"])
        self.assertNotEqual(first_env["TS_AUTHKEY_FILE"], second_env["TS_AUTHKEY_FILE"])
        self.assertNotEqual(first_binds[0], second_binds[0])
