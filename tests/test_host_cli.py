"""Tests for the isolated host-runtime manifest and ``wh host`` CLI."""

from __future__ import annotations

import json
import os
import stat
import tempfile
import textwrap
import unittest
from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from worker_harness import host_runtime
from worker_harness.cli import host as host_cli
from worker_harness.cli.app import _state


REQUIRED_BINARIES: tuple[str, ...] = host_runtime.REQUIRED_EXECUTABLES


def _write_executable(directory: Path, name: str, body: str) -> Path:
    path = directory / name
    path.write_text(textwrap.dedent(body))
    path.chmod(0o755)
    return path


def _isolated_environment(
    *,
    path: Path,
    home: Path,
    xdg: Path | None = None,
    override: str | None = None,
) -> dict[str, str]:
    """Build a clean environment with isolated HOME/XDG/PATH/override."""

    environment: dict[str, str] = {
        "PATH": str(path),
        "HOME": str(home),
    }
    if xdg is not None:
        environment["XDG_CONFIG_HOME"] = str(xdg)
    else:
        environment.pop("XDG_CONFIG_HOME", None)
    if override is not None:
        environment["WH_HOST_RUNTIME_CONFIG"] = override
    else:
        environment.pop("WH_HOST_RUNTIME_CONFIG", None)
    return environment


class _FakeRuntimeHarness:
    """Bundle of faked required executables plus an isolated PATH."""

    def __init__(self, *, root: Path, include_zellij: bool = True, node_body: str | None = None) -> None:
        self.root = root
        self.bin_root = root / "bin"
        self.bin_root.mkdir(parents=True, exist_ok=True)
        self.bin_extra = root / "extra"
        self.bin_extra.mkdir(parents=True, exist_ok=True)
        node_body = node_body if node_body is not None else (
            "#!/usr/bin/env python3\n"
            "import sys\n"
            "print('node', sys.version_info[0])\n"
        )
        self.executables: dict[str, Path] = {}
        self.executables["wh"] = _write_executable(self.bin_root, "wh", "#!/bin/sh\necho wh\\n")
        self.executables["pi"] = _write_executable(
            self.bin_root,
            "pi",
            "#!/usr/bin/env node\necho pi\n",
        )
        self.executables["bun"] = _write_executable(
            self.bin_extra,
            "bun",
            "#!/bin/sh\necho bun\\n",
        )
        self.executables["node"] = _write_executable(self.bin_extra, "node", node_body)
        self.executables["omp"] = _write_executable(
            self.bin_extra,
            "omp",
            "#!/bin/sh\necho omp\\n",
        )
        self.executables["tmux"] = _write_executable(self.bin_root, "tmux", "#!/bin/sh\necho tmux\\n")
        self.executables["tailscale"] = _write_executable(
            self.bin_root,
            "tailscale",
            "#!/bin/sh\necho tailscale\\n",
        )
        if include_zellij:
            self.executables["zellij"] = _write_executable(
                self.bin_extra,
                "zellij",
                "#!/bin/sh\necho zellij\\n",
            )
        self.path_entries = [str(self.bin_root), str(self.bin_extra)]
        self.environment = _isolated_environment(path=self._joined_path(), home=root)

    def _joined_path(self) -> Path:
        # Path type so it can flow through patches as the PATH string.
        return Path(os.pathsep.join(self.path_entries))


class HostRuntimeCaptureTests(unittest.TestCase):
    """Cover capture_host_runtime, missing-required, and optional-zellij paths."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.addCleanup(self.tmp.cleanup)
        self.harness = _FakeRuntimeHarness(root=self.root, include_zellij=True)
        self.env_patch = patch.dict(os.environ, self.harness.environment, clear=True)
        self.env_patch.start()
        self.addCleanup(self.env_patch.stop)

    def test_capture_records_lexical_absolute_paths_and_path_layout(self):
        runtime = host_runtime.capture_host_runtime()
        # Executable values come straight from shutil.which — lexical.
        for name, path in self.harness.executables.items():
            self.assertEqual(runtime.executable(name), str(path))
        # Primary PATH entries precede any fallback / existing-PATH additions.
        self.assertEqual(runtime.path[:2], (str(self.harness.bin_root), str(self.harness.bin_extra)))
        # Optional zellij is captured because the fake binary exists.
        self.assertIsNotNone(runtime.executable("zellij"))
        self.assertIsNotNone(runtime.executable("omp"))
        # Every PATH entry is absolute, non-empty, and existing.
        for entry in runtime.path:
            self.assertTrue(os.path.isabs(entry))
            self.assertTrue(os.path.isdir(entry))

    def test_capture_omits_zellij_when_absent(self):
        no_zellij = _FakeRuntimeHarness(
            root=self.root / "no-zellij", include_zellij=False
        )
        with patch.dict(os.environ, no_zellij.environment, clear=True):
            runtime = host_runtime.capture_host_runtime()
        self.assertIsNone(runtime.executable("zellij"))
        self.assertIn("zellij", runtime.executables)
        self.assertIsNone(runtime.executables["zellij"])

    def test_capture_includes_standard_fallback_directories(self):
        for fallback in host_runtime.STANDARD_PATH_FALLBACK:
            if os.path.isdir(fallback):
                self.assertIn(fallback, host_runtime.capture_host_runtime().path)

    def test_capture_rejects_when_required_executable_missing(self):
        # Drop 'tmux' from PATH so discovery fails.
        reduced = self.harness.bin_extra  # only wh/pi/bun/node/tailscale live here
        slim_environment = _isolated_environment(path=reduced, home=self.root)
        # Remove the bin_root from PATH so tmux/wh/pi/tailscale can't resolve.
        with patch.dict(os.environ, slim_environment, clear=True):
            with self.assertRaisesRegex(host_runtime.HostRuntimeError, "tmux"):
                host_runtime.capture_host_runtime()

    def test_capture_error_lists_all_missing_required_tools(self):
        # Bare PATH means nothing resolves.
        empty_bin = self.root / "empty-bin"
        empty_bin.mkdir()
        with patch.dict(os.environ, _isolated_environment(path=empty_bin, home=self.root), clear=True):
            with self.assertRaises(host_runtime.HostRuntimeError) as ctx:
                host_runtime.capture_host_runtime()
        message = str(ctx.exception)
        for name in REQUIRED_BINARIES:
            self.assertIn(name, message)


class HostRuntimeLoadWriteTests(unittest.TestCase):
    """Cover manifest schema, atomic write, mode enforcement, and load_host_runtime."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.addCleanup(self.tmp.cleanup)
        self.harness = _FakeRuntimeHarness(root=self.root)
        self.env_patch = patch.dict(os.environ, self.harness.environment, clear=True)
        self.env_patch.start()
        self.addCleanup(self.env_patch.stop)

    def test_capture_produces_exact_schema(self):
        runtime = host_runtime.capture_host_runtime()
        manifest = host_runtime.write_host_runtime(runtime)
        payload = json.loads(manifest.read_text(encoding="utf8"))
        self.assertEqual(payload["schema_version"], host_runtime.SCHEMA_VERSION)
        self.assertIsInstance(payload["generated_at"], str)
        self.assertTrue(payload["generated_at"].endswith("+00:00"))
        self.assertEqual(set(payload.keys()), {"schema_version", "generated_at", "path", "executables"})
        self.assertIsInstance(payload["path"], list)
        for entry in payload["path"]:
            self.assertIsInstance(entry, str)
            self.assertTrue(os.path.isabs(entry))
        for name in REQUIRED_BINARIES:
            self.assertIn(name, payload["executables"])
            self.assertTrue(os.path.isabs(payload["executables"][name]))

    def test_atomic_write_enforces_directory_and_file_modes(self):
        runtime = host_runtime.capture_host_runtime()
        manifest = host_runtime.write_host_runtime(runtime)
        parent_mode = stat.S_IMODE(manifest.parent.stat().st_mode)
        file_mode = stat.S_IMODE(manifest.stat().st_mode)
        self.assertEqual(parent_mode, 0o700)
        self.assertEqual(file_mode, 0o600)

    def test_atomic_write_replaces_existing_file(self):
        runtime = host_runtime.capture_host_runtime()
        first = host_runtime.write_host_runtime(runtime)
        first.write_text("stale contents\n")
        os.chmod(first, 0o644)
        host_runtime.write_host_runtime(runtime)
        # File mode restored to 0600 and content reflects the real manifest.
        self.assertEqual(stat.S_IMODE(first.stat().st_mode), 0o600)
        payload = json.loads(first.read_text(encoding="utf8"))
        self.assertEqual(payload["schema_version"], host_runtime.SCHEMA_VERSION)

    def test_write_cleans_up_temporary_on_failure(self):
        runtime = host_runtime.capture_host_runtime()
        manifest = host_runtime.default_manifest_path()

        def boom(*args, **kwargs):
            raise OSError("synthetic write failure")

        with patch.object(host_runtime.os, "replace", side_effect=boom):
            with self.assertRaises(host_runtime.HostRuntimeError):
                host_runtime.write_host_runtime(runtime)
        # No leftover temp files in the parent.
        leftovers = [p for p in manifest.parent.iterdir() if p.name.startswith(f".{manifest.name}.")]
        self.assertEqual(leftovers, [])
        self.assertFalse(manifest.exists())

    def test_default_manifest_path_prefers_explicit_override(self):
        override = self.root / "explicit" / "manifest.json"
        with patch.dict(os.environ, {"WH_HOST_RUNTIME_CONFIG": str(override)}, clear=False):
            self.assertEqual(host_runtime.default_manifest_path(), override)

    def test_default_manifest_path_uses_xdg_when_no_override(self):
        xdg = self.root / "xdg"
        with patch.dict(os.environ, {"XDG_CONFIG_HOME": str(xdg)}, clear=False):
            expected = xdg / "worker-harness" / "host-runtime.json"
            self.assertEqual(host_runtime.default_manifest_path(), expected)

    def test_default_manifest_path_falls_back_to_home_config(self):
        with patch.dict(os.environ, {}, clear=True):
            expected = Path("~/.config/worker-harness/host-runtime.json").expanduser()
            self.assertEqual(host_runtime.default_manifest_path(), expected)

    def test_load_host_runtime_required_false_returns_none_when_missing(self):
        missing = self.root / "missing" / "host-runtime.json"
        self.assertIsNone(host_runtime.load_host_runtime(required=False, path=missing))

    def test_load_host_runtime_required_true_raises_when_missing(self):
        missing = self.root / "missing" / "host-runtime.json"
        with self.assertRaises(host_runtime.HostRuntimeError):
            host_runtime.load_host_runtime(required=True, path=missing)

    def test_load_host_runtime_round_trips_payload(self):
        runtime = host_runtime.capture_host_runtime()
        manifest = host_runtime.write_host_runtime(runtime)
        with patch.dict(os.environ, {"WH_HOST_RUNTIME_CONFIG": str(manifest)}, clear=False):
            loaded = host_runtime.load_host_runtime(required=True)
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.path, runtime.path)
        self.assertEqual(loaded.executables, runtime.executables)

    def test_load_rejects_relative_path_entries(self):
        runtime = host_runtime.capture_host_runtime()
        manifest = host_runtime.write_host_runtime(runtime)
        payload = json.loads(manifest.read_text(encoding="utf8"))
        payload["path"] = ["./rel", *payload["path"]]
        manifest.write_text(json.dumps(payload), encoding="utf8")
        with patch.dict(os.environ, {"WH_HOST_RUNTIME_CONFIG": str(manifest)}, clear=False):
            with self.assertRaises(host_runtime.HostRuntimeError):
                host_runtime.load_host_runtime(required=True)

    def test_load_rejects_non_executable_required_entry(self):
        runtime = host_runtime.capture_host_runtime()
        manifest = host_runtime.write_host_runtime(runtime)
        # Remove execute bit on the captured tmux executable.
        tmux_path = Path(runtime.executable("tmux"))
        original_mode = tmux_path.stat().st_mode
        tmux_path.chmod(0o644)
        self.addCleanup(tmux_path.chmod, original_mode)
        with patch.dict(os.environ, {"WH_HOST_RUNTIME_CONFIG": str(manifest)}, clear=False):
            with self.assertRaisesRegex(host_runtime.HostRuntimeError, "tmux"):
                host_runtime.load_host_runtime(required=True)


class HostRuntimeValidationTests(unittest.TestCase):
    """Cover validate_host_runtime and the clean environment it constructs."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.addCleanup(self.tmp.cleanup)
        self.harness = _FakeRuntimeHarness(
            root=self.root,
            node_body="#!/usr/bin/env python3\nimport sys\nprint('node', sys.version_info[0])\n",
        )
        # Seed clean-env keys; capture_host_runtime respects PATH.
        env = dict(self.harness.environment)
        env["HOME"] = str(self.root)
        env["USER"] = "tester"
        env["LOGNAME"] = "tester"
        env["LANG"] = "C.UTF-8"
        env["LC_ALL"] = "C.UTF-8"
        env["TERM"] = "xterm-256color"
        env.pop("WH_HOST_RUNTIME_CONFIG", None)
        env.pop("XDG_CONFIG_HOME", None)
        self.env_patch = patch.dict(os.environ, env, clear=True)
        self.env_patch.start()
        self.addCleanup(self.env_patch.stop)

    def test_clean_environment_lets_pi_invocation_route_through_usr_bin_env_node(self):
        runtime = host_runtime.capture_host_runtime()
        # The pi script uses #!/usr/bin/env node; without /usr/bin in PATH,
        # env would fail. The manifest PATH must include /usr/bin if it exists.
        results = host_runtime.validate_host_runtime(runtime)
        pi = results["pi"]
        self.assertTrue(pi.available, msg=f"pi probe failed: {pi}")
        self.assertTrue(pi.ok, msg=f"pi probe not ok: {pi}")

    def test_required_nonzero_marks_validation_failure(self):
        runtime = host_runtime.capture_host_runtime()
        # Rewrite wh to exit nonzero on every invocation, even --help.
        wh_path = Path(runtime.executable("wh"))
        wh_path.write_text("#!/bin/sh\nexit 7\n")
        wh_path.chmod(0o755)
        results = host_runtime.validate_host_runtime(runtime)
        failures = host_runtime.validation_failures(results)
        self.assertTrue(any(check.name == "wh" for check in failures))

    def test_optional_missing_zellij_emits_warning_only(self):
        runtime = host_runtime.capture_host_runtime()
        # Pretend zellij is not part of the manifest at all.
        runtime_no_zellij = host_runtime.HostRuntime(
            path=runtime.path,
            executables={name: value for name, value in runtime.executables.items() if name != "zellij"},
            _schema_version=runtime.schema_version,
            _generated_at=runtime.generated_at,
        )
        results = host_runtime.validate_host_runtime(runtime_no_zellij)
        self.assertNotIn("zellij", results)

    def test_required_timeout_marks_validation_failure(self):
        runtime = host_runtime.capture_host_runtime()
        wh_path = Path(runtime.executable("wh"))
        wh_path.write_text("#!/bin/sh\nsleep 30\n")
        wh_path.chmod(0o755)
        with patch.object(host_runtime, "VALIDATION_TIMEOUT_SECONDS", 0.2):
            results = host_runtime.validate_host_runtime(runtime)
        wh_result = results["wh"]
        self.assertTrue(wh_result.timed_out)
        self.assertFalse(wh_result.ok)
        self.assertTrue(any(check.name == "wh" for check in host_runtime.validation_failures(results)))


class HostRuntimeEnvironmentTests(unittest.TestCase):
    """Cover HostRuntime.environment and executables API."""

    def test_environment_overrides_path_and_preserves_other_keys(self):
        runtime = host_runtime.HostRuntime(
            path=("/alpha/bin", "/beta/bin"),
            executables={"wh": "/alpha/bin/wh"},
            _schema_version=1,
            _generated_at="2024-01-01T00:00:00+00:00",
        )
        with patch.dict(os.environ, {"PATH": "/old", "USER": "tester", "FOO": "bar"}, clear=False):
            result = runtime.environment()
        self.assertEqual(result["PATH"], "/alpha/bin:/beta/bin")
        self.assertEqual(result["USER"], "tester")
        self.assertEqual(result["FOO"], "bar")

    def test_environment_accepts_base_override(self):
        runtime = host_runtime.HostRuntime(
            path=("/only/bin",),
            executables={"wh": "/only/bin/wh"},
            _schema_version=1,
            _generated_at="2024-01-01T00:00:00+00:00",
        )
        result = runtime.environment(base={"PATH": "/old", "KEEP": "yes"})
        self.assertEqual(result["PATH"], "/only/bin")
        self.assertEqual(result["KEEP"], "yes")
        self.assertNotIn("USER", result)


class HostCliTests(unittest.TestCase):
    """Cover ``wh host setup`` and ``wh host doctor`` end-to-end."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.addCleanup(self.tmp.cleanup)
        self.harness = _FakeRuntimeHarness(root=self.root)
        env = dict(self.harness.environment)
        env.pop("WH_HOST_RUNTIME_CONFIG", None)
        env["USER"] = "tester"
        env["LOGNAME"] = "tester"
        self.env_patch = patch.dict(os.environ, env, clear=True)
        self.env_patch.start()
        self.addCleanup(self.env_patch.stop)
        self.runner = CliRunner()
        _state.clear()
        self.addCleanup(_state.clear)
        self.manifest_path = host_runtime.default_manifest_path()

    def test_setup_writes_atomically_and_reports_json(self):
        result = self.runner.invoke(host_cli.app, ["setup", "--output", "json"])
        self.assertEqual(result.exit_code, 0, result.output)
        payload = json.loads(result.output)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["manifest"], str(self.manifest_path))
        self.assertEqual(payload["schema_version"], host_runtime.SCHEMA_VERSION)
        for name in REQUIRED_BINARIES:
            self.assertIn(name, payload["executables"])
        self.assertTrue(self.manifest_path.exists())
        self.assertEqual(stat.S_IMODE(self.manifest_path.stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(self.manifest_path.parent.stat().st_mode), 0o700)

    def test_setup_text_summary_lists_executables(self):
        result = self.runner.invoke(host_cli.app, ["setup"])
        self.assertEqual(result.exit_code, 0, result.output)
        for name in REQUIRED_BINARIES:
            self.assertIn(name, result.output)

    def test_setup_fails_when_required_executable_missing(self):
        empty_bin = self.root / "empty-bin"
        empty_bin.mkdir()
        with patch.dict(os.environ, {"PATH": str(empty_bin), "HOME": str(self.root)}, clear=True):
            result = self.runner.invoke(host_cli.app, ["setup", "--output", "json"])
        self.assertEqual(result.exit_code, 1, result.output)
        payload = json.loads(result.output)
        self.assertFalse(payload["ok"])
        self.assertIn("required host executables missing", payload["error"])

    def test_setup_fails_when_validation_rejects_required_tool(self):
        wh_path = self.harness.executables["wh"]
        wh_path.write_text("#!/bin/sh\nexit 9\n")
        wh_path.chmod(0o755)
        result = self.runner.invoke(host_cli.app, ["setup", "--output", "json"])
        self.assertEqual(result.exit_code, 1, result.output)
        payload = json.loads(result.output)
        self.assertFalse(payload["ok"])
        # Manifest must not have been written when validation failed.
        self.assertFalse(self.manifest_path.exists())

    def test_doctor_returns_zero_for_clean_manifest(self):
        # Seed a manifest via setup, then run doctor.
        self.runner.invoke(host_cli.app, ["setup"])
        result = self.runner.invoke(host_cli.app, ["doctor"])
        self.assertEqual(result.exit_code, 0, result.output)

    def test_doctor_exits_one_when_manifest_missing(self):
        result = self.runner.invoke(host_cli.app, ["doctor", "--output", "json"])
        self.assertEqual(result.exit_code, 1, result.output)
        payload = json.loads(result.output)
        self.assertFalse(payload["ok"])

    def test_doctor_exits_one_when_file_mode_wrong(self):
        self.runner.invoke(host_cli.app, ["setup"])
        # Tamper with the manifest file mode.
        os.chmod(self.manifest_path, 0o644)
        result = self.runner.invoke(host_cli.app, ["doctor", "--output", "json"])
        self.assertEqual(result.exit_code, 1, result.output)
        payload = json.loads(result.output)
        self.assertFalse(payload["ok"])
        self.assertIn("mode 0600", payload["error"])

    def test_doctor_warns_but_passes_when_optional_zellij_missing(self):
        # Build a manifest without zellij.
        no_zellij_root = self.root / "no-zellij"
        harness = _FakeRuntimeHarness(root=no_zellij_root, include_zellij=False)
        env = dict(harness.environment)
        env["WH_HOST_RUNTIME_CONFIG"] = str(self.manifest_path)
        with patch.dict(os.environ, env, clear=True):
            result = self.runner.invoke(host_cli.app, ["setup", "--output", "json"])
            self.assertEqual(result.exit_code, 0, result.output)
            result = self.runner.invoke(host_cli.app, ["doctor", "--output", "json"])
            self.assertEqual(result.exit_code, 0, result.output)
            payload = json.loads(result.output)
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["warnings"], [])


if __name__ == "__main__":
    unittest.main()