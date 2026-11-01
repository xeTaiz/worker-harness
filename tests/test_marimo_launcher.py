import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from worker_harness.marimo import (
    build_launch_command, delete_local_token, local_token_path, proxy_base_path,
    remote_token_path, upload_remote_token, write_local_token,
)
from worker_harness.models import Worker
from worker_harness.ssh import SSHResult


REPO_ROOT = Path(__file__).resolve().parents[1]
LAUNCHER = REPO_ROOT / "worker_container" / "wh-marimo-launch"


class MarimoLauncherTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.bin_dir = self.root / "bin"
        self.bin_dir.mkdir()
        self.argv_file = self.root / "uv-argv.json"
        fake_uv = self.bin_dir / "uv"
        fake_uv.write_text(
            "#!/usr/bin/env python3\n"
            "import json, os, sys\n"
            "with open(os.environ['UV_ARGV_FILE'], 'w') as output:\n"
            "    json.dump(sys.argv[1:], output)\n"
        )
        fake_uv.chmod(0o755)

        self.venv = self.root / "project-venv"
        (self.venv / "bin").mkdir(parents=True)
        self.python = self.venv / "bin" / "python"
        self.python.write_text("#!/bin/sh\nexit 0\n")
        self.python.chmod(0o755)

        self.project = self.root / "project"
        self.project.mkdir()
        self.token_file = self.root / "native.token"
        self.token_file.write_text("secret-native-token")
        self.base_url = "/api/v1/marimo/test-session/proxy"
        self.env = os.environ.copy()
        self.env.update(
            PATH=f"{self.bin_dir}:{self.env['PATH']}",
            UV_ARGV_FILE=str(self.argv_file),
        )

    def tearDown(self):
        self.tempdir.cleanup()

    def run_launcher(self, *args):
        return subprocess.run(
            [str(LAUNCHER), *map(str, args)],
            cwd=REPO_ROOT,
            env=self.env,
            capture_output=True,
            text=True,
        )

    def launch_args(self, notebook, environment=None, port="43123"):
        return (
            "--notebook",
            notebook,
            "--environment",
            environment or self.venv,
            "--port",
            port,
            "--token-file",
            self.token_file,
            "--base-url",
            self.base_url,
        )

    def test_uses_overlay_and_required_loopback_server_options(self):
        notebook = self.project / "notebooks" / "analysis.py"
        result = self.run_launcher(*self.launch_args(notebook))

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(notebook.parent.is_dir())
        argv = json.loads(self.argv_file.read_text())
        self.assertEqual(
            argv,
            [
                "run",
                "--no-project",
                "--python",
                str(self.python),
                "--with",
                "marimo==0.23.16",
                "marimo",
                "-q",
                "edit",
                str(notebook),
                "--token",
                "--token-password-file",
                str(self.token_file),
                "--base-url",
                self.base_url,
                "--host",
                "127.0.0.1",
                "--port",
                "43123",
                "--headless",
            ],
        )

    def test_accepts_python_executable_as_environment(self):
        notebook = self.project / "analysis.py"
        result = self.run_launcher(*self.launch_args(notebook, self.python))

        self.assertEqual(result.returncode, 0, result.stderr)
        argv = json.loads(self.argv_file.read_text())
        self.assertEqual(argv[argv.index("--python") + 1], str(self.python))

    def test_rejects_relative_and_newline_paths(self):
        for option, value in (
            ("--notebook", "relative.py"),
            ("--environment", "relative-venv"),
            ("--notebook", f"{self.project}/bad\nname.py"),
            ("--environment", f"{self.venv}\nbad"),
        ):
            args = list(self.launch_args(self.project / "analysis.py"))
            args[args.index(option) + 1] = value
            with self.subTest(option=option, value=value):
                result = self.run_launcher(*args)
                self.assertEqual(result.returncode, 2)

    def test_rejects_invalid_ports(self):
        for port in ("0", "65536", "12x", "-1"):
            with self.subTest(port=port):
                result = self.run_launcher(*self.launch_args(self.project / "analysis.py", port=port))
                self.assertEqual(result.returncode, 2)

    def test_rejects_missing_or_non_executable_target_python(self):
        missing_venv = self.root / "missing-venv"
        missing_venv.mkdir()
        result = self.run_launcher(*self.launch_args(self.project / "analysis.py", missing_venv))
        self.assertEqual(result.returncode, 2)

        self.python.chmod(0o644)
        result = self.run_launcher(*self.launch_args(self.project / "analysis.py", self.python))
        self.assertEqual(result.returncode, 2)

    def test_rejects_missing_token_and_invalid_proxy_paths(self):
        for option, value in (
            ("--token-file", self.root / "missing.token"),
            ("--token-file", "relative.token"),
            ("--token-file", f"{self.root}/../native.token"),
            ("--base-url", "/api/v1/marimo/other/session/proxy"),
            ("--base-url", "https://example.test/proxy"),
        ):
            args = list(self.launch_args(self.project / "analysis.py"))
            args[args.index(option) + 1] = value
            with self.subTest(option=option, value=value):
                result = self.run_launcher(*args)
                self.assertEqual(result.returncode, 2)
        self.assertFalse(self.argv_file.exists())

    def test_does_not_create_missing_project_roots(self):
        notebook = self.root / "missing-project" / "notebooks" / "analysis.py"
        result = self.run_launcher(*self.launch_args(notebook))

        self.assertEqual(result.returncode, 2)
        self.assertFalse((self.root / "missing-project").exists())
        self.assertFalse(self.argv_file.exists())


class MarimoTokenTests(unittest.IsolatedAsyncioTestCase):
    def test_local_token_permissions_and_exclusive_creation(self):
        with tempfile.TemporaryDirectory() as directory:
            path = local_token_path(Path(directory) / "db.sqlite", "test-session")
            write_local_token(path, "private-token")
            self.assertEqual(path.read_text(), "private-token")
            for parent in (path.parent, path.parent.parent):
                self.assertEqual(parent.stat().st_mode & 0o777, 0o700)
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            with self.assertRaises(FileExistsError):
                write_local_token(path, "replacement")
            delete_local_token(path)
            path.parent.mkdir(mode=0o700)
            path.symlink_to(Path(directory) / "victim")
            with self.assertRaises(FileExistsError):
                write_local_token(path, "replacement")

    def test_command_has_paths_not_secrets_and_validates_prefix(self):
        command = build_launch_command(
            notebook_path="/project/analysis.py", environment="/project/.venv", port=1234,
            token_file="/private/native.token", base_url=proxy_base_path("test-session"),
        )
        self.assertIn("--token-file /private/native.token", command)
        self.assertNotIn("private-token", command)
        for token_file, base_url in (
            ("relative", proxy_base_path("test-session")),
            ("/private/../native.token", proxy_base_path("test-session")),
            ("/private//native.token", proxy_base_path("test-session")),
            ("/private/native.token", "/foreign/proxy"),
        ):
            with self.assertRaises(ValueError):
                build_launch_command(
                    notebook_path="/project/analysis.py", environment="/project/.venv",
                    port=1234, token_file=token_file, base_url=base_url,
                )

    async def test_upload_secrets_only_in_stdin(self):
        worker = Worker(id="worker", name="worker", worker_ip="127.0.0.1", harness_dir="/private/harness")
        result = SSHResult(returncode=0, stdout="", stderr="")
        with patch("worker_harness.marimo.async_ssh_run", new=AsyncMock(return_value=result)) as prepare:
            with patch("worker_harness.marimo.ssh_upload_bytes", new=AsyncMock(return_value=result)) as upload:
                self.assertEqual(await upload_remote_token(worker, "test-session", "private-token"), result)
        self.assertNotIn("private-token", prepare.call_args.args[1])
        self.assertIn("umask 077", prepare.call_args.args[1])
        upload.assert_awaited_once_with(worker, b"private-token", remote_token_path(worker, "test-session"))


if __name__ == "__main__":
    unittest.main()
