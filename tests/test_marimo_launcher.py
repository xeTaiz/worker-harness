import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path


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
                "edit",
                str(notebook),
                "--host",
                "127.0.0.1",
                "--port",
                "43123",
                "--headless",
                "--no-token",
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

    def test_does_not_create_missing_project_roots(self):
        notebook = self.root / "missing-project" / "notebooks" / "analysis.py"
        result = self.run_launcher(*self.launch_args(notebook))

        self.assertEqual(result.returncode, 2)
        self.assertFalse((self.root / "missing-project").exists())
        self.assertFalse(self.argv_file.exists())


if __name__ == "__main__":
    unittest.main()
