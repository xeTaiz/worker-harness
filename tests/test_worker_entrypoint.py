"""Run the real bootstrap shell against local Tailnet/process boundaries."""

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ENTRYPOINT = Path(__file__).resolve().parents[1] / "worker_container" / "entrypoint.sh"


class WorkerEntrypointTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.home = self.root / "home"
        self.home.mkdir()
        self.key = self.root / "tailnet.key"
        self.key.write_text("secret-never-in-argv")
        self.env = {
            "PATH": f"{self.bin}:{os.environ['PATH']}",
            "HOME": str(self.home),
            "SSH_USER": "fixture-user",
            "WH_DIR": str(self.root / "allocation" / "runtime"),
            "WH_SLURM_WORKER_ID": self.root.name,
            "WH_SLURM_COMPUTE_KIND": "cpu",
            "TS_AUTHKEY_FILE": str(self.key),
            "TS_HOST": "https://headscale.invalid",
            "TS_HOSTNAME": "whc-" + self.root.name,
            "TEST_ROOT": str(self.root),
            "FAKE_TAILSCALED_FAILURES": "0",
            "CUDA_VISIBLE_DEVICES": "ambient-host-gpu",
            "TS_EXTRA_ARGS": "--advertise-tags=tag:admin",
        }
        self.install("getent", """
import os
import sys
if sys.argv[1] == 'passwd':
    print('fixture-user:x:1000:1000:Fixture:' + os.environ['HOME'] + ':/bin/bash')
else:
    print('fixture-user:x:1000:')
""")
        self.install("python3", f"""
import json
import os
import sys
from pathlib import Path
if sys.argv[1:] == ['/worker_daemon.py']:
    Path(os.environ['TEST_ROOT'], 'daemon-env.json').write_text(json.dumps(dict(os.environ)))
    sys.exit(int(os.environ.get('FAKE_DAEMON_EXIT', '0')))
os.execv({sys.executable!r}, [{sys.executable!r}] + sys.argv[1:])
""")
        self.install("tailscale", """
import json
import os
import sys
from pathlib import Path
with Path(os.environ['TEST_ROOT'], 'tailscale.jsonl').open('a') as log:
    log.write(json.dumps(sys.argv[1:]) + '\\n')
if 'ip' in sys.argv:
    print('100.64.0.50')
""")
        self.install("tailscaled", """
import json
import os
import selectors
import socket
import sys
from pathlib import Path
root = Path(os.environ['TEST_ROOT'])
log = root / 'tailscaled.jsonl'
with log.open('a') as output:
    output.write(json.dumps(sys.argv[1:]) + '\\n')
if os.environ.get('FAKE_FLEET') == '1':
    sys.exit(0)
attempt = len(log.read_text().splitlines())
if attempt <= int(os.environ['FAKE_TAILSCALED_FAILURES']):
    sys.exit(1)
args = dict(value[2:].split('=', 1) for value in sys.argv[1:])
host, port = args['socks5-server'].rsplit(':', 1)
listener = socket.socket()
listener.bind((host, int(port)))
listener.listen()
unix = socket.socket(socket.AF_UNIX)
unix.bind(args['socket'])
unix.listen()
(root / 'tailscaled.pid').write_text(str(os.getpid()))
selector = selectors.DefaultSelector()
selector.register(listener, selectors.EVENT_READ)
selector.register(unix, selectors.EVENT_READ)
while True:
    for key, _ in selector.select():
        connection, _ = key.fileobj.accept()
        connection.close()
""")

    def install(self, name, source):
        target = self.bin / name
        target.write_text(f"#!{sys.executable}\n" + source)
        target.chmod(0o755)

    def tearDown(self):
        self.tempdir.cleanup()

    def bootstrap(self):
        return subprocess.run(
            ["bash", str(ENTRYPOINT)], env=self.env, capture_output=True,
            text=True, timeout=20,
        )

    def records(self, name):
        return [json.loads(line) for line in (self.root / name).read_text().splitlines()]

    def test_cpu_file_auth_ephemeral_ports_and_finite_cleanup(self):
        result = self.bootstrap()

        self.assertEqual(result.returncode, 0, result.stderr)
        calls = self.records("tailscaled.jsonl")
        self.assertEqual(len(calls), 1)
        args = dict(value[2:].split("=", 1) for value in calls[0])
        self.assertEqual(args["port"], "0")
        self.assertTrue(args["state"].startswith(self.env["WH_DIR"] + "/tailscale/state/"))
        self.assertTrue(args["socket"].startswith("/tmp/wh-ts-"))
        self.assertLess(len(args["socket"].encode()), 108)
        host, port = args["socks5-server"].rsplit(":", 1)
        self.assertEqual(host, "127.0.0.1")
        self.assertGreater(int(port), 0)
        up = next(call for call in self.records("tailscale.jsonl") if "up" in call)
        self.assertIn(f"--auth-key=file:{self.key}", up)
        self.assertIn("--advertise-tags=tag:wh-cluster-job", up)
        self.assertNotIn("--advertise-tags=tag:admin", up)
        daemon = json.loads((self.root / "daemon-env.json").read_text())
        self.assertEqual(daemon["CUDA_VISIBLE_DEVICES"], "")
        self.assertEqual(daemon["NVIDIA_VISIBLE_DEVICES"], "void")
        self.assertNotIn("/usr/local/cuda/bin", daemon["PATH"])
        self.assertNotIn("/.singularity.d/libs", daemon.get("LD_LIBRARY_PATH", ""))
        self.assertEqual(daemon["WH_PROXY"], "socks5://" + args["socks5-server"])
        self.assertNotIn("secret-never-in-argv", (self.root / "tailscale.jsonl").read_text() + result.stdout + result.stderr)
        pid = int((self.root / "tailscaled.pid").read_text())
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)

    def test_gpu_restores_original_assignment_and_cuda_paths(self):
        self.env.update(WH_SLURM_COMPUTE_KIND="gpu", WH_SLURM_CUDA_VISIBLE_DEVICES="GPU-a,GPU-b")
        result = self.bootstrap()
        self.assertEqual(result.returncode, 0, result.stderr)
        daemon = json.loads((self.root / "daemon-env.json").read_text())
        self.assertEqual(daemon["CUDA_VISIBLE_DEVICES"], "GPU-a,GPU-b")
        self.assertIn("/usr/local/cuda/bin", daemon["PATH"])
        self.assertIn("/.singularity.d/libs", daemon["LD_LIBRARY_PATH"])

    def test_socks_bind_failure_retries_then_succeeds(self):
        self.env["FAKE_TAILSCALED_FAILURES"] = "2"
        result = self.bootstrap()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(self.records("tailscaled.jsonl")), 3)

    def test_socks_startup_is_bounded_and_never_launches_daemon_on_failure(self):
        self.env["FAKE_TAILSCALED_FAILURES"] = "99"
        result = self.bootstrap()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("after 5 attempts", result.stderr)
        self.assertEqual(len(self.records("tailscaled.jsonl")), 5)
        self.assertFalse((self.root / "daemon-env.json").exists())

    def test_missing_file_auth_fails_without_launch(self):
        self.key.unlink()
        result = self.bootstrap()
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((self.root / "tailscaled.jsonl").exists())

    def test_daemon_failure_is_preserved_and_tailscaled_reaped(self):
        self.env["FAKE_DAEMON_EXIT"] = "7"
        result = self.bootstrap()
        self.assertEqual(result.returncode, 7, result.stderr)
        pid = int((self.root / "tailscaled.pid").read_text())
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)

    def test_fleet_auth_and_network_defaults_are_unchanged(self):
        self.env.pop("WH_SLURM_WORKER_ID")
        self.env.pop("WH_SLURM_COMPUTE_KIND")
        self.env.pop("TS_AUTHKEY_FILE")
        self.env.pop("TS_EXTRA_ARGS")
        self.env.update(TS_AUTHKEY="legacy-test-key", FAKE_FLEET="1")
        result = self.bootstrap()
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = self.records("tailscaled.jsonl")
        self.assertIn("--socks5-server=127.0.0.1:1055", calls[0])
        self.assertNotIn("--port=0", calls[0])
        up = next(call for call in self.records("tailscale.jsonl") if "up" in call)
        self.assertIn("--authkey=legacy-test-key", up)
        self.assertFalse(any(value.startswith("--advertise-tags=") for value in up))
