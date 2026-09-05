import base64
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from worker_harness.herdr_host import encode_arg


SHIM = Path(__file__).resolve().parents[1] / "scripts" / "wh-remote-shim"


class RemoteShimTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.home = Path(self.tmp.name) / "home"
        self.home.mkdir()
        self.cache = self.home / ".cache" / "worker-harness"
        self.cache.mkdir(parents=True)
        self.bin = self.home / ".local" / "bin"
        self.bin.mkdir(parents=True)
        self.record = self.home / "executed.json"
        self.env = {
            **os.environ, "HOME": str(self.home),
            "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_SYSTEM": os.devnull,
        }

    def capture_binary(self, name):
        binary = self.bin / name
        binary.write_text(
            f"#!{sys.executable}\n"
            "import json, sys\n"
            f"from pathlib import Path\nPath({str(self.record)!r}).write_text(json.dumps(sys.argv[1:]))\n"
        )
        binary.chmod(0o700)

    def run_shim(self, *args, content=None, raw=None):
        command = raw if raw is not None else " ".join(encode_arg(str(arg)) for arg in args)
        return subprocess.run(
            ["bash", str(SHIM)], input=content, capture_output=True,
            env={**self.env, "SSH_ORIGINAL_COMMAND": command}, timeout=5,
        )

    def assert_refused(self, result):
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn(b"refused", result.stderr)
        self.assertFalse(self.record.exists(), "an invalid operand reached an executable")

    def test_option_operands_never_reach_git_or_gh(self):
        self.capture_binary("git")
        self.capture_binary("gh")
        repo = self.home / "repo"
        commands = [
            ("push", repo, "--mirror"),
            ("branch-delete", repo, "-r"),
            ("worktree-add", repo, "--orphan", "main", self.cache / "tree"),
            ("worktree-add", repo, "topic", "--detach", self.cache / "tree"),
            ("pr-create", "owner/repo", "--head", "main", self.cache / "title", self.cache / "body"),
            ("pr-create", "owner/repo", "topic", "--base", self.cache / "title", self.cache / "body"),
        ]
        for command in commands:
            with self.subTest(command=command):
                self.assert_refused(self.run_shim(*command))
        encoded_option = "b64:" + base64.b64encode(b"--mirror").decode()
        self.assert_refused(self.run_shim(raw=f"push {repo} {encoded_option}"))

    def test_decoded_paths_cannot_traverse_or_escape_through_symlinks(self):
        outside = Path(self.tmp.name) / "outside"
        outside.mkdir()
        victim = outside / "victim"
        victim.write_bytes(b"unchanged")
        (self.cache / "escape").symlink_to(outside, target_is_directory=True)
        (self.home / "escape").symlink_to(outside, target_is_directory=True)
        for command in [
            ("write-file", self.cache / "escape" / "victim"),
            ("rm-file", self.cache / "escape" / "victim"),
            ("tail", self.home / "escape" / "victim", "1"),
            ("write-file", f"{self.cache}/../../victim"),
            ("rm-file", f"{self.cache}/."),
        ]:
            with self.subTest(command=command):
                self.assert_refused(self.run_shim(*command, content=b"changed"))
                self.assertEqual(victim.read_bytes(), b"unchanged")
        self.assertTrue(self.cache.is_dir())

    def test_encoded_paths_support_spaces_and_private_file_creation(self):
        path = self.cache / "session space" / "env"
        result = self.run_shim("write-file", path, content=b"secret\n")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(path.read_bytes(), b"secret\n")
        self.assertEqual(path.stat().st_mode & 0o077, 0)
        result = self.run_shim("tail", path, "2")
        self.assertEqual(result.stdout, b"ecret\n")

    def test_base64_preserves_literal_prefix_empty_and_trailing_newlines(self):
        self.capture_binary("herdr")
        payloads = ["b64:YWJj", "", "echo ../repo\n\n", "π\n"]
        result = self.run_shim("herdr", "pane", "run", "pane-1", *payloads)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(self.record.read_text())[5:], payloads)

    def test_malformed_base64_and_nul_are_refused(self):
        self.capture_binary("herdr")
        for token in ["b64:%%%", "b64:YQ==junk", "b64:YQA="]:
            with self.subTest(token=token):
                self.assert_refused(self.run_shim(raw=f"herdr pane run p {token}"))
        with self.assertRaises(ValueError):
            encode_arg("a\0b")

    def test_real_git_worktree_push_and_delete_operands(self):
        repo = self.home / "repo"
        remote = self.home / "remote.git"
        tree = self.cache / "worktree space"

        def git(*args):
            return subprocess.run(
                ["git", *map(str, args)], check=True, capture_output=True, env=self.env,
                timeout=5,
            )

        git("init", "-b", "main", repo)
        git("-C", repo, "-c", "user.name=Test", "-c", "user.email=test@example.com",
            "commit", "--allow-empty", "-m", "initial")
        git("init", "--bare", remote)
        git("-C", repo, "remote", "add", "origin", remote)
        for command in [
            ("worktree-add", repo, "topic/test", "main", tree),
            ("push", tree, "topic/test"),
            ("worktree-remove", repo, tree),
            ("branch-delete", repo, "topic/test"),
        ]:
            result = self.run_shim(*command)
            self.assertEqual(result.returncode, 0, result.stderr)
        git("-C", remote, "show-ref", "--verify", "refs/heads/topic/test")
        self.assertFalse(tree.exists())
        self.assertEqual(git("-C", repo, "branch", "--list", "topic/test").stdout, b"")
