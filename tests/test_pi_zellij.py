"""Zellij attachment-tab orchestration tests."""

from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from worker_harness import pi_zellij


class PiZellijTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.runtime = Path(self.directory.name)
        self.environment = patch.dict(pi_zellij.os.environ, {
            "ZELLIJ_SESSION_NAME": "Pi",
            "ZELLIJ_PANE_ID": "12",
            "TMUX": "",
        }, clear=True)
        self.environment.start()
        self.runtime_patch = patch.object(pi_zellij, "_runtime_root", return_value=self.runtime)
        self.runtime_patch.start()

    def tearDown(self) -> None:
        self.runtime_patch.stop()
        self.environment.stop()
        self.directory.cleanup()

    @staticmethod
    def _panes():
        return [{"id": 12, "tab_id": 4, "tab_name": "π ✓ research", "is_plugin": False}]

    def test_current_tab_context_and_marker_round_trip(self):
        with patch.object(pi_zellij, "list_panes", return_value=self._panes()):
            self.assertEqual(pi_zellij.current_tab_context(), (4, "terminal_12"))
            self.assertEqual(pi_zellij.mark_current_attachment("session-1"), (4, "terminal_12"))
            self.assertEqual(pi_zellij.find_attachment_tab("session-1"), 4)
            pi_zellij.unmark_attachment("session-1")
            self.assertIsNone(pi_zellij.find_attachment_tab("session-1"))

    def test_unmark_does_not_delete_replacement_owner(self):
        marker = pi_zellij._session_marker_path("Pi", "session-1")
        pi_zellij._atomic_json(marker, {
            "session_id": "session-1", "tab_id": 4, "pane_id": "terminal_12",
            "pid": 999999, "mode": "stream",
        })
        pi_zellij.unmark_attachment("session-1", pid=123)
        self.assertTrue(marker.exists())

    def test_existing_live_marker_focuses_tab_without_creating(self):
        marker = pi_zellij._session_marker_path("Pi", "session-1")
        pi_zellij._atomic_json(marker, {
            "session_id": "session-1", "tab_id": 4, "pane_id": "terminal_12",
            "pid": pi_zellij.os.getpid(), "mode": "stream",
        })
        completed = subprocess.CompletedProcess([], 0, "", "")
        with (
            patch.object(pi_zellij, "list_panes", return_value=self._panes()),
            patch.object(pi_zellij, "_run_zellij", return_value=completed) as run,
        ):
            tab_id = pi_zellij.open_or_focus_attachment_tab({
                "id": "session-1", "name": "research", "state": "idle",
            })
        self.assertEqual(tab_id, 4)
        run.assert_called_once_with(["go-to-tab-by-id", "4"])

    def test_missing_marker_creates_named_tab_and_launch_reservation(self):
        completed = subprocess.CompletedProcess([], 0, "9\n", "")
        with (
            patch.object(pi_zellij, "list_panes", return_value=[]),
            patch.object(pi_zellij, "_run_zellij", return_value=completed) as run,
            patch.object(pi_zellij, "_wh_executable", return_value="/usr/bin/wh"),
        ):
            tab_id = pi_zellij.open_or_focus_attachment_tab({
                "id": "session-1", "name": "research", "state": "working",
            }, loopback=True)
        self.assertEqual(tab_id, 9)
        command = run.call_args.args[0]
        self.assertEqual(command[:5], ["new-tab", "--name", "π ● research", "--cwd", str(Path.cwd())])
        self.assertIn("--close-on-exit", command)
        self.assertEqual(command[-2:], ["--loopback", "session-1"])
        self.assertIn("--here", command)
        marker = json.loads(
            pi_zellij._session_marker_path("Pi", "session-1").read_text(encoding="utf8")
        )
        self.assertEqual(marker["tab_id"], 9)
        self.assertEqual(marker["mode"], "launching")


if __name__ == "__main__":
    unittest.main()
