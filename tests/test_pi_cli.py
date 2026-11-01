"""CLI coverage for the fleet Pi session surface."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from typer.testing import CliRunner

from worker_harness.cli import pi
from worker_harness.cli.app import _state


class PiCliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.runner = CliRunner()
        _state.clear()

    def tearDown(self) -> None:
        _state.clear()

    @staticmethod
    def session() -> dict:
        return {
            "id": "task-session-id",
            "session_type": "interactive",
            "role": "task",
            "state": "idle",
            "name": "task-agent",
            "host": "desktop",
            "cwd": "/repo-worktree",
            "agent": "omp",
        }

    def test_base_url_prefers_environment_then_bridge_configuration(self):
        with patch.dict(pi.os.environ, {"WH_ORCHESTRATOR_URL": "http://fleet:12889/"}):
            self.assertEqual(pi._base_url(), "http://fleet:12889")
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / ".pi" / "worker-harness" / "config.json"
            config.parent.mkdir(parents=True)
            config.write_text('{"orchestratorUrl":"http://bridge:12889/"}')
            with (
                patch.dict(pi.os.environ, {}, clear=True),
                patch.object(pi.Path, "home", return_value=Path(directory)),
            ):
                self.assertEqual(pi._base_url(), "http://bridge:12889")

    def test_headers_require_internal_role_and_token(self):
        with patch.dict(pi.os.environ, {
            "WH_SESSION_ROLE": "task", "WH_SESSION_TOKEN": "secret",
        }, clear=True):
            self.assertEqual(pi._headers(), {"Authorization": "Bearer secret"})
        for credentials in ({}, {"WH_SESSION_TOKEN": "secret"},
                            {"WH_SESSION_ROLE": "task", "WH_API_KEY": "human-secret"},
                            {"WH_API_KEY": "human-secret"}):
            with patch.dict(pi.os.environ, credentials, clear=True):
                with self.assertRaisesRegex(RuntimeError, "bootstrap administrator's web Pi view"):
                    pi._headers()

    def test_human_pi_cli_fails_before_network_request(self):
        with patch.dict(pi.os.environ, {"WH_API_KEY": "human-key"}, clear=True):
            result = self.runner.invoke(pi.app, ["sessions"])
        self.assertEqual(result.exit_code, 1)
        self.assertIn("bootstrap administrator's web Pi view", result.output)

    def test_sessions_text_renders_role(self):
        with patch.object(pi, "_request", new=AsyncMock(return_value=[self.session()])):
            result = self.runner.invoke(pi.app, ["sessions"])
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("task-agent", result.output)
        self.assertIn("task", result.output)

    def test_sessions_json_filters_type_state_and_agent(self):
        _state["output"] = "json"
        rows = [
            self.session(),
            {**self.session(), "id": "pm", "role": "pm", "agent": "pi"},
            {**self.session(), "id": "stopped", "state": "stopped"},
        ]
        with patch.object(pi, "_request", new=AsyncMock(return_value=rows)):
            result = self.runner.invoke(
                pi.app,
                ["sessions", "--type", "interactive", "--state", "idle", "--agent", "omp"],
            )
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertEqual([row["id"] for row in json.loads(result.output)], ["task-session-id"])


    def test_attach_reports_unavailable_session(self):
        request = AsyncMock(side_effect=[
            [self.session()],
            {"attachable": False, "reason": "pane stopped"},
        ])
        with patch.object(pi, "_request", new=request):
            result = self.runner.invoke(pi.app, ["attach", "task-session-id"])
        self.assertEqual(result.exit_code, 1)
        self.assertIn("pane stopped", result.output)

    def test_prompt_uses_role_aware_send_route(self):
        request = AsyncMock(return_value={"command_id": "command-1", "queued": True})
        with patch.object(pi, "_request", new=request):
            result = self.runner.invoke(
                pi.app, ["prompt", "task-session-id", "continue"]
            )
        self.assertEqual(result.exit_code, 0, result.output)
        request.assert_awaited_once_with(
            "POST",
            "/api/v1/pi/sessions/task-session-id:send",
            {"message": "continue"},
        )

    def test_deleted_lifecycle_subcommands_are_not_registered(self):
        help_result = self.runner.invoke(pi.app, ["--help"])
        self.assertEqual(help_result.exit_code, 0, help_result.output)
        for command in ("start", "resume", "cycle", "history-list"):
            self.assertNotIn(command, help_result.output)


if __name__ == "__main__":
    unittest.main()
