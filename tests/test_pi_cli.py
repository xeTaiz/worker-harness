"""CLI coverage for the Phase C Pi session surface."""

from __future__ import annotations

import json
import unittest
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
    def _session() -> dict:
        return {
            "id": "interactive-session-id",
            "session_type": "interactive",
            "state": "idle",
            "name": "repo-agent",
            "task": "",
            "host": "laptop",
            "worker_id": None,
            "cwd": "/repo",
        }

    def test_sessions_text(self):
        with patch.object(pi, "_request", new=AsyncMock(return_value=[self._session()])):
            result = self.runner.invoke(pi.app, ["sessions"])
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("repo-agent", result.output)
        self.assertIn("interactive", result.output)

    def test_sessions_json_and_filter(self):
        _state["output"] = "json"
        rows = [self._session(), {**self._session(), "id": "delegated", "session_type": "delegated"}]
        with patch.object(pi, "_request", new=AsyncMock(return_value=rows)):
            result = self.runner.invoke(pi.app, ["sessions", "--type", "interactive"])
        self.assertEqual(result.exit_code, 0, result.output)
        parsed = json.loads(result.output)
        self.assertEqual([row["id"] for row in parsed], ["interactive-session-id"])

    def test_prompt_uses_steer_delivery(self):
        request = AsyncMock(return_value={"id": "interactive-session-id", "command_id": "command-1"})
        with patch.object(pi, "_request", new=request):
            result = self.runner.invoke(
                pi.app, ["prompt", "interactive-session-id", "continue", "--steer"]
            )
        self.assertEqual(result.exit_code, 0, result.output)
        request.assert_awaited_once_with(
            "POST",
            "/api/v1/pi/sessions/interactive-session-id:prompt",
            {"message": "continue", "deliver_as": "steer"},
        )


if __name__ == "__main__":
    unittest.main()
