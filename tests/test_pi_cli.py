"""CLI coverage for the Phase C Pi session surface."""

from __future__ import annotations

import asyncio
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import ANY, AsyncMock, patch

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

    def test_base_url_reuses_pi_bridge_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / ".pi" / "worker-harness" / "config.json"
            config.parent.mkdir(parents=True)
            config.write_text('{"orchestratorUrl":"http://orchestrator.tail:12889/"}')
            with (
                patch.dict(pi.os.environ, {}, clear=True),
                patch.object(pi.Path, "home", return_value=Path(directory)),
            ):
                self.assertEqual(pi._base_url(), "http://orchestrator.tail:12889")

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

    def test_cycle_order_wraps_in_both_directions(self):
        rows = [
            {**self._session(), "id": "first"},
            {**self._session(), "id": "second"},
            {**self._session(), "id": "third"},
        ]
        self.assertEqual(
            [row["id"] for row in pi._cycle_order(rows, "second", "next")],
            ["third", "first"],
        )
        self.assertEqual(
            [row["id"] for row in pi._cycle_order(rows, "second", "previous")],
            ["first", "third"],
        )

    def test_attach_picker_filters_non_attachable_sessions(self):
        rows = [self._session(), {**self._session(), "id": "offline"}]
        request = AsyncMock(side_effect=[
            {"session_id": "interactive-session-id", "attachable": True},
            {"session_id": "offline", "attachable": False, "reason": "relay offline"},
        ])
        with patch.object(pi, "_request", new=request):
            selected = asyncio.run(pi._attachable_candidates(rows))
        self.assertEqual([row["id"] for row in selected], ["interactive-session-id"])

    def test_attach_picker_maps_full_fzf_row_back_to_session(self):
        rows = [self._session(), {**self._session(), "id": "second", "name": "other"}]
        chosen = subprocess.CompletedProcess([], 0, "second\tidle\tinteractive\tother\tlaptop\t/repo\n", "")
        with (
            patch.object(pi.shutil, "which", return_value="/usr/bin/fzf"),
            patch.object(pi.subprocess, "run", return_value=chosen),
        ):
            selected = pi._pick_session(rows)
        self.assertEqual(selected["id"], "second")

    def test_attach_resolves_prefix_and_streams_protocol_v2(self):
        request = AsyncMock(side_effect=[
            [self._session()],
            {
                "session_id": "interactive-session-id",
                "attachable": True,
                "transport": "direct-interactive-websocket",
                "protocol_version": 2,
                "websocket_url": "ws://100.64.0.2:27888/v1/sessions/interactive-session-id/attach",
            },
        ])
        focus = AsyncMock(return_value=False)
        terminal = AsyncMock(return_value=None)
        with (
            patch.object(pi, "_request", new=request),
            patch("worker_harness.pi_terminal.focus_local_session", new=focus),
            patch("worker_harness.pi_terminal.attach_terminal", new=terminal),
        ):
            result = self.runner.invoke(pi.app, ["attach", "interactive-"])
        self.assertEqual(result.exit_code, 0, result.output)
        focus.assert_awaited_once_with("interactive-session-id")
        terminal.assert_awaited_once_with(
            "ws://100.64.0.2:27888/v1/sessions/interactive-session-id/attach",
            cycle_requests=ANY,
        )
        self.assertEqual(request.await_args_list[1].args, (
            "GET", "/api/v1/pi/sessions/interactive-session-id/attach-info"
        ))

    def test_attach_cycles_to_next_available_session(self):
        first = {**self._session(), "id": "first"}
        second = {**self._session(), "id": "second", "name": "second-agent"}
        request = AsyncMock(side_effect=[
            [first, second],
            {
                "attachable": True,
                "protocol_version": 2,
                "websocket_url": "ws://relay/first",
            },
            [first, second],
            {"attachable": True},
            {
                "attachable": True,
                "protocol_version": 2,
                "websocket_url": "ws://relay/second",
            },
        ])
        terminal = AsyncMock(side_effect=["next", None])
        with (
            patch.object(pi, "_request", new=request),
            patch.object(pi, "_mark_attach_pane"),
            patch("worker_harness.pi_terminal.attach_terminal", new=terminal),
        ):
            result = self.runner.invoke(pi.app, ["attach", "first", "--stream"])
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertEqual(
            [call.args[0] for call in terminal.await_args_list],
            ["ws://relay/first", "ws://relay/second"],
        )

    def test_relative_attach_can_focus_the_next_local_session(self):
        first = {**self._session(), "id": "first"}
        second = {**self._session(), "id": "second"}
        request = AsyncMock(side_effect=[
            [first, second],
            {"attachable": True},
        ])
        focus = AsyncMock(return_value=True)
        with (
            patch.object(pi, "_request", new=request),
            patch.object(pi, "_mark_attach_pane"),
            patch("worker_harness.pi_terminal.focus_local_session", new=focus),
        ):
            result = self.runner.invoke(
                pi.app, ["attach", "first", "--relative", "next"]
            )
        self.assertEqual(result.exit_code, 0, result.output)
        focus.assert_awaited_once_with("second")

    def test_attach_focuses_local_tmux_without_requesting_attach_info(self):
        request = AsyncMock(return_value=[self._session()])
        focus = AsyncMock(return_value=True)
        terminal = AsyncMock(return_value=None)
        with (
            patch.object(pi, "_request", new=request),
            patch("worker_harness.pi_terminal.focus_local_session", new=focus),
            patch("worker_harness.pi_terminal.attach_terminal", new=terminal),
        ):
            result = self.runner.invoke(pi.app, ["attach", "repo-agent"])
        self.assertEqual(result.exit_code, 0, result.output)
        request.assert_awaited_once_with("GET", "/api/v1/pi/sessions")
        terminal.assert_not_awaited()

    def test_attach_reports_unavailable_session(self):
        request = AsyncMock(side_effect=[
            [self._session()],
            {"session_id": "interactive-session-id", "attachable": False, "reason": "relay offline"},
        ])
        with (
            patch.object(pi, "_request", new=request),
            patch("worker_harness.pi_terminal.focus_local_session", new=AsyncMock(return_value=False)),
        ):
            result = self.runner.invoke(pi.app, ["attach", "interactive-session-id"])
        self.assertEqual(result.exit_code, 1)
        self.assertIn("relay offline", result.output)

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
