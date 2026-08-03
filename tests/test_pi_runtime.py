"""Managed hidden-tmux Pi runtime tests."""

from __future__ import annotations

import asyncio
import shlex
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from worker_harness import pi_runtime


class PiRuntimeTests(unittest.TestCase):
    def test_local_relay_url_validates_port_and_quotes_session(self):
        with patch.dict(pi_runtime.os.environ, {"WH_PI_HOST_RELAY_LOCAL_PORT": "29000"}, clear=True):
            self.assertEqual(
                pi_runtime.local_relay_websocket_url("session/one"),
                "ws://127.0.0.1:29000/v1/sessions/session%2Fone/attach",
            )
        with patch.dict(pi_runtime.os.environ, {"WH_PI_HOST_RELAY_LOCAL_PORT": "nope"}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "must be an integer"):
                pi_runtime.local_relay_websocket_url("session")
        for port in ("0", "65536"):
            with self.subTest(port=port), patch.dict(
                pi_runtime.os.environ, {"WH_PI_HOST_RELAY_LOCAL_PORT": port}, clear=True
            ):
                with self.assertRaisesRegex(RuntimeError, "between 1 and 65535"):
                    pi_runtime.local_relay_websocket_url("session")

    def test_new_session_rejects_resume_and_identity_flags(self):
        for argument in (
            "--session", "--session=abc", "--session-id", "--continue", "-c",
            "--resume", "-r", "--fork=abc", "--no-session", "--name=other", "-n",
        ):
            with self.subTest(argument=argument):
                with self.assertRaisesRegex(RuntimeError, "not supported"):
                    pi_runtime.validate_new_session_args([argument])

    def test_existing_server_creates_one_safely_quoted_window(self):
        completed = subprocess.CompletedProcess([], 0, "%42\n", "")
        with tempfile.TemporaryDirectory() as directory:
            socket = Path(directory) / "pi-tmux.sock"
            cwd = Path(directory) / "work tree"
            cwd.mkdir()
            with (
                patch.object(pi_runtime, "managed_tmux_socket_path", return_value=socket),
                patch.object(pi_runtime, "_session_exists", return_value=True),
                patch.object(pi_runtime, "_configure_managed_server") as configure,
                patch.object(pi_runtime, "_run_tmux", return_value=completed) as run,
            ):
                managed = pi_runtime.start_managed_pi(
                    name="quote ' ; $(touch nope) λ",
                    pi_args=["--model", "openai/test", "hello world", "$(still-nope)"],
                    cwd=cwd,
                    session_id="00000000-0000-4000-8000-000000000001",
                    executable="/opt/Pi Agent/bin/pi",
                )
        self.assertEqual(managed.tmux_pane_id, "%42")
        configure.assert_called_once_with(socket)
        command_args = run.call_args.args
        self.assertEqual(command_args[:2], (socket, "new-window"))
        launched = shlex.split(command_args[-1])
        self.assertEqual(launched, [
            "env",
            "WH_MANAGED_PI=1",
            "/opt/Pi Agent/bin/pi",
            "--session-id",
            "00000000-0000-4000-8000-000000000001",
            "--name",
            "quote ' ; $(touch nope) λ",
            "--model",
            "openai/test",
            "hello world",
            "$(still-nope)",
        ])

    def test_first_window_creates_owner_session_with_initial_dimensions(self):
        completed = subprocess.CompletedProcess([], 0, "%7\n", "")
        with tempfile.TemporaryDirectory() as directory:
            socket = Path(directory) / "pi-tmux.sock"
            with (
                patch.object(pi_runtime, "managed_tmux_socket_path", return_value=socket),
                patch.object(pi_runtime, "_session_exists", return_value=False),
                patch.object(pi_runtime, "_configure_managed_server") as configure,
                patch.object(pi_runtime, "_run_tmux", return_value=completed) as run,
            ):
                managed = pi_runtime.start_managed_pi(
                    name=None,
                    pi_args=[],
                    cwd=Path(directory),
                    rows=51,
                    cols=177,
                    session_id="abcdef12-0000-4000-8000-000000000001",
                    executable="/usr/bin/pi",
                )
        self.assertEqual(managed.name, f"{Path(directory).name}-abcdef12")
        args = run.call_args.args
        self.assertIn("new-session", args)
        self.assertIn("-x", args)
        self.assertIn("177", args)
        self.assertIn("-y", args)
        self.assertIn("51", args)
        new_session_index = args.index("new-session")
        self.assertEqual(args[1], "start-server")
        self.assertLess(args.index("mouse"), new_session_index)
        self.assertLess(args.index("history-limit"), new_session_index)
        self.assertLess(args.index(str(pi_runtime.MANAGED_TMUX_HISTORY_LIMIT)), new_session_index)
        configure.assert_called_once_with(socket)

    def test_concurrent_owner_creation_falls_back_to_new_window(self):
        failed = subprocess.CompletedProcess([], 1, "", "duplicate session")
        created = subprocess.CompletedProcess([], 0, "%13\n", "")
        with tempfile.TemporaryDirectory() as directory:
            socket = Path(directory) / "pi-tmux.sock"
            with (
                patch.object(pi_runtime, "managed_tmux_socket_path", return_value=socket),
                patch.object(pi_runtime, "_session_exists", side_effect=[False, True]),
                patch.object(pi_runtime, "_configure_managed_server") as configure,
                patch.object(pi_runtime, "_run_tmux", side_effect=[failed, created]) as run,
            ):
                managed = pi_runtime.start_managed_pi(
                    name="concurrent",
                    pi_args=[],
                    cwd=Path(directory),
                    session_id="abcdef12-0000-4000-8000-000000000001",
                    executable="/usr/bin/pi",
                )
        self.assertEqual(managed.tmux_pane_id, "%13")
        self.assertEqual(run.call_args_list[0].args[1], "start-server")
        self.assertIn("new-session", run.call_args_list[0].args)
        self.assertEqual(run.call_args_list[1].args[1], "new-window")
        configure.assert_called_once_with(socket)

    def test_managed_server_configures_global_and_owner_options(self):
        completed = subprocess.CompletedProcess([], 0, "", "")
        with patch.object(pi_runtime, "_run_tmux", return_value=completed) as run:
            pi_runtime._configure_managed_server(Path("/tmp/pi.sock"))
        commands = [call.args[1:] for call in run.call_args_list]
        history_limit = str(pi_runtime.MANAGED_TMUX_HISTORY_LIMIT)
        self.assertIn(("set-option", "-g", "status", "off"), commands)
        self.assertIn(("set-option", "-g", "mouse", "on"), commands)
        self.assertIn(("set-option", "-g", "history-limit", history_limit), commands)
        self.assertIn(
            ("set-option", "-t", pi_runtime.MANAGED_TMUX_SESSION, "status", "off"),
            commands,
        )
        self.assertIn(
            ("set-option", "-t", pi_runtime.MANAGED_TMUX_SESSION, "mouse", "on"),
            commands,
        )
        self.assertIn(
            (
                "set-option",
                "-t",
                pi_runtime.MANAGED_TMUX_SESSION,
                "history-limit",
                history_limit,
            ),
            commands,
        )
        self.assertIn(
            ("set-option", "-t", pi_runtime.MANAGED_TMUX_SESSION, "window-size", "latest"),
            commands,
        )

    def test_tmux_command_ignores_user_configuration(self):
        command = pi_runtime._tmux_command(Path("/tmp/pi.sock"), "has-session")
        self.assertEqual(command[:5], [
            "tmux", "-f", "/dev/null", "-S", "/tmp/pi.sock",
        ])

    def test_tmux_environment_strips_outer_multiplexer_context(self):
        with patch.dict(pi_runtime.os.environ, {
            "TMUX": "/tmp/outer,1,0",
            "TMUX_PANE": "%1",
            "ZELLIJ": "0",
            "ZELLIJ_SESSION_NAME": "outer",
            "ZELLIJ_PANE_ID": "4",
            "KEEP": "yes",
        }, clear=True):
            environment = pi_runtime._tmux_environment()
        self.assertEqual(environment, {"KEEP": "yes"})


class PiRuntimeAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_wait_for_route_requires_exact_socket_and_pane(self):
        session = pi_runtime.ManagedPiSession(
            "session-1", "test", Path("/tmp/managed.sock"), "%9"
        )
        relay = AsyncMock(side_effect=[
            OSError("not ready"),
            {
                "ok": True,
                "multiplexer": "tmux",
                "tmux_socket": "/tmp/managed.sock",
                "tmux_pane_id": "%9",
            },
        ])
        with (
            patch("worker_harness.pi_terminal._relay_request", new=relay),
            patch.object(pi_runtime, "managed_pane_is_live", return_value=True),
            patch.object(pi_runtime, "_ROUTE_POLL_SECONDS", 0),
        ):
            route = await pi_runtime.wait_for_managed_route(session, timeout=1)
        self.assertEqual(route["tmux_pane_id"], "%9")
        self.assertEqual(relay.await_count, 2)

    async def test_wait_timeout_reports_running_session_and_manual_attach(self):
        session = pi_runtime.ManagedPiSession(
            "session-1", "test", Path("/tmp/managed.sock"), "%9"
        )
        with (
            patch(
                "worker_harness.pi_terminal._relay_request",
                new=AsyncMock(side_effect=OSError("not ready")),
            ),
            patch.object(pi_runtime, "managed_pane_is_live", return_value=True),
            patch.object(pi_runtime, "_ROUTE_POLL_SECONDS", 0),
        ):
            with self.assertRaisesRegex(
                RuntimeError, r"still running.*wh pi attach session-1"
            ):
                await pi_runtime.wait_for_managed_route(session, timeout=0.01)

    async def test_wait_reports_pi_exit_without_killing_other_windows(self):
        session = pi_runtime.ManagedPiSession(
            "session-1", "test", Path("/tmp/managed.sock"), "%9"
        )
        with (
            patch(
                "worker_harness.pi_terminal._relay_request",
                new=AsyncMock(side_effect=OSError("not ready")),
            ),
            patch.object(pi_runtime, "managed_pane_is_live", return_value=False),
        ):
            with self.assertRaisesRegex(RuntimeError, "Pi exited"):
                await pi_runtime.wait_for_managed_route(session, timeout=1)


if __name__ == "__main__":
    unittest.main()
