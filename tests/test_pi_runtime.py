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
from worker_harness.host_runtime import HostRuntime


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
                patch.object(pi_runtime, "_host_runtime", return_value=None),
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
        command_args = run.call_args_list[0].args
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
        self.assertEqual(
            run.call_args_list[-1].args[1:],
            (
                "set-option", "-p", "-t", "%42", "@wh_pi_session_id",
                "00000000-0000-4000-8000-000000000001",
            ),
        )

    def test_omp_window_omits_pi_identity_flags_and_defers_the_session_id(self):
        completed = subprocess.CompletedProcess([], 0, "%43\n", "")
        with tempfile.TemporaryDirectory() as directory:
            socket = Path(directory) / "pi-tmux.sock"
            with (
                patch.object(pi_runtime, "managed_tmux_socket_path", return_value=socket),
                patch.object(pi_runtime, "_host_runtime", return_value=None),
                patch.object(pi_runtime, "_session_exists", return_value=True),
                patch.object(pi_runtime, "_configure_managed_server"),
                patch.object(pi_runtime, "_run_tmux", return_value=completed) as run,
            ):
                managed = pi_runtime.start_managed_pi(
                    name="omp-probe",
                    pi_args=["hello world"],
                    cwd=Path(directory),
                    executable="/usr/bin/omp",
                    agent="omp",
                )
        self.assertEqual(managed.session_id, "")
        self.assertEqual(managed.name, "omp-probe")
        launched = shlex.split(run.call_args_list[0].args[-1])
        self.assertEqual(launched, [
            "env",
            "WH_MANAGED_PI=1",
            "/usr/bin/omp",
            "hello world",
        ])

    def test_first_window_on_older_tmux_uses_xterm_extended_keys(self):
        completed = subprocess.CompletedProcess([], 0, "%7\n", "")
        with tempfile.TemporaryDirectory() as directory:
            socket = Path(directory) / "pi-tmux.sock"
            with (
                patch.object(pi_runtime, "managed_tmux_socket_path", return_value=socket),
                patch.object(pi_runtime, "_session_exists", return_value=False),
                patch.object(pi_runtime, "_tmux_supports_csi_u", return_value=False),
                patch.object(pi_runtime, "_configure_managed_server"),
                patch.object(pi_runtime, "_run_tmux", return_value=completed) as run,
            ):
                pi_runtime.start_managed_pi(
                    name="older-tmux",
                    pi_args=[],
                    cwd=Path(directory),
                    session_id="abcdef12-0000-4000-8000-000000000001",
                    executable="/usr/bin/pi",
                )
        args = next(call.args for call in run.call_args_list if "new-session" in call.args)
        new_session_index = args.index("new-session")
        self.assertLess(args.index("extended-keys"), new_session_index)
        self.assertNotIn("extended-keys-format", args)
        self.assertNotIn(pi_runtime.MANAGED_TMUX_EXTENDED_KEYS_FORMAT, args)

    def test_first_window_creates_owner_session_with_initial_dimensions(self):
        completed = subprocess.CompletedProcess([], 0, "%7\n", "")
        with tempfile.TemporaryDirectory() as directory:
            socket = Path(directory) / "pi-tmux.sock"
            with (
                patch.object(pi_runtime, "managed_tmux_socket_path", return_value=socket),
                patch.object(pi_runtime, "_session_exists", return_value=False),
                patch.object(pi_runtime, "_tmux_supports_csi_u", return_value=True),
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
        args = next(
            call.args for call in run.call_args_list if "new-session" in call.args
        )
        self.assertIn("new-session", args)
        self.assertIn("-x", args)
        self.assertIn("177", args)
        self.assertIn("-y", args)
        self.assertIn("51", args)
        new_session_index = args.index("new-session")
        self.assertEqual(args[1], "start-server")
        self.assertLess(args.index("mouse"), new_session_index)
        self.assertLess(args.index("set-clipboard"), new_session_index)
        self.assertLess(args.index("external"), new_session_index)
        self.assertLess(args.index("extended-keys"), new_session_index)
        self.assertLess(args.index("extended-keys-format"), new_session_index)
        self.assertLess(args.index(pi_runtime.MANAGED_TMUX_EXTENDED_KEYS_FORMAT), new_session_index)
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
                patch.object(pi_runtime, "_run_tmux", side_effect=[failed, created, created]) as run,
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

    def test_resume_uses_exact_session_without_reassigning_identity_or_name(self):
        created = subprocess.CompletedProcess([], 0, "%21\n", "")
        marked = subprocess.CompletedProcess([], 0, "", "")
        with tempfile.TemporaryDirectory() as directory:
            socket = Path(directory) / "pi-tmux.sock"
            cwd = Path(directory) / "repo"
            cwd.mkdir()
            with (
                patch.object(pi_runtime, "managed_tmux_socket_path", return_value=socket),
                patch.object(pi_runtime, "_host_runtime", return_value=None),
                patch.object(pi_runtime, "_managed_session_id_is_live", return_value=False),
                patch.object(pi_runtime, "_session_exists", return_value=True),
                patch.object(pi_runtime, "_configure_managed_server"),
                patch.object(pi_runtime, "_run_tmux", side_effect=[created, marked]) as run,
            ):
                managed = pi_runtime.resume_managed_pi(
                    session_id="exact-history-id",
                    name="Stored name",
                    cwd=cwd,
                    executable="/usr/bin/pi",
                )
        launched = shlex.split(run.call_args_list[0].args[-1])
        self.assertEqual(launched, [
            "env", "WH_MANAGED_PI=1", "/usr/bin/pi", "--session", "exact-history-id",
        ])
        self.assertNotIn("--session-id", launched)
        self.assertNotIn("--name", launched)
        self.assertEqual(
            run.call_args_list[1].args[1:],
            ("set-option", "-p", "-t", "%21", "@wh_pi_session_id", "exact-history-id"),
        )
        self.assertEqual(managed.session_id, "exact-history-id")
        self.assertEqual(managed.name, "Stored name")

    def test_resume_refuses_id_already_live_in_managed_runtime(self):
        with tempfile.TemporaryDirectory() as directory:
            socket = Path(directory) / "pi-tmux.sock"
            with (
                patch.object(pi_runtime, "managed_tmux_socket_path", return_value=socket),
                patch.object(pi_runtime, "_managed_session_id_is_live", return_value=True),
            ):
                with self.assertRaisesRegex(RuntimeError, "already active"):
                    pi_runtime.resume_managed_pi(
                        session_id="exact-history-id",
                        name="Stored",
                        cwd=Path(directory),
                        executable="/usr/bin/pi",
                    )

    def test_managed_server_configures_global_and_owner_options(self):
        completed = subprocess.CompletedProcess([], 0, "", "")
        with (
            patch.object(pi_runtime, "_tmux_supports_csi_u", return_value=True),
            patch.object(pi_runtime, "_run_tmux", return_value=completed) as run,
        ):
            pi_runtime._configure_managed_server(Path("/tmp/pi.sock"))
        commands = [call.args[1:] for call in run.call_args_list]
        history_limit = str(pi_runtime.MANAGED_TMUX_HISTORY_LIMIT)
        self.assertIn(("set-option", "-g", "status", "off"), commands)
        self.assertIn(("set-option", "-g", "mouse", "on"), commands)
        self.assertIn(("set-option", "-s", "set-clipboard", "external"), commands)
        self.assertIn(("set-option", "-g", "extended-keys", "on"), commands)
        self.assertIn(
            (
                "set-option", "-g", "extended-keys-format",
                pi_runtime.MANAGED_TMUX_EXTENDED_KEYS_FORMAT,
            ),
            commands,
        )
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

    def test_managed_server_uses_xterm_extended_keys_on_older_tmux(self):
        completed = subprocess.CompletedProcess([], 0, "", "")
        with (
            patch.object(pi_runtime, "_tmux_supports_csi_u", return_value=False),
            patch.object(pi_runtime, "_run_tmux", return_value=completed) as run,
        ):
            pi_runtime._configure_managed_server(Path("/tmp/pi.sock"))
        commands = [call.args[1:] for call in run.call_args_list]
        self.assertIn(("set-option", "-g", "extended-keys", "on"), commands)
        self.assertFalse(any("extended-keys-format" in command for command in commands))

    def test_tmux_csi_u_support_requires_version_3_5(self):
        for version, supported in (
            ("tmux 3.2a\n", False),
            ("tmux 3.4\n", False),
            ("tmux 3.5\n", True),
            ("tmux 3.6a\n", True),
            ("unexpected\n", False),
        ):
            with self.subTest(version=version), patch.object(
                pi_runtime.subprocess,
                "run",
                return_value=subprocess.CompletedProcess([], 0, version, ""),
            ), patch.object(pi_runtime, "_tmux_executable", return_value="tmux"), patch.object(
                pi_runtime, "_tmux_environment", return_value={}
            ):
                self.assertEqual(pi_runtime._tmux_supports_csi_u(), supported)

    def test_real_tmux_accepts_managed_extended_key_options(self):
        executable = pi_runtime.shutil.which("tmux")
        if not executable:
            self.skipTest("tmux is not installed")
        with tempfile.TemporaryDirectory() as directory, patch.object(
            pi_runtime, "_host_runtime", return_value=None
        ):
            socket = Path(directory) / "pi.sock"
            created = subprocess.run(
                [
                    executable,
                    "-f", "/dev/null", "-S", str(socket),
                    "new-session", "-d", "-s", pi_runtime.MANAGED_TMUX_SESSION,
                    "sleep 30",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
            )
            self.assertEqual(created.returncode, 0, created.stderr)
            try:
                pi_runtime._configure_managed_server(socket)
                extended = subprocess.run(
                    [executable, "-S", str(socket), "show", "-gv", "extended-keys"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    check=False,
                )
                self.assertEqual(extended.returncode, 0, extended.stderr)
                self.assertEqual(extended.stdout.strip(), "on")
                clipboard = subprocess.run(
                    [executable, "-S", str(socket), "show", "-sv", "set-clipboard"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    check=False,
                )
                self.assertEqual(clipboard.returncode, 0, clipboard.stderr)
                self.assertEqual(clipboard.stdout.strip(), "external")
                if pi_runtime._tmux_supports_csi_u():
                    key_format = subprocess.run(
                        [
                            executable, "-S", str(socket), "show", "-gv",
                            "extended-keys-format",
                        ],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                        check=False,
                    )
                    self.assertEqual(key_format.returncode, 0, key_format.stderr)
                    self.assertEqual(
                        key_format.stdout.strip(),
                        pi_runtime.MANAGED_TMUX_EXTENDED_KEYS_FORMAT,
                    )
            finally:
                subprocess.run(
                    [executable, "-S", str(socket), "kill-server"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=False,
                )

    def test_tmux_command_ignores_user_configuration(self):
        command = pi_runtime._tmux_command(Path("/tmp/pi.sock"), "has-session")
        self.assertTrue(command[0].endswith("/tmux") or command[0] == "tmux")
        self.assertEqual(command[1:5], [
            "-f", "/dev/null", "-S", "/tmp/pi.sock",
        ])

    def test_manifest_pins_tmux_pi_and_managed_path(self):
        runtime = HostRuntime(
            path=("/opt/pi/bin", "/opt/node/bin", "/usr/bin"),
            executables={
                "wh": "/opt/wh/bin/wh",
                "pi": "/opt/pi/bin/pi",
                "bun": "/opt/bun/bin/bun",
                "node": "/opt/node/bin/node",
                "tmux": "/opt/tmux/bin/tmux",
                "tailscale": "/opt/tailscale/bin/tailscale",
                "zellij": None,
            },
            _schema_version=1,
            _generated_at="2026-08-05T00:00:00+00:00",
        )
        completed = subprocess.CompletedProcess([], 0, "%42\n", "")
        with tempfile.TemporaryDirectory() as directory:
            socket = Path(directory) / "pi-tmux.sock"
            cwd = Path(directory) / "repo"
            cwd.mkdir()
            with (
                patch.object(pi_runtime, "_host_runtime", return_value=runtime),
                patch.object(pi_runtime, "managed_tmux_socket_path", return_value=socket),
                patch.object(pi_runtime, "_session_exists", return_value=True),
                patch.object(pi_runtime, "_configure_managed_server"),
                patch.object(pi_runtime, "_run_tmux", return_value=completed) as run,
            ):
                pi_runtime.start_managed_pi(
                    name="manifest",
                    pi_args=[],
                    cwd=cwd,
                    session_id="00000000-0000-4000-8000-000000000001",
                )
        launched = shlex.split(run.call_args_list[0].args[-1])
        self.assertIn("PATH=/opt/pi/bin:/opt/node/bin:/usr/bin", launched)
        self.assertIn("/opt/pi/bin/pi", launched)
        with patch.object(pi_runtime, "_host_runtime", return_value=runtime):
            self.assertEqual(
                pi_runtime._tmux_command(Path("/tmp/pi.sock"), "has-session")[0],
                "/opt/tmux/bin/tmux",
            )
            environment = pi_runtime._tmux_environment()
        self.assertEqual(environment["PATH"], "/opt/pi/bin:/opt/node/bin:/usr/bin")

    def test_tmux_environment_strips_outer_multiplexer_context(self):
        with patch.dict(pi_runtime.os.environ, {
            "TMUX": "/tmp/outer,1,0",
            "TMUX_PANE": "%1",
            "ZELLIJ": "0",
            "ZELLIJ_SESSION_NAME": "outer",
            "ZELLIJ_PANE_ID": "4",
            "KEEP": "yes",
        }, clear=True), patch.object(pi_runtime, "_host_runtime", return_value=None):
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

    async def test_ensure_route_resolves_agent_chosen_session_id(self):
        session = pi_runtime.ManagedPiSession(
            "", "omp-probe", Path("/tmp/managed.sock"), "%9"
        )
        locate = AsyncMock(return_value="chosen-session")
        wait = AsyncMock(return_value={"multiplexer": "tmux"})
        with (
            patch.object(pi_runtime, "_locate_managed_session_id", new=locate),
            patch.object(pi_runtime, "wait_for_managed_route", new=wait),
        ):
            resolved, route = await pi_runtime.ensure_managed_route(session, timeout=3)
        self.assertEqual(resolved.session_id, "chosen-session")
        self.assertEqual(route["multiplexer"], "tmux")
        locate.assert_awaited_once_with(session, timeout=3)
        wait.assert_awaited_once_with(resolved, timeout=3)

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
                RuntimeError, r"still running.*wh attach session-1"
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
