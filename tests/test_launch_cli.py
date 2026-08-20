from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import httpx
import typer
from typer.testing import CliRunner

from worker_harness.cli import launch


class LaunchInventoryTests(unittest.TestCase):
    def test_parse_orders_standard_then_workers_and_excludes_other_tags(self):
        status = {
            "MagicDNSSuffix": "hs.example",
            "Self": {
                "HostName": "local",
                "DNSName": "local.hs.example.",
                "TailscaleIPs": ["100.64.0.1"],
                "OS": "linux",
            },
            "Peer": {
                "remote": {
                    "HostName": "remote-host",
                    "DNSName": "camel.hs.example.",
                    "TailscaleIPs": ["100.64.0.4"],
                    "Online": True,
                    "OS": "linux",
                },
                "offline": {
                    "HostName": "offline-host",
                    "DNSName": "offline.hs.example.",
                    "TailscaleIPs": ["100.64.0.9"],
                    "Online": False,
                    "OS": "macOS",
                },
                "worker": {
                    "HostName": "remote-host",
                    "DNSName": "worker-remote.hs.example.",
                    "TailscaleIPs": ["100.64.0.44"],
                    "Online": True,
                    "OS": "linux",
                    "Tags": ["tag:wh-worker"],
                },
                "orchestrator": {
                    "HostName": "orchestrator",
                    "DNSName": "orchestrator.hs.example.",
                    "TailscaleIPs": ["100.64.0.125"],
                    "Online": True,
                    "Tags": ["tag:wh-orchestrator"],
                },
            },
        }
        machines = launch.parse_launch_machines(status, [{
            "worker_ip": "100.64.0.44",
            "ssh_user": "worker-user",
        }])
        self.assertEqual([machine.alias for machine in machines], [
            "local", "camel", "offline", "worker-remote",
        ])
        self.assertTrue(machines[0].local)
        self.assertFalse(machines[2].online)
        self.assertTrue(machines[3].worker)
        self.assertEqual(machines[3].ssh_user, "worker-user")

    def test_resolve_machine_accepts_alias_dns_and_ip_but_rejects_ambiguity(self):
        standard = launch.LaunchMachine(
            "camel.hs.example", "KW", "camel.hs.example", "camel",
            ("100.64.0.4",), "linux", True, False, False,
        )
        worker = launch.LaunchMachine(
            "kw.hs.example", "KW", "kw.hs.example", "kw",
            ("100.64.0.44",), "linux", True, True, False,
        )
        self.assertIs(launch.resolve_machine([standard, worker], "camel"), standard)
        self.assertIs(launch.resolve_machine([standard, worker], "100.64.0.44"), worker)
        with self.assertRaisesRegex(RuntimeError, "ambiguous"):
            launch.resolve_machine([standard, worker], "KW")
        with self.assertRaisesRegex(RuntimeError, "invalid"):
            launch.resolve_machine([standard], "-oProxyCommand=bad")

    def test_picker_uses_selectable_multiline_section_records(self):
        machines = [
            launch.LaunchMachine(
                "local.hs.example", "local", "local.hs.example", "local",
                ("100.64.0.1",), "linux", True, False, True,
            ),
            launch.LaunchMachine(
                "camel.hs.example", "KW", "camel.hs.example", "camel",
                ("100.64.0.4",), "linux", True, False, False,
            ),
            launch.LaunchMachine(
                "worker.hs.example", "KW", "worker.hs.example", "worker",
                ("100.64.0.44",), "linux", True, True, False,
            ),
        ]

        def choose_worker(command, **kwargs):
            records = kwargs["input"].rstrip("\0").split("\0")
            return subprocess.CompletedProcess(command, 0, records[-1] + "\0", "")

        with (
            patch.object(launch.shutil, "which", return_value="/usr/bin/fzf"),
            patch.object(launch.subprocess, "run", side_effect=choose_worker) as run,
        ):
            selected = launch.pick_machine(machines)

        self.assertEqual(selected.alias, "worker")
        command = run.call_args.args[0]
        self.assertIn("--read0", command)
        self.assertIn("--print0", command)
        self.assertIn("--bind=load:pos(1)", command)
        self.assertNotIn("--no-sort", command)
        records = run.call_args.kwargs["input"].rstrip("\0").split("\0")
        self.assertIn("Standard machines\n  ├─", records[0])
        self.assertNotIn("Standard machines\n", records[1])
        self.assertIn("wh-worker machines\n  └─", records[2])


class LaunchCommandTests(unittest.TestCase):
    @staticmethod
    def remote_machine(**changes) -> launch.LaunchMachine:
        values = {
            "key": "camel.hs.example",
            "hostname": "KW",
            "dns_name": "camel.hs.example",
            "alias": "camel",
            "addresses": ("100.64.0.4",),
            "os_name": "linux",
            "online": True,
            "worker": False,
            "local": False,
            "ssh_user": "",
        }
        values.update(changes)
        return launch.LaunchMachine(**values)

    def test_ssh_destination_prefers_explicit_then_worker_user(self):
        worker = self.remote_machine(worker=True, ssh_user="engeld")
        self.assertEqual(launch.ssh_destination(worker), "engeld@camel")
        self.assertEqual(launch.ssh_destination(worker, "override"), "override@camel")
        self.assertEqual(launch.ssh_destination(self.remote_machine()), "camel")

    def test_ssh_failure_reports_destination_phase_and_duration(self):
        with (
            patch.object(launch, "ssh_command", return_value=["ssh", "camel", "remote"]),
            patch.object(
                launch,
                "_run_command",
                side_effect=launch.RemoteCommandError("permission denied"),
            ),
        ):
            with self.assertRaisesRegex(
                launch.RemoteCommandError,
                r"destination=camel phase=start duration=.*permission denied",
            ):
                launch._run_ssh_phase(
                    "camel", "remote", phase="start", timeout=10
                )

    def test_remote_directory_query_parses_home_and_dev_children(self):
        completed = subprocess.CompletedProcess(
            [], 0, b"/home/u\0/home/u/Dev/A\0/home/u/Dev/B C\0", b""
        )
        captured: dict[str, str] = {}

        def command(_destination, remote, **_kwargs):
            captured["remote"] = remote
            return ["ssh", "camel", remote]

        with (
            patch.object(launch, "ssh_command", side_effect=command),
            patch.object(launch, "_run_command", return_value=completed),
        ):
            paths = launch.list_working_directories(
                self.remote_machine(), destination="camel", timeout=20
            )
        self.assertEqual(paths, ["/home/u", "/home/u/Dev/A", "/home/u/Dev/B C"])
        self.assertIn("HOME", captured["remote"])
        self.assertIn('"$home"/Dev/*', captured["remote"])

    def test_directory_picker_manual_path_defaults_to_home(self):
        def choose_manual(command, **kwargs):
            record = next(
                item for item in kwargs["input"].split("\0")
                if item.startswith("__manual__\t")
            )
            return subprocess.CompletedProcess(command, 0, record + "\0", "")

        with (
            patch.object(launch.shutil, "which", return_value="/usr/bin/fzf"),
            patch.object(launch.subprocess, "run", side_effect=choose_manual) as run,
            patch.object(launch.typer, "prompt", return_value="/home/u/Dev/manual") as prompt,
        ):
            selected = launch.pick_working_directory(["/home/u", "/home/u/Dev/A"])
        self.assertEqual(selected, "/home/u/Dev/manual")
        self.assertNotIn("--no-sort", run.call_args.args[0])
        prompt.assert_called_once_with("Working directory", default="/home/u")

    def test_remote_launch_command_quotes_every_operator_value(self):
        command = launch.build_remote_launch_command(
            "/home/u/Dev/a b;echo BAD",
            "name ' quoted;echo BAD",
            ["--offline", "value;echo BAD", "line\nbreak"],
        )
        self.assertTrue(command.startswith("sh -lc "))
        self.assertIn("cd --", command)
        self.assertIn('exec "$wh_bin" --output json start --no-attach --name', command)
        self.assertIn("--agent pi", command)
        self.assertIn('wh_bin="$HOME/.local/bin/wh"', command)
        self.assertIn("run `wh host setup`", command)
        # The dangerous fragments exist only as quoted data, never as a bare
        # command separator in the outer remote command.
        parsed = shlex_split_once(command)
        self.assertEqual(parsed[0:2], ["sh", "-lc"])
        self.assertIn("'/home/u/Dev/a b;echo BAD'", parsed[2])
        self.assertIn("'value;echo BAD'", parsed[2])

    def test_remote_launch_command_forwards_the_selected_agent(self):
        command = launch.build_remote_launch_command(
            "/home/u/Dev/a",
            "omp-probe",
            [],
            agent="omp",
        )
        self.assertIn('exec "$wh_bin" --output json start --no-attach --name', command)
        self.assertIn("--agent omp", command)

    def test_remote_launch_falls_back_to_uv_tool_link_under_restricted_path(self):
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            repo = home / "repo"
            repo.mkdir()
            bin_dir = home / ".local" / "bin"
            bin_dir.mkdir(parents=True)
            args_file = home / "args.txt"
            fake_wh = bin_dir / "wh"
            fake_wh.write_text(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$HOME/args.txt\"\n"
                "printf '{\"session_id\":\"session-1\",\"name\":\"Repo\"}\\n'\n"
            )
            fake_wh.chmod(0o755)
            command = launch.build_remote_launch_command(
                str(repo), "Repo", ["--offline", "value;still-data"]
            )
            result = subprocess.run(
                ["sh", "-c", command],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env={"HOME": str(home), "PATH": "/usr/bin:/bin"},
                check=False,
                text=True,
            )
            captured_args = args_file.read_text().splitlines()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["session_id"], "session-1")
        self.assertEqual(captured_args[-2:], ["--offline", "value;still-data"])

    def test_local_launch_bypasses_ssh_and_uses_target_cwd(self):
        machine = self.remote_machine(local=True)
        completed = subprocess.CompletedProcess(
            [], 0, json.dumps({"session_id": "session-1", "name": "Repo"}).encode(), b""
        )
        with (
            patch.object(launch.shutil, "which", return_value="/usr/bin/wh"),
            patch.object(launch, "_run_command", return_value=completed) as run,
            patch.object(launch, "ssh_command") as ssh,
        ):
            result = launch.run_target_launch(
                machine,
                destination=None,
                cwd="/home/u/Dev/Repo",
                name="Repo",
                pi_args=["--offline"],
                timeout=30,
            )
        self.assertEqual(result["session_id"], "session-1")
        self.assertEqual(run.call_args.kwargs["cwd"], "/home/u/Dev/Repo")
        self.assertEqual(run.call_args.args[0][-2:], ["--", "--offline"])
        ssh.assert_not_called()

    def test_remote_launch_surfaces_malformed_json(self):
        machine = self.remote_machine()
        completed = subprocess.CompletedProcess([], 0, b"not json", b"target warning")
        with (
            patch.object(launch, "ssh_command", return_value=["ssh", "camel", "fixed"]),
            patch.object(launch, "_run_command", return_value=completed),
        ):
            with self.assertRaisesRegex(launch.RemoteCommandError, "malformed"):
                launch.run_target_launch(
                    machine,
                    destination="camel",
                    cwd="/home/u",
                    name="u",
                    pi_args=[],
                    timeout=30,
                )

    def test_target_history_remote_command_is_quoted_and_bounded(self):
        completed = subprocess.CompletedProcess([], 0, json.dumps([{
            "id": "history-1", "cwd": "/home/u/Repo",
        }]).encode(), b"")
        with (
            patch.object(launch, "ssh_command", return_value=["ssh", "camel", "remote"]) as ssh,
            patch.object(launch, "_run_command", return_value=completed),
        ):
            rows = launch.list_target_history(
                self.remote_machine(),
                destination="camel",
                cwd="/home/u/Repo",
                timeout=30,
            )
        self.assertEqual(rows[0]["id"], "history-1")
        remote = ssh.call_args.args[1]
        self.assertIn("history-list --cwd /home/u/Repo", shlex_split_once(remote)[2])

    def test_target_resume_uses_exact_id_and_no_attach(self):
        completed = subprocess.CompletedProcess([], 0, json.dumps({
            "session_id": "history-1", "name": "Stored",
        }).encode(), b"")
        with (
            patch.object(launch, "ssh_command", return_value=["ssh", "camel", "remote"]) as ssh,
            patch.object(launch, "_run_command", return_value=completed),
        ):
            result = launch.run_target_resume(
                self.remote_machine(),
                destination="camel",
                cwd="/home/u/Repo",
                history={"id": "history-1"},
                timeout=30,
            )
        self.assertEqual(result["session_id"], "history-1")
        script = shlex_split_once(ssh.call_args.args[1])[2]
        self.assertIn("resume history-1 --cwd /home/u/Repo --no-attach", script)

    def test_active_sessions_match_exact_target_and_cwd(self):
        machine = self.remote_machine()
        rows = [
            {"id": "yes", "session_type": "interactive", "state": "idle", "cwd": "/repo", "host": "KW"},
            {"id": "wrong-cwd", "session_type": "interactive", "state": "idle", "cwd": "/other", "host": "KW"},
            {"id": "wrong-host", "session_type": "interactive", "state": "idle", "cwd": "/repo", "host": "other"},
            {"id": "stopped", "session_type": "interactive", "state": "stopped", "cwd": "/repo", "host": "KW"},
        ]
        self.assertEqual(
            [row["id"] for row in launch._active_sessions_for_target(rows, machine, "/repo")],
            ["yes"],
        )

    def test_action_picker_without_fzf_preserves_start_new_fallback(self):
        with patch.object(launch.shutil, "which", return_value=None):
            self.assertEqual(
                launch.pick_launch_action(
                    [{"id": "active"}], [{"id": "history"}]
                ),
                ("new", None),
            )

    def test_action_picker_groups_running_previous_and_new(self):
        def choose_previous(command, **kwargs):
            records = kwargs["input"].rstrip("\0").split("\0")
            selected = next(record for record in records if record.startswith("resume:history-1\t"))
            return subprocess.CompletedProcess(command, 0, selected + "\0", "")

        with (
            patch.object(launch.shutil, "which", return_value="/usr/bin/fzf"),
            patch.object(launch.subprocess, "run", side_effect=choose_previous) as run,
        ):
            action, row = launch.pick_launch_action(
                [{"id": "active-1", "name": "Live", "state": "working"}],
                [{"id": "history-1", "name": "Stored", "modified_at": "2026-08-09T10:00:00Z"}],
            )
        self.assertEqual(action, "resume")
        self.assertEqual(row["id"], "history-1")
        records = run.call_args.kwargs["input"]
        self.assertNotIn("--no-sort", run.call_args.args[0])
        self.assertIn("Running sessions\n", records)
        self.assertIn("Previous sessions\n", records)
        self.assertIn("Start new\n", records)

    def test_defaults_name_to_directory_basename(self):
        self.assertEqual(launch.default_session_name("/home/u/Dev/DRRT"), "DRRT")
        self.assertEqual(launch.default_session_name("/"), "Pi")
        with self.assertRaisesRegex(RuntimeError, "absolute"):
            launch.validate_working_directory("~/Dev")


class LaunchTmuxHandoffTests(unittest.IsolatedAsyncioTestCase):
    async def test_tmux_handoff_opens_or_focuses_exact_invoking_client(self):
        selected = {"id": "session-1", "name": "Repo", "state": "idle"}
        opener = Mock(return_value="@7")
        with patch(
            "worker_harness.pi_tmux.open_or_focus_attachment_window", new=opener
        ):
            await launch._attach_selected_session(
                selected,
                tmux_target_session="$4",
                tmux_target_client="/dev/ttys009",
            )
        opener.assert_called_once_with(selected, "$4", "/dev/ttys009")

    async def test_tmux_handoff_refuses_incomplete_locator(self):
        with self.assertRaisesRegex(RuntimeError, "exact session and client"):
            await launch._attach_selected_session(
                {"id": "session-1"}, tmux_target_session="$4"
            )

    async def test_normal_terminal_attach_remains_unchanged(self):
        selected = {"id": "session-1", "name": "Repo", "state": "idle"}
        terminal = AsyncMock()
        with (
            patch("worker_harness.pi_zellij.is_immediate_zellij", return_value=False),
            patch("worker_harness.cli.pi._run_attach_loop", new=terminal),
        ):
            await launch._attach_selected_session(selected)
        terminal.assert_awaited_once_with(selected)


class LaunchCliTests(unittest.TestCase):
    @staticmethod
    def _app() -> typer.Typer:
        app = typer.Typer()
        app.command(
            context_settings={"allow_extra_args": True, "ignore_unknown_options": True}
        )(launch.launch)
        return app

    def test_cli_forwards_pi_arguments_after_separator(self):
        app = self._app()
        launched = {
            "session_id": "session-1",
            "name": "Repo",
            "machine": "camel",
            "machine_dns": "camel.hs.example",
            "cwd": "/home/u/Dev/Repo",
        }
        with patch.object(
            launch,
            "launch_managed_pi",
            new=AsyncMock(return_value=launched),
        ) as run:
            result = CliRunner().invoke(app, [
                "--machine", "camel",
                "--cwd", "/home/u/Dev/Repo",
                "--name", "Repo",
                "--agent", "omp",
                "--no-attach",
                "--",
                "--offline",
                "--model", "test/model",
            ])
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertEqual(run.call_args.kwargs["pi_args"], [
            "--offline", "--model", "test/model",
        ])
        self.assertEqual(run.call_args.kwargs["agent"], "omp")

    def test_cli_tmux_picker_passes_exact_popup_locator(self):
        launched = {
            "session_id": "session-1",
            "name": "Repo",
            "machine": "camel",
            "machine_dns": "camel.hs.example",
            "cwd": "/home/u/Dev/Repo",
        }
        with (
            patch.dict(launch.os.environ, {
                "WH_TMUX_TARGET_SESSION": "$4",
                "WH_TMUX_TARGET_CLIENT": "/dev/ttys009",
            }),
            patch.object(
                launch, "launch_managed_pi", new=AsyncMock(return_value=launched)
            ) as run,
        ):
            result = CliRunner().invoke(self._app(), [
                "--tmux-picker",
                "--machine", "camel",
                "--cwd", "/home/u/Dev/Repo",
                "--name", "Repo",
            ])
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertEqual(run.call_args.kwargs["tmux_target_session"], "$4")
        self.assertEqual(run.call_args.kwargs["tmux_target_client"], "/dev/ttys009")

    def test_cli_tmux_picker_requires_complete_locator_and_attach(self):
        with patch.dict(launch.os.environ, {}, clear=True):
            missing = CliRunner().invoke(self._app(), ["--tmux-picker"])
        self.assertEqual(missing.exit_code, 1)
        self.assertIn("requires its invoking tmux session and client", missing.output)

        with patch.dict(launch.os.environ, {
            "WH_TMUX_TARGET_SESSION": "$4",
            "WH_TMUX_TARGET_CLIENT": "/dev/ttys009",
        }):
            detached = CliRunner().invoke(
                self._app(), ["--tmux-picker", "--no-attach"]
            )
        self.assertEqual(detached.exit_code, 1)
        self.assertIn("cannot be combined with --no-attach", detached.output)

        launcher = AsyncMock()
        with (
            patch.dict(launch.os.environ, {
                "WH_TMUX_TARGET_SESSION": "#{session_id}",
                "WH_TMUX_TARGET_CLIENT": "/dev/ttys009",
            }),
            patch.object(launch, "launch_managed_pi", new=launcher),
        ):
            invalid = CliRunner().invoke(self._app(), ["--tmux-picker"])
        self.assertEqual(invalid.exit_code, 1)
        self.assertIn("exact tmux session ID", invalid.output)
        launcher.assert_not_awaited()


class _FakeAsyncClient:
    def __init__(self, responses):
        self.responses = responses

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def get(self, _path):
        return self.responses.pop(0)


class LaunchRegistrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_waits_for_exact_attachable_session_and_honors_rate_limit(self):
        request = httpx.Request("GET", "http://orchestrator/api/v1/pi/sessions/wanted")
        responses = [
            httpx.Response(404, request=request),
            httpx.Response(429, headers={"Retry-After": "1"}, request=request),
            httpx.Response(200, json={
                "id": "wanted", "state": "idle", "terminal_attachable": False,
            }, request=request),
            httpx.Response(200, json={
                "id": "wanted", "state": "idle", "terminal_attachable": True,
            }, request=request),
        ]
        sleep = AsyncMock()
        with (
            patch.object(
                launch.httpx,
                "AsyncClient",
                return_value=_FakeAsyncClient(responses),
            ),
            patch.object(launch.asyncio, "sleep", new=sleep),
        ):
            selected = await launch.wait_for_registered_session(
                "wanted", timeout=5, require_attachable=True
            )
        self.assertEqual(selected["id"], "wanted")
        self.assertFalse(responses)
        self.assertTrue(any(call.args[0] >= 1.0 for call in sleep.call_args_list))


def shlex_split_once(command: str) -> list[str]:
    import shlex

    return shlex.split(command)


if __name__ == "__main__":
    unittest.main()
