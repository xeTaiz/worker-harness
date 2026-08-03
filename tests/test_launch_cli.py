from __future__ import annotations

import json
import subprocess
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

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
            patch.object(launch.subprocess, "run", side_effect=choose_manual),
            patch.object(launch.typer, "prompt", return_value="/home/u/Dev/manual") as prompt,
        ):
            selected = launch.pick_working_directory(["/home/u", "/home/u/Dev/A"])
        self.assertEqual(selected, "/home/u/Dev/manual")
        prompt.assert_called_once_with("Working directory", default="/home/u")

    def test_remote_launch_command_quotes_every_operator_value(self):
        command = launch.build_remote_launch_command(
            "/home/u/Dev/a b;echo BAD",
            "name ' quoted;echo BAD",
            ["--offline", "value;echo BAD", "line\nbreak"],
        )
        self.assertTrue(command.startswith("sh -lc "))
        self.assertIn("cd --", command)
        self.assertIn("wh --output json pi start --no-attach --name", command)
        # The dangerous fragments exist only as quoted data, never as a bare
        # command separator in the outer remote command.
        parsed = shlex_split_once(command)
        self.assertEqual(parsed[0:2], ["sh", "-lc"])
        self.assertIn("'/home/u/Dev/a b;echo BAD'", parsed[2])
        self.assertIn("'value;echo BAD'", parsed[2])

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

    def test_defaults_name_to_directory_basename(self):
        self.assertEqual(launch.default_session_name("/home/u/Dev/DRRT"), "DRRT")
        self.assertEqual(launch.default_session_name("/"), "Pi")
        with self.assertRaisesRegex(RuntimeError, "absolute"):
            launch.validate_working_directory("~/Dev")


class LaunchCliTests(unittest.TestCase):
    def test_cli_forwards_pi_arguments_after_separator(self):
        app = typer.Typer()
        app.command(
            context_settings={"allow_extra_args": True, "ignore_unknown_options": True}
        )(launch.launch)
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
                "--no-attach",
                "--",
                "--offline",
                "--model", "test/model",
            ])
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertEqual(run.call_args.kwargs["pi_args"], [
            "--offline", "--model", "test/model",
        ])


class LaunchRegistrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_waits_for_exact_attachable_session(self):
        responses = [
            [],
            [{"id": "other", "state": "idle", "terminal_attachable": True}],
            [{"id": "wanted", "state": "idle", "terminal_attachable": False}],
            [{"id": "wanted", "state": "idle", "terminal_attachable": True}],
        ]

        async def request(_method, _path):
            return responses.pop(0)

        from worker_harness.cli import pi
        with (
            patch.object(pi, "_request", side_effect=request),
            patch.object(launch.asyncio, "sleep", new=AsyncMock()),
        ):
            selected = await launch.wait_for_registered_session(
                "wanted", timeout=5, require_attachable=True
            )
        self.assertEqual(selected["id"], "wanted")
        self.assertFalse(responses)


def shlex_split_once(command: str) -> list[str]:
    import shlex

    return shlex.split(command)


if __name__ == "__main__":
    unittest.main()
