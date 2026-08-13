"""CLI coverage for the Phase C Pi session surface."""

from __future__ import annotations

import asyncio
import json
import subprocess
import tempfile
import unittest
from pathlib import Path

import typer
from unittest.mock import ANY, AsyncMock, Mock, patch

from typer.testing import CliRunner

from worker_harness import pi_runtime
from worker_harness.cli import pi
from worker_harness.cli.app import _state


class PiCliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.runner = CliRunner()
        self.multiplexer_environment = patch.dict(pi.os.environ, {
            "TMUX": "",
            "TMUX_PANE": "",
            "ZELLIJ_SESSION_NAME": "",
            "ZELLIJ_PANE_ID": "",
        })
        self.multiplexer_environment.start()
        _state.clear()

    def tearDown(self) -> None:
        self.multiplexer_environment.stop()
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

    def test_parse_tailnet_dns_labels_uses_reported_magicdns_suffix(self):
        labels = pi._parse_tailnet_dns_labels({
            "MagicDNSSuffix": "hs.d0me.xyz",
            "Self": {
                "HostName": "archdome",
                "DNSName": "archdome.hs.d0me.xyz.",
                "TailscaleIPs": ["100.64.0.2"],
            },
            "Peer": {
                "interactive": {
                    "HostName": "KW60898",
                    "DNSName": "camel.hs.d0me.xyz.",
                    "TailscaleIPs": ["100.64.0.4", "fd7a::4"],
                },
                "worker": {
                    "HostName": "KW60898",
                    "DNSName": "kw60898.hs.d0me.xyz.",
                    "TailscaleIPs": ["100.64.0.126"],
                    "Tags": ["tag:wh-worker"],
                },
            },
        })
        self.assertEqual(labels, {
            "100.64.0.2": "archdome",
            "host:archdome": "archdome",
            "100.64.0.4": "camel",
            "fd7a::4": "camel",
            "host:kw60898": "camel",
            "100.64.0.126": "kw60898",
        })

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

    def test_attach_candidates_order_global_local_remote_then_delegated(self):
        rows = [
            {**self._session(), "id": "remote", "host": "zeta", "updated_at": 5},
            {**self._session(), "id": "delegated", "session_type": "delegated",
             "host": "", "worker_id": "worker-1", "updated_at": 9},
            {**self._session(), "id": "local-idle", "host": "local",
             "terminal_host": "100.64.0.2", "updated_at": 8},
            {**self._session(), "id": "local-working", "host": "local",
             "terminal_host": "100.64.0.2", "state": "working", "updated_at": 1},
            {**self._session(), "id": "global", "session_type": "global-router",
             "host": "router", "updated_at": 1},
            {**self._session(), "id": "alpha", "host": "alpha", "updated_at": 3},
        ]
        candidates = pi._attach_candidates(
            rows,
            [{"id": "worker-1", "name": "GPU Box", "worker_ip": "100.64.0.126"}],
            local_host="local",
            tailnet_dns_by_ip={
                "100.64.0.2": "local-tailnet",
                "host:zeta": "remote-tailnet",
                "100.64.0.126": "gpu-worker",
            },
        )
        self.assertEqual([row["id"] for row in candidates], [
            "global", "local-working", "local-idle", "alpha", "remote", "delegated",
        ])
        self.assertEqual(candidates[0]["_machine_label"], "Global")
        self.assertEqual(candidates[1]["_machine_label"], "Local · local")
        self.assertEqual(candidates[2]["_machine_label"], "Local · local")
        self.assertEqual(candidates[-1]["_machine_label"], "Delegated · GPU Box")
        self.assertEqual(candidates[1]["_machine_dns"], "local-tailnet")
        self.assertEqual(next(
            row["_machine_dns"] for row in candidates if row["id"] == "remote"
        ), "remote-tailnet")
        self.assertEqual(candidates[-1]["_machine_dns"], "gpu-worker")
        self.assertTrue(candidates[1]["_machine_first"])
        self.assertFalse(candidates[1]["_machine_last"])
        self.assertFalse(candidates[2]["_machine_first"])
        self.assertTrue(candidates[2]["_machine_last"])

    def test_picker_starts_on_first_local_with_global_one_step_up(self):
        rows = pi._attach_candidates([
            {**self._session(), "id": "global", "session_type": "global-router", "host": "router"},
            {**self._session(), "id": "local", "host": "local"},
            {**self._session(), "id": "remote", "host": "remote"},
        ], local_host="local")
        chosen = subprocess.CompletedProcess([], 0, "local\tlocal\tidle\tinteractive\tLocal · local\trepo-agent\t/repo\n", "")
        with (
            patch.object(pi.shutil, "which", return_value="/usr/bin/fzf"),
            patch.object(pi.subprocess, "run", return_value=chosen) as run,
        ):
            selected = pi._pick_session(rows)
        self.assertEqual(selected["id"], "local")
        self.assertIn("--bind=load:pos(2)", run.call_args.args[0])
        picker_input = run.call_args.kwargs["input"].rstrip("\0").split("\0")
        self.assertTrue(picker_input[0].startswith("global\tGlobal\n  └─ ✓   G"))
        self.assertTrue(picker_input[1].startswith("local\tLocal · local\n  └─ ✓   I"))

    def test_picker_uses_aligned_glyph_type_name_and_searchable_path_columns(self):
        rows = pi._attach_candidates([{
            **self._session(),
            "id": "local",
            "host": "local",
            "state": "working",
            "name": "0123456789abcdefghijkl",
            "cwd": "/full/path/to/the/repository",
        }], local_host="local")

        def choose_input(_command, **kwargs):
            return subprocess.CompletedProcess([], 0, kwargs["input"].split("\0", 1)[0] + "\0", "")

        with (
            patch.object(pi.shutil, "which", return_value="/usr/bin/fzf"),
            patch.object(pi.subprocess, "run", side_effect=choose_input) as run,
        ):
            selected = pi._pick_session(rows)

        self.assertEqual(selected["id"], "local")
        command = run.call_args.args[0]
        self.assertIn("--with-nth=2", command)
        self.assertIn("--nth=1", command)
        self.assertIn("--no-hscroll", command)
        self.assertNotIn("--no-sort", command)
        self.assertIn("--read0", command)
        self.assertIn("--print0", command)
        record = run.call_args.kwargs["input"].split("\0", 1)[0]
        fields = record.split("\t", 1)
        self.assertEqual(len(fields), 2)
        self.assertEqual(fields[0], "local")
        self.assertTrue(fields[1].startswith("Local · local\n  └─ ●   I"))
        self.assertIn("0123456789abcdef", fields[1])
        self.assertIn("Local · local", fields[1])
        self.assertTrue(fields[1].endswith("/full/path/to/the/repository"))

    def test_picker_renders_machine_heading_tree_and_tailnet_context(self):
        rows = pi._attach_candidates([
            {**self._session(), "id": "drrt", "host": "KW60898",
             "terminal_host": "100.64.0.4", "name": "DRRT",
             "cwd": "/home/engeld/Dev/DRRT", "state": "working"},
            {**self._session(), "id": "tomo", "host": "KW60898",
             "terminal_host": "100.64.0.4", "name": "TomoFoam",
             "cwd": "/home/engeld/Dev/radfoam"},
        ], local_host="archdome", tailnet_dns_by_ip={"100.64.0.4": "camel"})
        captured: dict[str, str] = {}

        def choose_first(command, **kwargs):
            captured["input"] = kwargs["input"]
            return subprocess.CompletedProcess(command, 0, kwargs["input"].split("\0", 1)[0] + "\0", "")

        with (
            patch.object(pi.shutil, "which", return_value="/usr/bin/fzf"),
            patch.object(pi.subprocess, "run", side_effect=choose_first),
        ):
            selected = pi._pick_session(rows)

        self.assertEqual(selected["id"], "drrt")
        records = captured["input"].rstrip("\0").split("\0")
        self.assertIn("KW60898  @camel\n  ├─ ●   I", records[0])
        self.assertNotIn("KW60898  @camel\n", records[1])
        self.assertIn("  └─ ✓   I", records[1])
        self.assertGreaterEqual(records[0].count("KW60898  @camel"), 2)
        self.assertEqual(records[1].count("KW60898  @camel"), 1)
        self.assertTrue(records[0].endswith("/home/engeld/Dev/DRRT"))
        self.assertTrue(records[1].endswith("/home/engeld/Dev/radfoam"))

    @unittest.skipUnless(pi.shutil.which("fzf"), "fzf is not installed")
    def test_installed_fzf_filters_visible_name_and_path(self):
        rows = pi._attach_candidates([{
            **self._session(),
            "id": "drrt",
            "host": "KW60898",
            "terminal_host": "100.64.0.4",
            "name": "DRRT",
            "cwd": "/home/engeld/Dev/DRRT",
        }], local_host="archdome", tailnet_dns_by_ip={"100.64.0.4": "camel"})
        captured: dict[str, object] = {}

        def capture(command, **kwargs):
            captured["command"] = command
            captured["input"] = kwargs["input"]
            return subprocess.CompletedProcess(command, 130, "", "")

        with (
            patch.object(pi.shutil, "which", return_value=pi.shutil.which("fzf")),
            patch.object(pi.subprocess, "run", side_effect=capture),
        ):
            with self.assertRaises(typer.Abort):
                pi._pick_session(rows)

        for query in ("d", "D", "DRRT", "engeld", "camel", "KW60898"):
            with self.subTest(query=query):
                result = subprocess.run(
                    [*captured["command"], f"--filter={query}"],
                    input=captured["input"],
                    text=True,
                    stdout=subprocess.PIPE,
                    check=False,
                )
                self.assertEqual(result.returncode, 0)
                self.assertTrue(result.stdout.startswith("drrt\t"))

    @unittest.skipUnless(pi.shutil.which("fzf"), "fzf is not installed")
    def test_installed_fzf_ranks_exact_match_above_earlier_weak_match(self):
        rows = pi._attach_candidates([
            {**self._session(), "id": "archive", "name": "A DRRT archive"},
            {**self._session(), "id": "exact", "name": "DRRT"},
        ], local_host="local")
        captured: dict[str, object] = {}

        def capture(command, **kwargs):
            captured["command"] = command
            captured["input"] = kwargs["input"]
            return subprocess.CompletedProcess(command, 130, "", "")

        with (
            patch.object(pi.shutil, "which", return_value=pi.shutil.which("fzf")),
            patch.object(pi.subprocess, "run", side_effect=capture),
        ):
            with self.assertRaises(typer.Abort):
                pi._pick_session(rows)

        result = subprocess.run(
            [*captured["command"], "--filter=DRRT"],
            input=captured["input"],
            text=True,
            stdout=subprocess.PIPE,
            check=False,
        )
        self.assertEqual(result.returncode, 0)
        self.assertTrue(result.stdout.startswith("exact\t"))

    def test_picker_abbreviates_global_and_delegated_types(self):
        rows = pi._attach_candidates([
            {**self._session(), "id": "global", "session_type": "global-router"},
            {**self._session(), "id": "delegate", "session_type": "delegated",
             "worker_id": "worker-1"},
        ], [{"id": "worker-1", "name": "GPU"}], local_host="local")
        captured: dict[str, str] = {}

        def choose_first(_command, **kwargs):
            captured["input"] = kwargs["input"]
            return subprocess.CompletedProcess([], 0, kwargs["input"].split("\0", 1)[0] + "\0", "")

        with (
            patch.object(pi.shutil, "which", return_value="/usr/bin/fzf"),
            patch.object(pi.subprocess, "run", side_effect=choose_first),
        ):
            pi._pick_session(rows)

        displays = [
            record.split("\t", 1)[1]
            for record in captured["input"].rstrip("\0").split("\0")
        ]
        self.assertIn("✓   G", displays[0])
        self.assertIn("✓   D", displays[1])

    def test_zellij_cycle_finds_original_pane_suppressed_by_in_place_helper(self):
        panes = [
            {"id": 7, "is_plugin": False, "is_suppressed": True, "tab_id": 3},
            {"id": 8, "is_plugin": False, "is_suppressed": False, "tab_id": 3},
            {"id": 9, "is_plugin": False, "is_suppressed": True, "tab_id": 4},
        ]
        completed = subprocess.CompletedProcess([], 0, json.dumps(panes), "")
        with (
            patch.dict(pi.os.environ, {
                "ZELLIJ_SESSION_NAME": "Pi", "ZELLIJ_PANE_ID": "8",
            }, clear=True),
            patch.object(pi.subprocess, "run", return_value=completed),
        ):
            self.assertEqual(pi._zellij_cycle_source_panes(), ["terminal_7"])

    def test_zellij_cycle_signals_existing_stream_process(self):
        origin = AsyncMock(return_value=("first", 1234))
        with (
            patch.object(pi, "_zellij_cycle_origin", new=origin),
            patch.object(pi.os, "kill") as kill,
        ):
            result = self.runner.invoke(pi.app, ["cycle", "next"])
        self.assertEqual(result.exit_code, 0, result.output)
        kill.assert_called_once_with(1234, pi.signal.SIGUSR1)

    def test_zellij_cycle_execs_normal_attach_from_local_source_pane(self):
        second = {**self._session(), "id": "second"}
        with (
            patch.object(pi, "_zellij_cycle_origin", new=AsyncMock(return_value=("first", None))),
            patch.object(pi, "_cycle_session", new=AsyncMock(return_value=second)),
            patch.object(pi.shutil, "which", return_value="/usr/bin/wh"),
            patch.object(pi.os, "execvp") as execute,
        ):
            result = self.runner.invoke(pi.app, ["cycle", "previous"])
        self.assertEqual(result.exit_code, 0, result.output)
        execute.assert_called_once_with(
            "/usr/bin/wh", ["/usr/bin/wh", "pi", "attach", "second"]
        )

    def test_start_creates_named_hidden_pi_and_can_return_without_attach(self):
        managed = pi_runtime.ManagedPiSession(
            "generated-id", "research", Path("/run/user/1000/worker-harness/pi-tmux.sock"), "%7"
        )
        start_managed = patch(
            "worker_harness.pi_runtime.start_managed_pi", return_value=managed
        )
        wait_route = AsyncMock(return_value={"multiplexer": "tmux"})
        with (
            start_managed as start_runtime,
            patch("worker_harness.pi_runtime.wait_for_managed_route", new=wait_route),
        ):
            result = self.runner.invoke(pi.app, [
                "start", "--name", "research", "--no-attach", "--",
                "--model", "openai/test", "hello world",
            ])
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("generated-id", result.output)
        self.assertEqual(
            start_runtime.call_args.kwargs["pi_args"],
            ["--model", "openai/test", "hello world"],
        )
        wait_route.assert_awaited_once_with(managed, timeout=10.0)

    def test_start_attaches_exact_generated_session_over_loopback(self):
        managed = pi_runtime.ManagedPiSession(
            "generated-id", "research", Path("/run/user/1000/worker-harness/pi-tmux.sock"), "%7"
        )
        attach_loop = AsyncMock(return_value=None)
        with (
            patch("worker_harness.pi_runtime.start_managed_pi", return_value=managed),
            patch(
                "worker_harness.pi_runtime.wait_for_managed_route",
                new=AsyncMock(return_value={"multiplexer": "tmux"}),
            ),
            patch(
                "worker_harness.pi_runtime.local_relay_websocket_url",
                return_value="ws://127.0.0.1:27890/v1/sessions/generated-id/attach",
            ),
            patch.object(pi, "_run_attach_loop", new=attach_loop),
        ):
            result = self.runner.invoke(pi.app, ["start", "--name", "research"])
        self.assertEqual(result.exit_code, 0, result.output)
        attach_loop.assert_awaited_once_with(
            {"id": "generated-id", "name": "research", "state": "idle"},
            initial_websocket_url="ws://127.0.0.1:27890/v1/sessions/generated-id/attach",
        )

    def test_start_in_zellij_opens_dedicated_loopback_tab(self):
        managed = pi_runtime.ManagedPiSession(
            "generated-id", "research", Path("/run/user/1000/worker-harness/pi-tmux.sock"), "%7"
        )
        opener = AsyncMock(return_value=None)
        with (
            patch.dict(pi.os.environ, {
                "ZELLIJ_SESSION_NAME": "Pi", "ZELLIJ_PANE_ID": "8", "TMUX": "",
            }),
            patch("worker_harness.pi_runtime.start_managed_pi", return_value=managed),
            patch(
                "worker_harness.pi_runtime.wait_for_managed_route",
                new=AsyncMock(return_value={"multiplexer": "tmux"}),
            ),
            patch.object(pi, "_open_in_zellij", new=opener),
            patch.object(pi, "_run_attach_loop", new=AsyncMock()) as attach_loop,
        ):
            result = self.runner.invoke(pi.app, ["start", "--name", "research"])
        self.assertEqual(result.exit_code, 0, result.output)
        opener.assert_awaited_once_with({
            "id": "generated-id", "name": "research", "state": "idle",
        }, loopback=True)
        attach_loop.assert_not_awaited()

    def test_attach_picker_filters_batched_attachability_snapshot(self):
        rows = [
            {**self._session(), "attach_info": {"attachable": True}},
            {
                **self._session(),
                "id": "offline",
                "attach_info": {"attachable": False, "reason": "relay offline"},
            },
        ]
        selected = asyncio.run(pi._attachable_candidates(rows))
        self.assertEqual([row["id"] for row in selected], ["interactive-session-id"])

    def test_attach_picker_reports_outdated_orchestrator(self):
        with self.assertRaisesRegex(RuntimeError, "orchestrator does not support"):
            asyncio.run(pi._attachable_candidates([self._session()]))

    def test_attach_picker_reports_partially_missing_attach_info(self):
        rows = [
            {**self._session(), "attach_info": {"attachable": True}},
            {**self._session(), "id": "missing"},
        ]
        with self.assertRaisesRegex(RuntimeError, "omitted attachment information for missing"):
            asyncio.run(pi._attachable_candidates(rows))

    def test_attach_picker_maps_full_fzf_row_back_to_session(self):
        rows = [self._session(), {**self._session(), "id": "second", "name": "other"}]
        chosen = subprocess.CompletedProcess([], 0, "second\tidle\tinteractive\tother\tlaptop\t/repo\n", "")
        with (
            patch.object(pi.shutil, "which", return_value="/usr/bin/fzf"),
            patch.object(pi.subprocess, "run", return_value=chosen),
        ):
            selected = pi._pick_session(rows)
        self.assertEqual(selected["id"], "second")

    def test_tmux_picker_accepts_authoritative_locator_from_popup_environment(self):
        opener = Mock(return_value="@7")
        with (
            patch.dict(pi.os.environ, {
                "WH_TMUX_TARGET_SESSION": "$4",
                "WH_TMUX_TARGET_CLIENT": "/dev/ttys009",
            }),
            patch.object(pi, "_candidate_inventory", new=AsyncMock(return_value=[self._session()])),
            patch.object(pi, "_attachable_candidates", new=AsyncMock(return_value=[self._session()])),
            patch.object(pi, "_pick_session", return_value=self._session()),
            patch("worker_harness.pi_tmux.open_or_focus_attachment_window", new=opener),
        ):
            result = self.runner.invoke(pi.app, ["attach", "--tmux-picker"])
        self.assertEqual(result.exit_code, 0, result.output)
        opener.assert_called_once_with(self._session(), "$4", "/dev/ttys009")

    def test_attach_in_zellij_opens_or_focuses_dedicated_tab(self):
        opener = AsyncMock(return_value=None)
        with (
            patch.dict(pi.os.environ, {
                "ZELLIJ_SESSION_NAME": "Pi", "ZELLIJ_PANE_ID": "8", "TMUX": "",
            }),
            patch.object(pi, "_request", new=AsyncMock(return_value=[self._session()])),
            patch.object(pi, "_open_in_zellij", new=opener),
            patch.object(pi, "_run_attach_loop", new=AsyncMock()) as attach_loop,
        ):
            result = self.runner.invoke(pi.app, ["attach", "repo-agent"])
        self.assertEqual(result.exit_code, 0, result.output)
        opener.assert_awaited_once()
        self.assertEqual(opener.await_args.args[0]["id"], "interactive-session-id")
        attach_loop.assert_not_awaited()

    def test_attach_here_loopback_skips_inventory_and_runs_in_tab(self):
        attach_loop = AsyncMock(return_value=None)
        relay = AsyncMock(return_value={"ok": True, "multiplexer": "tmux"})
        with (
            patch.dict(pi.os.environ, {
                "ZELLIJ_SESSION_NAME": "Pi", "ZELLIJ_PANE_ID": "8", "TMUX": "",
            }),
            patch("worker_harness.pi_terminal._relay_request", new=relay),
            patch(
                "worker_harness.pi_runtime.local_relay_websocket_url",
                return_value="ws://127.0.0.1:27890/v1/sessions/session-1/attach",
            ),
            patch.object(pi, "_run_attach_loop", new=attach_loop),
            patch.object(pi, "_request", new=AsyncMock()) as request,
        ):
            result = self.runner.invoke(pi.app, [
                "attach", "--here", "--loopback", "--session-name", "research",
                "--session-state", "idle", "session-1",
            ])
        self.assertEqual(result.exit_code, 0, result.output)
        request.assert_not_awaited()
        relay.assert_awaited_once_with({"action": "describe", "session_id": "session-1"})
        attach_loop.assert_awaited_once_with(
            {"id": "session-1", "name": "research", "state": "idle"},
            initial_websocket_url="ws://127.0.0.1:27890/v1/sessions/session-1/attach",
            zellij_tab=True,
        )

    def test_attach_resolves_prefix_and_streams_protocol_v2(self):
        request = AsyncMock(side_effect=[
            [self._session()],
            {
                "session_id": "interactive-session-id",
                "attachable": True,
                "transport": "direct-interactive-websocket",
                "protocol_version": 2,
                "websocket_url": "ws://100.64.0.2:27888/v1/sessions/interactive-session-id/attach",
                "direct_websocket_url": "ws://100.64.0.2:27888/v1/sessions/interactive-session-id/attach",
                "gateway_websocket_url": "ws://orchestrator:12889/api/v1/pi/sessions/interactive-session-id/attach-gateway",
            },
        ])
        focus = AsyncMock(return_value=False)
        terminal = AsyncMock(return_value=None)
        with (
            patch.object(pi, "_request", new=request),
            patch("worker_harness.pi_terminal.focus_local_zellij_session", new=focus),
            patch("worker_harness.pi_terminal.attach_terminal", new=terminal),
        ):
            result = self.runner.invoke(pi.app, ["attach", "interactive-"])
        self.assertEqual(result.exit_code, 0, result.output)
        focus.assert_awaited_once_with("interactive-session-id")
        terminal.assert_awaited_once_with(
            "ws://100.64.0.2:27888/v1/sessions/interactive-session-id/attach",
            fallback_websocket_url="ws://orchestrator:12889/api/v1/pi/sessions/interactive-session-id/attach-gateway",
            cycle_requests=ANY,
        )
        self.assertEqual(request.await_args_list[1].args, (
            "GET", "/api/v1/pi/sessions/interactive-session-id/attach-info"
        ))

    def test_attach_cycles_to_next_available_session(self):
        first = {**self._session(), "id": "first"}
        second = {**self._session(), "id": "second", "name": "second-agent"}
        inventory = [
            {**first, "attach_info": {"attachable": True}},
            {**second, "attach_info": {"attachable": True}},
        ]
        request = AsyncMock(side_effect=[
            inventory,
            {
                "attachable": True,
                "protocol_version": 2,
                "websocket_url": "ws://relay/first",
            },
            inventory,
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

    def test_cycle_anchors_on_current_session_when_it_becomes_unavailable(self):
        first = {**self._session(), "id": "first"}
        current = {**self._session(), "id": "current"}
        third = {**self._session(), "id": "third"}
        inventory = [
            {**first, "attach_info": {"attachable": True}},
            {**current, "attach_info": {"attachable": False, "reason": "relay offline"}},
            {**third, "attach_info": {"attachable": True}},
        ]
        with patch.object(pi, "_candidate_inventory", new=AsyncMock(return_value=inventory)):
            next_session = asyncio.run(pi._cycle_session("current", "next"))
            previous_session = asyncio.run(pi._cycle_session("current", "previous"))
        self.assertEqual(next_session["id"], "third")
        self.assertEqual(previous_session["id"], "first")

    def test_replacement_returns_attachment_window_to_picker(self):
        first = {**self._session(), "id": "first"}
        second = {**self._session(), "id": "second", "name": "second-agent"}
        request = AsyncMock(side_effect=[
            [{**first, "attach_info": {"attachable": True}}],
            {"attachable": True, "protocol_version": 2, "websocket_url": "ws://relay/first"},
            [{**second, "attach_info": {"attachable": True}}],
            {"attachable": True, "protocol_version": 2, "websocket_url": "ws://relay/second"},
        ])
        terminal = AsyncMock(side_effect=["select", None])
        with (
            patch.object(pi, "_request", new=request),
            patch.object(pi, "_mark_attach_pane"),
            patch.object(pi, "_pick_session", return_value=second),
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
        request = AsyncMock(return_value=[
            {**first, "attach_info": {"attachable": True}},
            {**second, "attach_info": {"attachable": True}},
        ])
        focus = AsyncMock(return_value=True)
        with (
            patch.object(pi, "_request", new=request),
            patch.object(pi, "_mark_attach_pane"),
            patch("worker_harness.pi_terminal.focus_local_zellij_session", new=focus),
        ):
            result = self.runner.invoke(
                pi.app, ["attach", "first", "--relative", "next"]
            )
        self.assertEqual(result.exit_code, 0, result.output)
        focus.assert_awaited_once_with("second")

    def test_attach_same_host_tmux_streams_through_attach_info(self):
        request = AsyncMock(side_effect=[
            [self._session()],
            {
                "attachable": True,
                "protocol_version": 2,
                "websocket_url": "ws://relay/interactive-session-id",
            },
        ])
        focus = AsyncMock(return_value=False)
        terminal = AsyncMock(return_value=None)
        with (
            patch.object(pi, "_request", new=request),
            patch("worker_harness.pi_terminal.focus_local_zellij_session", new=focus),
            patch("worker_harness.pi_terminal.attach_terminal", new=terminal),
        ):
            result = self.runner.invoke(pi.app, ["attach", "repo-agent"])
        self.assertEqual(result.exit_code, 0, result.output)
        focus.assert_awaited_once_with("interactive-session-id")
        terminal.assert_awaited_once()
        self.assertEqual(request.await_args_list[1].args, (
            "GET", "/api/v1/pi/sessions/interactive-session-id/attach-info"
        ))

    def test_stream_flag_still_focuses_local_zellij_to_prevent_recursive_render(self):
        request = AsyncMock(return_value=[self._session()])
        focus = AsyncMock(return_value=True)
        terminal = AsyncMock(return_value=None)
        with (
            patch.object(pi, "_request", new=request),
            patch("worker_harness.pi_terminal.focus_local_zellij_session", new=focus),
            patch("worker_harness.pi_terminal.attach_terminal", new=terminal),
        ):
            result = self.runner.invoke(pi.app, ["attach", "repo-agent", "--stream"])
        self.assertEqual(result.exit_code, 0, result.output)
        focus.assert_awaited_once_with("interactive-session-id")
        request.assert_awaited_once_with(
            "GET", "/api/v1/pi/sessions?include_attach_info=true"
        )
        terminal.assert_not_awaited()

    def test_resume_refuses_active_exact_id_before_target_history_lookup(self):
        request = AsyncMock(return_value=[{"id": "history-1", "state": "idle"}])
        with (
            patch.object(pi, "_request", new=request),
            patch("worker_harness.pi_history.resolve_session_history") as resolve,
        ):
            result = self.runner.invoke(
                pi.app,
                ["resume", "history-1", "--cwd", "/repo", "--no-attach"],
            )
        self.assertEqual(result.exit_code, 1)
        self.assertIn("already active", result.output)
        resolve.assert_not_called()

    def test_resume_preserves_stored_name_and_returns_exact_id(self):
        managed = pi_runtime.ManagedPiSession(
            "history-1", "Stored name", Path("/tmp/pi.sock"), "%7"
        )
        _state["output"] = "json"
        with (
            patch.object(pi, "_request", new=AsyncMock(return_value=[])),
            patch(
                "worker_harness.pi_history.resolve_session_history",
                return_value={"id": "history-1", "name": "Stored name", "cwd": "/repo"},
            ),
            patch("worker_harness.pi_runtime.resume_managed_pi", return_value=managed) as resume,
            patch("worker_harness.pi_runtime.wait_for_managed_route", new=AsyncMock(return_value={})),
        ):
            result = self.runner.invoke(
                pi.app,
                ["resume", "history-1", "--cwd", "/repo", "--no-attach"],
            )
        self.assertEqual(result.exit_code, 0, result.output)
        payload = json.loads(result.output)
        self.assertEqual(payload["session_id"], "history-1")
        self.assertTrue(payload["resumed"])
        self.assertEqual(resume.call_args.kwargs["name"], "Stored name")

    def test_attach_reports_unavailable_session(self):
        request = AsyncMock(side_effect=[
            [self._session()],
            {"session_id": "interactive-session-id", "attachable": False, "reason": "relay offline"},
        ])
        with (
            patch.object(pi, "_request", new=request),
            patch("worker_harness.pi_terminal.focus_local_zellij_session", new=AsyncMock(return_value=False)),
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


class PiAttachReconnectTests(unittest.IsolatedAsyncioTestCase):
    async def test_owned_tmux_window_reconnects_after_transport_failure(self):
        from worker_harness.pi_terminal import TerminalRelayUnavailable

        terminal = AsyncMock(side_effect=[
            TerminalRelayUnavailable("transport=direct"),
            None,
        ])
        request = AsyncMock(return_value={
            "attachable": True,
            "protocol_version": 2,
            "direct_websocket_url": "ws://direct/retry",
            "gateway_websocket_url": "ws://gateway/retry",
        })
        update = Mock(return_value=True)

        async def watcher(*_args, **_kwargs):
            await asyncio.Event().wait()

        with (
            patch.object(pi, "_request", new=request),
            patch.object(pi, "_mark_attach_pane"),
            patch.object(pi.asyncio, "sleep", new=AsyncMock()),
            patch("worker_harness.pi_terminal.focus_local_zellij_session", new=AsyncMock(return_value=False)),
            patch("worker_harness.pi_terminal.attach_terminal", new=terminal),
            patch("worker_harness.pi_tmux.current_attachment_window", return_value="@3"),
            patch("worker_harness.pi_tmux.update_attachment_window", new=update),
            patch("worker_harness.pi_zellij_state.watch_session_state", new=watcher),
        ):
            await pi._run_attach_loop(
                {"id": "session-1", "name": "Repo", "state": "idle"},
                initial_websocket_url="ws://direct/first",
                tmux_window=True,
            )
        self.assertEqual(terminal.await_count, 2)
        request.assert_awaited_once_with(
            "GET", "/api/v1/pi/sessions/session-1/attach-info"
        )
        self.assertTrue(any(call.args[1] == "disconnected" for call in update.call_args_list))


if __name__ == "__main__":
    unittest.main()
