"""Native Pi terminal attachment client tests."""

from __future__ import annotations

import asyncio
import json
import os
import pty
import subprocess
import termios
import unittest
from unittest.mock import AsyncMock, patch

from websockets.exceptions import ConnectionClosedError
from websockets.frames import Close

from worker_harness import pi_terminal


class FakeWebSocket:
    def __init__(self, messages=()):
        self.messages = list(messages)
        self.sent = []

    async def send(self, message):
        self.sent.append(message)

    def __aiter__(self):
        async def messages():
            for message in self.messages:
                yield message
        return messages()


class FakeConnection:
    def __init__(self, websocket=None, error=None):
        self.websocket = websocket
        self.error = error

    async def __aenter__(self):
        if self.error:
            raise self.error
        return self.websocket

    async def __aexit__(self, *_args):
        return False


class PiTerminalAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_ctrl_right_bracket_detaches_without_forwarding_suffix(self):
        websocket = FakeWebSocket()

        async def chunks(_fd):
            yield b"hello"
            yield b"before\x1dafter"
            yield b"unreachable"

        with patch.object(pi_terminal, "_stdin_chunks", chunks):
            direction = await pi_terminal._send_input(websocket, 0)
        self.assertIsNone(direction)
        self.assertEqual(websocket.sent, [b"hello", b"before"])

    async def test_zellij_control_bytes_cycle_without_reaching_remote_pty(self):
        for byte, expected in ((b"\x1e", "next"), (b"\x1f", "previous")):
            with self.subTest(expected):
                websocket = FakeWebSocket()

                async def chunks(_fd, control=byte):
                    yield b"before" + control + b"after"
                    yield b"unreachable"

                with patch.object(pi_terminal, "_stdin_chunks", chunks):
                    direction = await pi_terminal._send_input(websocket, 0)
                self.assertEqual(direction, expected)
                self.assertEqual(websocket.sent, [b"before"])

    async def test_attach_falls_back_to_gateway_and_returns_selector_on_idle(self):
        direct = "ws://direct/attach"
        gateway = "ws://gateway/attach"
        connections = []

        def fake_connect(url, **_kwargs):
            connections.append(url)
            if url.startswith(direct):
                return FakeConnection(error=OSError("direct unavailable"))
            return FakeConnection(FakeWebSocket([
                '{"type":"status","state":"idle-timeout","reason":"attachment inactive"}',
            ]))

        master_fd, slave_fd = pty.openpty()
        try:
            with patch.object(pi_terminal, "connect", side_effect=fake_connect):
                result = await pi_terminal.attach_terminal(
                    direct,
                    fallback_websocket_url=gateway,
                    stdin_fd=slave_fd,
                    stdout_fd=slave_fd,
                )
            self.assertEqual(result, "select")
            self.assertEqual(len(connections), 2)
            self.assertTrue(connections[0].startswith(direct))
            self.assertTrue(connections[1].startswith(gateway))
        finally:
            os.close(master_fd)
            os.close(slave_fd)

    async def test_resize_poll_detects_nested_tmux_size_change_without_signal(self):
        websocket = FakeWebSocket()
        sizes = iter([(24, 80), (24, 80), (40, 120), (40, 120)])
        with patch.object(pi_terminal, "terminal_size", side_effect=lambda _fd: next(sizes, (40, 120))):
            task = asyncio.create_task(pi_terminal._send_resizes(websocket, 1, asyncio.Event()))
            for _ in range(20):
                if len(websocket.sent) >= 2:
                    break
                await asyncio.sleep(0.1)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        frames = [json.loads(message) for message in websocket.sent]
        self.assertEqual(frames, [
            {"type": "resize", "rows": 24, "cols": 80},
            {"type": "resize", "rows": 40, "cols": 120},
        ])

    async def test_receive_output_writes_binary_and_ignores_status(self):
        read_fd, write_fd = os.pipe()
        try:
            websocket = FakeWebSocket([
                '{"type":"status","protocol_version":2}',
                b"terminal bytes",
            ])
            await pi_terminal._receive_output(websocket, write_fd)
            os.close(write_fd)
            write_fd = -1
            self.assertEqual(os.read(read_fd, 1024), b"terminal bytes")
        finally:
            os.close(read_fd)
            if write_fd >= 0:
                os.close(write_fd)

    async def test_receive_output_returns_to_selector_after_idle_timeout(self):
        websocket = FakeWebSocket([
            '{"type":"status","state":"idle-timeout","reason":"attachment inactive"}',
        ])
        self.assertEqual(await pi_terminal._receive_output(websocket, 1), "select")

    async def test_receive_output_returns_to_selector_when_replaced_at_capacity(self):
        websocket = FakeWebSocket([
            '{"type":"status","state":"replaced","reason":"capacity reclaimed"}',
        ])
        self.assertEqual(await pi_terminal._receive_output(websocket, 1), "select")

    async def test_receive_output_uses_replacement_close_code_if_status_is_lost(self):
        class ClosedWebSocket:
            def __aiter__(self):
                async def messages():
                    raise ConnectionClosedError(
                        Close(4410, "replaced by newer attachment"),
                        Close(4410, "replaced by newer attachment"),
                        True,
                    )
                    yield  # pragma: no cover
                return messages()

        self.assertEqual(await pi_terminal._receive_output(ClosedWebSocket(), 1), "select")

    async def test_receive_output_surfaces_relay_error(self):
        websocket = FakeWebSocket(['{"type":"error","detail":"already attached"}'])
        with self.assertRaisesRegex(RuntimeError, "already attached"):
            await pi_terminal._receive_output(websocket, 1)

    async def test_local_tmux_client_never_uses_direct_focus(self):
        route = {
            "ok": True,
            "multiplexer": "tmux",
            "tmux_socket": "/tmp/tmux-test/default",
            "tmux_session": "work",
            "window_index": "3",
            "pane_index": "2",
        }
        relay = AsyncMock(return_value=route)
        with (
            patch.dict(os.environ, {"TMUX": "/tmp/tmux-test/default,123,0"}, clear=True),
            patch.object(pi_terminal, "_relay_request", new=relay),
            patch.object(pi_terminal.subprocess, "run") as run,
        ):
            focused = await pi_terminal.focus_local_zellij_session("session-1")
        self.assertFalse(focused)
        relay.assert_not_awaited()
        run.assert_not_called()

    async def test_focus_local_zellij_pane_in_current_session(self):
        route = {
            "ok": True,
            "multiplexer": "zellij",
            "zellij_session_name": "Pi",
            "zellij_pane_id": "terminal_8",
        }
        completed = subprocess.CompletedProcess([], 0, "", "")
        with (
            patch.dict(os.environ, {"ZELLIJ_SESSION_NAME": "Pi"}, clear=True),
            patch.object(pi_terminal, "_relay_request", new=AsyncMock(return_value=route)),
            patch.object(pi_terminal.subprocess, "run", return_value=completed) as run,
        ):
            focused = await pi_terminal.focus_local_zellij_session("session-1")
        self.assertTrue(focused)
        self.assertEqual(
            run.call_args.args[0],
            ["zellij", "action", "focus-pane-id", "terminal_8"],
        )

    async def test_focus_local_zellij_pane_switches_session_and_client(self):
        route = {
            "ok": True,
            "multiplexer": "zellij",
            "zellij_session_name": "Research",
            "zellij_pane_id": "terminal_12",
        }
        completed = subprocess.CompletedProcess([], 0, "", "")
        with (
            patch.dict(os.environ, {"ZELLIJ_SESSION_NAME": "Pi"}, clear=True),
            patch.object(pi_terminal, "_relay_request", new=AsyncMock(return_value=route)),
            patch.object(pi_terminal.subprocess, "run", return_value=completed) as run,
        ):
            focused = await pi_terminal.focus_local_zellij_session("session-1")
        self.assertTrue(focused)
        self.assertEqual(
            run.call_args.args[0],
            [
                "zellij", "action", "switch-session", "Research",
                "--pane-id", "terminal_12",
            ],
        )

    async def test_local_zellij_focus_streams_across_multiplexers(self):
        route = {
            "ok": True,
            "multiplexer": "zellij",
            "zellij_session_name": "Pi",
            "zellij_pane_id": "terminal_8",
        }
        with (
            patch.dict(os.environ, {
                "TMUX": "/tmp/current/default,123,0",
                "ZELLIJ_SESSION_NAME": "outer-zellij",
            }, clear=True),
            patch.object(pi_terminal, "_relay_request", new=AsyncMock(return_value=route)),
            patch.object(pi_terminal.subprocess, "run") as run,
        ):
            focused = await pi_terminal.focus_local_zellij_session("session-1")
        self.assertFalse(focused)
        run.assert_not_called()


class PiTerminalTests(unittest.TestCase):
    def test_terminal_url_adds_initial_dimensions_and_preserves_query(self):
        self.assertEqual(
            pi_terminal.terminal_url("ws://host/attach?token=x", 52, 188),
            "ws://host/attach?token=x&rows=52&cols=188",
        )

    def test_raw_terminal_restores_attributes(self):
        master_fd, slave_fd = pty.openpty()
        try:
            before = termios.tcgetattr(slave_fd)
            with pi_terminal.raw_terminal(slave_fd):
                during = termios.tcgetattr(slave_fd)
                self.assertNotEqual(during, before)
            self.assertEqual(termios.tcgetattr(slave_fd), before)
        finally:
            os.close(master_fd)
            os.close(slave_fd)


if __name__ == "__main__":
    unittest.main()
