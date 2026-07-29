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


class PiTerminalAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_ctrl_right_bracket_detaches_without_forwarding_suffix(self):
        websocket = FakeWebSocket()

        async def chunks(_fd):
            yield b"hello"
            yield b"before\x1dafter"
            yield b"unreachable"

        with patch.object(pi_terminal, "_stdin_chunks", chunks):
            await pi_terminal._send_input(websocket, 0)
        self.assertEqual(websocket.sent, [b"hello", b"before"])

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

    async def test_receive_output_surfaces_relay_error(self):
        websocket = FakeWebSocket(['{"type":"error","detail":"already attached"}'])
        with self.assertRaisesRegex(RuntimeError, "already attached"):
            await pi_terminal._receive_output(websocket, 1)

    async def test_focus_local_session_switches_exact_registered_pane(self):
        route = {
            "ok": True,
            "tmux_socket": "/tmp/tmux-test/default",
            "tmux_session": "work",
            "window_index": "3",
            "pane_index": "2",
        }
        completed = subprocess.CompletedProcess([], 0, "", "")
        with (
            patch.dict(os.environ, {"TMUX": "/tmp/tmux-test/default,123,0"}),
            patch.object(pi_terminal, "_relay_request", new=AsyncMock(return_value=route)),
            patch.object(pi_terminal.subprocess, "run", return_value=completed) as run,
        ):
            focused = await pi_terminal.focus_local_session("session-1")
        self.assertTrue(focused)
        self.assertEqual(run.call_args.args[0], [
            "tmux", "-S", "/tmp/tmux-test/default", "switch-client", "-t", "work:3.2",
            ";", "select-pane", "-t", "work:3.2",
        ])

    async def test_focus_local_session_ignores_different_tmux_server(self):
        route = {
            "ok": True,
            "tmux_socket": "/tmp/other/default",
            "tmux_session": "work",
            "window_index": "1",
            "pane_index": "1",
        }
        with (
            patch.dict(os.environ, {"TMUX": "/tmp/current/default,123,0"}),
            patch.object(pi_terminal, "_relay_request", new=AsyncMock(return_value=route)),
            patch.object(pi_terminal.subprocess, "run") as run,
        ):
            focused = await pi_terminal.focus_local_session("session-1")
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
