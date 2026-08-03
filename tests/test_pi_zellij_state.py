"""Zellij Pi attachment tab-state tests."""

from __future__ import annotations

import asyncio
import json
import unittest

import httpx

from worker_harness import pi_zellij_state as state


class ZellijStateTests(unittest.TestCase):
    def test_exact_title_glyphs_and_name_sanitization(self):
        self.assertEqual(state.tab_title("research", state.WORKING), "π ● research")
        self.assertEqual(state.tab_title("research", state.IDLE), "π ✓ research")
        self.assertEqual(state.tab_title("research", state.ERROR), "π ! research")
        self.assertEqual(state.tab_title("research", state.DISCONNECTED), "π ? research")
        self.assertEqual(state.tab_title("  line\tone\n two  ", "unknown"), "π ? line one two")
        self.assertEqual(state.tab_title("\n\t", state.IDLE), "π ✓ Pi")
        self.assertEqual(state.state_glyph("failed"), "!")
        self.assertEqual(state.state_glyph("offline"), "?")

    def test_working_idle_and_sticky_error_until_next_turn(self):
        tracker = state.SessionStateTracker(state.IDLE)
        self.assertEqual(tracker.state, state.IDLE)
        self.assertTrue(tracker.feed({"event_type": "agent-start", "payload": {}}))
        self.assertEqual(tracker.state, state.WORKING)
        self.assertTrue(tracker.feed({
            "event_type": "tool-end", "payload": {"is_error": True},
        }))
        self.assertEqual(tracker.state, state.ERROR)
        self.assertFalse(tracker.feed({"event_type": "agent-settled", "payload": {}}))
        self.assertEqual(tracker.state, state.ERROR)
        self.assertTrue(tracker.feed({"event_type": "agent-start", "payload": {}}))
        self.assertEqual(tracker.state, state.WORKING)
        self.assertTrue(tracker.feed({"event_type": "agent-settled", "payload": {}}))
        self.assertEqual(tracker.state, state.IDLE)

    def test_all_error_shapes(self):
        events = [
            {"event_type": "tool-end", "payload": {"isError": True}},
            {"event_type": "message-end", "payload": {"errorMessage": "bad"}},
            {"event_type": "message-end", "payload": {"message": {"is_error": True}}},
            {"event_type": "control-error", "payload": {}},
        ]
        for event in events:
            with self.subTest(event=event):
                tracker = state.SessionStateTracker(state.WORKING)
                self.assertTrue(tracker.feed(event))
                self.assertEqual(tracker.state, state.ERROR)

    def test_sse_parser_handles_fragmentation_crlf_comments_and_cursor(self):
        parser = state.SSEParser()
        payload1 = {"sequence": 7, "event_type": "agent-start", "payload": {}}
        payload2 = {"sequence": 8, "event_type": "agent-settled", "payload": {}}
        chunks = [
            b": keep-alive\r",
            b"\nid: 7\r\nda",
            ("ta: " + json.dumps(payload1) + "\r\n\r").encode(),
            b"\nid: 8\ndata: " + json.dumps(payload2).encode() + b"\n\n",
        ]
        events = []
        for chunk in chunks:
            events.extend(parser.feed(chunk))
        self.assertEqual([event.id for event in events], ["7", "8"])
        self.assertEqual([event.data for event in events], [payload1, payload2])
        self.assertEqual(parser.cursor, "8")

    def test_sse_parser_preserves_utf8_split_across_byte_chunks(self):
        parser = state.SSEParser()
        encoded = 'data: {"event_type":"message-end","payload":{"text":"λ"}}\n\n'.encode()
        split = encoded.index("λ".encode()) + 1
        self.assertEqual(parser.feed(encoded[:split]), [])
        events = parser.feed(encoded[split:])
        self.assertEqual(events[0].data["payload"]["text"], "λ")

    def test_sse_parser_multiple_data_lines_and_invalid_json(self):
        parser = state.SSEParser()
        events = parser.feed("id: x\ndata: not\ndata: json\n\n")
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].data, {"_raw": "not\njson"})

    def test_sse_parser_rejects_unbounded_line(self):
        parser = state.SSEParser()
        with self.assertRaisesRegex(ValueError, "exceeds 1 MiB"):
            parser.feed("x" * (1024 * 1024 + 1))


class ZellijStateAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_watcher_debounces_duplicate_events(self):
        stop = asyncio.Event()
        payload = json.dumps({
            "sequence": 1,
            "event_type": "agent-start",
            "payload": {},
        }).encode()

        class Response:
            status_code = 200

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return False

            async def aiter_bytes(self):
                yield b"id: 1\ndata: " + payload + b"\n\n"
                yield b"id: 2\ndata: " + payload + b"\n\n"
                await asyncio.sleep(0.02)
                stop.set()

        class Client:
            def stream(self, *_args, **_kwargs):
                return Response()

        rendered: list[str] = []

        async def on_state(value: str) -> None:
            rendered.append(value)

        await state.watch_session_state(
            "http://orchestrator",
            "session-1",
            state.IDLE,
            on_state,
            stop_event=stop,
            debounce_seconds=0,
            client_factory=Client,
        )
        self.assertEqual(rendered, [state.IDLE, state.WORKING])

    async def test_watcher_renders_disconnected_and_retries_after_read_failure(self):
        stop = asyncio.Event()

        class BrokenStream:
            async def __aenter__(self):
                raise httpx.ReadTimeout("silent connection")

            async def __aexit__(self, *_args):
                return False

        class Client:
            def stream(self, *_args, **_kwargs):
                return BrokenStream()

        rendered: list[str] = []

        async def on_state(value: str) -> None:
            rendered.append(value)

        async def stop_after_retry(_seconds: float) -> None:
            stop.set()
            await asyncio.sleep(0)

        await state.watch_session_state(
            "http://orchestrator",
            "session-1",
            state.IDLE,
            on_state,
            stop_event=stop,
            client_factory=Client,
            sleep=stop_after_retry,
        )
        self.assertEqual(rendered, [state.IDLE, state.DISCONNECTED])


if __name__ == "__main__":
    unittest.main()
