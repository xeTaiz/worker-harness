"""Plugin-free Zellij tab lifecycle state for Pi sessions.

This module projects the durable per-session event stream onto a small state
machine that drives the ``\u03c0 <glyph> <name>`` Zellij tab title without
requiring a custom WASM plugin. It exposes:

- :func:`tab_title` — deterministic glyph mapping with name sanitization;
- :class:`SessionStateTracker` — pure event-driven state machine;
- :class:`SSEParser` — incremental, bytes/text tolerant SSE parser;
- :func:`watch_session_state` — async httpx-based watcher with bounded
  exponential reconnect backoff and debounced state callbacks.

The pure pieces (parser, tracker, title) are factored so they can be unit
tested in isolation. ``watch_session_state`` itself can be tested by
mocking :class:`httpx.AsyncClient`.
"""

from __future__ import annotations

import asyncio
import codecs
import json
import logging
import re
from dataclasses import dataclass
from typing import Any, Awaitable, Callable
from urllib.parse import quote, urlencode

import httpx

logger = logging.getLogger(__name__)


WORKING = "working"
IDLE = "idle"
ERROR = "error"
DISCONNECTED = "disconnected"

ALL_STATES: tuple[str, ...] = (WORKING, IDLE, ERROR, DISCONNECTED)

# Tab glyphs for each rendered state. ``UNKNOWN`` falls back to the question
# mark used for the disconnected / not-yet-known state.
_STATE_GLYPHS: dict[str, str] = {
    WORKING: "\u25cf",       # black circle
    IDLE: "\u2713",          # check mark
    ERROR: "!",              # exclamation
    DISCONNECTED: "?",       # question mark
}

_DEFAULT_NAME = "Pi"
_MAX_SSE_LINE_BYTES = 1024 * 1024

# Tabs and any newline-style whitespace collapse into single spaces so the
# rendered title fits on one Zellij tab row.
_TAB_OR_NEWLINE_RE = re.compile(r"[\t\n\r\f\v\u0085\u2028\u2029]+")
_INTERNAL_WHITESPACE_RE = re.compile(r"[ \u00a0\u1680\u2000-\u200a\u202f\u205f\u3000]+")


def sanitize_name(name: str | None) -> str:
    """Collapse tabs/newlines/whitespace and fall back to :data:`_DEFAULT_NAME`."""

    if name is None:
        return _DEFAULT_NAME
    text = _TAB_OR_NEWLINE_RE.sub(" ", str(name))
    text = _INTERNAL_WHITESPACE_RE.sub(" ", text)
    text = text.strip()
    return text or _DEFAULT_NAME


def state_glyph(state: str) -> str:
    """Return the tab/picker glyph for a projected session state."""

    normalized = {
        "failed": ERROR,
        "runtime_error": ERROR,
        "offline": DISCONNECTED,
        "unknown": DISCONNECTED,
    }.get(state, state)
    return _STATE_GLYPHS.get(normalized, _STATE_GLYPHS[DISCONNECTED])


def tab_title(name: str, state: str) -> str:
    """Return the deterministic Zellij tab title for ``state``."""

    return f"\u03c0 {state_glyph(state)} {sanitize_name(name)}"


# ---------------------------------------------------------------------------
# Event mapping
# ---------------------------------------------------------------------------


def _payload_truthy(payload: Any, *keys: str) -> bool:
    """Return True when ``payload`` (or nested ``message``) has any key truthy."""

    if not isinstance(payload, dict):
        return False
    for key in keys:
        value = payload.get(key)
        if value:
            return True
    message = payload.get("message")
    if isinstance(message, dict):
        for key in keys:
            value = message.get(key)
            if value:
                return True
    return False


class SessionStateTracker:
    """Project Pi session events onto the four rendered states.

    The tracker is initialized from the projected state returned by the
    orchestrator. ``working`` and ``idle`` keep their state; any other
    initial projection (including ``error`` and ``disconnected``) collapses
    to :data:`DISCONNECTED`. The tracker then consumes durable event dicts
    shaped ``{sequence, event_type, payload}`` and returns whether the
    rendered state changed.
    """

    __slots__ = ("_state", "_error_in_turn", "_sticky_error", "_last_event_type")

    def __init__(self, initial_state: str | None) -> None:
        if initial_state == WORKING:
            self._state = WORKING
        elif initial_state == IDLE:
            self._state = IDLE
        else:
            self._state = DISCONNECTED
        # Tracks whether the current agent turn has recorded any error event.
        # ``agent-settled`` flips to ``idle`` only when this is False.
        self._error_in_turn = False
        # Sticky error survives ``agent-settled`` and is cleared only by the
        # next ``agent-start``.
        self._sticky_error = False
        self._last_event_type: str | None = None

    @property
    def state(self) -> str:
        return self._state

    @property
    def sticky_error(self) -> bool:
        return self._sticky_error

    @property
    def error_in_turn(self) -> bool:
        return self._error_in_turn

    @property
    def last_event_type(self) -> str | None:
        return self._last_event_type

    def feed(self, event: dict[str, Any] | None) -> bool:
        """Apply ``event`` and return whether the rendered state changed."""

        if not isinstance(event, dict):
            return False
        event_type = event.get("event_type")
        payload = event.get("payload") or {}
        self._last_event_type = event_type

        if event_type == "agent-start":
            self._sticky_error = False
            self._error_in_turn = False
            return self._set_state(WORKING)

        if event_type == "agent-settled":
            new_state = ERROR if self._sticky_error else IDLE
            return self._set_state(new_state)

        if event_type == "tool-end":
            if _payload_truthy(payload, "is_error", "isError"):
                self._sticky_error = True
                self._error_in_turn = True
                return self._set_state(ERROR)
            return False

        if event_type == "message-end":
            if _payload_truthy(
                payload, "is_error", "isError", "errorMessage", "error_message"
            ):
                self._sticky_error = True
                self._error_in_turn = True
                return self._set_state(ERROR)
            return False

        if event_type == "control-error":
            self._sticky_error = True
            self._error_in_turn = True
            return self._set_state(ERROR)

        return False

    def _set_state(self, new_state: str) -> bool:
        if new_state == self._state:
            return False
        self._state = new_state
        return True

    def reset_to_disconnected(self) -> bool:
        """Force the rendered state to :data:`DISCONNECTED` (used by the watcher)."""

        return self._set_state(DISCONNECTED)


# ---------------------------------------------------------------------------
# SSE parser
# ---------------------------------------------------------------------------


@dataclass
class SSEEvent:
    """Single decoded Server-Sent Event."""

    id: str | None
    data: Any


class SSEParser:
    """Incremental, line-tolerant Server-Sent Events parser.

    Accepts arbitrary ``bytes`` or ``str`` chunks, normalises CRLF and bare
    CR line endings, ignores ``:`` comments/keep-alives, accumulates
    ``data:`` lines per record, decodes the joined data as JSON, and exposes
    the latest ``id:`` value as the reconnect cursor. Multiple events per
    chunk and split-across-chunks records are both supported.
    """

    __slots__ = ("_buffer", "_cursor", "_data_parts", "_id", "_decoder")

    def __init__(self) -> None:
        # ``_buffer`` holds text since the last completed line. Newline
        # characters are stripped from it as we process complete lines.
        self._buffer: str = ""
        # ``_cursor`` mirrors the most recent ``id:`` field; it doubles as
        # the reconnect cursor surfaced to the watcher.
        self._cursor: str | None = None
        # ``_data_parts`` accumulates ``data:`` lines of the current record.
        self._data_parts: list[str] = []
        # ``_id`` is the per-record id; flushed into ``_cursor`` on emit.
        self._id: str | None = None
        self._decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")

    @property
    def cursor(self) -> str | None:
        """Last ``id:`` value seen; used as the reconnect cursor."""

        return self._cursor

    def feed(self, chunk: bytes | str | bytearray | memoryview | None) -> list[SSEEvent]:
        """Consume ``chunk`` and return all complete events it produced."""

        if chunk is None or chunk == "":
            return []
        if isinstance(chunk, (bytes, bytearray, memoryview)):
            text = self._decoder.decode(bytes(chunk), final=False)
        else:
            text = str(chunk)
        self._buffer += text

        events: list[SSEEvent] = []
        while True:
            lf = self._buffer.find("\n")
            cr = self._buffer.find("\r")
            indexes = [index for index in (lf, cr) if index >= 0]
            if not indexes:
                if len(self._buffer.encode("utf8")) > _MAX_SSE_LINE_BYTES:
                    self._buffer = ""
                    raise ValueError("SSE line exceeds 1 MiB")
                return events
            newline_index = min(indexes)
            if len(self._buffer[:newline_index].encode("utf8")) > _MAX_SSE_LINE_BYTES:
                self._buffer = self._buffer[newline_index + 1 :]
                raise ValueError("SSE line exceeds 1 MiB")
            # A trailing CR may be the first byte of CRLF split across chunks.
            if self._buffer[newline_index] == "\r" and newline_index + 1 == len(self._buffer):
                return events
            delimiter_width = 2 if self._buffer.startswith("\r\n", newline_index) else 1
            raw_line = self._buffer[:newline_index]
            self._buffer = self._buffer[newline_index + delimiter_width :]
            emitted = self._process_line(raw_line)
            if emitted is not None:
                events.append(emitted)

    def reset(self) -> None:
        self._buffer = ""
        self._cursor = None
        self._data_parts = []
        self._id = None
        self._decoder.reset()

    # Internal ----------------------------------------------------------

    def _process_line(self, raw_line: str) -> SSEEvent | None:
        line = raw_line.lstrip("\ufeff")
        # A blank line closes the record (and emits it).
        if line == "":
            return self._emit_record()
        # Comment / keep-alive lines are ignored entirely.
        if line.startswith(":"):
            return None
        if ":" in line:
            field, _, value = line.partition(":")
            # SSE strips a single leading space after the colon.
            if value.startswith(" "):
                value = value[1:]
        else:
            field, value = line, ""
        if field == "data":
            self._data_parts.append(value)
        elif field == "id":
            self._id = value
        # All other fields (``event``, ``retry``, ...) are ignored.
        return None

    def _emit_record(self) -> SSEEvent | None:
        # A record without ``data:`` lines is not a real event — it is just an
        # id-only ``: ping`` or a stray blank line; we keep the id around for
        # the next record but emit nothing.
        if not self._data_parts:
            return None
        data_text = "\n".join(self._data_parts)
        self._data_parts = []
        event_id = self._id
        self._id = None
        try:
            decoded: Any = json.loads(data_text)
        except json.JSONDecodeError:
            decoded = {"_raw": data_text}
        if event_id is not None:
            self._cursor = event_id
        return SSEEvent(id=event_id, data=decoded)


# ---------------------------------------------------------------------------
# Async watcher
# ---------------------------------------------------------------------------


# Bounded exponential reconnect schedule: 0.5, 1, 2, 5 seconds, then 5 s.
_RECONNECT_SCHEDULE: tuple[float, ...] = (0.5, 1.0, 2.0, 5.0)


StateCallback = Callable[[str], Awaitable[None]]


def _stream_url(base_url: str, session_id: str, cursor: str | None) -> str:
    base = base_url.rstrip("/")
    path = f"/api/v1/pi/sessions/{quote(session_id, safe='')}/stream"
    if cursor is None or cursor == "":
        return f"{base}{path}"
    return f"{base}{path}?{urlencode({'after': cursor})}"


async def watch_session_state(
    base_url: str,
    session_id: str,
    initial_state: str,
    on_state: StateCallback,
    *,
    stop_event: asyncio.Event | None = None,
    debounce_seconds: float = 0.25,
    client_factory: Callable[[], httpx.AsyncClient] | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> None:
    """Tail ``/api/v1/pi/sessions/{id}/stream`` and emit state callbacks.

    The watcher:

    - emits the initial state synchronously before the first request,
    - opens a streaming GET against the orchestrator SSE endpoint with the
      last-seen ``after`` cursor,
    - feeds the parser with each bytes chunk and consumes the durable
      ``{sequence, event_type, payload}`` events,
    - debounces state changes by ``debounce_seconds`` so a rapid burst of
      events collapses into a single ``on_state`` callback,
    - reconnects with the bounded exponential schedule
      ``0.5, 1, 2, 5, 5, 5, …`` whenever the connection is dropped or an
      HTTP error is returned,
    - renders :data:`DISCONNECTED` whenever the orchestrator is unavailable
      after an attempt has been established or an HTTP error is returned,
    - honours ``stop_event`` and :class:`asyncio.CancelledError`.

    Cancellation and ``stop_event`` propagate normally; every other watcher
    failure is contained so the terminal attachment loop can keep running.
    """

    tracker = SessionStateTracker(initial_state)
    cursor: str | None = None
    last_emitted = tracker.state
    await on_state(last_emitted)

    backoff_index = 0
    debounce_task: asyncio.Task[None] | None = None

    def should_stop() -> bool:
        return stop_event is not None and stop_event.is_set()

    async def emit_if_changed(new_state: str) -> None:
        nonlocal last_emitted
        if new_state == last_emitted:
            return
        last_emitted = new_state
        await on_state(new_state)

    async def render_disconnected(reason: str) -> None:
        nonlocal debounce_task
        if debounce_task is not None:
            debounce_task.cancel()
            await asyncio.gather(debounce_task, return_exceptions=True)
            debounce_task = None
        tracker.reset_to_disconnected()
        await emit_if_changed(DISCONNECTED)
        logger.debug("pi session watcher disconnected: %s", reason)

    async def delayed_emit(state: str) -> None:
        await sleep(max(0.0, debounce_seconds))
        if tracker.state == state:
            await emit_if_changed(state)

    def schedule_debounce(state: str) -> None:
        nonlocal debounce_task
        if debounce_task is not None:
            debounce_task.cancel()
        debounce_task = asyncio.create_task(delayed_emit(state))

    while not should_stop():
        url = _stream_url(base_url, session_id, cursor)
        parser = SSEParser()
        client = client_factory() if client_factory is not None else httpx.AsyncClient()
        owns_client = client_factory is None
        disconnected_reason = "stream closed"
        try:
            async with client.stream(
                "GET",
                url,
                timeout=httpx.Timeout(connect=5.0, read=45.0, write=5.0, pool=5.0),
            ) as response:
                if response.status_code != 200:
                    disconnected_reason = f"http status {response.status_code}"
                else:
                    backoff_index = 0
                    async for chunk in response.aiter_bytes():
                        if should_stop():
                            break
                        for event in parser.feed(chunk):
                            payload = event.data if isinstance(event.data, dict) else {}
                            sequence = payload.get("sequence")
                            if sequence is not None:
                                cursor = str(sequence)
                            elif event.id is not None:
                                cursor = event.id
                            if tracker.feed(payload):
                                schedule_debounce(tracker.state)
        except asyncio.CancelledError:
            raise
        except (httpx.HTTPError, OSError, asyncio.TimeoutError) as exc:
            disconnected_reason = f"{type(exc).__name__}: {exc}"
        except Exception as exc:  # pragma: no cover — defensive containment
            logger.exception("pi session watcher unexpected error: %s", exc)
            disconnected_reason = f"{type(exc).__name__}: {exc}"
        finally:
            if owns_client:
                await client.aclose()

        if should_stop():
            break
        await render_disconnected(disconnected_reason)
        await _respectful_sleep(sleep, _RECONNECT_SCHEDULE[backoff_index])
        backoff_index = min(backoff_index + 1, len(_RECONNECT_SCHEDULE) - 1)

    # Final cancel of any pending debounced emission so we never leak a
    # callback after the watcher returns.
    if debounce_task is not None:
        debounce_task.cancel()
        await asyncio.gather(debounce_task, return_exceptions=True)


async def _respectful_sleep(
    sleep: Callable[[float], Awaitable[None]], seconds: float
) -> None:
    if seconds <= 0:
        # Yield once so cancellation can be observed promptly.
        await sleep(0)
        return
    await sleep(seconds)


__all__ = [
    "WORKING",
    "IDLE",
    "ERROR",
    "DISCONNECTED",
    "ALL_STATES",
    "sanitize_name",
    "tab_title",
    "SessionStateTracker",
    "SSEParser",
    "SSEEvent",
    "watch_session_state",
]