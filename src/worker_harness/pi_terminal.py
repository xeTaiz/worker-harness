"""Native terminal client for Pi session relay attachments."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import signal
import subprocess
import sys
import termios
import tty
from contextlib import contextmanager
from pathlib import Path
from typing import Any, AsyncIterator
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

DETACH_BYTE = b"\x1d"  # Ctrl-]


def _relay_socket_path() -> Path:
    configured = os.environ.get("WH_PI_HOST_RELAY_SOCKET")
    if configured:
        return Path(configured)
    runtime = os.environ.get("XDG_RUNTIME_DIR") or f"/tmp/worker-harness-{os.getuid()}"
    return Path(runtime) / "worker-harness" / "pi-host-relay.sock"


async def _relay_request(payload: dict[str, Any]) -> dict[str, Any]:
    reader, writer = await asyncio.wait_for(
        asyncio.open_unix_connection(_relay_socket_path()), timeout=0.5
    )
    try:
        writer.write(json.dumps(payload).encode() + b"\n")
        await writer.drain()
        raw = await asyncio.wait_for(reader.readline(), timeout=0.5)
        if not raw:
            raise RuntimeError("local Pi host relay closed without a response")
        response = json.loads(raw)
        if not response.get("ok"):
            raise RuntimeError(str(response.get("error") or "local Pi session is unavailable"))
        return response
    finally:
        writer.close()
        await writer.wait_closed()


async def focus_local_session(session_id: str) -> bool:
    """Switch the invoking tmux client when the requested Pi pane is local."""

    current_tmux = os.environ.get("TMUX", "").split(",", 1)[0]
    if not current_tmux:
        return False
    try:
        route = await _relay_request({"action": "describe", "session_id": session_id})
    except (OSError, asyncio.TimeoutError, RuntimeError, json.JSONDecodeError):
        return False
    route_socket = str(route.get("tmux_socket") or "")
    if not route_socket or os.path.realpath(route_socket) != os.path.realpath(current_tmux):
        return False
    target = (
        f"{route.get('tmux_session')}:{route.get('window_index')}"
        f".{route.get('pane_index')}"
    )
    result = subprocess.run(
        [
            "tmux", "-S", route_socket,
            "set-option", "-p", "-t", target, "@wh_pi_attach_session", session_id, ";",
            "set-option", "-p", "-t", target, "@wh_pi_attach_mode", "local", ";",
            "switch-client", "-t", target, ";",
            "select-pane", "-t", target,
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        timeout=3,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"could not focus local tmux pane: {result.stderr.strip()}")
    return True


def terminal_size(fd: int) -> tuple[int, int]:
    try:
        size = os.get_terminal_size(fd)
    except OSError:
        size = shutil.get_terminal_size(fallback=(80, 24))
    return max(1, size.lines), max(1, size.columns)


@contextmanager
def raw_terminal(fd: int):
    if not os.isatty(fd):
        raise RuntimeError("terminal attachment requires an interactive TTY")
    original = termios.tcgetattr(fd)
    try:
        tty.setraw(fd)
        yield
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, original)


async def _stdin_chunks(fd: int) -> AsyncIterator[bytes]:
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue[bytes | BaseException] = asyncio.Queue()

    def readable() -> None:
        try:
            queue.put_nowait(os.read(fd, 65536))
        except BaseException as exc:  # surface terminal read failures in the coroutine
            queue.put_nowait(exc)

    loop.add_reader(fd, readable)
    try:
        while True:
            item = await queue.get()
            if isinstance(item, BaseException):
                raise item
            if not item:
                return
            yield item
    finally:
        loop.remove_reader(fd)


async def _send_input(websocket: Any, stdin_fd: int) -> None:
    async for chunk in _stdin_chunks(stdin_fd):
        detach = chunk.find(DETACH_BYTE)
        if detach >= 0:
            if detach:
                await websocket.send(chunk[:detach])
            return
        await websocket.send(chunk)


async def _send_resizes(websocket: Any, stdout_fd: int, changed: asyncio.Event) -> None:
    last_size: tuple[int, int] | None = None
    while True:
        size = terminal_size(stdout_fd)
        if size != last_size:
            rows, cols = size
            await websocket.send(json.dumps({"type": "resize", "rows": rows, "cols": cols}))
            last_size = size
        # SIGWINCH is the fast path. Polling closes gaps in nested tmux and on
        # platforms that update the PTY dimensions without delivering it.
        try:
            await asyncio.wait_for(changed.wait(), timeout=0.5)
        except asyncio.TimeoutError:
            pass
        changed.clear()


async def _receive_output(websocket: Any, stdout_fd: int) -> None:
    async for message in websocket:
        if isinstance(message, bytes):
            os.write(stdout_fd, message)
            continue
        try:
            frame = json.loads(message)
        except json.JSONDecodeError:
            os.write(stdout_fd, message.encode())
            continue
        if frame.get("type") == "error":
            raise RuntimeError(str(frame.get("detail") or "terminal relay reported an error"))
        # Status frames are protocol metadata; tmux's binary redraw is the UI.


def terminal_url(websocket_url: str, rows: int, cols: int) -> str:
    """Include initial PTY dimensions in the WebSocket upgrade request."""

    parts = urlsplit(websocket_url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query.update({"rows": str(rows), "cols": str(cols)})
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


async def attach_terminal(
    websocket_url: str,
    *,
    stdin_fd: int | None = None,
    stdout_fd: int | None = None,
    cycle_requests: asyncio.Queue[str] | None = None,
) -> str | None:
    """Attach until disconnect/Ctrl-], returning a requested cycle direction."""

    stdin_fd = sys.stdin.fileno() if stdin_fd is None else stdin_fd
    stdout_fd = sys.stdout.fileno() if stdout_fd is None else stdout_fd
    if not websocket_url.startswith(("ws://", "wss://")):
        raise RuntimeError("attach-info returned an invalid WebSocket URL")

    initial_rows, initial_cols = terminal_size(stdout_fd)
    websocket_url = terminal_url(websocket_url, initial_rows, initial_cols)

    resize_changed = asyncio.Event()
    loop = asyncio.get_running_loop()
    signal_installed = False
    try:
        loop.add_signal_handler(signal.SIGWINCH, resize_changed.set)
        signal_installed = True
    except (NotImplementedError, RuntimeError):
        pass

    try:
        async with connect(
            websocket_url,
            max_size=None,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=2,
        ) as websocket:
            with raw_terminal(stdin_fd):
                tasks = {
                    asyncio.create_task(_send_input(websocket, stdin_fd), name="pi-attach-input"),
                    asyncio.create_task(_receive_output(websocket, stdout_fd), name="pi-attach-output"),
                    asyncio.create_task(_send_resizes(websocket, stdout_fd, resize_changed), name="pi-attach-resize"),
                }
                cycle_task: asyncio.Task[str] | None = None
                if cycle_requests is not None:
                    cycle_task = asyncio.create_task(cycle_requests.get(), name="pi-attach-cycle")
                    tasks.add(cycle_task)
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                direction = cycle_task.result() if cycle_task in done else None
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
                for task in done:
                    if task is not cycle_task:
                        task.result()
                return direction
    except (OSError, WebSocketException, asyncio.TimeoutError) as exc:
        raise RuntimeError(f"terminal relay unavailable: {exc}") from exc
    finally:
        if signal_installed:
            loop.remove_signal_handler(signal.SIGWINCH)
