"""Loopback Pi-session relay served by a worker daemon.

The relay is intentionally narrow at this stage: it publishes worker capability,
keeps a small delegated-session registry, and proves the direct Tailnet
WebSocket transport.  It does not start Pi processes or own a PTY yet.
"""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from dataclasses import dataclass, field
from time import time
from typing import Literal

import uvicorn
from fastapi import FastAPI, HTTPException, WebSocket
from pydantic import BaseModel, Field

PROTOCOL_VERSION = 1


class SessionRegistration(BaseModel):
    """Minimal worker-local delegated-session record.

    The bridge/worker launcher will become the writer in the next milestone.
    Keeping this model here makes the WebSocket endpoint useful and testable
    without prematurely exposing a shell or terminal implementation.
    """

    session_id: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9._:-]+$")
    state: Literal["starting", "working", "idle", "stopped"] = "starting"
    parent_session_id: str | None = Field(default=None, max_length=128)


class SessionRecord(SessionRegistration):
    registered_at: int


@dataclass
class RelayState:
    """In-memory registry owned by one worker-daemon process."""

    sessions: dict[str, SessionRecord] = field(default_factory=dict)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def register(self, registration: SessionRegistration) -> SessionRecord:
        record = SessionRecord(**registration.model_dump(), registered_at=int(time()))
        async with self._lock:
            self.sessions[record.session_id] = record
        return record

    async def get(self, session_id: str) -> SessionRecord | None:
        async with self._lock:
            return self.sessions.get(session_id)

    async def list(self) -> list[SessionRecord]:
        async with self._lock:
            return sorted(self.sessions.values(), key=lambda item: item.registered_at)

    async def remove(self, session_id: str) -> bool:
        async with self._lock:
            return self.sessions.pop(session_id, None) is not None


def create_relay_app(state: RelayState | None = None) -> FastAPI:
    """Create the loopback-only HTTP/WebSocket application."""

    relay_state = state or RelayState()
    app = FastAPI(title="Worker Harness Pi Relay", docs_url=None, redoc_url=None)
    app.state.pi_relay = relay_state

    @app.get("/healthz")
    async def healthz() -> dict[str, int | str]:
        return {
            "status": "healthy",
            "protocol_version": PROTOCOL_VERSION,
            "session_count": len(await relay_state.list()),
        }

    @app.get("/v1/sessions", response_model=list[SessionRecord])
    async def list_sessions() -> list[SessionRecord]:
        return await relay_state.list()

    @app.post("/v1/sessions", response_model=SessionRecord, status_code=201)
    async def register_session(registration: SessionRegistration) -> SessionRecord:
        return await relay_state.register(registration)

    @app.delete("/v1/sessions/{session_id}", status_code=204)
    async def remove_session(session_id: str) -> None:
        if not await relay_state.remove(session_id):
            raise HTTPException(status_code=404, detail="unknown session")

    @app.websocket("/v1/sessions/{session_id}/attach")
    async def attach_session(websocket: WebSocket, session_id: str) -> None:
        """Establish the direct WebSocket path before PTY support lands.

        The client always receives a typed status frame after a successful
        WebSocket upgrade.  A future terminal relay keeps this URL and adds
        input/output/resize frames rather than replacing the transport.
        """

        await websocket.accept()
        session = await relay_state.get(session_id)
        if session is None:
            await websocket.send_json(
                {
                    "type": "error",
                    "code": "session_not_found",
                    "session_id": session_id,
                }
            )
            await websocket.close(code=4404, reason="unknown session")
            return

        await websocket.send_json(
            {
                "type": "status",
                "session_id": session.session_id,
                "state": session.state,
                "terminal": "not_ready",
                "protocol_version": PROTOCOL_VERSION,
            }
        )
        await websocket.close(code=1013, reason="terminal relay is not installed")

    return app


class _EmbeddedUvicornServer(uvicorn.Server):
    """Uvicorn variant whose parent daemon, rather than Uvicorn, owns signals.

    Uvicorn normally re-raises a captured SIGTERM after its own shutdown. In
    an embedded server that bypasses the daemon's `finally` block and leaves
    the persistent `tailscale serve --bg` rule behind.
    """

    @contextmanager
    def capture_signals(self):  # type: ignore[override]
        yield


class RelayServer:
    """Lifecycle wrapper for Uvicorn bound exclusively to worker loopback."""

    def __init__(self, port: int, state: RelayState | None = None) -> None:
        if not 1 <= port <= 65535:
            raise ValueError("Pi relay port must be in range 1..65535")
        self.port = port
        self.app = create_relay_app(state)
        self._server = _EmbeddedUvicornServer(
            uvicorn.Config(
                self.app,
                host="127.0.0.1",
                port=port,
                log_level="warning",
                access_log=False,
                lifespan="off",
            )
        )
        self._task: asyncio.Task[None] | None = None

    @property
    def is_running(self) -> bool:
        return self._task is not None and not self._task.done() and self._server.started

    async def start(self, timeout_seconds: float = 5.0) -> None:
        if self._task is not None:
            raise RuntimeError("Pi relay is already started")
        self._task = asyncio.create_task(self._server.serve(), name="wh-pi-relay")
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        while not self._server.started:
            if self._task.done():
                await self._task
                raise RuntimeError("Pi relay exited before it started")
            if asyncio.get_running_loop().time() >= deadline:
                await self.stop()
                raise TimeoutError("Pi relay did not bind before startup timeout")
            await asyncio.sleep(0.01)

    async def stop(self, timeout_seconds: float = 5.0) -> None:
        if self._task is None:
            return
        self._server.should_exit = True
        try:
            await asyncio.wait_for(self._task, timeout=timeout_seconds)
        except TimeoutError:
            self._server.force_exit = True
            await self._task
        finally:
            self._task = None
