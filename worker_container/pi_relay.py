"""Direct Tailnet relay and local tmux owner for delegated Pi sessions.

The process binds only to worker loopback. ``worker_daemon`` publishes that
single port through userspace Tailscale Serve.  Tailnet membership is the
operator boundary, so this service deliberately has no second credential
layer; it is nevertheless narrow: it can start only the configured Pi command,
not arbitrary shell commands supplied by a client.
"""

from __future__ import annotations

import asyncio
import fcntl
import json
import os
import pty
import re
import shlex
import shutil
import signal
import struct
import subprocess
import termios
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from time import time
from typing import Literal

import uvicorn
from fastapi import FastAPI, HTTPException, WebSocket
from pydantic import BaseModel, Field

PROTOCOL_VERSION = 2
_SESSION_ID = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")


class SessionCreate(BaseModel):
    """Requested delegated Pi session. The worker determines its command."""

    session_id: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9._:-]+$")
    parent_session_id: str | None = Field(default=None, max_length=128)
    task: str = Field(default="", max_length=100_000)
    cwd: str | None = Field(default=None, max_length=4096)


class SessionRecord(BaseModel):
    session_id: str
    parent_session_id: str | None = None
    task: str = ""
    cwd: str
    tmux_session: str
    state: Literal["starting", "working", "idle", "stopped", "failed"] = "starting"
    detail: str = ""
    created_at: int
    updated_at: int


@dataclass
class RelayState:
    """Durable local session ownership; tmux remains alive across attaches."""

    root: Path
    command: str
    default_cwd: Path
    tmux_tmpdir: Path | None = None
    agent_config: Path | None = None
    # Optional orchestrator ingest hook. When set, the relay uploades state
    # transitions and events so the orchestrator's projection stays truthful.
    orchestrator_url: str | None = None
    worker_id: str | None = None
    sessions: dict[str, SessionRecord] = field(default_factory=dict)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    _observer_task: asyncio.Task[None] | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        self.default_cwd = self.default_cwd.expanduser().resolve()
        self.tmux_tmpdir = (self.tmux_tmpdir or (self.root / "tmux")).expanduser().resolve()
        self.tmux_tmpdir.mkdir(parents=True, exist_ok=True)
        self.tmux_tmpdir.chmod(0o1777)
        self._load()

    @staticmethod
    def _tmux_name(session_id: str) -> str:
        # The API regex above means this transformation remains deterministic
        # and prevents a session ID being interpreted as a tmux option.
        return "wh_pi_" + re.sub(r"[^A-Za-z0-9_]", "_", session_id)

    def _path(self, session_id: str) -> Path:
        if not _SESSION_ID.fullmatch(session_id):
            raise ValueError("invalid session id")
        return self.root / session_id / "session.json"

    def _persist(self, record: SessionRecord) -> None:
        path = self._path(record.session_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_suffix(".tmp")
        temp.write_text(record.model_dump_json(indent=2) + "\n", encoding="utf-8")
        temp.replace(path)

    def _load(self) -> None:
        for path in self.root.glob("*/session.json"):
            try:
                record = SessionRecord.model_validate_json(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            self.sessions[record.session_id] = record

    async def _tmux(self, *args: str) -> subprocess.CompletedProcess[str]:
        env = os.environ.copy()
        env["TMUX_TMPDIR"] = str(self.tmux_tmpdir)
        return await asyncio.to_thread(
            subprocess.run,
            ["tmux", *args],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=15,
            env=env,
        )

    async def _refresh(self, record: SessionRecord) -> SessionRecord:
        if record.state not in {"stopped", "failed"}:
            result = await self._tmux("has-session", "-t", record.tmux_session)
            if result.returncode != 0:
                record.state = "stopped"
                record.detail = "tmux session exited"
                record.updated_at = int(time())
                self._persist(record)
                await self._upload_state(record, "tmux-session-exited")
        return record

    async def _upload_state(self, record: SessionRecord, event_type: str) -> None:
        """Report a state transition to the orchestrator (best-effort)."""
        if not self.orchestrator_url or not self.worker_id:
            return
        import urllib.request

        url = (
            f"{self.orchestrator_url.rstrip('/')}/pi/worker/{self.worker_id}"
            f"/sessions/{record.session_id}/events"
        )
        payload = {
            "session_id": record.session_id,
            "state": record.state,
            "detail": record.detail,
            "events": [{"event_type": event_type, "payload": {"tmux_session": record.tmux_session}}],
        }
        body = json.dumps(payload).encode()

        def _send() -> None:
            req = urllib.request.Request(url, data=body, headers={"content-type": "application/json"}, method="POST")
            urllib.request.urlopen(req, timeout=5).read()

        try:
            await asyncio.to_thread(_send)
        except Exception:
            # Worker remains the source of truth on its local record; the
            # orchestrator catch-up happens via the next operator-initiated
            # command or the next state-poll if implemented.
            return

    async def create(self, request: SessionCreate) -> SessionRecord:
        async with self._lock:
            existing = self.sessions.get(request.session_id)
            if existing:
                return await self._refresh(existing)

            cwd = Path(request.cwd).expanduser().resolve() if request.cwd else self.default_cwd
            if not cwd.is_dir():
                raise ValueError(f"cwd is not a directory: {cwd}")
            now = int(time())
            record = SessionRecord(
                session_id=request.session_id,
                parent_session_id=request.parent_session_id,
                task=request.task,
                cwd=str(cwd),
                tmux_session=self._tmux_name(request.session_id),
                state="starting",
                created_at=now,
                updated_at=now,
            )
            self.sessions[record.session_id] = record
            self._persist(record)

            # Each child gets an isolated home, with the immutable managed
            # release config copied before launch. This keeps history/auth state
            # out of the image and avoids sharing mutable ~/.pi between children.
            session_home = self._path(record.session_id).parent / "home"
            if self.agent_config and self.agent_config.is_dir():
                destination = session_home / ".pi" / "agent"
                if not destination.exists():
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copytree(self.agent_config, destination)
            session_home.mkdir(parents=True, exist_ok=True)
            # This is an operator-controlled command from worker configuration,
            # never a request field. A requested task is written after launch
            # through normal terminal input.
            command = f"HOME={shlex.quote(str(session_home))} {self.command}"
            result = await self._tmux(
                "new-session", "-d", "-s", record.tmux_session, "-c", record.cwd, command
            )
            if result.returncode != 0:
                record.state = "failed"
                record.detail = result.stderr.strip() or "tmux failed to start Pi"
            else:
                record.state = "working"
                record.detail = "Pi process started"
            record.updated_at = int(time())
            self._persist(record)
            await self._upload_state(record, f"create-{record.state}")

        if record.state == "working" and request.task:
            # Give the TUI a short time to enter raw mode before injecting the
            # first prompt. Subsequent prompts use the same safe tmux path.
            await asyncio.sleep(0.25)
            await self.prompt(record.session_id, request.task)
        return record

    async def get(self, session_id: str) -> SessionRecord | None:
        async with self._lock:
            record = self.sessions.get(session_id)
            return await self._refresh(record) if record else None

    async def list(self) -> list[SessionRecord]:
        async with self._lock:
            return [await self._refresh(record) for record in sorted(self.sessions.values(), key=lambda item: item.created_at)]

    async def prompt(self, session_id: str, message: str) -> SessionRecord:
        if not message:
            raise ValueError("prompt must not be empty")
        async with self._lock:
            record = self.sessions.get(session_id)
            if not record:
                raise KeyError(session_id)
            await self._refresh(record)
            if record.state in {"stopped", "failed"}:
                raise RuntimeError(f"session is {record.state}")
            # -l sends literal text; the message is never evaluated by a shell.
            literal = await self._tmux("send-keys", "-t", record.tmux_session, "-l", message)
            enter = await self._tmux("send-keys", "-t", record.tmux_session, "Enter")
            if literal.returncode or enter.returncode:
                record.state = "failed"
                record.detail = (literal.stderr or enter.stderr).strip() or "failed to send prompt"
            else:
                record.state = "working"
                record.detail = "prompt delivered"
            record.updated_at = int(time())
            self._persist(record)
            await self._upload_state(record, f"prompt-{record.state}")
            return record

    async def cancel(self, session_id: str) -> SessionRecord:
        async with self._lock:
            record = self.sessions.get(session_id)
            if not record:
                raise KeyError(session_id)
            result = await self._tmux("kill-session", "-t", record.tmux_session)
            if result.returncode and "can't find session" not in result.stderr:
                raise RuntimeError(result.stderr.strip() or "failed to stop tmux session")
            record.state = "stopped"
            record.detail = "cancelled"
            record.updated_at = int(time())
            self._persist(record)
            await self._upload_state(record, "cancelled")
            return record


def _set_winsize(fd: int, rows: int, cols: int) -> None:
    if rows < 1 or cols < 1 or rows > 1000 or cols > 1000:
        raise ValueError("terminal dimensions must be between 1 and 1000")
    fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0))


async def _relay_terminal(websocket: WebSocket, record: SessionRecord, tmux_tmpdir: Path) -> None:
    """Attach a disposable tmux client PTY; never kill the underlying Pi."""

    master_fd, slave_fd = pty.openpty()
    _set_winsize(slave_fd, 24, 80)
    env = os.environ.copy()
    env["TMUX_TMPDIR"] = str(tmux_tmpdir)
    # The daemon is launched by systemd, which normally has no TERM. A tmux
    # client refuses to attach without one even though its PTY is valid.
    env.setdefault("TERM", "xterm-256color")
    proc = await asyncio.create_subprocess_exec(
        "tmux", "attach-session", "-t", record.tmux_session,
        stdin=slave_fd, stdout=slave_fd, stderr=slave_fd,
        start_new_session=True,
        env=env,
    )
    os.close(slave_fd)

    async def read_output() -> None:
        try:
            while True:
                chunk = await asyncio.to_thread(os.read, master_fd, 65536)
                if not chunk:
                    return
                await websocket.send_bytes(chunk)
        except OSError:
            return

    async def read_input() -> None:
        while True:
            message = await websocket.receive()
            kind = message.get("type")
            if kind == "websocket.disconnect":
                return
            if message.get("bytes") is not None:
                os.write(master_fd, message["bytes"])
                continue
            text = message.get("text")
            if text is None:
                continue
            try:
                frame = json.loads(text)
            except json.JSONDecodeError:
                os.write(master_fd, text.encode())
                continue
            if frame.get("type") == "resize":
                _set_winsize(master_fd, int(frame["rows"]), int(frame["cols"]))
            elif frame.get("type") == "input":
                os.write(master_fd, str(frame.get("data", "")).encode())

    output_task = asyncio.create_task(read_output())
    input_task = asyncio.create_task(read_input())
    try:
        done, pending = await asyncio.wait({output_task, input_task}, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        for task in done:
            task.result()
    finally:
        if proc.returncode is None:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=2)
            except TimeoutError:
                proc.kill()
                await proc.wait()
        os.close(master_fd)


def create_relay_app(
    state: RelayState | None = None,
    *,
    sessions_root: Path | None = None,
    pi_command: str = "pi",
    default_cwd: Path | None = None,
    tmux_tmpdir: Path | None = None,
    agent_config: Path | None = None,
    orchestrator_url: str | None = None,
    worker_id: str | None = None,
) -> FastAPI:
    """Create the loopback-only HTTP/WebSocket application."""

    relay_state = state or RelayState(
        root=sessions_root or Path("/tmp/wh-pi-sessions"),
        command=pi_command,
        default_cwd=default_cwd or Path.home(),
        tmux_tmpdir=tmux_tmpdir,
        agent_config=agent_config,
        orchestrator_url=orchestrator_url,
        worker_id=worker_id,
    )
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
    async def create_session(request: SessionCreate) -> SessionRecord:
        try:
            return await relay_state.create(request)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.post("/v1/sessions/{session_id}:prompt", response_model=SessionRecord)
    async def prompt_session(session_id: str, payload: dict[str, str]) -> SessionRecord:
        try:
            return await relay_state.prompt(session_id, payload.get("message", ""))
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="unknown session") from exc
        except (ValueError, RuntimeError) as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.post("/v1/sessions/{session_id}:cancel", response_model=SessionRecord)
    async def cancel_session(session_id: str) -> SessionRecord:
        try:
            return await relay_state.cancel(session_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="unknown session") from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.websocket("/v1/sessions/{session_id}/attach")
    async def attach_session(websocket: WebSocket, session_id: str) -> None:
        await websocket.accept()
        session = await relay_state.get(session_id)
        if session is None:
            await websocket.send_json({"type": "error", "code": "session_not_found", "session_id": session_id})
            await websocket.close(code=4404, reason="unknown session")
            return
        if session.state in {"stopped", "failed"}:
            await websocket.send_json({"type": "error", "code": "session_not_running", "state": session.state})
            await websocket.close(code=4409, reason=session.detail[:120])
            return
        await websocket.send_json(
            {
                "type": "status",
                "session_id": session.session_id,
                "state": session.state,
                "terminal": "ready",
                "protocol_version": PROTOCOL_VERSION,
            }
        )
        await _relay_terminal(websocket, session, relay_state.tmux_tmpdir)

    return app


class _EmbeddedUvicornServer(uvicorn.Server):
    """Uvicorn variant whose parent daemon, rather than Uvicorn, owns signals."""

    @contextmanager
    def capture_signals(self):  # type: ignore[override]
        yield


class RelayServer:
    """Lifecycle wrapper for Uvicorn bound exclusively to worker loopback."""

    def __init__(
        self,
        port: int,
        state: RelayState | None = None,
        *,
        sessions_root: Path | None = None,
        pi_command: str = "pi",
        default_cwd: Path | None = None,
        tmux_tmpdir: Path | None = None,
        agent_config: Path | None = None,
        orchestrator_url: str | None = None,
        worker_id: str | None = None,
    ) -> None:
        if not 1 <= port <= 65535:
            raise ValueError("Pi relay port must be in range 1..65535")
        self.port = port
        self.app = create_relay_app(
            state,
            sessions_root=sessions_root,
            pi_command=pi_command,
            default_cwd=default_cwd,
            tmux_tmpdir=tmux_tmpdir,
            agent_config=agent_config,
            orchestrator_url=orchestrator_url,
            worker_id=worker_id,
        )
        self._server = _EmbeddedUvicornServer(
            uvicorn.Config(self.app, host="127.0.0.1", port=port, log_level="warning", access_log=False, lifespan="off")
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
