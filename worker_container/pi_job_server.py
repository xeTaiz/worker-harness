"""Private loopback job service for delegated Pi ``bash`` calls.

This service is deliberately separate from the Tailnet-published terminal relay.
Only a child launched by the worker relay receives its loopback URL; requests are
bound to the path session ID, which becomes the immutable job origin.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import shlex
import subprocess
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from time import time
from typing import Literal
from uuid import uuid4

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from pi_relay import RelayState

_SESSION_ID = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
_JOB_ID = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
_EXIT_MARKER = re.compile(r"(?m)^EXIT:(-?\d+)\s*$")
_OUTPUT_LIMIT = 50 * 1024


class DelegatedBashRequest(BaseModel):
    command: str = Field(min_length=1, max_length=100_000)
    cwd: str | None = Field(default=None, max_length=4096)
    timeout: float | None = Field(default=None, gt=0, le=86_400)


class SessionStateUpdate(BaseModel):
    state: Literal["working", "idle"]
    detail: str = Field(default="", max_length=4096)
    event_type: str = Field(default="bridge-state", min_length=1, max_length=64)


class JobReport(BaseModel):
    """Wire shape accepted by the registration-port worker job ingest API."""

    id: str
    origin_session_id: str
    tmux_session: str
    command: str
    status: Literal["pending", "running", "done", "failed"]
    exit_code: int | None = None
    pty_enabled: bool = True
    started_at: int
    finished_at: int = 0
    report_revision: int = Field(ge=1)


class LocalJobRecord(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    origin_session_id: str
    tmux_session: str
    command: str
    cwd: str
    status: Literal["pending", "running", "done", "failed"] = "pending"
    exit_code: int | None = None
    started_at: int = 0
    finished_at: int = 0
    report_revision: int = 0
    # Absolute epoch deadline survives worker-daemon restarts.  Zero means no
    # requested timeout.
    deadline_at: float = 0.0
    outbox: list[JobReport] = Field(default_factory=list)


@dataclass
class PiJobService:
    """Durable local tmux executor plus worker-job report outbox."""

    sessions: RelayState
    sessions_root: Path
    harness_dir: Path
    tmux_tmpdir: Path
    orchestrator_url: str | None = None
    worker_id: str | None = None
    proxy: str | None = None
    jobs: dict[str, LocalJobRecord] = field(default_factory=dict)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    _outbox_task: asyncio.Task[None] | None = field(default=None, init=False)
    _monitor_tasks: dict[str, asyncio.Task[None]] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        self.sessions_root = self.sessions_root.expanduser().resolve()
        self.harness_dir = self.harness_dir.expanduser().resolve()
        self.tmux_tmpdir = self.tmux_tmpdir.expanduser().resolve()
        self.harness_dir.mkdir(parents=True, exist_ok=True)
        self.tmux_tmpdir.mkdir(parents=True, exist_ok=True)
        self.tmux_tmpdir.chmod(0o1777)
        self._load()

    @staticmethod
    def _validate_session_id(session_id: str) -> None:
        if not _SESSION_ID.fullmatch(session_id):
            raise ValueError("invalid session id")

    @staticmethod
    def _validate_job_id(job_id: str) -> None:
        if not _JOB_ID.fullmatch(job_id):
            raise ValueError("invalid job id")

    def _metadata_path(self, origin_session_id: str, job_id: str) -> Path:
        self._validate_session_id(origin_session_id)
        self._validate_job_id(job_id)
        return self.sessions_root / origin_session_id / "jobs" / job_id / "job.json"

    def _job_dir(self, job_id: str) -> Path:
        self._validate_job_id(job_id)
        return self.harness_dir / job_id

    def _log_path(self, job_id: str) -> Path:
        return self._job_dir(job_id) / "output.log"

    def _exit_path(self, job_id: str) -> Path:
        # Unlike a user command's stdout, this daemon-owned file cannot be
        # forged by output such as `printf EXIT:0`.
        return self._job_dir(job_id) / "exit-code"

    def _persist(self, record: LocalJobRecord) -> None:
        path = self._metadata_path(record.origin_session_id, record.id)
        path.parent.mkdir(parents=True, exist_ok=True)
        pending = path.with_suffix(".tmp")
        pending.write_text(record.model_dump_json(indent=2) + "\n", encoding="utf-8")
        pending.replace(path)

    def _load(self) -> None:
        for path in self.sessions_root.glob("*/jobs/*/job.json"):
            try:
                record = LocalJobRecord.model_validate_json(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            self.jobs[record.id] = record

    def _clear_stale_tmux_socket(self) -> None:
        socket_path = self.tmux_tmpdir / f"tmux-{os.getuid()}" / "default"
        try:
            socket_path.unlink(missing_ok=True)
        except OSError:
            pass

    async def _tmux(self, *args: str) -> subprocess.CompletedProcess[str]:
        env = os.environ.copy()
        env["TMUX_TMPDIR"] = str(self.tmux_tmpdir)

        def run() -> subprocess.CompletedProcess[str]:
            result = subprocess.run(
                ["tmux", *args],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
                timeout=15,
                env=env,
            )
            if result.returncode and "no server running" in result.stderr:
                self._clear_stale_tmux_socket()
                result = subprocess.run(
                    ["tmux", *args],
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    check=False,
                    timeout=15,
                    env=env,
                )
            return result

        return await asyncio.to_thread(run)

    @staticmethod
    def _report(record: LocalJobRecord) -> JobReport:
        return JobReport(
            id=record.id,
            origin_session_id=record.origin_session_id,
            tmux_session=record.tmux_session,
            command=record.command,
            status=record.status,
            exit_code=record.exit_code,
            started_at=record.started_at,
            finished_at=record.finished_at,
            report_revision=record.report_revision,
        )

    def _enqueue_report(self, record: LocalJobRecord) -> None:
        record.outbox.append(self._report(record))
        self._persist(record)

    async def _flush_record_outbox(self, record: LocalJobRecord) -> bool:
        if not self.orchestrator_url or not self.worker_id:
            return False
        url = f"{self.orchestrator_url.rstrip('/')}/pi/worker/{self.worker_id}/jobs"
        while record.outbox:
            pending = record.outbox[0]
            try:
                async with httpx.AsyncClient(proxy=self.proxy or None, timeout=5.0) as client:
                    response = await client.post(url, json={"jobs": [pending.model_dump(mode="json")]})
                    response.raise_for_status()
            except Exception:
                # Do not drop the report.  Its revision and origin are stable,
                # so an eventual retry is safe for the orchestrator upsert.
                return False
            record.outbox.pop(0)
            self._persist(record)
        return True

    async def _flush_all_outboxes(self) -> None:
        # Snapshot values before any network await so creation/monitor paths
        # can continue updating the durable records during an ingest outage.
        for record in list(self.jobs.values()):
            await self._flush_record_outbox(record)

    async def _outbox_loop(self) -> None:
        while True:
            await self._flush_all_outboxes()
            await asyncio.sleep(2)

    def _ensure_monitor(self, job_id: str) -> asyncio.Task[None]:
        task = self._monitor_tasks.get(job_id)
        if task is None or task.done():
            task = asyncio.create_task(self._monitor(job_id), name=f"wh-pi-job-{job_id}")
            self._monitor_tasks[job_id] = task
        return task

    async def start(self) -> None:
        async with self._lock:
            for record in self.jobs.values():
                # Reconciliation after daemon restart: preserve unacknowledged
                # reports; otherwise re-advertise the current projection.
                if not record.outbox:
                    self._enqueue_report(record)
                if record.status in {"pending", "running"}:
                    self._ensure_monitor(record.id)
        await self._flush_all_outboxes()
        if self._outbox_task is None:
            self._outbox_task = asyncio.create_task(self._outbox_loop(), name="wh-pi-job-outbox")

    async def stop(self) -> None:
        if self._outbox_task is not None:
            self._outbox_task.cancel()
            try:
                await self._outbox_task
            except asyncio.CancelledError:
                pass
            self._outbox_task = None
        tasks = list(self._monitor_tasks.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._monitor_tasks.clear()

    async def _require_running_origin(self, session_id: str) -> None:
        self._validate_session_id(session_id)
        session = await self.sessions.get(session_id)
        if session is None:
            raise KeyError(session_id)
        if session.state in {"stopped", "failed"}:
            raise RuntimeError(f"session is {session.state}")

    def _write_script(self, record: LocalJobRecord) -> Path:
        job_dir = self._job_dir(record.id)
        job_dir.mkdir(parents=True, exist_ok=True)
        log_path = self._log_path(record.id)
        script_path = job_dir / "script.sh"
        script = "\n".join(
            [
                "#!/usr/bin/env bash",
                "set +e",
                f"exec >>{shlex.quote(str(log_path))} 2>&1",
                f"cd -- {shlex.quote(record.cwd)}",
                "ec=$?",
                "if [ \"$ec\" -ne 0 ]; then",
                "  echo \"EXIT:$ec\"",
                "  exit \"$ec\"",
                "fi",
                record.command,
                "ec=$?",
                f"printf '%s\\n' \"$ec\" > {shlex.quote(str(self._exit_path(record.id)))}.tmp",
                f"mv {shlex.quote(str(self._exit_path(record.id)))}.tmp {shlex.quote(str(self._exit_path(record.id)))}",
                # Keep the legacy log marker for existing wh_get_job_logs
                # semantics, but never use it as the executor completion signal.
                "echo \"EXIT:$ec\"",
                # Preserve the tmux session briefly for live worker inspection;
                # the EXIT marker remains the canonical completion signal.
                "sleep 60",
                f"tmux kill-session -t {shlex.quote(record.tmux_session)} 2>/dev/null || true",
                "exit \"$ec\"",
                "",
            ]
        )
        script_path.write_text(script, encoding="utf-8")
        script_path.chmod(0o700)
        return script_path

    async def create_job(self, session_id: str, request: DelegatedBashRequest) -> LocalJobRecord:
        await self._require_running_origin(session_id)
        cwd = Path(request.cwd).expanduser().resolve() if request.cwd else Path.cwd()
        if not cwd.is_dir():
            raise ValueError(f"cwd is not a directory: {cwd}")
        now = int(time())
        record = LocalJobRecord(
            origin_session_id=session_id,
            tmux_session=f"wh_{uuid4()}",
            command=request.command,
            cwd=str(cwd),
            status="pending",
            started_at=now,
            deadline_at=time() + request.timeout if request.timeout else 0.0,
        )
        async with self._lock:
            self.jobs[record.id] = record
            self._persist(record)
            script_path = self._write_script(record)
            result = await self._tmux("new-session", "-d", "-s", record.tmux_session, "bash", str(script_path))
            if result.returncode:
                record.status = "failed"
                record.exit_code = -1
                record.finished_at = int(time())
                self._log_path(record.id).parent.mkdir(parents=True, exist_ok=True)
                self._exit_path(record.id).write_text("-1\n", encoding="utf-8")
                with self._log_path(record.id).open("a", encoding="utf-8") as log:
                    log.write((result.stderr.strip() or "failed to start delegated job") + "\nEXIT:-1\n")
            else:
                record.status = "running"
            record.report_revision += 1
            self._enqueue_report(record)
        return record

    @staticmethod
    def _read_exit_code(path: Path) -> int | None:
        try:
            text = path.read_text(encoding="utf-8").strip()
            return int(text)
        except (FileNotFoundError, ValueError):
            return None

    @staticmethod
    def _read_output(path: Path) -> str:
        try:
            with path.open("rb") as handle:
                handle.seek(0, os.SEEK_END)
                size = handle.tell()
                offset = max(0, size - _OUTPUT_LIMIT)
                handle.seek(offset)
                data = handle.read().decode("utf-8", errors="replace")
        except FileNotFoundError:
            return ""
        lines = [line for line in data.splitlines() if not _EXIT_MARKER.fullmatch(line)]
        prefix = "[Showing last 50KB of output]\n" if offset else ""
        return prefix + "\n".join(lines)

    async def _finish(self, record: LocalJobRecord, exit_code: int) -> None:
        if record.status in {"done", "failed"}:
            return
        record.status = "done" if exit_code == 0 else "failed"
        record.exit_code = exit_code
        record.finished_at = int(time())
        record.report_revision += 1
        self._enqueue_report(record)

    async def _monitor(self, job_id: str) -> None:
        while True:
            async with self._lock:
                record = self.jobs.get(job_id)
                if not record or record.status in {"done", "failed"}:
                    return
                log_path = self._log_path(job_id)
                exit_path = self._exit_path(job_id)
                deadline_at = record.deadline_at
            exit_code = await asyncio.to_thread(self._read_exit_code, exit_path)
            if exit_code is not None:
                async with self._lock:
                    record = self.jobs.get(job_id)
                    if record:
                        await self._finish(record, exit_code)
                return
            if deadline_at and time() >= deadline_at:
                await self._tmux("kill-session", "-t", record.tmux_session)
                exit_path.write_text("124\n", encoding="utf-8")
                with log_path.open("a", encoding="utf-8") as log:
                    log.write("command timed out\nEXIT:124\n")
                async with self._lock:
                    record = self.jobs.get(job_id)
                    if record:
                        await self._finish(record, 124)
                return
            exists = await self._tmux("has-session", "-t", record.tmux_session)
            if exists.returncode:
                exit_path.write_text("-1\n", encoding="utf-8")
                with log_path.open("a", encoding="utf-8") as log:
                    log.write("delegated job tmux session exited without daemon exit status\nEXIT:-1\n")
                async with self._lock:
                    record = self.jobs.get(job_id)
                    if record:
                        await self._finish(record, -1)
                return
            await asyncio.sleep(0.2)

    async def run(self, session_id: str, request: DelegatedBashRequest) -> tuple[LocalJobRecord, str]:
        record = await self.create_job(session_id, request)
        if record.status in {"pending", "running"}:
            # Shield the monitor from HTTP client disconnects: the durable job
            # must continue and report even when the Pi tool call is cancelled.
            await asyncio.shield(self._ensure_monitor(record.id))
        async with self._lock:
            current = self.jobs[record.id]
            log_path = self._log_path(record.id)
        return current, await asyncio.to_thread(self._read_output, log_path)

    async def get_job(self, session_id: str, job_id: str) -> LocalJobRecord:
        self._validate_session_id(session_id)
        self._validate_job_id(job_id)
        async with self._lock:
            record = self.jobs.get(job_id)
            if not record or record.origin_session_id != session_id:
                raise KeyError(job_id)
            return record


async def _set_session_state(sessions: RelayState, session_id: str, state: Literal["working", "idle"], detail: str, event_type: str) -> None:
    await sessions.set_reported_state(session_id, state, detail=detail, event_type=event_type)


def create_job_app(service: PiJobService) -> FastAPI:
    app = FastAPI(title="Worker Harness delegated Pi job service", docs_url=None, redoc_url=None)
    app.state.pi_job_service = service

    @app.get("/healthz")
    async def healthz() -> dict[str, int | str]:
        return {"status": "healthy", "job_count": len(service.jobs)}

    @app.post("/v1/sessions/{session_id}/jobs")
    async def create_job(session_id: str, request: DelegatedBashRequest):
        try:
            record, output = await service.run(session_id, request)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="unknown session") from exc
        except (ValueError, RuntimeError) as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {
            "id": record.id,
            "origin_session_id": record.origin_session_id,
            "tmux_session": record.tmux_session,
            "status": record.status,
            "exit_code": record.exit_code,
            "started_at": record.started_at,
            "finished_at": record.finished_at,
            "output": output,
        }

    @app.get("/v1/sessions/{session_id}/jobs/{job_id}")
    async def get_job(session_id: str, job_id: str):
        try:
            return (await service.get_job(session_id, job_id)).model_dump(mode="json")
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=404, detail="unknown job") from exc

    @app.post("/v1/sessions/{session_id}/state")
    async def update_session_state(session_id: str, payload: SessionStateUpdate):
        try:
            await _set_session_state(service.sessions, session_id, payload.state, payload.detail, payload.event_type)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="unknown session") from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {"session_id": session_id, "state": payload.state}

    return app


class _EmbeddedUvicornServer(uvicorn.Server):
    @contextmanager
    def capture_signals(self):  # type: ignore[override]
        yield


class PiJobServer:
    """Lifecycle wrapper for the unadvertised loopback-only job API."""

    def __init__(self, port: int, service: PiJobService) -> None:
        if not 1 <= port <= 65535:
            raise ValueError("Pi job port must be in range 1..65535")
        self.port = port
        self.service = service
        self.app = create_job_app(service)
        self._server = _EmbeddedUvicornServer(
            uvicorn.Config(self.app, host="127.0.0.1", port=port, log_level="warning", access_log=False, lifespan="off")
        )
        self._task: asyncio.Task[None] | None = None

    async def start(self, timeout_seconds: float = 5.0) -> None:
        if self._task is not None:
            raise RuntimeError("Pi job server is already started")
        await self.service.start()
        self._task = asyncio.create_task(self._server.serve(), name="wh-pi-jobs")
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        while not self._server.started:
            if self._task.done():
                await self._task
                raise RuntimeError("Pi job server exited before it started")
            if asyncio.get_running_loop().time() >= deadline:
                await self.stop()
                raise TimeoutError("Pi job server did not bind before startup timeout")
            await asyncio.sleep(0.01)

    async def stop(self, timeout_seconds: float = 5.0) -> None:
        if self._task is not None:
            self._server.should_exit = True
            try:
                await asyncio.wait_for(self._task, timeout=timeout_seconds)
            except TimeoutError:
                self._server.force_exit = True
                await self._task
            finally:
                self._task = None
        await self.service.stop()
