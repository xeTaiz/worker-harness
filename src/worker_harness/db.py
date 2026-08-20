"""Async SQLite repository layer."""

from __future__ import annotations

import asyncio
import aiosqlite
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from .models import (
    Failure,
    Job,
    JobKind,
    JobStatus,
    PiBridgeEventBatch,
    PiBridgeRegister,
    PiDelegation,
    PiRouterConfig,
    PiRouterRequest,
    PiSession,
    PiSessionCommand,
    PiSessionEvent,
    PiSessionState,
    PiSessionType,
    PortForward,
    MarimoSession,
    Worker,
    WorkerJobReport,
    WorkerRegistration,
    WorkerStatus,
)


class Database:
    """Async SQLite database with all repository methods."""

    def __init__(self, path: str | Path = "~/.config/worker-harness/db.sqlite") -> None:
        self.path = Path(path).expanduser()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._db: aiosqlite.Connection | None = None
        # Registration-port job reports can be retried concurrently; serialize
        # their conditional UPSERTs on this process's single SQLite writer.
        self._worker_job_report_lock = asyncio.Lock()
        self._pi_bridge_lock = asyncio.Lock()

    async def connect(self) -> None:
        self._db = await aiosqlite.connect(str(self.path))
        self._db.row_factory = aiosqlite.Row
        # Multi-agent reliability: without WAL + busy_timeout, two concurrent writers
        # (e.g. the heartbeat server's heartbeat-upsert vs. a CLI's _init_schema
        # ALTER TABLE) deadlock on a futex indefinitely. WAL allows concurrent
        # readers + 1 writer; busy_timeout=5000 makes any residual lock contention
        # retry for up to 5s instead of returning SQLITE_BUSY immediately.
        # See specs/MULTI_AGENT_RELIABILITY.md.
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA synchronous=NORMAL")
        await self._db.execute("PRAGMA busy_timeout=5000")
        await self._init_schema()

    async def close(self) -> None:
        if self._db:
            await self._db.close()

    async def __aenter__(self) -> 'Database':
        await self.connect()
        return self

    async def __aexit__(self, *args) -> None:
        await self.close()

    async def _init_schema(self) -> None:
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS workers (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                worker_ip TEXT NOT NULL,
                dns_name TEXT NOT NULL DEFAULT '',
                ssh_user TEXT NOT NULL DEFAULT 'root',
                harness_dir TEXT NOT NULL DEFAULT '/harness',
                gpu_count INTEGER DEFAULT 0,
                gpu_names TEXT DEFAULT '[]',
                gpu_vram_gb TEXT DEFAULT '[]',
                gpu_used_vram_gb TEXT DEFAULT '[]',
                cpu_cores INTEGER DEFAULT 0,
                total_ram_gb REAL DEFAULT 0,
                used_ram_gb REAL DEFAULT 0,
                total_disk_gb REAL DEFAULT 0,
                used_disk_gb REAL DEFAULT 0,
                data_paths TEXT DEFAULT '[]',
                pi_relay_port INTEGER DEFAULT 0,
                pi_relay_available INTEGER DEFAULT 0,
                pi_relay_protocol_version INTEGER DEFAULT 0,
                status TEXT DEFAULT 'offline',
                last_heartbeat_ts INTEGER DEFAULT 0,
                created_at INTEGER DEFAULT 0
            )
        """)
        # Migrations are explicitly guarded by PRAGMA metadata. Never swallow
        # arbitrary errors here: a real I/O/lock/schema error must be visible
        # instead of leaving a process blocked or half-migrated.
        cols = await self._db.execute_fetchall("PRAGMA table_info(workers)")
        colnames = {c["name"] for c in cols}
        if "gpu_used_vram_gb" not in colnames:
            await self._db.execute(
                "ALTER TABLE workers ADD COLUMN gpu_used_vram_gb TEXT DEFAULT '[]'"
            )
            colnames.add("gpu_used_vram_gb")

        # Migration: rename worker address column zerotier_ip -> worker_ip.
        if "worker_ip" not in colnames:
            if "zerotier_ip" in colnames:
                await self._db.execute("ALTER TABLE workers ADD COLUMN worker_ip TEXT")
                await self._db.execute(
                    "UPDATE workers SET worker_ip = zerotier_ip WHERE worker_ip IS NULL OR worker_ip = ''"
                )
            else:
                await self._db.execute("ALTER TABLE workers ADD COLUMN worker_ip TEXT NOT NULL DEFAULT ''")

        if "dns_name" not in colnames:
            await self._db.execute("ALTER TABLE workers ADD COLUMN dns_name TEXT NOT NULL DEFAULT ''")
        if "ssh_user" not in colnames:
            await self._db.execute("ALTER TABLE workers ADD COLUMN ssh_user TEXT NOT NULL DEFAULT 'root'")
        if "harness_dir" not in colnames:
            await self._db.execute("ALTER TABLE workers ADD COLUMN harness_dir TEXT NOT NULL DEFAULT '/harness'")
        if "data_paths" not in colnames:
            await self._db.execute("ALTER TABLE workers ADD COLUMN data_paths TEXT DEFAULT '[]'")
        if "pi_relay_port" not in colnames:
            await self._db.execute("ALTER TABLE workers ADD COLUMN pi_relay_port INTEGER DEFAULT 0")
        if "pi_relay_available" not in colnames:
            await self._db.execute("ALTER TABLE workers ADD COLUMN pi_relay_available INTEGER DEFAULT 0")
        if "pi_relay_protocol_version" not in colnames:
            await self._db.execute("ALTER TABLE workers ADD COLUMN pi_relay_protocol_version INTEGER DEFAULT 0")
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                worker_id TEXT REFERENCES workers(id),
                tmux_session TEXT,
                command TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                exit_code INTEGER,
                pty_enabled INTEGER DEFAULT 1,
                kind TEXT NOT NULL DEFAULT 'ssh',
                origin_session_id TEXT,
                report_revision INTEGER NOT NULL DEFAULT 0,
                started_at INTEGER DEFAULT 0,
                finished_at INTEGER DEFAULT 0
            )
        """)
        cursor = await self._db.execute("PRAGMA table_info(jobs)")
        job_cols = {row[1] for row in await cursor.fetchall()}
        if "kind" not in job_cols:
            await self._db.execute("ALTER TABLE jobs ADD COLUMN kind TEXT NOT NULL DEFAULT 'ssh'")
        if "origin_session_id" not in job_cols:
            await self._db.execute("ALTER TABLE jobs ADD COLUMN origin_session_id TEXT")
        if "report_revision" not in job_cols:
            await self._db.execute("ALTER TABLE jobs ADD COLUMN report_revision INTEGER NOT NULL DEFAULT 0")
        await self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_jobs_origin_session_id ON jobs(origin_session_id)"
        )
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS port_forwards (
                id TEXT PRIMARY KEY,
                worker_id TEXT REFERENCES workers(id),
                local_port INTEGER NOT NULL,
                remote_port INTEGER NOT NULL,
                service_name TEXT DEFAULT '',
                pid INTEGER DEFAULT 0,
                created_at INTEGER DEFAULT 0
            )
        """)
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS marimo_sessions (
                id TEXT PRIMARY KEY,
                worker_id TEXT REFERENCES workers(id),
                notebook_path TEXT NOT NULL,
                environment TEXT NOT NULL,
                job_id TEXT NOT NULL,
                tunnel_id TEXT NOT NULL,
                local_port INTEGER NOT NULL,
                remote_port INTEGER NOT NULL,
                bind_host TEXT NOT NULL,
                url TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'ready',
                created_at INTEGER DEFAULT 0
            )
        """)
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS failures (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT,
                worker_id TEXT,
                exit_code INTEGER,
                timestamp INTEGER,
                summary TEXT
            )
        """)
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS pi_sessions (
                id TEXT PRIMARY KEY,
                worker_id TEXT REFERENCES workers(id),
                parent_session_id TEXT,
                session_type TEXT NOT NULL,
                state TEXT NOT NULL,
                task TEXT DEFAULT '',
                cwd TEXT DEFAULT '',
                tmux_session TEXT DEFAULT '',
                detail TEXT DEFAULT '',
                name TEXT DEFAULT '',
                host TEXT DEFAULT '',
                agent TEXT DEFAULT 'pi',
                bridge_incarnation TEXT,
                terminal_attachable INTEGER DEFAULT 0,
                terminal_host TEXT DEFAULT '',
                terminal_port INTEGER DEFAULT 0,
                terminal_protocol_version INTEGER DEFAULT 0,
                has_pending_messages INTEGER DEFAULT 0,
                last_seen INTEGER DEFAULT 0,
                created_at INTEGER DEFAULT 0,
                updated_at INTEGER DEFAULT 0
            )
        """)
        cursor = await self._db.execute("PRAGMA table_info(pi_sessions)")
        pi_session_cols = {row[1] for row in await cursor.fetchall()}
        for column, declaration in {
            "name": "TEXT DEFAULT ''",
            "host": "TEXT DEFAULT ''",
            "agent": "TEXT DEFAULT 'pi'",
            "bridge_incarnation": "TEXT",
            "terminal_attachable": "INTEGER DEFAULT 0",
            "terminal_host": "TEXT DEFAULT ''",
            "terminal_port": "INTEGER DEFAULT 0",
            "terminal_protocol_version": "INTEGER DEFAULT 0",
            "has_pending_messages": "INTEGER DEFAULT 0",
            "last_seen": "INTEGER DEFAULT 0",
        }.items():
            if column not in pi_session_cols:
                await self._db.execute(f"ALTER TABLE pi_sessions ADD COLUMN {column} {declaration}")
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS pi_session_events (
                id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL REFERENCES pi_sessions(id),
                event_type TEXT NOT NULL,
                payload TEXT DEFAULT '{}',
                created_at INTEGER DEFAULT 0,
                sequence INTEGER NOT NULL DEFAULT 0
            )
        """)
        cursor = await self._db.execute("PRAGMA table_info(pi_session_events)")
        pi_event_cols = {row[1] for row in await cursor.fetchall()}
        if "sequence" not in pi_event_cols:
            await self._db.execute(
                "ALTER TABLE pi_session_events ADD COLUMN sequence INTEGER NOT NULL DEFAULT 0"
            )
        # Give pre-streaming events stable per-session cursors in insertion order.
        await self._db.execute("""
            UPDATE pi_session_events AS event SET sequence = (
                SELECT COUNT(*) FROM pi_session_events AS prior
                WHERE prior.session_id=event.session_id AND prior.rowid<=event.rowid
            ) WHERE sequence=0
        """)
        await self._db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_pi_events_sequence ON pi_session_events(session_id, sequence)"
        )
        await self._db.execute("""
            CREATE TRIGGER IF NOT EXISTS assign_pi_session_event_sequence
            AFTER INSERT ON pi_session_events WHEN NEW.sequence=0
            BEGIN
                UPDATE pi_session_events SET sequence = (
                    SELECT COALESCE(MAX(sequence), 0) + 1 FROM pi_session_events
                    WHERE session_id=NEW.session_id AND rowid!=NEW.rowid
                ) WHERE rowid=NEW.rowid;
            END
        """)
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS pi_session_commands (
                id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL REFERENCES pi_sessions(id),
                kind TEXT NOT NULL DEFAULT 'prompt',
                message TEXT NOT NULL,
                deliver_as TEXT NOT NULL DEFAULT 'followUp',
                payload TEXT NOT NULL DEFAULT '{}',
                created_at INTEGER DEFAULT 0,
                claimed_at INTEGER DEFAULT 0,
                claimed_by TEXT DEFAULT '',
                delivered_at INTEGER DEFAULT 0
            )
        """)
        cursor = await self._db.execute("PRAGMA table_info(pi_session_commands)")
        pi_command_cols = {row[1] for row in await cursor.fetchall()}
        if "payload" not in pi_command_cols:
            await self._db.execute(
                "ALTER TABLE pi_session_commands ADD COLUMN payload TEXT NOT NULL DEFAULT '{}'"
            )
        await self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_pi_commands_pending ON pi_session_commands(session_id, delivered_at, created_at)"
        )
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS pi_router_config (
                id INTEGER PRIMARY KEY CHECK (id=1),
                provider TEXT NOT NULL,
                model TEXT NOT NULL,
                thinking_level TEXT NOT NULL DEFAULT 'off',
                updated_at INTEGER NOT NULL DEFAULT 0
            )
        """)
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS pi_router_requests (
                id TEXT PRIMARY KEY,
                message TEXT NOT NULL,
                selection_mode TEXT NOT NULL,
                candidate_snapshot TEXT NOT NULL DEFAULT '[]',
                selected_session_id TEXT,
                router_output TEXT NOT NULL DEFAULT '',
                provider TEXT NOT NULL DEFAULT '',
                model TEXT NOT NULL DEFAULT '',
                thinking_level TEXT NOT NULL DEFAULT 'off',
                latency_ms INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'routing',
                error TEXT NOT NULL DEFAULT '',
                command_id TEXT,
                created_at INTEGER NOT NULL DEFAULT 0,
                completed_at INTEGER NOT NULL DEFAULT 0
            )
        """)
        await self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_pi_router_requests_created ON pi_router_requests(created_at DESC)"
        )
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS pi_delegations (
                id TEXT PRIMARY KEY,
                parent_session_id TEXT,
                worker_id TEXT NOT NULL REFERENCES workers(id),
                child_session_id TEXT NOT NULL REFERENCES pi_sessions(id),
                task TEXT NOT NULL,
                state TEXT NOT NULL,
                created_at INTEGER DEFAULT 0,
                completed_at INTEGER DEFAULT 0
            )
        """)
        cursor = await self._db.execute("PRAGMA table_info(pi_delegations)")
        delegation_cols = {row[1] for row in await cursor.fetchall()}
        if "timeout_seconds" not in delegation_cols:
            await self._db.execute("ALTER TABLE pi_delegations ADD COLUMN timeout_seconds INTEGER DEFAULT 0")
        await self._db.commit()

    # ── Workers ──────────────────────────────────────────────────────

    async def upsert_worker(self, reg: WorkerRegistration) -> Worker:
        existing = await self.get_worker(reg.worker_id)
        if existing:
            existing.update_from_registration(reg)
            await self._update_worker(existing)
            return existing
        else:
            worker = Worker.from_registration(reg)
            await self._insert_worker(worker)
            return worker

    async def get_worker(self, worker_id: str) -> Worker | None:
        cursor = await self._db.execute(
            "SELECT * FROM workers WHERE id = ?", (worker_id,)
        )
        row = await cursor.fetchone()
        if not row:
            return None
        return self._row_to_worker(row)

    async def list_workers(self) -> list[Worker]:
        rows = await self._db.execute_fetchall("SELECT * FROM workers ORDER BY name")
        return [self._row_to_worker(r) for r in rows]

    async def mark_workers_offline(self, cutoff_ts: int) -> int:
        cursor = await self._db.execute(
            "UPDATE workers SET status = ? WHERE last_heartbeat_ts < ? AND status != ?",
            (WorkerStatus.OFFLINE.value, cutoff_ts, WorkerStatus.OFFLINE.value),
        )
        await self._db.commit()
        return cursor.rowcount

    async def set_worker_status(self, worker_id: str, status: WorkerStatus) -> None:
        await self._db.execute(
            "UPDATE workers SET status = ? WHERE id = ?",
            (status.value, worker_id),
        )
        await self._db.commit()

    async def _insert_worker(self, w: Worker) -> None:
        await self._db.execute(
            """INSERT INTO workers
               (id, name, worker_ip, dns_name, ssh_user, harness_dir, gpu_count, gpu_names, gpu_vram_gb,
                gpu_used_vram_gb, cpu_cores, total_ram_gb, used_ram_gb, total_disk_gb, used_disk_gb,
                data_paths, pi_relay_port, pi_relay_available, pi_relay_protocol_version,
                status, last_heartbeat_ts, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                w.id, w.name, w.worker_ip, w.dns_name, w.ssh_user, w.harness_dir, w.gpu_count,
                json.dumps(w.gpu_names), json.dumps(w.gpu_vram_gb),
                json.dumps(w.gpu_used_vram_gb),
                w.cpu_cores, w.total_ram_gb, w.used_ram_gb,
                w.total_disk_gb, w.used_disk_gb, json.dumps(w.data_paths),
                w.pi_relay_port, int(w.pi_relay_available), w.pi_relay_protocol_version,
                w.status.value, w.last_heartbeat_ts, w.created_at,
            ),
        )
        await self._db.commit()

    async def _update_worker(self, w: Worker) -> None:
        await self._db.execute(
            """UPDATE workers SET
               name=?, worker_ip=?, dns_name=?, ssh_user=?, harness_dir=?, gpu_count=?, gpu_names=?,
               gpu_vram_gb=?, gpu_used_vram_gb=?, cpu_cores=?, total_ram_gb=?, used_ram_gb=?,
               total_disk_gb=?, used_disk_gb=?, data_paths=?, pi_relay_port=?, pi_relay_available=?,
               pi_relay_protocol_version=?, status=?, last_heartbeat_ts=?
               WHERE id=?""",
            (
                w.name, w.worker_ip, w.dns_name, w.ssh_user, w.harness_dir, w.gpu_count,
                json.dumps(w.gpu_names), json.dumps(w.gpu_vram_gb),
                json.dumps(w.gpu_used_vram_gb),
                w.cpu_cores, w.total_ram_gb, w.used_ram_gb,
                w.total_disk_gb, w.used_disk_gb, json.dumps(w.data_paths),
                w.pi_relay_port, int(w.pi_relay_available), w.pi_relay_protocol_version,
                w.status.value, w.last_heartbeat_ts, w.id,
            ),
        )
        await self._db.commit()

    def _row_to_worker(self, row: aiosqlite.Row) -> Worker:
        return Worker(
            id=row["id"],
            name=row["name"],
            worker_ip=row["worker_ip"],
            dns_name=row["dns_name"],
            ssh_user=row["ssh_user"],
            harness_dir=row["harness_dir"],
            gpu_count=row["gpu_count"],
            gpu_names=json.loads(row["gpu_names"]),
            gpu_vram_gb=json.loads(row["gpu_vram_gb"]),
            gpu_used_vram_gb=json.loads(row["gpu_used_vram_gb"]),
            cpu_cores=row["cpu_cores"],
            total_ram_gb=row["total_ram_gb"],
            used_ram_gb=row["used_ram_gb"],
            total_disk_gb=row["total_disk_gb"],
            used_disk_gb=row["used_disk_gb"],
            data_paths=json.loads(row["data_paths"] or "[]"),
            pi_relay_port=row["pi_relay_port"],
            pi_relay_available=bool(row["pi_relay_available"]),
            pi_relay_protocol_version=row["pi_relay_protocol_version"],
            status=WorkerStatus(row["status"]),
            last_heartbeat_ts=row["last_heartbeat_ts"],
            created_at=row["created_at"],
        )

    # ── Pi sessions ──────────────────────────────────────────────────

    @staticmethod
    def _row_to_pi_session(row: aiosqlite.Row) -> PiSession:
        return PiSession(
            id=row["id"], worker_id=row["worker_id"], parent_session_id=row["parent_session_id"],
            session_type=PiSessionType(row["session_type"]), state=PiSessionState(row["state"]),
            task=row["task"], cwd=row["cwd"], tmux_session=row["tmux_session"], detail=row["detail"],
            name=row["name"] if "name" in row.keys() else "",
            host=row["host"] if "host" in row.keys() else "",
            agent=row["agent"] if "agent" in row.keys() else "pi",
            bridge_incarnation=row["bridge_incarnation"] if "bridge_incarnation" in row.keys() else None,
            terminal_attachable=bool(row["terminal_attachable"]) if "terminal_attachable" in row.keys() else False,
            terminal_host=row["terminal_host"] if "terminal_host" in row.keys() else "",
            terminal_port=row["terminal_port"] if "terminal_port" in row.keys() else 0,
            terminal_protocol_version=(
                row["terminal_protocol_version"] if "terminal_protocol_version" in row.keys() else 0
            ),
            has_pending_messages=(
                bool(row["has_pending_messages"]) if "has_pending_messages" in row.keys() else False
            ),
            last_seen=row["last_seen"] if "last_seen" in row.keys() else 0,
            created_at=row["created_at"], updated_at=row["updated_at"],
        )

    async def insert_pi_session(self, session: PiSession) -> None:
        await self._db.execute(
            """INSERT INTO pi_sessions
               (id, worker_id, parent_session_id, session_type, state, task, cwd, tmux_session, detail,
                name, host, agent, bridge_incarnation, terminal_attachable, terminal_host, terminal_port,
                terminal_protocol_version, has_pending_messages, last_seen, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (session.id, session.worker_id, session.parent_session_id, session.session_type.value,
             session.state.value, session.task, session.cwd, session.tmux_session, session.detail,
             session.name, session.host, session.agent, session.bridge_incarnation,
             int(session.terminal_attachable), session.terminal_host, session.terminal_port,
             session.terminal_protocol_version,
             int(session.has_pending_messages), session.last_seen, session.created_at, session.updated_at),
        )
        await self._db.commit()

    async def update_pi_session(self, session: PiSession) -> None:
        await self._db.execute(
            """UPDATE pi_sessions SET worker_id=?, parent_session_id=?, session_type=?, state=?, task=?, cwd=?,
               tmux_session=?, detail=?, name=?, host=?, agent=?, bridge_incarnation=?, terminal_attachable=?,
               terminal_host=?, terminal_port=?, terminal_protocol_version=?, has_pending_messages=?,
               last_seen=?, updated_at=? WHERE id=?""",
            (session.worker_id, session.parent_session_id, session.session_type.value, session.state.value,
             session.task, session.cwd, session.tmux_session, session.detail, session.name, session.host,
             session.agent, session.bridge_incarnation, int(session.terminal_attachable), session.terminal_host,
             session.terminal_port, session.terminal_protocol_version, int(session.has_pending_messages),
             session.last_seen, session.updated_at, session.id),
        )
        await self._db.commit()

    async def get_pi_session(self, session_id: str) -> PiSession | None:
        cursor = await self._db.execute("SELECT * FROM pi_sessions WHERE id=?", (session_id,))
        row = await cursor.fetchone()
        return self._row_to_pi_session(row) if row else None

    async def list_pi_sessions(self, worker_id: str | None = None) -> list[PiSession]:
        if worker_id:
            rows = await self._db.execute_fetchall(
                "SELECT * FROM pi_sessions WHERE worker_id=? ORDER BY updated_at DESC", (worker_id,)
            )
        else:
            rows = await self._db.execute_fetchall("SELECT * FROM pi_sessions ORDER BY updated_at DESC")
        return [self._row_to_pi_session(row) for row in rows]

    async def register_interactive_pi_session(self, payload: PiBridgeRegister, now: int | None = None) -> PiSession:
        """Create or replace one plain-Pi bridge incarnation."""
        now = now if now is not None else int(time.time())
        async with self._pi_bridge_lock:
            existing = await self.get_pi_session(payload.session_id)
            if existing and existing.session_type != PiSessionType.INTERACTIVE:
                raise ValueError("session id belongs to a non-interactive session")
            created_at = existing.created_at if existing else now
            await self._db.execute(
                """INSERT INTO pi_sessions
                   (id, worker_id, parent_session_id, session_type, state, task, cwd, tmux_session,
                    detail, name, host, agent, bridge_incarnation, terminal_attachable, terminal_host,
                    terminal_port, terminal_protocol_version, has_pending_messages, last_seen, created_at, updated_at)
                   VALUES (?, NULL, NULL, ?, ?, '', ?, '', '', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(id) DO UPDATE SET
                     worker_id=NULL, parent_session_id=NULL, session_type=excluded.session_type,
                     state=excluded.state, cwd=excluded.cwd, detail='', name=excluded.name,
                     host=excluded.host, agent=excluded.agent,
                     bridge_incarnation=excluded.bridge_incarnation,
                     terminal_attachable=excluded.terminal_attachable,
                     terminal_host=excluded.terminal_host, terminal_port=excluded.terminal_port,
                     terminal_protocol_version=excluded.terminal_protocol_version,
                     has_pending_messages=excluded.has_pending_messages,
                     last_seen=excluded.last_seen, updated_at=excluded.updated_at""",
                (
                    payload.session_id, PiSessionType.INTERACTIVE.value, PiSessionState.IDLE.value,
                    payload.cwd, payload.name, payload.host, payload.agent, payload.incarnation,
                    int(payload.terminal_attachable), payload.terminal_host, payload.terminal_port,
                    payload.terminal_protocol_version, int(payload.has_pending_messages), now, created_at, now,
                ),
            )
            # A replacement bridge may reclaim commands whose response was
            # never acknowledged by the old incarnation.
            await self._db.execute(
                """UPDATE pi_session_commands SET claimed_at=0, claimed_by=''
                   WHERE session_id=? AND delivered_at=0 AND claimed_by!=?""",
                (payload.session_id, payload.incarnation),
            )
            # Registration snapshots contain at most one completed user /
            # assistant exchange. Match by message identity as well as event ID
            # so a reload does not duplicate messages already captured live.
            for event in payload.initial_events:
                if event.event_type not in {"message-start", "message-end"}:
                    continue
                message_id = event.payload.get("message_id")
                if not isinstance(message_id, str) or not message_id:
                    continue
                duplicate = await self._db.execute(
                    """SELECT 1 FROM pi_session_events
                       WHERE session_id=? AND event_type=?
                         AND json_extract(payload, '$.message_id')=? LIMIT 1""",
                    (payload.session_id, event.event_type, message_id),
                )
                if await duplicate.fetchone():
                    continue
                await self._db.execute(
                    """INSERT OR IGNORE INTO pi_session_events
                       (id, session_id, event_type, payload, created_at) VALUES (?, ?, ?, ?, ?)""",
                    (
                        event.id or str(uuid4()), payload.session_id, event.event_type,
                        json.dumps(event.payload), event.created_at or now,
                    ),
                )
            await self._db.commit()
            session = await self.get_pi_session(payload.session_id)
            assert session is not None
            return session

    async def apply_interactive_pi_events(
        self, session_id: str, payload: PiBridgeEventBatch, now: int | None = None,
    ) -> tuple[PiSession, list[PiSessionEvent]]:
        """Apply idempotent events only from the active bridge incarnation."""
        now = now if now is not None else int(time.time())
        async with self._pi_bridge_lock:
            session = await self.get_pi_session(session_id)
            if not session or session.session_type != PiSessionType.INTERACTIVE:
                raise KeyError(session_id)
            if session.bridge_incarnation != payload.incarnation:
                raise PermissionError("stale bridge incarnation")
            persisted: list[PiSessionEvent] = []
            for event in payload.events:
                event_id = event.id or str(uuid4())
                created_at = event.created_at or now
                cursor = await self._db.execute(
                    """INSERT OR IGNORE INTO pi_session_events
                       (id, session_id, event_type, payload, created_at) VALUES (?, ?, ?, ?, ?)""",
                    (event_id, session_id, event.event_type, json.dumps(event.payload), created_at),
                )
                if cursor.rowcount:
                    persisted.append(PiSessionEvent(
                        id=event_id, session_id=session_id, event_type=event.event_type,
                        payload=event.payload, created_at=created_at,
                    ))
            if payload.state is not None:
                session.state = payload.state
            session.detail = payload.detail
            if payload.has_pending_messages is not None:
                session.has_pending_messages = payload.has_pending_messages
            session.last_seen = now
            session.updated_at = now
            await self._db.execute(
                """UPDATE pi_sessions SET state=?, detail=?, has_pending_messages=?, last_seen=?, updated_at=?
                   WHERE id=?""",
                (session.state.value, session.detail, int(session.has_pending_messages), now, now, session_id),
            )
            await self._db.commit()
            return session, persisted

    async def enqueue_pi_session_command(self, command: PiSessionCommand) -> None:
        await self._db.execute(
            """INSERT INTO pi_session_commands
               (id, session_id, kind, message, deliver_as, payload, created_at, claimed_at, claimed_by, delivered_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                command.id, command.session_id, command.kind, command.message, command.deliver_as,
                json.dumps(command.payload), command.created_at, command.claimed_at,
                command.claimed_by, command.delivered_at,
            ),
        )
        await self._db.commit()

    @staticmethod
    def _row_to_pi_command(row: aiosqlite.Row) -> PiSessionCommand:
        return PiSessionCommand(
            id=row["id"], session_id=row["session_id"], kind=row["kind"], message=row["message"],
            deliver_as=row["deliver_as"], payload=json.loads(row["payload"] or "{}"),
            created_at=row["created_at"], claimed_at=row["claimed_at"],
            claimed_by=row["claimed_by"], delivered_at=row["delivered_at"],
        )

    async def claim_pi_session_commands(
        self, session_id: str, incarnation: str, now: int | None = None, lease_seconds: int = 30,
    ) -> list[PiSessionCommand]:
        now = now if now is not None else int(time.time())
        async with self._pi_bridge_lock:
            session = await self.get_pi_session(session_id)
            if not session or session.session_type != PiSessionType.INTERACTIVE:
                raise KeyError(session_id)
            if session.bridge_incarnation != incarnation:
                raise PermissionError("stale bridge incarnation")
            cutoff = now - lease_seconds
            await self._db.execute(
                """UPDATE pi_session_commands SET claimed_at=?, claimed_by=?
                   WHERE session_id=? AND delivered_at=0
                     AND (claimed_by='' OR claimed_by=? OR claimed_at<=?)""",
                (now, incarnation, session_id, incarnation, cutoff),
            )
            rows = await self._db.execute_fetchall(
                """SELECT * FROM pi_session_commands
                   WHERE session_id=? AND delivered_at=0 AND claimed_by=? ORDER BY created_at, rowid""",
                (session_id, incarnation),
            )
            await self._db.commit()
            return [self._row_to_pi_command(row) for row in rows]

    async def ack_pi_session_command(
        self, session_id: str, command_id: str, incarnation: str, now: int | None = None,
    ) -> bool:
        now = now if now is not None else int(time.time())
        async with self._pi_bridge_lock:
            session = await self.get_pi_session(session_id)
            if not session or session.session_type != PiSessionType.INTERACTIVE:
                raise KeyError(session_id)
            if session.bridge_incarnation != incarnation:
                raise PermissionError("stale bridge incarnation")
            cursor = await self._db.execute(
                """UPDATE pi_session_commands SET delivered_at=?
                   WHERE id=? AND session_id=? AND claimed_by=? AND delivered_at=0""",
                (now, command_id, session_id, incarnation),
            )
            await self._db.commit()
            return bool(cursor.rowcount)

    async def sweep_stale_interactive_pi_sessions(self, cutoff_ts: int, now: int | None = None) -> list[str]:
        now = now if now is not None else int(time.time())
        async with self._pi_bridge_lock:
            rows = await self._db.execute_fetchall(
                """SELECT id FROM pi_sessions WHERE session_type=? AND last_seen<?
                   AND state NOT IN (?, ?)""",
                (
                    PiSessionType.INTERACTIVE.value, cutoff_ts,
                    PiSessionState.STOPPED.value, PiSessionState.FAILED.value,
                ),
            )
            session_ids = [row["id"] for row in rows]
            for session_id in session_ids:
                await self._db.execute(
                    "UPDATE pi_sessions SET state=?, detail=?, updated_at=? WHERE id=?",
                    (PiSessionState.STOPPED.value, "bridge heartbeat expired", now, session_id),
                )
                await self._db.execute(
                    """INSERT INTO pi_session_events (id, session_id, event_type, payload, created_at)
                       VALUES (?, ?, 'bridge-stale', '{}', ?)""",
                    (str(uuid4()), session_id, now),
                )
            await self._db.commit()
            return session_ids

    async def insert_pi_session_event(self, event: PiSessionEvent) -> None:
        await self._db.execute(
            "INSERT OR IGNORE INTO pi_session_events (id, session_id, event_type, payload, created_at) VALUES (?, ?, ?, ?, ?)",
            (event.id, event.session_id, event.event_type, json.dumps(event.payload), event.created_at),
        )
        await self._db.commit()

    async def apply_pi_ingest(self, worker_id: str, payload) -> list[PiSessionEvent]:
        """Persist a worker-reported Pi session state update and its events.

        Workers are the only writers of the *reported* state themselves; the
        orchestrator's durable session row is updated only when an explicit
        event crosses this boundary. ``INSERT OR IGNORE`` on the event id keeps
        replay tolerant. Returns the events actually persisted.
        """
        from uuid import uuid4
        async with self._db.execute("SELECT id, state FROM pi_sessions WHERE id=?", (payload.session_id,)) as cursor:
            row = await cursor.fetchone()
        if not row:
            raise KeyError(payload.session_id)
        persisted: list[PiSessionEvent] = []
        for event in payload.events:
            event_id = event.id or str(uuid4())
            cursor = await self._db.execute(
                "INSERT OR IGNORE INTO pi_session_events (id, session_id, event_type, payload, created_at) VALUES (?, ?, ?, ?, ?)",
                (event_id, payload.session_id, event.event_type, json.dumps(event.payload), event.created_at or int(time.time())),
            )
            if cursor.rowcount:
                persisted.append(PiSessionEvent(
                    id=event_id, session_id=payload.session_id,
                    event_type=event.event_type, payload=event.payload,
                    created_at=event.created_at or int(time.time()),
                ))
        if payload.state is not None:
            current = PiSessionState(row["state"])
            # A delayed worker outbox must not resurrect a session after an
            # orchestrator timeout/cancel made its projection terminal. A
            # later observed terminal state may refine termination_unknown.
            may_update = current not in {
                PiSessionState.STOPPED,
                PiSessionState.FAILED,
                PiSessionState.TERMINATION_UNKNOWN,
            } or (
                current == PiSessionState.TERMINATION_UNKNOWN
                and payload.state in {PiSessionState.STOPPED, PiSessionState.FAILED}
            )
            if may_update:
                now = int(time.time())
                await self._db.execute(
                    "UPDATE pi_sessions SET state=?, detail=?, updated_at=? WHERE id=?",
                    (payload.state.value, payload.detail, now, payload.session_id),
                )
                completed_at = now if payload.state in {
                    PiSessionState.IDLE,
                    PiSessionState.STOPPED,
                    PiSessionState.FAILED,
                    PiSessionState.TERMINATION_UNKNOWN,
                } else 0
                await self._db.execute(
                    "UPDATE pi_delegations SET state=?, completed_at=? WHERE child_session_id=?",
                    (payload.state.value, completed_at, payload.session_id),
                )
        await self._db.commit()
        return persisted

    async def list_pi_session_events(
        self, session_id: str, after: int = 0, limit: int = 1000,
    ) -> list[PiSessionEvent]:
        rows = await self._db.execute_fetchall(
            """SELECT * FROM pi_session_events
               WHERE session_id=? AND sequence>? ORDER BY sequence LIMIT ?""",
            (session_id, max(0, after), max(1, min(limit, 1000))),
        )
        return [
            PiSessionEvent(
                id=row["id"], session_id=row["session_id"], event_type=row["event_type"],
                payload=json.loads(row["payload"] or "{}"), created_at=row["created_at"],
                sequence=row["sequence"],
            )
            for row in rows
        ]

    @staticmethod
    def _row_to_pi_router_request(row: aiosqlite.Row) -> PiRouterRequest:
        return PiRouterRequest(
            id=row["id"], message=row["message"], selection_mode=row["selection_mode"],
            candidate_snapshot=json.loads(row["candidate_snapshot"] or "[]"),
            selected_session_id=row["selected_session_id"], router_output=row["router_output"],
            provider=row["provider"], model=row["model"], thinking_level=row["thinking_level"],
            latency_ms=row["latency_ms"], status=row["status"], error=row["error"],
            command_id=row["command_id"], created_at=row["created_at"], completed_at=row["completed_at"],
        )

    async def get_pi_router_config(self) -> PiRouterConfig | None:
        row = await (await self._db.execute("SELECT * FROM pi_router_config WHERE id=1")).fetchone()
        if not row:
            return None
        return PiRouterConfig(
            provider=row["provider"], model=row["model"],
            thinking_level=row["thinking_level"], updated_at=row["updated_at"],
        )

    async def set_pi_router_config(self, config: PiRouterConfig) -> PiRouterConfig:
        await self._db.execute(
            """INSERT INTO pi_router_config (id, provider, model, thinking_level, updated_at)
               VALUES (1, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET provider=excluded.provider, model=excluded.model,
                 thinking_level=excluded.thinking_level, updated_at=excluded.updated_at""",
            (config.provider, config.model, config.thinking_level, config.updated_at),
        )
        await self._db.commit()
        return config

    async def insert_pi_router_request(self, request: PiRouterRequest) -> bool:
        cursor = await self._db.execute(
            """INSERT OR IGNORE INTO pi_router_requests
               (id, message, selection_mode, candidate_snapshot, selected_session_id, router_output,
                provider, model, thinking_level, latency_ms, status, error, command_id, created_at, completed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                request.id, request.message, request.selection_mode, json.dumps(request.candidate_snapshot),
                request.selected_session_id, request.router_output, request.provider, request.model,
                request.thinking_level, request.latency_ms, request.status, request.error,
                request.command_id, request.created_at, request.completed_at,
            ),
        )
        await self._db.commit()
        return bool(cursor.rowcount)

    async def update_pi_router_request(self, request: PiRouterRequest) -> None:
        await self._db.execute(
            """UPDATE pi_router_requests SET selection_mode=?, candidate_snapshot=?, selected_session_id=?,
               router_output=?, provider=?, model=?, thinking_level=?, latency_ms=?, status=?, error=?,
               command_id=?, completed_at=? WHERE id=?""",
            (
                request.selection_mode, json.dumps(request.candidate_snapshot), request.selected_session_id,
                request.router_output, request.provider, request.model, request.thinking_level,
                request.latency_ms, request.status, request.error, request.command_id,
                request.completed_at, request.id,
            ),
        )
        await self._db.commit()

    async def get_pi_router_request(self, request_id: str) -> PiRouterRequest | None:
        row = await (await self._db.execute(
            "SELECT * FROM pi_router_requests WHERE id=?", (request_id,)
        )).fetchone()
        return self._row_to_pi_router_request(row) if row else None

    async def get_latest_pi_router_request(
        self, *, dispatched_only: bool = False, classified_only: bool = False,
    ) -> PiRouterRequest | None:
        clauses = []
        if dispatched_only:
            clauses.append("status='dispatched'")
        if classified_only:
            clauses.append("selection_mode='auto' AND latency_ms>0")
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        row = await (await self._db.execute(
            f"""SELECT * FROM pi_router_requests {where}
                ORDER BY completed_at DESC, created_at DESC, rowid DESC LIMIT 1"""
        )).fetchone()
        return self._row_to_pi_router_request(row) if row else None

    async def get_latest_pi_message_event(
        self, session_id: str, role: str,
    ) -> PiSessionEvent | None:
        row = await (await self._db.execute(
            """SELECT * FROM pi_session_events
               WHERE session_id=? AND event_type='message-end'
                 AND json_extract(payload, '$.message.role')=?
               ORDER BY sequence DESC LIMIT 1""",
            (session_id, role),
        )).fetchone()
        if not row:
            return None
        return PiSessionEvent(
            id=row["id"], session_id=row["session_id"], event_type=row["event_type"],
            payload=json.loads(row["payload"] or "{}"), created_at=row["created_at"],
            sequence=row["sequence"],
        )

    async def list_recent_pi_session_events(
        self, session_id: str, limit: int = 500,
    ) -> list[PiSessionEvent]:
        rows = await self._db.execute_fetchall(
            """SELECT * FROM (
                   SELECT * FROM pi_session_events WHERE session_id=? ORDER BY sequence DESC LIMIT ?
               ) ORDER BY sequence""",
            (session_id, max(1, min(limit, 1000))),
        )
        return [
            PiSessionEvent(
                id=row["id"], session_id=row["session_id"], event_type=row["event_type"],
                payload=json.loads(row["payload"] or "{}"), created_at=row["created_at"],
                sequence=row["sequence"],
            )
            for row in rows
        ]

    async def insert_pi_delegation(self, delegation: PiDelegation) -> None:
        await self._db.execute(
            """INSERT INTO pi_delegations
               (id, parent_session_id, worker_id, child_session_id, task, state, timeout_seconds, created_at, completed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (delegation.id, delegation.parent_session_id, delegation.worker_id, delegation.child_session_id,
             delegation.task, delegation.state.value, delegation.timeout_seconds, delegation.created_at,
             delegation.completed_at),
        )
        await self._db.commit()

    async def update_pi_delegation(self, delegation: PiDelegation) -> None:
        await self._db.execute(
            "UPDATE pi_delegations SET state=?, completed_at=? WHERE id=?",
            (delegation.state.value, delegation.completed_at, delegation.id),
        )
        await self._db.commit()

    async def update_pi_delegation_state_for_session(
        self, session_id: str, state: PiSessionState, now: int | None = None,
    ) -> None:
        now = now if now is not None else int(time.time())
        completed_at = now if state in {
            PiSessionState.IDLE,
            PiSessionState.STOPPED,
            PiSessionState.FAILED,
            PiSessionState.TERMINATION_UNKNOWN,
        } else 0
        await self._db.execute(
            "UPDATE pi_delegations SET state=?, completed_at=? WHERE child_session_id=?",
            (state.value, completed_at, session_id),
        )
        await self._db.commit()

    @staticmethod
    def _row_to_pi_delegation(row: aiosqlite.Row) -> PiDelegation:
        return PiDelegation(
            id=row["id"], parent_session_id=row["parent_session_id"], worker_id=row["worker_id"],
            child_session_id=row["child_session_id"], task=row["task"], state=PiSessionState(row["state"]),
            timeout_seconds=row["timeout_seconds"] if "timeout_seconds" in row.keys() else 0,
            created_at=row["created_at"], completed_at=row["completed_at"],
        )

    async def get_pi_delegation(self, delegation_id: str) -> PiDelegation | None:
        cursor = await self._db.execute("SELECT * FROM pi_delegations WHERE id=?", (delegation_id,))
        row = await cursor.fetchone()
        return self._row_to_pi_delegation(row) if row else None

    async def list_pi_delegations(self) -> list[PiDelegation]:
        rows = await self._db.execute_fetchall("SELECT * FROM pi_delegations ORDER BY created_at DESC")
        return [self._row_to_pi_delegation(row) for row in rows]

    # ── Jobs ──────────────────────────────────────────────────────────

    async def insert_job(self, job: Job) -> None:
        await self._db.execute(
            """INSERT INTO jobs (id, worker_id, tmux_session, command, status,
                                 exit_code, pty_enabled, kind, origin_session_id,
                                 report_revision, started_at, finished_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                job.id, job.worker_id, job.tmux_session, job.command,
                job.status.value, job.exit_code, int(job.pty_enabled), job.kind.value,
                job.origin_session_id, job.report_revision, job.started_at, job.finished_at,
            ),
        )
        await self._db.commit()

    async def update_job(self, job: Job) -> None:
        await self._db.execute(
            """UPDATE jobs SET worker_id=?, tmux_session=?, command=?, status=?,
                                 exit_code=?, pty_enabled=?, kind=?, origin_session_id=?,
                                 report_revision=?, started_at=?, finished_at=?
               WHERE id=?""",
            (
                job.worker_id, job.tmux_session, job.command,
                job.status.value, job.exit_code, int(job.pty_enabled), job.kind.value,
                job.origin_session_id, job.report_revision, job.started_at, job.finished_at,
                job.id,
            ),
        )
        await self._db.commit()

    async def get_job(self, job_id: str) -> Job | None:
        cursor = await self._db.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
        row = await cursor.fetchone()
        if not row:
            return None
        return self._row_to_job(row)

    async def list_jobs(
        self,
        worker_id: str | None = None,
        status: JobStatus | None = None,
        origin_session_id: str | None = None,
    ) -> list[Job]:
        query = "SELECT * FROM jobs WHERE 1=1"
        params: list = []
        if worker_id:
            query += " AND worker_id = ?"
            params.append(worker_id)
        if status:
            query += " AND status = ?"
            params.append(status.value)
        if origin_session_id:
            query += " AND origin_session_id = ?"
            params.append(origin_session_id)
        query += " ORDER BY started_at DESC"
        rows = await self._db.execute_fetchall(query, params)
        return [self._row_to_job(r) for r in rows]

    async def get_running_job_count_for_worker(self, worker_id: str) -> int:
        cursor = await self._db.execute(
            "SELECT COUNT(*) FROM jobs WHERE worker_id = ? AND status = ?",
            (worker_id, JobStatus.RUNNING.value),
        )
        row = await cursor.__aenter__()
        count = (await row.fetchone())[0]
        await cursor.close()
        return count

    async def upsert_reported_worker_job(self, worker_id: str, report: WorkerJobReport) -> tuple[Job, bool]:
        """Apply a worker-owned delegated-job report exactly once per revision.

        The conditional UPSERT prevents a retry race from inserting duplicates
        or letting an older report overwrite a newer worker projection.
        """
        async with self._worker_job_report_lock:
            session = await self.get_pi_session(report.origin_session_id)
            if not session:
                raise KeyError(report.origin_session_id)
            if session.worker_id != worker_id:
                raise ValueError("origin session not found for worker")
            cursor = await self._db.execute(
                """INSERT INTO jobs
                   (id, worker_id, tmux_session, command, status, exit_code, pty_enabled,
                    kind, origin_session_id, report_revision, started_at, finished_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(id) DO UPDATE SET
                     tmux_session=excluded.tmux_session,
                     command=excluded.command,
                     status=excluded.status,
                     exit_code=excluded.exit_code,
                     pty_enabled=excluded.pty_enabled,
                     report_revision=excluded.report_revision,
                     started_at=excluded.started_at,
                     finished_at=excluded.finished_at
                   WHERE jobs.worker_id=excluded.worker_id
                     AND jobs.origin_session_id=excluded.origin_session_id
                     AND jobs.kind='delegated'
                     AND jobs.report_revision < excluded.report_revision""",
                (
                    report.id, worker_id, report.tmux_session, report.command,
                    report.status.value, report.exit_code, int(report.pty_enabled), JobKind.DELEGATED.value,
                    report.origin_session_id, report.report_revision, report.started_at, report.finished_at,
                ),
            )
            changed = bool(cursor.rowcount)
            row = await self._db.execute_fetchall("SELECT * FROM jobs WHERE id=?", (report.id,))
            if not row:
                # Defensive: a failed insert must never be presented as an ack.
                await self._db.rollback()
                raise RuntimeError("worker job report was not persisted")
            job = self._row_to_job(row[0])
            if job.worker_id != worker_id or job.origin_session_id != report.origin_session_id:
                await self._db.rollback()
                raise ValueError("job identity does not belong to worker origin session")
            await self._db.commit()
            return job, changed

    def _row_to_job(self, row: aiosqlite.Row) -> Job:
        return Job(
            id=row["id"],
            worker_id=row["worker_id"],
            tmux_session=row["tmux_session"],
            command=row["command"],
            status=JobStatus(row["status"]),
            exit_code=row["exit_code"],
            pty_enabled=bool(row["pty_enabled"]),
            kind=JobKind(row["kind"]) if "kind" in row.keys() else JobKind.SSH,
            origin_session_id=row["origin_session_id"] if "origin_session_id" in row.keys() else None,
            report_revision=row["report_revision"] if "report_revision" in row.keys() else 0,
            started_at=row["started_at"],
            finished_at=row["finished_at"],
        )

    # ── Port Forwards ─────────────────────────────────────────────────

    async def insert_port_forward(self, pf: PortForward) -> None:
        await self._db.execute(
            """INSERT INTO port_forwards
               (id, worker_id, local_port, remote_port, service_name, pid, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                pf.id, pf.worker_id, pf.local_port, pf.remote_port,
                pf.service_name, pf.pid, pf.created_at,
            ),
        )
        await self._db.commit()

    async def update_port_forward_pid(self, pf_id: str, pid: int) -> None:
        await self._db.execute(
            "UPDATE port_forwards SET pid = ? WHERE id = ?", (pid, pf_id)
        )
        await self._db.commit()

    async def list_port_forwards(self, worker_id: str | None = None) -> list[PortForward]:
        query = "SELECT * FROM port_forwards"
        params: list = []
        if worker_id:
            query += " WHERE worker_id = ?"
            params.append(worker_id)
        rows = await self._db.execute_fetchall(query, params)
        return [self._row_to_pf(r) for r in rows]

    async def delete_port_forward(self, pf_id: str) -> None:
        await self._db.execute("DELETE FROM port_forwards WHERE id = ?", (pf_id,))
        await self._db.commit()

    def _row_to_pf(self, row: aiosqlite.Row) -> PortForward:
        return PortForward(
            id=row["id"],
            worker_id=row["worker_id"],
            local_port=row["local_port"],
            remote_port=row["remote_port"],
            service_name=row["service_name"],
            pid=row["pid"],
            created_at=row["created_at"],
        )

    # ── Marimo Sessions ────────────────────────────────────────────────

    async def insert_marimo_session(self, session: MarimoSession) -> None:
        await self._db.execute(
            """INSERT INTO marimo_sessions
               (id, worker_id, notebook_path, environment, job_id, tunnel_id,
                local_port, remote_port, bind_host, url, status, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                session.id, session.worker_id, session.notebook_path,
                session.environment, session.job_id, session.tunnel_id,
                session.local_port, session.remote_port, session.bind_host,
                session.url, session.status, session.created_at,
            ),
        )
        await self._db.commit()

    async def get_marimo_session(self, session_id: str) -> MarimoSession | None:
        row = await self._db.execute_fetchall(
            "SELECT * FROM marimo_sessions WHERE id = ?", (session_id,)
        )
        return self._row_to_marimo(row[0]) if row else None

    async def list_marimo_sessions(self, worker_id: str | None = None) -> list[MarimoSession]:
        query = "SELECT * FROM marimo_sessions"
        params: list = []
        if worker_id:
            query += " WHERE worker_id = ?"
            params.append(worker_id)
        query += " ORDER BY created_at DESC"
        rows = await self._db.execute_fetchall(query, params)
        return [self._row_to_marimo(row) for row in rows]

    async def delete_marimo_session(self, session_id: str) -> None:
        await self._db.execute("DELETE FROM marimo_sessions WHERE id = ?", (session_id,))
        await self._db.commit()

    @staticmethod
    def _row_to_marimo(row: aiosqlite.Row) -> MarimoSession:
        return MarimoSession(**dict(row))

    # ── Failures ───────────────────────────────────────────────────────

    async def insert_failure(self, failure: Failure) -> None:
        await self._db.execute(
            """INSERT INTO failures (job_id, worker_id, exit_code, timestamp, summary)
               VALUES (?, ?, ?, ?, ?)""",
            (
                failure.job_id, failure.worker_id,
                failure.exit_code, failure.timestamp, failure.summary,
            ),
        )
        await self._db.commit()

    async def list_failures(self, limit: int = 50) -> list[Failure]:
        rows = await self._db.execute_fetchall(
            "SELECT * FROM failures ORDER BY timestamp DESC LIMIT ?", (limit,)
        )
        return [Failure(
            id=r["id"],
            job_id=r["job_id"],
            worker_id=r["worker_id"],
            exit_code=r["exit_code"],
            timestamp=r["timestamp"],
            summary=r["summary"],
        ) for r in rows]

    # ── Admin ─────────────────────────────────────────────────────────

    async def delete_worker(self, worker_id: str) -> bool:
        """Delete a worker and all its associated records."""
        cursor = await self._db.execute(
            "DELETE FROM jobs WHERE worker_id = ?", (worker_id,)
        )
        cursor = await self._db.execute(
            "DELETE FROM port_forwards WHERE worker_id = ?", (worker_id,)
        )
        cursor = await self._db.execute(
            "DELETE FROM marimo_sessions WHERE worker_id = ?", (worker_id,)
        )
        cursor = await self._db.execute(
            "DELETE FROM workers WHERE id = ?", (worker_id,)
        )
        await self._db.commit()
        return cursor.rowcount > 0

    async def prune_workers(self, older_than_ts: int) -> int:
        """Delete all workers not seen since older_than_ts."""
        cursor = await self._db.execute(
            "DELETE FROM workers WHERE last_heartbeat_ts < ?", (older_than_ts,)
        )
        await self._db.commit()
        return cursor.rowcount
