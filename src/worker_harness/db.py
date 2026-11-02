"""Async SQLite repository layer."""

from __future__ import annotations

import asyncio
import aiosqlite
import json
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from time import monotonic_ns
from typing import AsyncIterator
from uuid import uuid4

from .models import (
    ExecutionState,
    Failure,
    GPUInfo,
    Job,
    JobKind,
    JobPriority,
    JobStatus,
    MarimoSession,
    PiBridgeEventBatch,
    PiBridgeRegister,
    PiSession,
    PiSessionCommand,
    PiSessionEvent,
    PiSessionState,
    PiSessionType,
    PortForward,
    TerminationReason,
    Worker,
    WorkerJobReport,
    WorkerRegistration,
    WorkerStatus,
)
from .queue_board import forecast_worker
from .scheduling import delays


@dataclass(frozen=True)
class AuditActor:
    """Nonsecret identity recorded on queue audit events."""

    user_id: str
    display_name: str
    actor_id: str


class QueueConflict(Exception):
    """Stale or lifecycle-incompatible queue operation (HTTP 409)."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class QueueValidationError(ValueError):
    """Malformed queue move target (HTTP 422)."""


class IdentityConflict(Exception):
    """Identity state conflict (duplicate username, bootstrap protections)."""


class ListenerConflict(Exception):
    """A forward's orchestrator listener port is already reserved."""


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
        # Identifies this control process for monotonic GPU sample stamps.
        self.instance_id = str(uuid4())
        # Admits this process's scheduling transactions in arrival (FIFO)
        # order; SQLite's write lock still serializes other connections.
        self._tx_lock = asyncio.Lock()

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
                gpu_busy TEXT DEFAULT '[]',
                cpu_cores INTEGER DEFAULT 0,
                total_ram_gb REAL DEFAULT 0,
                used_ram_gb REAL DEFAULT 0,
                total_disk_gb REAL DEFAULT 0,
                used_disk_gb REAL DEFAULT 0,
                data_paths TEXT DEFAULT '[]',
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
        if "gpu_busy" not in colnames:
            await self._db.execute(
                "ALTER TABLE workers ADD COLUMN gpu_busy TEXT DEFAULT '[]'"
            )
            colnames.add("gpu_busy")

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
        for column, declaration in {
            "gpu_telemetry": "TEXT NOT NULL DEFAULT '[]'",
            "gpu_sample_id": "TEXT NOT NULL DEFAULT ''",
            "gpu_sample_monotonic_ns": "INTEGER NOT NULL DEFAULT 0",
            "worker_boot_id": "TEXT NOT NULL DEFAULT ''",
            "daemon_instance_id": "TEXT NOT NULL DEFAULT ''",
            "gpu_sample_received_ns": "INTEGER NOT NULL DEFAULT 0",
            "gpu_sample_server_instance": "TEXT NOT NULL DEFAULT ''",
        }.items():
            if column not in colnames:
                await self._db.execute(f"ALTER TABLE workers ADD COLUMN {column} {declaration}")
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                worker_id TEXT REFERENCES workers(id),
                name TEXT NOT NULL DEFAULT '',
                tmux_session TEXT,
                command TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                queue_managed INTEGER NOT NULL DEFAULT 0,
                exit_code INTEGER,
                pty_enabled INTEGER DEFAULT 1,
                kind TEXT NOT NULL DEFAULT 'ssh',
                origin_session_id TEXT,
                report_revision INTEGER NOT NULL DEFAULT 0,
                expected_seconds INTEGER NOT NULL DEFAULT 0,
                gpu_count INTEGER NOT NULL DEFAULT 0,
                gpu_indices TEXT NOT NULL DEFAULT '[]',
                queue_order INTEGER NOT NULL DEFAULT 0,
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
        if "name" not in job_cols:
            await self._db.execute("ALTER TABLE jobs ADD COLUMN name TEXT NOT NULL DEFAULT ''")
        if "queue_managed" not in job_cols:
            await self._db.execute(
                "ALTER TABLE jobs ADD COLUMN queue_managed INTEGER NOT NULL DEFAULT 0"
            )
        if "expected_seconds" not in job_cols:
            await self._db.execute(
                "ALTER TABLE jobs ADD COLUMN expected_seconds INTEGER NOT NULL DEFAULT 0"
            )
        if "gpu_count" not in job_cols:
            await self._db.execute(
                "ALTER TABLE jobs ADD COLUMN gpu_count INTEGER NOT NULL DEFAULT 0"
            )
        if "gpu_indices" not in job_cols:
            await self._db.execute(
                "ALTER TABLE jobs ADD COLUMN gpu_indices TEXT NOT NULL DEFAULT '[]'"
            )
        if "queue_order" not in job_cols:
            await self._db.execute(
                "ALTER TABLE jobs ADD COLUMN queue_order INTEGER NOT NULL DEFAULT 0"
            )
        for column, declaration in {
            "owner_user_id": "TEXT NOT NULL DEFAULT ''",
            "submitted_by_actor_id": "TEXT NOT NULL DEFAULT ''",
            "submitted_at": "INTEGER NOT NULL DEFAULT 0",
            "submission_sequence": "INTEGER NOT NULL DEFAULT 0",
            "priority": "TEXT NOT NULL DEFAULT 'normal'",
            "priority_note": "TEXT NOT NULL DEFAULT ''",
            "deadline_at": "INTEGER",
            "termination_reason": "TEXT",
            "worker_name_at_submission": "TEXT NOT NULL DEFAULT ''",
            "execution_state": "TEXT NOT NULL DEFAULT 'active'",
            "stop_requested_at": "INTEGER NOT NULL DEFAULT 0",
            "stop_reason": "TEXT",
            "stop_confirmed_at": "INTEGER NOT NULL DEFAULT 0",
        }.items():
            if column not in job_cols:
                await self._db.execute(f"ALTER TABLE jobs ADD COLUMN {column} {declaration}")
        # Legacy rows get an immutable submission time/sequence in insertion
        # order and a worker-name snapshot; ownership is assigned at bootstrap.
        await self._db.execute(
            "UPDATE jobs SET submitted_at=COALESCE(NULLIF(started_at, 0), unixepoch()) WHERE submitted_at=0"
        )
        unsequenced = await self._db.execute_fetchall(
            "SELECT rowid FROM jobs WHERE submission_sequence=0 ORDER BY rowid"
        )
        if unsequenced:
            base = (await self._db.execute_fetchall(
                "SELECT COALESCE(MAX(submission_sequence), 0) FROM jobs"
            ))[0][0]
            await self._db.executemany(
                "UPDATE jobs SET submission_sequence=? WHERE rowid=?",
                [(base + index, row[0]) for index, row in enumerate(unsequenced, 1)],
            )
        await self._db.execute("""
            UPDATE jobs SET worker_name_at_submission=COALESCE(
                (SELECT name FROM workers WHERE workers.id=jobs.worker_id), ''
            ) WHERE worker_name_at_submission=''
        """)
        await self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_jobs_origin_session_id ON jobs(origin_session_id)"
        )
        await self._db.execute(
            """CREATE INDEX IF NOT EXISTS idx_jobs_queue_active
               ON jobs(queue_managed, worker_id, status, queue_order)"""
        )
        await self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_jobs_owner ON jobs(owner_user_id, status)"
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
        cursor = await self._db.execute("PRAGMA table_info(port_forwards)")
        forward_cols = {row[1] for row in await cursor.fetchall()}
        for column, declaration in {
            "owner_user_id": "TEXT NOT NULL DEFAULT ''",
            "created_by_actor_id": "TEXT NOT NULL DEFAULT ''",
            "bind_host": "TEXT NOT NULL DEFAULT ''",
            "marimo_session_id": "TEXT",
        }.items():
            if column not in forward_cols:
                await self._db.execute(f"ALTER TABLE port_forwards ADD COLUMN {column} {declaration}")
        await self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_port_forwards_owner ON port_forwards(owner_user_id)"
        )
        # Listener ports are reserved atomically by the database.
        await self._db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_port_forwards_listener ON port_forwards(bind_host, local_port)"
        )
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
        cursor = await self._db.execute("PRAGMA table_info(marimo_sessions)")
        marimo_cols = {row[1] for row in await cursor.fetchall()}
        for column, declaration in {
            "owner_user_id": "TEXT NOT NULL DEFAULT ''",
            "created_by_actor_id": "TEXT NOT NULL DEFAULT ''",
            "upstream_token_path": "TEXT NOT NULL DEFAULT ''",
        }.items():
            if column not in marimo_cols:
                await self._db.execute(f"ALTER TABLE marimo_sessions ADD COLUMN {column} {declaration}")
        await self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_marimo_owner ON marimo_sessions(owner_user_id)"
        )
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
                role TEXT DEFAULT '',
                token_hash TEXT DEFAULT '',
                question TEXT DEFAULT '',
                meta TEXT DEFAULT '{}',
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
            "role": "TEXT DEFAULT ''",
            "token_hash": "TEXT DEFAULT ''",
            "question": "TEXT DEFAULT ''",
            "meta": "TEXT DEFAULT '{}'",
            "owner_user_id": "TEXT NOT NULL DEFAULT ''",
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
        # Delegated agents belonged to the removed worker-container runtime.
        # Delete their dependent records first; never reinterpret them as live
        # interactive agents or replay their commands into a replacement bridge.
        for table in ("pi_router_requests", "pi_router_config", "pi_delegations"):
            await self._db.execute(f"DROP TABLE IF EXISTS {table}")
        for table in ("pi_session_commands", "pi_session_events"):
            await self._db.execute(
                f"DELETE FROM {table} WHERE session_id IN "
                "(SELECT id FROM pi_sessions WHERE session_type='delegated')"
            )
        await self._db.execute(
            """UPDATE pi_sessions SET parent_session_id=NULL
               WHERE parent_session_id IN
                 (SELECT id FROM pi_sessions WHERE session_type='delegated')"""
        )
        await self._db.execute("DELETE FROM pi_sessions WHERE session_type='delegated'")
        await self._init_identity_schema()
        await self._init_queue_audit_schema()
        await self._db.commit()

    async def _init_identity_schema(self) -> None:
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,
                username TEXT NOT NULL UNIQUE,
                display_name TEXT NOT NULL,
                password_hash TEXT NOT NULL,
                is_admin INTEGER NOT NULL DEFAULT 0,
                disabled INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
        """)
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS api_keys (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL REFERENCES users(id),
                label TEXT NOT NULL,
                token_hash TEXT NOT NULL UNIQUE,
                prefix TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                last_used_at INTEGER,
                revoked_at INTEGER
            )
        """)
        await self._db.execute("CREATE INDEX IF NOT EXISTS idx_api_keys_user ON api_keys(user_id)")
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS web_sessions (
                id TEXT PRIMARY KEY,
                token_hash TEXT NOT NULL UNIQUE,
                user_id TEXT NOT NULL REFERENCES users(id),
                csrf_token TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL
            )
        """)
        await self._db.execute("CREATE INDEX IF NOT EXISTS idx_web_sessions_user ON web_sessions(user_id)")
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS identity_settings (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                bootstrap_user_id TEXT NOT NULL REFERENCES users(id),
                created_at INTEGER NOT NULL
            )
        """)

    async def _init_queue_audit_schema(self) -> None:
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS queue_revisions (
                worker_id TEXT PRIMARY KEY,
                revision INTEGER NOT NULL DEFAULT 0
            )
        """)
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS queue_previews (
                id TEXT PRIMARY KEY,
                actor_id TEXT NOT NULL,
                actor_user_id TEXT NOT NULL,
                worker_id TEXT NOT NULL,
                job_id TEXT NOT NULL,
                before_job_id TEXT,
                reason TEXT NOT NULL,
                revision INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL,
                before_order TEXT NOT NULL,
                after_order TEXT NOT NULL,
                impacts TEXT NOT NULL,
                committed_event_id INTEGER
            )
        """)
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS queue_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                worker_id TEXT NOT NULL,
                job_id TEXT NOT NULL,
                job_name TEXT NOT NULL,
                job_owner_user_id TEXT NOT NULL,
                job_owner_display_name TEXT NOT NULL,
                actor_user_id TEXT NOT NULL,
                actor_display_name TEXT NOT NULL,
                actor_id TEXT NOT NULL,
                reason TEXT NOT NULL DEFAULT '',
                created_at INTEGER NOT NULL,
                old_revision INTEGER NOT NULL,
                new_revision INTEGER NOT NULL,
                old_order TEXT NOT NULL DEFAULT '[]',
                new_order TEXT NOT NULL DEFAULT '[]',
                preview_id TEXT UNIQUE
            )
        """)
        await self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_queue_events_worker ON queue_events(worker_id, id)"
        )
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS queue_event_impacts (
                event_id INTEGER NOT NULL REFERENCES queue_events(id),
                job_id TEXT NOT NULL,
                name TEXT NOT NULL,
                owner_user_id TEXT NOT NULL,
                owner_display_name TEXT NOT NULL,
                before_position INTEGER,
                after_position INTEGER,
                before_start_at INTEGER,
                after_start_at INTEGER,
                delay_seconds INTEGER,
                PRIMARY KEY (event_id, job_id)
            )
        """)
        await self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_queue_impacts_job ON queue_event_impacts(job_id, event_id)"
        )

    # ── Workers ──────────────────────────────────────────────────────

    @staticmethod
    def _prediction_inputs(worker: Worker | None) -> tuple:
        if worker is None:
            return ()
        return (worker.status.value, worker.gpu_count, tuple(worker.gpu_busy))

    async def upsert_worker(self, reg: WorkerRegistration) -> Worker:
        existing = await self.get_worker(reg.worker_id)
        before = self._prediction_inputs(existing)
        previous_sample = existing.gpu_sample_id if existing else ""
        if existing:
            worker = existing
            worker.update_from_registration(reg)
        else:
            worker = Worker.from_registration(reg)
        if worker.gpu_sample_id and worker.gpu_sample_id != previous_sample:
            # Only a new sample receives a fresh control-process stamp; a
            # resent sample keeps its original receipt time.
            worker.gpu_sample_received_ns = monotonic_ns()
            worker.gpu_sample_server_instance = self.instance_id
        if existing:
            await self._write_worker(worker, insert=False)
        else:
            await self._write_worker(worker, insert=True)
        if self._prediction_inputs(worker) != before:
            await self.bump_queue_revision(worker.id)
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
        rows = await self._db.execute_fetchall(
            "SELECT id FROM workers WHERE last_heartbeat_ts < ? AND status != ?",
            (cutoff_ts, WorkerStatus.OFFLINE.value),
        )
        cursor = await self._db.execute(
            "UPDATE workers SET status = ? WHERE last_heartbeat_ts < ? AND status != ?",
            (WorkerStatus.OFFLINE.value, cutoff_ts, WorkerStatus.OFFLINE.value),
        )
        await self._db.commit()
        for row in rows:
            await self.bump_queue_revision(row["id"])
        return cursor.rowcount

    async def set_worker_status(self, worker_id: str, status: WorkerStatus) -> None:
        await self._db.execute(
            "UPDATE workers SET status = ? WHERE id = ?",
            (status.value, worker_id),
        )
        await self._db.commit()
        await self.bump_queue_revision(worker_id)

    _WORKER_COLUMNS = (
        "name", "worker_ip", "dns_name", "ssh_user", "harness_dir", "gpu_count", "gpu_names",
        "gpu_vram_gb", "gpu_used_vram_gb", "gpu_busy", "cpu_cores", "total_ram_gb",
        "used_ram_gb", "total_disk_gb", "used_disk_gb", "data_paths", "status",
        "last_heartbeat_ts", "gpu_telemetry", "gpu_sample_id", "gpu_sample_monotonic_ns",
        "worker_boot_id", "daemon_instance_id", "gpu_sample_received_ns",
        "gpu_sample_server_instance",
    )

    async def _write_worker(self, w: Worker, *, insert: bool) -> None:
        values = (
            w.name, w.worker_ip, w.dns_name, w.ssh_user, w.harness_dir, w.gpu_count,
            json.dumps(w.gpu_names), json.dumps(w.gpu_vram_gb), json.dumps(w.gpu_used_vram_gb),
            json.dumps(w.gpu_busy), w.cpu_cores, w.total_ram_gb, w.used_ram_gb,
            w.total_disk_gb, w.used_disk_gb, json.dumps(w.data_paths), w.status.value,
            w.last_heartbeat_ts,
            json.dumps([gpu.model_dump(mode="json") for gpu in w.gpu_telemetry]),
            w.gpu_sample_id, w.gpu_sample_monotonic_ns, w.worker_boot_id,
            w.daemon_instance_id, w.gpu_sample_received_ns, w.gpu_sample_server_instance,
        )
        if insert:
            columns = ("id", *self._WORKER_COLUMNS, "created_at")
            await self._db.execute(
                f"INSERT INTO workers ({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)})",
                (w.id, *values, w.created_at),
            )
        else:
            assignments = ", ".join(f"{column}=?" for column in self._WORKER_COLUMNS)
            await self._db.execute(
                f"UPDATE workers SET {assignments} WHERE id=?", (*values, w.id),
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
            gpu_busy=json.loads(row["gpu_busy"] or "[]"),
            cpu_cores=row["cpu_cores"],
            total_ram_gb=row["total_ram_gb"],
            used_ram_gb=row["used_ram_gb"],
            total_disk_gb=row["total_disk_gb"],
            used_disk_gb=row["used_disk_gb"],
            data_paths=json.loads(row["data_paths"] or "[]"),
            status=WorkerStatus(row["status"]),
            last_heartbeat_ts=row["last_heartbeat_ts"],
            created_at=row["created_at"],
            gpu_telemetry=[GPUInfo(**item) for item in json.loads(row["gpu_telemetry"] or "[]")],
            gpu_sample_id=row["gpu_sample_id"],
            gpu_sample_monotonic_ns=row["gpu_sample_monotonic_ns"],
            worker_boot_id=row["worker_boot_id"],
            daemon_instance_id=row["daemon_instance_id"],
            gpu_sample_received_ns=row["gpu_sample_received_ns"],
            gpu_sample_server_instance=row["gpu_sample_server_instance"],
        )

    # ── Pi sessions ──────────────────────────────────────────────────

    @staticmethod
    def _row_to_pi_session(row: aiosqlite.Row) -> PiSession:
        return PiSession(
            id=row["id"], worker_id=row["worker_id"], parent_session_id=row["parent_session_id"],
            session_type=PiSessionType(row["session_type"]), state=PiSessionState(row["state"]),
            task=row["task"], cwd=row["cwd"], tmux_session=row["tmux_session"], detail=row["detail"],
            role=row["role"] if "role" in row.keys() else "",
            token_hash=row["token_hash"] if "token_hash" in row.keys() else "",
            question=row["question"] if "question" in row.keys() else "",
            meta=(
                json.loads(row["meta"] or "{}")
                if "meta" in row.keys()
                else {}
            ),
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
            owner_user_id=row["owner_user_id"] if "owner_user_id" in row.keys() else "",
        )

    async def insert_pi_session(self, session: PiSession) -> None:
        if not session.owner_user_id:
            raise ValueError("Pi session requires a verified owner_user_id")
        await self._db.execute(
            """INSERT INTO pi_sessions
               (id, worker_id, parent_session_id, session_type, state, task, cwd, tmux_session, detail,
                role, token_hash, question, meta, name, host, agent, bridge_incarnation,
                terminal_attachable, terminal_host, terminal_port, terminal_protocol_version,
                has_pending_messages, last_seen, created_at, updated_at, owner_user_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                session.id, session.worker_id, session.parent_session_id, session.session_type.value,
                session.state.value, session.task, session.cwd, session.tmux_session, session.detail,
                session.role, session.token_hash, session.question, json.dumps(session.meta),
                session.name, session.host, session.agent, session.bridge_incarnation,
                int(session.terminal_attachable), session.terminal_host, session.terminal_port,
                session.terminal_protocol_version, int(session.has_pending_messages),
                session.last_seen, session.created_at, session.updated_at, session.owner_user_id,
            ),
        )
        await self._db.commit()

    async def update_pi_session(self, session: PiSession) -> None:
        await self._db.execute(
            """UPDATE pi_sessions SET worker_id=?, parent_session_id=?, session_type=?, state=?, task=?, cwd=?,
               tmux_session=?, detail=?, role=?, token_hash=?, question=?, meta=?, name=?, host=?, agent=?,
               bridge_incarnation=?, terminal_attachable=?, terminal_host=?, terminal_port=?,
               terminal_protocol_version=?, has_pending_messages=?, last_seen=?, updated_at=? WHERE id=?""",
            (
                session.worker_id, session.parent_session_id, session.session_type.value, session.state.value,
                session.task, session.cwd, session.tmux_session, session.detail, session.role,
                session.token_hash, session.question, json.dumps(session.meta), session.name, session.host,
                session.agent, session.bridge_incarnation, int(session.terminal_attachable),
                session.terminal_host, session.terminal_port, session.terminal_protocol_version,
                int(session.has_pending_messages), session.last_seen, session.updated_at, session.id,
            ),
        )
        await self._db.commit()

    async def get_pi_session(self, session_id: str) -> PiSession | None:
        cursor = await self._db.execute("SELECT * FROM pi_sessions WHERE id=?", (session_id,))
        row = await cursor.fetchone()
        return self._row_to_pi_session(row) if row else None

    async def get_pi_session_by_token_hash(self, token_hash: str) -> PiSession | None:
        cursor = await self._db.execute(
            "SELECT * FROM pi_sessions WHERE token_hash=?",
            (token_hash,),
        )
        row = await cursor.fetchone()
        return self._row_to_pi_session(row) if row else None

    async def list_pi_sessions_by_role(self, role: str) -> list[PiSession]:
        rows = await self._db.execute_fetchall(
            "SELECT * FROM pi_sessions WHERE role=? ORDER BY updated_at DESC",
            (role,),
        )
        return [self._row_to_pi_session(row) for row in rows]

    async def update_pi_session_meta(self, session_id: str, meta: dict) -> None:
        await self._db.execute(
            "UPDATE pi_sessions SET meta=json_patch(meta, ?) WHERE id=?",
            (json.dumps(meta), session_id),
        )
        await self._db.commit()

    async def next_pi_session_pane_sequence(self, session_id: str) -> int:
        async with self._pi_bridge_lock:
            cursor = await self._db.execute(
                """UPDATE pi_sessions
                   SET meta=json_set(meta, '$.pane_sequence',
                       COALESCE(json_extract(meta, '$.pane_sequence'), 0) + 1)
                   WHERE id=? RETURNING json_extract(meta, '$.pane_sequence')""",
                (session_id,),
            )
            row = await cursor.fetchone()
            await cursor.close()
            await self._db.commit()
            if row is None:
                raise KeyError(session_id)
            return int(row[0])

    async def reparent_pi_session_children(self, old_id: str, new_id: str) -> None:
        await self._db.execute(
            """UPDATE pi_sessions SET parent_session_id=?
               WHERE parent_session_id=? AND role='task'
                 AND json_extract(meta, '$.project') =
                   (SELECT json_extract(meta, '$.project') FROM pi_sessions WHERE id=?)""",
            (new_id, old_id, new_id),
        )
        await self._db.commit()

    async def finish_pi_session(
        self, session_id: str, state: PiSessionState, detail: str,
    ) -> None:
        async with self._pi_bridge_lock:
            await self._db.execute(
                """UPDATE pi_sessions SET state=?, detail=?, updated_at=?, question='',
                   token_hash='', bridge_incarnation=NULL, terminal_attachable=0
                   WHERE id=?""",
                (state.value, detail, int(time.time()), session_id),
            )
            await self._db.commit()


    async def set_pi_session_blocked(self, session_id: str, question: str) -> None:
        now = int(time.time())
        async with self._pi_bridge_lock:
            cursor = await self._db.execute(
                """UPDATE pi_sessions SET state=?, question=?, updated_at=? WHERE id=?
                   AND state NOT IN ('stopped', 'failed', 'termination_unknown')""",
                (PiSessionState.BLOCKED.value, question, now, session_id),
            )
            if not cursor.rowcount:
                await self._db.commit()
                raise ValueError("task session is no longer active")
            await self._db.execute(
                """INSERT INTO pi_session_events
                   (id, session_id, event_type, payload, created_at)
                   VALUES (?, ?, 'blocked', ?, ?)""",
                (str(uuid4()), session_id, json.dumps({"question": question}), now),
            )
            await self._db.commit()


    async def list_pi_sessions(self, worker_id: str | None = None) -> list[PiSession]:
        if worker_id:
            rows = await self._db.execute_fetchall(
                "SELECT * FROM pi_sessions WHERE worker_id=? ORDER BY updated_at DESC", (worker_id,)
            )
        else:
            rows = await self._db.execute_fetchall("SELECT * FROM pi_sessions ORDER BY updated_at DESC")
        return [self._row_to_pi_session(row) for row in rows]

    async def register_interactive_pi_session(
        self, payload: PiBridgeRegister, *, owner_user_id: str, now: int | None = None,
    ) -> PiSession:
        """Create or replace one plain-Pi bridge incarnation.

        ``owner_user_id`` comes from the verified caller; an existing row's
        owner is immutable and a mismatching registration is refused.
        """
        if not owner_user_id:
            raise ValueError("bridge registration requires a verified owner")
        now = now if now is not None else int(time.time())
        async with self._pi_bridge_lock:
            existing = await self.get_pi_session(payload.session_id)
            if existing and not existing.session_type.bridge_backed:
                raise ValueError("session id does not belong to a bridge-backed session")
            if existing and existing.owner_user_id and existing.owner_user_id != owner_user_id:
                raise PermissionError("bridge registration cannot change the session owner")
            created_at = existing.created_at if existing else now
            await self._db.execute(
                """INSERT INTO pi_sessions
                   (id, worker_id, parent_session_id, session_type, state, task, cwd, tmux_session,
                    detail, name, host, agent, bridge_incarnation, terminal_attachable, terminal_host,
                    terminal_port, terminal_protocol_version, has_pending_messages, last_seen, created_at, updated_at,
                    owner_user_id)
                   VALUES (?, NULL, NULL, ?, ?, '', ?, '', '', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(id) DO UPDATE SET
                     state=CASE
                         WHEN pi_sessions.state='blocked' THEN pi_sessions.state
                         ELSE excluded.state
                     END,
                     cwd=CASE WHEN pi_sessions.role!='' AND pi_sessions.cwd!=''
                         THEN pi_sessions.cwd ELSE excluded.cwd END, detail='',
                     name=CASE
                         WHEN excluded.name='' OR pi_sessions.role!='' THEN pi_sessions.name
                         ELSE excluded.name
                     END,
                     host=CASE
                         WHEN excluded.host='' OR pi_sessions.role!='' THEN pi_sessions.host
                         ELSE excluded.host
                     END,
                     agent=excluded.agent,
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
                    owner_user_id,
                ),
            )
            if payload.resume_path:
                await self._db.execute(
                    "UPDATE pi_sessions SET meta=json_set(meta, '$.resume_path', ?) WHERE id=?",
                    (payload.resume_path, payload.session_id),
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
            if not session or not session.session_type.bridge_backed:
                raise KeyError(session_id)
            if session.bridge_incarnation != payload.incarnation:
                raise PermissionError("stale bridge incarnation")
            previous_activity = (session.state, session.detail, session.has_pending_messages)
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
            if payload.state is not None and (
                session.state != PiSessionState.BLOCKED
                or payload.state in {
                    PiSessionState.STOPPED, PiSessionState.FAILED,
                    PiSessionState.TERMINATION_UNKNOWN,
                }
            ):
                session.state = payload.state
            if session.state in {
                PiSessionState.STOPPED, PiSessionState.FAILED, PiSessionState.TERMINATION_UNKNOWN,
            }:
                session.question = ""
            session.detail = payload.detail
            if payload.has_pending_messages is not None:
                pending = await self._db.execute_fetchall(
                    "SELECT 1 FROM pi_session_commands WHERE session_id=? AND delivered_at=0 LIMIT 1",
                    (session_id,),
                )
                session.has_pending_messages = payload.has_pending_messages or bool(pending)
            session.last_seen = now
            if persisted or previous_activity != (
                session.state, session.detail, session.has_pending_messages,
            ):
                session.updated_at = now
            await self._db.execute(
                """UPDATE pi_sessions SET state=?, detail=?, question=?, has_pending_messages=?, last_seen=?, updated_at=?
                   WHERE id=?""",
                (session.state.value, session.detail, session.question,
                 int(session.has_pending_messages), now, session.updated_at, session_id),
            )
            await self._db.commit()
            return session, persisted

    async def enqueue_pi_session_command(self, command: PiSessionCommand) -> None:
        async with self._pi_bridge_lock:
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
            await self._db.execute(
                "UPDATE pi_sessions SET updated_at=?, has_pending_messages=1 WHERE id=?",
                (command.created_at or int(time.time()), command.session_id),
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
            if not session or not session.session_type.bridge_backed:
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
            if not session or not session.session_type.bridge_backed:
                raise KeyError(session_id)
            if session.bridge_incarnation != incarnation:
                raise PermissionError("stale bridge incarnation")
            command_row = await (
                await self._db.execute(
                    "SELECT kind FROM pi_session_commands WHERE id=? AND session_id=?",
                    (command_id, session_id),
                )
            ).fetchone()
            cursor = await self._db.execute(
                """UPDATE pi_session_commands SET delivered_at=?
                   WHERE id=? AND session_id=? AND claimed_by=? AND delivered_at=0""",
                (now, command_id, session_id, incarnation),
            )
            if (
                cursor.rowcount
                and command_row
                and command_row["kind"] == "prompt"
                and session.state == PiSessionState.BLOCKED
            ):
                await self._db.execute(
                    "UPDATE pi_sessions SET state=?, question='', updated_at=? WHERE id=?",
                    (PiSessionState.WORKING.value, now, session_id),
                )
            await self._db.commit()
            return bool(cursor.rowcount)

    async def sweep_stale_interactive_pi_sessions(self, cutoff_ts: int, now: int | None = None) -> list[str]:
        now = now if now is not None else int(time.time())
        bridge_types = tuple(
            session_type.value for session_type in PiSessionType if session_type.bridge_backed
        )
        placeholders = ",".join("?" for _ in bridge_types)
        async with self._pi_bridge_lock:
            rows = await self._db.execute_fetchall(
                f"""SELECT id FROM pi_sessions
                    WHERE session_type IN ({placeholders}) AND last_seen<?
                    AND state NOT IN (?, ?)""",
                (
                    *bridge_types,
                    cutoff_ts,
                    PiSessionState.STOPPED.value,
                    PiSessionState.FAILED.value,
                ),
            )
            session_ids = [row["id"] for row in rows]
            for session_id in session_ids:
                await self._db.execute(
                    """UPDATE pi_sessions SET state=?, detail=?, updated_at=?, question='',
                       token_hash='', terminal_attachable=0 WHERE id=?""",
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


    # ── Scheduling transactions ───────────────────────────────────────

    @asynccontextmanager
    async def scheduling_tx(self) -> AsyncIterator[aiosqlite.Connection]:
        """One short-lived connection holding BEGIN IMMEDIATE for snapshot+write.

        Every update of scheduling-relevant job fields goes through here so
        reads and conditional writes are atomic across connections/processes.
        No SSH or other network I/O may happen inside the transaction.
        Within one process transactions are admitted in arrival order, so
        concurrent submissions keep their request order.
        """
        async with self._tx_lock:
            conn = await aiosqlite.connect(str(self.path), isolation_level=None)
            conn.row_factory = aiosqlite.Row
            try:
                await conn.execute("PRAGMA busy_timeout=5000")
                await conn.execute("BEGIN IMMEDIATE")
                try:
                    yield conn
                except BaseException:
                    await conn.execute("ROLLBACK")
                    raise
                await conn.execute("COMMIT")
            finally:
                await conn.close()

    @staticmethod
    async def _revision_tx(conn: aiosqlite.Connection, worker_id: str) -> int:
        row = await (await conn.execute(
            "SELECT revision FROM queue_revisions WHERE worker_id=?", (worker_id,),
        )).fetchone()
        return int(row[0]) if row else 0

    @staticmethod
    async def _bump_revision_tx(conn: aiosqlite.Connection, worker_id: str) -> int:
        cursor = await conn.execute(
            """INSERT INTO queue_revisions (worker_id, revision) VALUES (?, 1)
               ON CONFLICT(worker_id) DO UPDATE SET revision=revision+1
               RETURNING revision""",
            (worker_id,),
        )
        row = await cursor.fetchone()
        await cursor.close()
        return int(row[0])

    async def bump_queue_revision(self, worker_id: str) -> int:
        async with self.scheduling_tx() as conn:
            return await self._bump_revision_tx(conn, worker_id)

    async def queue_revisions(self) -> dict[str, int]:
        rows = await self._db.execute_fetchall("SELECT worker_id, revision FROM queue_revisions")
        return {row["worker_id"]: int(row["revision"]) for row in rows}

    # ── Jobs ──────────────────────────────────────────────────────────

    _JOB_COLUMNS = (
        "id", "worker_id", "name", "tmux_session", "command", "status", "queue_managed",
        "exit_code", "pty_enabled", "kind", "origin_session_id", "report_revision",
        "expected_seconds", "gpu_count", "gpu_indices", "queue_order", "started_at",
        "finished_at", "owner_user_id", "submitted_by_actor_id", "submitted_at",
        "submission_sequence", "priority", "priority_note", "deadline_at",
        "termination_reason", "worker_name_at_submission", "execution_state",
        "stop_requested_at", "stop_reason", "stop_confirmed_at",
    )

    @staticmethod
    def _job_values(job: Job) -> tuple:
        return (
            job.id, job.worker_id, job.name, job.tmux_session, job.command, job.status.value,
            int(job.queue_managed), job.exit_code, int(job.pty_enabled), job.kind.value,
            job.origin_session_id, job.report_revision, job.expected_seconds, job.gpu_count,
            json.dumps(job.gpu_indices), job.queue_order, job.started_at, job.finished_at,
            job.owner_user_id, job.submitted_by_actor_id, job.submitted_at,
            job.submission_sequence, job.priority.value, job.priority_note, job.deadline_at,
            job.termination_reason.value if job.termination_reason else None,
            job.worker_name_at_submission, job.execution_state.value, job.stop_requested_at,
            job.stop_reason.value if job.stop_reason else None, job.stop_confirmed_at,
        )

    async def _insert_new_job_tx(self, conn: aiosqlite.Connection, job: Job) -> Job:
        if not job.owner_user_id or not job.submitted_by_actor_id:
            raise ValueError("job requires owner_user_id and submitted_by_actor_id")
        job.submitted_at = job.submitted_at or int(time.time())
        row = await (await conn.execute(
            "SELECT COALESCE(MAX(submission_sequence), 0) + 1 FROM jobs",
        )).fetchone()
        job.submission_sequence = int(row[0])
        if job.worker_id and not job.worker_name_at_submission:
            worker_row = await (await conn.execute(
                "SELECT name FROM workers WHERE id=?", (job.worker_id,),
            )).fetchone()
            job.worker_name_at_submission = worker_row["name"] if worker_row else ""
        await conn.execute(
            f"INSERT INTO jobs ({', '.join(self._JOB_COLUMNS)}) "
            f"VALUES ({', '.join('?' for _ in self._JOB_COLUMNS)})",
            self._job_values(job),
        )
        return job

    async def insert_job(self, job: Job) -> Job:
        """Persist an immediately launched (CPU/setup) job before its launch."""
        async with self.scheduling_tx() as conn:
            return await self._insert_new_job_tx(conn, job)

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
        owner_user_id: str | None = None,
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
        if owner_user_id is not None:
            query += " AND owner_user_id = ?"
            params.append(owner_user_id)
        query += " ORDER BY submission_sequence DESC"
        rows = await self._db.execute_fetchall(query, params)
        return [self._row_to_job(r) for r in rows]

    async def insert_queued_job(self, job: Job) -> Job:
        async with self.scheduling_tx() as conn:
            row = await (await conn.execute(
                """SELECT COALESCE(MAX(queue_order), 0) FROM jobs
                   WHERE queue_managed = 1 AND worker_id = ? AND status IN (?, ?, ?)""",
                (job.worker_id, JobStatus.PENDING.value, JobStatus.STARTING.value, JobStatus.RUNNING.value),
            )).fetchone()
            job.queue_managed = True
            job.status = JobStatus.PENDING
            job.gpu_indices = []
            job.queue_order = int(row[0]) + 1
            job.started_at = 0
            job.finished_at = 0
            await self._insert_new_job_tx(conn, job)
            await self._bump_revision_tx(conn, job.worker_id or "")
            return job

    async def list_queued_jobs(self, worker_id: str | None = None) -> list[Job]:
        query = """
            SELECT * FROM jobs
            WHERE queue_managed = 1
              AND status IN (?, ?, ?)
        """
        params: list[object] = [
            JobStatus.STARTING.value,
            JobStatus.RUNNING.value,
            JobStatus.PENDING.value,
        ]
        if worker_id is not None:
            query += " AND worker_id = ?"
            params.append(worker_id)
        query += """
            ORDER BY worker_id,
                     CASE status
                       WHEN 'starting' THEN 0
                       WHEN 'running' THEN 1
                       ELSE 2
                     END,
                     CASE WHEN status = 'pending' THEN queue_order ELSE started_at END,
                     queue_order,
                     id
        """
        rows = await self._db.execute_fetchall(query, params)
        return [self._row_to_job(row) for row in rows]

    async def list_active_cpu_jobs(self, worker_id: str | None = None) -> list[Job]:
        query = """SELECT * FROM jobs WHERE queue_managed = 0 AND kind = ?
                   AND status IN (?, ?)"""
        params: list[object] = [JobKind.SSH.value, JobStatus.STARTING.value, JobStatus.RUNNING.value]
        if worker_id is not None:
            query += " AND worker_id = ?"
            params.append(worker_id)
        query += " ORDER BY worker_id, started_at, submission_sequence"
        rows = await self._db.execute_fetchall(query, params)
        return [self._row_to_job(row) for row in rows]

    async def list_active_jobs(self) -> list[Job]:
        rows = await self._db.execute_fetchall(
            "SELECT * FROM jobs WHERE kind = ? AND status IN (?, ?) ORDER BY started_at",
            (JobKind.SSH.value, JobStatus.STARTING.value, JobStatus.RUNNING.value),
        )
        return [self._row_to_job(row) for row in rows]

    async def claim_queued_job(
        self,
        job_id: str,
        gpu_indices: list[int],
        started_at: int,
        *,
        worker_id: str,
    ) -> Job | None:
        async with self.scheduling_tx() as conn:
            cursor = await conn.execute(
                """UPDATE jobs
                   SET status = ?, gpu_indices = ?, started_at = ?
                   WHERE id = ? AND queue_managed = 1 AND status = ?
                     AND worker_id = ? AND gpu_count = ?
                     AND id = (
                       SELECT id FROM jobs
                       WHERE queue_managed = 1 AND worker_id = ? AND status = ?
                       ORDER BY queue_order, id LIMIT 1
                     )
                     AND NOT EXISTS (
                       SELECT 1 FROM jobs AS active, json_each(active.gpu_indices) AS assigned
                       WHERE active.queue_managed = 1 AND active.worker_id = ?
                         AND active.status IN (?, ?)
                         AND assigned.value IN (SELECT value FROM json_each(?))
                     )""",
                (
                    JobStatus.STARTING.value, json.dumps(gpu_indices), started_at,
                    job_id, JobStatus.PENDING.value, worker_id, len(gpu_indices),
                    worker_id, JobStatus.PENDING.value,
                    worker_id, JobStatus.STARTING.value, JobStatus.RUNNING.value,
                    json.dumps(gpu_indices),
                ),
            )
            if not cursor.rowcount:
                return None
            await self._bump_revision_tx(conn, worker_id)
        return await self.get_job(job_id)

    async def _transition_tx(
        self,
        conn: aiosqlite.Connection,
        job_id: str,
        assignments: str,
        params: tuple,
        *,
        expected_statuses: tuple[JobStatus, ...],
        extra_condition: str = "",
    ) -> aiosqlite.Row | None:
        """Conditionally update one job; return the prior row when applied."""
        row = await (await conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,))).fetchone()
        if row is None or row["status"] not in {status.value for status in expected_statuses}:
            return None
        placeholders = ", ".join("?" for _ in expected_statuses)
        cursor = await conn.execute(
            f"UPDATE jobs SET {assignments} WHERE id=? AND status IN ({placeholders}) {extra_condition}",
            (*params, job_id, *(status.value for status in expected_statuses)),
        )
        if not cursor.rowcount:
            return None
        if row["queue_managed"]:
            await self._bump_revision_tx(conn, row["worker_id"])
        return row

    async def mark_job_running(self, job_id: str) -> Job | None:
        async with self.scheduling_tx() as conn:
            applied = await self._transition_tx(
                conn, job_id, "status=?", (JobStatus.RUNNING.value,),
                expected_statuses=(JobStatus.STARTING,),
            )
        return await self.get_job(job_id) if applied else None

    async def fail_job_launch(self, job_id: str, *, finished_at: int) -> Job | None:
        async with self.scheduling_tx() as conn:
            applied = await self._transition_tx(
                conn, job_id, "status=?, exit_code=-1, finished_at=?",
                (JobStatus.FAILED.value, finished_at),
                expected_statuses=(JobStatus.STARTING,),
            )
        return await self.get_job(job_id) if applied else None

    async def reconcile_starting_job(
        self,
        job_id: str,
        *,
        launch_observed: bool,
    ) -> Job | None:
        async with self.scheduling_tx() as conn:
            row = await (await conn.execute(
                "SELECT worker_id, queue_managed FROM jobs WHERE id = ? AND status = ?",
                (job_id, JobStatus.STARTING.value),
            )).fetchone()
            if row is None:
                return None
            if launch_observed:
                cursor = await conn.execute(
                    "UPDATE jobs SET status = ? WHERE id = ? AND status = ?",
                    (JobStatus.RUNNING.value, job_id, JobStatus.STARTING.value),
                )
            elif row["queue_managed"]:
                # A launch never observed returns to the head of its queue.
                await conn.execute(
                    """UPDATE jobs SET queue_order = queue_order + 1
                       WHERE queue_managed = 1 AND worker_id = ? AND status = ?""",
                    (row["worker_id"], JobStatus.PENDING.value),
                )
                cursor = await conn.execute(
                    """UPDATE jobs
                       SET status = ?, gpu_indices = '[]', started_at = 0, queue_order = 1
                       WHERE id = ? AND queue_managed = 1 AND status = ?""",
                    (JobStatus.PENDING.value, job_id, JobStatus.STARTING.value),
                )
            else:
                cursor = await conn.execute(
                    """UPDATE jobs SET status = ?, exit_code = -1, finished_at = unixepoch()
                       WHERE id = ? AND status = ?""",
                    (JobStatus.FAILED.value, job_id, JobStatus.STARTING.value),
                )
            if not cursor.rowcount:
                return None
            if row["queue_managed"]:
                await self._bump_revision_tx(conn, row["worker_id"])
        return await self.get_job(job_id)

    async def _renumber_pending_tx(self, conn: aiosqlite.Connection, worker_id: str) -> list[str]:
        ids = await self._pending_order_tx(conn, worker_id)
        await conn.executemany(
            "UPDATE jobs SET queue_order = ? WHERE id = ?",
            [(index, job_id) for index, job_id in enumerate(ids, 1)],
        )
        return ids

    async def cancel_pending_job(
        self, job_id: str, *, reason: TerminationReason, finished_at: int,
    ) -> Job | None:
        async with self.scheduling_tx() as conn:
            row = await (await conn.execute(
                "SELECT worker_id FROM jobs WHERE id = ? AND queue_managed = 1 AND status = ?",
                (job_id, JobStatus.PENDING.value),
            )).fetchone()
            if row is None:
                return None
            await conn.execute(
                """UPDATE jobs SET status = ?, exit_code = -1, finished_at = ?, queue_order = 0,
                   termination_reason = ? WHERE id = ? AND status = ?""",
                (JobStatus.FAILED.value, finished_at, reason.value, job_id, JobStatus.PENDING.value),
            )
            await self._renumber_pending_tx(conn, row["worker_id"])
            await self._bump_revision_tx(conn, row["worker_id"])
        return await self.get_job(job_id)

    async def cancel_pending_jobs_for_user(self, user_id: str, *, finished_at: int) -> list[str]:
        async with self.scheduling_tx() as conn:
            rows = await conn.execute_fetchall(
                "SELECT id, worker_id FROM jobs WHERE owner_user_id = ? AND status = ?",
                (user_id, JobStatus.PENDING.value),
            )
            if not rows:
                return []
            await conn.execute(
                """UPDATE jobs SET status = ?, exit_code = -1, finished_at = ?, queue_order = 0,
                   termination_reason = ? WHERE owner_user_id = ? AND status = ?""",
                (
                    JobStatus.FAILED.value, finished_at, TerminationReason.USER_DISABLED.value,
                    user_id, JobStatus.PENDING.value,
                ),
            )
            for worker_id in sorted({row["worker_id"] for row in rows if row["worker_id"]}):
                await self._renumber_pending_tx(conn, worker_id)
                await self._bump_revision_tx(conn, worker_id)
            return [row["id"] for row in rows]

    async def request_job_stop(
        self, job_id: str, *, reason: TerminationReason, now: int,
    ) -> Job | None:
        """Durably record a stop request before contacting the runner.

        The first request's reason wins; repeated requests are idempotent.
        """
        async with self.scheduling_tx() as conn:
            await self._transition_tx(
                conn, job_id,
                "stop_requested_at=?, stop_reason=?, execution_state=?",
                (now, reason.value, ExecutionState.STOPPING.value),
                expected_statuses=(JobStatus.STARTING, JobStatus.RUNNING),
                extra_condition="AND stop_requested_at = 0",
            )
        return await self.get_job(job_id)

    async def mark_termination_unknown(self, job_id: str) -> Job | None:
        async with self.scheduling_tx() as conn:
            await self._transition_tx(
                conn, job_id, "execution_state=?", (ExecutionState.TERMINATION_UNKNOWN.value,),
                expected_statuses=(JobStatus.STARTING, JobStatus.RUNNING),
                extra_condition="AND execution_state != 'termination_unknown'",
            )
        return await self.get_job(job_id)

    async def record_stop_confirmed(self, job_id: str, *, exit_code: int, confirmed_at: int) -> Job | None:
        """Persist a confirmed runner result while the allocation stays held."""
        async with self.scheduling_tx() as conn:
            await conn.execute(
                """UPDATE jobs SET stop_confirmed_at = ?, exit_code = ?
                   WHERE id = ? AND status IN (?, ?) AND stop_confirmed_at = 0""",
                (confirmed_at, exit_code, job_id, JobStatus.STARTING.value, JobStatus.RUNNING.value),
            )
        return await self.get_job(job_id)

    async def finish_job(
        self,
        job_id: str,
        *,
        exit_code: int,
        termination_reason: TerminationReason | None,
        finished_at: int,
    ) -> Job | None:
        """Atomically record confirmed termination and release the allocation."""
        status = (
            JobStatus.FAILED if termination_reason is not None or exit_code != 0 else JobStatus.DONE
        )
        async with self.scheduling_tx() as conn:
            applied = await self._transition_tx(
                conn, job_id,
                "status=?, exit_code=?, finished_at=?, termination_reason=?, execution_state=?",
                (
                    status.value, exit_code, finished_at,
                    termination_reason.value if termination_reason else None,
                    ExecutionState.ACTIVE.value,
                ),
                expected_statuses=(JobStatus.STARTING, JobStatus.RUNNING),
            )
        return await self.get_job(job_id) if applied else None

    async def running_queue_gpu_indices(self, worker_id: str) -> set[int]:
        rows = await self._db.execute_fetchall(
            """SELECT gpu_indices FROM jobs
               WHERE queue_managed = 1 AND worker_id = ? AND status IN (?, ?)""",
            (worker_id, JobStatus.STARTING.value, JobStatus.RUNNING.value),
        )
        return {
            index
            for row in rows
            for index in json.loads(row["gpu_indices"])
        }

    # ── Owner metadata edits, previews, moves and audit ───────────────

    @staticmethod
    async def _pending_order_tx(conn: aiosqlite.Connection, worker_id: str) -> list[str]:
        rows = await conn.execute_fetchall(
            """SELECT id FROM jobs WHERE queue_managed = 1 AND worker_id = ? AND status = ?
               ORDER BY queue_order, id""",
            (worker_id, JobStatus.PENDING.value),
        )
        return [row["id"] for row in rows]

    @staticmethod
    async def _display_names_tx(conn: aiosqlite.Connection, user_ids: set[str]) -> dict[str, str]:
        if not user_ids:
            return {}
        ids = sorted(user_ids)
        rows = await conn.execute_fetchall(
            f"SELECT id, display_name FROM users WHERE id IN ({', '.join('?' for _ in ids)})", ids,
        )
        return {row["id"]: row["display_name"] for row in rows}

    async def user_display_names(self, user_ids: set[str]) -> dict[str, str]:
        return await self._display_names_tx(self._db, user_ids)

    async def _insert_event_tx(
        self,
        conn: aiosqlite.Connection,
        *,
        event_type: str,
        worker_id: str,
        job: Job,
        actor: AuditActor,
        reason: str,
        now: int,
        old_revision: int,
        new_revision: int,
        old_order: list[str],
        new_order: list[str],
        impacts: list[dict],
        preview_id: str | None = None,
    ) -> int:
        names = await self._display_names_tx(conn, {job.owner_user_id})
        cursor = await conn.execute(
            """INSERT INTO queue_events
               (event_type, worker_id, job_id, job_name, job_owner_user_id, job_owner_display_name,
                actor_user_id, actor_display_name, actor_id, reason, created_at, old_revision,
                new_revision, old_order, new_order, preview_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                event_type, worker_id, job.id, job.name, job.owner_user_id,
                names.get(job.owner_user_id, ""), actor.user_id, actor.display_name,
                actor.actor_id, reason, now, old_revision, new_revision,
                json.dumps(old_order), json.dumps(new_order), preview_id,
            ),
        )
        event_id = int(cursor.lastrowid)
        await conn.executemany(
            """INSERT INTO queue_event_impacts
               (event_id, job_id, name, owner_user_id, owner_display_name, before_position,
                after_position, before_start_at, after_start_at, delay_seconds)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                (
                    event_id, impact["job_id"], impact["name"], impact["owner_user_id"],
                    impact["owner_display_name"], impact["before_position"],
                    impact["after_position"], impact["before_start_at"],
                    impact["after_start_at"], impact["delay_seconds"],
                )
                for impact in impacts
            ],
        )
        return event_id

    async def update_job_metadata(
        self,
        job_id: str,
        *,
        owner_user_id: str,
        actor: AuditActor,
        changes: dict,
        pending: bool,
        destination_worker_id: str | None = None,
        now: int,
    ) -> Job:
        """Owner-only edit of a pending (``pending=True``) or active job.

        Raises KeyError for missing/foreign jobs and ValueError for lifecycle
        or validation conflicts. Worker transfers append to the destination
        queue and record a ``worker_move`` event; other edits ``job_updated``.
        """
        allowed = (
            {"name", "expected_seconds", "gpu_count", "priority", "priority_note", "deadline_at"}
            if pending else {"expected_seconds", "priority", "priority_note", "deadline_at"}
        )
        if set(changes) - allowed:
            raise ValueError("field cannot be changed in this job state")
        async with self.scheduling_tx() as conn:
            row = await (await conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,))).fetchone()
            if row is None or row["owner_user_id"] != owner_user_id:
                raise KeyError(job_id)
            job = self._row_to_job(row)
            if pending:
                if not job.queue_managed or job.status != JobStatus.PENDING:
                    raise ValueError("only pending queued jobs can be updated")
            elif job.status not in (JobStatus.STARTING, JobStatus.RUNNING):
                raise ValueError("only starting or running jobs can be updated")
            source_worker_id = job.worker_id or ""
            destination = destination_worker_id or source_worker_id
            updated = job.model_copy(update=changes)
            if updated.priority == JobPriority.TIME_CRITICAL and not updated.priority_note.strip():
                raise ValueError("time_critical priority requires a priority_note")
            if pending:
                worker_row = await (await conn.execute(
                    "SELECT gpu_count FROM workers WHERE id=?", (destination,),
                )).fetchone()
                if worker_row is None:
                    raise ValueError("destination worker no longer exists")
                if updated.gpu_count > worker_row["gpu_count"]:
                    raise ValueError("gpu_count exceeds destination worker capacity")
            old_order = await self._pending_order_tx(conn, source_worker_id)
            old_revision = await self._revision_tx(conn, source_worker_id)
            await conn.execute(
                """UPDATE jobs SET name=?, expected_seconds=?, gpu_count=?, priority=?,
                   priority_note=?, deadline_at=? WHERE id=?""",
                (
                    updated.name, updated.expected_seconds, updated.gpu_count,
                    updated.priority.value, updated.priority_note, updated.deadline_at, job_id,
                ),
            )
            moved = pending and destination != source_worker_id
            if moved:
                tail = await (await conn.execute(
                    """SELECT COALESCE(MAX(queue_order), 0) FROM jobs
                       WHERE queue_managed = 1 AND worker_id = ? AND status IN (?, ?, ?)""",
                    (destination, JobStatus.PENDING.value, JobStatus.STARTING.value, JobStatus.RUNNING.value),
                )).fetchone()
                await conn.execute(
                    "UPDATE jobs SET worker_id=?, queue_order=? WHERE id=?",
                    (destination, int(tail[0]) + 1, job_id),
                )
                await self._renumber_pending_tx(conn, source_worker_id)
                await self._renumber_pending_tx(conn, destination)
                await self._bump_revision_tx(conn, destination)
            prediction_changed = moved or (
                job.queue_managed
                and (updated.expected_seconds, updated.gpu_count) != (job.expected_seconds, job.gpu_count)
            )
            new_revision = (
                await self._bump_revision_tx(conn, source_worker_id)
                if prediction_changed else old_revision
            )
            new_order = await self._pending_order_tx(conn, source_worker_id)
            changed_fields = sorted(key for key in changes if getattr(job, key) != getattr(updated, key))
            if moved or changed_fields:
                await self._insert_event_tx(
                    conn, event_type="worker_move" if moved else "job_updated",
                    worker_id=source_worker_id, job=updated, actor=actor,
                    reason=(
                        "moved to another worker by its owner" if moved
                        else "owner updated " + ", ".join(changed_fields)
                    ),
                    now=now, old_revision=old_revision, new_revision=new_revision,
                    old_order=old_order, new_order=new_order, impacts=[],
                )
        persisted = await self.get_job(job_id)
        assert persisted is not None
        return persisted

    async def _worker_snapshot_tx(
        self, conn: aiosqlite.Connection, worker_id: str,
    ) -> tuple[Worker | None, list[Job]]:
        worker_row = await (await conn.execute("SELECT * FROM workers WHERE id=?", (worker_id,))).fetchone()
        rows = await conn.execute_fetchall(
            "SELECT * FROM jobs WHERE queue_managed = 1 AND worker_id = ? AND status IN (?, ?, ?)",
            (worker_id, JobStatus.PENDING.value, JobStatus.STARTING.value, JobStatus.RUNNING.value),
        )
        worker = self._row_to_worker(worker_row) if worker_row else None
        return worker, [self._row_to_job(row) for row in rows]

    async def create_queue_preview(
        self,
        job_id: str,
        *,
        before_job_id: str | None,
        expected_revision: int,
        reason: str,
        actor: AuditActor,
        now: int,
        ttl_seconds: int = 60,
    ) -> dict:
        """Compute (never apply) a shared reorder and its estimated impacts."""
        async with self.scheduling_tx() as conn:
            row = await (await conn.execute(
                "SELECT * FROM jobs WHERE id=? AND queue_managed=1", (job_id,),
            )).fetchone()
            if row is None:
                raise KeyError(job_id)
            job = self._row_to_job(row)
            if job.status != JobStatus.PENDING:
                raise QueueConflict("NOT_PENDING", "only pending queued jobs can be moved")
            worker_id = job.worker_id or ""
            revision = await self._revision_tx(conn, worker_id)
            if revision != expected_revision:
                raise QueueConflict("QUEUE_CHANGED", "the queue changed; refresh and preview again")
            if before_job_id == job_id:
                raise QueueValidationError("a job cannot be moved before itself")
            before_order = await self._pending_order_tx(conn, worker_id)
            if before_job_id is not None and before_job_id not in before_order:
                raise QueueValidationError("target must be a pending job on the same worker")
            after_order = [item for item in before_order if item != job_id]
            if before_job_id is None:
                after_order.append(job_id)
            else:
                after_order.insert(after_order.index(before_job_id), job_id)
            if after_order == before_order:
                return {"changed": False}
            worker, jobs = await self._worker_snapshot_tx(conn, worker_id)
            before_estimates = forecast_worker(worker, jobs, now, pending_order=before_order)
            after_estimates = forecast_worker(worker, jobs, now, pending_order=after_order)
            shifts = delays(before_estimates, after_estimates, before_order)
            by_id = {item.id: item for item in jobs}
            names = await self._display_names_tx(conn, {item.owner_user_id for item in jobs})
            impacts = []
            for pending_id in before_order:
                old_position = before_order.index(pending_id) + 1
                new_position = after_order.index(pending_id) + 1
                before_start = before_estimates[pending_id].estimated_start_at
                after_start = after_estimates[pending_id].estimated_start_at
                if old_position == new_position and before_start == after_start:
                    continue
                item = by_id[pending_id]
                impacts.append({
                    "job_id": pending_id,
                    "name": item.name,
                    "owner_user_id": item.owner_user_id,
                    "owner_display_name": names.get(item.owner_user_id, ""),
                    "before_position": old_position,
                    "after_position": new_position,
                    "before_start_at": before_start,
                    "after_start_at": after_start,
                    "delay_seconds": shifts[pending_id],
                })
            preview_id = str(uuid4())
            expires_at = now + ttl_seconds
            await conn.execute(
                """INSERT INTO queue_previews
                   (id, actor_id, actor_user_id, worker_id, job_id, before_job_id, reason, revision,
                    created_at, expires_at, before_order, after_order, impacts)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    preview_id, actor.actor_id, actor.user_id, worker_id, job_id, before_job_id,
                    reason, revision, now, expires_at, json.dumps(before_order),
                    json.dumps(after_order), json.dumps(impacts),
                ),
            )
            # Expired, uncommitted previews carry no authority; prune them.
            await conn.execute(
                "DELETE FROM queue_previews WHERE committed_event_id IS NULL AND expires_at < ?",
                (now - 3600,),
            )
        return {
            "changed": True,
            "preview_id": preview_id,
            "revision": revision,
            "expires_at": expires_at,
            "before_order": before_order,
            "after_order": after_order,
            "impacts": impacts,
        }

    async def commit_queue_move(
        self,
        job_id: str,
        *,
        preview_id: str,
        expected_revision: int,
        actor: AuditActor,
        now: int,
    ) -> dict:
        """Apply exactly one previewed reorder with its audit record."""
        async with self.scheduling_tx() as conn:
            preview = await (await conn.execute(
                "SELECT * FROM queue_previews WHERE id=?", (preview_id,),
            )).fetchone()
            if (
                preview is None
                or preview["job_id"] != job_id
                or preview["actor_id"] != actor.actor_id
                or preview["actor_user_id"] != actor.user_id
            ):
                raise KeyError(preview_id)
            if preview["committed_event_id"] is not None:
                event_id = int(preview["committed_event_id"])
                revision = await self._revision_tx(conn, preview["worker_id"])
            else:
                if now > preview["expires_at"]:
                    raise QueueConflict("PREVIEW_EXPIRED", "the preview expired; preview again")
                worker_id = preview["worker_id"]
                current_revision = await self._revision_tx(conn, worker_id)
                current_order = await self._pending_order_tx(conn, worker_id)
                job_row = await (await conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,))).fetchone()
                if (
                    expected_revision != preview["revision"]
                    or current_revision != preview["revision"]
                    or current_order != json.loads(preview["before_order"])
                    or job_row is None
                    or job_row["status"] != JobStatus.PENDING.value
                    or job_row["worker_id"] != worker_id
                ):
                    raise QueueConflict("QUEUE_CHANGED", "the queue changed; refresh and preview again")
                after_order = json.loads(preview["after_order"])
                await conn.executemany(
                    "UPDATE jobs SET queue_order=? WHERE id=? AND status=?",
                    [
                        (index, pending_id, JobStatus.PENDING.value)
                        for index, pending_id in enumerate(after_order, 1)
                    ],
                )
                revision = await self._bump_revision_tx(conn, worker_id)
                event_id = await self._insert_event_tx(
                    conn, event_type="reorder", worker_id=worker_id,
                    job=self._row_to_job(job_row), actor=actor, reason=preview["reason"],
                    now=now, old_revision=current_revision, new_revision=revision,
                    old_order=current_order, new_order=after_order,
                    impacts=json.loads(preview["impacts"]), preview_id=preview_id,
                )
                await conn.execute(
                    "UPDATE queue_previews SET committed_event_id=? WHERE id=?",
                    (event_id, preview_id),
                )
            events = await self._load_events(conn, "WHERE e.id = ?", [event_id], 1)
        return {"event": events[0], "revision": revision}

    @staticmethod
    async def _load_events(
        conn: aiosqlite.Connection, where: str, params: list, limit: int,
    ) -> list[dict]:
        rows = await conn.execute_fetchall(
            f"SELECT e.* FROM queue_events AS e {where} ORDER BY e.id DESC LIMIT ?",
            [*params, limit],
        )
        if not rows:
            return []
        ids = [row["id"] for row in rows]
        impact_rows = await conn.execute_fetchall(
            f"""SELECT * FROM queue_event_impacts WHERE event_id IN ({', '.join('?' for _ in ids)})
                ORDER BY event_id, after_position""",
            ids,
        )
        impacts: dict[int, list[dict]] = {}
        for impact in impact_rows:
            impacts.setdefault(impact["event_id"], []).append({
                "job_id": impact["job_id"],
                "name": impact["name"],
                "owner_user_id": impact["owner_user_id"],
                "owner_display_name": impact["owner_display_name"],
                "before_position": impact["before_position"],
                "after_position": impact["after_position"],
                "before_start_at": impact["before_start_at"],
                "after_start_at": impact["after_start_at"],
                "delay_seconds": impact["delay_seconds"],
            })
        return [
            {
                "id": row["id"],
                "event_type": row["event_type"],
                "worker_id": row["worker_id"],
                "job_id": row["job_id"],
                "job_name": row["job_name"],
                "job_owner_user_id": row["job_owner_user_id"],
                "job_owner_display_name": row["job_owner_display_name"],
                "actor_user_id": row["actor_user_id"],
                "actor_display_name": row["actor_display_name"],
                "actor_id": row["actor_id"],
                "reason": row["reason"],
                "created_at": row["created_at"],
                "old_revision": row["old_revision"],
                "new_revision": row["new_revision"],
                "old_order": json.loads(row["old_order"]),
                "new_order": json.loads(row["new_order"]),
                "impacts": impacts.get(row["id"], []),
            }
            for row in rows
        ]

    async def list_queue_events(
        self,
        *,
        worker_id: str | None = None,
        job_id: str | None = None,
        before_event_id: int | None = None,
        limit: int = 50,
    ) -> list[dict]:
        clauses: list[str] = []
        params: list = []
        if worker_id:
            clauses.append("e.worker_id = ?")
            params.append(worker_id)
        if job_id:
            clauses.append(
                "(e.job_id = ? OR EXISTS (SELECT 1 FROM queue_event_impacts AS i "
                "WHERE i.event_id = e.id AND i.job_id = ?))"
            )
            params.extend([job_id, job_id])
        if before_event_id is not None:
            clauses.append("e.id < ?")
            params.append(before_event_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        return await self._load_events(self._db, where, params, limit)

    async def delay_history(
        self, job_ids: list[str], *, limit_per_job: int | None = None,
    ) -> dict[str, list[dict]]:
        """Estimated delays that reorders caused each job.

        Covers reorders of other jobs and moves of the job itself by someone
        other than its owner (``moved_itself``); owners moving their own job
        later are not reported back to them.
        """
        if not job_ids:
            return {}
        rows = await self._db.execute_fetchall(
            f"""SELECT i.*, e.job_id AS moved_job_id, e.job_name AS moved_job_name,
                       e.job_owner_display_name AS moved_owner, e.actor_user_id,
                       e.actor_display_name, e.reason, e.created_at
                FROM queue_event_impacts AS i JOIN queue_events AS e ON e.id = i.event_id
                WHERE i.job_id IN ({', '.join('?' for _ in job_ids)})
                  AND e.event_type = 'reorder'
                  AND (e.job_id != i.job_id OR e.actor_user_id != e.job_owner_user_id)
                  AND (i.delay_seconds > 0
                       OR (i.delay_seconds IS NULL AND i.after_position > i.before_position))
                ORDER BY i.event_id DESC""",
            job_ids,
        )
        history: dict[str, list[dict]] = {}
        for row in rows:
            entries = history.setdefault(row["job_id"], [])
            if limit_per_job is not None and len(entries) >= limit_per_job:
                continue
            entries.append({
                "event_id": row["event_id"],
                "created_at": row["created_at"],
                "delay_seconds": row["delay_seconds"],
                "moved_job_id": row["moved_job_id"],
                "moved_itself": row["moved_job_id"] == row["job_id"],
                "moved_job_name": row["moved_job_name"],
                "moved_job_owner_display_name": row["moved_owner"],
                "actor_user_id": row["actor_user_id"],
                "actor_display_name": row["actor_display_name"],
                "reason": row["reason"],
            })
        return history

    # ── Usage ─────────────────────────────────────────────────────────

    async def usage(
        self, *, now: int, worker_id: str | None = None, user_id: str | None = None,
    ) -> list[dict]:
        """Allocated wall/GPU time per owner and worker, derived from jobs.

        Totals are recomputed from durable start/finish times, never
        incremented, so restarts and repeated reconciliation cannot double
        count. Waiting jobs contribute nothing; active ones are provisional.
        """
        query = """SELECT j.owner_user_id, j.worker_id, j.worker_name_at_submission,
                          j.gpu_count, j.started_at, j.finished_at, j.status,
                          j.submitted_by_actor_id, w.name AS current_worker_name,
                          u.display_name AS owner_display_name
                   FROM jobs AS j
                   LEFT JOIN workers AS w ON w.id = j.worker_id
                   LEFT JOIN users AS u ON u.id = j.owner_user_id
                   WHERE j.started_at > 0"""
        params: list = []
        if worker_id:
            query += " AND j.worker_id = ?"
            params.append(worker_id)
        if user_id:
            query += " AND j.owner_user_id = ?"
            params.append(user_id)
        rows = await self._db.execute_fetchall(query, params)
        totals: dict[tuple[str, str], dict] = {}
        for row in rows:
            key = (row["owner_user_id"], row["worker_id"] or "")
            entry = totals.setdefault(key, {
                "owner_user_id": row["owner_user_id"],
                "owner_display_name": row["owner_display_name"] or "",
                "worker_id": row["worker_id"],
                "worker_name": row["current_worker_name"] or row["worker_name_at_submission"] or None,
                "job_seconds": 0,
                "cpu_job_seconds": 0,
                "allocated_gpu_seconds": 0,
                "running_jobs": 0,
                "provisional": False,
                "historical_estimate": False,
            })
            active = row["status"] in (JobStatus.STARTING.value, JobStatus.RUNNING.value)
            end = now if active or not row["finished_at"] else row["finished_at"]
            seconds = max(0, end - row["started_at"])
            entry["job_seconds"] += seconds
            if row["gpu_count"] == 0:
                entry["cpu_job_seconds"] += seconds
            entry["allocated_gpu_seconds"] += seconds * row["gpu_count"]
            if active:
                entry["running_jobs"] += 1
                entry["provisional"] = True
            if str(row["submitted_by_actor_id"]).startswith("migration:"):
                entry["historical_estimate"] = True
        return sorted(
            totals.values(),
            key=lambda item: (item["owner_display_name"], item["worker_name"] or "", item["worker_id"] or ""),
        )

    async def upsert_reported_worker_job(self, worker_id: str, report: WorkerJobReport) -> tuple[Job, bool]:
        """Apply a worker-owned delegated-job report exactly once per revision.

        The conditional UPSERT prevents a retry race from inserting duplicates
        or letting an older report overwrite a newer worker projection. The
        human owner derives from the verified origin session, never the report.
        """
        async with self._worker_job_report_lock:
            session = await self.get_pi_session(report.origin_session_id)
            if not session:
                raise KeyError(report.origin_session_id)
            if session.worker_id != worker_id:
                raise ValueError("origin session not found for worker")
            if not session.owner_user_id:
                raise ValueError("origin session has no verified owner")
            now = int(time.time())
            cursor = await self._db.execute(
                """INSERT INTO jobs
                   (id, worker_id, name, tmux_session, command, status, queue_managed,
                    exit_code, pty_enabled, kind, origin_session_id, report_revision,
                    expected_seconds, gpu_count, gpu_indices, queue_order, started_at, finished_at,
                    owner_user_id, submitted_by_actor_id, submitted_at, submission_sequence,
                    worker_name_at_submission)
                   VALUES (?, ?, 'Setup job', ?, ?, ?, 0, ?, ?, ?, ?, ?, 0, 0, '[]', 0, ?, ?, ?, ?, ?,
                           (SELECT COALESCE(MAX(submission_sequence), 0) + 1 FROM jobs),
                           COALESCE((SELECT name FROM workers WHERE id = ?), ''))
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
                    report.status.value, report.exit_code, int(report.pty_enabled),
                    JobKind.DELEGATED.value, report.origin_session_id,
                    report.report_revision, report.started_at, report.finished_at,
                    session.owner_user_id, f"pi_session:{session.id}", now, worker_id,
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
            name=row["name"],
            tmux_session=row["tmux_session"] or "",
            command=row["command"],
            status=JobStatus(row["status"]),
            queue_managed=bool(row["queue_managed"]),
            exit_code=row["exit_code"],
            pty_enabled=bool(row["pty_enabled"]),
            kind=JobKind(row["kind"]),
            origin_session_id=row["origin_session_id"],
            report_revision=row["report_revision"],
            expected_seconds=row["expected_seconds"],
            gpu_count=row["gpu_count"],
            gpu_indices=json.loads(row["gpu_indices"]),
            queue_order=row["queue_order"],
            started_at=row["started_at"] or 0,
            finished_at=row["finished_at"] or 0,
            owner_user_id=row["owner_user_id"],
            submitted_by_actor_id=row["submitted_by_actor_id"],
            submitted_at=row["submitted_at"],
            submission_sequence=row["submission_sequence"],
            priority=JobPriority(row["priority"]),
            priority_note=row["priority_note"],
            deadline_at=row["deadline_at"],
            termination_reason=(
                TerminationReason(row["termination_reason"]) if row["termination_reason"] else None
            ),
            worker_name_at_submission=row["worker_name_at_submission"],
            execution_state=ExecutionState(row["execution_state"]),
            stop_requested_at=row["stop_requested_at"],
            stop_reason=TerminationReason(row["stop_reason"]) if row["stop_reason"] else None,
            stop_confirmed_at=row["stop_confirmed_at"],
        )

    # ── Port Forwards ─────────────────────────────────────────────────

    async def insert_port_forward(self, pf: PortForward) -> None:
        """Insert (reserve) a forward; the listener (bind_host, local_port) is unique."""
        if not pf.owner_user_id:
            raise ValueError("port forward requires an owner")
        try:
            await self._db.execute(
                """INSERT INTO port_forwards
                   (id, worker_id, local_port, remote_port, service_name, pid, created_at,
                    owner_user_id, created_by_actor_id, bind_host, marimo_session_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    pf.id, pf.worker_id, pf.local_port, pf.remote_port, pf.service_name,
                    pf.pid, pf.created_at, pf.owner_user_id, pf.created_by_actor_id,
                    pf.bind_host, pf.marimo_session_id,
                ),
            )
        except aiosqlite.IntegrityError as exc:
            await self._db.rollback()
            raise ListenerConflict("listener port is already reserved") from exc
        await self._db.commit()

    async def update_port_forward_pid(self, pf_id: str, pid: int) -> None:
        await self._db.execute(
            "UPDATE port_forwards SET pid = ? WHERE id = ?", (pid, pf_id)
        )
        await self._db.commit()

    async def list_port_forwards(
        self, worker_id: str | None = None, *, owner_user_id: str | None = None,
    ) -> list[PortForward]:
        query = "SELECT * FROM port_forwards WHERE 1=1"
        params: list = []
        if worker_id:
            query += " AND worker_id = ?"
            params.append(worker_id)
        if owner_user_id is not None:
            query += " AND owner_user_id = ?"
            params.append(owner_user_id)
        query += " ORDER BY created_at, id"
        rows = await self._db.execute_fetchall(query, params)
        return [self._row_to_pf(r) for r in rows]

    async def get_port_forward(self, pf_id: str) -> PortForward | None:
        rows = await self._db.execute_fetchall("SELECT * FROM port_forwards WHERE id = ?", (pf_id,))
        return self._row_to_pf(rows[0]) if rows else None

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
            owner_user_id=row["owner_user_id"],
            created_by_actor_id=row["created_by_actor_id"],
            bind_host=row["bind_host"],
            marimo_session_id=row["marimo_session_id"],
        )

    # ── Marimo Sessions ────────────────────────────────────────────────

    async def insert_marimo_session(self, session: MarimoSession) -> None:
        if not session.owner_user_id:
            raise ValueError("marimo session requires an owner")
        await self._db.execute(
            """INSERT INTO marimo_sessions
               (id, worker_id, notebook_path, environment, job_id, tunnel_id,
                local_port, remote_port, bind_host, url, status, created_at,
                owner_user_id, created_by_actor_id, upstream_token_path)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                session.id, session.worker_id, session.notebook_path,
                session.environment, session.job_id, session.tunnel_id,
                session.local_port, session.remote_port, session.bind_host,
                session.url, session.status, session.created_at,
                session.owner_user_id, session.created_by_actor_id, session.upstream_token_path,
            ),
        )
        await self._db.commit()

    async def get_marimo_session(self, session_id: str) -> MarimoSession | None:
        row = await self._db.execute_fetchall(
            "SELECT * FROM marimo_sessions WHERE id = ?", (session_id,)
        )
        return self._row_to_marimo(row[0]) if row else None

    async def get_marimo_session_for_job(self, job_id: str) -> MarimoSession | None:
        row = await self._db.execute_fetchall(
            "SELECT * FROM marimo_sessions WHERE job_id = ?", (job_id,)
        )
        return self._row_to_marimo(row[0]) if row else None

    async def list_marimo_sessions(
        self, worker_id: str | None = None, *, owner_user_id: str | None = None,
    ) -> list[MarimoSession]:
        query = "SELECT * FROM marimo_sessions WHERE 1=1"
        params: list = []
        if worker_id:
            query += " AND worker_id = ?"
            params.append(worker_id)
        if owner_user_id is not None:
            query += " AND owner_user_id = ?"
            params.append(owner_user_id)
        query += " ORDER BY created_at DESC"
        rows = await self._db.execute_fetchall(query, params)
        return [self._row_to_marimo(row) for row in rows]

    async def delete_marimo_session(self, session_id: str) -> None:
        await self._db.execute("DELETE FROM marimo_sessions WHERE id = ?", (session_id,))
        await self._db.commit()

    @staticmethod
    def _row_to_marimo(row: aiosqlite.Row) -> MarimoSession:
        return MarimoSession(**dict(row))

    # ── Identity ───────────────────────────────────────────────────────

    _USER_FIELDS = ("id", "username", "display_name", "is_admin", "disabled", "created_at", "updated_at")

    @staticmethod
    def _user(row: aiosqlite.Row | None) -> dict | None:
        if row is None:
            return None
        user = dict(row)
        user["is_admin"] = bool(user["is_admin"])
        user["disabled"] = bool(user["disabled"])
        return user

    async def bootstrap_user_id(self) -> str | None:
        rows = await self._db.execute_fetchall("SELECT bootstrap_user_id FROM identity_settings")
        return rows[0]["bootstrap_user_id"] if rows else None

    async def bootstrap_admin(
        self, *, username: str, display_name: str, password_hash: str, now: int,
    ) -> dict:
        """Atomically create the first administrator and assign legacy owners."""
        user_id = str(uuid4())
        actor = f"migration:{user_id}"
        async with self.scheduling_tx() as conn:
            existing = await (await conn.execute("SELECT 1 FROM identity_settings")).fetchone()
            if existing:
                raise IdentityConflict("bootstrap administrator already exists")
            try:
                await conn.execute(
                    """INSERT INTO users (id, username, display_name, password_hash, is_admin,
                       disabled, created_at, updated_at) VALUES (?, ?, ?, ?, 1, 0, ?, ?)""",
                    (user_id, username, display_name, password_hash, now, now),
                )
            except aiosqlite.IntegrityError as exc:
                raise IdentityConflict("username already exists") from exc
            await conn.execute(
                "INSERT INTO identity_settings (singleton, bootstrap_user_id, created_at) VALUES (1, ?, ?)",
                (user_id, now),
            )
            await conn.execute(
                """UPDATE jobs SET owner_user_id=?, submitted_by_actor_id=CASE
                     WHEN submitted_by_actor_id='' THEN ? ELSE submitted_by_actor_id END
                   WHERE owner_user_id=''""",
                (user_id, actor),
            )
            await conn.execute("UPDATE pi_sessions SET owner_user_id=? WHERE owner_user_id=''", (user_id,))
            for table in ("port_forwards", "marimo_sessions"):
                await conn.execute(
                    f"""UPDATE {table} SET owner_user_id=?, created_by_actor_id=CASE
                          WHEN created_by_actor_id='' THEN ? ELSE created_by_actor_id END
                        WHERE owner_user_id=''""",
                    (user_id, actor),
                )
            row = await (await conn.execute("SELECT * FROM users WHERE id=?", (user_id,))).fetchone()
        return self._safe_user(self._user(row))

    @classmethod
    def _safe_user(cls, user: dict | None) -> dict | None:
        if user is None:
            return None
        return {field: user[field] for field in cls._USER_FIELDS}

    async def get_user(self, user_id: str) -> dict | None:
        rows = await self._db.execute_fetchall("SELECT * FROM users WHERE id=?", (user_id,))
        return self._user(rows[0] if rows else None)

    async def get_user_by_username(self, username: str) -> dict | None:
        rows = await self._db.execute_fetchall("SELECT * FROM users WHERE username=?", (username,))
        return self._user(rows[0] if rows else None)

    async def list_users(self) -> list[dict]:
        rows = await self._db.execute_fetchall("SELECT * FROM users ORDER BY username")
        return [self._safe_user(self._user(row)) for row in rows]

    async def create_user(
        self, *, username: str, display_name: str, password_hash: str, is_admin: bool, now: int,
    ) -> dict:
        user_id = str(uuid4())
        try:
            await self._db.execute(
                """INSERT INTO users (id, username, display_name, password_hash, is_admin,
                   disabled, created_at, updated_at) VALUES (?, ?, ?, ?, ?, 0, ?, ?)""",
                (user_id, username, display_name, password_hash, int(is_admin), now, now),
            )
        except aiosqlite.IntegrityError as exc:
            await self._db.rollback()
            raise IdentityConflict("username already exists") from exc
        await self._db.commit()
        return self._safe_user(await self.get_user(user_id))

    async def update_user(
        self,
        user_id: str,
        *,
        display_name: str | None = None,
        disabled: bool | None = None,
        is_admin: bool | None = None,
        now: int,
    ) -> tuple[dict, bool]:
        """Update a user; returns (user, newly_disabled).

        Disabling revokes every key and browser session in the same
        transaction. The bootstrap user cannot be disabled or demoted and the
        last enabled administrator cannot be removed.
        """
        async with self.scheduling_tx() as conn:
            row = await (await conn.execute("SELECT * FROM users WHERE id=?", (user_id,))).fetchone()
            if row is None:
                raise KeyError(user_id)
            user = self._user(row)
            settings = await (await conn.execute("SELECT bootstrap_user_id FROM identity_settings")).fetchone()
            is_bootstrap = settings is not None and settings[0] == user_id
            new_disabled = user["disabled"] if disabled is None else disabled
            new_admin = user["is_admin"] if is_admin is None else is_admin
            if is_bootstrap and (new_disabled or not new_admin):
                raise IdentityConflict("the bootstrap administrator cannot be disabled or demoted")
            if user["is_admin"] and not user["disabled"] and (new_disabled or not new_admin):
                remaining = await (await conn.execute(
                    "SELECT COUNT(*) FROM users WHERE is_admin=1 AND disabled=0 AND id!=?", (user_id,),
                )).fetchone()
                if remaining[0] == 0:
                    raise IdentityConflict("cannot remove the last enabled administrator")
            await conn.execute(
                "UPDATE users SET display_name=?, disabled=?, is_admin=?, updated_at=? WHERE id=?",
                (
                    display_name if display_name is not None else user["display_name"],
                    int(new_disabled), int(new_admin), now, user_id,
                ),
            )
            newly_disabled = new_disabled and not user["disabled"]
            if newly_disabled:
                await conn.execute(
                    "UPDATE api_keys SET revoked_at=? WHERE user_id=? AND revoked_at IS NULL", (now, user_id),
                )
                await conn.execute("DELETE FROM web_sessions WHERE user_id=?", (user_id,))
            updated = await (await conn.execute("SELECT * FROM users WHERE id=?", (user_id,))).fetchone()
        return self._safe_user(self._user(updated)), newly_disabled

    async def set_password_hash(self, user_id: str, password_hash: str, *, now: int) -> None:
        """Replace a password hash and revoke every browser session of the user."""
        async with self.scheduling_tx() as conn:
            cursor = await conn.execute(
                "UPDATE users SET password_hash=?, updated_at=? WHERE id=?", (password_hash, now, user_id),
            )
            if not cursor.rowcount:
                raise KeyError(user_id)
            await conn.execute("DELETE FROM web_sessions WHERE user_id=?", (user_id,))

    async def rehash_password(self, user_id: str, password_hash: str, *, expected_hash: str) -> bool:
        """Compare-and-swap rehash; a concurrent reset/change always wins."""
        cursor = await self._db.execute(
            "UPDATE users SET password_hash=? WHERE id=? AND password_hash=?",
            (password_hash, user_id, expected_hash),
        )
        await self._db.commit()
        return bool(cursor.rowcount)

    _KEY_FIELDS = ("id", "user_id", "label", "prefix", "created_at", "last_used_at", "revoked_at")

    async def create_api_key(
        self, *, user_id: str, label: str, token_hash: str, prefix: str, now: int,
    ) -> dict:
        key_id = str(uuid4())
        async with self.scheduling_tx() as conn:
            user = await (await conn.execute("SELECT disabled FROM users WHERE id=?", (user_id,))).fetchone()
            if user is None:
                raise KeyError(user_id)
            if user["disabled"]:
                raise IdentityConflict("disabled users cannot receive API keys")
            await conn.execute(
                """INSERT INTO api_keys (id, user_id, label, token_hash, prefix, created_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (key_id, user_id, label, token_hash, prefix, now),
            )
            row = await (await conn.execute("SELECT * FROM api_keys WHERE id=?", (key_id,))).fetchone()
        return {field: row[field] for field in self._KEY_FIELDS}

    async def list_api_keys(self, user_id: str) -> list[dict]:
        rows = await self._db.execute_fetchall(
            "SELECT * FROM api_keys WHERE user_id=? ORDER BY created_at, id", (user_id,),
        )
        return [{field: row[field] for field in self._KEY_FIELDS} for row in rows]

    async def revoke_api_key(self, user_id: str, key_id: str, *, now: int) -> bool:
        rows = await self._db.execute_fetchall(
            "SELECT revoked_at FROM api_keys WHERE id=? AND user_id=?", (key_id, user_id),
        )
        if not rows:
            return False
        if rows[0]["revoked_at"] is None:
            await self._db.execute("UPDATE api_keys SET revoked_at=? WHERE id=?", (now, key_id))
            await self._db.commit()
        return True

    async def resolve_api_key(self, token_hash: str, *, now: int) -> tuple[dict, dict] | None:
        """Return (key, user) for an active key of an enabled user."""
        rows = await self._db.execute_fetchall(
            """SELECT k.id AS key_id, k.last_used_at, u.* FROM api_keys AS k
               JOIN users AS u ON u.id = k.user_id
               WHERE k.token_hash=? AND k.revoked_at IS NULL AND u.disabled=0""",
            (token_hash,),
        )
        if not rows:
            return None
        row = rows[0]
        if row["last_used_at"] is None or now - row["last_used_at"] >= 60:
            await self._db.execute("UPDATE api_keys SET last_used_at=? WHERE id=?", (now, row["key_id"]))
            await self._db.commit()
        user = self._user(row)
        user.pop("key_id", None)
        user.pop("last_used_at", None)
        return {"id": row["key_id"]}, user

    async def create_web_session(
        self, *, user_id: str, token_hash: str, csrf_token: str, now: int, expires_at: int,
        password_hash: str,
    ) -> str | None:
        """Issue a session only while ``password_hash`` is current and the user enabled.

        The single conditional INSERT makes a login that verified a password
        lose against a reset/disable committed before it; resets committed
        afterwards delete the new session along with the others.
        """
        session_id = str(uuid4())
        cursor = await self._db.execute(
            """INSERT INTO web_sessions (id, token_hash, user_id, csrf_token, created_at, expires_at)
               SELECT ?, ?, ?, ?, ?, ? WHERE EXISTS (
                 SELECT 1 FROM users WHERE id=? AND password_hash=? AND disabled=0
               )""",
            (session_id, token_hash, user_id, csrf_token, now, expires_at, user_id, password_hash),
        )
        await self._db.execute("DELETE FROM web_sessions WHERE expires_at <= ?", (now,))
        await self._db.commit()
        return session_id if cursor.rowcount else None

    async def resolve_web_session(self, token_hash: str, *, now: int) -> tuple[dict, dict] | None:
        rows = await self._db.execute_fetchall(
            """SELECT s.id AS session_id, s.csrf_token, s.expires_at, u.* FROM web_sessions AS s
               JOIN users AS u ON u.id = s.user_id
               WHERE s.token_hash=? AND s.expires_at > ? AND u.disabled=0""",
            (token_hash, now),
        )
        if not rows:
            return None
        row = rows[0]
        user = self._user(row)
        session = {
            "id": user.pop("session_id"),
            "csrf_token": user.pop("csrf_token"),
            "expires_at": user.pop("expires_at"),
        }
        return session, user

    async def delete_web_session(self, session_id: str) -> None:
        await self._db.execute("DELETE FROM web_sessions WHERE id=?", (session_id,))
        await self._db.commit()

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
        """Delete a worker registration and its live service records.

        Jobs are retained: usage accounting and history keep the worker-name
        snapshot and left-join absent workers.
        """
        await self._db.execute(
            "DELETE FROM port_forwards WHERE worker_id = ?", (worker_id,)
        )
        await self._db.execute(
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
