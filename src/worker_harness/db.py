"""Async SQLite repository layer."""

from __future__ import annotations

import asyncio
import aiosqlite
import json
import time
import hashlib
import hmac
from datetime import datetime, timezone
from pathlib import Path
from contextlib import asynccontextmanager
from functools import wraps
from uuid import uuid4

from .models import (
    Failure,
    Job,
    JobKind,
    JobStatus,
    JobEvent,
    JobResultReport,
    RunnerResult,
    RunnerStartObservation,
    SlurmObservation,
    SlurmReservationRequest,
    SlurmWorkerInfo,
    PiBridgeEventBatch,
    PiBridgeRegister,
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

class SlurmError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


_TERMINAL_SLURM_STATES = {
    "BOOT_FAIL", "CANCELLED", "COMPLETED", "DEADLINE", "FAILED", "NODE_FAIL",
    "OUT_OF_MEMORY", "PREEMPTED", "REVOKED", "TIMEOUT",
}

def _write_locked(method):
    """Prevent an unrelated writer from committing another method's transaction."""
    @wraps(method)
    async def locked(self, *args, **kwargs):
        async with self._queue_lock:
            try:
                return await method(self, *args, **kwargs)
            except BaseException:
                await self._db.rollback()
                raise
    return locked




class Database:
    """Async SQLite database with all repository methods."""

    def __init__(self, path: str | Path = "~/.config/worker-harness/db.sqlite") -> None:
        self.path = Path(path).expanduser()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._db: aiosqlite.Connection | None = None
        # All writers share the connection, so even unrelated table writes must
        # not interleave commits with an allocation/job transaction.
        self._queue_lock = asyncio.Lock()
        self._worker_job_report_lock = self._queue_lock
        self._pi_bridge_lock = self._queue_lock

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

    @asynccontextmanager
    async def _queue_transaction(self):
        async with self._queue_lock:
            await self._db.execute("BEGIN IMMEDIATE")
            try:
                yield
                await self._db.commit()
            except BaseException:
                await self._db.rollback()
                raise

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
        await self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_jobs_origin_session_id ON jobs(origin_session_id)"
        )
        await self._db.execute(
            """CREATE INDEX IF NOT EXISTS idx_jobs_queue_active
               ON jobs(queue_managed, worker_id, status, queue_order)"""
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
        if "gpu_device_ids" not in colnames:
            await self._db.execute("ALTER TABLE workers ADD COLUMN gpu_device_ids TEXT NOT NULL DEFAULT '[]'")
        for column, declaration in {
            "notify_recipient_id": "TEXT",
            "submission_key": "TEXT",
            "submission_digest": "TEXT",
            "runner_result": "TEXT",
        }.items():
            if column not in job_cols:
                await self._db.execute(f"ALTER TABLE jobs ADD COLUMN {column} {declaration}")
        await self._db.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_submission_key
            ON jobs(submission_key) WHERE submission_key IS NOT NULL
        """)
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS slurm_workers (
                worker_id TEXT PRIMARY KEY REFERENCES workers(id),
                request_id TEXT NOT NULL UNIQUE,
                request_digest TEXT NOT NULL,
                policy TEXT NOT NULL,
                recipient_id TEXT NOT NULL,
                registration_token_hash TEXT NOT NULL,
                token_revoked INTEGER NOT NULL DEFAULT 0,
                initial_job_id TEXT,
                slurm_job_id TEXT,
                scheduler_state TEXT,
                scheduler_reason TEXT,
                observed_at REAL,
                submission_attempted INTEGER NOT NULL DEFAULT 0,
                submission_failed INTEGER NOT NULL DEFAULT 0,
                cancel_requested INTEGER NOT NULL DEFAULT 0,
                submitted_at REAL,
                started_at REAL,
                finished_at REAL,
                failure_deadline REAL,
                startup_error TEXT,
                checkpoint_requested INTEGER NOT NULL DEFAULT 0
            )
        """)
        slurm_cols = {row["name"] for row in await self._db.execute_fetchall("PRAGMA table_info(slurm_workers)")}
        for column, declaration in {
            "last_useful_started_at": "REAL",
            "last_useful_started_monotonic": "REAL",
            "last_useful_boot_id": "TEXT",
            "failure_finished_monotonic": "REAL",
            "failure_boot_id": "TEXT",
        }.items():
            if column not in slurm_cols:
                await self._db.execute(f"ALTER TABLE slurm_workers ADD COLUMN {column} {declaration}")
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS job_events (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                recipient_id TEXT NOT NULL,
                worker_id TEXT NOT NULL,
                job_id TEXT,
                event_type TEXT NOT NULL,
                details TEXT NOT NULL DEFAULT '{}',
                created_at REAL NOT NULL
            )
        """)
        await self._db.execute("""
            CREATE INDEX IF NOT EXISTS idx_job_events_recipient
            ON job_events(recipient_id, sequence)
        """)
        # Triggers keep every ordinary status writer (including report UPSERTs)
        # in the same SQLite statement as its durable notification.
        await self._db.execute("""
            CREATE TRIGGER IF NOT EXISTS job_status_event
            AFTER UPDATE OF status ON jobs
            WHEN OLD.status != NEW.status
              AND OLD.status NOT IN ('done', 'failed')
              AND NEW.status IN ('running', 'done', 'failed')
            BEGIN
                INSERT INTO job_events
                  (recipient_id, worker_id, job_id, event_type, details, created_at)
                SELECT NEW.notify_recipient_id, NEW.worker_id, NEW.id,
                  CASE NEW.status WHEN 'running' THEN 'job_started'
                    WHEN 'failed' THEN 'job_failed' ELSE 'job_finished' END,
                  json_object('exit_code', NEW.exit_code, 'finished_at', NEW.finished_at,
                    'failure_deadline', (SELECT failure_deadline FROM slurm_workers
                      WHERE worker_id=NEW.worker_id),
                    'log_context', 'Use ordinary job logs or retained shared allocation logs'),
                  unixepoch()
                WHERE NEW.notify_recipient_id IS NOT NULL;
            END
        """)
        await self._db.execute("""
            CREATE TRIGGER IF NOT EXISTS initial_slurm_job
            AFTER INSERT ON jobs WHEN NEW.queue_managed=1 AND NEW.submission_key IS NOT NULL
            BEGIN
                UPDATE slurm_workers SET initial_job_id=NEW.id
                WHERE worker_id=NEW.worker_id AND request_id=NEW.submission_key
                  AND initial_job_id IS NULL;
            END
        """)
        await self._db.execute("DROP TRIGGER IF EXISTS slurm_job_failure_deadline")
        await self._db.execute("""
            CREATE TRIGGER IF NOT EXISTS slurm_job_failure_deadline
            BEFORE UPDATE OF status ON jobs
            WHEN OLD.status NOT IN ('done', 'failed') AND NEW.status='failed'
              AND NEW.queue_managed=1
              AND (OLD.status IN ('starting','running') OR NEW.runner_result IS NOT NULL)
            BEGIN
                UPDATE slurm_workers SET
                  failure_boot_id=json_extract(NEW.runner_result, '$.boot_id'),
                  failure_finished_monotonic=json_extract(NEW.runner_result, '$.finished_monotonic'),
                  failure_deadline=NEW.finished_at + json_extract(policy, '$.failure_grace_seconds')
                WHERE worker_id=NEW.worker_id
                  AND (failure_deadline IS NULL OR CASE
                    WHEN failure_boot_id=json_extract(NEW.runner_result, '$.boot_id')
                        AND failure_finished_monotonic IS NOT NULL
                        AND json_extract(NEW.runner_result, '$.finished_monotonic') IS NOT NULL
                      THEN json_extract(NEW.runner_result, '$.finished_monotonic') < failure_finished_monotonic
                      ELSE NEW.finished_at < failure_deadline - json_extract(policy, '$.failure_grace_seconds')
                    END)
                  AND NOT (
                  CASE WHEN last_useful_boot_id=json_extract(NEW.runner_result, '$.boot_id')
                      AND last_useful_started_monotonic IS NOT NULL
                      AND json_extract(NEW.runner_result, '$.finished_monotonic') IS NOT NULL
                    THEN last_useful_started_monotonic > json_extract(NEW.runner_result, '$.finished_monotonic')
                    ELSE COALESCE(last_useful_started_at, 0) > NEW.finished_at
                  END
                );
            END
        """)
        await self._db.execute("DROP TRIGGER IF EXISTS slurm_useful_job_started")
        await self._db.execute("""
            CREATE TRIGGER IF NOT EXISTS slurm_useful_job_started
            BEFORE UPDATE OF status ON jobs
            WHEN OLD.status != 'running' AND NEW.status='running' AND NEW.queue_managed=1
            BEGIN
                UPDATE slurm_workers SET failure_deadline=NULL, failure_boot_id=NULL,
                  failure_finished_monotonic=NULL WHERE worker_id=NEW.worker_id;
            END
        """)
        await self._db.commit()

    # ── Workers ──────────────────────────────────────────────────────

    @_write_locked
    async def upsert_worker(self, reg: WorkerRegistration) -> Worker:
        existing = await self.get_worker(reg.worker_id)
        if existing and existing.slurm:
            raise PermissionError("reserved workers require scoped authenticated registration")
        if reg.worker_id.startswith("cluster:"):
            raise ValueError("cluster: is reserved for client-only targets")
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
        return await self._project_worker(row)

    async def list_workers(self) -> list[Worker]:
        rows = await self._db.execute_fetchall("SELECT * FROM workers ORDER BY name")
        return [await self._project_worker(r) for r in rows]

    @_write_locked
    async def mark_workers_offline(self, cutoff_ts: int) -> int:
        cursor = await self._db.execute(
            "UPDATE workers SET status = ? WHERE last_heartbeat_ts < ? AND status = ?",
            (WorkerStatus.OFFLINE.value, cutoff_ts, WorkerStatus.ONLINE.value),
        )
        await self._db.commit()
        return cursor.rowcount

    @_write_locked
    async def set_worker_status(self, worker_id: str, status: WorkerStatus) -> None:
        await self._db.execute(
            "UPDATE workers SET status = ? WHERE id = ?",
            (status.value, worker_id),
        )
        await self._db.commit()

    async def _insert_worker(self, w: Worker, *, commit: bool = True) -> None:
        await self._db.execute(
            """INSERT INTO workers
               (id, name, worker_ip, dns_name, ssh_user, harness_dir, gpu_count, gpu_names, gpu_vram_gb,
                gpu_used_vram_gb, gpu_busy, cpu_cores, total_ram_gb, used_ram_gb, total_disk_gb,
                used_disk_gb, data_paths, status, last_heartbeat_ts, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                w.id, w.name, w.worker_ip, w.dns_name, w.ssh_user, w.harness_dir, w.gpu_count,
                json.dumps(w.gpu_names), json.dumps(w.gpu_vram_gb),
                json.dumps(w.gpu_used_vram_gb),
                json.dumps(w.gpu_busy),
                w.cpu_cores, w.total_ram_gb, w.used_ram_gb,
                w.total_disk_gb, w.used_disk_gb, json.dumps(w.data_paths),
                w.status.value, w.last_heartbeat_ts, w.created_at,
            ),
        )
        await self._db.execute("UPDATE workers SET gpu_device_ids=? WHERE id=?",
                               (json.dumps(w.gpu_device_ids), w.id))
        if commit:
            await self._db.commit()

    async def _update_worker(self, w: Worker, *, commit: bool = True) -> None:
        await self._db.execute(
            """UPDATE workers SET
               name=?, worker_ip=?, dns_name=?, ssh_user=?, harness_dir=?, gpu_count=?, gpu_names=?,
               gpu_vram_gb=?, gpu_used_vram_gb=?, gpu_busy=?, cpu_cores=?, total_ram_gb=?,
               used_ram_gb=?, total_disk_gb=?, used_disk_gb=?, data_paths=?,
               status=?, last_heartbeat_ts=? WHERE id=?""",
            (
                w.name, w.worker_ip, w.dns_name, w.ssh_user, w.harness_dir, w.gpu_count,
                json.dumps(w.gpu_names), json.dumps(w.gpu_vram_gb),
                json.dumps(w.gpu_used_vram_gb),
                json.dumps(w.gpu_busy),
                w.cpu_cores, w.total_ram_gb, w.used_ram_gb,
                w.total_disk_gb, w.used_disk_gb, json.dumps(w.data_paths),
                w.status.value, w.last_heartbeat_ts, w.id,
            ),
        )
        await self._db.execute("UPDATE workers SET gpu_device_ids=? WHERE id=?",
                               (json.dumps(w.gpu_device_ids), w.id))
        if commit:
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
            gpu_device_ids=json.loads(row["gpu_device_ids"] or "[]"),
            cpu_cores=row["cpu_cores"],
            total_ram_gb=row["total_ram_gb"],
            used_ram_gb=row["used_ram_gb"],
            total_disk_gb=row["total_disk_gb"],
            used_disk_gb=row["used_disk_gb"],
            data_paths=json.loads(row["data_paths"] or "[]"),
            status=WorkerStatus(row["status"]),
            last_heartbeat_ts=row["last_heartbeat_ts"],
            created_at=row["created_at"],
        )

    async def _project_worker(self, row: aiosqlite.Row) -> Worker:
        worker = self._row_to_worker(row)
        metadata = await (await self._db.execute(
            "SELECT * FROM slurm_workers WHERE worker_id=?", (worker.id,),
        )).fetchone()
        if metadata:
            worker.slurm = SlurmWorkerInfo(
                **json.loads(metadata["policy"]),
                **{key: metadata[key] for key in (
                    "request_id", "recipient_id", "initial_job_id", "slurm_job_id",
                    "scheduler_state", "scheduler_reason", "observed_at",
                    "submission_attempted", "submission_failed", "cancel_requested",
                    "submitted_at", "started_at", "finished_at", "failure_deadline",
                    "last_useful_started_at", "last_useful_started_monotonic", "last_useful_boot_id",
                )},
            )
        return worker

    @staticmethod
    def _digest(payload: dict) -> str:
        return hashlib.sha256(json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
        ).encode()).hexdigest()

    async def reserve_slurm_worker(self, request: SlurmReservationRequest) -> Worker:
        request = SlurmReservationRequest.model_validate(request)
        digest = self._digest(request.model_dump())
        async with self._queue_lock:
            await self._db.execute("BEGIN IMMEDIATE")
            try:
                previous = await (await self._db.execute(
                    "SELECT worker_id, request_digest FROM slurm_workers WHERE request_id=?",
                    (request.request_id,),
                )).fetchone()
                if previous:
                    if previous["request_digest"] != digest:
                        raise SlurmError("IDEMPOTENCY_CONFLICT", "reservation request_id reused with a different body")
                    worker = await self.get_worker(previous["worker_id"])
                else:
                    worker = Worker(
                        id=str(uuid4()), name=request.name, worker_ip="",
                        gpu_count=request.gpu_count, cpu_cores=request.slurm.cpus,
                        total_ram_gb=request.slurm.memory_mb / 1024,
                        created_at=int(time.time()),
                    )
                    await self._insert_worker(worker, commit=False)
                    await self._db.execute(
                        """INSERT INTO slurm_workers
                           (worker_id, request_id, request_digest, policy, recipient_id, registration_token_hash)
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        (worker.id, request.request_id, digest, request.slurm.model_dump_json(),
                         request.recipient_id, request.registration_token_hash),
                    )
                    worker = await self.get_worker(worker.id)
                await self._db.commit()
                assert worker is not None
                return worker
            except BaseException:
                await self._db.rollback()
                raise

    async def authenticate_slurm_worker(self, worker_id: str, token: str) -> bool:
        row = await (await self._db.execute(
            "SELECT registration_token_hash, token_revoked FROM slurm_workers WHERE worker_id=?",
            (worker_id,),
        )).fetchone()
        return bool(row and not row["token_revoked"] and hmac.compare_digest(
            hashlib.sha256(token.encode()).hexdigest(), row["registration_token_hash"],
        ))

    async def revoke_slurm_worker_token(self, worker_id: str) -> None:
        async with self._queue_lock:
            await self._db.execute("UPDATE slurm_workers SET token_revoked=1 WHERE worker_id=?", (worker_id,))
            await self._db.commit()

    async def assert_worker_accepts_jobs(self, worker_id: str) -> Worker:
        worker = await self.get_worker(worker_id)
        if worker is None:
            raise KeyError(worker_id)
        if worker.status == WorkerStatus.DRAINING or (
            worker.slurm and (worker.slurm.finished_at is not None
                              or worker.slurm.submission_failed or worker.slurm.cancel_requested)
        ):
            raise SlurmError(
                "SLURM_WORKER_FINISHED",
                "Worker is draining or its allocation has finished; submit a fresh allocation",
            )
        return worker

    @staticmethod
    def _validate_queue_capacity(worker: Worker, gpu_count: int) -> None:
        cpu = worker.slurm is not None and worker.slurm.compute_kind == "cpu"
        if gpu_count < 0 or (cpu and gpu_count != 0) or (not cpu and gpu_count == 0):
            raise ValueError("gpu_count must be zero only for a CPU Slurm worker, and positive otherwise")
        if gpu_count > worker.gpu_count:
            raise ValueError("gpu_count exceeds worker capacity")

    async def _worker_event(self, worker: Worker, event_type: str, details: dict | None = None) -> None:
        assert worker.slurm is not None
        encoded = json.dumps(details or {}, ensure_ascii=True)
        if len(encoded.encode()) > 8192:
            raise ValueError("job event details exceed 8192 bytes")
        await self._db.execute(
            """INSERT INTO job_events
               (recipient_id, worker_id, job_id, event_type, details, created_at)
               SELECT ?, ?, NULL, ?, ?, ?
               WHERE NOT EXISTS (SELECT 1 FROM job_events
                 WHERE worker_id=? AND job_id IS NULL AND event_type=?)""",
            (worker.slurm.recipient_id, worker.id, event_type, encoded, time.time(),
             worker.id, event_type),
        )

    async def list_job_events(self, recipient_id: str, after: int = 0, limit: int = 100) -> list[JobEvent]:
        rows = await self._db.execute_fetchall(
            "SELECT * FROM job_events WHERE recipient_id=? AND sequence>? ORDER BY sequence LIMIT ?",
            (recipient_id, max(0, after), max(1, min(limit, 1000))),
        )
        return [JobEvent(**{**dict(row), "details": json.loads(row["details"])}) for row in rows]

    async def _record_useful_start(
        self, worker: Worker, job: Job, started_at: float,
        started_monotonic: float, boot_id: str,
    ) -> None:
        # Admission and usefulness come from the assigned job, never a report's
        # queue_managed flag. Ordinary workers retain their existing semantics.
        if not worker.slurm or not job.queue_managed:
            return
        metadata = await (await self._db.execute(
            "SELECT * FROM slurm_workers WHERE worker_id=?", (worker.id,),
        )).fetchone()
        same_boot = (metadata["last_useful_boot_id"] == boot_id
                     and metadata["last_useful_started_monotonic"] is not None)
        newer = (started_monotonic > metadata["last_useful_started_monotonic"] if same_boot
                 else metadata["last_useful_started_at"] is None
                 or started_at > metadata["last_useful_started_at"])
        if newer:
            await self._db.execute(
                """UPDATE slurm_workers SET last_useful_started_at=?,
                   last_useful_started_monotonic=?, last_useful_boot_id=? WHERE worker_id=?""",
                (started_at, started_monotonic, boot_id, worker.id),
            )
        if metadata["failure_deadline"] is not None:
            same_failure_boot = (metadata["failure_boot_id"] == boot_id
                                 and metadata["failure_finished_monotonic"] is not None)
            after_failure = (started_monotonic > metadata["failure_finished_monotonic"]
                             if same_failure_boot else
                             started_at > metadata["failure_deadline"] - worker.slurm.failure_grace_seconds)
            if after_failure:
                await self._db.execute(
                    """UPDATE slurm_workers SET failure_deadline=NULL,
                       failure_boot_id=NULL, failure_finished_monotonic=NULL WHERE worker_id=?""",
                    (worker.id,),
                )

    async def _apply_active_runner_starts(self, worker: Worker, active_jobs: list[dict]) -> None:
        observations = []
        for value in active_jobs:
            if not any(key in value for key in ("started_at", "started_monotonic", "boot_id")):
                continue
            observation = RunnerStartObservation.model_validate(value)
            job = await self.get_job(observation.job_id)
            if job is None or job.worker_id != worker.id:
                raise ValueError("runner start does not match a job assigned to this worker")
            observations.append((job, observation))
        for job, observation in observations:
            await self._record_useful_start(
                worker, job, observation.started_at, observation.started_monotonic, observation.boot_id,
            )

    async def _apply_runner_results(self, worker: Worker, reports: list[JobResultReport]) -> None:
        # Validate the complete batch first so a bad linkage never partly commits.
        jobs = []
        for report in reports:
            job = await self.get_job(report.job_id)
            if job is None or job.worker_id != worker.id:
                raise ValueError("runner result does not match a job assigned to this worker")
            jobs.append((job, report.result))
        # Observe every actual start before any finish. A newer useful job may
        # already have exited before an older failure is replayed.
        for job, result in jobs:
            evidence = job.runner_result if job.status in (JobStatus.DONE, JobStatus.FAILED) else result
            if evidence and evidence.started_at is not None:
                await self._record_useful_start(
                    worker, job, evidence.started_at, evidence.started_monotonic, evidence.boot_id,
                )
        for job, result in jobs:
            if job.status in (JobStatus.DONE, JobStatus.FAILED):
                continue
            await self._db.execute(
                """UPDATE jobs SET status=?, exit_code=?, finished_at=?, runner_result=?
                   WHERE id=? AND status NOT IN ('done','failed')""",
                (JobStatus.DONE.value if result.exit_code == 0 else JobStatus.FAILED.value,
                 result.exit_code, result.finished_at, result.model_dump_json(), job.id),
            )

    async def apply_job_runner_result(
        self, worker_id: str, job_id: str, result: RunnerResult,
    ) -> Job:
        """Persist verified SSH runner evidence through the heartbeat result path."""
        report = JobResultReport(job_id=job_id, result=RunnerResult.model_validate(result))
        async with self._queue_transaction():
            worker = await self.get_worker(worker_id)
            if worker is None:
                raise KeyError(worker_id)
            await self._apply_runner_results(worker, [report])
            job = await self.get_job(job_id)
            assert job is not None
            return job

    async def register_slurm_worker(self, reg: WorkerRegistration, ready: bool) -> Worker:
        async with self._queue_lock:
            await self._db.execute("BEGIN IMMEDIATE")
            try:
                worker = await self.get_worker(reg.worker_id)
                if not worker or not worker.slurm:
                    raise KeyError(reg.worker_id)
                info = worker.slurm
                if info.finished_at is not None or info.submission_failed:
                    raise SlurmError("SLURM_WORKER_FINISHED", "allocation finished; submit a fresh allocation")
                if (reg.name != worker.name or reg.gpu_count != worker.gpu_count
                        or reg.cpu_cores != info.cpus
                        or abs(reg.total_ram_gb * 1024 - info.memory_mb) > 1):
                    raise SlurmError("SLURM_IDENTITY_CONFLICT", "registration differs from reserved identity/capacity")
                startup_failed = bool(reg.slurm and reg.slurm.startup_error)
                if not startup_failed:
                    ids = [gpu.uuid for gpu in reg.gpus]
                    if (len(reg.gpus) != worker.gpu_count
                            or [gpu.index for gpu in reg.gpus] != list(range(worker.gpu_count))
                            or any(not value or not value.startswith("GPU-") for value in ids)
                            or len(set(ids)) != len(ids)
                            or (worker.gpu_device_ids and ids != worker.gpu_device_ids)):
                        raise SlurmError("SLURM_IDENTITY_CONFLICT", "registration requires stable ordered physical GPU UUIDs")
                previous_status = worker.status
                if startup_failed:
                    # GPU mapping can itself be the startup failure. Record
                    # the authenticated error without treating incomplete
                    # telemetry as an allocation identity or ready mapping.
                    worker.last_heartbeat_ts = int(time.time())
                else:
                    worker.update_from_registration(reg)
                worker.status = (WorkerStatus.DRAINING if previous_status == WorkerStatus.DRAINING
                                 or info.cancel_requested else
                                 WorkerStatus.ONLINE if ready or previous_status == WorkerStatus.ONLINE
                                 else WorkerStatus.OFFLINE)
                if reg.slurm:
                    await self._apply_active_runner_starts(worker, reg.active_jobs)
                    await self._apply_runner_results(worker, reg.slurm.results)
                    if reg.slurm.startup_error:
                        cursor = await self._db.execute(
                            "UPDATE slurm_workers SET startup_error=? WHERE worker_id=? AND startup_error IS NULL",
                            (reg.slurm.startup_error, worker.id),
                        )
                        if cursor.rowcount:
                            await self._worker_event(worker, "worker_start_failed", {"error": reg.slurm.startup_error})
                        if worker.status != WorkerStatus.DRAINING:
                            worker.status = WorkerStatus.OFFLINE
                    if reg.slurm.checkpoint_requested:
                        cursor = await self._db.execute(
                            "UPDATE slurm_workers SET checkpoint_requested=1 WHERE worker_id=? AND checkpoint_requested=0",
                            (worker.id,),
                        )
                        worker.status = WorkerStatus.DRAINING
                        if cursor.rowcount:
                            await self._worker_event(worker, "checkpoint_requested")
                    if reg.slurm.draining:
                        # Useful-work emptiness and admission close share this writer transaction.
                        useful = await self._db.execute_fetchall(
                            """SELECT 1 FROM jobs WHERE worker_id=? AND queue_managed=1
                               AND status IN ('pending','starting','running')
                               UNION ALL SELECT 1 FROM slurm_workers WHERE worker_id=?
                                 AND failure_deadline > ? LIMIT 1""",
                            (worker.id, worker.id, time.time()),
                        )
                        if not useful:
                            worker.status = WorkerStatus.DRAINING
                await self._update_worker(worker, commit=False)
                if worker.status == WorkerStatus.ONLINE:
                    cursor = await self._db.execute(
                        "UPDATE slurm_workers SET started_at=? WHERE worker_id=? AND started_at IS NULL",
                        (time.time(), worker.id),
                    )
                    if cursor.rowcount:
                        await self._worker_event(worker, "worker_ready")
                await self._db.commit()
                return await self.get_worker(worker.id)
            except BaseException:
                await self._db.rollback()
                raise

    async def observe_slurm_worker(self, worker_id: str, observation: SlurmObservation) -> Worker:
        observation = SlurmObservation.model_validate(observation)
        async with self._queue_lock:
            await self._db.execute("BEGIN IMMEDIATE")
            try:
                worker = await self.get_worker(worker_id)
                if not worker or not worker.slurm:
                    raise KeyError(worker_id)
                info = worker.slurm
                if observation.cancel_if_empty:
                    useful = await self._db.execute_fetchall(
                        """SELECT 1 FROM jobs WHERE worker_id=? AND queue_managed=1
                           AND status IN ('pending','starting','running') LIMIT 1""",
                        (worker_id,),
                    )
                    if (useful or info.started_at is not None or info.finished_at is not None
                            or info.submission_failed):
                        # Job-scope cancellation may only retire an empty
                        # pre-start allocation. The same writer transaction
                        # fences concurrent admission and readiness.
                        await self._db.commit()
                        return worker
                if (observation.slurm_job_id and info.slurm_job_id
                        and observation.slurm_job_id != info.slurm_job_id):
                    raise SlurmError("SLURM_IDENTITY_CONFLICT", "scheduler job ID is already bound")
                # Results precede terminal evidence, including results from a stale
                # scheduler poll. They prove individual outcomes, never readiness.
                await self._apply_runner_results(worker, observation.recovered_results)
                if observation.slurm_job_id:
                    await self._db.execute(
                        "UPDATE slurm_workers SET slurm_job_id=COALESCE(slurm_job_id, ?), submitted_at=COALESCE(submitted_at, ?) WHERE worker_id=?",
                        (observation.slurm_job_id, observation.observed_at, worker_id),
                    )
                if observation.submission_attempted:
                    await self._db.execute("UPDATE slurm_workers SET submission_attempted=1 WHERE worker_id=?", (worker_id,))
                if observation.cancel_requested:
                    await self._db.execute("UPDATE slurm_workers SET cancel_requested=1 WHERE worker_id=?", (worker_id,))
                    if info.finished_at is None:
                        await self._db.execute("UPDATE workers SET status='draining' WHERE id=?", (worker_id,))
                state = observation.scheduler_state
                terminal = bool(state and state.upper().split()[0].rstrip("+") in _TERMINAL_SLURM_STATES)
                fresh = info.observed_at is None or observation.observed_at >= info.observed_at
                if fresh and info.finished_at is None:
                    await self._db.execute(
                        """UPDATE slurm_workers SET
                           scheduler_state=COALESCE(?, scheduler_state),
                           scheduler_reason=COALESCE(?, scheduler_reason), observed_at=?
                           WHERE worker_id=?""",
                        (state, observation.scheduler_reason, observation.observed_at, worker_id),
                    )
                failed_submission = bool(observation.submission_failed and info.slurm_job_id is None
                                         and observation.slurm_job_id is None and info.started_at is None)
                if observation.submission_failed and not failed_submission and not info.submission_failed:
                    raise SlurmError("SLURM_IDENTITY_CONFLICT", "cannot mark a submitted or started allocation submission-failed")
                if (terminal and fresh or failed_submission) and info.finished_at is None:
                    await self._db.execute(
                        """UPDATE slurm_workers SET finished_at=?, token_revoked=1,
                           submission_failed=MAX(submission_failed, ?) WHERE worker_id=?""",
                        (observation.observed_at, int(failed_submission), worker_id),
                    )
                    await self._db.execute("UPDATE workers SET status='offline' WHERE id=?", (worker_id,))
                    await self._db.execute(
                        """UPDATE jobs SET status='failed', exit_code=-1, finished_at=?
                           WHERE worker_id=? AND status NOT IN ('done','failed')""",
                        (observation.observed_at, worker_id),
                    )
                    details = {
                        "scheduler_state": state, "reason": observation.scheduler_reason,
                        "submission_failed": failed_submission,
                    }
                    if info.started_at is None:
                        await self._worker_event(worker, "worker_start_failed", details)
                    await self._worker_event(worker, "worker_stopped", details)
                await self._db.commit()
                return await self.get_worker(worker_id)
            except BaseException:
                await self._db.rollback()
                raise

    async def drain_slurm_worker(self, worker_id: str) -> bool:
        async with self._queue_lock:
            cursor = await self._db.execute(
                """UPDATE workers SET status='draining' WHERE id=?
                   AND EXISTS (SELECT 1 FROM slurm_workers WHERE worker_id=workers.id AND finished_at IS NULL
                     AND (failure_deadline IS NULL OR failure_deadline<=unixepoch()))
                   AND NOT EXISTS (SELECT 1 FROM jobs WHERE worker_id=workers.id
                     AND queue_managed=1 AND status IN ('pending','starting','running'))""",
                (worker_id,),
            )
            await self._db.commit()
            return bool(cursor.rowcount)

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
        )

    @_write_locked
    async def insert_pi_session(self, session: PiSession) -> None:
        await self._db.execute(
            """INSERT INTO pi_sessions
               (id, worker_id, parent_session_id, session_type, state, task, cwd, tmux_session, detail,
                role, token_hash, question, meta, name, host, agent, bridge_incarnation,
                terminal_attachable, terminal_host, terminal_port, terminal_protocol_version,
                has_pending_messages, last_seen, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                session.id, session.worker_id, session.parent_session_id, session.session_type.value,
                session.state.value, session.task, session.cwd, session.tmux_session, session.detail,
                session.role, session.token_hash, session.question, json.dumps(session.meta),
                session.name, session.host, session.agent, session.bridge_incarnation,
                int(session.terminal_attachable), session.terminal_host, session.terminal_port,
                session.terminal_protocol_version, int(session.has_pending_messages),
                session.last_seen, session.created_at, session.updated_at,
            ),
        )
        await self._db.commit()

    @_write_locked
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

    @_write_locked
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

    @_write_locked
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

    async def register_interactive_pi_session(self, payload: PiBridgeRegister, now: int | None = None) -> PiSession:
        """Create or replace one plain-Pi bridge incarnation."""
        now = now if now is not None else int(time.time())
        async with self._pi_bridge_lock:
            existing = await self.get_pi_session(payload.session_id)
            if existing and not existing.session_type.bridge_backed:
                raise ValueError("session id does not belong to a bridge-backed session")
            created_at = existing.created_at if existing else now
            await self._db.execute(
                """INSERT INTO pi_sessions
                   (id, worker_id, parent_session_id, session_type, state, task, cwd, tmux_session,
                    detail, name, host, agent, bridge_incarnation, terminal_attachable, terminal_host,
                    terminal_port, terminal_protocol_version, has_pending_messages, last_seen, created_at, updated_at)
                   VALUES (?, NULL, NULL, ?, ?, '', ?, '', '', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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

    @_write_locked
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


    # ── Jobs ──────────────────────────────────────────────────────────

    async def _insert_job_record(self, job: Job, digest: str | None = None) -> None:
        await self._db.execute(
            """INSERT INTO jobs
               (id, worker_id, name, tmux_session, command, status, queue_managed,
                exit_code, pty_enabled, kind, origin_session_id, report_revision,
                expected_seconds, gpu_count, gpu_indices, queue_order, started_at, finished_at,
                notify_recipient_id, submission_key, submission_digest)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (job.id, job.worker_id, job.name, job.tmux_session, job.command,
             job.status.value, int(job.queue_managed), job.exit_code, int(job.pty_enabled),
             job.kind.value, job.origin_session_id, job.report_revision,
             job.expected_seconds, job.gpu_count, json.dumps(job.gpu_indices),
             job.queue_order, job.started_at, job.finished_at, job.notify_recipient_id,
             job.submission_key, digest),
        )

    async def insert_job(self, job: Job) -> None:
        async with self._queue_lock:
            await self._db.execute("BEGIN IMMEDIATE")
            try:
                if job.worker_id:
                    worker = await self.get_worker(job.worker_id)
                    if worker:
                        await self.assert_worker_accepts_jobs(job.worker_id)
                        if job.notify_recipient_id is None and worker.slurm:
                            job.notify_recipient_id = worker.slurm.recipient_id
                await self._insert_job_record(job)
                await self._db.commit()
            except BaseException:
                await self._db.rollback()
                raise

    async def update_job(self, job: Job) -> None:
        async with self._queue_transaction():
            previous = await self.get_job(job.id)
            if previous and previous.worker_id:
                source = await self.get_worker(previous.worker_id)
                if (source and source.slurm and source.slurm.initial_job_id == job.id
                        and source.slurm.submission_attempted
                        and (job.worker_id != previous.worker_id or job.gpu_count != previous.gpu_count)):
                    raise SlurmError("SLURM_IDENTITY_CONFLICT", "submitted allocation resources are frozen; cancel and enqueue a fresh allocation")
            # Terminal outcomes are absorbing, including a result recovered while
            # an ordinary SSH launch or reconciliation was still outstanding.
            await self._db.execute(
                """UPDATE jobs
                   SET worker_id=?, name=?, tmux_session=?, command=?, status=?,
                       queue_managed=?, exit_code=?, pty_enabled=?, kind=?,
                       origin_session_id=?, report_revision=?, expected_seconds=?,
                       gpu_count=?, gpu_indices=?, queue_order=?, started_at=?, finished_at=?
                   WHERE id=? AND status NOT IN ('done','failed')""",
                (job.worker_id, job.name, job.tmux_session, job.command,
                 job.status.value, int(job.queue_managed), job.exit_code,
                 int(job.pty_enabled), job.kind.value, job.origin_session_id,
                 job.report_revision, job.expected_seconds, job.gpu_count,
                 json.dumps(job.gpu_indices), job.queue_order, job.started_at,
                 job.finished_at, job.id),
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
    async def get_job_by_submission_key(self, key: str) -> Job | None:
        row = await (await self._db.execute("SELECT * FROM jobs WHERE submission_key=?", (key,))).fetchone()
        return self._row_to_job(row) if row else None

    async def insert_queued_job(self, job: Job) -> Job:
        async with self._queue_lock:
            await self._db.execute("BEGIN IMMEDIATE")
            try:
                existing_worker = await self.get_worker(job.worker_id)
                payload = {key: getattr(job, key) for key in (
                    "worker_id", "name", "command", "pty_enabled", "origin_session_id",
                    "expected_seconds", "gpu_count", "notify_recipient_id",
                )}
                if payload["notify_recipient_id"] is None and existing_worker and existing_worker.slurm:
                    payload["notify_recipient_id"] = existing_worker.slurm.recipient_id
                digest = self._digest(payload)
                if job.submission_key:
                    previous = await (await self._db.execute(
                        "SELECT * FROM jobs WHERE submission_key=?", (job.submission_key,),
                    )).fetchone()
                    if previous:
                        if previous["submission_digest"] != digest:
                            raise SlurmError("IDEMPOTENCY_CONFLICT", "submission_key reused with a different job payload")
                        await self._db.commit()
                        return self._row_to_job(previous)
                worker = await self.assert_worker_accepts_jobs(job.worker_id)
                self._validate_queue_capacity(worker, job.gpu_count)
                if job.notify_recipient_id is None and worker.slurm:
                    job.notify_recipient_id = worker.slurm.recipient_id
                cursor = await self._db.execute(
                    """SELECT COALESCE(MAX(queue_order), 0) FROM jobs
                       WHERE queue_managed=1 AND worker_id=? AND status IN ('pending','starting','running')""",
                    (job.worker_id,),
                )
                job.queue_managed = True
                job.status = JobStatus.PENDING
                job.gpu_indices = []
                job.queue_order = (await cursor.fetchone())[0] + 1
                job.started_at = 0
                job.finished_at = 0
                await self._insert_job_record(job, digest)
                await self._db.commit()
                return job
            except BaseException:
                await self._db.rollback()
                raise

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

    async def claim_queued_job(
        self,
        job_id: str,
        gpu_indices: list[int],
        started_at: int,
        *,
        worker_id: str,
    ) -> Job | None:
        async with self._queue_lock:
            cursor = await self._db.execute(
                """UPDATE jobs
                   SET status = ?, gpu_indices = ?, started_at = ?
                   WHERE id = ? AND queue_managed = 1 AND status = ?
                     AND worker_id = ? AND gpu_count = ?
                     AND EXISTS (
                       SELECT 1 FROM workers WHERE id=jobs.worker_id AND status='online'
                     )
                     AND NOT EXISTS (
                       SELECT 1 FROM slurm_workers WHERE worker_id=jobs.worker_id
                         AND (finished_at IS NOT NULL OR submission_failed=1 OR cancel_requested=1)
                     )
                     AND (gpu_count>0 OR (
                       EXISTS (SELECT 1 FROM slurm_workers WHERE worker_id=jobs.worker_id
                         AND json_extract(policy, '$.compute_kind')='cpu')
                       AND NOT EXISTS (SELECT 1 FROM jobs AS active_cpu
                         WHERE active_cpu.worker_id=jobs.worker_id AND active_cpu.queue_managed=1
                           AND active_cpu.status IN ('starting','running'))
                     ))
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
                    JobStatus.STARTING.value,
                    json.dumps(gpu_indices),
                    started_at,
                    job_id,
                    JobStatus.PENDING.value,
                    worker_id,
                    len(gpu_indices),
                    worker_id,
                    JobStatus.PENDING.value,
                    worker_id,
                    JobStatus.STARTING.value,
                    JobStatus.RUNNING.value,
                    json.dumps(gpu_indices),
                ),
            )
            if not cursor.rowcount:
                await self._db.rollback()
                return None
            await self._db.commit()
            return await self.get_job(job_id)

    async def update_pending_job(
        self,
        job_id: str,
        *,
        worker_id: str | None,
        position: int | None,
        name: str | None,
        expected_seconds: int | None,
        gpu_count: int | None,
    ) -> Job:
        async with self._queue_transaction():
            row = await (
                await self._db.execute(
                    "SELECT * FROM jobs WHERE id = ? AND queue_managed = 1",
                    (job_id,),
                )
            ).fetchone()
            if row is None:
                raise KeyError(job_id)
            if row["status"] != JobStatus.PENDING.value:
                raise ValueError("queued job is not pending")

            source_worker_id = row["worker_id"]
            destination_worker_id = worker_id if worker_id is not None else source_worker_id
            destination_worker = await self.assert_worker_accepts_jobs(destination_worker_id)
            requested_gpu_count = gpu_count if gpu_count is not None else row["gpu_count"]
            source = await self.get_worker(source_worker_id)
            if (source and source.slurm and source.slurm.initial_job_id == job_id
                    and source.slurm.submission_attempted
                    and (destination_worker_id != source_worker_id or requested_gpu_count != row["gpu_count"])):
                raise SlurmError("SLURM_IDENTITY_CONFLICT", "submitted allocation resources are frozen; cancel and enqueue a fresh allocation")
            self._validate_queue_capacity(destination_worker, requested_gpu_count)
            source_rows = await self._db.execute_fetchall(
                """SELECT id FROM jobs
                   WHERE queue_managed = 1 AND worker_id = ? AND status = ? AND id != ?
                   ORDER BY queue_order, id""",
                (source_worker_id, JobStatus.PENDING.value, job_id),
            )
            source_ids = [item["id"] for item in source_rows]

            if destination_worker_id == source_worker_id:
                destination_ids = source_ids
                if position is None:
                    original_rows = await self._db.execute_fetchall(
                        """SELECT id FROM jobs
                           WHERE queue_managed = 1 AND worker_id = ? AND status = ?
                           ORDER BY queue_order, id""",
                        (source_worker_id, JobStatus.PENDING.value),
                    )
                    insertion_index = next(
                        index for index, item in enumerate(original_rows) if item["id"] == job_id
                    )
                else:
                    insertion_index = min(position - 1, len(destination_ids))
                destination_ids.insert(insertion_index, job_id)
            else:
                destination_rows = await self._db.execute_fetchall(
                    """SELECT id FROM jobs
                       WHERE queue_managed = 1 AND worker_id = ? AND status = ?
                       ORDER BY queue_order, id""",
                    (destination_worker_id, JobStatus.PENDING.value),
                )
                destination_ids = [item["id"] for item in destination_rows]
                insertion_index = (
                    len(destination_ids)
                    if position is None
                    else min(position - 1, len(destination_ids))
                )
                destination_ids.insert(insertion_index, job_id)

            cursor = await self._db.execute(
                """UPDATE jobs
                   SET worker_id = ?, name = ?, expected_seconds = ?, gpu_count = ?
                   WHERE id = ? AND queue_managed = 1 AND status = ?""",
                (
                    destination_worker_id,
                    name if name is not None else row["name"],
                    expected_seconds if expected_seconds is not None else row["expected_seconds"],
                    gpu_count if gpu_count is not None else row["gpu_count"],
                    job_id,
                    JobStatus.PENDING.value,
                ),
            )
            if not cursor.rowcount:
                await self._db.rollback()
                raise ValueError("queued job is not pending")

            if destination_worker_id != source_worker_id:
                await self._db.executemany(
                    "UPDATE jobs SET queue_order = ? WHERE id = ?",
                    [(index, queued_id) for index, queued_id in enumerate(source_ids, 1)],
                )
            await self._db.executemany(
                "UPDATE jobs SET queue_order = ? WHERE id = ?",
                [(index, queued_id) for index, queued_id in enumerate(destination_ids, 1)],
            )
            await self._db.commit()
            updated = await self.get_job(job_id)
            if updated is None:
                raise RuntimeError("queued job disappeared after update")
            return updated

    async def fail_pending_job(self, job_id: str, finished_at: int) -> Job | None:
        async with self._queue_lock:
            row = await (
                await self._db.execute(
                    """SELECT worker_id FROM jobs
                       WHERE id = ? AND queue_managed = 1 AND status = ?""",
                    (job_id, JobStatus.PENDING.value),
                )
            ).fetchone()
            if row is None:
                return None
            cursor = await self._db.execute(
                """UPDATE jobs
                   SET status = ?, exit_code = -1, finished_at = ?, queue_order = 0
                   WHERE id = ? AND queue_managed = 1 AND status = ?""",
                (
                    JobStatus.FAILED.value,
                    finished_at,
                    job_id,
                    JobStatus.PENDING.value,
                ),
            )
            if not cursor.rowcount:
                await self._db.rollback()
                return None
            pending = await self._db.execute_fetchall(
                """SELECT id FROM jobs
                   WHERE queue_managed = 1 AND worker_id = ? AND status = ?
                   ORDER BY queue_order, id""",
                (row["worker_id"], JobStatus.PENDING.value),
            )
            await self._db.executemany(
                "UPDATE jobs SET queue_order = ? WHERE id = ?",
                [(index, item["id"]) for index, item in enumerate(pending, 1)],
            )
            await self._db.commit()
            return await self.get_job(job_id)

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

    async def reconcile_starting_job(
        self,
        job_id: str,
        *,
        launch_observed: bool,
    ) -> Job | None:
        async with self._queue_lock:
            row = await (
                await self._db.execute(
                    """SELECT worker_id, queue_managed FROM jobs
                       WHERE id = ? AND status = ?""",
                    (job_id, JobStatus.STARTING.value),
                )
            ).fetchone()
            if row is None:
                return None
            if launch_observed:
                cursor = await self._db.execute(
                    """UPDATE jobs SET status = ?
                       WHERE id = ? AND status = ?""",
                    (JobStatus.RUNNING.value, job_id, JobStatus.STARTING.value),
                )
            elif row["queue_managed"]:
                await self._db.execute(
                    """UPDATE jobs SET queue_order = queue_order + 1
                       WHERE queue_managed = 1 AND worker_id = ? AND status = ?""",
                    (row["worker_id"], JobStatus.PENDING.value),
                )
                cursor = await self._db.execute(
                    """UPDATE jobs
                       SET status = ?, gpu_indices = '[]', started_at = 0, queue_order = 1
                       WHERE id = ? AND queue_managed = 1 AND status = ?""",
                    (JobStatus.PENDING.value, job_id, JobStatus.STARTING.value),
                )
            else:
                cursor = await self._db.execute(
                    """UPDATE jobs
                       SET status = ?, exit_code = -1, finished_at = unixepoch()
                       WHERE id = ? AND status = ?""",
                    (JobStatus.FAILED.value, job_id, JobStatus.STARTING.value),
                )
            if not cursor.rowcount:
                await self._db.rollback()
                return None
            await self._db.commit()
            return await self.get_job(job_id)

    async def get_running_job_count_for_worker(self, worker_id: str) -> int:
        cursor = await self._db.execute(
            "SELECT COUNT(*) FROM jobs WHERE worker_id = ? AND status IN (?, ?)",
            (worker_id, JobStatus.STARTING.value, JobStatus.RUNNING.value),
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
            worker = await self.get_worker(worker_id)
            if worker and worker.slurm:
                raise PermissionError("reserved workers cannot use legacy job reports")
            session = await self.get_pi_session(report.origin_session_id)
            if not session:
                raise KeyError(report.origin_session_id)
            if session.worker_id != worker_id:
                raise ValueError("origin session not found for worker")
            cursor = await self._db.execute(
                """INSERT INTO jobs
                   (id, worker_id, name, tmux_session, command, status, queue_managed,
                    exit_code, pty_enabled, kind, origin_session_id, report_revision,
                    expected_seconds, gpu_count, gpu_indices, queue_order, started_at, finished_at)
                   VALUES (?, ?, '', ?, ?, ?, 0, ?, ?, ?, ?, ?, 0, 0, '[]', 0, ?, ?)
                   ON CONFLICT(id) DO UPDATE SET
                     name=excluded.name,
                     tmux_session=excluded.tmux_session,
                     command=excluded.command,
                     status=excluded.status,
                     queue_managed=excluded.queue_managed,
                     exit_code=excluded.exit_code,
                     pty_enabled=excluded.pty_enabled,
                     report_revision=excluded.report_revision,
                     expected_seconds=excluded.expected_seconds,
                     gpu_count=excluded.gpu_count,
                     gpu_indices=excluded.gpu_indices,
                     queue_order=excluded.queue_order,
                     started_at=excluded.started_at,
                     finished_at=excluded.finished_at
                   WHERE jobs.worker_id=excluded.worker_id
                     AND jobs.origin_session_id=excluded.origin_session_id
                     AND jobs.kind='delegated'
                     AND jobs.report_revision < excluded.report_revision
                     AND jobs.status NOT IN ('done','failed')""",
                (
                    report.id, worker_id, report.tmux_session, report.command,
                    report.status.value, report.exit_code, int(report.pty_enabled),
                    JobKind.DELEGATED.value, report.origin_session_id,
                    report.report_revision, report.started_at, report.finished_at,
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
            name=row["name"] if "name" in row.keys() else "",
            tmux_session=row["tmux_session"],
            command=row["command"],
            status=JobStatus(row["status"]),
            queue_managed=bool(row["queue_managed"]) if "queue_managed" in row.keys() else False,
            exit_code=row["exit_code"],
            pty_enabled=bool(row["pty_enabled"]),
            kind=JobKind(row["kind"]) if "kind" in row.keys() else JobKind.SSH,
            origin_session_id=row["origin_session_id"] if "origin_session_id" in row.keys() else None,
            report_revision=row["report_revision"] if "report_revision" in row.keys() else 0,
            expected_seconds=row["expected_seconds"] if "expected_seconds" in row.keys() else 0,
            gpu_count=row["gpu_count"] if "gpu_count" in row.keys() else 0,
            gpu_indices=json.loads(row["gpu_indices"]) if "gpu_indices" in row.keys() else [],
            queue_order=row["queue_order"] if "queue_order" in row.keys() else 0,
            notify_recipient_id=row["notify_recipient_id"],
            submission_key=row["submission_key"],
            runner_result=RunnerResult.model_validate_json(row["runner_result"]) if row["runner_result"] else None,
            started_at=row["started_at"],
            finished_at=row["finished_at"],
        )

    # ── Port Forwards ─────────────────────────────────────────────────

    @_write_locked
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

    @_write_locked
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

    @_write_locked
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

    @_write_locked
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

    @_write_locked
    async def delete_marimo_session(self, session_id: str) -> None:
        await self._db.execute("DELETE FROM marimo_sessions WHERE id = ?", (session_id,))
        await self._db.commit()

    @staticmethod
    def _row_to_marimo(row: aiosqlite.Row) -> MarimoSession:
        return MarimoSession(**dict(row))

    # ── Failures ───────────────────────────────────────────────────────

    @_write_locked
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

    @_write_locked
    async def delete_worker(self, worker_id: str) -> bool:
        """Delete a worker and all its associated records."""
        worker = await self.get_worker(worker_id)
        if worker and worker.slurm:
            raise SlurmError("SLURM_WORKER_FINISHED", "Slurm reservation and job history cannot be deleted")
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

    @_write_locked
    async def prune_workers(self, older_than_ts: int) -> int:
        """Delete all workers not seen since older_than_ts."""
        cursor = await self._db.execute(
            """DELETE FROM workers WHERE last_heartbeat_ts < ?
               AND id NOT IN (SELECT worker_id FROM slurm_workers)
               AND id NOT IN (SELECT worker_id FROM jobs WHERE queue_managed=1
                 AND worker_id IS NOT NULL AND status IN ('pending','starting','running'))""",
            (older_than_ts,),
        )
        await self._db.commit()
        return cursor.rowcount
