"""Async SQLite repository layer."""

from __future__ import annotations

import aiosqlite
import json
from datetime import datetime, timezone
from pathlib import Path

from .models import (
    Failure,
    Job,
    JobStatus,
    PortForward,
    Worker,
    WorkerRegistration,
    WorkerStatus,
)


class Database:
    """Async SQLite database with all repository methods."""

    def __init__(self, path: str | Path = "~/.config/worker-harness/db.sqlite") -> None:
        self.path = Path(path).expanduser()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._db: aiosqlite.Connection | None = None

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
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                worker_id TEXT REFERENCES workers(id),
                tmux_session TEXT,
                command TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                exit_code INTEGER,
                pty_enabled INTEGER DEFAULT 1,
                started_at INTEGER DEFAULT 0,
                finished_at INTEGER DEFAULT 0
            )
        """)
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
            CREATE TABLE IF NOT EXISTS failures (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT,
                worker_id TEXT,
                exit_code INTEGER,
                timestamp INTEGER,
                summary TEXT
            )
        """)
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
                status, last_heartbeat_ts, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                w.id, w.name, w.worker_ip, w.dns_name, w.ssh_user, w.harness_dir, w.gpu_count,
                json.dumps(w.gpu_names), json.dumps(w.gpu_vram_gb),
                json.dumps(w.gpu_used_vram_gb),
                w.cpu_cores, w.total_ram_gb, w.used_ram_gb,
                w.total_disk_gb, w.used_disk_gb,
                w.status.value, w.last_heartbeat_ts, w.created_at,
            ),
        )
        await self._db.commit()

    async def _update_worker(self, w: Worker) -> None:
        await self._db.execute(
            """UPDATE workers SET
               name=?, worker_ip=?, dns_name=?, ssh_user=?, harness_dir=?, gpu_count=?, gpu_names=?,
               gpu_vram_gb=?, gpu_used_vram_gb=?, cpu_cores=?, total_ram_gb=?, used_ram_gb=?,
               total_disk_gb=?, used_disk_gb=?, status=?, last_heartbeat_ts=?
               WHERE id=?""",
            (
                w.name, w.worker_ip, w.dns_name, w.ssh_user, w.harness_dir, w.gpu_count,
                json.dumps(w.gpu_names), json.dumps(w.gpu_vram_gb),
                json.dumps(w.gpu_used_vram_gb),
                w.cpu_cores, w.total_ram_gb, w.used_ram_gb,
                w.total_disk_gb, w.used_disk_gb,
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
            status=WorkerStatus(row["status"]),
            last_heartbeat_ts=row["last_heartbeat_ts"],
            created_at=row["created_at"],
        )

    # ── Jobs ──────────────────────────────────────────────────────────

    async def insert_job(self, job: Job) -> None:
        await self._db.execute(
            """INSERT INTO jobs (id, worker_id, tmux_session, command, status,
                                 exit_code, pty_enabled, started_at, finished_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                job.id, job.worker_id, job.tmux_session, job.command,
                job.status.value, job.exit_code, int(job.pty_enabled),
                job.started_at, job.finished_at,
            ),
        )
        await self._db.commit()

    async def update_job(self, job: Job) -> None:
        await self._db.execute(
            """UPDATE jobs SET worker_id=?, tmux_session=?, command=?, status=?,
                                 exit_code=?, pty_enabled=?, started_at=?, finished_at=?
               WHERE id=?""",
            (
                job.worker_id, job.tmux_session, job.command,
                job.status.value, job.exit_code, int(job.pty_enabled),
                job.started_at, job.finished_at, job.id,
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
    ) -> list[Job]:
        query = "SELECT * FROM jobs WHERE 1=1"
        params: list = []
        if worker_id:
            query += " AND worker_id = ?"
            params.append(worker_id)
        if status:
            query += " AND status = ?"
            params.append(status.value)
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

    def _row_to_job(self, row: aiosqlite.Row) -> Job:
        return Job(
            id=row["id"],
            worker_id=row["worker_id"],
            tmux_session=row["tmux_session"],
            command=row["command"],
            status=JobStatus(row["status"]),
            exit_code=row["exit_code"],
            pty_enabled=bool(row["pty_enabled"]),
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
