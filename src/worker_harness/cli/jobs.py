"""job subcommand group."""

from __future__ import annotations

import asyncio

import typer
from rich.console import Console
from rich.table import Table

console = Console()


def _config():
    from worker_harness.config import Config
    return Config.load()


def _db():
    from worker_harness.db import Database
    return Database(_config().db_path)


def _job_manager():
    from worker_harness.job import JobManager
    return _job_manager._instance  # type: ignore
_job_manager._instance = None  # type: ignore


def _get_jm():
    from worker_harness.job import JobManager
    if _job_manager._instance is None:  # type: ignore
        _job_manager._instance = JobManager(_db())
    return _job_manager._instance  # type: ignore


async def _db_connect():
    db = _db()
    await db.connect()
    return db


def _job_start_impl(
    worker_id: str,
    command: str,
    name: str | None = None,
    no_pty: bool = False,
):
    async def run():
        db = await _db_connect()
        async with db:
            jm = _get_jm()
            jm.db = db
            from worker_harness.cli.app import _state
            output_mode = _state.get("output", "text")

            worker = await db.get_worker(worker_id)
            if not worker:
                workers = await db.list_workers()
                worker = next((w for w in workers if w.name == worker_id), None)
            if not worker:
                console.print(f"[red]Worker not found: {worker_id}[/]")
                raise typer.Exit(1)

            job = await jm.start_job(worker, command, name=name, pty_enabled=not no_pty)

            if output_mode == "json":
                import json
                console.print(json.dumps({
                    "job_id": job.id,
                    "tmux_session": job.tmux_session,
                    "status": job.status.value
                }))
            else:
                from worker_harness.models import JobStatus
                if job.status == JobStatus.FAILED:
                    console.print(f"[red]Failed to start job on {worker.name}[/]")
                    raise typer.Exit(1)
                console.print(f"[green]Job started[/]: {job.id}")
                console.print(f"  Worker:  {worker.name} ({worker.zerotier_ip})")
                console.print(f"  Session: {job.tmux_session}")
                short_cmd = command[:80] + ("..." if len(command) > 80 else "")
                console.print(f"  Command: {short_cmd}")
    asyncio.run(run())


def _job_list_impl(
    worker_id: str | None = None,
    status_filter: str | None = None,
):
    async def run():
        db = await _db_connect()
        async with db:
            from worker_harness.cli.app import _state
            from worker_harness.models import JobStatus
            output_mode = _state.get("output", "text")

            status = None
            if status_filter:
                try:
                    status = JobStatus(status_filter)
                except ValueError:
                    console.print(f"[red]Invalid status: {status_filter}[/]")
                    raise typer.Exit(1)

            jobs_list = await db.list_jobs(worker_id=worker_id, status=status)
            all_workers = {w.id: w for w in await db.list_workers()}

            if output_mode == "json":
                import json
                data = [
                    {
                        **j.model_dump(mode="json"),
                        "worker_name": getattr(all_workers.get(j.worker_id), "name", None),
                    }
                    for j in jobs_list
                ]
                console.print(json.dumps(data, indent=2))
            else:
                table = Table(title="Jobs")
                table.add_column("ID")
                table.add_column("Status")
                table.add_column("Worker")
                table.add_column("Command")
                table.add_column("Exit")
                table.add_column("Started")

                for j in jobs_list:
                    worker = all_workers.get(j.worker_id)
                    status_color = {
                        JobStatus.RUNNING: "yellow",
                        JobStatus.DONE: "green",
                        JobStatus.FAILED: "red",
                        JobStatus.PENDING: "dim",
                    }.get(j.status, "")
                    short_cmd = j.command[:40] + ("..." if len(j.command) > 40 else "")
                    table.add_row(
                        j.id[:12],
                        f"[{status_color}]{j.status.value}[/]",
                        (worker.name if worker else (j.worker_id or "-"))[:15],
                        short_cmd,
                        str(j.exit_code) if j.exit_code is not None else "-",
                        _format_timestamp(j.started_at),
                    )
                console.print(table)
    asyncio.run(run())


def _job_logs_impl(
    job_id: str,
    tail: int | None = None,
    head: int | None = None,
    follow: bool = False,
):
    async def run():
        db = await _db_connect()
        async with db:
            job = await db.get_job(job_id)
            if not job or not job.worker_id:
                console.print(f"[red]Job not found: {job_id}[/]")
                raise typer.Exit(1)

            worker = await db.get_worker(job.worker_id)
            if not worker:
                console.print(f"[red]Worker not found for job: {job.worker_id}[/]")
                raise typer.Exit(1)

            jm = _get_jm()
            jm.db = db

            if follow:
                _stream_logs(jm, worker, job_id)
            else:
                t = tail if tail is not None else (None if head is not None else 10)
                h = head
                logs = await jm.get_logs(worker, job_id, tail=t, head=h)
                if logs:
                    console.print(logs, end="")
                else:
                    console.print("[dim]No log output yet.[/]")
    asyncio.run(run())


def _job_stop_impl(job_id: str):
    async def run():
        db = await _db_connect()
        async with db:
            jm = _get_jm()
            jm.db = db
            from worker_harness.cli.app import _state
            output_mode = _state.get("output", "text")

            job = await db.get_job(job_id)
            if not job or not job.worker_id:
                console.print(f"[red]Job not found: {job_id}[/]")
                raise typer.Exit(1)

            worker = await db.get_worker(job.worker_id)
            if not worker:
                console.print(f"[red]Worker not found for job: {job.worker_id}[/]")
                raise typer.Exit(1)

            success = await jm.stop_job(worker, job_id)
            if output_mode == "json":
                import json
                console.print(json.dumps({"job_id": job_id, "stopped": success}))
            elif success:
                console.print(f"[green]Job stopped: {job_id}[/]")
            else:
                console.print(f"[red]Failed to stop job: {job_id}[/]")
                raise typer.Exit(1)
    asyncio.run(run())


async def _stream_logs_async(jm, worker, job_id: str) -> None:
    last_len = 0
    try:
        while True:
            logs = await jm.get_logs(worker, job_id, tail=100)
            lines = logs.splitlines()
            for line in lines[last_len:]:
                console.print(line)
            last_len = len(lines)
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        pass


def _stream_logs(jm, worker, job_id: str) -> None:
    try:
        asyncio.run(_stream_logs_async(jm, worker, job_id))
    except KeyboardInterrupt:
        pass


def _format_timestamp(ts: int) -> str:
    if not ts:
        return "-"
    from datetime import datetime, timezone
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%m-%d %H:%M")


# Build the typer app
app = typer.Typer(name="job", help="Manage jobs on workers.")
app.command(name="start")(_job_start_impl)
app.command(name="list")(_job_list_impl)
app.command(name="logs")(_job_logs_impl)
app.command(name="stop")(_job_stop_impl)
