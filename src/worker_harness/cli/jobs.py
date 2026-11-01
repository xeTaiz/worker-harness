"""Owner-authorized jobs and the shared public FIFO board."""

from __future__ import annotations

import asyncio
import json
import math
from datetime import datetime, timezone
from enum import Enum
from urllib.parse import quote

import typer
from rich.console import Console
from rich.table import Table

from worker_harness.client import request

console = Console(markup=False)


class Priority(str, Enum):
    flexible = "flexible"
    normal = "normal"
    blocking = "blocking"
    time_critical = "time_critical"


def _output_mode():
    from worker_harness.cli.app import _state
    return _state.get("output", "text")


def _run(coroutine):
    try:
        return asyncio.run(coroutine)
    except RuntimeError as exc:
        console.print(str(exc), markup=False)
        raise typer.Exit(1) from exc


def _format_timestamp(ts: int | None) -> str:
    if not ts:
        return "-"
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%m-%d %H:%M")


def delay_messages(entries: list[dict]) -> list[str]:
    """Render individual estimate-at-move events, never sum historic delays."""
    messages = []
    for entry in entries:
        delay = entry.get("delay_seconds")
        if delay is None:
            message = "Moved later; delay estimate unavailable"
        elif delay > 0:
            message = (f"+{math.ceil(delay / 60)} minutes estimated delay because of "
                       f"{entry['moved_job_name']} by {entry['moved_job_owner_display_name']}")
        else:
            continue
        messages.append(f"{message}; Moved by {entry['actor_display_name']}; "
                        f"{entry['reason']} ({_format_timestamp(entry.get('created_at'))})")
    return messages


def _job_start_impl(
    worker_id: str,
    command: str,
    name: str = typer.Option(..., "--name", help="Public queue label (not a private command)"),
    expected_seconds: int = typer.Option(..., "--expected-seconds", min=1),
    gpu_count: int = typer.Option(0, "--gpu-count", min=0, help="0: immediate CPU setup; positive: FIFO GPU queue"),
    priority: Priority = typer.Option(Priority.normal, "--priority"),
    priority_note: str = typer.Option("", "--priority-note"),
    deadline_at: int | None = typer.Option(None, "--deadline-at"),
    no_pty: bool = False,
):
    if not name.strip():
        raise typer.BadParameter("name must not be blank", param_hint="--name")
    if priority == Priority.time_critical and not priority_note.strip():
        raise typer.BadParameter("time_critical requires --priority-note")

    async def run():
        payload = dict(worker_id=worker_id, command=command, name=name,
                       expected_seconds=expected_seconds, priority=priority.value,
                       priority_note=priority_note, deadline_at=deadline_at, no_pty=no_pty)
        path = "/api/v1/jobs"
        if gpu_count:
            payload["gpu_count"] = gpu_count
            path += "/queue"
        job = await request("POST", path, json=payload)
        if _output_mode() == "json":
            console.print(json.dumps(job))
        else:
            console.print(f"Job {job['status']}: {job['id']}", markup=False)
        if job["status"] == "failed":
            raise typer.Exit(1)
    _run(run())


def _job_list_impl(
    worker_id: str | None = typer.Option(None, "--worker", help="Filter by worker ID or name"),
    status_val: str | None = typer.Option(None, "--status", help="Filter by status"),
):
    async def run():
        params = {k: v for k, v in {"worker_id": worker_id, "status": status_val}.items() if v is not None}
        jobs = await request("GET", "/api/v1/jobs", params=params)
        if _output_mode() == "json":
            console.print(json.dumps(jobs, indent=2))
            return
        table = Table(title="Your Jobs")
        for column in ("ID", "Status", "Worker", "Name", "Command", "Exit", "Started", "Stop reason"):
            table.add_column(column)
        for job in jobs:
            table.add_row(job["id"][:12], job.get("execution_state") if job.get("stop_requested_at") else job["status"],
                          job.get("worker_name") or job["worker_id"], job.get("name") or "-",
                          job["command"][:40], str(job.get("exit_code")),
                          _format_timestamp(job.get("started_at")), job.get("termination_reason") or "-")
        console.print(table)
    _run(run())


def _job_queue_impl(worker_id: str | None = typer.Option(None, "--worker")):
    async def run():
        board = await request("GET", "/api/v1/jobs/queue", params={"worker_id": worker_id} if worker_id else {})
        if _output_mode() == "json":
            console.print(json.dumps(board, indent=2))
            return
        for worker in board["workers"]:
            table = Table(title=f"{worker['worker_name']} — FIFO revision {worker['revision']}")
            for column in ("Position", "ID", "Owner", "Name", "GPU", "Priority", "Status", "Estimate", "Delay history"):
                table.add_column(column)
            for job in worker["jobs"] + worker["cpu_jobs"]:
                eta = (f"{_format_timestamp(job.get('estimated_start_at'))} → "
                       f"{_format_timestamp(job.get('estimated_finish_at'))}") if job["estimate_status"] == "known" else (
                           "unknown: " + job.get("estimate_explanation", ""))
                duration = f"{job['expected_seconds']}s" if job.get("expected_seconds") else "unknown runtime"
                table.add_row(str(job["position"]), job["id"][:12], job["owner_display_name"], job["name"],
                              str(job["gpu_count"]), job["priority"],
                              job.get("execution_state") if job.get("stop_requested_at") else job["status"],
                              f"{duration}; {eta}", "\n".join(delay_messages(job.get("delay_history", []))))
            console.print(table)
    _run(run())


def _job_logs_impl(job_id: str, tail: int | None = None, head: int | None = None, follow: bool = False):
    async def run():
        path = f"/api/v1/jobs/{quote(job_id, safe='')}/logs"
        params = {}
        if tail is not None:
            params["tail"] = tail
        elif head is None and not follow:
            params["tail"] = 10
        if head is not None:
            params["head"] = head
        previous = ""
        while True:
            data = await request("GET", path, params=params)
            logs = data["logs"]
            if follow:
                # Poll the whole selected log, not a sliding tail whose length stays constant.
                common = min(len(previous), len(logs))
                if logs[:common] == previous[:common]:
                    console.print(logs[common:], end="", markup=False, highlight=False)
                else:
                    console.print(logs, end="", markup=False, highlight=False)
                previous = logs
                await asyncio.sleep(1)
            else:
                console.print(logs or "No log output yet.\n", end="", markup=False, highlight=False)
                return
    try:
        _run(run())
    except KeyboardInterrupt:
        pass


def _job_stop_impl(job_id: str):
    async def run():
        result = await request("DELETE", f"/api/v1/jobs/{quote(job_id, safe='')}")
        if _output_mode() == "json":
            console.print(json.dumps(result))
        else:
            console.print(f"Job stopped: {result['job_id']}", markup=False)
    _run(run())


app = typer.Typer(name="job", help="Manage owned jobs and view the shared FIFO board.")
app.command(name="start")(_job_start_impl)
app.command(name="list")(_job_list_impl)
app.command(name="queue")(_job_queue_impl)
app.command(name="logs")(_job_logs_impl)
app.command(name="stop")(_job_stop_impl)
