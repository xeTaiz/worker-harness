"""Pi session registry and prompt commands."""

from __future__ import annotations

import asyncio
import json

import httpx
import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer(help="Inspect and message registered Pi sessions")
console = Console()


def _base_url() -> str:
    from worker_harness.cli.app import get_config

    return get_config().control.url.rstrip("/")


async def _request(method: str, path: str, payload: dict | None = None):
    try:
        async with httpx.AsyncClient(base_url=_base_url(), timeout=30.0) as client:
            response = await client.request(method, path, json=payload)
            response.raise_for_status()
            return response.json()
    except httpx.HTTPStatusError as exc:
        detail = exc.response.text
        raise RuntimeError(f"control API returned {exc.response.status_code}: {detail}") from exc
    except httpx.HTTPError as exc:
        raise RuntimeError(f"control API unavailable: {exc}") from exc


def _output_mode() -> str:
    from worker_harness.cli.app import _state

    return _state.get("output", "text")


@app.command("sessions")
def sessions(
    session_type: str | None = typer.Option(None, "--type", help="Filter by interactive, delegated, or global-router"),
    state: str | None = typer.Option(None, "--state", help="Filter by session state"),
):
    """List sessions registered with the orchestrator."""

    async def run() -> None:
        rows = await _request("GET", "/api/v1/pi/sessions")
        if session_type:
            rows = [row for row in rows if row.get("session_type") == session_type]
        if state:
            rows = [row for row in rows if row.get("state") == state]
        if _output_mode() == "json":
            console.print(json.dumps(rows, indent=2))
            return
        table = Table(title="Pi Sessions")
        table.add_column("ID")
        table.add_column("Type")
        table.add_column("State")
        table.add_column("Name / Task")
        table.add_column("Host / Worker")
        table.add_column("CWD")
        for row in rows:
            label = row.get("name") or row.get("task") or "-"
            location = row.get("host") or row.get("worker_id") or "-"
            table.add_row(
                str(row.get("id", ""))[:12],
                str(row.get("session_type", "")),
                str(row.get("state", "")),
                str(label)[:40],
                str(location)[:24],
                str(row.get("cwd") or "-")[:40],
            )
        console.print(table)

    try:
        asyncio.run(run())
    except RuntimeError as exc:
        console.print(f"[red]{exc}[/]")
        raise typer.Exit(1) from exc


@app.command("events")
def events(session_id: str):
    """Show the durable event history for one session."""

    async def run() -> None:
        rows = await _request("GET", f"/api/v1/pi/sessions/{session_id}/events")
        if _output_mode() == "json":
            console.print(json.dumps(rows, indent=2))
            return
        for row in rows:
            console.print(f"{row.get('created_at', 0)}  {row.get('event_type', '')}  {json.dumps(row.get('payload', {}))}")

    try:
        asyncio.run(run())
    except RuntimeError as exc:
        console.print(f"[red]{exc}[/]")
        raise typer.Exit(1) from exc


@app.command("prompt")
def prompt(
    session_id: str,
    message: str,
    steer: bool = typer.Option(False, "--steer", help="Steer an active turn instead of queueing a follow-up"),
):
    """Queue a prompt for a delegated or interactive Pi session."""

    async def run() -> None:
        result = await _request(
            "POST",
            f"/api/v1/pi/sessions/{session_id}:prompt",
            {"message": message, "deliver_as": "steer" if steer else "followUp"},
        )
        if _output_mode() == "json":
            console.print(json.dumps(result, indent=2))
        else:
            command_id = result.get("command_id")
            suffix = f" ({command_id})" if command_id else ""
            console.print(f"[green]Prompt accepted[/]{suffix}")

    try:
        asyncio.run(run())
    except RuntimeError as exc:
        console.print(f"[red]{exc}[/]")
        raise typer.Exit(1) from exc
