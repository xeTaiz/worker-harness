"""Inspect, message, and attach to fleet Pi sessions."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
from pathlib import Path
from urllib.parse import quote

import httpx
import typer
from rich.console import Console
from rich.table import Table

from worker_harness.agents import row_agent, validate_agent

app = typer.Typer(help="Inspect and message registered Pi sessions")
orchestrator_app = typer.Typer(
    help="Inspect and message the global orchestrator",
    no_args_is_help=True,
)
console = Console()

_ACTIVE_STATES = {"working", "idle", "blocked"}


def _base_url() -> str:
    from worker_harness.cli.app import get_config

    configured_url = os.environ.get("WH_ORCHESTRATOR_URL")
    if configured_url:
        return configured_url.rstrip("/")
    bridge_config = Path.home() / ".pi" / "worker-harness" / "config.json"
    try:
        configured_url = str(
            json.loads(bridge_config.read_text(encoding="utf8")).get("orchestratorUrl") or ""
        )
        if configured_url.startswith(("http://", "https://")):
            return configured_url.rstrip("/")
    except (OSError, json.JSONDecodeError):
        pass
    return get_config().control.url.rstrip("/")


def _headers() -> dict[str, str]:
    token = os.environ.get("WH_SESSION_TOKEN", "").strip()
    if not token and not os.environ.get("WH_SESSION_ROLE", "").strip():
        token = os.environ.get("WH_OPERATOR_TOKEN", "").strip()
        token_file = os.environ.get("WH_OPERATOR_TOKEN_FILE", "").strip()
        if not token and token_file:
            token = Path(token_file).expanduser().read_text(encoding="utf8").strip()
    return {"Authorization": f"Bearer {token}"} if token else {}


async def _request(
    method: str,
    path: str,
    payload: dict | None = None,
    *,
    allow_not_found: bool = False,
):
    try:
        async with httpx.AsyncClient(base_url=_base_url(), timeout=30.0) as client:
            response = await client.request(method, path, json=payload, headers=_headers())
            if allow_not_found and response.status_code == 404:
                return None
            response.raise_for_status()
            return response.json()
    except httpx.HTTPStatusError as exc:
        raise RuntimeError(
            f"control API returned {exc.response.status_code}: {exc.response.text}"
        ) from exc
    except httpx.HTTPError as exc:
        raise RuntimeError(f"control API unavailable: {exc}") from exc


def _output_mode() -> str:
    from worker_harness.cli.app import _state

    return _state.get("output", "text")


def _filter_agent(rows: list[dict], agent: str | None) -> list[dict]:
    if not agent:
        return rows
    validate_agent(agent)
    return [row for row in rows if row_agent(row) == agent]


@app.command("sessions")
def sessions(
    session_type: str | None = typer.Option(None, "--type", help="Filter by session type"),
    state: str | None = typer.Option(None, "--state", help="Filter by session state"),
    agent: str | None = typer.Option(None, "--agent", help="Filter by agent implementation"),
):
    """List sessions registered with the control plane."""

    async def run() -> None:
        rows = await _request("GET", "/api/v1/pi/sessions")
        if session_type:
            rows = [row for row in rows if row.get("session_type") == session_type]
        if state:
            rows = [row for row in rows if row.get("state") == state]
        rows = _filter_agent(rows, agent)
        if _output_mode() == "json":
            console.print(json.dumps(rows, indent=2))
            return
        table = Table(title="Pi Sessions")
        table.add_column("ID")
        table.add_column("Role")
        table.add_column("State")
        table.add_column("Name / Task")
        table.add_column("Host")
        table.add_column("CWD")
        for row in rows:
            table.add_row(
                str(row.get("id", ""))[:12],
                str(row.get("role") or row.get("session_type") or "-"),
                str(row.get("state") or ""),
                str(row.get("name") or row.get("task") or "-")[:40],
                str(row.get("host") or "-")[:24],
                str(row.get("cwd") or "-")[:40],
            )
        console.print(table)

    try:
        asyncio.run(run())
    except RuntimeError as exc:
        console.print(f"[red]{exc}[/]")
        raise typer.Exit(1) from exc


def _resolve_session(rows: list[dict], target: str) -> dict:
    exact = [row for row in rows if row.get("id") == target or row.get("name") == target]
    if len(exact) == 1:
        return exact[0]
    prefix = [row for row in rows if str(row.get("id", "")).startswith(target)]
    if len(prefix) == 1:
        return prefix[0]
    if not exact and not prefix:
        raise RuntimeError(f"no active Pi session matches {target!r}")
    raise RuntimeError(f"Pi session selector {target!r} is ambiguous")


def _pick_session(rows: list[dict]) -> dict:
    if not rows:
        raise RuntimeError("no active Pi sessions are registered")
    fzf = shutil.which("fzf")
    if not fzf:
        raise RuntimeError("fzf is required when no session ID is supplied")
    by_id = {str(row.get("id") or ""): row for row in rows}
    choices = "\n".join(
        "\t".join(
            (
                session_id,
                str(row.get("role") or row.get("session_type") or "-"),
                str(row.get("state") or "-"),
                str(row.get("name") or row.get("task") or "-"),
                str(row.get("cwd") or "-"),
            )
        )
        for session_id, row in by_id.items()
    )
    selected = subprocess.run(
        [fzf, "--delimiter=\\t", "--with-nth=2..", "--height=70%", "--layout=reverse"],
        input=choices,
        text=True,
        stdout=subprocess.PIPE,
        check=False,
    )
    if selected.returncode == 130 or not selected.stdout.strip():
        raise typer.Abort()
    if selected.returncode != 0:
        raise RuntimeError(f"fzf failed with exit code {selected.returncode}")
    session_id = selected.stdout.split("\t", 1)[0]
    if session_id not in by_id:
        raise RuntimeError("fzf returned an unknown Pi session")
    return by_id[session_id]


@app.command("attach")
def attach(
    target: str | None = typer.Argument(None, help="Session ID, unique prefix, or exact name"),
    agent: str | None = typer.Option(None, "--agent", help="Filter picker by agent implementation"),
):
    """Attach this terminal to an active session; press Ctrl-] to detach."""

    async def run() -> None:
        rows = _filter_agent(await _request("GET", "/api/v1/pi/sessions"), agent)
        rows = [row for row in rows if row.get("state") in _ACTIVE_STATES]
        selected = _resolve_session(rows, target) if target else _pick_session(rows)
        session_id = str(selected.get("id") or "")
        info = await _request(
            "GET", f"/api/v1/pi/sessions/{quote(session_id, safe='')}/attach-info"
        )
        if not info.get("attachable"):
            raise RuntimeError(str(info.get("reason") or "Pi session is not attachable"))
        if int(info.get("protocol_version") or 0) != 2:
            raise RuntimeError(
                f"unsupported Pi terminal protocol {info.get('protocol_version')!r}; expected 2"
            )
        from worker_harness.pi_terminal import attach_terminal

        await attach_terminal(
            str(info.get("direct_websocket_url") or info.get("websocket_url") or ""),
            fallback_websocket_url=str(info.get("gateway_websocket_url") or "") or None,
            fallback_headers=_headers(),
        )

    try:
        asyncio.run(run())
    except typer.Abort:
        raise
    except (RuntimeError, KeyboardInterrupt) as exc:
        console.print(f"[red]{exc or 'attachment interrupted'}[/]")
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
            console.print(
                f"{row.get('created_at', 0)}  {row.get('event_type', '')}  "
                f"{json.dumps(row.get('payload', {}))}"
            )

    try:
        asyncio.run(run())
    except RuntimeError as exc:
        console.print(f"[red]{exc}[/]")
        raise typer.Exit(1) from exc


@app.command("prompt")
def prompt(session_id: str, message: str):
    """Send a message to one session as the current caller."""

    async def run() -> None:
        result = await _request(
            "POST",
            f"/api/v1/pi/sessions/{session_id}:send",
            {"message": message},
        )
        if _output_mode() == "json":
            console.print(json.dumps(result, indent=2))
        else:
            console.print(f"[green]Message accepted[/] ({result['command_id']})")

    try:
        asyncio.run(run())
    except RuntimeError as exc:
        console.print(f"[red]{exc}[/]")
        raise typer.Exit(1) from exc


@orchestrator_app.command("show")
def orchestrator_show():
    """Show the live global orchestrator session."""

    async def run() -> None:
        session = await _request(
            "GET",
            "/api/v1/pi/orchestrator",
            allow_not_found=True,
        )
        if session is None:
            if _output_mode() == "json":
                console.print(json.dumps({"session": None}, indent=2))
            else:
                console.print("[yellow]No orchestrator session[/]")
            return
        if _output_mode() == "json":
            console.print(json.dumps(session, indent=2))
            return
        table = Table(title="Orchestrator Session")
        table.add_column("ID")
        table.add_column("State")
        table.add_column("Host")
        table.add_column("CWD")
        table.add_row(
            str(session.get("id", "")),
            str(session.get("state") or ""),
            str(session.get("host") or "-"),
            str(session.get("cwd") or "-"),
        )
        console.print(table)

    try:
        asyncio.run(run())
    except RuntimeError as exc:
        console.print(f"[red]{exc}[/]")
        raise typer.Exit(1) from exc


@orchestrator_app.command("send")
def orchestrator_send(message: str):
    """Send a message, launching the global orchestrator when needed."""

    async def run() -> None:
        result = await _request(
            "POST",
            "/api/v1/pi/orchestrator:send",
            {"message": message},
        )
        if _output_mode() == "json":
            console.print(json.dumps(result, indent=2))
        else:
            console.print(f"[green]Message accepted[/] ({result['command_id']})")

    try:
        asyncio.run(run())
    except RuntimeError as exc:
        console.print(f"[red]{exc}[/]")
        raise typer.Exit(1) from exc
