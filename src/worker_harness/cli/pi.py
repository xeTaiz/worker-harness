"""Pi session registry and prompt commands."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import signal
import subprocess
from pathlib import Path
from urllib.parse import quote

import httpx
import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer(help="Inspect and message registered Pi sessions")
console = Console()

_ACTIVE_STATES = {"working", "idle"}


def _base_url() -> str:
    from worker_harness.cli.app import get_config

    if os.environ.get("WH_ORCHESTRATOR_URL"):
        return get_config().control.url.rstrip("/")
    # The Pi bridge already persists this setting; native attach should work
    # from tmux without requiring a duplicate shell environment variable.
    bridge_config = Path.home() / ".pi" / "worker-harness" / "config.json"
    try:
        configured = str(json.loads(bridge_config.read_text(encoding="utf8")).get("orchestratorUrl") or "")
        if configured.startswith(("http://", "https://")):
            return configured.rstrip("/")
    except (OSError, json.JSONDecodeError):
        pass
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


def _attach_candidates(rows: list[dict]) -> list[dict]:
    candidates = [row for row in rows if row.get("state") in _ACTIVE_STATES]
    state_order = {"working": 0, "idle": 1}
    type_order = {"interactive": 0, "delegated": 1}
    return sorted(
        candidates,
        key=lambda row: (
            state_order.get(str(row.get("state")), 9),
            type_order.get(str(row.get("session_type")), 9),
            -int(row.get("updated_at") or 0),
        ),
    )


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


def _cycle_order(rows: list[dict], current_session_id: str, direction: str) -> list[dict]:
    """Return candidates after the current session, wrapping in picker order."""

    if direction not in {"next", "previous"}:
        raise ValueError(f"unknown cycle direction {direction!r}")
    if len(rows) < 2:
        return []
    current_index = next(
        (index for index, row in enumerate(rows) if str(row.get("id") or "") == current_session_id),
        -1,
    )
    step = 1 if direction == "next" else -1
    if current_index < 0:
        return rows if step > 0 else list(reversed(rows))
    return [rows[(current_index + step * offset) % len(rows)] for offset in range(1, len(rows))]


async def _attachable_candidates(rows: list[dict]) -> list[dict]:
    async def available(row: dict) -> dict | None:
        session_id = str(row.get("id") or "")
        try:
            info = await _request(
                "GET", f"/api/v1/pi/sessions/{quote(session_id, safe='')}/attach-info"
            )
        except RuntimeError:
            return None
        return row if info.get("attachable") else None

    checked = await asyncio.gather(*(available(row) for row in rows))
    return [row for row in checked if row is not None]


async def _cycle_session(current_session_id: str, direction: str) -> dict:
    candidates = _attach_candidates(await _request("GET", "/api/v1/pi/sessions"))
    available = await _attachable_candidates(
        _cycle_order(candidates, current_session_id, direction)
    )
    if not available:
        raise RuntimeError("no other attachable Pi sessions are registered")
    return available[0]


def _mark_attach_pane(session_id: str | None) -> None:
    """Expose streamed attachment state to the dedicated tmux key table."""

    pane = os.environ.get("TMUX_PANE")
    if not pane:
        return
    values = {
        "@wh_pi_attach_session": session_id,
        "@wh_pi_attach_mode": "stream" if session_id else None,
    }
    for option, value in values.items():
        command = ["tmux", "set-option", "-p", "-t", pane]
        command.extend([option, value] if value else ["-u", option])
        subprocess.run(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )


def _pick_session(rows: list[dict]) -> dict:
    if not rows:
        raise RuntimeError("no working or idle Pi sessions are registered")
    fzf = shutil.which("fzf")
    if not fzf:
        if len(rows) == 1:
            return rows[0]
        raise RuntimeError("fzf is required when no session ID is supplied")
    by_id = {str(row.get("id")): row for row in rows}
    lines = []
    for row in rows:
        session_id = str(row.get("id", ""))
        label = str(row.get("name") or row.get("task") or "-").replace("\t", " ")
        location = str(row.get("host") or row.get("worker_id") or "-").replace("\t", " ")
        cwd = str(row.get("cwd") or "-").replace("\t", " ")
        lines.append(
            "\t".join((
                session_id,
                str(row.get("state") or ""),
                str(row.get("session_type") or ""),
                label,
                location,
                cwd,
            ))
        )
    result = subprocess.run(
        [
            fzf,
            "--no-tmux",
            "--height=100%",
            "--layout=reverse",
            "--border",
            "--delimiter=\\t",
            "--with-nth=2..",
            "--header=STATE  TYPE  NAME/TASK  HOST/WORKER  CWD",
            "--prompt=Pi session> ",
        ],
        input="\n".join(lines) + "\n",
        text=True,
        stdout=subprocess.PIPE,
        check=False,
    )
    if result.returncode == 130 or not result.stdout.strip():
        raise typer.Abort()
    if result.returncode != 0:
        raise RuntimeError(f"fzf session picker failed with exit code {result.returncode}")
    session_id = result.stdout.strip().split("\t", 1)[0]
    if session_id not in by_id:
        raise RuntimeError("fzf returned an unknown Pi session")
    return by_id[session_id]


@app.command("attach")
def attach(
    target: str | None = typer.Argument(None, help="Session ID, unique ID prefix, or exact session name"),
    stream: bool = typer.Option(
        False,
        "--stream",
        help="Stream through the relay even when the original pane is in this local tmux server",
    ),
    relative: str | None = typer.Option(
        None,
        "--relative",
        help="Select the next or previous attachable session relative to TARGET",
        hidden=True,
    ),
):
    """Attach this terminal to a discovered Pi session; press Ctrl-] to detach."""

    async def run() -> None:
        from worker_harness.pi_terminal import attach_terminal, focus_local_session

        if relative:
            if relative not in {"next", "previous"} or not target:
                raise RuntimeError("--relative requires TARGET and either next or previous")
            selected = await _cycle_session(target, relative)
        else:
            candidates = _attach_candidates(await _request("GET", "/api/v1/pi/sessions"))
            selected = (
                _resolve_session(candidates, target)
                if target
                else _pick_session(await _attachable_candidates(candidates))
            )
        cycle_requests: asyncio.Queue[str] = asyncio.Queue()
        loop = asyncio.get_running_loop()
        installed_signals: list[signal.Signals] = []
        for signum, direction in ((signal.SIGUSR1, "next"), (signal.SIGUSR2, "previous")):
            try:
                loop.add_signal_handler(signum, cycle_requests.put_nowait, direction)
                installed_signals.append(signum)
            except (NotImplementedError, RuntimeError):
                pass

        try:
            while True:
                session_id = str(selected.get("id") or "")
                _mark_attach_pane(session_id)
                if not stream and await focus_local_session(session_id):
                    return
                info = await _request(
                    "GET", f"/api/v1/pi/sessions/{quote(session_id, safe='')}/attach-info"
                )
                if not info.get("attachable"):
                    raise RuntimeError(str(info.get("reason") or "Pi session is not attachable"))
                if int(info.get("protocol_version") or 0) != 2:
                    raise RuntimeError(
                        f"unsupported Pi terminal protocol {info.get('protocol_version')!r}; expected 2"
                    )
                websocket_url = str(info.get("websocket_url") or "")
                direction = await attach_terminal(
                    websocket_url,
                    cycle_requests=cycle_requests,
                )
                if direction is None:
                    return
                selected = await _cycle_session(session_id, direction)
        finally:
            _mark_attach_pane(None)
            for signum in installed_signals:
                loop.remove_signal_handler(signum)

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
