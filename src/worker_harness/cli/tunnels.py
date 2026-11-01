"""Owned SSH forwards on the trusted Tailnet; target services must authenticate."""

from __future__ import annotations

import asyncio
import json
from urllib.parse import quote

import typer
from rich.console import Console
from rich.table import Table

from worker_harness.client import request

console = Console(markup=False)
_WARNING = "Raw TCP tunnels are reachable on the trusted Tailnet; the target service must authenticate users."


def _run(coroutine):
    try:
        asyncio.run(coroutine)
    except RuntimeError as exc:
        console.print(str(exc))
        raise typer.Exit(1) from exc


def _json_output():
    from worker_harness.cli.app import _state
    return _state.get("output", "text") == "json"


def _tunnel_add_impl(worker_id: str, local_port: int, remote_port: int, name: str = ""):
    async def run():
        tunnel = await request("POST", "/api/v1/tunnels", json=dict(
            worker_id=worker_id, local_port=local_port, remote_port=remote_port, name=name))
        if _json_output():
            console.print(json.dumps(tunnel))
        else:
            console.print(f"Tunnel started: {tunnel['id']} — "
                          f"orchestrator:{tunnel['local_port']} → {tunnel['worker_name']}:{tunnel['remote_port']}")
            console.print(_WARNING)
    _run(run())


def _tunnel_list_impl():
    async def run():
        tunnels = await request("GET", "/api/v1/tunnels")
        if _json_output():
            console.print(json.dumps(tunnels, indent=2))
            return
        table = Table(title="Your Tunnels")
        for column in ("ID", "Service", "Direction"):
            table.add_column(column)
        for tunnel in tunnels:
            table.add_row(tunnel["id"][:12], tunnel["service_name"],
                          f"orchestrator:{tunnel['local_port']} → {tunnel['worker_name']}:{tunnel['remote_port']}")
        console.print(table)
        console.print(_WARNING)
    _run(run())


def _tunnel_remove_impl(tunnel_id: str):
    async def run():
        # Resolve only against this principal's inventory; never select a global listener.
        tunnels = await request("GET", "/api/v1/tunnels")
        exact = [row for row in tunnels if row["id"] == tunnel_id]
        matches = exact or [row for row in tunnels if row["id"].startswith(tunnel_id)]
        if len(matches) > 1:
            raise RuntimeError("Tunnel ID prefix is ambiguous")
        resolved = matches[0]["id"] if matches else tunnel_id
        result = await request("DELETE", f"/api/v1/tunnels/{quote(resolved, safe='')}")
        if _json_output():
            console.print(json.dumps(result))
        else:
            console.print(f"Tunnel removed: {result['tunnel_id']}")
    _run(run())


app = typer.Typer(name="tunnel", help="Manage owned SSH tunnels. " + _WARNING)
app.command(name="add")(_tunnel_add_impl)
app.command(name="list")(_tunnel_list_impl)
app.command(name="remove")(_tunnel_remove_impl)
