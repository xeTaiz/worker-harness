"""tunnel subcommand group — manage SSH port forwards."""

from __future__ import annotations

import asyncio
import subprocess
from datetime import datetime, timezone
from subprocess import Popen

import typer
from rich.console import Console
from rich.table import Table

console = Console()

_tunnel_processes: dict[str, Popen] = {}


def _config():
    from worker_harness.config import Config
    return Config.load()


def _db():
    from worker_harness.db import Database
    return Database(_config().db_path)


async def _db_connect():
    db = _db()
    await db.connect()
    return db


def _tunnel_add_impl(
    worker_id: str,
    local_port: int,
    remote_port: int,
    name: str = "",
):
    async def run():
        db = await _db_connect()
        async with db:
            from worker_harness.cli.app import _state
            output_mode = _state.get("output", "text")

            worker = await db.get_worker(worker_id)
            if not worker:
                workers = await db.list_workers()
                worker = next((w for w in workers if w.name == worker_id), None)
            if not worker:
                console.print(f"[red]Worker not found: {worker_id}[/]")
                raise typer.Exit(1)

            from worker_harness.models import PortForward
            from worker_harness.ssh import ssh_port_forward

            existing = await db.list_port_forwards()
            conflict = next((p for p in existing if p.local_port == local_port), None)
            if conflict:
                console.print(f"[yellow]Local port {local_port} already forwarded.[/]")
                if not typer.confirm("Overwrite existing tunnel?"):
                    raise typer.Exit(0)

            pf = PortForward(
                worker_id=worker.id,
                local_port=local_port,
                remote_port=remote_port,
                service_name=name or f"port-{remote_port}",
                created_at=int(datetime.now(timezone.utc).timestamp()),
            )

            proc = await ssh_port_forward(worker, local_port, remote_port)
            pf.pid = proc.pid
            await db.insert_port_forward(pf)
            _tunnel_processes[pf.id] = proc

            if output_mode == "json":
                import json
                console.print(json.dumps({
                    "tunnel_id": pf.id,
                    "local_port": local_port,
                    "remote_port": remote_port,
                    "worker": worker.name,
                    "service": pf.service_name,
                    "pid": proc.pid,
                }))
            else:
                direction = f"localhost:{local_port} → {worker.name}:{remote_port}"
                console.print(f"[green]Tunnel started[/]: {direction}")
                console.print(f"  Service:  {pf.service_name}")
                console.print(f"  PID:      {proc.pid}")
    asyncio.run(run())


def _tunnel_list_impl():
    async def run():
        db = await _db_connect()
        async with db:
            from worker_harness.cli.app import _state
            output_mode = _state.get("output", "text")
            forwards = await db.list_port_forwards()
            workers = {w.id: w for w in await db.list_workers()}

            if output_mode == "json":
                import json
                data = [
                    {
                        **p.model_dump(mode="json"),
                        "worker_name": getattr(workers.get(p.worker_id), "name", None),
                    }
                    for p in forwards
                ]
                console.print(json.dumps(data, indent=2))
            else:
                table = Table(title="Active Tunnels")
                table.add_column("ID")
                table.add_column("Service")
                table.add_column("Direction")
                table.add_column("PID")
                for pf in forwards:
                    worker = workers.get(pf.worker_id)
                    wname = worker.name if worker else pf.worker_id[:12]
                    direction = f"localhost:{pf.local_port} → {wname}:{pf.remote_port}"
                    table.add_row(pf.id[:12], pf.service_name, direction, str(pf.pid))
                console.print(table)
    asyncio.run(run())


def _tunnel_remove_impl(tunnel_id: str):
    async def run():
        db = await _db_connect()
        async with db:
            from worker_harness.cli.app import _state
            output_mode = _state.get("output", "text")

            forwards = await db.list_port_forwards()
            pf = next(
                (p for p in forwards if p.id == tunnel_id or p.id.startswith(tunnel_id)),
                None,
            )
            if not pf:
                console.print(f"[red]Tunnel not found: {tunnel_id}[/]")
                raise typer.Exit(1)

            proc = _tunnel_processes.pop(pf.id, None)
            if proc:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()

            await db.delete_port_forward(pf.id)

            if output_mode == "json":
                import json
                console.print(json.dumps({"tunnel_id": pf.id, "removed": True}))
            else:
                console.print(f"[green]Tunnel removed: {pf.id}[/]")
    asyncio.run(run())


# Build the typer app
app = typer.Typer(name="tunnel", help="Manage SSH port tunnels.")
app.command(name="add")(_tunnel_add_impl)
app.command(name="list")(_tunnel_list_impl)
app.command(name="remove")(_tunnel_remove_impl)
