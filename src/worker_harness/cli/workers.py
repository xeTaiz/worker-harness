"""workers subcommand group."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from functools import partial

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

console = Console()


def _get_db():
    from worker_harness.db import Database
    from worker_harness.config import Config
    return Database(Config.load().db_path)


def _db_lifespan(db):
    from contextlib import asynccontextmanager
    @asynccontextmanager
    async def _ls():
        await db.connect()
        try:
            yield
        finally:
            await db.close()
    return _ls()


def _workers_list_impl():
    async def run():
        db = _get_db()
        async with _db_lifespan(db):
            workers_list = await db.list_workers()
            from worker_harness.models import WorkerStatus
            from worker_harness.cli.app import _state

            output_mode = _state.get("output", "text")

            if output_mode == "json":
                import json
                data = [w.model_dump(mode="json") for w in workers_list]
                console.print(json.dumps(data, indent=2))
            else:
                table = Table(title="Workers")
                table.add_column("Status", style="bold")
                table.add_column("Name")
                table.add_column("ZeroTier IP")
                table.add_column("SSH Port")
                table.add_column("GPUs")
                table.add_column("CPU Cores")
                table.add_column("RAM")
                table.add_column("Last Seen")

                for w in workers_list:
                    status_icon = "●" if w.status == WorkerStatus.ONLINE else "○"
                    status_color = "green" if w.status == WorkerStatus.ONLINE else "red"
                    last_seen = (
                        datetime.fromtimestamp(w.last_heartbeat_ts, tz=timezone.utc)
                        .strftime("%H:%M:%S")
                        if w.last_heartbeat_ts else "never"
                    )
                    table.add_row(
                        f"[{status_color}]{status_icon}[/]",
                        w.name, w.zerotier_ip, str(w.ssh_port),
                        str(w.gpu_count) if w.gpu_count > 0 else "-",
                        str(w.cpu_cores),
                        f"{w.used_ram_gb:.0f}/{w.total_ram_gb:.0f} GB",
                        last_seen,
                    )
                console.print(table)
    asyncio.run(run())


def _workers_show_impl(worker_id: str):
    async def run():
        db = _get_db()
        async with _db_lifespan(db):
            from worker_harness.models import WorkerStatus
            from worker_harness.cli.app import _state

            worker = await db.get_worker(worker_id)
            if not worker:
                workers = await db.list_workers()
                worker = next((w for w in workers if w.name == worker_id), None)
            if not worker:
                console.print(f"[red]Worker not found: {worker_id}[/]")
                raise typer.Exit(1)

            output_mode = _state.get("output", "text")
            if output_mode == "json":
                import json
                console.print(json.dumps(worker.model_dump(mode="json"), indent=2))
            else:
                _print_worker_detail(worker)
    asyncio.run(run())


def _workers_status_impl(worker_id: str, status: str):
    async def run():
        db = _get_db()
        async with _db_lifespan(db):
            from worker_harness.models import WorkerStatus
            try:
                new_status = WorkerStatus(status)
            except ValueError:
                console.print(f"[red]Invalid status: {status}.[/]")
                raise typer.Exit(1)
            await db.set_worker_status(worker_id, new_status)
            console.print(f"[green]Worker {worker_id} status → {new_status.value}[/]")
    asyncio.run(run())


def _print_worker_detail(w) -> None:
    from worker_harness.models import WorkerStatus
    ram_bar = _progress_bar(w.used_ram_gb, w.total_ram_gb, width=20)
    disk_bar = _progress_bar(w.used_disk_gb, w.total_disk_gb, width=20)
    content = Text()
    content += f"[bold]ID:[/bold]          {w.id}\n"
    content += f"[bold]Status:[/bold]     {w.status.value}\n"
    content += f"[bold]ZeroTier IP:[/bold] {w.zerotier_ip}:{w.ssh_port}\n"
    content += f"[bold]CPUs:[/bold]        {w.cpu_cores}\n"
    content += f"[bold]RAM:[/bold]         {ram_bar} {w.used_ram_gb:.1f}/{w.total_ram_gb:.1f} GB\n"
    content += f"[bold]Disk:[/bold]        {disk_bar} {w.used_disk_gb:.1f}/{w.total_disk_gb:.1f} GB\n"
    if w.gpu_count > 0:
        gpu_lines = []
        for i in range(w.gpu_count):
            name = w.gpu_names[i] if i < len(w.gpu_names) else f"GPU {i}"
            total = w.gpu_vram_gb[i] if i < len(w.gpu_vram_gb) else 0
            used = w.gpu_used_vram_gb[i] if i < len(w.gpu_used_vram_gb) else 0
            bar = _progress_bar(used, total, width=16)
            gpu_lines.append(f"  [[bold]{name}[/bold]] {bar} {used:.1f}/{total:.1f} GB")
        content += f"[bold]GPUs:[/bold]\n" + '\n'.join(gpu_lines) + "\n"
    panel = Panel(content, title=f"Worker: {w.name}", expand=False)
    console.print(panel)


def _progress_bar(used: float, total: float, width: int = 20) -> str:
    if total <= 0:
        return "[░" + "░" * width + "]"
    filled = int(width * used / total)
    empty = width - filled
    return f"[blue]{'█' * filled}[/][░{'░' * empty}]"


def _workers_prune_impl(minutes: int = 5):
    """Remove workers not seen in the last N minutes (default: 5)."""
    async def run():
        import time as _time
        db = _get_db()
        async with _db_lifespan(db):
            cutoff = int(_time.time()) - (minutes * 60)
            count = await db.prune_workers(cutoff)
            console.print(f"[green]Removed {count} stale worker(s).[/]")
    asyncio.run(run())


# Build the typer app
app = typer.Typer(name="workers", help="Manage worker nodes.")
app.command(name="list")(_workers_list_impl)
app.command(name="show")(_workers_show_impl)
app.command(name="status")(_workers_status_impl)
app.command(name="prune")(_workers_prune_impl)
