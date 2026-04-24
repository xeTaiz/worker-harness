"""Textual TUI for Worker Harness."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical
from textual.reactive import reactive
from textual.widgets import DataTable, Header, Footer, Static, Log

from ..db import Database
from ..models import Worker, WorkerStatus


class WorkerHarnessTUI(App):
    """Main TUI application."""

    CSS = """
    Screen {
        background: $surface;
    }
    #sidebar {
        width: 40;
        border: solid $primary;
        height: 100%;
    }
    #detail {
        width: 1fr;
        border: solid $accent;
        padding: 1;
    }
    #worker-table {
        height: 1fr;
    }
    WorkerInfo {
        height: auto;
    }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("r", "refresh", "Refresh"),
        Binding("j", "cursor_down", "Down", show=False),
        Binding("k", "cursor_up", "Up", show=False),
        Binding("l", "view_logs", "Logs"),
        Binding("s", "stop_job", "Stop Job"),
        Binding("t", "add_tunnel", "Tunnel"),
        Binding("i", "shell", "Shell"),
        Binding("?", "help", "Help"),
    ]

    selected_worker_id: reactive[str | None] = reactive(None)

    def __init__(self, db: Database, **kwargs):
        super().__init__(**kwargs)
        self.db = db
        self.workers: list[Worker] = []
        self.title = "Worker Harness"

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal():
            with Vertical(id="sidebar"):
                yield Static("WORKERS", id="sidebar-title")
                yield DataTable(id="worker-table")
            with Vertical(id="detail"):
                yield WorkerDetail(id="worker-detail")

    def on_mount(self) -> None:
        table = self.query_one("#worker-table", DataTable)
        table.add_columns("S", "Name", "IP", "GPUs", "RAM")
        self.refresh_worker_list()

        def on_row_selected(event: DataTable.RowSelected) -> None:
            row_key = event.row_key.value
            if row_key and row_key < len(self.workers):
                self.selected_worker_id = self.workers[row_key].id
                self.query_one("#worker-detail", WorkerDetail).update_worker(
                    self.workers[row_key]
                )

        table.on_row_selected = on_row_selected  # type: ignore

    def refresh_worker_list(self) -> None:
        """Reload worker list from DB and update the table."""
        table = self.query_one("#worker-table", DataTable)
        table.clear()
        self.workers = asyncio.run(self._load_workers())

        for i, w in enumerate(self.workers):
            icon = "●" if w.status == WorkerStatus.ONLINE else "○"
            ram = f"{w.used_ram_gb:.0f}/{w.total_ram_gb:.0f}G"
            table.add_row(icon, w.name, w.zerotier_ip, str(w.gpu_count), ram, key=str(i))

    async def _load_workers(self) -> list[Worker]:
        return await self.db.list_workers()

    def action_refresh(self) -> None:
        self.refresh_worker_list()

    def action_cursor_down(self) -> None:
        table = self.query_one("#worker-table", DataTable)
        table.action_cursor_down()

    def action_cursor_up(self) -> None:
        table = self.query_one("#worker-table", DataTable)
        table.action_cursor_up()


class WorkerDetail(Static):
    """Detail panel showing selected worker's full info."""

    def __init__(self, **kwargs):
        super().__init__("", **kwargs)

    def update_worker(self, worker: Worker) -> None:
        ram_pct = worker.used_ram_gb / worker.total_ram_gb if worker.total_ram_gb else 0
        disk_pct = worker.used_disk_gb / worker.total_disk_gb if worker.total_disk_gb else 0
        ram_bar = self._bar(ram_pct)
        disk_bar = self._bar(disk_pct)

        lines = [
            f"[bold]{worker.name}[/bold]  ({worker.status.value})",
            f"  ID:      {worker.id[:20]}...",
            f"  IP:      {worker.zerotier_ip}:{worker.ssh_port}",
            f"  CPUs:    {worker.cpu_cores}",
            f"  RAM:     {ram_bar}  {worker.used_ram_gb:.1f}/{worker.total_ram_gb:.1f} GB",
            f"  Disk:    {disk_bar}  {worker.used_disk_gb:.1f}/{worker.total_disk_gb:.1f} GB",
        ]
        if worker.gpu_count > 0:
            lines.append(f"  GPUs:    {worker.gpu_count}x {', '.join(worker.gpu_names)}")
            lines.append(f"  VRAM:    {', '.join(str(v) + 'GB' for v in worker.gpu_vram_gb)}")

        lines.append(f"\n[dim]Last heartbeat: {datetime.fromtimestamp(worker.last_heartbeat_ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}[/]")
        self.update("\n".join(lines))

    def _bar(self, fraction: float, width: int = 20) -> str:
        filled = min(int(width * fraction), width)
        return f"[blue]{'█' * filled}[/][░{'░' * (width - filled)}]"


def run_tui(db: Database) -> None:
    """Launch the TUI with a pre-connected database."""
    app = WorkerHarnessTUI(db)
    app.run()
