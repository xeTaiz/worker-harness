"""Worker Harness Textual TUI."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from textual import on
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.message import Message
from textual.reactive import reactive
from textual.screen import ModalScreen, Screen
from textual.widgets import (
    Button,
    DataTable,
    Footer,
    Header,
    Input,
    Static,
)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _pct(a: float, b: float) -> str:
    return f"{int(100 * a / b)}%" if b != 0 else "0%"


def _gpu_summary(worker) -> str:
    if not worker.gpu_count:
        return "none"
    parts = []
    for i in range(min(worker.gpu_count, 4)):
        name = worker.gpu_names[i] if i < len(worker.gpu_names) else f"GPU{i}"
        total = worker.gpu_vram_gb[i] if i < len(worker.gpu_vram_gb) else 0
        used = worker.gpu_used_vram_gb[i] if i < len(worker.gpu_used_vram_gb) else 0
        pct = _pct(used, total)
        parts.append(f"{name} {pct}")
    extra = f" +{worker.gpu_count - 4} more" if worker.gpu_count > 4 else ""
    return ", ".join(parts) + extra


# ── Tracked tables ────────────────────────────────────────────────────────────
# Subclasses that keep a reference back to the screen for cursor tracking.

class TrackedDataTable(DataTable, can_focus=True):
    """DataTable subclass that tracks cursor movement and calls screen callbacks.

    Avoids the RowHighlighted message which may not route to Screen handlers.
    Instead, directly calls the screen's _on_cursor_moved() method.
    """

    def __init__(
        self,
        table_id: str,
        track_target: "MainScreen",
        track_attr: str,
        track_list_attr: str,
    ) -> None:
        super().__init__(id=table_id)
        self._track_screen = track_target
        self._track_attr = track_attr        # e.g. "_selected_worker_id"
        self._track_list_attr = track_list_attr  # e.g. "_workers"
        self._on_cursor_callback = None
        self._initialized = False

    def on_mount(self) -> None:
        self.cursor_type = "row"
        self._initialized = True

    def watch_cursor_coordinate(self, old, value) -> None:
        """Called by Textual's reactive system whenever cursor_coordinate changes."""
        if not self._initialized:
            return
        row_index = value.row
        # Sync selection to screen
        items = getattr(self._track_screen, self._track_list_attr)
        try:
            row_key = self._row_locations.get_key(row_index)
            idx = int(row_key.value)
        except (ValueError, TypeError, Exception):
            return
        if 0 <= idx < len(items):
            setattr(self._track_screen, self._track_attr, items[idx].id)
            # Notify screen for display update
            self._track_screen._on_cursor_moved(self.id, idx, items[idx])


# ── Modal screens ───────────────────────────────────────────────────────────────

class HelpScreen(ModalScreen[None]):
    def compose(self) -> ComposeResult:
        yield Vertical(
            Static("[bold]Worker Harness[/bold]  [dim]-- key bindings[/dim]", markup=True),
            Static(
                "[bold]Navigation[/bold]\n"
                "  Tab / Shift+Tab    Switch between worker and job list\n"
                "  j / k              Move down / up in focused list\n"
                "  Enter              Select row (worker → load jobs / job → select)\n\n"
                "[bold]Actions[/bold]\n"
                "  n                  New job (select worker first)\n"
                "  s                  Stop selected job\n"
                "  l                  View log in detail panel\n"
                "  r                  Refresh\n"
                "  c                  Clear detail panel\n\n"
                "[bold]Quit[/bold]     q",
                markup=True,
            ),
            Button("Close [?]", id="btn-close-help", variant="primary"),
            id="help-inner",
        )

    @on(Button.Pressed, "#btn-close-help")
    def on_close(self, event: Button.Pressed) -> None:
        self.app.pop_screen()


class StartJobScreen(ModalScreen[None]):
    def __init__(self, worker_id: str, app_db, app_jm, on_done) -> None:
        super().__init__()
        self.worker_id = worker_id
        self.app_db = app_db
        self.app_jm = app_jm
        self.on_done = on_done

    def compose(self) -> ComposeResult:
        yield Vertical(
            Static("[bold]Start Job[/bold]", markup=True),
            Input(placeholder="Command to run...", id="job-input"),
            Horizontal(
                Button("Start [Enter]", id="btn-do-start", variant="success"),
                Button("Cancel", id="btn-do-cancel", variant="error"),
            ),
            id="start-inner",
        )

    def on_mount(self) -> None:
        self.query_one("#job-input", Input).focus()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        self._do_start()

    def _do_start(self) -> None:
        cmd = self.query_one("#job-input", Input).value.strip()
        if not cmd:
            return

        async def _start():
            await self.app_db.connect()
            try:
                worker = await self.app_db.get_worker(self.worker_id)
                if worker:
                    job = await self.app_jm.start_job(worker, cmd, pty_enabled=False)
                    if self.on_done:
                        self.on_done(job)
            finally:
                await self.app_db.close()

        asyncio.get_event_loop().run_in_executor(None, lambda: asyncio.run(_start()))
        self.app.pop_screen()

    @on(Button.Pressed, "#btn-do-start")
    def on_start(self, event: Button.Pressed) -> None:
        self._do_start()

    @on(Button.Pressed, "#btn-do-cancel")
    def on_cancel(self, event: Button.Pressed) -> None:
        self.app.pop_screen()


# ── Main screen ───────────────────────────────────────────────────────────────

class MainScreen(Screen[None]):
    BINDINGS = [
        Binding("tab", "cycle_focus", show=False),
        Binding("shift+tab", "cycle_focus_rev", show=False),
        Binding("j", "cursor_down", show=False),
        Binding("k", "cursor_up", show=False),
        Binding("enter", "select_focused", show=False),
        Binding("n", "new_job", "New Job"),
        Binding("s", "stop_job", "Stop"),
        Binding("l", "show_log", "Log"),
        Binding("r", "refresh", "Refresh"),
        Binding("?", "toggle_help", "Help"),
        Binding("q", "quit", "Quit"),
        Binding("c", "clear_detail", "Clear", show=False),
    ]

    def __init__(self, db, jm) -> None:
        super().__init__()
        self.db = db
        self.jm = jm
        self._workers: list = []
        self._jobs: list = []
        self._selected_worker_id: str | None = None
        self._selected_job_id: str | None = None
        self._focus_order = ["worker-table", "jobs-table"]
        self._focus_idx = 0
        self._detail_mode: str = "worker"  # "worker" | "log"

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal():
            with Vertical(id="sidebar"):
                yield Static("[b]WORKERS[/b]", markup=True, id="worker-heading")
                yield TrackedDataTable(
                    "worker-table",
                    track_target=self,
                    track_attr="_selected_worker_id",
                    track_list_attr="_workers",
                )
            with Vertical(id="main-area"):
                with Vertical(id="detail-area"):
                    yield Static("[dim]Select a worker[/dim]", id="detail-text", markup=True)
                with Vertical(id="jobs-area"):
                    yield Static("[b]JOBS[/b]", markup=True, id="jobs-heading")
                    yield TrackedDataTable(
                        "jobs-table",
                        track_target=self,
                        track_attr="_selected_job_id",
                        track_list_attr="_jobs",
                    )
        yield Footer()

    def on_mount(self) -> None:
        w_table = self.query_one("#worker-table", DataTable)
        w_table.add_columns("S", "Name", "IP", "GPU", "RAM%")

        j_table = self.query_one("#jobs-table", DataTable)
        j_table.add_columns("Status", "Command", "Start", "Exit")

        self.set_interval(15, self._do_refresh)
        self._do_refresh()
        w_table.focus()

    # ── Cursor tracking via TrackedDataTable ────────────────────────────

    def _on_cursor_moved(self, table_id: str, idx: int, item) -> None:
        """Called by TrackedDataTable.watch_cursor_coordinate when j/k moves cursor."""
        if table_id == "worker-table":
            self._detail_mode = "worker"
            self.query_one("#detail-text", Static).update(
                f"[bold]{item.name}[/bold]  {item.status.value}\n"
                f"IP: {item.zerotier_ip}:{item.ssh_port}  |  "
                f"CPUs: {item.cpu_cores}  |  "
                f"RAM: {_pct(item.used_ram_gb, item.total_ram_gb)}  |  "
                f"GPUs: {_gpu_summary(item)}"
            )
        elif table_id == "jobs-table":
            self._selected_job_id = item.id
            self._detail_mode = "job_preview"
            started_fmt = (
                datetime.fromtimestamp(item.started_at, tz=timezone.utc).strftime("%H:%M")
                if item.started_at else "?"
            )
            self.query_one("#detail-text", Static).update(
                f"[bold]Job:[/bold] {item.command[:50]}\n"
                f"Status: {item.status.value}  |  Started: {started_fmt}  |  "
                f"Exit: {item.exit_code if item.exit_code is not None else '-'}\n"
                f"[dim]Press l for full log / s to stop / Enter to confirm[/dim]"
            )

    # ── Explicit row selection (Enter / click) ─────────────────────────

    @on(DataTable.RowSelected, "#worker-table")
    def on_worker_selected(self, event: DataTable.RowSelected) -> None:
        try:
            idx = int(event.row_key.value)
        except (ValueError, TypeError):
            return
        if not (0 <= idx < len(self._workers)):
            return
        w = self._workers[idx]
        self._selected_worker_id = w.id
        self._selected_job_id = None
        self._detail_mode = "worker"

        self.query_one("#detail-text", Static).update(
            f"[bold]{w.name}[/bold]  {w.status.value}\n"
            f"IP: {w.zerotier_ip}:{w.ssh_port}  |  "
            f"CPUs: {w.cpu_cores}  |  "
            f"RAM: {_pct(w.used_ram_gb, w.total_ram_gb)}  |  "
            f"GPUs: {_gpu_summary(w)}"
        )
        self._do_load_jobs(w.id)
        self.set_timer(0.05, self._focus_jobs)

    @on(DataTable.RowSelected, "#jobs-table")
    def on_job_selected(self, event: DataTable.RowSelected) -> None:
        try:
            idx = int(event.row_key.value)
        except (ValueError, TypeError):
            return
        if not (0 <= idx < len(self._jobs)):
            return
        self._selected_job_id = self._jobs[idx].id
        self._detail_mode = "job_preview"
        self.app.notify(f"Job selected: {self._jobs[idx].command[:30]}", timeout=1)

    # ── Focus cycling ───────────────────────────────────────────────────

    def action_cycle_focus(self) -> None:
        self._focus_idx = (self._focus_idx + 1) % len(self._focus_order)
        self.query_one(f"#{self._focus_order[self._focus_idx]}", DataTable).focus()

    def action_cycle_focus_rev(self) -> None:
        self._focus_idx = (self._focus_idx - 1) % len(self._focus_order)
        self.query_one(f"#{self._focus_order[self._focus_idx]}", DataTable).focus()

    def action_select_focused(self) -> None:
        focused = self.focused
        if isinstance(focused, DataTable):
            try:
                focused.action_select_cursor()
            except Exception:
                pass

    def _focus_jobs(self) -> None:
        self.query_one("#jobs-table", DataTable).focus()
        self._focus_idx = 1

    # ── Clear ─────────────────────────────────────────────────────────

    def action_clear_detail(self) -> None:
        self._selected_worker_id = None
        self._selected_job_id = None
        self._detail_mode = "worker"
        self.query_one("#detail-text", Static).update("[dim]Select a worker[/dim]")
        self.query_one("#jobs-table", DataTable).clear()

    # ── Refresh ────────────────────────────────────────────────────────

    def action_refresh(self) -> None:
        self._do_refresh()

    def _do_refresh(self) -> None:
        async def _load():
            await self.db.connect()
            try:
                workers = await self.db.list_workers()
                self._workers = workers
                table = self.screen.query_one("#worker-table", DataTable)
                table.clear()
                for i, w in enumerate(workers):
                    icon = "●" if w.status.value == "online" else "○"
                    table.add_row(
                        icon, w.name[:13], w.zerotier_ip,
                        str(w.gpu_count) if w.gpu_count else "-",
                        _pct(w.used_ram_gb, w.total_ram_gb),
                        key=i,
                    )
            finally:
                await self.db.close()
        asyncio.get_event_loop().run_in_executor(None, lambda: asyncio.run(_load()))

    # ── Jobs list ─────────────────────────────────────────────────────

    def _do_load_jobs(self, worker_id: str) -> None:
        async def _load():
            await self.db.connect()
            try:
                jobs = await self.db.list_jobs(worker_id=worker_id, status=None)
                self._jobs = jobs
                table = self.screen.query_one("#jobs-table", DataTable)
                table.clear()
                for i, j in enumerate(jobs):
                    ec_str = str(j.exit_code) if j.exit_code is not None else "-"
                    started = (
                        datetime.fromtimestamp(j.started_at, tz=timezone.utc).strftime("%H:%M")
                        if j.started_at else "?"
                    )
                    table.add_row(j.status.value, j.command[:36], started, ec_str, key=i)
            finally:
                await self.db.close()
        asyncio.get_event_loop().run_in_executor(None, lambda: asyncio.run(_load()))

    # ── Job actions ────────────────────────────────────────────────────

    def action_new_job(self) -> None:
        if not self._selected_worker_id:
            self.app.notify("Select a worker first", severity="warning", timeout=3)
            return
        self.app.push_screen(
            StartJobScreen(self._selected_worker_id, self.db, self.jm, on_done=self._on_job_done)
        )

    def _on_job_done(self, job) -> None:
        if job and self._selected_worker_id:
            self._do_load_jobs(self._selected_worker_id)
            self.app.notify(f"Job started: {job.id[:8]}", timeout=3)

    def action_stop_job(self) -> None:
        if not self._selected_job_id:
            self.app.notify("Select a job first", severity="warning", timeout=3)
            return
        job_id = self._selected_job_id
        worker_id = self._selected_worker_id

        async def _stop():
            await self.db.connect()
            try:
                job = await self.db.get_job(job_id)
                if job:
                    worker = await self.db.get_worker(job.worker_id)
                    if worker:
                        await self.jm.stop_job(worker, job_id)
                        updated = await self.jm.refresh_job_status(worker, job)
                        ec_msg = f" (exit {updated.exit_code})" if updated.exit_code is not None else ""
                        self.app.notify(f"Job stopped{ec_msg}", timeout=3)
                        if worker_id:
                            self._do_load_jobs(worker_id)
            finally:
                await self.db.close()
        asyncio.get_event_loop().run_in_executor(None, lambda: asyncio.run(_stop()))

    def action_show_log(self) -> None:
        if not self._selected_job_id:
            self.app.notify("Select a job first", severity="warning", timeout=3)
            return
        job_id = self._selected_job_id

        async def _fetch():
            await self.db.connect()
            try:
                job = await self.db.get_job(job_id)
                if job:
                    worker = await self.db.get_worker(job.worker_id)
                    if worker:
                        from ..ssh import ssh_read_log
                        content = await ssh_read_log(worker, job_id, tail=40)
                        text = content if content else "[dim]No log yet[/dim]"
                        # Post update safely
                        self.set_timer(0, lambda: self._show_log(job_id[:8], text))
            finally:
                await self.db.close()
        asyncio.get_event_loop().run_in_executor(None, lambda: asyncio.run(_fetch()))

    def _show_log(self, job_id_short: str, text: str) -> None:
        self._detail_mode = "log"
        # Format with line numbers for readability
        lines = text.split("\n")
        formatted = "\n".join(f"  {i+1:3d}: {line}" for i, line in enumerate(lines[:30]))
        self.query_one("#detail-text", Static).update(
            f"[bold]Log:[/bold] {job_id_short}\n{formatted}"
        )

    # ── Help ───────────────────────────────────────────────────────────

    def action_toggle_help(self) -> None:
        if len(self.app.screen_stack) > 1:
            self.app.pop_screen()
        else:
            self.app.push_screen(HelpScreen())


# ── App ───────────────────────────────────────────────────────────────────────

class WorkerHarnessTUI(App):
    CSS = """
    Screen { background: $surface; }

    #sidebar {
        width: 36;
        border: solid $primary;
    }

    #worker-heading {
        padding: 0 1;
        height: 1;
    }

    #worker-table { height: 1fr; border: none; }
    #main-area { width: 1fr; }
    #detail-area {
        height: 5;
        border: solid $accent;
        padding: 1 2;
        background: $panel;
    }
    #jobs-area { height: 1fr; border: solid $accent; }
    #jobs-heading { height: 1; padding: 0 1; }
    #jobs-table { height: 1fr; border: none; }

    #help-inner {
        align: center middle;
        width: 54;
        height: auto;
        background: $panel;
        border: thick $primary;
        padding: 2 3;
    }

    #start-inner {
        align: center middle;
        width: 70;
        height: auto;
        background: $panel;
        border: thick $primary;
        padding: 2;
    }
    """

    BINDINGS = [Binding("q", "quit", "Quit")]

    def __init__(self, db, jm, **kwargs):
        super().__init__(**kwargs)
        self.db = db
        self.jm = jm

    def on_mount(self) -> None:
        self.push_screen(MainScreen(self.db, self.jm))


def run_tui(db, jm) -> None:
    app = WorkerHarnessTUI(db, jm)
    app.run()