"""Worker Harness Textual control-API client.

The public board contains labels only; command/log/stop controls use owned rows.
Local worker administration remains in the separate operator CLI diagnostics.
"""

from __future__ import annotations

from datetime import datetime, timezone

from rich.text import Text
from textual import on
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen, Screen
from textual.widgets import Button, DataTable, Footer, Header, Input, Select, Static
from urllib.parse import quote

from worker_harness.client import request
from worker_harness.cli.jobs import delay_messages


def _timestamp(value) -> str:
    return datetime.fromtimestamp(value, tz=timezone.utc).strftime("%m-%d %H:%M") if value else "unknown"


class HelpScreen(ModalScreen[None]):
    def compose(self) -> ComposeResult:
        yield Vertical(
            Static("Worker Harness — key bindings\n\n"
                   "Tab / Shift+Tab: switch tables\nj / k: move selection\n"
                   "Enter: select worker or job\nn: new CPU/GPU job\n"
                   "s: stop owned job\nl: owned job logs\nr: refresh\nc: clear\nq: quit",
                   markup=False),
            Button("Close", id="btn-close-help"), id="help-inner")

    @on(Button.Pressed, "#btn-close-help")
    def on_close(self) -> None:
        self.dismiss()


class StartJobScreen(ModalScreen[None]):
    def __init__(self, worker_id: str, api_request, on_done) -> None:
        super().__init__()
        self.worker_id = worker_id
        self.api_request = api_request
        self.on_done = on_done
        self._submitting = False

    def compose(self) -> ComposeResult:
        yield Vertical(
            Static("Start Job — name and priority note are public", markup=False),
            Input(placeholder="Public name (required)", id="job-name"),
            Input(placeholder="Command (private)", id="job-input"),
            Input(placeholder="Expected seconds (positive, required)", id="job-duration", type="integer"),
            Select([("CPU setup (immediate)", "cpu"), ("GPU work (FIFO)", "gpu")],
                   value="cpu", allow_blank=False, id="job-mode"),
            Input(placeholder="GPU count (GPU mode only)", value="1", type="integer", id="job-gpus"),
            Select([(p, p) for p in ("flexible", "normal", "blocking", "time_critical")],
                   value="normal", allow_blank=False, id="job-priority"),
            Input(placeholder="Public priority note (required for time_critical)", id="job-note"),
            Input(placeholder="Deadline UTC epoch seconds (optional)", id="job-deadline", type="integer"),
            Static("", id="start-error", markup=False),
            Horizontal(Button("Start", id="btn-do-start", variant="success"),
                       Button("Cancel", id="btn-do-cancel", variant="error")), id="start-inner")

    def on_mount(self) -> None:
        self.query_one("#job-name", Input).focus()

    @on(Button.Pressed, "#btn-do-start")
    async def on_start(self) -> None:
        if self._submitting:
            return
        value = lambda selector: self.query_one(selector, Input).value
        try:
            name, command = value("#job-name").strip(), value("#job-input")
            duration = int(value("#job-duration"))
            gpu_mode = self.query_one("#job-mode", Select).value == "gpu"
            gpu_count = int(value("#job-gpus")) if gpu_mode else 0
            priority = self.query_one("#job-priority", Select).value
            note = value("#job-note")
            deadline = int(value("#job-deadline")) if value("#job-deadline") else None
            if not name or not command.strip() or duration <= 0 or (gpu_mode and gpu_count <= 0):
                raise ValueError("Enter a public name, command, positive duration and GPU count when queued")
            if priority == "time_critical" and not note.strip():
                raise ValueError("time_critical requires a public priority note")
            payload = dict(worker_id=self.worker_id, name=name, command=command,
                           expected_seconds=duration, priority=priority, priority_note=note,
                           deadline_at=deadline, no_pty=True)
            if gpu_mode:
                payload["gpu_count"] = gpu_count
            self._submitting = True
            self.query_one("#btn-do-start", Button).disabled = True
            job = await self.api_request("POST", "/api/v1/jobs/queue" if gpu_mode else "/api/v1/jobs", json=payload)
        except (ValueError, RuntimeError) as exc:
            self.query_one("#start-error", Static).update(str(exc))
            self._submitting = False
            self.query_one("#btn-do-start", Button).disabled = False
            return
        self.on_done(job)
        self.dismiss()

    @on(Button.Pressed, "#btn-do-cancel")
    def on_cancel(self) -> None:
        if not self._submitting:
            self.dismiss()


class MainScreen(Screen[None]):
    BINDINGS = [Binding("j", "cursor_down", show=False), Binding("k", "cursor_up", show=False),
                Binding("n", "new_job", "New Job"), Binding("s", "stop_job", "Stop own"),
                Binding("l", "show_log", "Own log"), Binding("r", "refresh", "Refresh"),
                Binding("?", "toggle_help", "Help"), Binding("q", "quit", "Quit"),
                Binding("c", "clear_detail", "Clear", show=False)]

    def __init__(self, api_request) -> None:
        super().__init__()
        self.api_request = api_request
        self._workers = {}
        self._jobs = {}
        self._owned = {}
        self._selected_worker_id = None
        self._selected_job_id = None
        self._loading = False

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal():
            with Vertical(id="sidebar"):
                yield Static("WORKERS")
                yield DataTable(id="worker-table", cursor_type="row")
            with Vertical(id="main-area"):
                yield Static("Select a worker", id="detail-text", markup=False)
                yield Static("SHARED BOARD + YOUR HISTORY")
                yield DataTable(id="jobs-table", cursor_type="row")
        yield Footer()

    async def on_mount(self) -> None:
        self.query_one("#worker-table", DataTable).add_columns("Status", "Name", "GPUs", "CPUs")
        self.query_one("#jobs-table", DataTable).add_columns("Position", "Status", "Owner", "Public name", "GPUs", "Priority", "Runtime", "ETA")
        self.set_interval(5, self.action_refresh)
        await self.action_refresh()
        self.query_one("#worker-table", DataTable).focus()

    async def action_refresh(self) -> None:
        if self._loading:
            return
        self._loading = True
        try:
            workers = await self.api_request("GET", "/api/v1/workers")
            self._workers = {row["id"]: row for row in workers}
            table = self.query_one("#worker-table", DataTable)
            table.clear()
            for row in workers:
                table.add_row(*(Text(str(row[key])) for key in ("status", "name", "gpu_count", "cpu_cores")), key=row["id"])
            if self._selected_worker_id:
                await self._load_jobs(self._selected_worker_id)
        except RuntimeError as exc:
            self._clear_private()
            self.app.notify(str(exc), severity="error")
        finally:
            self._loading = False

    def _clear_private(self) -> None:
        self._owned.clear()
        self._jobs.clear()
        self._selected_job_id = None
        self.query_one("#jobs-table", DataTable).clear()
        self.query_one("#detail-text", Static).update("No private job data loaded")

    async def _load_jobs(self, worker_id: str) -> None:
        try:
            board = await self.api_request("GET", "/api/v1/jobs/queue", params={"worker_id": worker_id})
            owned = await self.api_request("GET", "/api/v1/jobs", params={"worker_id": worker_id})
        except RuntimeError:
            self._clear_private()
            raise
        self._owned = {row["id"]: row for row in owned}
        rows = [job for worker in board["workers"] for job in worker["jobs"] + worker["cpu_jobs"]]
        shared_ids = {row["id"] for row in rows}
        rows += [row for row in owned if row["id"] not in shared_ids]
        self._jobs = {row["id"]: row for row in rows}
        table = self.query_one("#jobs-table", DataTable)
        table.clear()
        for row in rows:
            eta = (f"{_timestamp(row.get('estimated_start_at'))} → {_timestamp(row.get('estimated_finish_at'))}"
                   if row.get("estimate_status") == "known" else row.get("estimate_explanation", "not queued"))
            values = (row.get("position", "-"), row.get("execution_state") if row.get("stop_requested_at") else row["status"],
                      row["owner_display_name"], row.get("name") or "-", row["gpu_count"], row["priority"],
                      f"{row['expected_seconds']}s" if row.get("expected_seconds") else "unknown", eta)
            table.add_row(*(Text(str(value)) for value in values), key=row["id"])

    @on(DataTable.RowSelected, "#worker-table")
    async def on_worker_selected(self, event: DataTable.RowSelected) -> None:
        worker_id = event.row_key.value
        if worker_id not in self._workers:
            return
        self._selected_worker_id = worker_id
        row = self._workers[worker_id]
        self.query_one("#detail-text", Static).update(
            f"{row['name']} — {row['status']}\nCPUs: {row['cpu_cores']}; GPUs: {', '.join(row['gpu_names']) or 'none'}")
        try:
            await self._load_jobs(worker_id)
        except RuntimeError as exc:
            self.app.notify(str(exc), severity="error")
        self.query_one("#jobs-table", DataTable).focus()

    @on(DataTable.RowSelected, "#jobs-table")
    async def on_job_selected(self, event: DataTable.RowSelected) -> None:
        job_id = event.row_key.value
        if job_id not in self._jobs:
            return
        self._selected_job_id = job_id
        row = self._jobs[job_id]
        detail = f"{row.get('name') or 'Job'} — {row['status']}\nOwner: {row['owner_display_name']}"
        if job_id in self._owned:
            try:
                private = await self.api_request("GET", f"/api/v1/jobs/{quote(job_id, safe='')}")
            except RuntimeError as exc:
                self._clear_private()
                self.app.notify(str(exc), severity="error")
                return
            self._owned[job_id] = private
            detail += f"\nCommand: {private['command']}\nPress l for logs / s to stop"
            if private.get("termination_reason"):
                detail += f"\nStop reason: {private['termination_reason']}"
            history = private.get("delay_history", [])
        else:
            detail += "\nShared metadata only; logs and stop are owner-only"
            history = row.get("delay_history", [])
        detail += "\n" + "\n".join(delay_messages(history))
        self.query_one("#detail-text", Static).update(detail)

    def action_new_job(self) -> None:
        if not self._selected_worker_id:
            self.app.notify("Select a worker first", severity="warning")
            return
        self.app.push_screen(StartJobScreen(self._selected_worker_id, self.api_request, self._on_job_done))

    def _on_job_done(self, job) -> None:
        self.app.notify(f"Job {job['status']}: {job['id']}")
        self.run_worker(self.action_refresh())

    def _owned_selection(self):
        if self._selected_job_id not in self._owned:
            self.app.notify("Select one of your own jobs; shared rows cannot be stopped or logged", severity="warning")
            return None
        return self._selected_job_id

    async def action_stop_job(self) -> None:
        job_id = self._owned_selection()
        if job_id is None:
            return
        try:
            await self.api_request("DELETE", f"/api/v1/jobs/{quote(job_id, safe='')}")
            self.app.notify("Job stopped (termination confirmed)")
            await self.action_refresh()
        except RuntimeError as exc:
            self.app.notify(str(exc), severity="error")

    async def action_show_log(self) -> None:
        job_id = self._owned_selection()
        if job_id is None:
            return
        try:
            data = await self.api_request("GET", f"/api/v1/jobs/{quote(job_id, safe='')}/logs", params={"tail": 40})
            self.query_one("#detail-text", Static).update(f"Log: {job_id}\n{data['logs'] or 'No log yet'}")
        except RuntimeError as exc:
            self._clear_private()
            self.app.notify(str(exc), severity="error")

    def action_cursor_down(self) -> None:
        if isinstance(self.focused, DataTable):
            self.focused.action_cursor_down()

    def action_cursor_up(self) -> None:
        if isinstance(self.focused, DataTable):
            self.focused.action_cursor_up()

    def action_clear_detail(self) -> None:
        self._selected_worker_id = None
        self._clear_private()

    def action_toggle_help(self) -> None:
        self.app.push_screen(HelpScreen())


class WorkerHarnessTUI(App):
    CSS = """
    Screen { background: $surface; }
    #sidebar { width: 36; border: solid $primary; }
    #worker-table { height: 1fr; border: none; }
    #main-area { width: 1fr; }
    #detail-text { height: 12; border: solid $accent; padding: 1 2; overflow-y: auto; }
    #jobs-table { height: 1fr; border: solid $accent; }
    #help-inner, #start-inner {
        align: center middle; width: 76; height: auto;
        background: $panel; border: thick $primary; padding: 2;
    }
    """
    BINDINGS = [Binding("q", "quit", "Quit")]

    def __init__(self, api_request=request, **kwargs):
        super().__init__(**kwargs)
        self.api_request = api_request

    def on_mount(self) -> None:
        self.push_screen(MainScreen(self.api_request))


def run_tui(api_request=request) -> None:
    WorkerHarnessTUI(api_request).run()
