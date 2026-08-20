"""Pi session registry and prompt commands."""

from __future__ import annotations

import asyncio
import functools
import hashlib
import json
import logging
import os
import shutil
import signal
import socket
import subprocess
import sys
from pathlib import Path
from urllib.parse import quote

import httpx
import typer
from rich.cells import set_cell_size
from rich.console import Console
from rich.table import Table

from worker_harness.agents import pick_agent, row_agent, validate_agent

app = typer.Typer(help="Inspect and message registered Pi sessions")
console = Console()
logger = logging.getLogger(__name__)

_ACTIVE_STATES = {"working", "idle"}
_PICKER_CONTEXT_WIDTH = 28
_PICKER_NAME_WIDTH = 16
_PICKER_TYPE = {
    "interactive": "I",
    "delegated": "D",
    "global-router": "G",
}
_PICKER_TEXT_TRANSLATION = str.maketrans({"\t": " ", "\n": " ", "\r": " "})


def _parse_tailnet_dns_labels(status: dict) -> dict[str, str]:
    """Map Tailnet IPs to their short MagicDNS labels."""

    current_tailnet = status.get("CurrentTailnet")
    suffix = str(status.get("MagicDNSSuffix") or (
        current_tailnet.get("MagicDNSSuffix")
        if isinstance(current_tailnet, dict)
        else ""
    ) or "").strip(".")
    peers = status.get("Peer")
    nodes = [status.get("Self")]
    if isinstance(peers, dict):
        nodes.extend(peers.values())
    labels: dict[str, str] = {}
    host_priorities: dict[str, int] = {}
    for node in nodes:
        if not isinstance(node, dict):
            continue
        dns_name = str(node.get("DNSName") or "").strip().rstrip(".")
        if not dns_name:
            continue
        suffix_marker = f".{suffix}" if suffix else ""
        short = (
            dns_name[: -len(suffix_marker)]
            if suffix_marker and dns_name.casefold().endswith(suffix_marker.casefold())
            else dns_name
        )
        for address in node.get("TailscaleIPs") or []:
            if address:
                labels[str(address)] = short
        host_name = str(node.get("HostName") or "").strip().casefold()
        if host_name:
            # Interactive hosts should prefer their ordinary Tailnet node over
            # a separately tagged worker identity with the same OS hostname.
            priority = 0 if not node.get("Tags") else 1
            host_key = f"host:{host_name}"
            if priority < host_priorities.get(host_key, 99):
                labels[host_key] = short
                host_priorities[host_key] = priority
    return labels


@functools.lru_cache(maxsize=1)
def _tailnet_dns_labels() -> dict[str, str]:
    """Best-effort local Tailnet inventory; picker operation never depends on it."""

    tailscale = shutil.which("tailscale")
    if not tailscale:
        return {}
    try:
        result = subprocess.run(
            [tailscale, "status", "--json"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=2,
            check=False,
        )
        if result.returncode != 0:
            return {}
        payload = json.loads(result.stdout)
    except (OSError, subprocess.TimeoutExpired, json.JSONDecodeError):
        return {}
    return _parse_tailnet_dns_labels(payload)


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
    agent: str | None = typer.Option(None, "--agent", help="Only pi or omp sessions; both when omitted"),
):
    """List sessions registered with the orchestrator."""

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
        table.add_column("Type")
        table.add_column("Agent")
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
                row_agent(row),
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


def _attach_candidates(
    rows: list[dict],
    workers: list[dict] | None = None,
    *,
    local_host: str | None = None,
    tailnet_dns_by_ip: dict[str, str] | None = None,
) -> list[dict]:
    """Return active sessions in global/local/remote/delegated picker order."""

    worker_records = {
        str(worker.get("id") or ""): worker
        for worker in workers or []
    }
    tailnet_dns_by_ip = tailnet_dns_by_ip or {}
    local = (local_host or socket.gethostname()).casefold()
    state_order = {"working": 0, "idle": 1}
    candidates: list[dict] = []
    for source in rows:
        if source.get("state") not in _ACTIVE_STATES:
            continue
        row = dict(source)
        session_type = str(row.get("session_type") or "")
        host = str(row.get("host") or "").strip()
        tailnet_host_name = host
        tailnet_address = str(row.get("terminal_host") or "").strip()
        if session_type == "global-router":
            rank, label = 0, "Global"
        elif session_type == "delegated":
            worker_id = str(row.get("worker_id") or "").strip()
            worker = worker_records.get(worker_id) or {}
            machine = str(worker.get("name") or worker_id or "Unknown worker")
            tailnet_host_name = machine
            tailnet_address = str(worker.get("worker_ip") or "").strip()
            rank, label = 3, f"Delegated · {machine}"
        elif host and host.casefold() == local:
            rank, label = 1, f"Local · {host}"
        else:
            machine = host or "Unknown machine"
            rank, label = 2, machine
        tailnet_dns = (
            tailnet_dns_by_ip.get(tailnet_address, "")
            or tailnet_dns_by_ip.get(f"host:{tailnet_host_name.casefold()}", "")
        )
        row.update({
            "_machine_rank": rank,
            "_machine_label": label,
            "_machine_dns": tailnet_dns,
            "_machine_group": f"{rank}:{label.casefold()}",
        })
        candidates.append(row)
    candidates.sort(key=lambda row: (
        int(row["_machine_rank"]),
        str(row["_machine_label"]).casefold(),
        state_order.get(str(row.get("state")), 9),
        -int(row.get("updated_at") or 0),
        str(row.get("id") or ""),
    ))
    for index, row in enumerate(candidates):
        group = str(row["_machine_group"])
        row["_machine_first"] = (
            index == 0 or str(candidates[index - 1]["_machine_group"]) != group
        )
        row["_machine_last"] = (
            index == len(candidates) - 1
            or str(candidates[index + 1]["_machine_group"]) != group
        )
    return candidates


async def _candidate_inventory(agent: str | None = None) -> list[dict]:
    rows = _filter_agent(await _request("GET", "/api/v1/pi/sessions?include_attach_info=true"), agent)
    workers: list[dict] = []
    if any(str(row.get("session_type") or "") == "delegated" for row in rows):
        try:
            workers = await _request("GET", "/api/v1/workers")
        except RuntimeError:
            pass
    return _attach_candidates(
        rows,
        workers,
        tailnet_dns_by_ip=_tailnet_dns_labels(),
    )


def _filter_agent(rows: list[dict], agent: str | None) -> list[dict]:
    if not agent:
        return rows
    validate_agent(agent)
    return [row for row in rows if row_agent(row) == agent]


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
    """Filter one server-generated attachment snapshot without request fanout."""

    missing = [str(row.get("id") or "") for row in rows if not isinstance(row.get("attach_info"), dict)]
    if missing:
        if len(missing) == len(rows):
            raise RuntimeError(
                "orchestrator does not support batched attachment inventory; "
                "update the orchestrator to Worker Harness main@1751e74 or newer"
            )
        raise RuntimeError(
            "session inventory omitted attachment information for "
            + ", ".join(session_id[:12] or "<unknown>" for session_id in missing)
        )
    return [row for row in rows if row["attach_info"].get("attachable")]


async def _cycle_session(current_session_id: str, direction: str) -> dict:
    inventory = await _candidate_inventory()
    candidates = _cycle_order(inventory, current_session_id, direction)
    available_ids = {
        str(row.get("id") or "")
        for row in await _attachable_candidates(inventory)
    }
    for candidate in candidates:
        if str(candidate.get("id") or "") in available_ids:
            return candidate
    raise RuntimeError("no other attachable Pi sessions are registered")


def _zellij_marker_path(session_name: str, pane_id: str) -> Path:
    runtime = os.environ.get("XDG_RUNTIME_DIR") or f"/tmp/worker-harness-{os.getuid()}"
    digest = hashlib.sha256(f"{session_name}\0{pane_id}".encode()).hexdigest()
    return Path(runtime) / "worker-harness" / "zellij-attachments" / f"{digest}.json"


def _mark_attach_pane(session_id: str | None) -> None:
    """Expose streamed attachment state to tmux/Zellij shortcut helpers."""

    pane = os.environ.get("TMUX_PANE")
    if pane:
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

    zellij_session = os.environ.get("ZELLIJ_SESSION_NAME", "")
    zellij_pane = os.environ.get("ZELLIJ_PANE_ID", "")
    if not zellij_session or not zellij_pane:
        return
    from worker_harness.pi_zellij import mark_current_attachment, unmark_attachment

    normalized_zellij_pane = (
        zellij_pane if zellij_pane.startswith("terminal_") else f"terminal_{zellij_pane}"
    )
    marker = _zellij_marker_path(zellij_session, normalized_zellij_pane)
    try:
        previous = json.loads(marker.read_text(encoding="utf8"))
    except (OSError, json.JSONDecodeError):
        previous = {}
    previous_session = str(previous.get("session_id") or "")
    previous_pid = int(previous.get("pid") or 0)
    if not session_id:
        if previous_pid not in {0, os.getpid()}:
            return
        if previous_session:
            unmark_attachment(previous_session, pid=os.getpid())
        marker.unlink(missing_ok=True)
        return
    if previous_session and previous_session != session_id:
        unmark_attachment(previous_session, pid=previous_pid)
    marker.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = marker.with_suffix(f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps({
        "session_id": session_id,
        "pid": os.getpid(),
        "mode": "stream",
    }), encoding="utf8")
    temporary.chmod(0o600)
    temporary.replace(marker)
    mark_current_attachment(session_id)


def _zellij_cycle_source_panes() -> list[str]:
    """Return the original pane(s) suppressed by an in-place shortcut helper."""

    session_name = os.environ.get("ZELLIJ_SESSION_NAME", "")
    pane_id = os.environ.get("ZELLIJ_PANE_ID", "")
    if not session_name or not pane_id:
        return []
    result = subprocess.run(
        ["zellij", "action", "list-panes", "--json", "--all"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        timeout=3,
        check=False,
    )
    if result.returncode != 0:
        return []
    try:
        panes = json.loads(result.stdout)
    except json.JSONDecodeError:
        return []
    current = next((pane for pane in panes if (
        not pane.get("is_plugin") and str(pane.get("id")) == pane_id
    )), None)
    if not current:
        return []
    candidates = [pane for pane in panes if (
        not pane.get("is_plugin")
        and pane.get("is_suppressed")
        and pane.get("tab_id") == current.get("tab_id")
    )]
    return [f"terminal_{pane.get('id')}" for pane in reversed(candidates)]


async def _zellij_cycle_origin() -> tuple[str, int | None]:
    from worker_harness.pi_terminal import _relay_request

    session_name = os.environ.get("ZELLIJ_SESSION_NAME", "")
    for pane_id in _zellij_cycle_source_panes():
        marker = _zellij_marker_path(session_name, pane_id)
        try:
            payload = json.loads(marker.read_text(encoding="utf8"))
            session_id = str(payload.get("session_id") or "")
            pid = int(payload.get("pid") or 0)
            if session_id and pid > 0:
                return session_id, pid
        except (OSError, ValueError, json.JSONDecodeError):
            pass
        try:
            located = await _relay_request({
                "action": "locate",
                "multiplexer": "zellij",
                "zellij_session_name": session_name,
                "zellij_pane_id": pane_id,
            })
        except (OSError, asyncio.TimeoutError, RuntimeError, json.JSONDecodeError):
            continue
        session_id = str(located.get("session_id") or "")
        if session_id:
            return session_id, None
    raise RuntimeError("current Zellij pane is not a Worker Harness Pi attachment")


def _pick_session(rows: list[dict]) -> dict:
    if not rows:
        raise RuntimeError("no working or idle Pi sessions are registered")
    if any("_machine_rank" not in row for row in rows):
        rows = _attach_candidates(rows)
    fzf = shutil.which("fzf")
    if not fzf:
        if len(rows) == 1:
            return rows[0]
        raise RuntimeError("fzf is required when no session ID is supplied")
    from worker_harness.pi_zellij_state import state_glyph

    by_id = {str(row.get("id")): row for row in rows}
    lines = []
    for row in rows:
        session_id = str(row.get("id", ""))
        state = str(row.get("state") or "")
        session_type = str(row.get("session_type") or "")
        label = str(row.get("name") or row.get("task") or "-").translate(
            _PICKER_TEXT_TRANSLATION
        )
        cwd = str(row.get("cwd") or "-").translate(_PICKER_TEXT_TRANSLATION)
        machine = str(row.get("_machine_label") or "Unknown machine").translate(
            _PICKER_TEXT_TRANSLATION
        )
        tailnet_dns = str(row.get("_machine_dns") or "").translate(
            _PICKER_TEXT_TRANSLATION
        )
        machine_context = machine + (f"  @{tailnet_dns}" if tailnet_dns else "")
        dim_context = (
            "\x1b[2m"
            + set_cell_size(machine_context, _PICKER_CONTEXT_WIDTH)
            + "\x1b[0m"
        )
        branch = "└─" if row.get("_machine_last") else "├─"
        child = "  " + branch + " " + "   ".join((
            state_glyph(state),
            _PICKER_TYPE.get(session_type, "?"),
            set_cell_size(row_agent(row), 4),
            dim_context,
            set_cell_size(label, _PICKER_NAME_WIDTH),
            cwd,
        ))
        display = f"{machine_context}\n{child}" if row.get("_machine_first") else child
        lines.append("\t".join((session_id, display)))
    header = "      " + "   ".join((
        "S",
        "T",
        set_cell_size("AGT", 4),
        set_cell_size("MACHINE @TAILNET", _PICKER_CONTEXT_WIDTH),
        set_cell_size("NAME", _PICKER_NAME_WIDTH),
        "PATH",
    ))
    command = [
        fzf,
        "--no-tmux",
        "--height=100%",
        "--layout=reverse",
        "--border",
        "--sync",
        "--no-hscroll",
        "--ansi",
        "--read0",
        "--print0",
        "--delimiter=\\t",
        "--with-nth=2",
        # fzf applies --nth after --with-nth. Search the one transformed
        # display field; combining original field indexes here matches nothing.
        "--nth=1",
        f"--header={header}",
        "--prompt=Pi session> ",
    ]
    local_position = next((
        index + 1 for index, row in enumerate(rows)
        if int(row.get("_machine_rank") or -1) == 1
    ), 1)
    if local_position > 1:
        command.append(f"--bind=load:pos({local_position})")
    result = subprocess.run(
        command,
        input="\0".join(lines) + "\0",
        text=True,
        stdout=subprocess.PIPE,
        check=False,
    )
    selected_record = result.stdout.rstrip("\0")
    if result.returncode == 130 or not selected_record:
        raise typer.Abort()
    if result.returncode != 0:
        raise RuntimeError(f"fzf session picker failed with exit code {result.returncode}")
    session_id = selected_record.split("\t", 1)[0]
    if session_id not in by_id:
        raise RuntimeError("fzf returned an unknown Pi session")
    return by_id[session_id]


async def _run_attach_loop(
    selected: dict,
    *,
    initial_websocket_url: str | None = None,
    zellij_tab: bool = False,
    tmux_window: bool = False,
) -> None:
    """Run the native attachment/cycling loop for one selected session."""

    from worker_harness.pi_terminal import (
        TerminalRelayUnavailable,
        attach_terminal,
        focus_local_zellij_session,
    )
    from worker_harness.pi_tmux import current_attachment_window, update_attachment_window
    from worker_harness.pi_zellij import current_tab_context, rename_tab
    from worker_harness.pi_zellij_state import watch_session_state

    cycle_requests: asyncio.Queue[str] = asyncio.Queue()
    loop = asyncio.get_running_loop()
    installed_signals: list[signal.Signals] = []
    for signum, direction in ((signal.SIGUSR1, "next"), (signal.SIGUSR2, "previous")):
        try:
            loop.add_signal_handler(signum, cycle_requests.put_nowait, direction)
            installed_signals.append(signum)
        except (NotImplementedError, RuntimeError):
            pass

    reconnect_attempts = 0
    try:
        while True:
            session_id = str(selected.get("id") or "")
            name = str(selected.get("name") or selected.get("task") or "Pi")
            _mark_attach_pane(session_id)
            # A Zellij source in this same immediate Zellij client must be
            # focused directly to avoid recursively rendering that client.
            # Tmux sources always stream, including on their source host.
            if await focus_local_zellij_session(session_id):
                return
            state_watcher: asyncio.Task[None] | None = None
            update_state = None
            if zellij_tab and (tab_context := current_tab_context()) is not None:
                tab_id, _pane_id = tab_context

                async def update_tab(state: str, *, target_tab: int = tab_id, label: str = name) -> None:
                    await asyncio.to_thread(rename_tab, target_tab, label, state)

                update_state = update_tab
            elif tmux_window and (window_id := current_attachment_window()) is not None:

                async def update_window(
                    state: str,
                    *,
                    target_window: str = window_id,
                    label: str = name,
                ) -> None:
                    await asyncio.to_thread(
                        update_attachment_window,
                        label,
                        state,
                        target_window,
                    )

                update_state = update_window
            if update_state is not None:
                state_watcher = asyncio.create_task(
                    watch_session_state(
                        _base_url(),
                        session_id,
                        str(selected.get("state") or "disconnected"),
                        update_state,
                    ),
                    name=f"pi-window-state-{session_id[:12]}",
                )
            try:
                if initial_websocket_url is not None:
                    websocket_url = initial_websocket_url
                    gateway_websocket_url = None
                    initial_websocket_url = None
                else:
                    info = await _request(
                        "GET", f"/api/v1/pi/sessions/{quote(session_id, safe='')}/attach-info"
                    )
                    if not info.get("attachable"):
                        raise RuntimeError(str(info.get("reason") or "Pi session is not attachable"))
                    if int(info.get("protocol_version") or 0) != 2:
                        raise RuntimeError(
                            f"unsupported Pi terminal protocol {info.get('protocol_version')!r}; expected 2"
                        )
                    websocket_url = str(
                        info.get("direct_websocket_url") or info.get("websocket_url") or ""
                    )
                    gateway_websocket_url = str(info.get("gateway_websocket_url") or "") or None
                try:
                    direction = await attach_terminal(
                        websocket_url,
                        fallback_websocket_url=gateway_websocket_url,
                        cycle_requests=cycle_requests,
                    )
                except TerminalRelayUnavailable as exc:
                    if update_state is None or reconnect_attempts >= 4:
                        raise
                    reconnect_attempts += 1
                    logger.warning(
                        "Pi attachment %s reconnect %d/4 after %s",
                        session_id,
                        reconnect_attempts,
                        exc,
                    )
                    if update_state is not None:
                        await update_state("disconnected")
                    await asyncio.sleep(min(0.5 * (2 ** (reconnect_attempts - 1)), 4.0))
                    continue
            finally:
                if state_watcher is not None:
                    state_watcher.cancel()
                    await asyncio.gather(state_watcher, return_exceptions=True)
            reconnect_attempts = 0
            if direction is None:
                return
            if direction == "select":
                _mark_attach_pane(None)
                selected = _pick_session(await _attachable_candidates(await _candidate_inventory()))
                continue
            selected = await _cycle_session(session_id, direction)
    finally:
        _mark_attach_pane(None)
        for signum in installed_signals:
            loop.remove_signal_handler(signum)


async def _open_in_zellij(selected: dict, *, loopback: bool = False) -> None:
    """Focus a plain local Zellij source or open/reuse an attachment tab."""

    from worker_harness.pi_terminal import focus_local_zellij_session
    from worker_harness.pi_zellij import open_or_focus_attachment_tab

    session_id = str(selected.get("id") or "")
    if await focus_local_zellij_session(session_id):
        return
    await asyncio.to_thread(open_or_focus_attachment_tab, selected, loopback=loopback)


@app.command(
    "start",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def start(
    ctx: typer.Context,
    name: str | None = typer.Option(None, "--name", "-n", help="Human-facing Pi session name"),
    attach_after_start: bool = typer.Option(
        True,
        "--attach/--no-attach",
        help="Attach this terminal after the managed Pi route is ready",
    ),
    timeout: float = typer.Option(
        10.0,
        "--timeout",
        min=0.1,
        help="Seconds to wait for the local Pi terminal route",
    ),
    agent: str | None = typer.Option(None, "--agent", help="Agent to launch: pi or omp; asks when omitted"),
):
    """Start a new agent session in the hidden managed tmux backend."""

    from worker_harness.pi_runtime import (
        ensure_managed_route,
        local_relay_websocket_url,
        start_managed_pi,
    )

    try:
        agent = pick_agent(agent)
        size = shutil.get_terminal_size(fallback=(80, 24))
        managed = start_managed_pi(
            name=name,
            pi_args=list(ctx.args),
            rows=size.lines,
            cols=size.columns,
            agent=agent,
        )

        async def run() -> None:
            session, _route = await ensure_managed_route(managed, timeout=timeout)
            if not attach_after_start:
                result = {
                    "session_id": session.session_id,
                    "name": session.name,
                    "tmux_socket": str(session.tmux_socket),
                    "tmux_pane_id": session.tmux_pane_id,
                }
                if _output_mode() == "json":
                    console.print(json.dumps(result, indent=2))
                else:
                    console.print(
                        f"[green]Started {agent}[/] {session.name} "
                        f"([dim]{session.session_id}[/])"
                    )
                return
            selected = {"id": session.session_id, "name": session.name, "state": "idle"}
            from worker_harness.pi_zellij import is_immediate_zellij
            if is_immediate_zellij():
                await _open_in_zellij(selected, loopback=True)
                return
            await _run_attach_loop(
                selected,
                initial_websocket_url=local_relay_websocket_url(session.session_id),
            )

        asyncio.run(run())
    except typer.Abort:
        raise
    except (RuntimeError, KeyboardInterrupt) as exc:
        console.print(f"[red]{exc or 'Pi startup interrupted'}[/]")
        raise typer.Exit(1) from exc


@app.command("history-list", hidden=True)
def history_list(
    cwd: str = typer.Option(..., "--cwd", help="Absolute target working directory"),
):
    """Return bounded target-local Pi SessionManager history metadata."""

    try:
        from worker_harness.pi_history import list_session_history

        sys.stdout.write(json.dumps(list_session_history(cwd), separators=(",", ":")) + "\n")
    except RuntimeError as exc:
        console.print(f"[red]{exc}[/]")
        raise typer.Exit(1) from exc


@app.command(
    "resume",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def resume(
    ctx: typer.Context,
    session_id: str = typer.Argument(..., help="Exact target-local Pi session ID"),
    cwd: str = typer.Option(..., "--cwd", help="Absolute working directory containing the history"),
    name: str | None = typer.Option(None, "--name", "-n", help="Window name override"),
    attach_after_start: bool = typer.Option(
        True,
        "--attach/--no-attach",
        help="Attach after the resumed route is ready",
    ),
    timeout: float = typer.Option(
        10.0,
        "--timeout",
        min=0.1,
        help="Seconds to wait for the local Pi terminal route",
    ),
):
    """Resume one inactive, exact target-local Pi history safely."""

    async def run() -> dict:
        from worker_harness.pi_history import resolve_session_history
        from worker_harness.pi_runtime import resume_managed_pi, wait_for_managed_route

        rows = await _request("GET", "/api/v1/pi/sessions")
        if any(
            str(row.get("id") or "") == session_id
            and str(row.get("state") or "") in _ACTIVE_STATES
            for row in rows
        ):
            raise RuntimeError(f"Pi session {session_id} is already active")
        history = await asyncio.to_thread(resolve_session_history, cwd, session_id)
        history_name = str(history.get("name") or "").strip()
        display_name = (name or history_name or Path(cwd).name or "Pi").strip()
        terminal_size = shutil.get_terminal_size(fallback=(80, 24))
        managed = await asyncio.to_thread(
            resume_managed_pi,
            session_id=session_id,
            name=display_name,
            cwd=Path(cwd),
            pi_args=list(ctx.args),
            rows=terminal_size.lines,
            cols=terminal_size.columns,
        )
        await wait_for_managed_route(managed, timeout=timeout)
        result = {
            "session_id": managed.session_id,
            "name": managed.name,
            "tmux_socket": str(managed.tmux_socket),
            "tmux_pane_id": managed.tmux_pane_id,
            "resumed": True,
        }
        if attach_after_start:
            selected = {
                "id": managed.session_id,
                "name": managed.name,
                "state": "idle",
            }
            from worker_harness.pi_runtime import local_relay_websocket_url
            from worker_harness.pi_zellij import is_immediate_zellij

            if is_immediate_zellij():
                await _open_in_zellij(selected, loopback=True)
            else:
                await _run_attach_loop(
                    selected,
                    initial_websocket_url=local_relay_websocket_url(managed.session_id),
                )
        return result

    try:
        result = asyncio.run(run())
        if not attach_after_start:
            if _output_mode() == "json":
                console.print(json.dumps(result, indent=2))
            else:
                console.print(
                    f"[green]Resumed Pi[/] {result['name']} "
                    f"([dim]{result['session_id']}[/])"
                )
    except typer.Abort:
        raise
    except (RuntimeError, KeyboardInterrupt) as exc:
        console.print(f"[red]{exc or 'Pi resume interrupted'}[/]")
        raise typer.Exit(1) from exc


@app.command("attach")
def attach(
    target: str | None = typer.Argument(None, help="Session ID, unique ID prefix, or exact session name"),
    stream: bool = typer.Option(
        False,
        "--stream",
        help="Compatibility flag; tmux sessions always stream",
    ),
    relative: str | None = typer.Option(
        None,
        "--relative",
        help="Select the next or previous attachable session relative to TARGET",
        hidden=True,
    ),
    agent: str | None = typer.Option(
        None,
        "--agent",
        help="Restrict discovery to pi or omp sessions; both when omitted",
    ),
    here: bool = typer.Option(False, "--here", hidden=True),
    loopback: bool = typer.Option(False, "--loopback", hidden=True),
    session_name: str | None = typer.Option(None, "--session-name", hidden=True),
    session_state: str | None = typer.Option(None, "--session-state", hidden=True),
    tmux_picker: bool = typer.Option(False, "--tmux-picker", hidden=True),
    tmux_child: bool = typer.Option(False, "--tmux-child", hidden=True),
    tmux_target_session: str | None = typer.Option(
        None, "--tmux-target-session", hidden=True
    ),
    tmux_target_client: str | None = typer.Option(
        None, "--tmux-target-client", hidden=True
    ),
):
    """Attach this terminal to a discovered Pi session; press Ctrl-] to detach."""

    # Tmux popup format expansion varies across supported client versions.
    # Prefer explicit CLI values for compatibility, but let the popup inject its
    # authoritative invoking session/client as environment entries so `$N`
    # session IDs never pass through an intermediate shell expansion.
    picker_target_session = tmux_target_session or os.environ.get(
        "WH_TMUX_TARGET_SESSION"
    )
    picker_target_client = tmux_target_client or os.environ.get(
        "WH_TMUX_TARGET_CLIENT"
    )

    async def run() -> None:
        from worker_harness.pi_zellij import is_immediate_zellij

        _ = stream  # retained for compatibility with existing shortcuts/scripts
        if tmux_picker:
            if tmux_child or relative or here or loopback:
                raise RuntimeError("--tmux-picker cannot be combined with attachment child modes")
            if picker_target_session is None or picker_target_client is None:
                raise RuntimeError("--tmux-picker requires its invoking tmux session and client")
            from worker_harness.pi_tmux import validate_attachment_target

            validated_target_session, validated_target_client = (
                validate_attachment_target(
                    picker_target_session,
                    picker_target_client,
                )
            )
            selected = (
                _resolve_session(await _candidate_inventory(), target)
                if target
                else _pick_session(
                    await _attachable_candidates(await _candidate_inventory(agent))
                )
            )
            from worker_harness.pi_tmux import open_or_focus_attachment_window

            await asyncio.to_thread(
                open_or_focus_attachment_window,
                selected,
                validated_target_session,
                validated_target_client,
            )
            return
        if tmux_child:
            if relative or here or loopback or not target or session_name is None:
                raise RuntimeError("--tmux-child requires one exact session and its name")
            from worker_harness.pi_tmux import current_attachment_window

            if current_attachment_window() is None:
                raise RuntimeError("--tmux-child requires a Worker Harness-owned tmux window")
            selected = {
                "id": target,
                "name": session_name,
                "state": session_state or "disconnected",
            }
        elif relative:
            if relative not in {"next", "previous"} or not target:
                raise RuntimeError("--relative requires TARGET and either next or previous")
            selected = await _cycle_session(target, relative)
        elif here and target and session_name is not None:
            selected = {
                "id": target,
                "name": session_name,
                "state": session_state or "disconnected",
            }
        else:
            selected = (
                _resolve_session(await _candidate_inventory(), target)
                if target
                else _pick_session(
                    await _attachable_candidates(await _candidate_inventory(agent))
                )
            )
        if is_immediate_zellij() and not here:
            await _open_in_zellij(selected)
            return
        initial_url: str | None = None
        if loopback:
            from worker_harness.pi_runtime import local_relay_websocket_url
            from worker_harness.pi_terminal import _relay_request

            session_id = str(selected.get("id") or "")
            await _relay_request({"action": "describe", "session_id": session_id})
            initial_url = local_relay_websocket_url(session_id)
        attach_options = {
            "initial_websocket_url": initial_url,
            "zellij_tab": here and is_immediate_zellij(),
        }
        if tmux_child:
            attach_options["tmux_window"] = True
        await _run_attach_loop(selected, **attach_options)

    try:
        asyncio.run(run())
    except typer.Abort:
        raise
    except (RuntimeError, KeyboardInterrupt) as exc:
        console.print(f"[red]{exc or 'attachment interrupted'}[/]")
        raise typer.Exit(1) from exc


@app.command("cycle", hidden=True)
def cycle(direction: str = typer.Argument(..., help="next or previous")):
    """Cycle from the current Zellij Pi pane or streamed attachment."""

    async def run() -> None:
        if direction not in {"next", "previous"}:
            raise RuntimeError("cycle direction must be next or previous")
        current_session_id, stream_pid = await _zellij_cycle_origin()
        if stream_pid is not None:
            signum = signal.SIGUSR1 if direction == "next" else signal.SIGUSR2
            try:
                os.kill(stream_pid, signum)
            except ProcessLookupError as exc:
                raise RuntimeError("the streamed Pi attachment is no longer running") from exc
            return
        selected = await _cycle_session(current_session_id, direction)
        executable = shutil.which("wh") or sys.argv[0]
        os.execvp(executable, [executable, "attach", str(selected.get("id") or "")])

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
