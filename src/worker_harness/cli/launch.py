"""Launch managed interactive Pi sessions on Tailnet machines."""

from __future__ import annotations

import asyncio
import json
import os
import shlex
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Sequence

import httpx
import typer
from rich.console import Console

from worker_harness.agents import pick_agent, row_agent

console = Console()

_WORKER_TAG = "tag:wh-worker"
_ACTIVE_STATES = {"working", "idle"}
_DIRECTORY_SCRIPT = """set -eu
home=${HOME:?}
printf '%s\\0' "$home"
if [ -d "$home/Dev" ]; then
    for path in "$home"/Dev/*; do
        [ -d "$path" ] && printf '%s\\0' "$path"
    done
fi
"""


@dataclass(frozen=True)
class LaunchMachine:
    """One selectable Tailnet launch target."""

    key: str
    hostname: str
    dns_name: str
    alias: str
    addresses: tuple[str, ...]
    os_name: str
    online: bool
    worker: bool
    local: bool
    ssh_user: str = ""


class RemoteCommandError(RuntimeError):
    """A local or SSH launch command failed."""


def _magicdns_suffix(status: dict[str, Any]) -> str:
    current_tailnet = status.get("CurrentTailnet")
    return str(
        status.get("MagicDNSSuffix")
        or (
            current_tailnet.get("MagicDNSSuffix")
            if isinstance(current_tailnet, dict)
            else ""
        )
        or ""
    ).strip(".")


def _short_dns_name(dns_name: str, suffix: str) -> str:
    dns_name = dns_name.strip().rstrip(".")
    marker = f".{suffix}" if suffix else ""
    if marker and dns_name.casefold().endswith(marker.casefold()):
        return dns_name[: -len(marker)]
    return dns_name


def parse_launch_machines(
    status: dict[str, Any],
    workers: Sequence[dict[str, Any]] = (),
) -> list[LaunchMachine]:
    """Build the standard-then-worker launch inventory from Tailscale status."""

    suffix = _magicdns_suffix(status)
    worker_users = {
        str(worker.get("worker_ip") or ""): str(worker.get("ssh_user") or "")
        for worker in workers
    }
    nodes: list[tuple[dict[str, Any], bool]] = []
    own = status.get("Self")
    if isinstance(own, dict):
        nodes.append((own, True))
    peers = status.get("Peer")
    if isinstance(peers, dict):
        nodes.extend(
            (node, False)
            for node in peers.values()
            if isinstance(node, dict)
        )

    machines: list[LaunchMachine] = []
    for node, local in nodes:
        tags = tuple(str(tag) for tag in (node.get("Tags") or []))
        is_worker = _WORKER_TAG in tags
        if tags and not is_worker:
            # Service identities such as the orchestrator are not launch targets.
            continue
        dns_name = str(node.get("DNSName") or "").strip().rstrip(".")
        addresses = tuple(str(address) for address in (node.get("TailscaleIPs") or []) if address)
        alias = _short_dns_name(dns_name, suffix)
        hostname = str(node.get("HostName") or alias or (addresses[0] if addresses else "Unknown"))
        key = dns_name or (addresses[0] if addresses else hostname)
        if not key:
            continue
        ssh_user = next((worker_users[address] for address in addresses if worker_users.get(address)), "")
        machines.append(LaunchMachine(
            key=key,
            hostname=hostname,
            dns_name=dns_name,
            alias=alias or hostname,
            addresses=addresses,
            os_name=str(node.get("OS") or "unknown"),
            online=True if local else bool(node.get("Online")),
            worker=is_worker,
            local=local,
            ssh_user=ssh_user if is_worker else "",
        ))

    machines.sort(key=lambda machine: (
        1 if machine.worker else 0,
        0 if machine.local else 1,
        0 if machine.online else 1,
        machine.alias.casefold(),
        machine.hostname.casefold(),
        machine.key.casefold(),
    ))
    return machines


def tailscale_launch_machines(workers: Sequence[dict[str, Any]] = ()) -> list[LaunchMachine]:
    """Read the local Tailscale inventory."""

    tailscale = shutil.which("tailscale")
    if not tailscale:
        raise RuntimeError("tailscale is not installed or not on PATH")
    try:
        result = subprocess.run(
            [tailscale, "status", "--json"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError(f"could not read Tailnet machines: {exc}") from exc
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise RuntimeError(f"tailscale status failed: {detail or f'exit {result.returncode}'}")
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError("tailscale status returned malformed JSON") from exc
    return parse_launch_machines(payload, workers)


def resolve_machine(machines: Sequence[LaunchMachine], selector: str) -> LaunchMachine:
    """Resolve an exact alias, DNS name, hostname, or Tailnet IP."""

    selector = selector.strip()
    if not selector or selector.startswith("-") or any(char in selector for char in "\r\n\0"):
        raise RuntimeError("invalid machine selector")
    folded = selector.rstrip(".").casefold()
    matches = [
        machine
        for machine in machines
        if folded in {
            machine.key.rstrip(".").casefold(),
            machine.dns_name.rstrip(".").casefold(),
            machine.alias.casefold(),
            machine.hostname.casefold(),
            *(address.casefold() for address in machine.addresses),
        }
    ]
    if not matches:
        raise RuntimeError(f"no Tailnet machine matches {selector!r}")
    if len(matches) > 1:
        labels = ", ".join(f"@{machine.alias}" for machine in matches)
        raise RuntimeError(f"machine selector {selector!r} is ambiguous: {labels}")
    return matches[0]


def pick_machine(machines: Sequence[LaunchMachine]) -> LaunchMachine:
    """Select a machine with section headings attached to selectable records."""

    if not machines:
        raise RuntimeError("no standard or wh-worker Tailnet machines found")
    fzf = shutil.which("fzf")
    if not fzf:
        raise RuntimeError("fzf is required when --machine is omitted")

    lines: list[str] = []
    local_position: int | None = None
    for index, machine in enumerate(machines):
        first = index == 0 or machines[index - 1].worker != machine.worker
        last = index == len(machines) - 1 or machines[index + 1].worker != machine.worker
        heading = "wh-worker machines" if machine.worker else "Standard machines"
        branch = "└─" if last else "├─"
        status = "●" if machine.online else "?"
        kind = "W" if machine.worker else "I"
        address = machine.addresses[0] if machine.addresses else "-"
        child = (
            f"  {branch} {status}   {kind}   {machine.hostname}   "
            f"@{machine.alias}   {machine.os_name}   {address}"
        )
        display = f"{heading}\n{child}" if first else child
        lines.append(f"{machine.key}\t{display}")
        if machine.local and local_position is None:
            local_position = index + 1

    command = [
        fzf,
        "--height=100%",
        "--layout=reverse",
        "--border",
        "--sync",
        "--no-hscroll",
        "--read0",
        "--print0",
        "--delimiter=\\t",
        "--with-nth=2",
        "--nth=1",
        "--header=      S   T   MACHINE   TAILNET   OS   IP",
        "--prompt=Launch on › ",
    ]
    if local_position is not None:
        command.append(f"--bind=load:pos({local_position})")
    result = subprocess.run(
        command,
        input="\0".join(lines) + "\0",
        text=True,
        stdout=subprocess.PIPE,
        check=False,
    )
    selected = result.stdout.rstrip("\0")
    if result.returncode == 130 or not selected:
        raise typer.Abort()
    if result.returncode != 0:
        raise RuntimeError(f"fzf machine picker failed with exit code {result.returncode}")
    key = selected.split("\t", 1)[0]
    return next(machine for machine in machines if machine.key == key)


def _validate_ssh_component(value: str, label: str) -> str:
    value = value.strip()
    if not value or value.startswith("-") or any(char in value for char in "\r\n\0"):
        raise RuntimeError(f"invalid SSH {label}")
    return value


def ssh_destination(machine: LaunchMachine, ssh_user: str | None = None) -> str:
    """Return an OpenSSH destination, using worker metadata only as fallback."""

    host = _validate_ssh_component(machine.alias or machine.dns_name or machine.key, "host")
    user = (ssh_user or machine.ssh_user).strip()
    if user:
        return f"{_validate_ssh_component(user, 'user')}@{host}"
    return host


def ssh_command(destination: str, remote_command: str, *, connect_timeout: int = 10) -> list[str]:
    ssh = shutil.which("ssh")
    if not ssh:
        raise RuntimeError("ssh is not installed or not on PATH")
    return [
        ssh,
        "-o",
        f"ConnectTimeout={max(1, int(connect_timeout))}",
        "--",
        _validate_ssh_component(destination, "destination"),
        remote_command,
    ]


def _bounded_detail(stdout: str, stderr: str, limit: int = 4000) -> str:
    detail = (stderr.strip() or stdout.strip() or "no diagnostic output")
    return detail[-limit:]


def _run_command(
    command: Sequence[str],
    *,
    timeout: float,
    input_bytes: bytes | None = None,
    cwd: str | None = None,
) -> subprocess.CompletedProcess[bytes]:
    try:
        result = subprocess.run(
            list(command),
            input=input_bytes,
            cwd=cwd,
            stdin=None if input_bytes is not None else subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise RemoteCommandError(f"command timed out after {timeout:g} seconds") from exc
    except OSError as exc:
        raise RemoteCommandError(str(exc)) from exc
    if result.returncode != 0:
        stdout = result.stdout.decode("utf8", errors="replace")
        stderr = result.stderr.decode("utf8", errors="replace")
        raise RemoteCommandError(
            f"command failed with exit code {result.returncode}: "
            f"{_bounded_detail(stdout, stderr)}"
        )
    return result


def _run_ssh_phase(
    destination: str,
    remote_command: str,
    *,
    phase: str,
    timeout: float,
) -> subprocess.CompletedProcess[bytes]:
    """Run one SSH phase with bounded destination/phase/duration diagnostics."""

    started = time.monotonic()
    try:
        return _run_command(
            ssh_command(
                destination,
                remote_command,
                connect_timeout=min(10, max(1, int(timeout))),
            ),
            timeout=timeout,
        )
    except RemoteCommandError as exc:
        raise RemoteCommandError(
            f"SSH destination={destination} phase={phase} "
            f"duration={time.monotonic() - started:.2f}s: {exc}"
        ) from exc


def list_working_directories(
    machine: LaunchMachine,
    *,
    destination: str | None,
    timeout: float,
) -> list[str]:
    """Return target HOME and immediate child directories of target ~/Dev."""

    if machine.local:
        home = Path.home().resolve()
        paths = [str(home)]
        dev = home / "Dev"
        if dev.is_dir():
            try:
                paths.extend(str(path.resolve()) for path in sorted(dev.iterdir()) if path.is_dir())
            except OSError as exc:
                raise RemoteCommandError(f"could not list {dev}: {exc}") from exc
        return paths

    if destination is None:
        raise RuntimeError("remote machine requires an SSH destination")
    remote = "sh -lc " + shlex.quote(_DIRECTORY_SCRIPT)
    result = _run_ssh_phase(
        destination,
        remote,
        phase="list-directories",
        timeout=timeout,
    )
    paths = [
        value.decode("utf8", errors="strict")
        for value in result.stdout.split(b"\0")
        if value
    ]
    if not paths:
        raise RemoteCommandError("target did not report its HOME directory")
    return paths


def pick_working_directory(paths: Sequence[str]) -> str:
    """Select HOME, one ~/Dev child, or a manual absolute path."""

    if not paths:
        raise RuntimeError("no target HOME directory available")
    fzf = shutil.which("fzf")
    if not fzf:
        raise RuntimeError("fzf is required when --cwd is omitted")
    home = paths[0]
    lines = [f"{home}\t⌂   HOME   {home}"]
    lines.extend(
        f"{path}\t▸   {PurePosixPath(path).name or path}   {path}"
        for path in paths[1:]
    )
    lines.append("__manual__\t…   Manual path…")
    result = subprocess.run(
        [
            fzf,
            "--height=100%",
            "--layout=reverse",
            "--border",
            "--sync",
            "--no-hscroll",
            "--read0",
            "--print0",
            "--delimiter=\\t",
            "--with-nth=2",
            "--nth=1",
            "--header=WORKING DIRECTORY",
            "--prompt=Directory › ",
        ],
        input="\0".join(lines) + "\0",
        text=True,
        stdout=subprocess.PIPE,
        check=False,
    )
    selected = result.stdout.rstrip("\0")
    if result.returncode == 130 or not selected:
        raise typer.Abort()
    if result.returncode != 0:
        raise RuntimeError(f"fzf directory picker failed with exit code {result.returncode}")
    value = selected.split("\t", 1)[0]
    if value == "__manual__":
        value = typer.prompt("Working directory", default=home)
    return validate_working_directory(value)


def validate_working_directory(value: str) -> str:
    value = value.strip()
    if not value or "\0" in value or not PurePosixPath(value).is_absolute():
        raise RuntimeError("working directory must be an absolute target path")
    return value.rstrip("/") or "/"


def default_session_name(cwd: str) -> str:
    return PurePosixPath(cwd.rstrip("/")).name or "Pi"


def _build_remote_wh_command(cwd: str, argv: Sequence[str]) -> str:
    """Build one strictly quoted target-side Worker Harness command."""

    cwd = validate_working_directory(cwd)
    script = "\n".join((
        "set -eu",
        "wh_bin=$(command -v wh 2>/dev/null || true)",
        "if [ -z \"$wh_bin\" ] || [ ! -x \"$wh_bin\" ]; then",
        "  wh_bin=\"$HOME/.local/bin/wh\"",
        "fi",
        "if [ ! -x \"$wh_bin\" ]; then",
        "  echo 'wh executable not found; install Worker Harness and run `wh host setup`' >&2",
        "  exit 127",
        "fi",
        f"cd -- {shlex.quote(cwd)}",
        f'exec "$wh_bin" {shlex.join(argv)}',
    ))
    return "sh -lc " + shlex.quote(script)


def build_remote_launch_command(
    cwd: str,
    name: str,
    pi_args: Sequence[str],
    *,
    agent: str = "pi",
) -> str:
    """Build the one strictly quoted target-side new-session command."""

    if not name.strip() or "\0" in name:
        raise RuntimeError("Pi session name may not be empty")
    argv = [
        "--output",
        "json",
        "start",
        "--no-attach",
        "--name",
        name,
        "--agent",
        agent,
    ]
    if pi_args:
        argv.extend(("--", *pi_args))
    return _build_remote_wh_command(cwd, argv)


def _parse_launch_result(result: subprocess.CompletedProcess[bytes]) -> dict[str, Any]:
    stdout = result.stdout.decode("utf8", errors="replace")
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError as exc:
        stderr = result.stderr.decode("utf8", errors="replace")
        raise RemoteCommandError(
            "target returned malformed launch JSON: " + _bounded_detail(stdout, stderr)
        ) from exc
    session_id = str(payload.get("session_id") or "")
    if not session_id:
        raise RemoteCommandError("target launch result did not include session_id")
    return payload


def run_target_launch(
    machine: LaunchMachine,
    *,
    destination: str | None,
    cwd: str,
    name: str,
    pi_args: Sequence[str],
    timeout: float,
    agent: str = "pi",
) -> dict[str, Any]:
    """Launch through local argv or one SSH remote command and parse its UUID."""

    if machine.local:
        wh = shutil.which("wh")
        if not wh:
            raise RuntimeError("wh is not installed or not on PATH")
        command = [
            wh,
            "--output",
            "json",
            "start",
            "--no-attach",
            "--name",
            name,
            "--agent",
            agent,
        ]
        if pi_args:
            command.extend(("--", *pi_args))
        result = _run_command(command, cwd=validate_working_directory(cwd), timeout=timeout)
    else:
        if destination is None:
            raise RuntimeError("remote machine requires an SSH destination")
        remote = build_remote_launch_command(cwd, name, pi_args, agent=agent)
        result = _run_ssh_phase(
            destination,
            remote,
            phase="start",
            timeout=timeout,
        )
    return _parse_launch_result(result)


def list_target_history(
    machine: LaunchMachine,
    *,
    destination: str | None,
    cwd: str,
    timeout: float,
) -> list[dict[str, Any]]:
    """List target-local, version-checked SessionManager history metadata."""

    cwd = validate_working_directory(cwd)
    if machine.local:
        from worker_harness.pi_history import list_session_history

        return list_session_history(cwd)
    if destination is None:
        raise RuntimeError("remote machine requires an SSH destination")
    remote = _build_remote_wh_command(
        cwd,
        ["--output", "json", "history-list", "--cwd", cwd],
    )
    result = _run_ssh_phase(
        destination,
        remote,
        phase="list-history",
        timeout=timeout,
    )
    try:
        rows = json.loads(result.stdout.decode("utf8", errors="replace"))
    except json.JSONDecodeError as exc:
        raise RemoteCommandError("target returned malformed Pi history JSON") from exc
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise RemoteCommandError("target returned an invalid Pi history list")
    return rows


def run_target_resume(
    machine: LaunchMachine,
    *,
    destination: str | None,
    cwd: str,
    history: dict[str, Any],
    timeout: float,
) -> dict[str, Any]:
    """Re-resolve and resume one exact history ID on its owning target."""

    cwd = validate_working_directory(cwd)
    session_id = str(history.get("id") or "")
    if not session_id or session_id.startswith("-") or any(c in session_id for c in "\0\r\n"):
        raise RuntimeError("invalid exact Pi history ID")
    argv = [
        "--output",
        "json",
        "resume",
        session_id,
        "--cwd",
        cwd,
        "--no-attach",
        "--timeout",
        str(timeout),
    ]
    if machine.local:
        wh = shutil.which("wh")
        if not wh:
            raise RuntimeError("wh is not installed or not on PATH")
        result = _run_command([wh, *argv], cwd=cwd, timeout=timeout + 5)
    else:
        if destination is None:
            raise RuntimeError("remote machine requires an SSH destination")
        remote = _build_remote_wh_command(cwd, argv)
        result = _run_ssh_phase(
            destination,
            remote,
            phase="resume",
            timeout=timeout + 5,
        )
    payload = _parse_launch_result(result)
    if str(payload.get("session_id") or "") != session_id:
        raise RemoteCommandError("target resumed a different Pi session ID")
    return payload


async def wait_for_registered_session(
    session_id: str,
    *,
    timeout: float,
    require_attachable: bool,
) -> dict[str, Any]:
    """Wait for the exact generated UUID; never infer readiness from output."""

    from worker_harness.cli import pi

    deadline = time.monotonic() + timeout
    last_error = "session has not registered"
    endpoint = f"/api/v1/pi/sessions/{session_id}"
    async with httpx.AsyncClient(base_url=pi._base_url(), timeout=5.0) as client:
        while True:
            delay = 1.0
            try:
                response = await client.get(endpoint)
                if response.status_code == 429:
                    # The control service has a shared per-operator-IP token
                    # bucket. Honor its rounded Retry-After rather than polling
                    # quickly enough to keep the bucket permanently empty, and
                    # do not hide a more useful prior registration state.
                    try:
                        retry_after = float(response.headers.get("Retry-After", "1"))
                    except ValueError:
                        retry_after = 1.0
                    delay = max(1.0, retry_after) + 0.05
                elif response.status_code == 404:
                    last_error = "session has not registered"
                else:
                    response.raise_for_status()
                    selected = response.json()
                    state = str(selected.get("state") or "")
                    if state not in _ACTIVE_STATES:
                        last_error = f"session registered with state {state or 'unknown'}"
                    elif not require_attachable or bool(selected.get("terminal_attachable")):
                        return selected
                    else:
                        last_error = "session registered but is not terminal-attachable yet"
            except (httpx.HTTPError, json.JSONDecodeError) as exc:
                last_error = f"control API unavailable: {exc}"

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError(
                    f"Pi {session_id} started on the target, but {last_error} "
                    f"within {timeout:g} seconds. The target Pi is still running; "
                    "check its bridge orchestrator URL/connectivity, then attach by session ID."
                )
            await asyncio.sleep(min(delay, remaining))


async def _load_worker_records() -> list[dict[str, Any]]:
    from worker_harness.cli import pi

    try:
        workers = await pi._request("GET", "/api/v1/workers")
    except RuntimeError:
        return []
    return workers if isinstance(workers, list) else []


async def _load_registered_sessions() -> list[dict[str, Any]]:
    from worker_harness.cli import pi

    rows = await pi._request("GET", "/api/v1/pi/sessions")
    return rows if isinstance(rows, list) else []


def _active_sessions_for_target(
    rows: Sequence[dict[str, Any]],
    machine: LaunchMachine,
    cwd: str,
) -> list[dict[str, Any]]:
    identifiers = {
        machine.alias.casefold(),
        machine.hostname.casefold(),
        machine.dns_name.rstrip(".").casefold(),
        *(address.casefold() for address in machine.addresses),
    }
    if machine.local:
        import socket

        identifiers.update((socket.gethostname().casefold(), "localhost", "127.0.0.1"))
    result = []
    for row in rows:
        if (
            str(row.get("session_type") or "") != "interactive"
            or str(row.get("state") or "") not in _ACTIVE_STATES
            or str(row.get("cwd") or "") != cwd
        ):
            continue
        locations = {
            str(row.get("host") or "").rstrip(".").casefold(),
            str(row.get("terminal_host") or "").rstrip(".").casefold(),
        }
        if identifiers.intersection(locations):
            result.append(dict(row))
    return sorted(
        result,
        key=lambda row: (
            0 if str(row.get("state")) == "working" else 1,
            str(row.get("name") or "").casefold(),
            str(row.get("id") or ""),
        ),
    )


def _clean_picker_text(value: object) -> str:
    return " ".join(str(value or "").replace("\0", " ").split())


def pick_launch_action(
    active: Sequence[dict[str, Any]],
    history: Sequence[dict[str, Any]],
) -> tuple[str, dict[str, Any] | None]:
    """Choose Running, Previous, or Start new after machine/cwd selection."""

    fzf = shutil.which("fzf")
    if not fzf:
        # Preserve the pre-history behavior on minimal hosts: lack of an
        # optional picker must never prevent starting a new managed Pi.
        return "new", None
    records: dict[str, tuple[str, dict[str, Any] | None]] = {}
    lines: list[str] = []
    for index, row in enumerate(active):
        key = f"attach:{row.get('id')}"
        records[key] = ("attach", dict(row))
        prefix = "Running sessions\n" if index == 0 else ""
        branch = "└─" if index == len(active) - 1 else "├─"
        lines.append(
            f"{key}\t{prefix}  {branch} {_clean_picker_text(row.get('state')):7}  "
            f"{_clean_picker_text(row.get('name') or row.get('task') or 'Pi')}"
        )
    for index, row in enumerate(history):
        key = f"resume:{row.get('id')}"
        records[key] = ("resume", dict(row))
        prefix = "Previous sessions\n" if index == 0 else ""
        branch = "└─" if index == len(history) - 1 else "├─"
        label = row.get("name") or row.get("first_message") or "Pi"
        lines.append(
            f"{key}\t{prefix}  {branch} {_clean_picker_text(row.get('modified_at'))[:19]:19}  "
            f"{_clean_picker_text(label)}"
        )
    records["new"] = ("new", None)
    lines.append("new\tStart new\n  └─ ＋   New managed Pi session")
    result = subprocess.run(
        [
            fzf,
            "--height=100%",
            "--layout=reverse",
            "--border",
            "--sync",
            "--no-hscroll",
            "--read0",
            "--print0",
            "--delimiter=\\t",
            "--with-nth=2",
            "--nth=1",
            "--header=PI SESSION",
            "--prompt=Action › ",
        ],
        input="\0".join(lines) + "\0",
        text=True,
        stdout=subprocess.PIPE,
        check=False,
    )
    selected = result.stdout.rstrip("\0")
    if result.returncode == 130 or not selected:
        raise typer.Abort()
    if result.returncode != 0:
        raise RuntimeError(f"fzf launch action picker failed with exit code {result.returncode}")
    key = selected.split("\t", 1)[0]
    if key not in records:
        raise RuntimeError("fzf returned an unknown launch action")
    return records[key]


async def _attach_selected_session(
    selected: dict[str, Any],
    *,
    tmux_target_session: str | None = None,
    tmux_target_client: str | None = None,
) -> None:
    from worker_harness.cli import pi
    from worker_harness.pi_zellij import is_immediate_zellij

    if (tmux_target_session is None) != (tmux_target_client is None):
        raise RuntimeError("tmux launch handoff requires an exact session and client")
    if tmux_target_session is not None and tmux_target_client is not None:
        from worker_harness.pi_tmux import open_or_focus_attachment_window

        await asyncio.to_thread(
            open_or_focus_attachment_window,
            selected,
            tmux_target_session,
            tmux_target_client,
        )
    elif is_immediate_zellij():
        await pi._open_in_zellij(selected)
    else:
        await pi._run_attach_loop(selected)


async def launch_managed_pi(
    *,
    machine_selector: str | None,
    cwd: str | None,
    name: str | None,
    ssh_user: str | None,
    attach_after_start: bool,
    timeout: float,
    pi_args: Sequence[str],
    agent: str = "pi",
    tmux_target_session: str | None = None,
    tmux_target_client: str | None = None,
) -> dict[str, Any]:
    """Run the complete select → SSH launch → registration → attach flow."""

    if (tmux_target_session is None) != (tmux_target_client is None):
        raise RuntimeError("tmux launch handoff requires an exact session and client")
    if tmux_target_session is not None and tmux_target_client is not None:
        if not attach_after_start:
            raise RuntimeError("tmux launch handoff requires attachment")
        from worker_harness.pi_tmux import validate_attachment_target

        tmux_target_session, tmux_target_client = validate_attachment_target(
            tmux_target_session,
            tmux_target_client,
        )

    workers = await _load_worker_records()
    machines = await asyncio.to_thread(tailscale_launch_machines, workers)
    machine = (
        resolve_machine(machines, machine_selector)
        if machine_selector
        else pick_machine(machines)
    )
    destination = None if machine.local else ssh_destination(machine, ssh_user)
    if cwd is None:
        paths = await asyncio.to_thread(
            list_working_directories,
            machine,
            destination=destination,
            timeout=timeout,
        )
        cwd = pick_working_directory(paths)
    else:
        cwd = validate_working_directory(cwd)

    interactive_action = os.isatty(0) and name is None and not pi_args
    action: str = "new"
    chosen: dict[str, Any] | None = None
    if interactive_action:
        try:
            registered = await _load_registered_sessions()
        except RuntimeError as exc:
            console.print(f"[yellow]Running Pi sessions unavailable:[/] {exc}")
            registered = []
        active = [
            row for row in _active_sessions_for_target(registered, machine, cwd)
            if row_agent(row) == agent
        ]
        histories: list[dict[str, Any]] = []
        if agent == "pi":
            try:
                histories = await asyncio.to_thread(
                    list_target_history,
                    machine,
                    destination=destination,
                    cwd=cwd,
                    timeout=timeout,
                )
            except (RuntimeError, RemoteCommandError) as exc:
                console.print(f"[yellow]Previous Pi sessions unavailable:[/] {exc}")
        active_ids = {
            str(row.get("id") or "")
            for row in registered
            if str(row.get("state") or "") in _ACTIVE_STATES
        }
        histories = [
            row for row in histories
            if str(row.get("id") or "") not in active_ids
            and str(row.get("cwd") or "") == cwd
        ]
        action, chosen = pick_launch_action(active, histories)
    if action == "attach":
        assert chosen is not None
        if attach_after_start:
            await _attach_selected_session(
                chosen,
                tmux_target_session=tmux_target_session,
                tmux_target_client=tmux_target_client,
            )
        return {
            "session_id": str(chosen.get("id") or ""),
            "name": str(chosen.get("name") or chosen.get("task") or "Pi"),
            "machine": machine.alias,
            "machine_dns": machine.dns_name,
            "cwd": cwd,
            "action": "attach",
        }
    if action == "resume":
        assert chosen is not None
        resumed = await asyncio.to_thread(
            run_target_resume,
            machine,
            destination=destination,
            cwd=cwd,
            history=chosen,
            timeout=timeout,
        )
        session_id = str(resumed["session_id"])
        selected = await wait_for_registered_session(
            session_id,
            timeout=timeout,
            require_attachable=attach_after_start,
        )
        result = {
            **resumed,
            "machine": machine.alias,
            "machine_dns": machine.dns_name,
            "cwd": cwd,
            "action": "resume",
        }
        if attach_after_start:
            await _attach_selected_session(
                selected,
                tmux_target_session=tmux_target_session,
                tmux_target_client=tmux_target_client,
            )
        return result

    default_name = default_session_name(cwd)
    if name is None:
        name = typer.prompt("Pi name", default=default_name) if os.isatty(0) else default_name
    name = name.strip()
    if not name:
        raise RuntimeError("Pi session name may not be empty")

    launched = await asyncio.to_thread(
        run_target_launch,
        machine,
        destination=destination,
        cwd=cwd,
        name=name,
        pi_args=pi_args,
        timeout=timeout,
        agent=agent,
    )
    session_id = str(launched["session_id"])
    selected = await wait_for_registered_session(
        session_id,
        timeout=timeout,
        require_attachable=attach_after_start,
    )
    result = {
        **launched,
        "machine": machine.alias,
        "machine_dns": machine.dns_name,
        "cwd": cwd,
        "action": "new",
    }
    if attach_after_start:
        await _attach_selected_session(
            selected,
            tmux_target_session=tmux_target_session,
            tmux_target_client=tmux_target_client,
        )
    return result


def launch(
    ctx: typer.Context,
    machine: str | None = typer.Option(
        None,
        "--machine",
        "-m",
        help="Tailnet alias, DNS name, hostname, or IP; picker when omitted",
    ),
    cwd: str | None = typer.Option(
        None,
        "--cwd",
        help="Absolute working directory on the target; picker when omitted",
    ),
    name: str | None = typer.Option(None, "--name", "-n", help="Human-facing Pi name"),
    agent: str | None = typer.Option(None, "--agent", help="Agent to launch: pi or omp; asks when omitted"),
    ssh_user: str | None = typer.Option(None, "--ssh-user", help="OpenSSH user override"),
    attach_after_start: bool = typer.Option(
        True,
        "--attach/--no-attach",
        help="Attach after the exact session registers",
    ),
    timeout: float = typer.Option(
        30.0,
        "--timeout",
        min=1.0,
        help="Seconds allowed for each target operation and registration",
    ),
    tmux_picker: bool = typer.Option(False, "--tmux-picker", hidden=True),
) -> None:
    """Launch a managed Pi on a standard or wh-worker Tailnet machine."""

    try:
        tmux_target_session = None
        tmux_target_client = None
        if tmux_picker:
            if not attach_after_start:
                raise RuntimeError("--tmux-picker cannot be combined with --no-attach")
            tmux_target_session = os.environ.get("WH_TMUX_TARGET_SESSION")
            tmux_target_client = os.environ.get("WH_TMUX_TARGET_CLIENT")
            if not tmux_target_session or not tmux_target_client:
                raise RuntimeError("--tmux-picker requires its invoking tmux session and client")
            from worker_harness.pi_tmux import validate_attachment_target

            tmux_target_session, tmux_target_client = validate_attachment_target(
                tmux_target_session,
                tmux_target_client,
            )
        agent = pick_agent(agent)
        result = asyncio.run(launch_managed_pi(
            machine_selector=machine,
            cwd=cwd,
            name=name,
            ssh_user=ssh_user,
            attach_after_start=attach_after_start,
            timeout=timeout,
            pi_args=list(ctx.args),
            agent=agent,
            tmux_target_session=tmux_target_session,
            tmux_target_client=tmux_target_client,
        ))
        if not attach_after_start:
            from worker_harness.cli.pi import _output_mode

            if _output_mode() == "json":
                console.print(json.dumps(result, indent=2))
            else:
                action = str(result.get("action") or "new")
                verb = {"attach": "Selected", "resume": "Resumed"}.get(action, "Started")
                console.print(
                    f"[green]{verb} Pi[/] {result.get('name') or name} on "
                    f"@{result['machine']}:{result['cwd']} "
                    f"([dim]{result['session_id']}[/])"
                )
    except typer.Abort:
        raise
    except (RuntimeError, KeyboardInterrupt) as exc:
        console.print(f"[red]{exc or 'launch interrupted'}[/]")
        raise typer.Exit(1) from exc
