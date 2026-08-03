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
        "--no-sort",
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
    result = _run_command(
        ssh_command(destination, remote, connect_timeout=min(10, max(1, int(timeout)))),
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
            "--no-sort",
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


def build_remote_launch_command(cwd: str, name: str, pi_args: Sequence[str]) -> str:
    """Build the one strictly quoted target-side shell command."""

    cwd = validate_working_directory(cwd)
    if not name.strip() or "\0" in name:
        raise RuntimeError("Pi session name may not be empty")
    argv = [
        "wh",
        "--output",
        "json",
        "pi",
        "start",
        "--no-attach",
        "--name",
        name,
    ]
    if pi_args:
        argv.extend(("--", *pi_args))
    script = f"cd -- {shlex.quote(cwd)} && exec {shlex.join(argv)}"
    return "sh -lc " + shlex.quote(script)


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
            "pi",
            "start",
            "--no-attach",
            "--name",
            name,
        ]
        if pi_args:
            command.extend(("--", *pi_args))
        result = _run_command(command, cwd=validate_working_directory(cwd), timeout=timeout)
    else:
        if destination is None:
            raise RuntimeError("remote machine requires an SSH destination")
        remote = build_remote_launch_command(cwd, name, pi_args)
        result = _run_command(
            ssh_command(destination, remote, connect_timeout=min(10, max(1, int(timeout)))),
            timeout=timeout,
        )
    return _parse_launch_result(result)


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


async def launch_managed_pi(
    *,
    machine_selector: str | None,
    cwd: str | None,
    name: str | None,
    ssh_user: str | None,
    attach_after_start: bool,
    timeout: float,
    pi_args: Sequence[str],
) -> dict[str, Any]:
    """Run the complete select → SSH launch → registration → attach flow."""

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
    }
    if attach_after_start:
        from worker_harness.cli import pi
        from worker_harness.pi_zellij import is_immediate_zellij

        if is_immediate_zellij():
            await pi._open_in_zellij(selected)
        else:
            await pi._run_attach_loop(selected)
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
) -> None:
    """Launch a managed Pi on a standard or wh-worker Tailnet machine."""

    try:
        result = asyncio.run(launch_managed_pi(
            machine_selector=machine,
            cwd=cwd,
            name=name,
            ssh_user=ssh_user,
            attach_after_start=attach_after_start,
            timeout=timeout,
            pi_args=list(ctx.args),
        ))
        if not attach_after_start:
            from worker_harness.cli.pi import _output_mode

            if _output_mode() == "json":
                console.print(json.dumps(result, indent=2))
            else:
                console.print(
                    f"[green]Started Pi[/] {result.get('name') or name} on "
                    f"@{result['machine']}:{result['cwd']} "
                    f"([dim]{result['session_id']}[/])"
                )
    except typer.Abort:
        raise
    except (RuntimeError, KeyboardInterrupt) as exc:
        console.print(f"[red]{exc or 'launch interrupted'}[/]")
        raise typer.Exit(1) from exc
