"""Managed hidden-tmux runtime for ordinary interactive Pi sessions."""

from __future__ import annotations

import asyncio
import dataclasses
import fcntl
import hashlib
import json
import os
import re
import shlex
import shutil
import stat
import subprocess
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Sequence
from urllib.parse import quote

from worker_harness.agents import AGENT_EXECUTABLE_ENV, validate_agent

MANAGED_TMUX_SESSION = "wh-pi"
MANAGED_TMUX_HISTORY_LIMIT = 50_000
MANAGED_TMUX_EXTENDED_KEYS_FORMAT = "csi-u"
_ROUTE_POLL_SECONDS = 0.1
_PANE_ID = re.compile(r"^%\d+$")
_TMUX_VERSION = re.compile(r"^tmux (\d+)\.(\d+)")
_CONFLICTING_PI_OPTIONS = {
    "--session",
    "--session-id",
    "--continue",
    "-c",
    "--resume",
    "-r",
    "--fork",
    "--no-session",
    "--name",
    "-n",
}
_CONFLICTING_PI_PREFIXES = (
    "--session=",
    "--session-id=",
    "--fork=",
    "--name=",
)


@dataclass(frozen=True)
class ManagedPiSession:
    session_id: str
    name: str
    tmux_socket: Path
    tmux_pane_id: str


def managed_tmux_socket_path() -> Path:
    configured = os.environ.get("WH_PI_MANAGED_TMUX_SOCKET")
    if configured:
        return Path(configured).expanduser()
    runtime = os.environ.get("XDG_RUNTIME_DIR") or f"/tmp/worker-harness-{os.getuid()}"
    return Path(runtime) / "worker-harness" / "pi-tmux.sock"


def _prepare_managed_runtime(socket: Path) -> None:
    if not os.environ.get("XDG_RUNTIME_DIR") and not os.environ.get("WH_PI_MANAGED_TMUX_SOCKET"):
        fallback_root = socket.parent.parent
        fallback_root.mkdir(parents=False, exist_ok=True, mode=0o700)
        metadata = fallback_root.lstat()
        if not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.getuid():
            raise RuntimeError(f"unsafe managed Pi runtime directory: {fallback_root}")
        fallback_root.chmod(0o700)
    socket.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    metadata = socket.parent.lstat()
    if not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.getuid():
        raise RuntimeError(f"unsafe managed Pi socket directory: {socket.parent}")
    socket.parent.chmod(0o700)


@contextmanager
def _resume_identity_lock(session_id: str) -> Iterator[None]:
    socket = managed_tmux_socket_path()
    _prepare_managed_runtime(socket)
    lock_dir = socket.parent / "pi-resume-locks"
    lock_dir.mkdir(mode=0o700, exist_ok=True)
    metadata = lock_dir.lstat()
    if not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.getuid():
        raise RuntimeError(f"unsafe Pi resume lock directory: {lock_dir}")
    lock_dir.chmod(0o700)
    digest = hashlib.sha256(session_id.encode("utf8")).hexdigest()
    flags = os.O_CREAT | os.O_RDWR | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(lock_dir / f"{digest}.lock", flags, 0o600)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.getuid():
            raise RuntimeError("unsafe Pi resume identity lock")
        os.fchmod(descriptor, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def local_relay_websocket_url(session_id: str) -> str:
    port = os.environ.get("WH_PI_HOST_RELAY_LOCAL_PORT", "27890")
    try:
        parsed_port = int(port)
    except ValueError as exc:
        raise RuntimeError("WH_PI_HOST_RELAY_LOCAL_PORT must be an integer") from exc
    if not 1 <= parsed_port <= 65535:
        raise RuntimeError("WH_PI_HOST_RELAY_LOCAL_PORT must be between 1 and 65535")
    encoded = quote(session_id, safe="")
    return f"ws://127.0.0.1:{parsed_port}/v1/sessions/{encoded}/attach"


def validate_new_session_args(pi_args: Sequence[str]) -> None:
    for argument in pi_args:
        if argument in _CONFLICTING_PI_OPTIONS or argument.startswith(_CONFLICTING_PI_PREFIXES):
            raise RuntimeError(
                f"agent option {argument!r} conflicts with managed new-session identity; "
                "resume/continue/fork modes are not supported by wh start yet"
            )


def _host_runtime():
    from worker_harness.host_runtime import HostRuntimeError, load_host_runtime

    try:
        return load_host_runtime(required=False)
    except HostRuntimeError as exc:
        raise RuntimeError(
            f"host runtime manifest is invalid: {exc}; rerun `wh host setup`"
        ) from exc


def _real_agent_executable(agent: str) -> str:
    override = AGENT_EXECUTABLE_ENV[validate_agent(agent)]
    configured = os.environ.get(override)
    if configured:
        candidate = Path(configured).expanduser()
        if not candidate.is_file() or not os.access(candidate, os.X_OK):
            raise RuntimeError(f"{override} is not executable: {candidate}")
        return str(candidate.resolve())
    runtime = _host_runtime()
    executable = runtime.executable(agent) if runtime else shutil.which(agent)
    if not executable:
        raise RuntimeError(
            f"{agent} executable not found; run `wh host setup` from a shell where {agent} is "
            f"available or set {override}"
        )
    return str(Path(executable).resolve())


def _tmux_executable() -> str:
    runtime = _host_runtime()
    executable = runtime.executable("tmux") if runtime else shutil.which("tmux")
    if not executable:
        raise RuntimeError(
            "tmux is required for wh start; run `wh host setup` from a prepared shell"
        )
    return executable


def _tmux_environment() -> dict[str, str]:
    runtime = _host_runtime()
    environment = runtime.environment() if runtime else dict(os.environ)
    for key in ("TMUX", "TMUX_PANE", "ZELLIJ", "ZELLIJ_SESSION_NAME", "ZELLIJ_PANE_ID"):
        environment.pop(key, None)
    return environment


def _tmux_command(socket: Path, *args: str) -> list[str]:
    return [_tmux_executable(), "-f", "/dev/null", "-S", str(socket), *args]


def _tmux_supports_csi_u() -> bool:
    try:
        result = subprocess.run(
            [_tmux_executable(), "-V"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=5.0,
            check=False,
            env=_tmux_environment(),
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False
    match = _TMUX_VERSION.match(result.stdout.strip())
    return result.returncode == 0 and match is not None and tuple(
        int(part) for part in match.groups()
    ) >= (3, 5)


def _run_tmux(
    socket: Path,
    *args: str,
    check: bool = True,
    timeout: float = 5.0,
) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            _tmux_command(socket, *args),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
            check=False,
            env=_tmux_environment(),
        )
    except FileNotFoundError as exc:
        raise RuntimeError("tmux is required for wh start") from exc
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"tmux command timed out: {' '.join(args)}") from exc
    if check and result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or f"exit code {result.returncode}"
        raise RuntimeError(f"tmux {' '.join(args[:2])} failed: {detail}")
    return result


def _session_exists(socket: Path) -> bool:
    return _run_tmux(socket, "has-session", "-t", MANAGED_TMUX_SESSION, check=False).returncode == 0


def _managed_session_id_is_live(socket: Path, session_id: str) -> bool:
    if not _session_exists(socket):
        return False
    result = _run_tmux(
        socket,
        "list-panes",
        "-s",
        "-t",
        MANAGED_TMUX_SESSION,
        "-F",
        "#{pane_dead}\t#{@wh_pi_session_id}",
        check=False,
    )
    if result.returncode != 0:
        return False
    return any(
        line.split("\t", 1) == ["0", session_id]
        for line in result.stdout.splitlines()
    )


def _configure_managed_server(socket: Path) -> None:
    for key in ("ZELLIJ", "ZELLIJ_SESSION_NAME", "ZELLIJ_PANE_ID"):
        _run_tmux(socket, "set-environment", "-g", "-u", key, check=False)
    # This server is exclusively Worker Harness-owned. Keep managed defaults
    # isolated from the user's tmux configuration, then reinforce the options
    # that attached clients depend on directly on the owner session.
    supports_csi_u = _tmux_supports_csi_u()
    _run_tmux(socket, "set-option", "-g", "status", "off")
    _run_tmux(socket, "set-option", "-g", "mouse", "on")
    _run_tmux(socket, "set-option", "-s", "set-clipboard", "external")
    _run_tmux(socket, "set-option", "-g", "extended-keys", "on")
    if supports_csi_u:
        _run_tmux(
            socket,
            "set-option",
            "-g",
            "extended-keys-format",
            MANAGED_TMUX_EXTENDED_KEYS_FORMAT,
        )
    _run_tmux(
        socket,
        "set-option",
        "-g",
        "history-limit",
        str(MANAGED_TMUX_HISTORY_LIMIT),
    )
    _run_tmux(socket, "set-option", "-t", MANAGED_TMUX_SESSION, "status", "off")
    _run_tmux(socket, "set-option", "-t", MANAGED_TMUX_SESSION, "mouse", "on")
    _run_tmux(
        socket,
        "set-option",
        "-t",
        MANAGED_TMUX_SESSION,
        "history-limit",
        str(MANAGED_TMUX_HISTORY_LIMIT),
    )
    _run_tmux(
        socket,
        "set-option",
        "-t",
        MANAGED_TMUX_SESSION,
        "window-size",
        "latest",
    )


def _default_name(cwd: Path, session_id: str) -> str:
    base = cwd.name.strip() or "pi"
    return f"{base}-{session_id[:8]}"


def start_managed_pi(
    *,
    name: str | None,
    pi_args: Sequence[str],
    cwd: Path | None = None,
    rows: int = 24,
    cols: int = 80,
    session_id: str | None = None,
    executable: str | None = None,
    agent: str = "pi",
    _resume_existing: bool = False,
) -> ManagedPiSession:
    """Create one detached Pi window in the dedicated hidden tmux server."""

    validate_new_session_args(pi_args)
    cwd = (cwd or Path.cwd()).resolve()
    if not cwd.is_dir():
        raise RuntimeError(f"Pi working directory does not exist: {cwd}")
    session_id = session_id or str(uuid.uuid4())
    display_name = (name or _default_name(cwd, session_id)).strip()
    if not display_name:
        raise RuntimeError("Pi session name cannot be empty")
    pi_executable = executable or _real_agent_executable(agent)
    socket = managed_tmux_socket_path()
    _prepare_managed_runtime(socket)
    if _resume_existing and _managed_session_id_is_live(socket, session_id):
        raise RuntimeError(f"Pi session {session_id} is already active in the managed runtime")

    runtime = _host_runtime()
    runtime_assignments = (
        [f"PATH={os.pathsep.join(runtime.path)}"] if runtime is not None else []
    )
    if validate_agent(agent) == "omp":
        if _resume_existing:
            raise RuntimeError("omp sessions cannot be resumed through the managed runtime yet")
        # omp has no --session-id/--name; its id is read back from the pane after start.
        identity_args: list[str] = []
        session_id = ""
    else:
        identity_args = (
            ["--session", session_id]
            if _resume_existing
            else ["--session-id", session_id, "--name", display_name]
        )
    command = shlex.join([
        "env",
        "WH_MANAGED_PI=1",
        *runtime_assignments,
        pi_executable,
        *identity_args,
        *pi_args,
    ])
    dimensions = ["-x", str(max(1, cols)), "-y", str(max(1, rows))]

    if _session_exists(socket):
        _configure_managed_server(socket)
        created = _run_tmux(
            socket,
            "new-window",
            "-d",
            "-P",
            "-F",
            "#{pane_id}",
            "-t",
            MANAGED_TMUX_SESSION,
            "-n",
            display_name,
            "-c",
            str(cwd),
            command,
        )
    else:
        # Set global options in the same tmux command queue that starts the
        # server, before the first pane is allocated. In particular, tmux fixes
        # a pane's history limit at creation time, so configuring it after
        # new-session would leave the first managed Pi at the 2,000-line default.
        extended_keys_format = (
            [
                "set-option",
                "-g",
                "extended-keys-format",
                MANAGED_TMUX_EXTENDED_KEYS_FORMAT,
                ";",
            ]
            if _tmux_supports_csi_u()
            else []
        )
        created = _run_tmux(
            socket,
            "start-server",
            ";",
            "set-option",
            "-g",
            "status",
            "off",
            ";",
            "set-option",
            "-g",
            "mouse",
            "on",
            ";",
            "set-option",
            "-s",
            "set-clipboard",
            "external",
            ";",
            "set-option",
            "-g",
            "extended-keys",
            "on",
            ";",
            *extended_keys_format,
            "set-option",
            "-g",
            "history-limit",
            str(MANAGED_TMUX_HISTORY_LIMIT),
            ";",
            "new-session",
            "-d",
            "-P",
            "-F",
            "#{pane_id}",
            "-s",
            MANAGED_TMUX_SESSION,
            "-n",
            display_name,
            "-c",
            str(cwd),
            *dimensions,
            command,
            check=False,
        )
        if created.returncode != 0:
            # Another concurrent launcher may have created the owner session.
            if not _session_exists(socket):
                detail = created.stderr.strip() or created.stdout.strip() or "unknown error"
                raise RuntimeError(f"tmux new-session failed: {detail}")
            _configure_managed_server(socket)
            created = _run_tmux(
                socket,
                "new-window",
                "-d",
                "-P",
                "-F",
                "#{pane_id}",
                "-t",
                MANAGED_TMUX_SESSION,
                "-n",
                display_name,
                "-c",
                str(cwd),
                command,
            )
        else:
            _configure_managed_server(socket)

    pane_id = created.stdout.strip().splitlines()[-1] if created.stdout.strip() else ""
    if not _PANE_ID.fullmatch(pane_id):
        raise RuntimeError(f"tmux did not return a stable pane ID: {pane_id!r}")
    _run_tmux(
        socket,
        "set-option",
        "-p",
        "-t",
        pane_id,
        "@wh_pi_session_id",
        session_id,
    )
    if socket.exists():
        socket.chmod(0o600)
    return ManagedPiSession(session_id, display_name, socket, pane_id)


def resume_managed_pi(
    *,
    session_id: str,
    name: str,
    cwd: Path,
    pi_args: Sequence[str] = (),
    rows: int = 24,
    cols: int = 80,
    executable: str | None = None,
) -> ManagedPiSession:
    """Resume one already target-resolved exact Pi history ID."""

    if (
        not session_id
        or session_id.startswith("-")
        or any(character in session_id for character in "\0\r\n")
    ):
        raise RuntimeError("exact Pi session ID is invalid")
    with _resume_identity_lock(session_id):
        return start_managed_pi(
            name=name,
            pi_args=pi_args,
            cwd=cwd,
            rows=rows,
            cols=cols,
            session_id=session_id,
            executable=executable,
            _resume_existing=True,
        )


def managed_pane_is_live(session: ManagedPiSession) -> bool:
    result = _run_tmux(
        session.tmux_socket,
        "display-message",
        "-p",
        "-t",
        session.tmux_pane_id,
        "#{pane_id}",
        check=False,
    )
    return result.returncode == 0 and result.stdout.strip() == session.tmux_pane_id


def _same_socket(left: str | os.PathLike[str], right: str | os.PathLike[str]) -> bool:
    return os.path.realpath(os.fspath(left)) == os.path.realpath(os.fspath(right))


async def wait_for_managed_route(
    session: ManagedPiSession,
    *,
    timeout: float = 10.0,
) -> dict[str, Any]:
    """Wait until the bridge registers the exact hidden tmux pane locally."""

    from worker_harness.pi_terminal import _relay_request

    if timeout <= 0:
        raise RuntimeError("route wait timeout must be positive")
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    last_error = "local Pi route is not registered"
    while loop.time() < deadline:
        try:
            route = await _relay_request({"action": "describe", "session_id": session.session_id})
            if str(route.get("multiplexer") or "") != "tmux":
                raise RuntimeError("generated Pi session registered a non-tmux route")
            if not _same_socket(str(route.get("tmux_socket") or ""), session.tmux_socket):
                raise RuntimeError("generated Pi session registered the wrong tmux socket")
            if str(route.get("tmux_pane_id") or "") != session.tmux_pane_id:
                raise RuntimeError("generated Pi session registered the wrong tmux pane")
            return route
        except (OSError, asyncio.TimeoutError, RuntimeError, json.JSONDecodeError) as exc:
            last_error = str(exc)
        if not await asyncio.to_thread(managed_pane_is_live, session):
            raise RuntimeError(
                f"Pi exited before its local terminal route became ready (session {session.session_id})"
            )
        await asyncio.sleep(min(_ROUTE_POLL_SECONDS, max(0.0, deadline - loop.time())))
    raise RuntimeError(
        f"Pi is still running but its local terminal route did not become ready within {timeout:g}s: "
        f"{last_error}. Attach later with: wh attach {session.session_id}"
    )


async def _locate_managed_session_id(session: ManagedPiSession, *, timeout: float) -> str:
    """Read back the session id the agent chose for one exact managed pane."""

    from worker_harness.pi_terminal import _relay_request

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    last_error = "managed pane has not registered a session"
    while loop.time() < deadline:
        try:
            located = await _relay_request({
                "action": "locate",
                "multiplexer": "tmux",
                "tmux_socket": str(session.tmux_socket),
                "tmux_pane_id": session.tmux_pane_id,
            })
            session_id = str(located.get("session_id") or "")
            if session_id:
                return session_id
            last_error = "relay returned an empty session id"
        except (OSError, asyncio.TimeoutError, RuntimeError, json.JSONDecodeError) as exc:
            last_error = str(exc)
        if not await asyncio.to_thread(managed_pane_is_live, session):
            raise RuntimeError("agent exited before registering its local terminal route")
        await asyncio.sleep(min(_ROUTE_POLL_SECONDS, max(0.0, deadline - loop.time())))
    raise RuntimeError(
        f"managed agent pane did not register a session within {timeout:g}s: {last_error}"
    )


async def ensure_managed_route(
    session: ManagedPiSession,
    *,
    timeout: float = 10.0,
) -> tuple[ManagedPiSession, dict[str, Any]]:
    """Resolve an agent-chosen session id when absent, then validate its route."""

    if not session.session_id:
        session = dataclasses.replace(
            session,
            session_id=await _locate_managed_session_id(session, timeout=timeout),
        )
    return session, await wait_for_managed_route(session, timeout=timeout)
