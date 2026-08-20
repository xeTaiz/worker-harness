"""Operator-tmux windows for persistent Worker Harness Pi attachments.

The popup is only a picker.  The streamed terminal always lives in a real,
WH-owned tmux window that can be found again by the authoritative Pi UUID.
"""

from __future__ import annotations

import fcntl
import hashlib
import os
import re
import shlex
import shutil
import stat
import subprocess
import sys
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

from worker_harness.pi_zellij_state import (
    DISCONNECTED,
    ERROR,
    IDLE,
    WORKING,
    tab_title,
)

WORKING_COLOR = "#89b4fa"
IDLE_COLOR = "#a6e3a1"
ERROR_COLOR = "#f38ba8"
DISCONNECTED_COLOR = "#6c7086"

_STATE_COLORS = {
    WORKING: WORKING_COLOR,
    IDLE: IDLE_COLOR,
    ERROR: ERROR_COLOR,
    DISCONNECTED: DISCONNECTED_COLOR,
}
_STATE_ALIASES = {
    "failed": ERROR,
    "runtime_error": ERROR,
    "offline": DISCONNECTED,
    "unknown": DISCONNECTED,
}
_TMUX_SESSION_ID = re.compile(r"^\$\d+$")
_WINDOW_ID = re.compile(r"^@\d+$")
_PANE_ID = re.compile(r"^%\d+$")
_TIMEOUT_SECONDS = 5.0


def is_immediate_tmux() -> bool:
    """Return whether this process is running in a tmux pane."""

    return bool(os.environ.get("TMUX") and os.environ.get("TMUX_PANE"))


def _normalize_state(state: object) -> str:
    value = str(state or DISCONNECTED).strip().lower()
    value = _STATE_ALIASES.get(value, value)
    return value if value in _STATE_COLORS else DISCONNECTED


def _validate_text(value: object, label: str, *, reject_option: bool = False) -> str:
    text = str(value or "")
    if not text or any(character in text for character in "\0\r\n"):
        raise RuntimeError(f"{label} is missing or invalid")
    if reject_option and text.startswith("-"):
        raise RuntimeError(f"{label} must not start with '-'")
    return text


def _validate_tmux_session(value: object) -> str:
    text = _validate_text(value, "tmux target session")
    if not _TMUX_SESSION_ID.fullmatch(text):
        raise RuntimeError("tmux target session must be an exact tmux session ID")
    return text


def validate_attachment_target(
    target_session: object,
    target_client: object,
) -> tuple[str, str]:
    """Validate an exact invoking tmux session/client before side effects."""

    return (
        _validate_tmux_session(target_session),
        _validate_text(target_client, "tmux target client", reject_option=True),
    )


def _tmux_executable() -> str:
    return shutil.which("tmux") or "tmux"


def _wh_executable() -> str:
    executable = shutil.which("wh")
    if executable:
        return executable
    try:
        from worker_harness.host_runtime import load_host_runtime

        runtime = load_host_runtime(required=False)
    except RuntimeError:  # invalid manifests are reported by the child command itself
        runtime = None
    return (runtime.executable("wh") if runtime else None) or sys.argv[0]


def _run_tmux(
    args: Sequence[str],
    *,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    command = [_tmux_executable(), *args]
    try:
        result = subprocess.run(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
            timeout=_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"tmux command timed out: {shlex.join(command)}") from exc
    except OSError as exc:
        raise RuntimeError(f"could not run tmux: {exc}") from exc
    if check and result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or f"exit code {result.returncode}"
        raise RuntimeError(f"tmux command failed ({shlex.join(command[1:])}): {detail}")
    return result


def _ensure_private_directory(path: Path) -> None:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    metadata = path.lstat()
    if not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.getuid():
        raise RuntimeError(f"unsafe tmux attachment runtime directory: {path}")
    path.chmod(0o700)


def _runtime_root() -> Path:
    base = os.environ.get("XDG_RUNTIME_DIR")
    if base:
        root = Path(base)
        metadata = root.lstat()
        if not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.getuid():
            raise RuntimeError(f"unsafe XDG runtime directory: {root}")
    else:
        root = Path(f"/tmp/worker-harness-{os.getuid()}")
        _ensure_private_directory(root)
    result = root / "worker-harness" / "tmux-attachments"
    for child in (root / "worker-harness", result, result / "locks"):
        _ensure_private_directory(child)
    return result


def _tmux_socket_identity() -> str:
    tmux = _validate_text(os.environ.get("TMUX"), "TMUX environment")
    return tmux.split(",", 1)[0]


@contextmanager
def _attachment_lock(target_session: str, pi_session_id: str) -> Iterator[None]:
    identity = "\0".join((_tmux_socket_identity(), target_session, pi_session_id))
    digest = hashlib.sha256(identity.encode("utf8")).hexdigest()
    path = _runtime_root() / "locks" / f"{digest}.lock"
    flags = os.O_CREAT | os.O_RDWR | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.getuid():
            raise RuntimeError(f"unsafe tmux attachment lock: {path}")
        os.fchmod(descriptor, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _pid_is_live(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _find_reusable_window(target_session: str, pi_session_id: str) -> str | None:
    result = _run_tmux(
        [
            "list-panes",
            "-s",
            "-t",
            target_session,
            "-F",
            "#{pane_id}\t#{window_id}\t#{@wh_pi_attach_session}\t"
            "#{@wh_pi_owned}\t#{pane_dead}\t#{pane_pid}",
        ],
        check=False,
    )
    if result.returncode != 0:
        return None
    for line in result.stdout.splitlines():
        fields = line.split("\t")
        if len(fields) != 6:
            continue
        pane_id, window_id, attached_id, owned, pane_dead, pane_pid = fields
        if not _PANE_ID.fullmatch(pane_id) or not _WINDOW_ID.fullmatch(window_id):
            continue
        try:
            pid = int(pane_pid)
        except ValueError:
            continue
        if (
            attached_id == pi_session_id
            and owned == "1"
            and pane_dead != "1"
            and _pid_is_live(pid)
        ):
            return window_id
    return None


def _focus_client(target_client: str, window_id: str) -> None:
    _run_tmux(["switch-client", "-c", target_client, "-t", window_id])


def _set_pane_markers(pane_id: str, pi_session_id: str) -> None:
    for option, value in (
        ("@wh_pi_owned", "1"),
        ("@wh_pi_attach_session", pi_session_id),
        ("@wh_pi_attach_mode", "stream"),
    ):
        _run_tmux(["set-option", "-p", "-t", pane_id, option, value])


def _window_is_owned(window_id: str) -> bool:
    if not _WINDOW_ID.fullmatch(window_id):
        return False
    result = _run_tmux(
        ["show-options", "-wv", "-t", window_id, "@wh_pi_owned"],
        check=False,
    )
    return result.returncode == 0 and result.stdout.strip() == "1"


def _set_window_state(window_id: str, name: str, state: object) -> None:
    normalized = _normalize_state(state)
    for args in (
        ["set-option", "-w", "-t", window_id, "automatic-rename", "off"],
        ["set-option", "-w", "-t", window_id, "@wh_pi_owned", "1"],
        ["set-option", "-w", "-t", window_id, "@wh_pi_state", normalized],
        ["set-option", "-w", "-t", window_id, "@wh_pi_color", _STATE_COLORS[normalized]],
        ["rename-window", "-t", window_id, tab_title(name, normalized)],
    ):
        _run_tmux(args)


def open_or_focus_attachment_window(
    selected: Mapping[str, Any],
    target_session: str,
    target_client: str,
) -> str:
    """Reuse or create one WH-owned attachment window, then focus one client."""

    target_session, target_client = validate_attachment_target(
        target_session, target_client
    )
    pi_session_id = _validate_text(
        selected.get("id"), "Pi session ID", reject_option=True
    )
    name = str(selected.get("name") or selected.get("task") or "Pi")
    state = _normalize_state(selected.get("state"))

    with _attachment_lock(target_session, pi_session_id):
        reusable = _find_reusable_window(target_session, pi_session_id)
        if reusable is not None:
            _set_window_state(reusable, name, state)
            _focus_client(target_client, reusable)
            return reusable

        child = [
            _wh_executable(),
            "attach",
            "--tmux-child",
            f"--session-name={name}",
            f"--session-state={state}",
            pi_session_id,
        ]
        ready_channel = f"wh-pi-attach-{uuid.uuid4()}"
        gated_child = [
            "sh",
            "-c",
            'tmux wait-for "$1"; shift; exec "$@"',
            "wh-pi-child",
            ready_channel,
            *child,
        ]
        result = _run_tmux(
            [
                "new-window",
                "-d",
                "-P",
                "-F",
                "#{window_id}\t#{pane_id}",
                "-t",
                f"{target_session}:",
                "-n",
                tab_title(name, state),
                "-c",
                os.getcwd(),
                shlex.join(gated_child),
            ]
        )
        fields = result.stdout.strip().split("\t")
        if (
            len(fields) != 2
            or not _WINDOW_ID.fullmatch(fields[0])
            or not _PANE_ID.fullmatch(fields[1])
        ):
            raise RuntimeError("tmux new-window returned an invalid window locator")
        window_id, pane_id = fields
        try:
            _set_pane_markers(pane_id, pi_session_id)
            _set_window_state(window_id, name, state)
        except BaseException:
            _run_tmux(["kill-window", "-t", window_id], check=False)
            raise
        _run_tmux(["wait-for", "-S", ready_channel])
        _focus_client(target_client, window_id)
        return window_id


def current_attachment_window() -> str | None:
    """Return the current window ID only when it is Worker Harness-owned."""

    pane_id = os.environ.get("TMUX_PANE", "")
    if not _PANE_ID.fullmatch(pane_id):
        return None
    result = _run_tmux(
        [
            "display-message",
            "-p",
            "-t",
            pane_id,
            "#{window_id}\t#{@wh_pi_owned}",
        ],
        check=False,
    )
    if result.returncode != 0:
        return None
    fields = result.stdout.strip().split("\t")
    if len(fields) != 2 or not _WINDOW_ID.fullmatch(fields[0]) or fields[1] != "1":
        return None
    return fields[0]


def update_attachment_window(
    name: str,
    state: object,
    window_id: str | None = None,
) -> bool:
    """Update one owned attachment window; never rename an ordinary window."""

    target = window_id or current_attachment_window()
    if target is None or not _window_is_owned(target):
        return False
    _set_window_state(target, name, state)
    return True


__all__ = [
    "DISCONNECTED_COLOR",
    "ERROR_COLOR",
    "IDLE_COLOR",
    "WORKING_COLOR",
    "current_attachment_window",
    "is_immediate_tmux",
    "open_or_focus_attachment_window",
    "update_attachment_window",
]
