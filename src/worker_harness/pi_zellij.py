"""Zellij tab orchestration for native Pi terminal attachments."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from worker_harness.pi_zellij_state import tab_title


def is_immediate_zellij() -> bool:
    """Return whether the invoking terminal is Zellij rather than nested tmux."""

    return bool(os.environ.get("ZELLIJ_SESSION_NAME") and not os.environ.get("TMUX"))


def _runtime_root() -> Path:
    runtime = os.environ.get("XDG_RUNTIME_DIR") or f"/tmp/worker-harness-{os.getuid()}"
    return Path(runtime) / "worker-harness" / "zellij-attachments"


def _marker_key(zellij_session: str, session_id: str) -> str:
    return hashlib.sha256(f"{zellij_session}\0{session_id}".encode()).hexdigest()


def _session_marker_path(zellij_session: str, session_id: str) -> Path:
    return _runtime_root() / "by-session" / f"{_marker_key(zellij_session, session_id)}.json"


def _session_lock_path(zellij_session: str, session_id: str) -> Path:
    return _runtime_root() / "locks" / f"{_marker_key(zellij_session, session_id)}.lock"


def _private_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.parent.chmod(0o700)


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    _private_parent(path)
    temporary = path.with_suffix(f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf8")
    temporary.chmod(0o600)
    temporary.replace(path)


def _run_zellij(args: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            ["zellij", "action", *args],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=5,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError(f"Zellij action unavailable: {exc}") from exc
    if check and result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or f"exit code {result.returncode}"
        raise RuntimeError(f"Zellij {' '.join(args[:2])} failed: {detail}")
    return result


def list_panes() -> list[dict[str, Any]]:
    result = _run_zellij(["list-panes", "--json", "--all"], check=False)
    if result.returncode != 0:
        return []
    try:
        panes = json.loads(result.stdout)
    except json.JSONDecodeError:
        return []
    return panes if isinstance(panes, list) else []


def current_tab_context() -> tuple[int, str] | None:
    raw_pane_id = os.environ.get("ZELLIJ_PANE_ID", "")
    if not raw_pane_id:
        return None
    pane_id = raw_pane_id.removeprefix("terminal_")
    pane = next((
        item for item in list_panes()
        if not item.get("is_plugin") and str(item.get("id")) == pane_id
    ), None)
    if pane is None:
        return None
    try:
        return int(pane["tab_id"]), f"terminal_{pane_id}"
    except (KeyError, TypeError, ValueError):
        return None


def _pid_is_live(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except (ProcessLookupError, PermissionError):
        return False
    return True


def _read_marker(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _pane_matches_marker(marker: dict[str, Any], panes: list[dict[str, Any]]) -> bool:
    try:
        tab_id = int(marker.get("tab_id"))
    except (TypeError, ValueError):
        return False
    raw_pane = str(marker.get("pane_id") or "").removeprefix("terminal_")
    if raw_pane:
        return any(
            not pane.get("is_plugin")
            and str(pane.get("id")) == raw_pane
            and int(pane.get("tab_id", -1)) == tab_id
            for pane in panes
        )
    # Launch reservations have a tab ID before the child knows its pane ID.
    return any(int(pane.get("tab_id", -1)) == tab_id for pane in panes)


def find_attachment_tab(session_id: str) -> int | None:
    zellij_session = os.environ.get("ZELLIJ_SESSION_NAME", "")
    if not zellij_session:
        return None
    path = _session_marker_path(zellij_session, session_id)
    marker = _read_marker(path)
    if marker is None:
        return None
    if str(marker.get("session_id") or "") != session_id:
        path.unlink(missing_ok=True)
        return None
    pid = int(marker.get("pid") or 0)
    launching = marker.get("mode") == "launching" and time.time() - float(marker.get("created_at") or 0) < 10
    if not launching and not _pid_is_live(pid):
        path.unlink(missing_ok=True)
        return None
    if not _pane_matches_marker(marker, list_panes()):
        path.unlink(missing_ok=True)
        return None
    return int(marker["tab_id"])


def mark_current_attachment(session_id: str) -> tuple[int, str] | None:
    zellij_session = os.environ.get("ZELLIJ_SESSION_NAME", "")
    context = current_tab_context()
    if not zellij_session or context is None:
        return None
    tab_id, pane_id = context
    with _session_lock(zellij_session, session_id):
        _atomic_json(_session_marker_path(zellij_session, session_id), {
            "session_id": session_id,
            "zellij_session_name": zellij_session,
            "tab_id": tab_id,
            "pane_id": pane_id,
            "pid": os.getpid(),
            "mode": "stream",
            "created_at": time.time(),
        })
    return context


def unmark_attachment(session_id: str, *, pid: int | None = None) -> None:
    zellij_session = os.environ.get("ZELLIJ_SESSION_NAME", "")
    if not zellij_session:
        return
    path = _session_marker_path(zellij_session, session_id)
    owner_pid = os.getpid() if pid is None else pid
    with _session_lock(zellij_session, session_id):
        marker = _read_marker(path)
        if marker is not None and int(marker.get("pid") or 0) not in {0, owner_pid}:
            return
        path.unlink(missing_ok=True)


@contextmanager
def _session_lock(zellij_session: str, session_id: str) -> Iterator[None]:
    path = _session_lock_path(zellij_session, session_id)
    _private_parent(path)
    with path.open("a+", encoding="utf8") as lock:
        os.fchmod(lock.fileno(), 0o600)
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def focus_tab(tab_id: int) -> None:
    _run_zellij(["go-to-tab-by-id", str(tab_id)])


def rename_tab(tab_id: int, name: str, state: str) -> None:
    _run_zellij(["rename-tab", "--tab-id", str(tab_id), tab_title(name, state)])


def _wh_executable() -> str:
    return shutil.which("wh") or sys.argv[0]


def open_or_focus_attachment_tab(
    selected: dict[str, Any],
    *,
    loopback: bool = False,
) -> int:
    """Focus an existing attachment tab or create one for ``selected``."""

    zellij_session = os.environ.get("ZELLIJ_SESSION_NAME", "")
    if not zellij_session:
        raise RuntimeError("a Zellij client is required to open an attachment tab")
    session_id = str(selected.get("id") or "")
    if not session_id:
        raise RuntimeError("Pi session ID is required")
    name = str(selected.get("name") or selected.get("task") or "Pi")
    state = str(selected.get("state") or "disconnected")
    with _session_lock(zellij_session, session_id):
        existing = find_attachment_tab(session_id)
        if existing is not None:
            focus_tab(existing)
            return existing
        command = [
            "new-tab",
            "--name",
            tab_title(name, state),
            "--cwd",
            os.getcwd(),
            "--close-on-exit",
            "--",
            _wh_executable(),
            "pi",
            "attach",
            "--here",
            f"--session-name={name}",
            f"--session-state={state}",
        ]
        if loopback:
            command.append("--loopback")
        command.append(session_id)
        result = _run_zellij(command)
        try:
            tab_id = int(result.stdout.strip().splitlines()[-1])
        except (IndexError, ValueError) as exc:
            raise RuntimeError("Zellij did not return the created attachment tab ID") from exc
        _atomic_json(_session_marker_path(zellij_session, session_id), {
            "session_id": session_id,
            "zellij_session_name": zellij_session,
            "tab_id": tab_id,
            "pane_id": "",
            "pid": 0,
            "mode": "launching",
            "created_at": time.time(),
        })
        return tab_id


__all__ = [
    "is_immediate_zellij",
    "list_panes",
    "current_tab_context",
    "find_attachment_tab",
    "mark_current_attachment",
    "unmark_attachment",
    "focus_tab",
    "rename_tab",
    "open_or_focus_attachment_tab",
]
