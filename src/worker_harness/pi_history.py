"""Version-checked, target-local access to Pi session history metadata."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any


def _host_runtime():
    from worker_harness.host_runtime import HostRuntimeError, load_host_runtime

    try:
        return load_host_runtime(required=False)
    except HostRuntimeError as exc:
        raise RuntimeError(f"host runtime manifest is invalid: {exc}") from exc


def _executable(name: str, environment_name: str) -> Path:
    configured = os.environ.get(environment_name)
    runtime = _host_runtime()
    value = configured or (runtime.executable(name) if runtime else None) or shutil.which(name)
    if not value:
        raise RuntimeError(
            f"{name} is required for Pi history; run `wh host setup` from a prepared shell"
        )
    path = Path(value).expanduser()
    if not path.is_file() or not os.access(path, os.X_OK):
        raise RuntimeError(f"{name} executable is invalid: {path}")
    return path.resolve()


def _pi_package_root(pi_executable: Path) -> Path:
    # npm/Bun global bins resolve to <package>/dist/cli.js.  Compiled standalone
    # binaries intentionally fail closed because they cannot expose the pinned
    # SessionManager API used by the helper.
    if pi_executable.name not in {"cli.js", "cli.mjs"} or pi_executable.parent.name != "dist":
        raise RuntimeError(
            "installed Pi layout cannot provide SessionManager.list(); "
            "install the >=0.83.0 package distribution"
        )
    root = pi_executable.parent.parent
    if not (root / "package.json").is_file() or not (root / "dist" / "index.js").is_file():
        raise RuntimeError("installed Pi package is incomplete")
    return root


def _helper_path() -> Path:
    path = Path(__file__).with_name("pi_history_helper.mjs")
    if not path.is_file():
        raise RuntimeError("Worker Harness Pi history helper is missing")
    return path


def _run_helper(cwd: str, session_id: str | None = None) -> Any:
    cwd_path = Path(cwd).expanduser()
    if not cwd_path.is_absolute():
        raise RuntimeError("Pi history cwd must be absolute")
    cwd_path = cwd_path.resolve(strict=True)
    if not cwd_path.is_dir():
        raise RuntimeError("Pi history cwd must be a directory")
    if session_id is not None and (
        not session_id or any(character in session_id for character in "\0\r\n")
    ):
        raise RuntimeError("exact Pi session ID is invalid")

    bun = _executable("bun", "WH_BUN_EXECUTABLE")
    pi = _executable("pi", "WH_PI_EXECUTABLE")
    package_root = _pi_package_root(pi)
    command = [
        str(bun),
        str(_helper_path()),
        "resolve" if session_id is not None else "list",
        str(package_root),
        str(cwd_path),
    ]
    if session_id is not None:
        command.append(session_id)
    runtime = _host_runtime()
    environment = runtime.environment() if runtime else dict(os.environ)
    try:
        result = subprocess.run(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
            timeout=20,
            env=environment,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError(f"could not inspect Pi history: {exc}") from exc
    if result.returncode != 0:
        detail = (result.stderr.strip() or result.stdout.strip() or "unknown helper failure")[-2000:]
        raise RuntimeError(f"could not inspect Pi history: {detail}")
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Pi history helper returned malformed JSON") from exc


def list_session_history(cwd: str) -> list[dict[str, Any]]:
    """List bounded metadata from SessionManager.list(cwd)."""

    rows = _run_helper(cwd)
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise RuntimeError("Pi history helper returned an invalid session list")
    return rows


def resolve_session_history(cwd: str, session_id: str) -> dict[str, Any]:
    """Re-resolve one exact opaque ID under the target cwd before resume."""

    row = _run_helper(cwd, session_id)
    if not isinstance(row, dict) or str(row.get("id") or "") != session_id:
        raise RuntimeError("Pi history helper did not resolve the exact session ID")
    return row
