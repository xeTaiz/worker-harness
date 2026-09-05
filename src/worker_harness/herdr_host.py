"""Adapter for the per-machine herdr server, reached through the shim.

herdr is the presentation backend: it owns panes, workspaces, and the sidebar.
Everything durable — sessions, events, worktrees, PRs — lives in worker-harness.
Swapping multiplexers means reimplementing the handful of wrappers below and
nothing else.

Two rules make this reliable:

* Errors are discriminated on ``error.code`` from herdr's JSON, never on exit
  status or message text. A dead server answers ``server_not_running`` on
  *stdout or stderr* with a nonzero exit, which is indistinguishable from a
  transport failure by status alone.
* Arguments that contain whitespace travel to the shim as ``b64:`` tokens,
  because the shim word-splits ``$SSH_ORIGINAL_COMMAND`` and never eval's it.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import re
from typing import Any

from .machines import Machine
from .ssh import async_machine_run, async_machine_run_bytes

log = logging.getLogger(__name__)

# Tokens the shim can carry literally: no whitespace, no quoting, no traversal.
_LITERAL_TOKEN = re.compile(r"^[A-Za-z0-9._/@:=+,-]+$")

_COLD_START_POLL_SECONDS = 0.5
_COLD_START_TIMEOUT_SECONDS = 10.0


class HerdrUnavailable(Exception):
    """The herdr server is not running and could not be revived."""


class HerdrError(Exception):
    """herdr answered with a structured error."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


class ShimError(Exception):
    """The remote shim refused or failed the verb."""

    def __init__(self, machine: str, verb: str, returncode: int, stderr: str) -> None:
        super().__init__(f"{machine}: {verb} failed ({returncode}): {stderr.strip()}")
        self.machine = machine
        self.verb = verb
        self.returncode = returncode
        self.stderr = stderr


def encode_arg(value: str) -> str:
    """Encode one shim argument, base64-wrapping anything unsafe to word-split."""
    if "\0" in value:
        raise ValueError("shim arguments cannot contain NUL")
    if value and _LITERAL_TOKEN.fullmatch(value) and ".." not in value and not value.startswith("b64:"):
        return value
    return "b64:" + base64.b64encode(value.encode()).decode()


async def shim(
    machine: Machine,
    *args: str,
    stdin: bytes | None = None,
    timeout: float = 30.0,
    check: bool = True,
) -> str:
    """Run one allowlisted verb on ``machine`` and return its stdout."""
    if not args:
        raise ValueError("shim() needs a verb")
    command = " ".join(encode_arg(arg) for arg in args)
    result = await async_machine_run(machine, command, timeout=timeout, input_data=stdin)
    if check and result.returncode != 0:
        raise ShimError(machine.name, args[0], result.returncode, result.stderr)
    return result.stdout


def _parse_herdr(stdout: str, stderr: str, returncode: int) -> dict[str, Any]:
    """Parse one herdr CLI response.

    Side-effecting subcommands (``pane run``, ``pane close``, ``report-*``)
    print nothing at all on success, and ``pane read`` prints raw terminal text.
    Only failures and query subcommands emit JSON, and a failing server puts it
    on stderr with a nonzero exit — so silence plus exit 0 is success.
    """
    for stream in (stdout, stderr):
        text = stream.strip()
        if not text:
            continue
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            return parsed
    if returncode == 0:
        return {"result": {}}
    raise HerdrError("invalid_response", (stderr or stdout).strip()[:500] or "empty response")


async def _herdr_once(machine: Machine, args: tuple[str, ...], timeout: float) -> dict[str, Any]:
    command = " ".join(["herdr", *(encode_arg(arg) for arg in args)])
    result = await async_machine_run(machine, command, timeout=timeout)
    try:
        return _parse_herdr(result.stdout, result.stderr, result.returncode)
    except HerdrError:
        if result.returncode in (-1, 2, 127, 255):
            # Structured herdr errors take precedence over transport exit codes.
            raise ShimError(machine.name, "herdr", result.returncode, result.stderr) from None
        raise


async def herdr(machine: Machine, *args: str, timeout: float = 30.0) -> dict[str, Any]:
    """Call the machine's herdr server, reviving it once if it is down.

    Returns the ``result`` object. Raises :class:`HerdrError` for any structured
    error other than a cold server, and :class:`HerdrUnavailable` when the
    server stays down after a start attempt.
    """
    document = await _herdr_once(machine, args, timeout)
    error = document.get("error")
    if not isinstance(error, dict):
        return document.get("result") or {}

    code = str(error.get("code") or "unknown")
    if code != "server_not_running":
        raise HerdrError(code, str(error.get("message") or ""))

    log.info("herdr on %s is down; starting herdr-wh", machine.name)
    try:
        await shim(machine, "herdr-up", timeout=15.0)
    except ShimError as exc:
        raise HerdrUnavailable(f"could not start herdr server on {machine.name}: {exc}") from exc

    try:
        # Bound the whole readiness phase, including SSH and lane admission,
        # rather than granting every probe a fresh command timeout.
        async with asyncio.timeout(_COLD_START_TIMEOUT_SECONDS):
            while True:
                await asyncio.sleep(_COLD_START_POLL_SECONDS)
                probe = await _herdr_once(machine, ("api", "snapshot"), timeout)
                probe_error = probe.get("error")
                if not isinstance(probe_error, dict):
                    break
                probe_code = str(probe_error.get("code") or "unknown")
                if probe_code != "server_not_running":
                    raise HerdrError(probe_code, str(probe_error.get("message") or ""))
    except TimeoutError as exc:
        raise HerdrUnavailable(f"herdr server on {machine.name} did not come up") from exc

    document = await _herdr_once(machine, args, timeout)
    error = document.get("error")
    if not isinstance(error, dict):
        return document.get("result") or {}
    code = str(error.get("code") or "unknown")
    if code == "server_not_running":
        raise HerdrUnavailable(f"herdr server on {machine.name} did not come up")
    raise HerdrError(code, str(error.get("message") or ""))


# ── Typed wrappers ─────────────────────────────────────────────────────


async def workspace_create(machine: Machine, *, cwd: str, label: str) -> dict[str, Any]:
    """Create an unfocused workspace. Returns ``{workspace, tab, pane_id}``."""
    result = await herdr(
        machine, "workspace", "create", "--cwd", cwd, "--label", label, "--no-focus",
    )
    root_pane = result.get("root_pane") or {}
    return {
        "workspace_id": (result.get("workspace") or {}).get("workspace_id")
        or (result.get("workspace") or {}).get("id"),
        "tab_id": (result.get("tab") or {}).get("tab_id") or (result.get("tab") or {}).get("id"),
        "pane_id": root_pane.get("pane_id") or root_pane.get("id"),
        "raw": result,
    }


async def pane_run(machine: Machine, pane_id: str, command: str) -> dict[str, Any]:
    """Type ``command`` into the pane and press Enter, atomically."""
    return await herdr(machine, "pane", "run", pane_id, command)


async def pane_read(
    machine: Machine, pane_id: str, *, source: str = "recent-unwrapped", lines: int = 120,
) -> str:
    """Read pane terminal output. This subcommand prints plain text, not JSON."""
    args = ["herdr", "pane", "read", pane_id, "--source", source, "--lines", str(lines)]
    result = await async_machine_run(machine, " ".join(encode_arg(a) for a in args), timeout=30.0)
    if result.returncode != 0:
        raise HerdrError("pane_read_failed", result.stderr.strip()[:500])
    return result.stdout


async def pane_close(machine: Machine, pane_id: str) -> dict[str, Any]:
    return await herdr(machine, "pane", "close", pane_id)


async def agent_rename(machine: Machine, pane_id: str, name: str) -> dict[str, Any]:
    return await herdr(machine, "agent", "rename", pane_id, name)


async def report_agent(
    machine: Machine,
    pane_id: str,
    state: str,
    *,
    source: str,
    agent: str,
    sequence: int,
    message: str = "",
    agent_session_id: str = "",
    agent_session_path: str = "",
) -> dict[str, Any]:
    """Project authoritative lifecycle state into herdr's sidebar.

    Called from outside the sandbox: the agent itself cannot reach the socket.
    """
    args = [
        "pane", "report-agent", pane_id,
        "--source", source, "--agent", agent, "--state", state, "--seq", str(sequence),
    ]
    if message:
        args += ["--message", message]
    if agent_session_id:
        args += ["--agent-session-id", agent_session_id]
    if agent_session_path:
        args += ["--agent-session-path", agent_session_path]
    return await herdr(machine, *args)


async def report_metadata(
    machine: Machine, pane_id: str, *, source: str, agent: str, display_agent: str,
) -> dict[str, Any]:
    return await herdr(
        machine, "pane", "report-metadata", pane_id,
        "--source", source, "--agent", agent, "--display-agent", display_agent,
    )


async def release_agent(
    machine: Machine, pane_id: str, *, source: str, agent: str, sequence: int,
) -> dict[str, Any]:
    return await herdr(
        machine, "pane", "release-agent", pane_id,
        "--source", source, "--agent", agent, "--seq", str(sequence),
    )


# ── Host-side file and transcript helpers ──────────────────────────────


async def write_file(machine: Machine, path: str, content: str) -> None:
    """Write a file under the machine's worker-harness cache directory."""
    await shim(machine, "write-file", path, stdin=content.encode())


async def remove_file(machine: Machine, path: str) -> None:
    await shim(machine, "rm-file", path)


async def tail_transcript(machine: Machine, path: str, offset: int) -> tuple[bytes, int]:
    """Read from a one-based byte offset; return raw bytes and the next offset."""
    start = max(1, offset)
    chunk, result = await async_machine_run_bytes(
        machine, f"tail {encode_arg(path)} {start}", timeout=30.0,
    )
    if result.returncode != 0:
        return b"", offset
    return chunk, start + len(chunk)


__all__ = [
    "HerdrError",
    "HerdrUnavailable",
    "ShimError",
    "agent_rename",
    "encode_arg",
    "herdr",
    "pane_close",
    "pane_read",
    "pane_run",
    "release_agent",
    "remove_file",
    "report_agent",
    "report_metadata",
    "shim",
    "tail_transcript",
    "workspace_create",
    "write_file",
]
