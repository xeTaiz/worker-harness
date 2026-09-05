"""Herdr machines: the fleet of user workstations that host interactive agents.

A *machine* is not a *worker*. Workers (``models.Worker``) are GPU compute nodes
reached through ``tailscale ssh`` and running the worker daemon. Machines are
personal workstations that run a headless herdr server plus the forced-command
``wh-remote-shim``; the harness reaches them with plain OpenSSH and a dedicated
service key, and every remote verb is allowlisted by the shim.
"""

from __future__ import annotations

import logging
import os
import tomllib
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger(__name__)

DEFAULT_MACHINES_PATH = Path("~/.config/worker-harness/machines.toml").expanduser()
DEFAULT_KEY_PATH = Path("~/.ssh/wh_herdr").expanduser()


@dataclass(frozen=True)
class Machine:
    name: str
    ssh_target: str  # OpenSSH destination, e.g. "dome@desktop.hs.d0me.xyz"
    home: str
    herdr_session: str = "wh"
    key_path: str = str(DEFAULT_KEY_PATH)

    @property
    def cache_root(self) -> str:
        """Root of everything the shim is allowed to write on this machine."""
        return f"{self.home.rstrip('/')}/.cache/worker-harness"

    def session_dir(self, session_id: str) -> str:
        return f"{self.cache_root}/{session_id}"

    def worktree_dir(self, repo_name: str, slug: str) -> str:
        return f"{self.cache_root}/worktrees/{repo_name}/{slug}"


def load_machines(path: Path | str | None = None) -> dict[str, Machine]:
    """Load ``machines.toml``. A missing file is a warning, never a crash —
    the harness still serves workers, jobs, and the web app without a fleet."""
    configured_path = os.environ.get("WH_MACHINES_PATH", "").strip()
    resolved = Path(path if path is not None else configured_path or DEFAULT_MACHINES_PATH).expanduser()
    default_key = os.environ.get("WH_HERDR_KEY", "").strip() or str(DEFAULT_KEY_PATH)
    try:
        raw = resolved.read_bytes()
    except FileNotFoundError:
        log.warning("No herdr machines file at %s; agent orchestration disabled", resolved)
        return {}
    except OSError as exc:
        log.warning("Cannot read herdr machines file %s: %s", resolved, exc)
        return {}

    try:
        document = tomllib.loads(raw.decode())
    except (tomllib.TOMLDecodeError, UnicodeDecodeError) as exc:
        log.error("Invalid herdr machines file %s: %s", resolved, exc)
        return {}

    machines: dict[str, Machine] = {}
    entries = document.get("machine", {})
    if not isinstance(entries, dict):
        log.error("machines.toml: machine must be a table")
        return {}
    for name, entry in entries.items():
        if not isinstance(entry, dict):
            log.error("machines.toml: [machine.%s] is not a table", name)
            continue
        ssh_target = str(entry.get("ssh_target", "")).strip()
        home = str(entry.get("home", "")).strip()
        if not ssh_target or ssh_target.startswith("-") or not home.startswith("/") or home == "/":
            log.error("machines.toml: [machine.%s] needs ssh_target and an absolute home", name)
            continue
        machines[name] = Machine(
            name=name,
            ssh_target=ssh_target,
            home=home.rstrip("/"),
            herdr_session=str(entry.get("herdr_session", "wh")).strip() or "wh",
            key_path=str(Path(str(entry.get("key_path", default_key))).expanduser()),
        )
    return machines


__all__ = ["Machine", "load_machines", "DEFAULT_MACHINES_PATH", "DEFAULT_KEY_PATH"]
