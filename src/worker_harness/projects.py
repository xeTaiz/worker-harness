"""Projects the fleet manages: one project manager agent per entry.

The catalog lives in the orchestrator repo (``projects.toml``) so the fleet can
maintain its own configuration through the same PR gate as everything else.
"""

from __future__ import annotations

import logging
import os
import tomllib
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger(__name__)

DEFAULT_PROJECTS_PATH = Path("~/Work/agent-orchestrator/projects.toml").expanduser()


@dataclass(frozen=True)
class Project:
    name: str
    machine: str  # key into machines.toml
    repo: str  # absolute path of the main checkout on that machine
    remote: str  # GitHub slug, e.g. "xeTaiz/worker-harness"
    base_branch: str = "main"

    @property
    def repo_name(self) -> str:
        return self.repo.rstrip("/").rsplit("/", 1)[-1]


def projects_path() -> Path:
    override = os.environ.get("WH_PROJECTS_PATH", "").strip()
    if override:
        return Path(override).expanduser()
    source = os.environ.get("WH_ORCHESTRATOR_REPO", "").strip()
    return Path(source).expanduser() / "projects.toml" if source else DEFAULT_PROJECTS_PATH


def load_projects(path: Path | str | None = None) -> dict[str, Project]:
    """Load ``projects.toml``. A missing file disables dispatch, not the service."""
    resolved = Path(path).expanduser() if path is not None else projects_path()
    try:
        document = tomllib.loads(resolved.read_bytes().decode())
    except FileNotFoundError:
        log.warning("No projects file at %s; agent dispatch disabled", resolved)
        return {}
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        log.error("Invalid projects file %s: %s", resolved, exc)
        return {}

    projects: dict[str, Project] = {}
    entries = document.get("project", {})
    if not isinstance(entries, dict):
        log.error("projects.toml: project must be a table")
        return {}
    for name, entry in entries.items():
        if not isinstance(entry, dict):
            log.error("projects.toml: [project.%s] is not a table", name)
            continue
        machine = str(entry.get("machine", "")).strip()
        repo = str(entry.get("repo", "")).strip().rstrip("/")
        remote = str(entry.get("remote", "")).strip()
        if not machine or not repo.startswith("/") or "/" not in remote:
            log.error(
                "projects.toml: [project.%s] needs machine, an absolute repo, and an owner/name remote",
                name,
            )
            continue
        projects[name] = Project(
            name=name,
            machine=machine,
            repo=repo,
            remote=remote,
            base_branch=str(entry.get("base_branch", "main")).strip() or "main",
        )
    return projects


__all__ = ["DEFAULT_PROJECTS_PATH", "Project", "load_projects", "projects_path"]
