"""Host-side pull request validation and creation for fleet task agents."""

from __future__ import annotations

import re

from .herdr_host import shim, write_file
from .machines import Machine
from .projects import Project

PR_SECTIONS = (
    "## Task",
    "## Planned implementation",
    "## Deviations and issues",
    "## Surprises and decisions",
    "## Outcome",
    "## Change size and architecture impact",
)


class PrRejected(Exception):
    """A proposed PR summary does not satisfy the required template."""

    def __init__(self, missing: list[str]) -> None:
        self.missing = missing
        super().__init__(f"missing required PR sections: {', '.join(missing)}")


def missing_sections(summary: str) -> list[str]:
    """Return required section headings absent from ``summary``."""
    lines = summary.splitlines()
    return [
        section
        for section in PR_SECTIONS
        if not any(re.fullmatch(rf"{re.escape(section)}\s*", line) for line in lines)
    ]


def _task_title(summary: str) -> str:
    in_task = False
    for line in summary.splitlines():
        if re.fullmatch(rf"{re.escape(PR_SECTIONS[0])}\s*", line):
            in_task = True
            continue
        if in_task and line.startswith("## "):
            break
        if in_task and line.strip():
            return line.strip()[:72]
    raise ValueError("PR summary has no title text under ## Task")


async def open_pr(
    machine: Machine,
    project: Project,
    *,
    branch: str,
    worktree: str,
    summary: str,
    session_id: str,
) -> str:
    """Push a reviewed task branch and open its pull request outside the sandbox."""
    missing = missing_sections(summary)
    if missing:
        raise PrRejected(missing)

    title = _task_title(summary)
    session_dir = machine.session_dir(session_id)
    title_file = f"{session_dir}/pr-title.txt"
    body_file = f"{session_dir}/pr-body.md"

    await shim(machine, "push", worktree, branch)
    await write_file(machine, title_file, title + "\n")
    await write_file(machine, body_file, summary)
    output = await shim(
        machine,
        "pr-create",
        project.remote,
        branch,
        project.base_branch,
        title_file,
        body_file,
    )

    pattern = re.compile(rf"https://github\.com/{re.escape(project.remote)}/pull/[1-9][0-9]*")
    urls = [line.strip() for line in output.splitlines() if pattern.fullmatch(line.strip())]
    if not urls:
        raise RuntimeError("pr-create succeeded without returning a PR URL for the project")
    return urls[-1]


__all__ = ["PR_SECTIONS", "PrRejected", "missing_sections", "open_pr"]
