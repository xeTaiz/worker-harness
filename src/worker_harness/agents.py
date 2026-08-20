"""Agent-binary selection shared by the managed runtime and the CLI."""

from __future__ import annotations

import os
import shutil
import subprocess

import typer

AGENTS: tuple[str, ...] = ("pi", "omp")
AGENT_EXECUTABLE_ENV: dict[str, str] = {"pi": "WH_PI_EXECUTABLE", "omp": "WH_OMP_EXECUTABLE"}
_AGENT_DESCRIPTION: dict[str, str] = {
    "pi": "pi    upstream Pi coding agent",
    "omp": "omp   Oh My Pi",
}


def validate_agent(agent: str) -> str:
    if agent not in AGENTS:
        raise RuntimeError(f"unsupported agent {agent!r}; expected one of {', '.join(AGENTS)}")
    return agent


def row_agent(row: dict) -> str:
    """Agent recorded for one registry row; pre-upgrade rows are pi."""

    return str(row.get("agent") or "pi")


def pick_agent(explicit: str | None) -> str:
    """Explicit choice, an fzf prompt on a TTY, or the pi default."""

    if explicit:
        return validate_agent(explicit)
    fzf = shutil.which("fzf")
    if not os.isatty(0) or not fzf:
        return "pi"
    result = subprocess.run(
        [
            fzf, "--height=100%", "--layout=reverse", "--border", "--sync",
            "--read0", "--print0", "--delimiter=\\t", "--with-nth=2", "--nth=1",
            "--header=      AGENT", "--prompt=Launch agent> ",
        ],
        input="\0".join(f"{name}\t  {_AGENT_DESCRIPTION[name]}" for name in AGENTS) + "\0",
        text=True,
        stdout=subprocess.PIPE,
        check=False,
    )
    selected = result.stdout.rstrip("\0")
    if result.returncode == 130 or not selected:
        raise typer.Abort()
    if result.returncode != 0:
        raise RuntimeError(f"fzf agent picker failed with exit code {result.returncode}")
    return validate_agent(selected.split("\t", 1)[0])
