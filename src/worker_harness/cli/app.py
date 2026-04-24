"""Worker Harness CLI — Typer-based command-line interface."""

from __future__ import annotations

import asyncio
import logging
import sys
from contextlib import asynccontextmanager
from typing import AsyncIterator

import typer
from rich.console import Console
from rich.logging import RichHandler

console = Console()

# Global state shared across commands
_state: dict = {}


def get_config():
    from worker_harness.config import Config
    return _state.setdefault("config", Config.load())


def get_db():
    from worker_harness.db import Database
    if "db" not in _state:
        _state["db"] = Database(get_config().db_path)
    return _state["db"]


def get_job_manager():
    from worker_harness.job import JobManager
    if "job_manager" not in _state:
        _state["job_manager"] = JobManager(get_db())
    return _state["job_manager"]


@asynccontextmanager
async def db_lifespan() -> AsyncIterator[None]:
    """Connect/disconnect DB for each CLI invocation."""
    db = get_db()
    await db.connect()
    try:
        yield
    finally:
        await db.close()


app = typer.Typer(
    name="worker-harness",
    help="Worker Harness — orchestrate GPU/ML workers over ZeroTier VPN",
    add_completion=False,
)


@app.callback()
def main(
    ctx: typer.Context,
    output: str = typer.Option("text", "--output", "-o",
                               help="Output format: text (default) or json"),
    verbose: bool = typer.Option(False, "--verbose", "-v",
                                 help="Enable verbose logging"),
):
    """Root command. Use `--output json` for machine-readable output."""
    config = get_config()
    level = "DEBUG" if verbose else config.logging.level
    logging.basicConfig(
        level=level,
        format="%(message)s",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
    )
    _state["output"] = output


def main_entry():
    """Entry point installed as the `worker-harness` console script."""
    # Register subcommands lazily to avoid circular imports
    from worker_harness.cli import workers, jobs, tunnels, agent as agent_mod
    app.add_typer(workers.app, name="workers")
    app.add_typer(jobs.app, name="job")
    app.add_typer(tunnels.app, name="tunnel")
    app.add_typer(agent_mod.app, name="agent")
    app()
