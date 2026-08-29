#!/usr/bin/env python3
"""
Worker Harness Orchestrator — main entry point.

Usage:
    worker-harness serve         # Run heartbeat server only
    worker-harness tui           # Run TUI only
    worker-harness all          # Run heartbeat server + TUI together
    worker-harness run-server    # (alias for serve)
"""

from __future__ import annotations

import asyncio
import logging
import signal
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from rich.console import Console

from .config import Config
from .db import Database
from .heartbeat import run_control_server, run_registration_server

console = Console()


async def prune_stale_workers_once(
    db: Database,
    cutoff_seconds: int,
    *,
    now: int | None = None,
) -> int:
    """Remove worker registrations that missed the heartbeat cutoff."""
    current = int(time.time()) if now is None else now
    return await db.prune_workers(current - cutoff_seconds)


async def serve(config: Config) -> None:
    """Run the heartbeat HTTP server only."""
    db = Database(config.db_path)
    await db.connect()
    console.print(f"[green]Starting registration server on {config.heartbeat.host}:{config.heartbeat.port}[/]")
    console.print(f"[green]Starting control server on {config.control.host}:{config.control.port}[/]")
    console.print(f"[dim]DB: {config.db_path}[/]")

    loop = asyncio.get_event_loop()
    stop_event = asyncio.Event()

    def shutdown(signum, frame):
        console.print("\n[yellow]Shutting down...[/]")
        stop_event.set()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        # Cluster workers are ephemeral. Remove stale registrations rather than
        # retaining an ever-growing offline inventory; a returning worker
        # re-registers with its persisted ID on its next heartbeat.
        async def stale_worker_pruner():
            while not stop_event.is_set():
                await asyncio.sleep(30)
                count = await prune_stale_workers_once(
                    db,
                    config.heartbeat.offline_cutoff_seconds,
                )
                if count > 0:
                    console.print(f"[dim]Pruned {count} stale worker(s)[/]")

        sweeper_task = asyncio.create_task(stale_worker_pruner())
        registration_task = asyncio.create_task(
            run_registration_server(db, config.heartbeat.host, config.heartbeat.port)
        )
        control_task = asyncio.create_task(
            run_control_server(db, config.control.host, config.control.port)
        )

        await stop_event.wait()
        sweeper_task.cancel()
        registration_task.cancel()
        control_task.cancel()
    finally:
        await db.close()


def run_tui(config: Config) -> None:
    """Run the Textual TUI."""
    from .tui.app import run_tui

    db = Database(config.db_path)
    asyncio.run(db.connect())

    console.print(f"[green]Starting Worker Harness TUI...[/]")
    console.print(f"[dim]DB: {config.db_path}[/]")
    console.print("[dim]Press ? for key bindings[/]")

    try:
        run_tui(db)
    finally:
        asyncio.run(db.close())


def main():
    import argparse

    parser = argparse.ArgumentParser(prog="worker-harness orchestrator",
                                     description="Worker Harness Orchestrator")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("serve", help="Run heartbeat HTTP server")
    sub.add_parser("tui", help="Run TUI")
    sub.add_parser("all", help="Run heartbeat server + TUI together")
    sub.add_parser("run-server", help="Alias for serve")

    args = parser.parse_args()

    # Load config
    config = Config.load()
    logging.basicConfig(
        level=config.logging.level,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    if args.command in ("serve", "run-server"):
        asyncio.run(serve(config))
    elif args.command == "tui":
        run_tui(config)
    elif args.command == "all":
        console.print("[yellow]'all' mode: run 'serve' and 'tui' in separate terminals,[/]")
        console.print("[yellow]or implement multi-process / threading here.[/]")
        console.print("[dim]For now, starting heartbeat server. Use --tui for TUI.[/]")
        asyncio.run(serve(config))


if __name__ == "__main__":
    main()
