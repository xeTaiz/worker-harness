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
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from rich.console import Console

from .config import Config
from .db import Database
from .heartbeat import run_heartbeat_server

console = Console()


async def serve(config: Config) -> None:
    """Run the heartbeat HTTP server only."""
    db = Database(config.db_path)
    await db.connect()
    console.print(f"[green]Starting heartbeat server on {config.heartbeat.host}:{config.heartbeat.port}[/]")
    console.print(f"[dim]DB: {config.db_path}[/]")

    loop = asyncio.get_event_loop()
    stop_event = asyncio.Event()

    def shutdown(signum, frame):
        console.print("\n[yellow]Shutting down...[/]")
        stop_event.set()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        # Periodically mark stale workers as offline
        async def offline_sweeper():
            while not stop_event.is_set():
                await asyncio.sleep(config.heartbeat.offline_cutoff_seconds)
                cutoff = int(asyncio.get_event_loop().time()) - config.heartbeat.offline_cutoff_seconds
                count = await db.mark_workers_offline(cutoff)
                if count > 0:
                    console.print(f"[dim]Marked {count} worker(s) offline[/]")

        sweeper_task = asyncio.create_task(offline_sweeper())
        server_task = asyncio.create_task(
            run_heartbeat_server(db, config.heartbeat.host, config.heartbeat.port)
        )

        await stop_event.wait()
        sweeper_task.cancel()
        server_task.cancel()
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
