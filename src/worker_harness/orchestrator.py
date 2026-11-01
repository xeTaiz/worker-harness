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

        # A server that fails (e.g. invalid configuration or a busy port) must
        # stop the process instead of leaving it half-running.
        stop_task = asyncio.create_task(stop_event.wait())
        servers = {registration_task, control_task}
        done, _ = await asyncio.wait({stop_task, *servers}, return_when=asyncio.FIRST_COMPLETED)
        for task in (sweeper_task, registration_task, control_task, stop_task):
            task.cancel()
        failed = [task for task in servers if task in done]
        if failed and not stop_event.is_set():
            name = "registration" if failed[0] is registration_task else "control"
            raise RuntimeError(f"{name} server stopped") from failed[0].exception()
    finally:
        await db.close()


def run_tui(config: Config) -> None:
    """Run the Textual TUI against the authenticated control API."""
    from .client import request
    from .tui.app import run_tui as run_textual_tui

    console.print("[green]Starting Worker Harness TUI...[/]")
    console.print("[dim]Press ? for key bindings[/]")
    run_textual_tui(request)


async def bootstrap_admin(db_path: Path, username: str, password: str) -> dict:
    """Create the first administrator and assign legacy records to it."""
    from .auth import PasswordService, normalize_username, validate_password

    username = normalize_username(username)
    validate_password(password)
    password_hash = await PasswordService().hash(password)
    db = Database(db_path)
    await db.connect()
    try:
        return await db.bootstrap_admin(
            username=username, display_name=username, password_hash=password_hash,
            now=int(time.time()),
        )
    finally:
        await db.close()


def run_bootstrap_admin(config: Config, username: str) -> int:
    """Interactive bootstrap; passwords are never accepted from argv/env."""
    import getpass

    from .db import IdentityConflict

    password = getpass.getpass("Bootstrap administrator password: ")
    confirmation = getpass.getpass("Repeat password: ")
    if password != confirmation:
        console.print("[red]Passwords do not match.[/]")
        return 2
    try:
        user = asyncio.run(bootstrap_admin(Path(config.db_path), username, password))
    except ValueError as exc:
        console.print(f"[red]{exc}[/]")
        return 2
    except IdentityConflict as exc:
        console.print(f"[red]Bootstrap refused: {exc}.[/]")
        return 1
    console.print(f"[green]Bootstrap administrator {user['username']} created ({user['id']}).[/]")
    return 0


def main():
    import argparse

    parser = argparse.ArgumentParser(prog="worker-harness orchestrator",
                                     description="Worker Harness Orchestrator")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("serve", help="Run heartbeat HTTP server")
    sub.add_parser("tui", help="Run TUI")
    sub.add_parser("all", help="Run heartbeat server + TUI together")
    sub.add_parser("run-server", help="Alias for serve")
    bootstrap = sub.add_parser(
        "bootstrap-admin",
        help="Create the first administrator (prompts for the password twice)",
    )
    bootstrap.add_argument("--username", required=True)

    args = parser.parse_args()

    # Load config
    config = Config.load()
    logging.basicConfig(
        level=config.logging.level,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    if args.command in ("serve", "run-server"):
        asyncio.run(serve(config))
    elif args.command == "bootstrap-admin":
        sys.exit(run_bootstrap_admin(config, args.username))
    elif args.command == "tui":
        run_tui(config)
    elif args.command == "all":
        console.print("[yellow]'all' mode: run 'serve' and 'tui' in separate terminals,[/]")
        console.print("[yellow]or implement multi-process / threading here.[/]")
        console.print("[dim]For now, starting heartbeat server. Use --tui for TUI.[/]")
        asyncio.run(serve(config))


if __name__ == "__main__":
    main()
