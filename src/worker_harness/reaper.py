"""Background tunnel reaper.

Runs every 60s and walks the in-memory tunnel registry. If a persistent
port-forward died but was not explicitly removed, its registry entry is
reaped. Normal SSH requests are cleaned synchronously by ssh.py's ``finally``
blocks; we deliberately do *not* use a broad ``ps | kill ssh`` scan because
that could kill a legitimate in-flight request.

A systemd-managed server already has ``KillMode=control-group``, so a server
crash/restart kills its complete process tree. Together with ssh.py cleanup,
this removes the zombie paths without unsafe global process matching.
"""

from __future__ import annotations

import asyncio
import logging
import time

log = logging.getLogger("reaper")


async def reap_loop(app, interval_seconds: float = 60.0) -> None:
    """Main loop. Run as an asyncio.Task."""
    while True:
        try:
            await asyncio.sleep(interval_seconds)
        except asyncio.CancelledError:
            log.info("reaper: cancelled, exiting")
            return
        try:
            await _reap_once(app)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.exception(f"reaper: tick failed: {e}")


async def release_orphan_forwards(app, tunnel_ids: list[str] | None = None) -> int:
    """Delete persisted generic forwards whose listener process is gone.

    ``tunnel_ids`` are reaped registry entries; ``None`` releases every
    generic row this process does not own (startup after a restart/crash).
    Managed-notebook forwards keep their row (the notebook record owns it and
    its owner stops the notebook); their port stays excluded from allocation.
    """
    db = app.state.db
    live = app.state.tunnels.ids()
    released = 0
    for pf in await db.list_port_forwards():
        if pf.marimo_session_id is not None or pf.id in live:
            continue
        if tunnel_ids is not None and pf.id not in tunnel_ids:
            continue
        await db.delete_port_forward(pf.id)
        await app.state.cache.invalidate(f"tunnels:{pf.owner_user_id}")
        released += 1
    return released


async def _reap_once(app) -> None:
    from .metrics import get_metrics
    metrics = get_metrics()

    # 1. Reap dead tunnels and release their listener reservations.
    tunnels = getattr(app.state, "tunnels", None)
    reaped_tunnels = 0
    if tunnels is not None:
        reaped = tunnels.reap_dead()
        reaped_tunnels = len(reaped)
        metrics.reaped_tunnels_total.inc(reaped_tunnels)
        if reaped and getattr(app.state, "db", None) is not None:
            await release_orphan_forwards(app, reaped)

    # Normal SSH processes are killed by ssh.py in a request-local finally
    # block. systemd KillMode=control-group covers server crashes, so there is
    # no safe need for a global ps-based ssh killer here.
    reaped_ssh = 0

    # 2. Update last-run timestamp
    metrics.reaper_last_run_ts.set(time.time())

    if reaped_tunnels or reaped_ssh:
        log.info(
            f"reaper: killed {reaped_tunnels} tunnel(s), {reaped_ssh} ssh subprocess(es)"
        )
