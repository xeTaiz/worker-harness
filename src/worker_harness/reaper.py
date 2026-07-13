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


async def _reap_once(app) -> None:
    from .metrics import get_metrics
    metrics = get_metrics()

    # 1. Reap dead tunnels
    tunnels = getattr(app.state, "tunnels", None)
    reaped_tunnels = 0
    if tunnels is not None:
        reaped_tunnels = tunnels.reap_dead()
        metrics.reaped_tunnels_total.inc(reaped_tunnels)

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
