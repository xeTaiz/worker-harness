"""Bounded FIFO per-worker SSH concurrency lanes.

Each worker gets a small independent lane:
- at most ``max_concurrent`` SSH round-trips execute concurrently (default 4);
- further requests wait in a bounded FIFO queue (default 32 waiters);
- a caller waits at most ``timeout`` seconds, then receives ``LaneTimeout``.

The lane is held only while an SSH round-trip is in progress. A six-hour tmux
job therefore holds a lane for only the brief command that creates it, not for
its full runtime.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Deque


class LaneTimeout(Exception):
    """The bounded FIFO lane could not be acquired in time."""

    def __init__(self, worker_id: str, waited_seconds: float, *, queue_full: bool = False) -> None:
        message = (
            f"Lane queue for worker {worker_id!r} is full"
            if queue_full
            else f"Lane for worker {worker_id!r} not acquired within {waited_seconds:.1f}s"
        )
        super().__init__(message)
        self.worker_id = worker_id
        self.waited_seconds = waited_seconds
        self.queue_full = queue_full


@dataclass
class _WorkerLaneState:
    available: int
    max_queue: int
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    waiters: Deque[asyncio.Future[None]] = field(default_factory=deque)
    in_use: int = 0
    timeouts_total: int = 0
    acquires_total: int = 0


@dataclass(frozen=True)
class Lane:
    """Metadata returned from ``async with lanes.acquire(...) as lane``."""

    worker_id: str
    waited_seconds: float


class WorkerLanes:
    """Registry of independent, bounded FIFO SSH lanes keyed by worker id."""

    def __init__(self, *, max_concurrent: int = 4, max_queue: int = 32) -> None:
        if max_concurrent < 1:
            raise ValueError("max_concurrent must be >= 1")
        if max_queue < 0:
            raise ValueError("max_queue must be >= 0")
        self._max_concurrent = max_concurrent
        self._max_queue = max_queue
        self._states: dict[str, _WorkerLaneState] = {}
        self._create_lock = asyncio.Lock()

    async def _state_for(self, worker_id: str) -> _WorkerLaneState:
        state = self._states.get(worker_id)
        if state is not None:
            return state
        async with self._create_lock:
            state = self._states.get(worker_id)
            if state is None:
                state = _WorkerLaneState(
                    available=self._max_concurrent,
                    max_queue=self._max_queue,
                )
                self._states[worker_id] = state
            return state

    @staticmethod
    def _release_locked(state: _WorkerLaneState) -> None:
        """Release one held slot or hand it directly to the next FIFO waiter.

        ``state.lock`` must be held. Keeping ``in_use`` unchanged on handoff
        avoids a transient false-free capacity while a queued caller wakes.
        """
        while state.waiters:
            waiter = state.waiters.popleft()
            if not waiter.done():
                waiter.set_result(None)
                return
        state.in_use -= 1
        state.available += 1

    async def _abandon_waiter(self, state: _WorkerLaneState, waiter: asyncio.Future[None]) -> bool:
        """Remove a timed-out/cancelled waiter.

        Returns True when the waiter already received a handoff, in which case
        this method releases that slot to the next waiter rather than leaking it.
        """
        async with state.lock:
            try:
                state.waiters.remove(waiter)
            except ValueError:
                # It was popped by _release_locked. If done, it owns a slot.
                if waiter.done() and not waiter.cancelled():
                    self._release_locked(state)
                    return True
            else:
                waiter.cancel()
            return False

    @asynccontextmanager
    async def acquire(self, worker_id: str, *, timeout: float = 30.0):
        """Acquire one SSH slot for ``worker_id``.

        Raises ``LaneTimeout`` on a full queue or a timed-out wait. The queue is
        FIFO, bounded, and independent per worker: a dead ``kw996`` cannot
        delay SSH work for ``gpu-rig-1``.
        """
        state = await self._state_for(worker_id)
        started = time.monotonic()
        waiter: asyncio.Future[None] | None = None
        owns_slot = False

        async with state.lock:
            # Preserve FIFO: once anyone is queued, later requests must queue too.
            if state.available > 0 and not state.waiters:
                state.available -= 1
                state.in_use += 1
                owns_slot = True
            else:
                if len(state.waiters) >= state.max_queue:
                    state.timeouts_total += 1
                    self._record_timeout()
                    raise LaneTimeout(worker_id, 0.0, queue_full=True)
                waiter = asyncio.get_running_loop().create_future()
                state.waiters.append(waiter)

        if waiter is not None:
            try:
                await asyncio.wait_for(asyncio.shield(waiter), timeout=timeout)
                owns_slot = True
            except asyncio.TimeoutError:
                waited = time.monotonic() - started
                await self._abandon_waiter(state, waiter)
                async with state.lock:
                    state.timeouts_total += 1
                self._record_timeout()
                raise LaneTimeout(worker_id, waited) from None
            except asyncio.CancelledError:
                await self._abandon_waiter(state, waiter)
                raise

        waited = time.monotonic() - started
        state.acquires_total += 1
        self._record_acquire(waited)
        try:
            yield Lane(worker_id=worker_id, waited_seconds=waited)
        finally:
            if owns_slot:
                async with state.lock:
                    self._release_locked(state)

    @staticmethod
    def _record_acquire(waited_seconds: float) -> None:
        try:
            from .metrics import get_metrics
            metrics = get_metrics()
            metrics.lane_acquires_total.inc()
            metrics.lane_wait_ms.observe(waited_seconds * 1000)
        except Exception:
            # Observability must never make a request fail.
            pass

    @staticmethod
    def _record_timeout() -> None:
        try:
            from .metrics import get_metrics
            get_metrics().lane_timeouts_total.inc()
        except Exception:
            pass

    def stats(self) -> dict[str, dict]:
        """JSON-serializable per-worker queue state for /api/v1/_stats."""
        return {
            worker_id: {
                "in_use": state.in_use,
                "queue_depth": len(state.waiters),
                "acquires_total": state.acquires_total,
                "timeouts_total": state.timeouts_total,
            }
            for worker_id, state in self._states.items()
        }

    async def shutdown(self) -> None:
        """Cancel queued waiters during app shutdown; in-flight calls clean up
        themselves through their SSH function ``finally`` blocks."""
        for state in self._states.values():
            async with state.lock:
                while state.waiters:
                    waiter = state.waiters.popleft()
                    if not waiter.done():
                        waiter.cancel()
        self._states.clear()