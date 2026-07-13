"""Per-agent rate limiting using token bucket.

Each agent (identified by X-Agent-Name header or peer IP fallback) gets its
own bucket with capacity=10 and refill rate=1 token/second (≈60 req/min).

On exhaustion, RateLimited is raised; the FastAPI middleware translates to
HTTP 429 + Retry-After.

The bucket is process-local (not shared across server instances). For
multi-process deployments, swap in a Redis-backed implementation.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass


class RateLimited(Exception):
    """Raised when an agent exceeds its rate limit."""

    def __init__(self, agent: str, retry_after_seconds: float) -> None:
        super().__init__(
            f"Rate limit exceeded for agent {agent!r}; retry in {retry_after_seconds:.1f}s"
        )
        self.agent = agent
        self.retry_after_seconds = retry_after_seconds


@dataclass
class _Bucket:
    tokens: float
    last_refill_at: float


class TokenBucket:
    """One bucket's worth of state + math."""

    __slots__ = ("capacity", "refill_rate", "_state", "_lock")

    def __init__(self, capacity: float, refill_rate: float) -> None:
        self.capacity = capacity
        self.refill_rate = refill_rate
        self._state = _Bucket(tokens=capacity, last_refill_at=time.monotonic())
        self._lock = threading.Lock()

    def try_consume(self, n: float = 1.0) -> tuple[bool, float]:
        """Try to consume n tokens.

        Returns (ok, retry_after_seconds). retry_after_seconds is meaningful
        only when ok=False (time until 1 token is available again).
        """
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._state.last_refill_at
            self._state.tokens = min(
                self.capacity,
                self._state.tokens + elapsed * self.refill_rate,
            )
            self._state.last_refill_at = now

            if self._state.tokens >= n:
                self._state.tokens -= n
                return True, 0.0

            # Compute retry_after for the next token.
            deficit = n - self._state.tokens
            retry_after = deficit / self.refill_rate
            return False, retry_after

    def stats(self) -> dict:
        with self._lock:
            return {
                "tokens": round(self._state.tokens, 2),
                "capacity": self.capacity,
                "refill_rate": self.refill_rate,
            }


class AgentRateLimiter:
    """Per-agent token bucket registry.

    One bucket per agent name. Buckets are created lazily on first request.
    """

    def __init__(
        self,
        *,
        capacity: float = 10.0,
        refill_rate: float = 1.0,
    ) -> None:
        self._capacity = capacity
        self._refill_rate = refill_rate
        self._buckets: dict[str, TokenBucket] = {}
        self._lock = threading.Lock()

    def _get_or_create(self, agent: str) -> TokenBucket:
        bucket = self._buckets.get(agent)
        if bucket is not None:
            return bucket
        with self._lock:
            bucket = self._buckets.get(agent)
            if bucket is not None:
                return bucket
            bucket = TokenBucket(capacity=self._capacity, refill_rate=self._refill_rate)
            self._buckets[agent] = bucket
            return bucket

    def check(self, agent: str) -> None:
        """Try to consume 1 token for the agent. Raises RateLimited on exhaustion."""
        bucket = self._get_or_create(agent)
        ok, retry_after = bucket.try_consume(1.0)
        if not ok:
            # Record globally for /api/v1/_stats
            try:
                from .metrics import get_metrics
                get_metrics().rate_limited_total.inc()
            except Exception:
                pass
            raise RateLimited(agent, retry_after)

    def stats(self) -> dict:
        """Per-agent remaining tokens."""
        return {
            agent: {
                "tokens": b.stats()["tokens"],
                "capacity": self._capacity,
                "refill_rate": self._refill_rate,
            }
            for agent, b in self._buckets.items()
        }

    def known_agents(self) -> list[str]:
        return list(self._buckets.keys())


def resolve_agent_name(headers: dict[str, str], peer_ip: str) -> str:
    """Extract agent identity from request. Falls back to peer IP."""
    name = headers.get("x-agent-name") or headers.get("X-Agent-Name")
    if name:
        return name.strip()[:128]  # clamp to a sane length
    return f"ip:{peer_ip}"