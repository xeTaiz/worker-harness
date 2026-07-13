"""In-memory TTL cache with per-key expiry.

Single shared instance per server process (attached to app.state.cache).
No external deps. Thread-safe + asyncio-safe.

Best-effort: any cache error logs and serves fresh from the source.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

log = logging.getLogger("cache")


@dataclass
class _Entry:
    value: Any
    expires_at: float


class TTLCache:
    """Process-local TTL cache.

    - get returns None on miss or expired.
    - set overwrites + extends TTL.
    - invalidate / invalidate_prefix are O(N) (small N, fine for our scale).
    - stats() is O(1).
    """

    def __init__(self) -> None:
        self._store: dict[str, _Entry] = {}
        self._lock = asyncio.Lock()
        # Stats are recorded from the same instance, but read by metrics.py
        # which uses threading.Lock — keep a parallel mirror for atomicity.
        from .metrics import Counter, Gauge
        self._hits = Counter()
        self._misses = Counter()
        self._evictions = Counter()
        self._size_gauge = Gauge()

    async def get(self, key: str) -> Any | None:
        async with self._lock:
            entry = self._store.get(key)
            if entry is None:
                self._misses.inc()
                return None
            if entry.expires_at < time.monotonic():
                # Expired — drop lazily on read.
                del self._store[key]
                self._size_gauge.set(len(self._store))
                self._evictions.inc()
                self._misses.inc()
                return None
            self._hits.inc()
            return entry.value

    async def set(self, key: str, value: Any, ttl_seconds: float) -> None:
        async with self._lock:
            self._store[key] = _Entry(value=value, expires_at=time.monotonic() + ttl_seconds)
            self._size_gauge.set(len(self._store))

    async def invalidate(self, key: str) -> bool:
        async with self._lock:
            if key in self._store:
                del self._store[key]
                self._size_gauge.set(len(self._store))
                self._evictions.inc()
                return True
            return False

    async def invalidate_prefix(self, prefix: str) -> int:
        async with self._lock:
            keys = [k for k in self._store if k.startswith(prefix)]
            for k in keys:
                del self._store[k]
                self._evictions.inc()
            self._size_gauge.set(len(self._store))
            return len(keys)

    async def clear(self) -> None:
        async with self._lock:
            self._store.clear()
            self._size_gauge.set(0)
            self._evictions.inc()

    def stats(self) -> dict:
        return {
            "hits": self._hits.value,
            "misses": self._misses.value,
            "size": len(self._store),
            "evictions": self._evictions.value,
        }


# ── Decorator / route helper ───────────────────────────────────────────

def cache_key_for_request(request_path: str, query: dict) -> str:
    """Build a deterministic cache key from request path + query."""
    if not query:
        return request_path
    items = sorted((k, str(v)) for k, v in query.items())
    return f"{request_path}?" + "&".join(f"{k}={v}" for k, v in items)


def cached_response(
    cache: TTLCache,
    key_fn: Callable[..., str],
    ttl_seconds: float,
):
    """Decorator for FastAPI handlers.

    On call: build key → try cache → on miss, run handler → store result → return.
    If the handler raises, the cache is not populated.

    Usage:
        @app.get("/api/v1/workers")
        @cached_response(cache, lambda: "workers_list", ttl_seconds=5)
        async def workers_list():
            ...
    """
    def decorator(handler):
        async def wrapper(*args, **kwargs):
            key = key_fn(*args, **kwargs)
            try:
                cached = await cache.get(key)
            except Exception as e:
                log.warning(f"cache get failed for {key}: {e}")
                cached = None
            if cached is not None:
                return cached
            result = await handler(*args, **kwargs)
            try:
                await cache.set(key, result, ttl_seconds=ttl_seconds)
            except Exception as e:
                log.warning(f"cache set failed for {key}: {e}")
            return result
        # Preserve the wrapped function's name for FastAPI introspection
        wrapper.__name__ = handler.__name__
        wrapper.__doc__ = handler.__doc__
        return wrapper
    return decorator