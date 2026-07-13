"""In-process metrics: counters, gauges, histograms.

Exposed at GET /api/v1/_stats. Single shared instance per server process.
No external deps; uses plain dicts + threading.Lock for thread safety.
For multi-process deployments, swap in a Prometheus client.

Designed to fail safe: any metric error must not break the request path.
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Deque


class Counter:
    """Monotonically increasing counter."""

    __slots__ = ("_value", "_lock")

    def __init__(self) -> None:
        self._value = 0
        self._lock = threading.Lock()

    def inc(self, n: int = 1) -> None:
        with self._lock:
            self._value += n

    @property
    def value(self) -> int:
        with self._lock:
            return self._value


class Gauge:
    """Point-in-time value that can go up or down."""

    __slots__ = ("_value", "_lock")

    def __init__(self) -> None:
        self._value = 0
        self._lock = threading.Lock()

    def set(self, v: int | float) -> None:
        with self._lock:
            self._value = v

    def inc(self, n: int | float = 1) -> None:
        with self._lock:
            self._value += n

    def dec(self, n: int | float = 1) -> None:
        with self._lock:
            self._value -= n

    @property
    def value(self) -> int | float:
        with self._lock:
            return self._value


class Histogram:
    """Sliding-window histogram. Records last `max_samples` observations,
    exposes p50/p95/p99 + count.

    Bounded memory: 1000 samples * ~24B ≈ 24KB. Fits comfortably.
    """

    __slots__ = ("_samples", "_lock", "max_samples")

    def __init__(self, max_samples: int = 1000) -> None:
        self._samples: Deque[float] = deque(maxlen=max_samples)
        self._lock = threading.Lock()

    def observe(self, value_ms: float) -> None:
        with self._lock:
            self._samples.append(value_ms)

    def percentile(self, p: float) -> float:
        with self._lock:
            if not self._samples:
                return 0.0
            sorted_samples = sorted(self._samples)
            idx = max(0, min(len(sorted_samples) - 1, int(p * len(sorted_samples))))
            return sorted_samples[idx]

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._samples)


@dataclass
class Metrics:
    """Container for all metrics. Attached to app.state.metrics."""

    # Cache
    cache_hits: Counter = field(default_factory=Counter)
    cache_misses: Counter = field(default_factory=Counter)
    cache_evictions: Counter = field(default_factory=Counter)
    cache_size: Gauge = field(default_factory=Gauge)

    # Lanes (per-worker)
    lane_acquires_total: Counter = field(default_factory=Counter)
    lane_timeouts_total: Counter = field(default_factory=Counter)
    lane_wait_ms: Histogram = field(default_factory=Histogram)

    # SSH (per operation type)
    ssh_call_ms: dict[str, Histogram] = field(default_factory=lambda: defaultdict(Histogram))

    # Rate limiting
    rate_limited_total: Counter = field(default_factory=Counter)

    # HTTP
    in_flight_requests: Gauge = field(default_factory=Gauge)
    requests_total: Counter = field(default_factory=Counter)

    # Reaper
    reaped_tunnels_total: Counter = field(default_factory=Counter)
    reaped_ssh_total: Counter = field(default_factory=Counter)
    reaper_last_run_ts: Gauge = field(default_factory=Gauge)

    # Uptime
    started_at: float = field(default_factory=time.time)

    def observe_ssh(self, op: str, duration_ms: float) -> None:
        """Record an SSH call duration, labeled by operation."""
        if op not in self.ssh_call_ms:
            self.ssh_call_ms[op] = Histogram()
        self.ssh_call_ms[op].observe(duration_ms)

    def snapshot(self) -> dict:
        """Return a JSON-serializable snapshot for /api/v1/_stats."""
        return {
            "cache": {
                "hits": self.cache_hits.value,
                "misses": self.cache_misses.value,
                "evictions": self.cache_evictions.value,
                "size": int(self.cache_size.value),
            },
            "lanes": {
                "_totals": {
                    "acquires": self.lane_acquires_total.value,
                    "timeouts": self.lane_timeouts_total.value,
                    "wait_p50_ms": round(self.lane_wait_ms.percentile(0.50), 1),
                    "wait_p95_ms": round(self.lane_wait_ms.percentile(0.95), 1),
                },
            },
            "rate_limit": {
                "rate_limited_total": self.rate_limited_total.value,
            },
            "ssh": {
                op: {
                    "calls": h.count,
                    "p50_ms": round(h.percentile(0.50), 1),
                    "p95_ms": round(h.percentile(0.95), 1),
                    "p99_ms": round(h.percentile(0.99), 1),
                }
                for op, h in self.ssh_call_ms.items()
            },
            "http": {
                "in_flight": int(self.in_flight_requests.value),
                "requests_total": self.requests_total.value,
            },
            "reaper": {
                "killed_tunnels_total": self.reaped_tunnels_total.value,
                "killed_ssh_total": self.reaped_ssh_total.value,
                "last_run_ts": int(self.reaper_last_run_ts.value),
            },
            "uptime_seconds": int(time.time() - self.started_at),
        }


# Convenience: singleton instance the reaper/ssh/cache can record into
# without an explicit dependency. Wired in heartbeat.py lifespan.
_global_metrics: Metrics | None = None


def set_global_metrics(m: Metrics) -> None:
    global _global_metrics
    _global_metrics = m


def get_metrics() -> Metrics:
    global _global_metrics
    if _global_metrics is None:
        _global_metrics = Metrics()
    return _global_metrics