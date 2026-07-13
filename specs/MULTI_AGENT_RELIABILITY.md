# Multi-Agent Reliability

## Goal

Make `worker-harness serve` safe and predictable when **multiple research agents**
hit it concurrently. Today, a single slow operation (e.g. compute-manager's
`worker_harness_cli.py agent workers` racing the heartbeat server's `_init_schema`
ALTER TABLE writes) can wedge the entire SQLite database on a futex, blocking
every other agent's HTTP requests indefinitely.

After this spec lands:

- 9+ agents can hammer the server concurrently without deadlocks.
- One slow worker's SSH never blocks another worker's reads.
- A cancelled or timed-out HTTP request can never leave an orphaned SSH subprocess.
- One rogue agent cannot starve the others (rate-limited per-agent identity).
- An operator can answer "why was agent X slow?" from `/api/v1/_stats` in seconds.

## Non-Goals

- Migrating SQLite → Postgres. SQLite + WAL is sufficient at the current scale.
- Multi-worker uvicorn. SQLite reads finish in microseconds; the bottleneck is
  long-running SSH, not throughput.
- SSH ControlMaster / connection multiplexing for tunnels. Per-tunnel `Popen`
  is fine until we observe >20 simultaneous tunnels to one worker.
- HTTP → CLI migration for humans. The CLI stays as-is; we only strip `bash`
  from one agent.

## Root Cause Recap

`sqlite3 PRAGMA journal_mode; PRAGMA busy_timeout;` on
`~/.config/worker-harness/db.sqlite` returns `delete` / `0`. With `delete`
journal mode and zero busy timeout, two writers racing on the same DB file
deadlock on the futex (both threads park in `futex_do_wait` indefinitely).
I reproduced this with two concurrent `worker_harness_cli.py agent workers`
invocations: one completed, one hung forever.

Layered on top:
1. The CLI is a side door that bypasses the HTTP API. Compute-manager hits
   the DB directly via bash, racing the server's writes.
2. The HTTP server has no per-worker concurrency control, so one slow SSH
   call to a worker holds an aiosqlite Connection slot while it waits.
3. Cancelled / timed-out HTTP requests can leak SSH subprocesses.
4. No cache, no rate limit, no metrics — every problem requires `ps`,
   `/proc/<pid>/wchan`, and archaeology.

## Architecture

Four new modules + one edit set, integrated into the existing `serve` /
`heartbeat.py` FastAPI app.

```
                ┌──────────────────────────────┐
   Agent ──HTTP─►  FastAPI (heartbeat.py)      │
                │                              │
                │  ┌─ RateLimit (per agent)    │  ← 60 req/min, burst 10
                │  │                           │
                │  ├─ Cache (TTL, per route)   │  ← 5s for workers_list
                │  │                           │
                │  ├─ WorkerLanes (per worker) │  ← 4 concurrent SSH ops
                │  │   semaphore + bounded     │     queue depth 32
                │  │   queue + acquire timeout │
                │  │                           │
                │  └─ ssh.py  (kill-on-exit)   │  ← kill process group in finally
                │                              │
                │  Reaper (background task)    │  ← every 60s, reap dead tunnels
                └──────────────────────────────┘
                            │
                            ▼
                       SQLite (WAL, busy_timeout=5000)
```

### 1. Cache (`worker_harness/cache.py`, ~80 LOC)

Per-process in-memory TTL cache. Single shared instance, attached to the
FastAPI app via `app.state.cache`.

```python
class TTLCache:
    async def get(key) -> Any | None        # None on miss or expired
    async def set(key, value, ttl) -> None
    async def invalidate(key) -> None
    async def invalidate_prefix(prefix) -> None
    def stats() -> dict                      # hits, misses, size, evictions
```

Decorators route handlers through the cache:

| Endpoint | TTL | Invalidation trigger |
|---|---|---|
| `GET /api/v1/workers` | 5s | `POST /register`, `DELETE /prune`, `set_worker_status` |
| `GET /api/v1/workers/summary` | 2s | same |
| `GET /api/v1/tunnels` | 10s | `POST /api/v1/tunnels`, `DELETE /api/v1/tunnels/{id}` |

Cache misses fall through to the DB. Cache writes happen on the response path,
not pre-emptively. Cache is best-effort: any cache error → log + serve fresh.

### 2. WorkerLanes (`worker_harness/lanes.py`, ~120 LOC)

Per-worker SSH concurrency control. One shared instance, attached to app state.

```python
class WorkerLanes:
    async def acquire(worker_id, *, timeout=30.0) -> Lane  # context manager
    def stats() -> dict                                     # depth, in_use per worker
    async def shutdown() -> None                            # cancel waiters on app stop

class Lane:
    async def __aenter__(self): ...   # blocks until lane or timeout
    async def __aexit__(self, *exc): ...
```

Per-worker:
- Four immediately available SSH slots — at most 4 concurrent round-trips.
- A bounded FIFO deque (max 32 waiters) for requests beyond those slots.
- `acquire(timeout=30)` returns a `Lane` context manager.
- If timeout elapses (or the queue is full), `LaneTimeout` → HTTP 503 + `Retry-After: 2`.

Tunnels: creation briefly acquires the lane (to run `ssh -N -L`); the persistent
`Popen` is **not** held in the lane — it's tracked in `app.state.tunnels`.

Lanes are created lazily per `worker_id`. A worker never seen has no overhead.

### 3. ssh.py kill-on-exit (edit, ~30 LOC change)

Every SSH function in `ssh.py` is wrapped:

```python
async def async_ssh_run(worker, cmd, *, timeout=30):
    async with lanes.acquire(worker.id, timeout=10) as lane:
        proc = await asyncio.create_subprocess_exec("ssh", ...)
        try:
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=timeout,
            )
            return SshResult(returncode=proc.returncode, stdout=stdout, stderr=stderr)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            await terminate_process_group(proc)  # SIGTERM, then SIGKILL
            raise
        finally:
            if proc.returncode is None:
                await terminate_process_group(proc)
```

The `finally` block guarantees no orphaned subprocess, even if the FastAPI
request is cancelled mid-flight. Same pattern for `ssh_upload_bytes`,
`ssh_download_bytes`, `ssh_port_forward` (creation only — the persistent
tunnel goes into `TunnelRegistry`).

### 4. Tunnels registry (edit heartbeat.py + tunnels.py CLI, ~50 LOC)

New in-memory registry on `app.state`:

```python
@dataclass
class TunnelProc:
    id: str
    worker_id: str
    local_port: int
    remote_port: int
    proc: Popen
    created_at: int

class TunnelRegistry:
    def add(tp: TunnelProc) -> None
    def remove(tunnel_id: str) -> Popen | None    # returns the proc for cleanup
    def for_worker(worker_id: str) -> list[TunnelProc]
    def reap_dead() -> int                        # polls each proc, returns count reaped
```

`/api/v1/tunnels` (POST) → acquire lane → `ssh -N -L ...` → register → release lane.
`/api/v1/tunnels/{id}` (DELETE) → unregister → terminate its complete process group →
SIGKILL after the grace period.

### 5. Reaper (`worker_harness/reaper.py`, ~70 LOC)

Background task started in FastAPI `lifespan`. Runs every 60s.

```python
async def reap_loop(app):
    while True:
        await asyncio.sleep(60)
        try:
            reaped_tunnels = app.state.tunnels.reap_dead()
            metrics.reaper_last_run_ts.set(time.time())
        except asyncio.CancelledError:
            break
        except Exception as e:
            log.exception(f"reaper: {e}")
```

Normal SSH calls are deliberately not discovered with a broad `ps | kill ssh`
scan: that could kill a legitimate in-flight request. Each ssh.py call owns and
kills its process group in a request-local `finally`; systemd
`KillMode=control-group` cleans the complete tree on service restart/crash.

### 6. RateLimit (`worker_harness/ratelimit.py`, ~80 LOC)

Per-agent token bucket, keyed by `X-Agent-Name` header (or fall back to peer IP).

```python
class TokenBucket:
    capacity: int = 10
    refill_rate: float = 1.0     # tokens per second
    def try_consume(self, n: int = 1) -> bool

class AgentRateLimiter:
    def __init__(self, *, capacity=10, refill_rate=1.0): ...   # 60 req/min, burst 10
    def check(self, agent_name: str) -> None                  # raises RateLimited
    def stats() -> dict                                        # per-agent remaining + last-refill
```

FastAPI middleware:

```python
@app.middleware("http")
async def rate_limit_middleware(request, call_next):
    agent = request.headers.get("X-Agent-Name", request.client.host)
    try:
        limiter.check(agent)
    except RateLimited as e:
        return JSONResponse(
            {"error": {"code": "RATE_LIMITED", "message": str(e)}},
            status_code=429,
            headers={"Retry-After": "2"},
        )
    return await call_next(request)
```

### 7. Metrics + `/api/v1/_stats` (`worker_harness/metrics.py`, ~90 LOC)

Counters + histograms attached to `app.state.metrics`.

```python
class Metrics:
    cache_hits: Counter
    cache_misses: Counter
    lane_acquires_total: Counter
    lane_timeouts_total: Counter
    lane_wait_seconds: Histogram           # time spent waiting for a lane
    ssh_call_seconds: Histogram            # per-call ssh duration, labeled by op
    rate_limited_total: Counter
    in_flight_requests: Gauge
```

`GET /api/v1/_stats`:

```json
{
  "cache": {"hits": 421, "misses": 53, "size": 4, "evictions": 0},
  "lanes": {
    "kw996":   {"in_use": 1, "queue_depth": 0, "timeouts_total": 0},
    "gpu-rig": {"in_use": 4, "queue_depth": 8, "timeouts_total": 2}
  },
  "rate_limit": {
    "compute-manager":   {"remaining": 9, "refill_at_ms": 412},
    "data-expert":       {"remaining": 7, "refill_at_ms": 813}
  },
  "ssh": {
    "async_ssh_run_p50_ms": 38, "p95_ms": 612, "p99_ms": 2104,
    "ssh_port_forward_p50_ms": 41, "p95_ms": 89,
    "calls_total": 1422
  },
  "reaper": {"last_run_ts": 1783948400, "killed_tunnels_total": 0, "killed_ssh_total": 1},
  "uptime_seconds": 8421
}
```

### 8. Agent config (edit `~/.pi/agent/agents/research-compute-manager.md`)

Drop `bash,write,edit` from the `tools:` list. Keep `wh_dispatch,wh_read,read,grep,find,ls`.
Compute-manager can still dispatch to workers via `wh_dispatch`; it just can't
run local bash commands on the orchestrator host.

```yaml
tools: read,grep,find,ls,wh_read,wh_dispatch
```

## File-Level Changes

### New files

| Path | LOC | Purpose |
|---|---|---|
| `src/worker_harness/cache.py` | ~80 | TTL cache + decorators |
| `src/worker_harness/lanes.py` | ~120 | Per-worker SSH concurrency |
| `src/worker_harness/ratelimit.py` | ~80 | Per-agent token bucket |
| `src/worker_harness/metrics.py` | ~90 | Counters + histograms + `/api/v1/_stats` |
| `src/worker_harness/reaper.py` | ~70 | Background zombie reaper |
| `specs/MULTI_AGENT_RELIABILITY.md` | (this) | Spec |
| `tests/test_cache.py` | ~60 | Unit tests for TTLCache |
| `tests/test_lanes.py` | ~80 | Unit tests for WorkerLanes |
| `tests/test_ratelimit.py` | ~40 | Unit tests for TokenBucket |
| `tests/test_concurrent_safety.py` | ~120 | Integration: 10 concurrent agents |

### Edits

| Path | Change |
|---|---|
| `src/worker_harness/db.py` | `_init_schema`: add `PRAGMA journal_mode=WAL`, `PRAGMA busy_timeout=5000`, `PRAGMA synchronous=NORMAL`. Remove the silent `try/except: pass` around `gpu_used_vram_gb` migration. |
| `src/worker_harness/ssh.py` | Wrap every SSH call in `lanes.acquire(...)` + process-group cleanup in `finally`. Add `asyncio.wait_for` to `communicate()`. |
| `src/worker_harness/heartbeat.py` | Wire cache + lanes + ratelimit + metrics into `create_app`. Add `GET /api/v1/_stats`. Start reaper in `lifespan`. |
| `~/.pi/agent/agents/research-compute-manager.md` | Drop `bash,write,edit` from tools. |

Total: **~440 LOC across 6 new files + 4 edits + 300 LOC of tests.**

## Test Plan

### Unit

- `test_cache.py`: TTL expiry, invalidation, stats counters, error handling.
- `test_lanes.py`: 5 acquires with N=4 + queue=2 → first 4 immediate, 5th waits, 6th+7th timeout. Timeout raises. `__aexit__` releases exactly once even on exception. Stats accurate.
- `test_ratelimit.py`: Token bucket math (burst, refill, exhausted). Per-agent isolation.

### Integration

- `test_concurrent_safety.py`: spin up the FastAPI app in-process via `httpx.ASGITransport`; simulate 10 agents each issuing 20 `GET /api/v1/workers` + 5 `POST /api/v1/tunnels` concurrently; assert:
  - All requests eventually complete (within `deadline_seconds`)
  - No SSH subprocess leaks (`pgrep -P <pid>` count ≤ N + small constant)
  - Cache hit rate > 50%
  - Rate limiter triggers at least once for one agent if rate is set artificially low
  - `/api/v1/_stats` returns a sane payload

### Manual smoke

1. Restart the heartbeat server (`worker-harness serve`).
2. Hit `/api/v1/_stats` — all counters present, lanes empty.
3. From a separate shell, dispatch 5 concurrent `agent workers` CLI calls — all complete; verify with `ps` that no SSH subprocess leaks.
4. Have compute-manager (after reload) try `bash: echo hi` — confirm the agent refuses (no `bash` tool available).

### Failure injection

- Pause SSH to one worker (`tc qdisc add dev eth0 root netem delay 30s`). Other workers' HTTP calls must still complete promptly. Verify lanes[that_worker].queue_depth > 0 and lanes[other_worker].queue_depth == 0.
- Kill -9 the heartbeat server during a long SSH call. Confirm SSH subprocess reaped within 5s on next server start (reaper walks the registry).

## Rollout

1. Land spec + new modules + tests behind the existing API surface. Server still works.
2. Land `db.py` PRAGMA change. **One-time** DB file migration: when the server starts, it detects journal_mode=delete, runs `PRAGMA journal_mode=WAL`, leaves the file as `<db>-wal` + `<db>-shm` siblings. This is a permanent switch — there's no path back without explicit operator action.
3. Land `ssh.py` kill-on-exit. Server still works, but no more zombie processes.
4. Land `heartbeat.py` cache + lanes + rate-limit + metrics wiring. `/api/v1/_stats` is the operator-visible new endpoint.
5. Land agent config change for compute-manager. The agent loses `bash` on next extension reload.
6. Validation: see test plan.

## Validation Checklist

- [ ] `python -m compileall src` — no syntax errors
- [ ] `pytest tests/test_cache.py tests/test_lanes.py tests/test_ratelimit.py tests/test_concurrent_safety.py` — all pass
- [ ] `sqlite3 ~/.config/worker-harness/db.sqlite 'PRAGMA journal_mode;'` — returns `wal`
- [ ] `sqlite3 ~/.config/worker-harness/db.sqlite 'PRAGMA busy_timeout;'` — returns `5000`
- [ ] Manual: two concurrent `worker_harness_cli.py agent workers` complete (no hang)
- [ ] Manual: `bash` tool absent from compute-manager after reload (run `/agents-list` to confirm tools, or send a test prompt)
- [ ] Manual: hit `/api/v1/_stats` — non-empty counters
- [ ] Manual: pause one worker's SSH via `tc`, observe lanes[<worker>].queue_depth grows, lanes[other].queue_depth stays 0
- [ ] Timeout/cancellation kills the complete SSH process group; systemd `KillMode=control-group` handles service restart/crash cleanup

## Risk

- **WAL mode requires shared memory files.** Filesystems that don't support
  `mmap` (some FUSE mounts, some NFS) break WAL. The default `~/.config/`
  filesystem on this machine is ext4 — fine. If anyone moves the DB to NFS,
  they need to revert to `journal_mode=delete` explicitly.
- **Cache invalidation is best-effort.** If a `POST /register` is in flight
  and a `GET /api/v1/workers` reads the cache at the same time, the cache
  might serve stale data for up to 5s. This matches today's eventual
  consistency between heartbeat-server and CLI readers.
- **Per-agent rate limiting uses the `X-Agent-Name` header.** If a malicious
  agent sends random values, they get separate buckets. The fallback to peer
  IP is a soft mitigation. If abuse becomes real, switch to a signed token.