# Findings: `wh_dispatch exec` Failure Root Cause Analysis

This document records the conclusions from the investigation that began after the symptoms report (`symptoms.md`). It is **not** a parallel symptom list — only the conclusions and what was actually verified.

## 1. Verified root cause (high confidence)

The orchestrator's `wh_dispatch exec` path calls `_exec_ssh` (in `src/worker_harness/ssh.py`), which spawns the **`tailscale` CLI** as a subprocess — specifically `tailscale ssh engeld@worker` (see `src/worker_harness/ssh.py:126`):

```python
def _ssh_base_args(worker: Worker) -> list[str]:
    return ["tailscale", "ssh", _ssh_target(worker)]
```

The Go runtime inside `tailscale ssh` requires threads for I/O (magicsock, SSH subsystem, TLS handshakes). On 2026-08-17 ~13:01 UTC, the orchestrator's container hit its **`RLIMIT_NPROC` / cgroup `pids.max` ceiling**:

```
runtime: failed to create new OS thread (have 7 already; errno=11)
runtime: may need to increase max user processes (ulimit -u)
fatal error: newosproc
```

- `errno=11` is `EAGAIN`, returned by `clone(2)` when the per-user process/thread limit is hit.
- The "have 7 already" message indicates Go was attempting to spawn an 8th OS thread (M).
- The panic is fatal: the `tailscale ssh` subprocess dies, the orchestrator's job-manager records "Failed to start job", and the user-visible symptom is a job with `started_at == finished_at / exit_code: -1`.

The orchestrator Dockerfile sets **no `pids_limit` and no `ulimit -u`** — verified by reading `orchestrator_container/Dockerfile` and `orchestrator_container/entrypoint.sh`. The container inherits whatever limit the host runtime provides, which on many rootless Docker / Kubernetes / Podman configurations is as low as 64–4096.

## 2. Why only SSH-based exec broke (and not heartbeats, Pi relay, web UI)

Verified by reading `~/.pi/agent/extensions/pi-worker-harness/api.ts`:

```ts
let orchestratorUrl = process.env.WH_ORCHESTRATOR_URL?.trim()
  || "http://orchestrator.hs.d0me.xyz:12889";
```

The path of failure is:

- `wh_dispatch` → orchestrator HTTP (`http://orchestrator.hs.d0me.xyz:12889`) → orchestrator Python (`asyncio.create_subprocess_exec(["tailscale", "ssh", ...])`) → `tailscale ssh` Go process → tries to spawn threads → **`EAGAIN`** → panic → subprocess dies → job-manager records failure.

What kept working:

- HTTP heartbeats are worker → orchestrator one-way POSTs; they don't spawn subprocesses on the orchestrator.
- Pi interactive sessions use the Tailscale control-plane protocol directly (port 27888), not `tailscale ssh`.
- Web UI is in-process Python.
- Existing tunnel `8c1affa0-...` stayed up because it was already established and its `tailscale ssh -N -g -L` process was already allocated.

## 3. Why the cutover was synchronous across all 6 workers

Each `wh_dispatch exec` independently spawns a `tailscale ssh` subprocess that independently tries to spawn an OS thread. Once the container's process/thread ceiling is reached, **every** subsequent exec attempt is affected regardless of which worker it targets. The cutover timestamp 13:01:00 UTC is simply the moment the ceiling was crossed, not six independent worker-side events.

## 4. Why the orchestrator's `pids.max` was hit (verified analysis)

Each `tailscale ssh` invocation is process-heavy:
- Go runtime (itself multi-threaded)
- magicsock connection to the Tailscale control plane
- SSH subsystem threads (key exchange, channel management)
- Possibly per-peer goroutines

The orchestrator source uses `tailscale ssh` for **every** SSH operation, including high-frequency polling paths. Specific hot paths in the source:

- **`jobs_logs_stream`** (`heartbeat.py:1638`) — SSE stream that polls `ssh_read_log` every `poll_seconds` (default `1.0`) in an infinite `while True:` loop while the SSE client is connected.
- **Sync-mode `wh_dispatch exec`** (`heartbeat.py:1546`) — polls `refresh_job_status` every `0.5s` for up to `sync_timeout=120s` (~240 SSH calls per sync exec).
- **`GET /api/v1/jobs`** (`heartbeat.py:1610`) — refreshes every listed RUNNING/PENDING SSH job on every request.
- **`GET /api/v1/jobs/{id}/logs`** (`heartbeat.py:1604`) — calls `jm.get_logs` (an SSH call) per request.

Each call is `tailscale ssh engeld@worker` → forks a Go runtime → spins up magicsock + SSH-protocol threads. Over days of activity, with the user's high job throughput, the container's process/thread count accumulated toward whatever its `pids.max` is.

## 5. Verified code-level leak in `src/worker_harness/ssh.py`

I read the full 467 lines. There are three subprocess-cleanup helpers, all with the **same structural leak**: when `os.killpg` raises `ProcessLookupError` (the process group is already gone — which can happen on a race between the proc dying and our killpg call), the function returns without calling `proc.wait()`. A dead-but-unreaped subprocess is a **zombie**.

### 5.1 `_terminate_async_process` (ssh.py:67-92) — primary

```python
async def _terminate_async_process(proc, grace_seconds: float = 2.0) -> None:
    if proc.returncode is not None:
        return
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        return               # ← leak: returns without proc.wait()
    try:
        await asyncio.wait_for(proc.wait(), timeout=grace_seconds)
        return
    except asyncio.TimeoutError:
        pass
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass                 # ← same shape on SIGKILL branch
    try:
        await asyncio.wait_for(proc.wait(), timeout=1.0)
    except asyncio.TimeoutError:
        pass                 # ← gives up silently if proc is stuck in D
```

Three distinct leak paths:

1. **Early return on `ProcessLookupError`** (line 79): if the process group vanished between `create_subprocess_exec` and `killpg`, we exit without reaping. The proc may be a zombie (dead, awaiting `wait()`).
2. **Silent `pass` on SIGKILL `ProcessLookupError`** (line 87): same shape, second branch.
3. **Final silent `pass` on `wait_for(1.0)` timeout** (line 91): if the proc is stuck in `D` state (uninterruptible sleep on I/O), neither signal is delivered, `proc.wait()` never returns, and the helper returns. The proc occupies a PID and kernel thread entry **indefinitely** until container restart.

### 5.2 `_terminate_popen_process_group` (ssh.py:95-115)

Same pattern, sync version for `subprocess.Popen` (used by `ssh_port_forward`). Same `ProcessLookupError` early-return → same leak shape.

### 5.3 `TunnelRegistry.stop` (tunnel_registry.py:58-78)

Same pattern. Same `ProcessLookupError` skip-reap path.

### 5.4 How this connects to the observed outage

The "have 7 already" Go-runtime panic message refers to the number of M (OS-thread) records the Go runtime is currently tracking. In the standard Go runtime this is the number of M's that exist, not the number of threads in the process. In a healthy Go process each M holds an OS thread. So "have 7 already" is consistent with a process that already has 7 OS threads.

The reason `tailscale ssh` could not create an 8th OS thread is that the orchestrator container's `pids.max` (or `RLIMIT_NPROC`) had been reached. A new `clone(2)` would push it over.

This state can arise via two distinct paths:

**(a) Cumulative NPROC pressure**: the orchestrator was running for many days, spawning many `tailscale ssh` processes. Even if every one was correctly reaped, the *transient* peak process count during heavy concurrent activity could have been close to the limit, and one extra concurrent exec at ~13:01 UTC pushed it over.

**(b) The leak paths in §5.1–§5.3 actually leaking**: every time one of those three leak paths triggers, a `tailscale ssh` proc stays around as a zombie (or alive in D state) holding a PID and likely a kernel thread entry. Each such leak reduces the available headroom. Over days, accumulated leaks exhaust the limit.

Both paths are consistent with the user's observation that the issue "was not there an hour ago" — the limit was reached by a combination of gradual accumulation (path b) and a transient spike (path a) around 13:01 UTC.

## 6. My best guess at the trigger at 13:01 UTC

What I can verify with high confidence:
- The orchestrator's `pids.max` was hit (Go runtime panic, errno=11 EAGAIN on `clone(2)`).
- After orch restart, `wh_dispatch exec` works again immediately and `pgrep 'tailscale ssh'` returns 0.
- The orchestrator source has three subprocess-cleanup helpers with `ProcessLookupError` early-return paths that can leak.

What I cannot verify (no orchestrator host access from the Mac):
- The exact `pids.max` value the container had at the moment of failure.
- Whether the orchestrator container or its `tailscaled` daemon was restarted at ~13:00 UTC (the `magicsock: new contact` event at 14:16:49 is post-failure and shows ongoing peer-reconnect activity but not a daemon restart).
- The actual `ps -eLf` snapshot from the orchestrator before restart (which would show whether there were `tailscale ssh` zombie/defunct entries).
- Whether the leaked processes were zombies (defunct) or stuck-in-D (occupying a kernel task struct).

My best guess: the trigger at 13:01 UTC was the orchestrator's `pids.max` being crossed — by either a transient burst or accumulated leaks from the three cleanup helpers. **The leak in `_terminate_async_process` and its siblings is the most plausible accumulating cause**; without a fix to those helpers, the situation will recur after another multi-day run.

This matches the user's stated hypothesis exactly: "zombie processes stacked up since it was running for several days and many execs were performed, potentially building up to a limit."

## 7. Why the Mac's `~/.ssh/authorized_keys` is irrelevant (verified)

- `wh_dispatch` is purely HTTP from the Mac → orchestrator at `http://orchestrator.hs.d0me.xyz:12889`.
- The orchestrator uses **Tailscale SSH** (control-plane ACLs), not OpenSSH with `authorized_keys`. There is no `authorized_keys` file anywhere in the SSH pipeline.
- The Mac's `~/.ssh/` directory exists but is never used by the harness.
- Tailscale status on the Mac confirms `100.64.2.55 orchestrator linux active` is reachable on Tailscale. SSH from Mac to orchestrator:22 is refused because sshd is not exposed on the orchestrator (which is expected; not a problem).

## 8. Why the dotfiles symlinks are irrelevant (verified)

- `~/.pi -> dotfiles/pi/.pi` and `~/.omp -> dotfiles/oh-my-pi/.omp` are stow-style symlinks. They affect only local Pi/OMP agent state.
- No dotfiles commit since 2026-08-13 touches anything that would propagate to the orchestrator.
- The OMP rule files (`wh-over-ssh.md`, `wh-over-scp.md`) modified at 12:41 UTC are advisory text only ("use wh_dispatch, not ssh"). Cannot affect the orchestrator's SSH behavior.
- The `host-relay.ts` Bun process on the Mac has been running continuously since 2026-07-29 (19 days uptime at investigation time).

## 9. Recommended fixes (ordered by impact)

### 9.1 Immediate / unblocks recovery

If a leak recurs and the orchestrator hits the limit again before code can be fixed:

```bash
# from inside the orchestrator container (via Tailscale SSH from Mac, or host console):
pkill -9 -f 'tailscale ssh' || true
# (then resume dispatching)
```

### 9.2 Container-level guard (prevents the failure mode from recurring)

Edit `orchestrator_container/entrypoint.sh`, prepend near the top:

```bash
ulimit -u 32768    # raise per-user process ceiling so tailscale ssh
                   # never hits EAGAIN on clone(2). Default Linux is 64k+;
                   # Docker default is unlimited, but rootless / k8s /
                   # Podman often default to 4096 or 512.
```

And in `docker-compose.tailscale.example.yml`:

```yaml
services:
  orchestrator:
    pids_limit: 32768
```

### 9.3 Code-level fix (the leak)

Patch `_terminate_async_process` (`src/worker_harness/ssh.py:67`), `_terminate_popen_process_group` (`src/worker_harness/ssh.py:95`), and `TunnelRegistry.stop` (`src/worker_harness/tunnel_registry.py:58`) so they **always attempt to reap**, never early-return on `ProcessLookupError` without first trying `proc.wait()`.

Suggested patch shape for `_terminate_async_process`:

```python
async def _terminate_async_process(proc, grace_seconds: float = 2.0) -> None:
    def _kill(sig: int) -> None:
        try:
            os.killpg(proc.pid, sig)
        except (ProcessLookupError, PermissionError):
            pass

    if proc.returncode is None:
        _kill(signal.SIGTERM)
        try:
            await asyncio.wait_for(proc.wait(), timeout=grace_seconds)
        except asyncio.TimeoutError:
            _kill(signal.SIGKILL)
            try:
                await asyncio.wait_for(proc.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass  # genuinely stuck in D state; accept the leak
    # Final fallback reap: cover any race where the proc died between
    # our killpg and our wait(). Safe; returns immediately if already reaped.
    if proc.returncode is None:
        try:
            await asyncio.wait_for(proc.wait(), timeout=0.5)
        except asyncio.TimeoutError:
            pass
```

Apply the analogous pattern (always try to reap) to the other two helpers.

### 9.4 Defensive: reduce SSH-call volume

Raise the default polling cadence in the high-frequency paths so the orchestrator makes fewer `tailscale ssh` calls per minute:

- `jobs_logs_stream` (`heartbeat.py:1627`): raise default `poll_seconds` from `1.0` to `2.0` or `5.0`.
- Sync-mode exec poll loop (`heartbeat.py:1546`): raise the `0.5` sleep to `1.0` or `2.0`.

Each doubling of the interval halves `tailscale ssh` calls per unit time.

## 10. Confidence and unknowns

| Claim | Confidence | Why |
|---|---|---|
| Orchestrator container hit `RLIMIT_NPROC` / `pids.max` | **High** | Direct evidence: Go runtime panic with `errno=11 EAGAIN` on `clone(2)` |
| `wh_dispatch exec` path goes through `tailscale ssh` Go subprocess | **High** | Verified by reading `ssh.py:126` |
| `~/.ssh/authorized_keys` on the Mac is irrelevant | **High** | Verified: `wh_dispatch` is HTTP; orchestrator uses Tailscale SSH |
| The leak in `_terminate_async_process` exists | **High** | Verified by reading the source — the early-return on `ProcessLookupError` is unambiguous |
| The leak in `_terminate_async_process` is *the* cause | **Medium** | Plausible and consistent with user's observation; without access to the orchestrator host I cannot directly confirm leak accumulation. The cutover could also be path (a) alone (transient burst hitting a tight default limit) without the leak being involved. |
| Orchestrator restart drained the leak | **Medium-High** | `pgrep` returns 0 after restart; symptom immediately cleared. |