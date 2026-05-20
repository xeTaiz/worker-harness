# Worker-Only Userspace Tailscale Patch Plan

## Goal
Run **worker containers** in Tailscale userspace networking mode (no `/dev/net/tun`, no `NET_ADMIN`) while keeping the **orchestrator** in current kernel/TUN mode.

This is an incremental migration to reduce worker container privileges without changing orchestrator-side SSH/client architecture.

---

## Scope

### In scope
- Worker container networking mode switch to userspace.
- Preserve worker functionality:
  - inbound SSH from orchestrator (`:22` or configured `WORKER_SSH_PORT`)
  - outbound heartbeat/register to orchestrator (`/register` on `:12888`)
- Add explicit userspace forwarding/proxy wiring in worker entrypoint/runtime.
- Compose example updates for worker-only userspace mode.
- Validation checklist for job execution + tunnel workflows.

### Out of scope
- Orchestrator userspace migration.
- API authentication changes.
- Refactoring orchestrator SSH client (`src/worker_harness/ssh.py`) for proxy mode.

---

## Current state assumptions
- Orchestrator remains in kernel mode (keeps `NET_ADMIN` + `/dev/net/tun`).
- Orchestrator reaches workers by `worker_ip` + `ssh_port` using normal `ssh/scp`.
- Worker daemon uses `httpx` to POST to `http://ORCHESTRATOR_HOST:ORCHESTRATOR_PORT/register`.

Relevant files (current):
- `worker_container/entrypoint.sh`
- `worker_container/worker_daemon.py`
- `docker-compose.tailscale.example.yml`
- `README.md` (runtime guidance)

---

## Design approach (worker-only userspace)

### 1) Inbound SSH path (orchestrator -> worker)
In userspace mode, worker no longer has a kernel tailnet interface. We must explicitly expose SSH through tailscaled userspace forwarding.

Plan:
- Start `tailscaled` in userspace mode on worker.
- After `tailscale up`, configure tailnet TCP forwarding so tailnet `:<WORKER_SSH_PORT>` maps to local `127.0.0.1:<WORKER_SSH_PORT>` (or local sshd port).
- Keep sshd running locally as today.

Result: orchestrator can continue dialing worker tailnet IP/port with no orchestrator code changes.

### 2) Outbound heartbeat path (worker -> orchestrator)
In userspace mode, direct `http://orchestrator.tailnet:12888` may not route transparently from normal processes.

Plan:
- Enable userspace outbound proxy endpoint from tailscaled (SOCKS5).
- Route worker daemon HTTP calls through that proxy.
- Keep current heartbeat endpoint URL format unchanged.

Implementation choice:
- Use a single app-specific env var: `WH_PROXY` (e.g. `socks5://127.0.0.1:1055`).
- Only `worker_daemon.py` reads this var and configures `httpx` explicitly.

---

## Planned file changes

## 1) `worker_container/entrypoint.sh`

### Add userspace mode controls
New env vars (worker):
- `TS_USERSPACE` (default `true` for this mode; allows rollback)
- `TS_SOCKS5_ADDR` (default `127.0.0.1:1055`)
- `TS_LOCAL_ADDR_PORT` (default `127.0.0.1:0` or fixed if needed)
- `TS_SERVE_SSH_PORT` (defaults to `${WORKER_SSH_PORT:-22}`)

### Tailscaled startup branch
- If `TS_USERSPACE=true`:
  - start `tailscaled` with userspace flags (`--tun=userspace-networking`, socks/local endpoints)
- Else:
  - keep existing kernel/TUN startup path (backward compatibility)

### Configure tailnet inbound forwarding for SSH
After successful `tailscale up`:
- configure userspace tailnet TCP forwarding from tailnet SSH port to local sshd port.
- verify forwarding command success; fail startup if forwarding cannot be established.

### Pass daemon-only proxy config
- Set `WH_PROXY` for the daemon process only (example: `socks5://127.0.0.1:1055`).
- Do **not** rely on global `HTTP_PROXY`/`ALL_PROXY` env vars.
- This keeps proxy behavior scoped to worker heartbeat traffic in `worker_daemon.py`.

---

## 2) `worker_container/worker_daemon.py`

### Make outbound HTTP robust with userspace proxy
Current `httpx.AsyncClient()` does not explicitly set proxy behavior.

Patch plan:
- Read `WH_PROXY` in `worker_daemon.py` (empty/unset = direct mode).
- Construct `httpx.AsyncClient(...)` with explicit proxy settings from `WH_PROXY`.
- Keep timeouts/retry behavior unchanged.
- Add startup log line indicating proxy mode (`WH_PROXY` set/unset) and target orchestrator URL.

### Keep registration schema unchanged
No changes to payload fields (`worker_ip`, etc.) or endpoint path.

---

## 3) `docker-compose.tailscale.example.yml`

For `worker` service:
- Remove privileged networking bits:
  - `cap_add: [NET_ADMIN]`
  - `devices: [/dev/net/tun:/dev/net/tun]`
- Add userspace env vars:
  - `TS_USERSPACE: "true"`
  - `TS_SOCKS5_ADDR: "127.0.0.1:1055"`
  - `WH_PROXY: "socks5://127.0.0.1:1055"`
  - (optional explicit serve mapping var)

For `orchestrator` service:
- No changes in this phase (still kernel mode).

---

## 4) `README.md`

Update worker runtime requirements:
- Kernel/TUN no longer mandatory when `TS_USERSPACE=true`.
- Add userspace notes:
  - inbound SSH is provided by tailscale userspace forwarding
  - outbound daemon HTTP uses daemon-scoped `WH_PROXY`
- Add rollback note (`TS_USERSPACE=false` restores prior behavior).

---

## Compatibility and risk notes

### Risk 1: `WH_PROXY` misconfiguration
Mitigation:
- Validate `WH_PROXY` format at daemon startup (or fail fast on first connect).
- Set proxy explicitly in daemon client construction.
- Add startup diagnostics and heartbeat failure detail.

### Risk 2: SSH forwarding command semantics vary by tailscale version
Mitigation:
- Pin/test on container-installed tailscale version.
- Validate forwarding setup command during startup; fail fast with actionable logs.

### Risk 3: ACL expectations around worker SSH port
Mitigation:
- Keep ACL unchanged (`tag:wh-orchestrator -> tag:wh-worker:22` or configured port).
- Confirm tailnet-reachable worker port after migration.

### Risk 4: Operational confusion mixed modes
Mitigation:
- Explicit env switch names and logs showing mode per container.

---

## Validation plan

## Pre-flight
- `docker compose config` renders without worker NET_ADMIN/TUN.
- Worker logs show userspace tailscaled start and successful tailnet join.
- Worker logs confirm inbound SSH forwarding setup success.

## Functional
1. Worker registers successfully (`/register` heartbeat OK).
2. Orchestrator lists worker as online (`/api/v1/workers`).
3. Start job from orchestrator; verify remote command runs on worker.
4. Fetch logs (`/api/v1/jobs/{id}/logs`).
5. Stop job (`DELETE /api/v1/jobs/{id}`).
6. Tunnel create/list/delete still works end-to-end.

## Negative checks
- Set invalid `WH_PROXY`; verify heartbeat fails with clear error.
- Break forwarding setup intentionally; verify orchestrator SSH fails and worker startup reports forwarding issue.

---

## Rollout strategy

1. Implement behind env flags (default off in code if desired, on in example compose).
2. Test single worker in userspace mode against existing orchestrator.
3. Soak test job lifecycle + tunnel workflow.
4. If stable, promote userspace worker mode as recommended default.

---

## Rollback plan
- Set `TS_USERSPACE=false` on worker.
- Re-enable `NET_ADMIN` + `/dev/net/tun` for worker in compose.
- Restart worker container.

No data migration required.

---

## Estimated patch surface
- `worker_container/entrypoint.sh` (medium)
- `worker_container/worker_daemon.py` (small/medium)
- `docker-compose.tailscale.example.yml` (small)
- `README.md` (small)

Total complexity: **moderate** (mostly runtime networking behavior + version-specific tailscale command correctness).