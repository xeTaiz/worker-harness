---
title: Distributed Pi Session Fabric
status: draft
scope: single-operator Tailnet
created: 2026-07-24
---

# Distributed Pi Session Fabric

## 1. Goal

Extend Worker Harness from worker/job orchestration into a Pi session fabric:

- discover every opted-in Pi session on the Tailnet;
- attach to interactive and delegated sessions from any trusted device;
- route a prompt from a global Pi router into an existing interactive session;
- let an interactive agent use `wh_delegate` to start a constrained Pi child on a selected worker;
- expose every delegated shell command as an existing-style Worker Harness job with logs.

The system is for one operator. Tailnet enrollment is the trust boundary; all non-worker Tailnet members are trusted operators. This draft deliberately does not add application credentials, multi-user RBAC, public exposure, or tenant isolation.

## 2. Scope and terminology

| Term | Meaning |
|---|---|
| **Interactive session** | Normal `pi` started by the operator on a non-worker machine, normally from tmux or Zellij. It may use ordinary `wh_*` tools. |
| **Delegated session** | Pi child started inside a Worker Harness worker by `wh_delegate`. It never receives `wh_*`, subagent, planning, vault, or file-permission tools. |
| **Job** | A tmux-backed shell command with a log and exit result. Jobs are not Pi sessions, but delegated jobs link to their creating session. |
| **Bridge** | `pi-session-bridge`, an extension installed with Pi configuration. It reports lifecycle/state, accepts semantic prompts, and captures terminal-multiplexer metadata. |
| **Relay** | A host-local helper that attaches a tmux/Zellij client in a PTY and relays terminal bytes. |

Out of scope: sandboxing interactive sessions, LiteLLM, native mobile apps, public/Funnel access, multi-user authorization, worker-to-worker delegation, child subdelegation, and vault access in delegated children.

## 3. Trust and network topology

### 3.1 Tailnet policy

- `tag:wh-worker` may reach only the worker registration/event-ingest service on orchestrator port `12888`.
- The orchestrator may reach worker services and Tailscale SSH.
- Normal Tailnet members intentionally may reach worker services and control port `12889`.
- Worker relay services are therefore directly reachable by trusted non-worker members but not by other worker-tagged nodes.

This is intentional. A compromised normal Tailnet member is accepted as full-fleet compromise risk.

### 3.2 Worker relay service

Each worker daemon publishes a Pi relay service on loopback, for example:

```text
127.0.0.1:${WH_PI_RELAY_PORT:-27888}
```

Because workers use userspace Tailscale, it is published to trusted Tailnet members with the already-validated pattern:

```bash
tailscale --socket "$TS_SOCKET" serve --bg --yes \
  --tcp "$WH_PI_RELAY_PORT" "tcp://127.0.0.1:$WH_PI_RELAY_PORT"
```

The relay service provides a multiplexed WebSocket/HTTP protocol for:

- worker session command delivery (`start_delegate`, `cancel`, `prompt`);
- terminal attach streams for delegated sessions;
- session/job state lookup by the orchestrator or trusted terminal client.

It is not a general remote shell and is not reachable by worker-tagged peers under the ACL.

The current user-facing `wh_add_tunnel` feature is not required for normal worker session attach. It remains available as a fallback/debug primitive. The existing tunnel is an orchestrator-owned `ssh -L` local forward, not a worker control protocol.

### 3.3 Attachment paths

```text
Interactive Pi, non-worker host:
  terminal client -- direct Tailnet WebSocket --> host relay --> tmux/Zellij --> Pi
  PWA            -- orchestrator gateway fallback ------------------------^ 

Delegated Pi, worker:
  terminal client -- direct Tailnet WebSocket --> worker relay --> tmux --> Pi
  PWA            -- orchestrator gateway fallback --> worker relay ------^
```

CLI/Zellij prefer direct Tailnet attachment. The Tailnet-served PWA may proxy via the orchestrator gateway to avoid per-host browser/TLS/origin setup. Both paths use the same terminal frame protocol.

The global router does not send terminal bytes. It sends a semantic prompt through the bridge.

## 4. Pi release/config distribution for workers

### 4.1 Source of truth

The operator maintains a Git-controlled Pi fleet source only on the orchestrator machine. Workers do not clone or fetch this repository.

A release build produces an immutable artifact:

```text
manifest.json        # release ID, source commit, Pi version, config revision, hashes
runtime/             # isolated Bun/Pi runtime
agent-config/        # managed extensions, agents, skills, settings fragments
```

Provider API keys, if used initially, are injected from an ignored orchestrator-local release input and never committed to Git.

### 4.2 Direct rollout

`wh_pi_release_push` (name provisional) streams the release artifact directly to a worker over the existing orchestrator-to-worker transfer path. It stages to:

```text
${WH_DIR}/pi/releases/<release-id>.tmp/
```

The worker verifies `manifest.json` and hashes, then atomically promotes it to:

```text
${WH_DIR}/pi/releases/<release-id>/
${WH_DIR}/pi/current -> releases/<release-id>
```

The release transport must support runtime artifacts larger than the current 10 MB file-upload API limit; reuse streamed SSH/rsync-style transfer rather than worker Git access.

Only managed Pi configuration paths are replaced. Session history/state must not be overwritten. Existing Pi processes retain their loaded runtime/config until they exit or explicitly reload; newly launched delegated sessions use `pi/current`.

### 4.3 Initial provider configuration

Initial releases configure the existing provider API key and one fixed model. LiteLLM is a later provider-only migration and must not block session/attach work.

## 5. Session bridge

### 5.1 Registration and liveness

`pi-session-bridge` auto-loads from the normal Pi extension locations. It registers each session with:

- Pi session ID plus bridge incarnation;
- session type (`interactive`, `delegated`, `global-router`);
- host/worker ID, CWD, session name, model, Pi/config revision;
- terminal locator (`tmux`, `zellij`, or none);
- `attachable` flag;
- lifecycle state and `last_activity_ts`.

It emits `working`, `idle`, and `stopped`; a future `needs_input` display may be derived from Pi lifecycle/bridge state but must not claim certainty without a reliable signal.

The bridge sends periodic liveness. The orchestrator reaper marks stale sessions stopped. Bare-terminal sessions are registered but use `attachable=false`. A session can opt out through bridge configuration.

### 5.2 Transport adapters

The same extension has two deterministic transports:

| Environment | Bridge transport |
|---|---|
| Non-worker interactive/global session | Direct control/session API on `:12889` plus direct relay endpoint advertisement. |
| Worker delegated session | Local Unix socket to `worker_daemon`; daemon publishes events to worker-ingest endpoints on `:12888` and serves commands/attach through the worker relay. |

Worker mode is selected by an explicit `WH_WORKER=1` marker and verified Unix socket availability. Background handles must be aborted on `session_shutdown` and recreated after session replacement/reload; no stale extension context may survive.

### 5.3 Prompt injection

The bridge accepts a typed command with a message and delivery mode. It uses the supported Pi API:

```ts
pi.sendUserMessage(message, { deliverAs: "steer" | "followUp" })
```

`steer` is used for a working session; `followUp` queues after a settled session. The bridge reports command acceptance and resulting session events.

## 6. Terminal attachment

### 6.1 Plain Pi is supported

The operator continues to run plain `pi` inside tmux or Zellij; no mandatory `wh pi start` wrapper is introduced.

On attach request, the bridge/relay uses its captured locator to start a second local multiplexer client inside a PTY:

```text
tmux:   tmux attach-session -t <captured-session>
zellij: zellij attach <captured-session>
```

The helper forwards terminal input, output, resize, and close frames. tmux is the first supported implementation. Zellij v1 may attach the whole session rather than focus the exact pane.

### 6.2 Terminal protocol

The protocol is shared by direct and gateway routes and contains:

- `open(session_id, rows, cols)`;
- raw terminal `input` and `output` byte frames;
- `resize(rows, cols)`;
- `close(reason)`;
- `status`/reconnect metadata.

All trusted clients may write in v1, matching normal shared tmux behavior. Concurrent input interleaves by arrival order; this is accepted UX, not treated as an authorization violation.

The PWA gateway is a transport proxy only. It does not attempt to render Pi semantics itself.

## 7. Session data and APIs

### 7.1 Data model

Add orchestrator-owned tables:

```text
pi_sessions(
  id, pi_session_id, incarnation, type,
  worker_id nullable, host_name, cwd, display_name,
  model, pi_version, config_revision,
  terminal_locator JSON, attachable,
  state, last_activity_ts, parent_session_id nullable,
  created_at, stopped_at
)

pi_session_events(
  id, session_id, sequence, type, payload JSON,
  occurred_at, received_at
)

pi_delegations(
  id, parent_session_id, worker_id, child_session_id,
  task, state, created_at, completed_at
)
```

Extend jobs with:

```text
kind = ssh | delegated
origin_session_id nullable
```

All session/delegation/job writes are performed by the orchestrator process: direct control handlers for non-worker sessions and worker-ingest handlers for worker reports. Workers and CLI clients never open SQLite directly for this data.

### 7.2 Control API additions

Operator-facing endpoints on `:12889`:

```text
GET  /api/v1/pi/sessions
GET  /api/v1/pi/sessions/{id}
GET  /api/v1/pi/sessions/{id}/events
GET  /api/v1/pi/events/stream
POST /api/v1/pi/sessions/{id}:prompt
POST /api/v1/pi/sessions/{id}:cancel
POST /api/v1/pi/delegations
GET  /api/v1/pi/delegations/{id}
```

Worker-ingest endpoints on `:12888`:

```text
POST /pi/worker/{worker_id}/sessions
POST /pi/worker/{worker_id}/events
POST /pi/worker/{worker_id}/jobs
```

The control service communicates commands to a worker daemon through its direct worker relay endpoint; workers do not call `:12889`.

## 8. Delegation

### 8.1 Public tool

Interactive, non-worker Pi sessions receive:

```ts
wh_delegate({
  task: string,
  worker_id?: string,
  worker_selector?: { labels?: string[], min_gpu_vram_gb?: number },
  timeout_seconds?: number,
  sync?: boolean
})
```

`sync=false` returns `{ delegation_id, child_session_id }` immediately. `sync=true` waits for final child state/result until `timeout_seconds`; it does not fabricate completion if the worker is unreachable.

Add read actions for Pi sessions, delegations, and linked jobs.

### 8.2 Fixed delegated profile

Delegated children run from worker home and receive only:

```text
read, write, edit, grep, find, ls,
bash (worker override),
context-mode tools
```

They do not receive `wh_*`, vault tools, subagent tools, `claude_plan`, `specs_plan`, or the file-permission extension. They cannot delegate further.

### 8.3 Transparent bash override

A worker-only extension registers a custom tool named `bash`, intentionally replacing Pi's builtin tool. The override:

1. asks the local worker daemon over Unix socket to create a tmux job;
2. stores standard command output in a durable log;
3. waits for the normal exit marker and returns a standard-like bash result;
4. reports job creation/update to the orchestrator with `kind=delegated` and `origin_session_id`.

Long-running work remains observable through existing job list/log surfaces. The parent may see a short propagation delay while worker reports reach the orchestrator.

## 9. Global router

The global router is one special Pi session on the orchestrator. It has no filesystem or worker-management tools. It receives only:

```text
list_active_interactive_sessions
inspect_session_state
send_session_prompt
```

It selects the likely target from session name, CWD, recent activity, and explicit user references. It asks only when no target is clearly favored. It never creates a remote session; it routes to registered interactive sessions.

## 10. Client delivery

1. **CLI:** `wh pi sessions`, `wh pi watch`, `wh pi attach`, `wh pi delegate`.
2. **tmux/Zellij:** local focus where locator matches; otherwise an attach client in a new pane.
3. **PWA:** Tailnet-served session list/state, xterm-style terminal attachment, and global-router chat. It uses orchestrator proxy fallback for browser transport.

## 11. Milestones and acceptance gates

**Implementation status (2026-07-24; source status updated):** the direct worker relay, durable worker-local delegated-session records, PTY-backed tmux attachment, release activation helper, and control-plane delegation/session/prompt/cancel APIs are implemented. A worker binds a loopback FastAPI/WebSocket relay on `WH_PI_RELAY_PORT` (default `27888`), publishes only that TCP port through userspace Tailscale Serve, advertises relay capability in its heartbeat schema, and removes its specific Serve rule during graceful daemon shutdown. A direct Tailnet WebSocket now receives typed status plus terminal byte frames and can resize/input a disposable tmux client without ending its Pi. `scripts/build-pi-release.sh` packages a Bun/Pi release from the orchestrator host; `wh-pi-release-activate` verifies hashes and atomically changes `WH_DIR/pi/current`. The parent Pi extension has `wh_delegate`/Pi-session tool support.

Worker event ingest is now implemented in source: the relay stores each lifecycle transition in a per-session, persisted FIFO outbox with stable event IDs and retries it in order after failures/restarts; registration-port ingest deduplicates retried IDs. Delegation duration gates report acknowledged expiry as `stopped` and unacknowledged expiry as `termination_unknown`. `wh_delegate.sync` waits for an idle/known terminal projection, applies the duration gate at its exact deadline, and returns `settled=false` for `termination_unknown` rather than inventing a result. These additions have local regression coverage; live end-to-end validation requires deployment of the current orchestrator and worker images. The relay intentionally does **not** infer `idle` from tmux output: a reliable Pi lifecycle bridge is still required before `sync=true` can observe ordinary model-turn completion. The bridge-based `bash` job override, that lifecycle bridge, interactive-session bridge, global router, and PWA remain future milestones.

**Current Phase A checklist:** A1 (delegation smoke) and A2 (release provider config) are live-verified. A3 (durable worker event ingest), A4 (timeout/`termination_unknown`), and A5 (`sync`) are implemented and regression-tested in source, but remain pending a deployment smoke. A3/A5 remain partially incomplete until the worker Pi lifecycle bridge reports trustworthy `idle` transitions. Do not mark M5 complete until delegated `bash` jobs exist.

### M0 — contracts and schema

Define session/job schemas, state transitions, relay frame protocol, direct-vs-gateway transport selection, and SQLite single-writer ownership.

**Gate:** schema/API unit tests cover state transitions, stale session reaping, event idempotency, and linked job visibility.

### M1 — worker Pi release rollout

Implement release build, direct streamed artifact push, staged hash verification, promotion, and heartbeat version fields.

**Gate:** canary worker receives a release; a running child retains old runtime while a new child reports the new Pi/config revision.

### M2 — non-worker bridge, registry, and injection

Implement direct bridge registration/liveness/event upload, session list/events APIs, and `sendUserMessage` command delivery. Implement global router after this API exists.

**Gate:** plain Pi inside tmux registers; router sends a steer/follow-up; response appears in original terminal and session event stream; reload leaves no stale bridge handle.

### M3 — tmux relay and direct attach

Implement host relay helper and direct terminal WebSocket protocol for tmux. Add gateway proxy fallback.

**Gate:** attach from a second Tailnet machine, resize, disconnect/reconnect, send input, and continue the same Pi session. Confirm accepted concurrent-input behavior.

### M4 — worker daemon session service

Extend worker daemon with Unix socket bridge API, loopback relay service, Tailscale Serve publication, worker session/event/job ingest, and lifecycle/reconnect management.

**Gate:** a local worker bridge registers a synthetic delegated session; an operator connects to its published relay; other worker-tagged nodes are denied by ACL.

### M5 — async `wh_delegate` and delegated bash jobs

Implement worker child launch, fixed tool profile, `bash` override, local tmux executor, job reporting, and asynchronous delegation state.

**Gate:** an interactive Pi delegates to a selected worker; child edits/runs commands; every command is listed as a linked delegated job with logs; child has no `wh_*` or subagent tools.

### M6 — sync delegation and worker-child attach

Add sync wait/result behavior, cancellation, direct/gateway attach to worker children, and linked session/job UI details.

**Gate:** attach to a running delegated child from a second device, inspect its command logs, cancel it, and observe terminal/session/job convergence.

### M7 — Zellij and PWA

Implement local-focus/remote-attach Zellij UX and Tailnet PWA session directory, terminal, and global-router chat.

**Gate:** phone/browser attaches through gateway fallback; Zellij focuses local session or opens a remote attach pane; global router forwards a request and displays the linked response.

## 12. Required spikes before broad implementation

1. **Tmux relay:** host-side PTY helper, direct WebSocket, resize/reconnect/input test.
2. **Bridge lifecycle:** session replacement/reload while bridge transport is active.
3. **Worker relay publication:** loopback worker daemon service published with userspace `tailscale serve`, direct connection from an ordinary member, denied worker-to-worker path.
4. **Bash override:** same-name extension tool overrides builtin `bash`, returns compatible results, and cannot expose blocked tools.
5. **Release artifact:** direct large-file rollout and integrity verification into WH_DIR.
6. **SQLite concurrency:** concurrent bridge/worker event writers plus control reads remain correct under WAL/busy timeout.

## 13. Explicit non-goals

- Interactive-session sandboxing.
- Any access-control layer beyond the manually approved Tailnet and existing ACL segmentation.
- Vault on workers/delegated children.
- LiteLLM in the first release.
- Git/checkouts/deploy keys on workers for Pi release distribution.
- Public browser access, Funnel, native mobile apps, and multi-tenant operation.
- Exact-pane Zellij focus in the first attach release.
- Guaranteeing exactly-once execution or solving simultaneous terminal input interleaving.
