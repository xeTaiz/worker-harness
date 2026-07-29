---
title: Distributed Pi Session Fabric
status: active
scope: single-operator Tailnet
created: 2026-07-24
updated: 2026-07-29
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

CLI and multiplexer clients prefer direct Tailnet attachment. The Tailnet-served PWA uses the orchestrator gateway by default to avoid per-host browser/TLS/origin setup. CLI clients fall back to the gateway when direct relay connection fails. Both paths use the same terminal frame protocol and durable writer lease.

**V1 transport decision:** direct relay TCP `27888` is the primary data path and the only relay port published through Tailscale Serve. The previously explored internal `SessionTunnelManager`/SSH-local-forward replacement is not part of the current roadmap. Reconsider it only if direct Tailnet relay reachability or the approved ACL posture proves insufficient. The existing user-managed tunnel plane remains separate.

The global router is orthogonal to terminal clients and does not send terminal bytes. It sends semantic prompts through the bridge.

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

### 6.1 Plain Pi and tmux v1

The operator continues to run plain `pi` inside tmux or Zellij; no mandatory `wh pi start` wrapper is introduced.

The tmux implementation is feature-complete. The bridge captures stable tmux socket and pane identity, and the host relay resolves mutable session/window/pane indices. `wh pi attach` switches directly to the original pane when it belongs to the invoking tmux server; otherwise it starts a disposable linked tmux client in a PTY and streams it through the relay. Native attachment supports fullscreen rendering, upgrade-time dimensions, resize polling, raw input/output, `Ctrl-]` detach, and agent cycling across local and remote sessions. The companion dotfiles reserve `Ctrl-a` as a Worker Harness prefix while leaving tmux's normal prefix unchanged.

The remaining tmux work is rollout and acceptance across all hosts, not new attachment architecture. Attachments created before pane-marker support must be reopened once before cycling.

Zellij follows durable leasing and gateway fallback. Its adapter must provide the same behavior: exact local-pane focus where the Zellij API permits it, remote PTY attachment, resize, detach, and cross-agent cycling. A full plugin is optional; CLI and keybinding parity come first.

### 6.2 Terminal protocol

Protocol v2, currently deployed, contains:

- WebSocket upgrade with `session_id`, initial `rows`, and `cols`;
- raw terminal `input` and `output` byte frames;
- `resize(rows, cols)`;
- `close(reason)`;
- `status`/reconnect metadata.

Direct and gateway routes share this framing. Protocol v3 will add `role`, lease identity, monotonic writer epoch, lease-loss/handoff status, and reconnect metadata. The gateway remains a byte transport proxy and does not reinterpret Pi semantics.

### 6.3 Durable writer lease and handoff — next milestone

The current relays are not a durable ownership system: the interactive host relay has an in-memory one-writer exclusion, while the delegated worker relay does not yet provide equivalent durable fencing. Before Zellij, both relays must implement one shared model:

- the orchestrator SQLite database is authoritative for the current writer lease and monotonic per-session writer epoch;
- one read-write attachment is allowed per session;
- read-only observers never send terminal input and do not own the writer lease;
- a stable device ID plus unique attach/lease ID identifies the holder for UI, renew, reconnect, and audit;
- lease acquisition, renewal, release, expiry, and explicit takeover are serialized transactionally using orchestrator time;
- a higher epoch fences every lower-epoch writer on both direct and gateway paths;
- cycling and clean detach release promptly; crash recovery relies on bounded expiry;
- reconnect with the same unexpired lease resumes the same tmux client view without restarting Pi;
- cross-device takeover closes or demotes the old writer and redraws the same surviving Pi session on the new device.

For restart-safe fencing, a relay must not forget the highest accepted epoch. The implementation must either persist the high-water mark locally and have the orchestrator synchronize each grant before returning it to a client, or use an equivalently strong relay-validation mechanism. Merely carrying an epoch from the client to an in-memory relay is insufficient because a relay restart could temporarily admit a stale writer.

Proposed timing is a 30-second lease with renewal every 10 seconds. Existing direct streams should remain usable during a short orchestrator outage; no new takeover can occur until authority returns. Final expiry/grace behavior and takeover confirmation are explicit decisions to settle before implementation.

The lease unquestionably fences Worker Harness relay/gateway writers. The original physical tmux client on the source host is a separate decision: leaving it as trusted break-glass access is simple and matches the Tailnet/single-operator model, while strict exclusivity would require dynamically toggling matching tmux clients read-only (`switch-client -r`) and reliably restoring them as they navigate. V1 must state which guarantee it makes rather than implying that a remote lease can silently control arbitrary local keyboard input.

Observer support should begin with delegated sessions. Interactive tmux observers must not zoom or resize the operator's shared source window; semantic transcript viewing remains the safe read-only fallback until a non-perturbing raw observer path is proven.

### 6.4 Orchestrator gateway fallback — following lease enforcement

The control service on `:12889` will expose a WebSocket gateway that opens the selected direct host/worker relay upstream and pumps protocol frames in both directions. It uses the same lease and writer epoch as the direct path; direct and gateway clients therefore cannot become simultaneous writers.

The gateway requirements are:

- no SQLite work in the per-frame byte pumps;
- strict receive-then-send backpressure with bounded WebSocket queues;
- no unbounded terminal buffering or dropped terminal bytes;
- per-send stall watchdog and clean close propagation;
- a small configurable concurrency cap and observable refusal/health metrics;
- direct-first fallback for CLI/multiplexer clients and gateway-first same-origin behavior for the PWA;
- reconnect using the existing lease where valid, otherwise an explicit lease-lost/takeover flow.

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
  writer_epoch,
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

pi_attach_leases(
  session_id, lease_id, device_id, client_label,
  role, writer_epoch, acquired_at, renewed_at, expires_at
)
```

`pi_attach_leases` stores the current durable writer holder; lease lifecycle and handoff events are also appended to `pi_session_events` for diagnosis. `writer_epoch` is monotonic and never reused. Observers may be connection-scoped rather than durable rows, but are bounded and visible in attachment status/metrics.

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
GET  /api/v1/pi/sessions/{id}/attach-info
POST /api/v1/pi/sessions/{id}/attach-leases
POST /api/v1/pi/sessions/{id}/attach-leases/{lease_id}:renew
DELETE /api/v1/pi/sessions/{id}/attach-leases/{lease_id}
WS   /api/v1/pi/sessions/{id}/attach-gateway
```

Lease mutation is explicit and never hidden in a cacheable `GET`. `attach-info` reports protocol capabilities plus direct and gateway URLs. Acquisition returns `409` when another unexpired writer holds the session unless the caller explicitly requests a confirmed takeover.

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

Long-running work remains observable through existing job list/log surfaces. The parent may see a short propagation delay while worker reports reach the orchestrator. The existing log reader uses Tailscale SSH, so the Tailnet policy must grant `tag:wh-orchestrator` SSH access to `tag:wh-worker`. On archdome's rootless Apptainer installation, the approved `WH_FAKEROOT=1` canary also supplies the credential-switch capability inner `tailscaled` needs; direct SSH, generic dispatch, and linked log retrieval are live-verified under that runtime mode.

## 9. Global router

The global router is one special Pi session on the orchestrator. It has no filesystem or worker-management tools. It receives only:

```text
list_active_interactive_sessions
inspect_session_state
send_session_prompt
```

It selects the likely target from session name, CWD, recent activity, and explicit user references. It asks only when no target is clearly favored. It never creates a remote session; it routes to registered interactive sessions.

## 10. Client delivery

1. **CLI:** `wh pi sessions`, `wh pi events`, `wh pi prompt`, and `wh pi attach` are implemented. `wh pi watch` and a CLI delegation convenience command remain optional parity work because the Pi extension already exposes delegation.
2. **tmux:** local exact-pane focus and remote attachment are implemented, including fullscreen resize, detach, and cross-agent cycling.
3. **Zellij:** follows lease and gateway completion and reuses the same discovery/attachment contract; it is a client adapter, not a new session plane.
4. **Mobile webapp/PWA:** the Tailnet-served session directory, durable semantic transcript, session switching, prompt/steer composer, model/thinking controls, and terminal preview are implemented. Durable writer/observer UX, gateway transport, HTTPS installation, and optional xterm-grade rendering remain.

The global router in §9 is an orthogonal agent/service, not another terminal client. Its implementation and acceptance must not be combined with Zellij or PWA delivery milestones.

Semantic transcript events share the durable `pi_session_events` log. SQLite assigns a monotonically increasing sequence per session; `GET /api/v1/pi/sessions/{id}/events?after=<sequence>` provides bounded replay and `/stream` tails the same log as SSE using that sequence as `Last-Event-ID`. Both ordinary and delegated bridges batch text deltas, emit final sanitized messages plus tool/lifecycle status, omit hidden thinking blocks, and retry stable event IDs. Worker children first persist accepted batches into the existing worker-local FIFO before registration-port delivery. The webapp uses SSE for replayable output and the existing semantic prompt API for input; tool calls/results are compact previews with explicit full-text expansion. Interactive bridges also report their current/available models and thinking level, and accept durable `configure` commands from web controls through the same claim/ack queue. Raw terminal transport remains a separate protocol/UI mode from the semantic transcript even though both are now implemented.

## 11. Milestones and acceptance gates

**Implementation status (2026-07-29):** the worker/delegation plane, session/event plane, semantic web client, and tmux client are implemented.

- Delegated children use immutable activated releases, isolated homes, explicit Pi lifecycle hooks, truthful timeout states, durable outboxes, and private Unix-socket bash/job execution linked by `origin_session_id`. The approved archdome `WH_FAKEROOT=1` canary remains required and live-verified. Delegated children receive no parent Worker Harness, vault, or planning/subagent surfaces.
- Ordinary interactive bridges register on `:12889`, survive incarnation replacement, report lifecycle/model/thinking state, upload durable sanitized transcript events, and claim/ack prompt/configure commands. Delegated reports enter through `:12888`; stale projections receive permanent `410 Gone` handling.
- SQLite sequence cursors, bounded replay, SSE, latest-exchange backfill, and the mobile-first semantic webapp are implemented. Live acceptance still needs repeated-reload deduplication and orchestrator-restart bridge re-registration checks.
- Direct protocol-v2 terminal relays are implemented for delegated worker sessions and ordinary tmux sessions. The native CLI supports attachable-only discovery, exact local-pane focus, remote fullscreen streaming, reliable initial/dynamic sizing, `Ctrl-]`, and a dedicated tmux cycling key table. The latest tmux slice is feature-complete; fleet rollout and a local/remote/delegated cycling matrix remain.
- The next shared milestones are protocol-v3 durable writer leases/handoff, observer semantics, and the orchestrator gateway. Zellij follows those shared layers. The global router remains a separate orthogonal milestone.

### M0 — contracts and schema — complete

Define session/job schemas, state transitions, relay frame protocol, direct-vs-gateway transport selection, and orchestrator-only SQLite write ownership. Protocol-v3 attachment-lease schema is added in M3b.

**Gate:** schema/API unit tests cover state transitions, stale session reaping, event idempotency, and linked job visibility.

### M1 — worker Pi release rollout — complete

Implement release build, direct streamed artifact push, staged hash verification, promotion, and heartbeat version fields.

**Gate:** canary worker receives a release; a running child retains old runtime while a new child reports the new Pi/config revision.

### M2 — non-worker bridge, registry, and injection — complete

Implement direct bridge registration/liveness/event upload, session list/events APIs, and `sendUserMessage` command delivery. The global router consumes this API later in M9.

**Gate:** plain Pi inside tmux registers; a steer/follow-up appears in the original terminal and durable session event stream; reload leaves no stale bridge handle.

### M3a — tmux relay and direct attach — feature-complete

The host relay, native terminal client, direct local focus, remote PTY stream, fullscreen sizing, resize, detach, and cross-agent cycling are implemented.

**Remaining gate:** deploy current Worker Harness/dotfiles to every operator and interactive host; validate local→local, local→remote, remote→local, and delegated cycling plus reconnect and clean detach.

### M3b — durable lease, observers, and handoff — next

Implement protocol-v3 lease APIs, monotonic writer epochs, restart-safe relay fencing, one writer across direct/gateway paths, bounded observers, renew/release/takeover, and client reconnect state machines.

**Gate:** two devices cannot write concurrently; explicit handoff continues the same Pi session; stale clients are fenced even across relay/orchestrator restart; observer input is impossible; crash recovery converges within the documented bound.

### M3c — orchestrator gateway fallback — follows M3b

Implement the bounded `:12889` WebSocket proxy using the same protocol-v3 lease and frame semantics. PWA uses gateway-first; CLI/multiplexer clients use direct-first with fallback.

**Gate:** direct and gateway clients race safely under one writer epoch; resize/input/detach/reconnect work through the proxy; slow/dead clients cannot create unbounded memory or starve control-plane SQLite work.

### M4 — worker daemon session service — complete

Extend worker daemon with Unix socket bridge API, loopback relay service, Tailscale Serve publication, worker session/event/job ingest, and lifecycle/reconnect management.

**Gate:** a local worker bridge registers a synthetic delegated session; an operator connects to its published relay; other worker-tagged nodes are denied by ACL.

### M5 — async `wh_delegate` and delegated bash jobs — complete

Implement worker child launch, fixed tool profile, `bash` override, local tmux executor, job reporting, and asynchronous delegation state.

**Gate:** an interactive Pi delegates to a selected worker; child edits/runs commands; every command is listed as a linked delegated job with logs; child has no `wh_*` or subagent tools.

### M6 — sync delegation and worker-child attach — direct path complete

Sync wait/result behavior, truthful cancellation/timeout handling, direct worker-child attachment, and linked session/job UI details are implemented. Durable ownership and gateway transport are shared M3b/M3c work rather than delegation-specific work.

**Gate:** attach to a running delegated child from a second device, inspect its command logs, cancel it, and observe terminal/session/job convergence; then rerun through the gateway after M3c.

### M7 — Zellij adapter — after M3c

Implement Zellij registration metadata, exact local focus where supported, remote PTY attachment, resize/detach, and the same cross-agent cycling UX. Reuse protocol v3, leases, and gateway fallback; do not invent a second attachment control plane.

**Gate:** Zellij focuses an existing local Pi pane or opens a remote attachment, then cycles between local/remote/delegated agents with the same ownership and handoff behavior as tmux.

### M8 — PWA attachment hardening

Add writer/observer/takeover UI, automatic gateway reconnect, lease-loss handling, and HTTPS publication for installable behavior. An xterm-grade renderer is optional and does not block semantic chat.

**Gate:** a phone/browser observes or owns a session through the gateway, survives reload/network interruption, and hands ownership to/from the native client without duplicate writers.

### M9 — global router — orthogonal

Implement the constrained router from §9 independently of terminal-client delivery. It lists active interactive sessions and sends semantic prompts; it neither owns terminal leases nor blocks Zellij/PWA work.

**Gate:** ambiguous requests ask for a target; unambiguous requests reach the intended existing interactive session and render the linked durable response.

## 12. Required spikes and acceptance work

Completed spikes: tmux PTY relay/direct WebSocket/resize/input, bridge replacement and stale-context handling, worker relay publication with worker-tag ACL denial, same-name delegated bash override, immutable release activation, and SQLite event idempotency/concurrency.

Remaining work, in order:

1. **Tmux closure matrix:** current versions on every host; local/remote/delegated cycle and wraparound; reconnect; orchestrator restart re-registration; repeated web reload without duplicate backfill.
2. **Lease/fence prototype:** two clients contend for one delegated session; transactional acquire/renew/release/takeover; relay restart preserves the high-water fence; old writer cannot send after takeover.
3. **Observer prototype:** delegated `tmux attach -r` path, input dropped by relay, bounded observer count, no effect on writer dimensions. Interactive raw observers remain gated on a non-perturbing layout/size proof.
4. **Gateway backpressure:** direct upstream relay plus deliberately slow downstream client; prove bounded memory, send watchdog, close propagation, and direct/gateway writer fencing.
5. **Zellij adapter spike:** stable session/tab/pane identity, exact local focus command, second-client PTY behavior, resize semantics, and detach cleanup before production integration.
6. **PWA HTTPS:** choose Tailnet HTTPS publication and validate service-worker installation/update behavior.

## 13. Explicit non-goals

- Interactive-session sandboxing.
- Any access-control layer beyond the manually approved Tailnet and existing ACL segmentation.
- Vault on workers/delegated children.
- LiteLLM in the first release.
- Git/checkouts/deploy keys on workers for Pi release distribution.
- Public browser access, Funnel, native mobile apps, and multi-tenant operation.
- Replacing direct relay `27888` with an internal SSH `SessionTunnelManager` unless a demonstrated reachability/ACL problem reopens that decision.
- Collaborative multi-writer terminals, CRDT terminal editing, or treating interleaved concurrent input as acceptable ownership semantics.
- Guaranteeing exactly-once execution; commands/events remain idempotent and uncertainty is surfaced explicitly.
