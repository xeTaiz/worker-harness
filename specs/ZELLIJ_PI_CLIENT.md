---
title: Zellij Pi Session Client and Host-Relay Adapter
status: active
created: 2026-08-01
updated: 2026-08-01
owner: Worker Harness
milestone: M7
---

# Zellij Pi Session Client and Host-Relay Adapter

## 1. Goal

Make Zellij a first-class Worker Harness operator client and interactive-session host while preserving the existing terminal fabric contracts:

- delegated worker Pi sessions remain tmux-backed;
- interactive Pi may run in either tmux or Zellij without a wrapper;
- a client running in either tmux or Zellij can attach to either source multiplexer;
- same-host, same-multiplexer targets focus the original pane directly;
- cross-host and cross-multiplexer targets use the existing protocol-v2 direct WebSocket with orchestrator-gateway fallback;
- the picker, next/previous cycling, detach, resize, bounded multi-attach, idle timeout, and longest-idle replacement behave the same as the tmux client.

This is a client/host-relay milestone. It must not introduce a second registry, a second terminal protocol, an orchestrator schema change, a worker-image requirement, or Zellij-specific attachment ownership.

## 2. Fixed decisions

1. **Delegated workers stay on tmux.** A Zellij operator pane runs `wh pi attach` and renders the worker relay's disposable tmux client. No worker runtime conversion is required.
2. **Source and client multiplexers are independent.** A tmux client attaching to a Zellij-hosted Pi, or a Zellij client attaching to a tmux-hosted Pi, uses terminal streaming.
3. **Direct local focus is same-multiplexer only.** Tmux keeps socket/pane switching. Zellij uses its client-aware pane actions.
4. **Zellij 0.44.2 is the initial compatibility floor.** The adapter relies on `ZELLIJ_SESSION_NAME`, `ZELLIJ_PANE_ID`, `zellij action list-panes --json --all`, `focus-pane-id`, `switch-session --pane-id`, and `list-clients`.
5. **No WASM plugin in v1.** The existing native CLI/fzf picker plus KDL bindings are sufficient. A custom dashboard/status-bar plugin is optional follow-up work.
6. **Protocol v2 is unchanged.** Direct/gateway framing, close codes, capacity eight, one-hour idle timeout, and longest-idle replacement remain authoritative.
7. **One Bun host relay supports both source multiplexers.** Tailscale Serve continues to publish only `27888`; loopback remains `127.0.0.1:27890`; the UDS remains mode `0600`.

## 3. Behavior matrix

| Source Pi | Operator client | Path |
|---|---|---|
| delegated tmux worker | Zellij | direct relay, gateway fallback |
| interactive tmux, same tmux server | tmux | exact local pane focus |
| interactive tmux | Zellij or remote tmux | host-relay stream |
| interactive Zellij, same Zellij host | Zellij | exact client-local session/pane focus |
| interactive Zellij | tmux or remote Zellij | host-relay stream through a disposable Zellij client |
| bare terminal | any | visible/messageable, `attachable=false` |

## 4. Multiplexer-neutral route contract

The bridge-to-relay UDS registration becomes a backward-compatible discriminated union.

### 4.1 Tmux locator

```json
{
  "action": "register",
  "session_id": "...",
  "incarnation": "...",
  "multiplexer": "tmux",
  "tmux_socket": "/tmp/tmux-1000/default",
  "tmux_pane_id": "%12"
}
```

Legacy registrations that omit `multiplexer` but include tmux fields remain accepted as tmux.

### 4.2 Zellij locator

```json
{
  "action": "register",
  "session_id": "...",
  "incarnation": "...",
  "multiplexer": "zellij",
  "zellij_session_name": "Pi",
  "zellij_pane_id": "terminal_8"
}
```

The bridge derives the pane from `ZELLIJ_PANE_ID`; bare numeric values are normalized to `terminal_<id>`. Session names and pane IDs are treated as data, never interpolated into an unquoted shell command.

### 4.3 Describe response

`describe` returns `multiplexer` plus the relevant locator. Existing tmux fields remain unchanged for compatibility.

The orchestrator only receives `terminal_attachable`, host, port, and protocol version, as today. Multiplexer-specific locators remain host-local in the protected UDS relay.

## 5. Host-relay adapter

Refactor host-relay route and attachment operations behind a small multiplexer boundary while preserving common reservation/WebSocket logic.

### 5.1 Common operations

- route registration, incarnation replacement, TTL, unregister, and stale cleanup;
- direct WebSocket upgrade and protocol-v2 status;
- initial dimensions, input, changed-resize activity, output forwarding;
- cap eight, exact attachment IDs, longest-idle replacement, `4410`;
- idle detach, `4408`;
- Tailscale publication and health.

### 5.2 Tmux adapter

Retain current behavior unchanged: stable pane resolution, linked disposable session, exact target pane, shared zoom/layout reference count, final-only restoration, and linked-session cleanup.

### 5.3 Zellij adapter

- Liveness: `zellij --session <name> action list-panes --json --all`; require the registered non-plugin terminal pane.
- Local focus: same session uses `focus-pane-id`; another local Zellij session uses `switch-session <name> --pane-id <terminal_N>`.
- Remote/cross-multiplexer attachment: create a unique disposable bootstrap Zellij session/client in `Bun.Terminal`, then switch that client to the registered target session and pane.
- Client-specific focus proof: compare `list-clients` before and after bootstrap; the newly added client must report the target pane while pre-existing clients retain their focused panes.
- Suppress bootstrap-shell output until the target client is confirmed; then forward target redraw bytes and send protocol `connected` status.
- Resize only the disposable client PTY. The source session and other clients must remain alive and retain focus.
- Cleanup closes the disposable PTY/client and kills only the unique bootstrap session. It never kills the source Zellij session or target pane.
- Strip inherited `ZELLIJ*` variables from relay-spawned Zellij CLI/client environments so a relay launched from inside Zellij cannot accidentally act as the source client.

If exact client targeting cannot be confirmed within a bounded startup timeout, fail the attachment with a typed relay error and clean all bootstrap resources; never silently attach the wrong pane.

## 6. Native CLI behavior

`focus_local_session()` becomes multiplexer-aware:

- tmux route + matching current tmux socket: existing exact switch;
- Zellij route + current Zellij client:
  - same session: `zellij action focus-pane-id terminal_N`;
  - different session: `zellij action switch-session SESSION --pane-id terminal_N`;
- cross-multiplexer: return false so the ordinary direct/gateway streaming path runs.

Picker ordering, attach-info, raw terminal mode, direct-first/gateway fallback, close-code handling, and cycle ordering remain shared.

## 7. Zellij UX and shortcuts

Two access styles must coexist.

### 7.1 Direct bindings

Initial collision-free defaults:

- `Alt a`: open the attachable-session picker with local-focus optimization;
- `Alt u`: next agent from either an original local Zellij Pi pane or a stream;
- `Alt y`: previous agent from either source;
- `Ctrl-^` (`0x1e`) / `Ctrl-_` (`0x1f`): additional next/previous controls while in a native stream;
- `Ctrl-]` (`0x1d`): detach a native stream.

`Alt h/j/k/l` remain ordinary pane navigation and are not reused. Stream control bytes are consumed by `wh pi attach` and never reach the source PTY.

### 7.2 Prefix/input-mode bindings

`Ctrl-a` enters Zellij's existing prefix-like `tmux` input mode, while the existing `Ctrl-b` entry remains available. Add:

- `Ctrl-a Ctrl-a`: open the picker with `--stream` so the cycling process remains active;
- `Ctrl-a Ctrl-j` / `Ctrl-a Ctrl-l`: next;
- `Ctrl-a Ctrl-h` / `Ctrl-a Ctrl-k`: previous;
- `Ctrl-a x`: detach/return.

The built-in status bar therefore exposes the active input mode and available keys. It may label the fixed mode `TMUX`; a custom `WH-PI` label requires a status-bar/plugin follow-up and does not block v1.

### 7.3 Cycling control

The stream client accepts dedicated local control bytes for next/previous in addition to the existing SIGUSR path. These bytes are consumed locally and never reach the remote PTY.

`Alt-y/u` and the prefix-mode cycle keys launch a short in-place `wh pi cycle` helper. Zellij exposes the original pane as suppressed while the helper is active. For a streamed pane, a mode-0600 runtime marker maps that original pane to the active `wh` PID and the helper sends the existing SIGUSR direction before exiting; the original stream resumes and reconnects in the same process. For a directly focused local Pi pane, the helper asks host-relay revision 9 to reverse-resolve `(Zellij session, pane)` to the Worker Harness session ID, then attaches the relative target normally. This avoids process-tree guessing, control-byte injection into ordinary Pi, duplicate stream slots, and recursive local Zellij streaming.

## 8. Files and rollout

Expected implementation files:

- dotfiles `session-bridge.ts`;
- dotfiles `host-relay.ts`;
- Worker Harness `src/worker_harness/pi_terminal.py`;
- Worker Harness `src/worker_harness/cli/pi.py`;
- Worker Harness terminal/CLI tests;
- dotfiles `zellij/.config/zellij/config.kdl`;
- README and the parent distributed-session spec status.

No orchestrator image, SQLite migration, or worker SIF rebuild is expected. Operator hosts require updated dotfiles/CLI, host-relay revision restart, Pi `/reload`, and a fresh/reloaded Zellij configuration.

## 9. Implementation slices

### Z1 — capability spike and contract — completed

- exact same-session and cross-session local focus are supported by Zellij 0.44.2;
- an isolated two-client test confirmed different client IDs can focus different pane IDs in one source session;
- the revision-8 relay smoke registered the current Zellij Pi pane, opened an exact disposable client at `31x101`, returned protocol `connected`, and cleaned the attachment back to zero without stopping the source;
- mixed-client resize acceptance remains part of Z5;
- the discriminated route contract and cleanup invariant are recorded here.

### Z2 — bridge, route, and local focus

- add Zellij locator discovery/registration;
- add relay route union, liveness, describe, and re-key cleanup;
- add Python local-focus adapter;
- preserve tmux compatibility tests.

### Z3 — remote Zellij attachment

- implement disposable bootstrap client;
- exact target confirmation, startup timeout, suppressed bootstrap output;
- resize/input/output/cleanup;
- common cap/idle/replacement behavior.

### Z4 — picker/cycle/detach KDL UX

- direct bindings and `Ctrl-a` input-mode bindings;
- stream-local next/previous controls;
- directly focused local-session cycle/return helper;
- config validation and operator documentation.

### Z5 — live acceptance

- source/client matrix across local, remote, interactive, and delegated sessions;
- mixed tmux/Zellij routes on one host;
- direct and gateway fallback;
- multi-attach/longest-idle replacement;
- source tmux/Zellij sessions survive every detach and relay restart.

## 10. Acceptance gates

1. Plain Pi started in a Zellij terminal pane registers `attachable=true` with the exact stable session/pane locator.
2. Picking that session from the same Zellij client focuses its original pane without opening a WebSocket.
3. Picking another local Zellij session switches the current client to its exact pane.
4. A remote tmux and a remote Zellij client each attach to the same Zellij-hosted Pi through protocol v2 and can type/resize/detach.
5. A Zellij client attaches to delegated worker tmux with no worker changes.
6. The disposable Zellij client has an independent client ID and target pane; the original client's focused pane does not change.
7. Closing/replacing/idling a remote attachment removes only the disposable client/bootstrap session; source Zellij and Pi survive.
8. Direct and prefix-mode picker/cycle/detach bindings work without breaking existing lowercase Alt pane movement or normal `Ctrl-b` behavior.
9. Existing tmux and full non-live Worker Harness regressions remain green.

## 11. Non-goals

- converting delegated workers from tmux to Zellij;
- replacing the existing fzf picker with a WASM dashboard;
- a custom named Zellij input mode/status-bar plugin in v1;
- changing the orchestrator registry, terminal protocol, gateway, trust model, or attachment ownership semantics;
- attaching bare-terminal Pi sessions;
- synchronizing focus between multiple Zellij clients.
