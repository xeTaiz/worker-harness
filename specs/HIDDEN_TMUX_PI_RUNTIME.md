---
title: Hidden tmux Pi Runtime and wh pi start
status: implemented-pending-live-acceptance
created: 2026-08-02
updated: 2026-08-02
owner: Worker Harness
related:
  - DISTRIBUTED_PI_SESSION_FABRIC.md
  - ZELLIJ_PI_CLIENT.md
---

# Hidden tmux Pi Runtime and `wh pi start`

## 1. Goal

Add an optional managed launcher that makes tmux the invisible, uniform runtime backend for ordinary interactive Pi sessions while preserving Zellij, an unrelated outer tmux, a bare terminal, native `wh`, and the PWA as independent operator clients.

The intended command is:

```bash
wh pi start [launcher options] -- [Pi arguments]
```

A later shell alias may make this feel like ordinary `pi`, but plain `pi` in an existing tmux or Zellij pane remains supported. This proposal does not make a wrapper mandatory until its resume, failure, and rollout behavior has passed live acceptance.

## 2. Fixed design direction

1. **Tmux is the managed source backend.** Interactive sessions launched by `wh pi start` run in one dedicated hidden tmux server per host. Delegated workers remain on their existing tmux backend.
2. **The hidden server uses a distinct socket.** It is not the operator's outer tmux server. An outer tmux client therefore keeps its own status bar, windows, prefix, and session state while the managed Pi backend remains invisible.
3. **One Pi per window, one pane per window.** The managed server has no split-pane topology. This avoids relay zoom conflicts and lets each Pi window participate independently in tmux sizing.
4. **One owner session groups the Pi windows.** The launcher recreates the owner session when absent. It may disappear when the last Pi exits; the next launch recreates it.
5. **No visible backend chrome, but native scrollback, clipboard export, and complete modified-key input.** Force `status off`, `mouse on`, `set-clipboard external`, `extended-keys on`, `history-limit 50000`, and `window-size latest` on the managed server/owner session. On tmux 3.5+, also select `extended-keys-format csi-u`; tmux 3.2–3.4 keeps its supported `xterm` format. Relay-created grouped sessions defensively reinforce `status off`, `mouse on`, and `window-size latest`. The history limit must be set before the first pane is created because tmux fixes each pane's allocation at creation time.
6. **The backend is intentionally hard to operate directly.** Worker Harness owns creation, naming, attachment, cycling, detach, and cleanup. Lack of convenient nested tmux bindings is a benefit, not a UX defect, provided the Worker Harness escape/detach path is reliable.
7. **Protocol v2 remains unchanged.** This adds launcher and local-path policy only; no orchestrator schema, terminal protocol, worker SIF, attachment ownership, or gateway change is required.

## 3. Topology

```text
Host A
  dedicated tmux socket: $XDG_RUNTIME_DIR/worker-harness/pi-tmux.sock
  owner session: wh-pi
    window 1 / pane 1 -> Pi session A
    window 2 / pane 1 -> Pi session B
    window 3 / pane 1 -> Pi session C

Operator client
  Zellij pane, outer tmux pane, or bare terminal
    -> wh native attachment
    -> local loopback relay for same-host managed Pi
    -> disposable client on dedicated tmux socket
    -> exact Pi window/pane

Remote client
    -> direct Tailnet relay, gateway fallback
    -> disposable client on dedicated tmux socket
    -> exact Pi window/pane
```

The outer tmux and hidden tmux are truly nested only in the terminal data path. They are different servers/sockets. Tmux direct-focus/`switch-client` support has been removed, so the managed Pi is streamed into the current outer pane regardless of socket identity. This preserves the outer status bar and makes outer tmux, Zellij, and a bare terminal follow the same attachment UX.

## 4. Identity: generated ID plus optional human name

The operator is not expected to know or type a session UUID.

For a new session, `wh pi start` generates a UUID internally and launches Pi with:

```bash
real-pi --session-id <generated-uuid> --name <human-name> ...
```

- `--session-id` is the private correlation key known to the launcher, bridge, host relay, and orchestrator.
- `--name` is the optional human-facing selector shown in `wh pi sessions`, fzf, Zellij, and the PWA.
- If no name is supplied, derive a readable default from the CWD plus a short suffix.
- Names need not be globally unique because attachment after launch uses the generated UUID.

Pi already supports both `--session-id` and `--name`. The launcher must not require the operator to remember either generated value.

Resume modes need explicit handling. If the operator supplies `--session`, `--session-id`, `--continue`, `--resume`, or `--fork`, the launcher must preserve Pi's semantics rather than blindly injecting a new incompatible ID. Initial implementation may support new sessions first and gate ambiguous resume modes with a clear error until their mapping is proven.

## 5. Start lifecycle

1. Resolve the real Pi executable before any optional `pi -> wh pi start` alias to avoid recursion. Prefer the private host-runtime manifest created by `wh host setup`; `WH_PI_EXECUTABLE` remains the explicit override.
2. Determine the managed socket and create its parent directory mode `0700`.
3. Ensure the dedicated tmux server and owner session exist.
4. In the first server-start command queue, set managed global options before creating the first pane; then reinforce the owner-session options on every launch:

   ```tmux
   set-option -g status off
   set-option -g mouse on
   set-option -s set-clipboard external
   set-option -g extended-keys on
   # tmux 3.5+ only; tmux 3.2–3.4 retains its xterm format
   set-option -g extended-keys-format csi-u
   set-option -g history-limit 50000
   set-option -t wh-pi status off
   set-option -t wh-pi mouse on
   set-option -t wh-pi history-limit 50000
   set-option -t wh-pi window-size latest
   ```

   `mouse on` lets wheel and drag gestures pass through the outer terminal multiplexer into the managed tmux's copy mode and authoritative scrollback. `set-clipboard external` makes a completed inner selection emit OSC 52 without accepting arbitrary pane applications as tmux buffers; an outer tmux may use `set-clipboard on` to accept and relay that selection to the local terminal clipboard. `extended-keys on` lets Pi distinguish modified Enter and other modified keys; `csi-u` is Pi's preferred format where tmux supports it. Existing panes cannot be enlarged retroactively; the configured history limit applies when each pane is allocated.

5. Generate the private session UUID and human display name.
6. Create a detached one-pane window and capture its stable pane ID. Treat the name and all Pi arguments as untrusted argv data: use an argv-safe launcher helper or strict shell quoting for tmux's command string; never concatenate operator/model-generated text into an executable shell fragment.

   ```text
   tmux -S <managed-socket> new-window -d -P \
     -t wh-pi -n <name> -F '#{pane_id}' \
     'exec <real-pi> --session-id <uuid> --name <name> ...'
   ```

7. Pi's ordinary bridge discovers the managed tmux socket/pane, registers the host-relay route, and asynchronously registers with the orchestrator.
8. Attach the invoking client using the strategy in §6.
9. If Pi exits, its one-pane window closes. Do not kill unrelated Pi windows.

Creating the backend window detached means no attach-then-detach dance is required.

## 6. Local attachment strategy

### 6.1 Zellij, outer tmux, and bare terminal

Use the native Worker Harness stream in the invoking pane. Because `wh pi start` already knows the generated session UUID, it can poll the host-relay UDS for that exact local route and then connect directly to:

```text
ws://127.0.0.1:27890/v1/sessions/<uuid>/attach
```

This local start path does not require an orchestrator lookup, Tailnet self-connection, Tailscale Serve, or gateway. The bridge still registers with the orchestrator in parallel so remote devices can discover and attach later.

The invoking `wh` process retains the existing raw-mode, resize, cycling, replacement, idle, and `Ctrl-]` detach behavior. Detach restores the outer terminal process directly; there is no cross-session return stack to maintain.

A bounded startup wait must show a clear `Starting Pi…` state. Normal local registration is expected to complete in well under one second, but correctness must use a documented timeout and actionable fallback/error rather than relying on that expectation.

### 6.2 Invocation from the managed backend itself

This is not a primary operator path. Tmux direct-focus/`switch-client` logic has been removed from the native client; even a client on the managed socket uses the ordinary terminal stream. This keeps attachment, detach, cycling, replacement, and sizing behavior uniform and prevents backend tmux state from becoming operator UI.

### 6.3 Existing unmanaged local sessions

`wh pi attach` now uses:

- any tmux source, including the same tmux server -> native stream;
- same immediate Zellij client -> exact client-local focus to prevent recursive rendering;
- cross-host or cross-multiplexer -> native stream.

The managed launcher does not remove compatibility with existing plain Pi sessions, but unmanaged tmux Pi panes no longer receive a direct `switch-client` optimization.

## 7. Resize behavior

A detached Pi window may initially have a stale/default size. This is acceptable: when the first client attaches, the protocol carries its current rows/columns, the relay creates/resizes the disposable tmux client, tmux's `window-size latest` updates the shared window, Pi receives `SIGWINCH`, and the TUI redraws.

The launcher may seed the initial window from the invoking terminal's dimensions to reduce the first redraw, but this is an optimization, not a correctness requirement.

The established single-operator policy remains:

- the most recently connected or actually resized client controls a tmux window's dimensions;
- unchanged resize messages do not refresh activity or fight for ownership;
- multiple differently sized simultaneous clients cannot all receive independent native layouts;
- after a client detaches, tmux recalculates from remaining clients;
- Pi and its window survive attachment resize/detach.

## 8. Nested-server and relay environment safety

The host relay currently starts from whichever Pi first bootstraps it and may inherit `TMUX`/`TMUX_PANE`. A relay attaching a fresh PTY client to an explicitly selected tmux socket must not be rejected by tmux's nested-session guard.

Before this runtime is production-ready:

1. strip `TMUX` and `TMUX_PANE` from the long-lived host-relay daemon environment, just as Zellij client context is sanitized;
2. strip them again from each relay-spawned disposable tmux client environment;
3. continue using the route's explicit `tmux -S <socket>` for every control/attach operation;
4. never infer the managed socket from the relay daemon's inherited environment;
5. preserve mode-`0600` runtime sockets and the existing Tailnet publication boundary.

This is required for a dedicated inner tmux server to remain independent of an outer tmux or Zellij client.

## 9. User-facing command contract

Proposed initial surface:

```text
wh pi start [--name NAME] [--attach/--no-attach] -- [PI_ARGS...]
wh pi attach [selector]
wh pi sessions
```

Recommended defaults:

- `--attach` is true when stdin/stdout are interactive;
- `--no-attach` creates the managed Pi and returns after local route/orchestrator registration status is known;
- new sessions receive an internal generated UUID;
- all arguments after `--` pass unchanged to real Pi except launcher-managed identity flags;
- conflicting identity/resume flags fail clearly rather than being silently rewritten;
- exit/detach from the client does not stop Pi;
- stopping Pi remains an explicit Pi/session action, not an attachment side effect.

`wh pi` is already a Typer command namespace, so `wh pi start` is clearer than making bare `wh pi` unexpectedly launch a process. A shell alias/function may provide the shorter local UX after acceptance.

## 10. Benefits

- Uniform, already-proven tmux sizing for interactive and delegated Pi sources.
- Zellij remains the visible workspace while no Zellij source-session layout policy affects Pi.
- Outer tmux status and navigation remain intact because the backend uses another socket.
- Backend status is always hidden; grouped disposable relay clients inherit the hidden presentation.
- Same local streaming/detach behavior from Zellij, outer tmux, and bare terminals.
- Source Pi survives client, outer multiplexer, SSH, and browser disconnects.
- No meaningful latency penalty: measured incremental local tmux PTY round-trip was about `0.043 ms` median and cold server/session creation about `7.3 ms` on the development host.
- Remote clients continue using the existing direct/gateway fabric without knowing how Pi was launched.

## 11. Costs and tradeoffs

- One additional tmux server process and PTY layer per host.
- One hidden-server failure can stop all managed interactive Pi windows on that host.
- Same-host managed attachment intentionally streams instead of `switch-client`, even from an outer tmux.
- Local startup waits briefly for the bridge/host-relay route before presenting the native stream.
- Resume/continue/fork identity semantics require explicit design and tests.
- A local loopback attach path adds a small client branch, though terminal framing and relay behavior stay shared.
- The managed launcher must distinguish its real Pi executable from any shell alias/wrapper.
- Non-interactive launches consume `specs/HOST_RUNTIME_SETUP.md`: the captured Pi/tmux paths and PATH are injected into managed panes so Pi's Node shebang and the bridge's Bun relay startup do not depend on shell profile files.
- Formal client distribution remains Git/uv-tool based until a versioned release workflow exists.

## 12. Acceptance gates

1. Start a new managed Pi from Zellij; it appears in the same pane, has no inner tmux status bar, resizes continuously, detaches with `Ctrl-]`, and survives detach.
2. Start from an outer tmux with its status bar enabled; exactly the outer bar remains visible, no inner `wh-pi`/`wh_attach_*` bar appears or leaks into later Zellij attachments, and detach returns to the original shell/window without switching the outer client into the hidden server. This gate was reopened by the user's live two-bar report; the P0 diagnosis/fix is specified in `specs/PI_ATTACH_UX_NEXT.md`.
3. Start from a bare terminal with the same attach/resize/detach behavior.
4. Start at least three Pi sessions concurrently; each occupies one managed window/pane, registers under its generated UUID and requested/default name, and can be independently attached/stopped.
5. Connect from a remote Zellij and remote tmux client; initial size and changed resizes redraw Pi correctly.
6. Two differently sized clients follow documented `window-size latest` behavior; source Pi survives both detach paths.
7. A same-host managed start attaches through loopback even when the orchestrator is temporarily unavailable; registration retries and later makes the session remotely discoverable.
8. A host relay bootstrapped from inside outer tmux or managed tmux strips inherited `TMUX`/`TMUX_PANE` and can create the disposable client without a nested-session warning.
9. Existing unmanaged tmux/Zellij sessions, delegated workers, persistent multi-attach replacement, gateway fallback, and all non-live tests remain green.
10. Failure before Pi starts cleans only the new window; failure after Pi starts reports the still-running session and a manual attach command rather than silently killing it.
11. Names may collide without attaching the wrong session because the start path uses its generated UUID.
12. Names, CWDs, and Pi passthrough arguments containing whitespace, quotes, shell metacharacters, and Unicode launch exactly as argv data without command injection or truncation.
13. Explicit Pi identity/resume flags are either preserved correctly or rejected with a clear unsupported-mode error.

## 13. Rollout

1. **Implemented:** dedicated socket/session manager and pure command construction tests.
2. **Implemented and hardened through relay revision 14:** nested tmux/Zellij environment sanitization, managed-runtime route metadata, managed-server global `status off`, and fail-closed grouped-session `status off` verification landed through revision 12; revision 13 additionally reinforces `mouse on` globally and on each managed grouped attachment session; revision 14 removes application-inactivity attachment detachment.
3. **Implemented:** new-session identity/name handling, argv-safe detached window lifecycle, and explicit rejection of resume/continue/fork conflicts.
4. **Implemented:** exact local UDS route wait plus loopback native attach and `--no-attach`.
5. **Revision-12 fix live; final user confirmation pending:** the reopened two-status-bar gate now enforces global and owner `status off` on the managed server plus grouped-session verification in the relay. Live outer-tmux instrumentation showed the outer session `on` while global, `wh-pi`, and active `wh_attach_*` were all `off`; the user should repeat the original visual sequence.
6. **Pending live matrix:** run remote tmux/Zellij, persistent multi-attach, capacity replacement, and resize acceptance.
7. Keep plain `pi` documented throughout rollout.
8. Only after acceptance consider a `pi` shell alias or making the managed launcher the preferred default.

## 14. Open decisions

- Portability validation for the selected `$XDG_RUNTIME_DIR/worker-harness/pi-tmux.sock` path and `wh-pi` owner session on macOS.
- Resume/continue/fork mapping; v1 intentionally supports new sessions only and rejects conflicting flags.
- Garbage collection for dead windows/session metadata after abnormal Pi exits.
- Whether the implemented `wh pi start` loopback optimization should later be generalized to ordinary same-host `wh pi attach` calls.
