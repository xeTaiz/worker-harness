---
title: Hidden tmux Pi Runtime and wh pi start
status: proposed
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
5. **No visible backend chrome.** Force `status off` and `window-size latest` on the owner session and on relay-created grouped sessions. Existing probes confirm grouped sessions inherit both values, but the relay should enforce them defensively.
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

The outer tmux and hidden tmux are truly nested only in the terminal data path. They are different servers/sockets. Because the route socket differs from the outer client's `TMUX` socket, existing same-server `switch-client` optimization does not fire; the managed Pi is streamed into the current outer pane. This preserves the outer status bar and makes outer tmux, Zellij, and a bare terminal follow the same attachment UX.

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

1. Resolve the real Pi executable before any optional `pi -> wh pi start` alias to avoid recursion.
2. Determine the managed socket and create its parent directory mode `0700`.
3. Ensure the dedicated tmux server and owner session exist.
4. Force owner options:

   ```tmux
   set-option -t wh-pi status off
   set-option -t wh-pi window-size latest
   ```

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

This is not a primary operator path. If a client is already attached to the same managed tmux socket, exact `switch-client` remains legal, but no UX depends on it.

### 6.3 Existing unmanaged local sessions

`wh pi attach` keeps its current behavior:

- same tmux server -> exact `switch-client`;
- same Zellij host/session -> exact client-local focus;
- cross-host or cross-multiplexer -> native stream.

The managed launcher does not remove compatibility with existing plain Pi sessions.

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
- Formal client distribution remains Git/uv-tool based until a versioned release workflow exists.

## 12. Acceptance gates

1. Start a new managed Pi from Zellij; it appears in the same pane, has no inner tmux status bar, resizes continuously, detaches with `Ctrl-]`, and survives detach.
2. Start from an outer tmux with its status bar enabled; the outer status remains visible while the managed Pi stream runs in the pane, and detach returns to the original shell/window without switching the outer client into the hidden server.
3. Start from a bare terminal with the same attach/resize/detach behavior.
4. Start at least three Pi sessions concurrently; each occupies one managed window/pane, registers under its generated UUID and requested/default name, and can be independently attached/stopped.
5. Connect from a remote Zellij and remote tmux client; initial size and changed resizes redraw Pi correctly.
6. Two differently sized clients follow documented `window-size latest` behavior; source Pi survives both detach paths.
7. A same-host managed start attaches through loopback even when the orchestrator is temporarily unavailable; registration retries and later makes the session remotely discoverable.
8. A host relay bootstrapped from inside outer tmux or managed tmux strips inherited `TMUX`/`TMUX_PANE` and can create the disposable client without a nested-session warning.
9. Existing unmanaged tmux/Zellij sessions, delegated workers, multi-attach replacement, idle cleanup, gateway fallback, and all non-live tests remain green.
10. Failure before Pi starts cleans only the new window; failure after Pi starts reports the still-running session and a manual attach command rather than silently killing it.
11. Names may collide without attaching the wrong session because the start path uses its generated UUID.
12. Names, CWDs, and Pi passthrough arguments containing whitespace, quotes, shell metacharacters, and Unicode launch exactly as argv data without command injection or truncation.
13. Explicit Pi identity/resume flags are either preserved correctly or rejected with a clear unsupported-mode error.

## 13. Rollout

1. Implement the dedicated socket/session manager and pure command construction tests.
2. Harden relay environment sanitization for nested tmux.
3. Implement new-session identity/name handling and detached window lifecycle.
4. Implement exact local UDS route wait plus loopback native attach.
5. Validate Zellij, outer tmux, and bare-terminal start paths in isolation.
6. Run remote tmux/Zellij, multi-attach, idle, replacement, and resize acceptance.
7. Keep plain `pi` documented throughout rollout.
8. Only after acceptance consider a `pi` shell alias or making the managed launcher the preferred default.

## 14. Open decisions

- Exact managed socket path and owner session naming across Linux/macOS runtime-directory conventions.
- Real Pi executable resolution and recursion-proof alias behavior.
- Resume/continue/fork mapping and whether v1 intentionally supports new sessions only.
- Whether `--no-attach` waits for local route only or also orchestrator registration.
- Garbage collection for dead windows/session metadata after abnormal Pi exits.
- Whether local loopback attach is implemented inside `wh pi start` only or generalized to all same-host cross-multiplexer `wh pi attach` calls.
