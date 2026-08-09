---
title: Pi Attach UX Next Slice
status: implemented-pending-final-user-acceptance
created: 2026-08-02
updated: 2026-08-02
owner: Worker Harness
baseline:
  worker_harness: giga-wh@1dc1adb
  dotfiles: main@44f75fb
  host_relay_revision: 11
related:
  - HIDDEN_TMUX_PI_RUNTIME.md
  - ZELLIJ_PI_CLIENT.md
  - DISTRIBUTED_PI_SESSION_FABRIC.md
---

# Pi Attach UX Next Slice

## 1. Scope

Implement the next attachment slice in four ordered commits:

1. **P0:** reproduce and permanently fix the second inner tmux status bar reported when a managed Pi is attached from an outer tmux, including the reported persistence into later Zellij attachments.
2. Replace Zellij's in-place picker/attachment overlay with a **floating picker** that opens or focuses a dedicated one-pane `π` attachment tab.
3. Group picker candidates visually by the machine on which Pi is running.
4. Add plugin-free live working/idle/error glyphs to the Zellij attachment tab name. Actual per-tab background colors remain a later WASM tab-bar milestone.

This slice must preserve the existing protocol-v2 terminal stream, direct/gateway ordering, bounded multi-attachment, resize behavior, longest-idle replacement, plain tmux/bare-terminal attachment, and same-client Zellij recursion prevention.

## 2. Decisions

The following choices are fixed for this slice:

- The Zellij picker runs in a floating pane. Zellij 0.44.2 and the current KDL support `Run { floating true }`; current configuration already uses the same property elsewhere.
- A streamed attachment owns one single-pane tab named from the selected session. The initial disconnected form is `π ? <name>`.
- Reopening the same session in the same Zellij session focuses the existing tab by stable tab ID instead of creating a duplicate.
- `Ctrl-]` ends the child attachment; `new-tab --close-on-exit` closes its one-pane tab and Zellij naturally returns to the previously focused tab.
- A same-client, plain Zellij-hosted source keeps exact focus of its original pane/tab rather than creating a redundant attachment tab. Managed hidden-tmux, delegated, remote, and cross-multiplexer sources use dedicated tabs.
- Cycle ordering follows the machine-grouped picker order so picker and next/previous navigation share one model.
- Error state is sticky through `agent-settled` and clears on the next `agent-start`.
- State titles use text glyphs first: `π ● <name>` working, `π ✓ <name>` idle, `π ! <name>` error, and `π ? <name>` disconnected. A stopped Pi exits the child and closes the tab instead of retaining a detached tab.
- Stock Zellij per-tab background colors are out of scope. They require a custom/forked WASM tab-bar renderer and follow only after the text-title workflow is accepted.

## 3. Commit 1 — P0 inner tmux status-bar regression — implemented

### 3.1 Reproduction before modification

Run the exact reported order against one managed Pi:

1. Start a managed Pi with `wh pi start --no-attach`.
2. Attach from Zellij and confirm no inner bar.
3. Attach concurrently or subsequently from inside an unrelated outer tmux with its own status enabled; confirm two rows are visible.
4. Detach the outer-tmux client.
5. Reattach from Zellij and confirm whether the inner row persists.

During every phase capture, on the dedicated managed socket only:

```text
show-options -gv status
show-options -v -t wh-pi status
list-sessions -F '#{session_name}\t#{session_group}\t#{session_attached}\t#{status}'
show-options -v -t <each-wh_attach-session> status
```

Also identify the text/session name rendered by the inner bar. Do not assume the persistence mechanism before these observations; session-group options are intended to be independent, so this report needs direct evidence.

### 3.2 Instrumentation

Add `WH_PI_RELAY_DEBUG=1`-gated diagnostics in `createTmuxAttachment()` around:

- grouped-session creation;
- per-session option writes;
- immediately before spawning `attach-session`;
- shortly after the client process starts;
- final cleanup.

Log route session ID, managed flag, source/relay session names, global status default, owner status, relay-session status, and attachment ID. Never log terminal bytes or prompts.

### 3.3 Structural fix

**Worker Harness — `src/worker_harness/pi_runtime.py::_configure_managed_server`:**

- set the dedicated server's global session default `status off`;
- retain explicit `status off` on `wh-pi`;
- retain `window-size latest` on `wh-pi`;
- unit-test all three writes.

The dedicated server is Worker Harness-owned and started with `-f /dev/null`, so a global default is correct there. It must never be applied to an ordinary user tmux server.

**Bridge route metadata — `session-bridge.ts`:**

- extend the tmux locator/register payload with `managed_runtime: process.env.WH_MANAGED_PI === "1"`;
- preserve backward compatibility when the field is absent.

**Host relay — `host-relay.ts`:**

- add the optional managed flag to `TmuxRoute` and registration/describe diagnostics;
- retain per-`wh_attach_*` `status off` and `window-size latest` for every tmux route;
- for managed routes only, reinforce the dedicated server's global `status off` before creating/attaching the grouped session;
- verify the effective relay-session status is `off` immediately before `attach-session`; if it cannot be forced off, fail and clean the disposable attachment rather than exposing backend chrome;
- do not alter global or source-session options for unmanaged tmux routes;
- bump host relay revision 11 to 12.

If instrumentation reveals a different concrete mutation, fix that root cause as well; keep the managed-server global default as defense in depth.

### 3.4 P0 acceptance

- Outer tmux shows exactly its own one status bar.
- Zellij shows no tmux status bar before, during, or after an outer-tmux attachment.
- `wh-pi`, every `wh_attach_*`, and the managed server global default report `off` throughout.
- An unmanaged tmux source retains its own status configuration.
- Source Pi survives both attachments and detach paths.

## 4. Commit 2 — floating picker and dedicated/reused Zellij tab — implemented

### 4.1 Outer/inner attachment split

Inside Zellij, split `wh pi attach` into:

- an **outer launcher**, normally run in the floating picker pane, which selects a session and decides whether to focus or create a destination tab;
- a hidden **`--here` child**, which runs `_run_attach_loop()` in the destination tab's pane and never recursively launches another tab.

Outside Zellij, `wh pi attach` remains in the invoking terminal as today.

### 4.2 Decision order after selection

1. Ask `focus_local_zellij_session(session_id)` whether this is a same-client, plain Zellij source. If yes, focus its original pane/tab and close the floating picker.
2. Search for a live attachment marker for this session in the current Zellij session. Cross-check its pane and tab with `zellij action list-panes --json --all`. If live, call `zellij action go-to-tab-by-id <tab_id>` and close the picker.
3. Otherwise create:

```bash
zellij action new-tab \
  --name "π ? <session-name>" \
  --cwd <session-or-picker-cwd> \
  --close-on-exit \
  -- wh pi attach <session-id> --here
```

The command returns the stable tab ID on stdout; capture and validate it.

### 4.3 Tab identity and markers

Extend the existing mode-`0600` attachment marker design rather than using tab names as identity:

```json
{
  "session_id": "...",
  "zellij_session_name": "Pi",
  "tab_id": 4,
  "pane_id": "terminal_12",
  "pid": 1234,
  "mode": "stream"
}
```

- Write atomically after the `--here` child resolves its pane/tab.
- Store a by-session index under the private runtime directory for fast lookup.
- Treat the marker as a hint; `list-panes --json --all` plus a live PID is authoritative.
- Remove only the exact marker owned by the exiting PID so stale cleanup cannot delete a replacement attachment's marker.
- Use a per-session file lock during find-or-create to prevent duplicate tabs from concurrent picker invocations.

Duplicate user tab names are harmless because focus uses marker + tab ID, not the title.

### 4.4 `wh pi start` in Zellij

After creating the managed Pi and waiting for its exact local route:

- create/focus the same dedicated tab instead of streaming over the current pane;
- invoke the child with hidden `--here --loopback` flags;
- `--loopback` must validate that the exact session has a live local UDS route and construct `ws://127.0.0.1:27890/...`, preserving initial attach without orchestrator/Tailnet/gateway;
- keep `--no-attach` unchanged;
- keep tmux and bare-terminal start behavior unchanged.

### 4.5 KDL

Update both direct picker bindings (`Alt-a` and `Ctrl-a Ctrl-a`) to:

```kdl
Run "wh" "pi" "attach" {
    floating true
    close_on_exit true
}
```

Optionally set an 80% width and 70% height after a live visual check. Keep short cycle helpers (`Alt-y/u`, prefix Ctrl-h/j/k/l) in-place for now; they do not own the long-running stream.

### 4.6 Commit 2 tests

- `--here` runs in place and never calls `new-tab`.
- A missing marker creates one correctly named tab with safe argv handling.
- A live marker focuses its stable tab ID and does not create a duplicate.
- Stale PID/pane/tab markers are removed and replaced.
- Same-client plain Zellij source exact-focuses the original pane without creating a tab.
- Detach removes only the child-owned marker.
- Concurrent open attempts produce one tab.
- `wh pi start` in Zellij uses `--here --loopback` and performs no initial orchestrator attach lookup.
- Tmux/bare attachment behavior is unchanged.

## 5. Commit 3 — machine-grouped picker — implemented

### 5.1 Machine identity

Build one session inventory from `/api/v1/pi/sessions` plus a best-effort `/api/v1/workers` fetch:

- interactive session: `host`, with `terminal_host` used to resolve its Tailnet identity;
- delegated session: mapped worker name by `worker_id`, falling back to worker ID, with `worker_ip` used for its Tailnet identity;
- global router: dedicated `Global router` group;
- missing location: `Unknown machine`.

Best-effort `tailscale status --json` supplies the current `MagicDNSSuffix` plus IP/hostname→DNS maps. Strip the suffix and show the short identity next to the machine heading (for example `KW60898  @camel` for an interactive host and `Delegated · KW60898  @kw60898` for its separately tagged worker). When an unattached interactive bridge has no `terminal_host`, resolve by its reported OS hostname; if an ordinary Tailnet node and tagged worker share that hostname, prefer the untagged node for interactive sessions. Cache this lookup once per CLI process; absence, timeout, or malformed output must not block the picker.

Failure to fetch workers must not block the picker; use IDs as fallback. Compare interactive hosts case-insensitively with `socket.gethostname()`; order the global router first, the local machine second, remote interactive machines next, and delegated workers last.

### 5.2 Ordering

Sort candidates by:

1. global router first;
2. local-machine group second;
3. remote interactive machine labels alphabetically, case-insensitive;
4. delegated worker groups at the bottom, alphabetically by worker name/ID;
5. within a group: working before idle, then most recently updated;
6. deterministic final session-ID tie-break.

Use this same ordered inventory for next/previous cycling. In fzf, automatically place the initial cursor on the first local session; pressing Up selects the global entry immediately above it. If there is no local session, keep the first available row selected.

### 5.3 One-stage fzf presentation

Keep every session selectable and emulate a tree without introducing selectable heading rows. Use NUL-delimited multiline fzf records:

```text
id<TAB>display<NUL>
```

Example:

```text
KW60898  @camel
  ├─ ●   I   KW60898 @camel              DRRT               /home/engeld/Dev/DRRT
  └─ ✓   I   KW60898 @camel              TomoFoam           /home/engeld/Dev/radfoam
```

- Attach the machine heading to the first selectable session record in each group; it is visual context, not an independent row, so Up/Down/Enter never land on a non-session heading.
- Prefix children with `├─`/`└─`. Group ordering remains Global, Local, remote interactive, delegated.
- Status reuses the tab glyphs: `●` working, `✓` idle, `!` failed, `?` disconnected.
- Type is one unambiguous letter: `I` interactive, `D` delegated, `G` global router.
- The visible name is capped at 16 cells and the full path remains on the right.
- Repeat a fixed-width, dimmed `machine @tailnet` context column on every child, before the name, so the full path remains the rightmost column. If filtering removes the first child/heading, the surviving result still identifies its machine and Tailnet identity; searching either machine or Tailnet label matches every child in the group.
- Use `--ansi --read0 --print0 --with-nth=2 --nth=1`: fzf strips the dimming escape codes for matching and applies `--nth` after the `--with-nth` transformation, so referring to original hidden field indexes would make every query return zero matches. Leave fuzzy ranking enabled (do not use `--no-sort`) so a typed query selects the highest-scoring match; an empty query retains inventory order and the Local initial cursor.
- Preserve full session ID as the hidden selection key and disable horizontal scroll so the aligned leading columns remain stable.

Search covers the complete visible multiline record, including heading/context, displayed name, full path, and Tailnet label. Do not add selectable separator/header rows.

### 5.4 Commit 3 tests

- Interactive host, delegated worker-name mapping, worker-ID fallback, global router, and unknown group.
- Local host first, alphabetical remote groups, state/recent/ID order within groups.
- Multiline machine heading on the first child, correct `├─`/`└─` branches, and compact machine/Tailnet context on every child.
- MagicDNS suffix parsing and best-effort IP→short-label mapping for interactive and delegated identities.
- Installed-fzf filtering by lowercase/uppercase name, full path, machine, and Tailnet label still maps the complete NUL-delimited record to the correct session.
- Next/previous cycles across the exact grouped order and wraps.

## 6. Commit 4 — live plugin-free state glyphs — implemented

### 6.1 Initial state

Create the tab with the selected projection state:

- `working` → `π ● <name>`;
- `idle` → `π ✓ <name>`;
- disconnected/unknown → `π ? <name>`.

### 6.2 SSE watcher

Only the Zellij `--here` child starts a state watcher. Tail:

```text
GET /api/v1/pi/sessions/<id>/stream
```

Honor event sequence/`Last-Event-ID`, reconnect with bounded exponential backoff, and never allow watcher failure to terminate the terminal attachment. The loopback start path may attach before the orchestrator is reachable; retain neutral/current title and retry in the background.

Event mapping:

- `agent-start` → working and clear prior error;
- `agent-settled` → idle unless the current turn has recorded a session-level error;
- failed `tool-end` events remain working because tool failures are recoverable within an active agent turn;
- `message-end` carrying `isError`/`errorMessage` or `control-error` → sticky error;
- next `agent-start` clears sticky error;
- stopped source closes the child/tab through normal terminal teardown rather than rendering a permanent stopped tab.

Debounce title writes for 250–400 ms and rename only on actual state/name changes:

```bash
zellij action rename-tab --tab-id <id> "π <glyph> <name>"
```

### 6.3 Cycling

When an in-tab attachment cycles to another Pi:

- atomically move the marker from the old session to the new one;
- cancel/restart the SSE watcher from the new session's current event sequence;
- rename the same tab to the new session's name/state;
- preserve one attachment slot and existing SIGUSR/control-byte behavior.

### 6.4 Commit 4 tests

- Event parser handles replay, comments/keep-alive, fragmented lines, and reconnect cursor.
- Working → idle title transition.
- Failed tool calls remain working; message/control errors become sticky through settle.
- Next agent-start clears error.
- Debounce avoids repeated rename calls.
- Watcher loss does not interrupt terminal bytes.
- Cycling swaps marker/watcher/title without duplicate tabs or attachments.

## 7. Live acceptance matrix

| # | Scenario | Expected |
|---|---|---|
| 1 | Managed Pi attached from outer tmux | Exactly one outer bar; no inner bar |
| 2 | Zellij attach after #1 | No leaked inner bar |
| 3 | Unmanaged tmux source | User server/global status remains untouched |
| 4 | `Alt-a` / `Ctrl-a Ctrl-a` | Picker floats; source pane remains visible |
| 5 | Pick managed/remote/delegated Pi | New single-pane `π` tab; terminal resizes correctly |
| 6 | Reopen an already attached session | Existing tab focused by ID; no duplicate |
| 7 | Pick same-client plain Zellij source | Original pane/tab focused directly |
| 8 | `Ctrl-]` in attachment tab | Tab closes and prior tab regains focus; Pi survives |
| 9 | `wh pi start` inside Zellij | Dedicated tab uses exact local loopback initial attach |
| 10 | Two differently sized Zellij attachment tabs | Latest resize behavior remains live and source survives |
| 11 | Picker with global, local, at least two remote machines, and a delegated worker | Global first, initial cursor on first Local with Up→Global, remote below, delegated bottom, searchable by machine |
| 12 | Next/previous cycling | Follows displayed machine-group order and wraps |
| 13 | Agent working/settled/error | Tab glyph updates live with sticky-error semantics |
| 14 | Orchestrator unavailable after local start | Terminal remains usable; glyph watcher retries harmlessly |
| 15 | Cap/idle/replacement on a tab attachment | Existing protocol-v2 selector behavior remains correct |

## 8. Migration and compatibility

- Relay revision 12 adds optional managed-route metadata; absent means unmanaged and preserves revision-11 behavior.
- New CLI with old KDL still creates/focuses dedicated tabs, but the picker remains in-place until KDL reload.
- New KDL with old CLI only changes the picker to floating; attachment otherwise remains current behavior.
- New runtime markers are additive, private, atomically written, and self-cleaning.
- `--here` and `--loopback` are hidden implementation flags.
- `--stream` remains a compatibility no-op until a separate cleanup release.
- Plain Zellij-hosted Pi, tmux, bare terminal, PWA, delegated worker runtime, terminal protocol, and orchestrator schema remain compatible.
- Reload/restart order for rollout: install/update `wh`; update KDL; restart host relay to revision 12; `/reload` existing managed Pi bridges so they advertise `managed_runtime`; reload/restart Zellij configuration. New managed Pi sessions advertise the field immediately.

## 9. Tmux persistent-window extension — implemented

- `Ctrl-a Ctrl-a` uses `display-popup` only for fzf selection. The popup passes
  exact invoking tmux session/client IDs to hidden `--tmux-picker` mode.
- A process-shared private lock serializes find/create by tmux socket, target
  session, and Pi UUID. Reuse requires an exact UUID pane marker, WH ownership,
  a live pane PID, and a non-dead pane.
- New streams run in one real `--tmux-child` window. Only `switch-client -c`
  focuses the invoking client; session-wide selection is forbidden.
- SSE state projection reuses `watch_session_state()`. Titles preserve the exact
  glyph contract and per-window status colors are working `#89b4fa`, idle
  `#a6e3a1`, error `#f38ba8`, disconnected `#6c7086`.
- Catppuccin's generated formats remain the fallback for non-WH windows. Only
  windows carrying `@wh_pi_owned=1` use the state-colored format.
- Dedicated Zellij/tmux attachments retry four bounded unexpected transport
  failures after the initial attempt. Intentional Ctrl-] detach never retries.
  Final diagnostics include transport, duration, fallback use, close code, and
  close reason.

## 10. Out of scope

- Per-tab background colors or replacing Zellij's built-in tab bar.
- A WASM plugin.
- Changes to protocol-v2 terminal framing.
- New application authentication/RBAC.
- Resume/continue/fork support in `wh pi start`.
- Replacing fzf with a custom full-screen TUI.
- Changing shared tmux `window-size latest` multi-client semantics.
