---
status: approved
branch: main
date: 2026-08-03
---

# Remote managed Pi launch

## Goal

Add a top-level `wh launch` command that starts a managed interactive Pi on a selected Tailnet machine even when that machine currently has no registered Pi sessions.

This is an explicit operator action. It does not add an orchestrator remote-execution endpoint, host launcher daemon, application credentials, or a new terminal transport.

## Fixed decisions

1. Machine discovery uses local `tailscale status --json`, independently of the Pi session registry.
2. The picker has two ordered sections:
   - standard untagged machines;
   - machines tagged `tag:wh-worker`.
   Other tagged service identities are not launch targets in v1.
3. The local Tailnet node sorts first in the standard section. Online nodes sort before offline nodes, then by short MagicDNS label/hostname.
4. Both sections use the same SSH launch mechanism. Worker-tagged selection does not silently invoke delegation; if its SSH environment lacks the interactive `wh`/Pi/bridge prerequisites, launch fails normally.
5. The transport is ordinary `ssh` over the Tailnet/MagicDNS. No host daemon or orchestrator execution endpoint is introduced. The installed `wh` CLI provides bounded target-local history list/resume subcommands.
6. SSH destination defaults to the short MagicDNS label. OpenSSH configuration supplies per-host user/key policy; `--ssh-user` can override it. For matched worker records, their advertised `ssh_user` is the fallback when no explicit override is supplied.
7. Working-directory choices are:
   - target `$HOME`;
   - each immediate child directory of target `~/Dev`, when it exists;
   - `Manual path…`, whose prompt defaults to target `$HOME`.
8. After machine/cwd selection, interactive launch groups actions as Running sessions, Previous sessions, and Start new. Explicit/scripted name or Pi arguments retain deterministic new-session behavior.
9. Running sessions come from the orchestrator and attach directly. Previous histories come from target-local Pi `SessionManager.list(cwd)`, require package version `>=0.83.0,<1.0.0`, exclude every currently active ID, and preserve stored names.
10. Resume sends only an opaque ID to the target. The target re-runs `SessionManager.list(cwd)`, requires one exact match under `~/.pi/agent/sessions`, rejects active IDs, and invokes Pi with exact `--session`; it never accepts a client-supplied history path.
11. Start new defaults the human name to the directory basename. The launcher executes target-side `wh --output json pi start --no-attach --name … -- [PI_ARGS…]`; generated UUID remains authoritative and duplicate names are allowed.
12. Remote shell values are quoted with `shlex.quote`/`shlex.join`; the local target bypasses SSH and executes argv directly. SSH errors name the destination, phase, and duration with bounded output.
13. The target must already have SSH, `wh`, Pi, its bridge extension, tmux/Bun, orchestrator configuration, and host-relay/Tailscale Serve prerequisites. `wh host setup` and `wh host doctor` capture and validate the target's non-interactive runtime; launch performs no automatic bootstrap.
14. After start/resume returns the exact UUID, the operator polls the existing orchestrator registry until active/attachable, then reuses direct-first/gateway-fallback and dedicated/reused attachment flow.

## CLI

Interactive:

```text
wh launch
```

Scriptable:

```text
wh launch --machine camel --cwd /home/engeld/Dev/DRRT
wh launch --machine mac --cwd /Users/engeld/Dev/project --name experiment
wh launch --machine kw60898 --ssh-user engeld --no-attach -- --offline
```

Options:

- `--machine/-m`: exact short/full MagicDNS name, hostname, or Tailnet IP; picker when omitted.
- `--cwd`: target path; directory picker when omitted.
- `--name/-n`: human name; basename default.
- `--ssh-user`: OpenSSH user override.
- `--attach/--no-attach`: attach after registration, default attach.
- `--timeout`: SSH/start and orchestrator-registration timeout.
- remaining arguments after `--` pass to Pi, subject to `wh pi start`'s existing identity/resume conflict rejection.

## Machine picker

Use NUL-delimited multiline fzf records so section headings are attached to their first selectable machine, never independently selectable. Show online/offline state, type (`I` standard, `W` worker), hostname, short MagicDNS label, OS, and IP. Search covers every visible field.

Offline machines remain visible for inventory completeness; selecting one is allowed and fails through the normal SSH path.

## Directory query

The launcher runs a fixed POSIX shell command locally or through SSH. It emits NUL-separated absolute paths:

1. `$HOME`;
2. sorted immediate children of `$HOME/Dev` when that directory exists.

The directory command does not recursively scan home or execute project files. The local fzf adds the synthetic `Manual path…` option. History lookup is a separate installed Worker Harness helper and runs only after cwd selection.

## Launch and registration

1. Select/resolve machine.
2. Resolve SSH destination/user.
3. Query target directory choices unless `--cwd` is supplied.
4. Prompt for manual path if chosen.
5. Load active orchestrator sessions and bounded target-local histories for that exact cwd.
6. Choose attach running, resume previous, or start new.
7. For resume, re-resolve the opaque ID target-side and preserve its stored name. For new, default name to `PurePath(cwd).name` with a safe `Pi` fallback.
8. Run local or SSH target command and parse its JSON `{session_id, name, …}`.
9. Poll `GET /api/v1/pi/sessions/{session_id}` for the exact UUID until active/attachable or timeout. Poll no faster than once per second and honor `429 Retry-After`.
10. With `--no-attach`, print action, machine, cwd, and exact session ID. Otherwise reuse the existing dedicated attachment path.

## Security and failure semantics

- Tailnet membership and SSH authorization remain the trust boundary.
- No orchestrator endpoint gains arbitrary process execution.
- Never disable SSH host-key checking.
- Never interpolate unquoted machine/user/path/name/Pi arguments into the remote shell command.
- Machine selectors may not start with `-`; pass `--` before the SSH destination where supported by the local OpenSSH CLI.
- A nonzero SSH/target command exit is failure. Include bounded stdout/stderr plus SSH destination, phase, and duration in the error.
- Session history paths never cross the SSH boundary. Exact IDs are re-resolved under the selected cwd and active IDs fail closed.
- Malformed or missing JSON is failure.
- Registration timeout does not claim the target Pi stopped; report its UUID, state that launch succeeded remotely, and direct the operator to the target bridge orchestrator URL/connectivity before attaching by session ID.

## Tests

- Tailnet parser: self/peers, dynamic MagicDNS suffix, untagged/worker grouping, other-tag exclusion, local/online/alphabetic ordering.
- Machine resolver: alias/full DNS/hostname/IP, ambiguity, option-like selector rejection.
- fzf machine picker: headings are not separate rows, first local cursor, NUL framing, worker section below standard.
- Directory command: HOME first, immediate `~/Dev` children only, NUL parsing, manual default HOME.
- SSH command: exact shell quoting for spaces, quotes, semicolons, newlines, and Pi args; explicit/fallback/default SSH user.
- Local launch bypasses SSH and uses target cwd.
- Remote nonzero exit and malformed JSON surface bounded diagnostics.
- Registration polling uses exact UUID and does not infer readiness from terminal output.
- Target history helper checks Pi version/API, cwd/path containment, bounded output, exact-ID re-resolution, and active-ID refusal.
- Action picker groups active/history/new, filters active histories, preserves names, and quotes resume commands.
- `--no-attach` output and immediate-Zellij/terminal attachment reuse.
- Existing non-live suite and installed-fzf tests remain green.
