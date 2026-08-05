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
5. The transport is ordinary `ssh` over the Tailnet/MagicDNS. No target-side helper or agent is introduced.
6. SSH destination defaults to the short MagicDNS label. OpenSSH configuration supplies per-host user/key policy; `--ssh-user` can override it. For matched worker records, their advertised `ssh_user` is the fallback when no explicit override is supplied.
7. Working-directory choices are:
   - target `$HOME`;
   - each immediate child directory of target `~/Dev`, when it exists;
   - `Manual path…`, whose prompt defaults to target `$HOME`.
8. The default human name is the selected directory basename. The launcher supplies it explicitly to `wh pi start`; generated session UUID remains authoritative and duplicate names are allowed.
9. The launcher executes target-side existing `wh --output json pi start --no-attach --name … -- [PI_ARGS…]` after `cd -- CWD`. Remote shell values are quoted with `shlex.quote`; the local target bypasses SSH and executes argv directly.
10. The target must already have SSH, `wh`, Pi, its bridge extension, tmux/Bun, orchestrator configuration, and host-relay/Tailscale Serve prerequisites. `wh host setup` and `wh host doctor` now capture and validate the target's non-interactive runtime per `specs/HOST_RUNTIME_SETUP.md`; launch still performs no automatic bootstrap and surfaces target failures through bounded stdout/stderr.
11. After target `wh pi start` returns its generated UUID, the operator polls the existing orchestrator registry until that exact session is active and attachable, then reuses the existing direct-first/gateway-fallback and dedicated/reused Zellij attachment flow.

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

The command does not recursively scan home, execute project files, or require a remote Worker Harness helper. The local fzf adds the synthetic `Manual path…` option.

## Launch and registration

1. Select/resolve machine.
2. Resolve SSH destination/user.
3. Query target directory choices unless `--cwd` is supplied.
4. Prompt for manual path if chosen.
5. Default name to `PurePath(cwd).name`, with a safe `Pi` fallback.
6. Run local or SSH launch command and parse its JSON `{session_id, name, …}`.
7. Poll `GET /api/v1/pi/sessions/{session_id}` for the exact UUID until active/attachable or timeout. Poll no faster than once per second and honor `429 Retry-After` without replacing a more useful prior registration state in the final diagnostic.
8. With `--no-attach`, print the launch result including machine and cwd.
9. Otherwise reuse the existing Pi attachment path; in immediate Zellij create/focus the dedicated state-named tab.

## Security and failure semantics

- Tailnet membership and SSH authorization remain the trust boundary.
- No orchestrator endpoint gains arbitrary process execution.
- Never disable SSH host-key checking.
- Never interpolate unquoted machine/user/path/name/Pi arguments into the remote shell command.
- Machine selectors may not start with `-`; pass `--` before the SSH destination where supported by the local OpenSSH CLI.
- A nonzero SSH/target command exit is failure. Include bounded stdout/stderr in the error.
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
- `--no-attach` output and immediate-Zellij/terminal attachment reuse.
- Existing non-live suite and installed-fzf tests remain green.
