---
title: Worker Harness interactive-host runtime setup
status: implemented-pending-live-fleet-rollout
updated: 2026-08-05
---

# Interactive-host runtime setup

## 1. Problem

`uv tool install` installs the Python `wh` console script, but it cannot and
must not execute project-specific post-install hooks. Interactive shells also
commonly construct PATH in zsh/fish/NVM/Bun startup files that POSIX
`sh -lc` does not read. A remote `wh launch` can consequently reach the host
while failing to find `wh`, Pi, Node, or Bun. Pi itself may be an executable
script with `#!/usr/bin/env node`, and its bridge later needs Bun to start the
host relay.

The managed runtime must not depend on whichever shell happens to invoke it.

## 2. Operator contract

Run setup explicitly from a prepared interactive shell on every interactive Pi
source host:

```bash
uv tool install --force --reinstall <pinned-worker-harness-source>
wh host setup
wh host doctor
```

`setup` never changes `.profile`, `.zshrc`, `.bashrc`, or fish configuration.
It captures the shell environment only when explicitly invoked. Rerun it after
moving/upgrading any captured runtime or changing the desired PATH.

## 3. Manifest

Default path:

```text
${XDG_CONFIG_HOME:-$HOME/.config}/worker-harness/host-runtime.json
```

`WH_HOST_RUNTIME_CONFIG` overrides the path for tests or deliberate alternate
installations. Schema version 1 stores:

- UTC generation time;
- an ordered, deduplicated list of absolute existing PATH directories;
- required lexical absolute executable paths for `wh`, `pi`, `bun`, `node`,
  `tmux`, and `tailscale`;
- optional `zellij`, represented as an absolute path or JSON `null`.

Lexical symlink paths are retained so stable user-facing links can move to a
new version. Loading and doctor validation follow the current link and require
a regular executable target.

The parent directory is mode `0700`; the manifest is atomically replaced at
mode `0600` using a same-directory temporary file, file fsync, rename, and a
best-effort directory fsync.

## 4. Capture and validation

Capture resolves tools with `shutil.which()` in the invoking shell. Required
missing tools fail setup before writing. PATH is built from captured executable
directories first, followed by absolute existing directories from the invoking
PATH, with duplicates removed. Temporary directories are excluded unless a
captured executable actually resides there. Existing `/usr/local/bin`,
`/usr/bin`, and `/bin` are retained.

Before writing, setup probes every required executable with a five-second
budget under a clean SSH-like environment containing only normal identity,
locale, terminal variables, and the captured PATH. The Pi probe verifies that
an `env node` shebang works. Optional Zellij absence or probe failure is a
warning; required failures prevent the write.

`wh host doctor` reloads the manifest, checks file/directory modes, checks for
missing PATH directories, repeats the clean-environment probes, and exits
nonzero on any required failure.

## 5. Runtime consumption

`wh pi start` uses the manifest when present:

1. `WH_PI_EXECUTABLE` remains the highest-priority explicit Pi override.
2. Otherwise use the manifest Pi path; without a manifest, retain PATH lookup
   for backward compatibility.
3. Use the manifest tmux path.
4. Pass the manifest PATH to tmux subprocesses and explicitly into the managed
   Pi pane. This supports existing managed servers whose global tmux
environment predates setup.
5. Continue stripping outer `TMUX`, `TMUX_PANE`, and Zellij variables.

Pi inherits the captured PATH, so its Node shebang resolves and the bridge's Bun
host-relay spawn resolves without a separate dotfiles configuration change.

## 6. Remote launch

Remote launch continues to use ordinary SSH and strict `shlex` quoting. The
target script resolves `wh` from PATH first, then falls back to the standard uv
tool link `$HOME/.local/bin/wh`. If neither is executable it exits 127 with an
actionable installation/setup message. It does not modify the target or run
setup automatically.

## 7. Security and boundaries

- Manifest values are never interpolated as unquoted operator shell text.
- Setup is explicit; package installation cannot silently snapshot or mutate a
  user's environment.
- The manifest is user-private configuration, not a credential and not shared
  between users.
- A stale manifest fails closed with `wh host doctor`/runtime diagnostics.
- This feature prepares ordinary interactive hosts only. Delegated worker SIF
  runtime distribution remains separate.
