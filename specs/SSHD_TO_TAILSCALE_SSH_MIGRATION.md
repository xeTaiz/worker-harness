# Migrate worker control from OpenSSH sshd to Tailscale SSH

## Goal

Replace the worker container's OpenSSH server, baked authorized keys, orchestrator private key, and SSH port configuration with Tailscale SSH only.

This is intentionally a breaking migration. We do **not** keep the old sshd/key-based path.

Primary goals:

- make Singularity/Apptainer the simplest supported HPC runtime;
- remove runtime writes to root filesystem paths such as `/etc/ssh`, `/run/sshd`, `/ssh`, `/var/lib/tailscale`, and `/var/run/tailscale`;
- remove build-time SSH key exchange;
- remove SSH port configurability; Tailscale SSH always uses Tailnet port `22`;
- standardize worker Tailscale runtime to userspace networking only (no kernel/TUN mode switch);
- keep existing worker-harness job and tunnel semantics using normal `ssh`, `scp`, `tmux`, and `ssh -N -L` commands over Tailscale SSH.

## Simplified target architecture

### Worker

The worker:

1. starts `tailscaled` with writable user-controlled Tailscale directories;
2. runs `tailscale up --ssh`;
3. determines its runtime login user;
4. registers with the orchestrator using:
   - `worker_ip`
   - `ssh_user`
5. runs `worker_daemon.py`.

The worker does **not**:

- install or start OpenSSH `sshd`;
- generate or consume `/ssh/authorized_keys`;
- mutate `/etc/ssh/sshd_config`;
- create `/run/sshd`;
- run `tailscale serve` for SSH;
- expose or configure `WORKER_SSH_PORT`, `TS_SERVE_SSH_PORT`, `WH_SSH_MODE`, or similar compatibility knobs.

### Orchestrator

The orchestrator:

1. stores `ssh_user` per worker;
2. connects as `<worker.ssh_user>@<worker.worker_ip>`;
3. uses Tailscale SSH authentication;
4. does not pass `-i <key>`;
5. does not read `SSH_KEY_PATH`;
6. assumes SSH destination port `22`.

### Policy

The Tailnet/Headscale policy must include both:

1. normal ACL reachability, including worker heartbeat access to the orchestrator;
2. a Tailscale SSH policy allowing the orchestrator tag to SSH to worker-tagged nodes as the runtime worker user.

Worker-harness tunnels rely on SSH local forwarding (`ssh -N -L`). With Headscale `action: "accept"` SSH rules, Headscale emits Tailscale SSH actions with local/remote forwarding enabled.

## Non-goals

- No compatibility with the old sshd/key-based worker control path.
- No `WORKER_SSH_PORT` support.
- No port `2222` documentation.
- No `WH_SSH_MODE` or `SSH_AUTH_MODE`.
- No orchestrator SSH private key flow.
- No user override env var for SSH user.
- No `TS_USERSPACE` toggle (always userspace mode).
- No `/var/lib/tailscale` or `/var/run/tailscale` bind-mount requirement.

## Worker directory defaults

Expose one writable worker-harness directory:

```bash
WH_DIR="${WH_DIR:-$HOME/.local/worker-harness}"
TS_STATE_DIR="$WH_DIR/tailscale/state"
TS_SOCKET_DIR="$WH_DIR/tailscale/run"
TS_SOCKET="$TS_SOCKET_DIR/tailscaled.sock"
WORKER_DAEMON_DIR="$WH_DIR/worker-daemon"
HARNESS_DIR="$WH_DIR/harness"
```

Notes:

- `WH_DIR` is the only user-facing writable directory knob.
- Tailscale state, Tailscale runtime socket, worker daemon ID, and job/log harness data live under this fixed structure.
- `TS_SOCKET` is derived internally from `WH_DIR`; users should not need to set it.
- All Tailscale CLI calls must use `--socket="$TS_SOCKET"`.
- `tailscaled` must use `--state="$WH_DIR/tailscale/state/tailscaled.state" --socket="$TS_SOCKET"`.

## Runtime SSH user

The worker determines its SSH user at runtime and sends it in `/register`.

Implementation rule:

```python
ssh_user = getpass.getuser() or "root"
```

Do not add an env var override.

Rationale:

- Docker/rootful workers usually run as `root`.
- Singularity/Apptainer workers usually run as the invoking host user.
- Tailscale SSH runs commands as local OS users; the runtime process knows the right user better than the orchestrator.
- If the detected user is not allowed by policy or cannot execute jobs, that is a deployment/configuration error, not something worker-harness should hide with extra fallback modes.

## Implementation plan

### Phase 1 — Simplify worker models and registration

Files:

- `src/worker_harness/models.py`
- `src/worker_harness/db.py`
- `worker_container/worker_daemon.py`
- worker list/show/agent summary UI files
- tests

Steps:

1. Add `ssh_user: str = "root"` to `WorkerRegistration`.
2. Add `ssh_user: str = "root"` to `Worker` and worker summary/output models.
3. Remove `ssh_port` from the registration payload and public worker model where practical.
   - If a DB transition is easier with `ssh_port` left in storage temporarily, stop exposing/configuring it and treat it as obsolete.
   - New SSH command construction must not depend on registered port values.
4. Update `Worker.from_registration()` and `Worker.update_from_registration()` to persist `ssh_user`.
5. Update database schema:
   - add `ssh_user TEXT NOT NULL DEFAULT 'root'`;
   - remove/rebuild obsolete `ssh_port` column if convenient;
   - because this is a breaking migration, it is acceptable to require DB reset or perform a simple destructive schema migration if needed.
6. Update worker CLI/TUI/API displays from `ip:port` to `ssh_user@worker_ip`.
7. Update `worker_daemon.py`:
   - remove `WORKER_SSH_PORT` parsing;
   - determine `ssh_user` via `getpass.getuser()` with fallback `root`;
   - include `ssh_user` in registration and heartbeat payloads;
   - use the derived Tailscale socket path instead of hardcoded `/var/run/tailscale/tailscaled.sock`.

Acceptance criteria:

- worker registration includes `ssh_user`;
- orchestrator persists and displays `ssh_user`;
- no user-facing worker SSH port remains.

### Phase 2 — Replace worker sshd startup with Tailscale SSH

Files:

- `worker_container/entrypoint.sh`
- `worker_container/Dockerfile`

Steps:

1. Remove OpenSSH runtime setup from `entrypoint.sh`:
   - delete `sed -i ... /etc/ssh/sshd_config`;
   - delete `/ssh/authorized_keys` validation;
   - delete `chmod 600 /ssh/authorized_keys`;
   - delete `mkdir -p /run/sshd`;
   - delete `/usr/sbin/sshd` startup.
2. Remove `tailscale serve` SSH forwarding from `entrypoint.sh`.
3. Remove `TS_USERSPACE` parsing/branching and always run `tailscaled` in userspace networking mode.
4. Add worker directory setup:

   ```bash
   WH_DIR="${WH_DIR:-$HOME/.local/worker-harness}"
   TS_STATE_DIR="$WH_DIR/tailscale/state"
   TS_SOCKET_DIR="$WH_DIR/tailscale/run"
   TS_SOCKET="$TS_SOCKET_DIR/tailscaled.sock"
   WORKER_DAEMON_DIR="$WH_DIR/worker-daemon"
   HARNESS_DIR="$WH_DIR/harness"
   mkdir -p "$TS_STATE_DIR" "$TS_SOCKET_DIR" "$WORKER_DAEMON_DIR" "$HARNESS_DIR"
   ```

5. Start `tailscaled` with the new paths in userspace mode only:

   ```bash
   tailscaled \
     --state="$TS_STATE_DIR/tailscaled.state" \
     --socket="$TS_SOCKET" \
     --tun=userspace-networking \
     --socks5-server="$TS_SOCKS5_ADDR" &
   ```

6. Run `tailscale up` with `--ssh` unconditionally:

   ```bash
   UP_ARGS=(
     --login-server="$TS_HOST"
     --authkey="$TS_AUTHKEY"
     --accept-routes="$TS_ACCEPT_ROUTES"
     --ssh
   )
   tailscale --socket="$TS_SOCKET" up "${UP_ARGS[@]}"
   ```

7. Export the derived socket path for `worker_daemon.py`:

   ```bash
   export TS_SOCKET
   ```

8. Remove OpenSSH packages and baked-key setup from `worker_container/Dockerfile`:
   - remove `openssh-server` if no other dependency needs it;
   - remove SSH host key generation;
   - remove `/ssh/authorized_keys` copy;
   - remove sshd config changes.
9. Keep `openssh-client` only if useful for debugging inside the worker; the orchestrator is the component that requires SSH client functionality.

Acceptance criteria:

- worker starts without writing to `/etc/ssh`, `/run/sshd`, `/ssh`, `/var/lib/tailscale`, or `/var/run/tailscale`;
- worker always runs Tailscale in userspace networking mode;
- worker joins tailnet with Tailscale SSH enabled;
- `tailscale --socket="$TS_SOCKET" ip -4` works;
- Singularity/Apptainer works by setting only `WH_DIR` to a writable location.

### Phase 3 — Remove SSH key exchange and orchestrator key dependency

Files:

- `justfile`
- `orchestrator_container/Dockerfile`
- `orchestrator_container/entrypoint.sh`
- `worker_container/Dockerfile`
- `.gitignore`
- docs

Steps:

1. Simplify `just build`:
   - do not generate `orchestrator_container/ssh/orchestrator_ed25519`;
   - do not copy public keys into `worker_container/authorized_keys`;
   - build orchestrator and worker images directly.
2. Remove or repurpose `just clearkeys`.
   - Prefer deleting it if no remaining key artifacts exist.
3. Remove orchestrator private key copy from orchestrator image build.
4. Remove `SSH_KEY_PATH` validation from `orchestrator_container/entrypoint.sh`.
5. Remove `SSH_KEY_PATH` env var documentation.
6. Remove key artifacts from `.gitignore` if they are no longer generated.

Acceptance criteria:

- clean checkout can build both images without generating SSH keys;
- worker image build does not require `authorized_keys`;
- orchestrator image does not contain a private SSH key.

### Phase 4 — Update orchestrator SSH command construction

Files:

- `src/worker_harness/ssh.py`
- any tests covering SSH command construction

Steps:

1. Remove global `SSH_USER`.
2. Remove `SSH_KEY_PATH`.
3. Add a target helper:

   ```python
   def ssh_target(worker: Worker) -> str:
       return f"{worker.ssh_user}@{worker.worker_ip}"
   ```

4. Simplify common SSH args:

   ```python
   def ssh_common_args() -> list[str]:
       return [
           "-o", "StrictHostKeyChecking=no",
           "-o", "UserKnownHostsFile=/dev/null",
       ]
   ```

5. Do not pass `-p`; Tailscale SSH uses port `22`.
   - Passing `-p 22` is harmless, but omitting it makes the intended model clearer.
6. Do not pass `-i`.
7. Update all SSH/SCP/forwarding calls:
   - command execution;
   - tmux job start;
   - tmux kill;
   - tmux running check;
   - tmux capture;
   - file copy;
   - `ssh -N -L` tunnels.
8. Ensure `scp` uses the same `worker.ssh_user@worker.worker_ip` target.
9. Keep the current tmux/job/tunnel behavior unchanged apart from auth/target construction.

Acceptance criteria:

- generated SSH commands target `ssh_user@worker_ip`;
- no generated SSH/SCP command includes `-i`;
- no generated SSH/SCP command depends on a worker SSH port;
- jobs and tunnels still call normal OpenSSH client binaries from the orchestrator.

### Phase 5 — Update Tailscale/Headscale policy

Files:

- `headscale-policy.example.json`
- `README.md`
- `specs/TAILSCALE.md`

Steps:

1. Keep normal ACLs for heartbeat/API reachability:

   ```json
   {
     "action": "accept",
     "src": ["tag:wh-worker"],
     "dst": ["tag:wh-orchestrator:12888"]
   }
   ```

2. Keep broad orchestrator-to-worker network ACL if needed for non-SSH traffic:

   ```json
   {
     "action": "accept",
     "src": ["tag:wh-orchestrator"],
     "dst": ["tag:wh-worker:*"]
   }
   ```

3. Add Tailscale SSH policy.

Example:

```json
"ssh": [
  {
    "action": "accept",
    "src": ["tag:wh-orchestrator"],
    "dst": ["tag:wh-worker"],
    "users": ["root", "autogroup:nonroot"]
  }
]
```

Notes:

- Tailscale SSH policy does not specify a port; it always applies to Tailscale SSH on Tailnet port `22`.
- `users` must include the runtime user registered by the worker.
- For Docker/rootful workers this is often `root`.
- For Singularity/Apptainer workers this is often the invoking HPC username.
- Headscale's policy schema does not accept `allowLocalPortForwarding` / `allowRemotePortForwarding` fields. For `action: "accept"`, Headscale compiles the emitted Tailscale SSH action with local/remote forwarding enabled.

Acceptance criteria:

- orchestrator can `ssh <worker.ssh_user>@<worker.worker_ip>`;
- job startup works;
- local port forwarding works when policy allows forwarding;
- policy denial produces a clear Tailscale SSH rejection.

### Phase 6 — Documentation updates

Files:

- `README.md`
- `specs/TAILSCALE.md`
- `specs/TAILSCALE_VALIDATION.md`
- `docker-compose.tailscale.example.yml` if env vars change there

Steps:

1. Document Tailscale SSH as the only supported worker control channel.
2. Remove documentation for:
   - build-time SSH key exchange;
   - `just clearkeys`;
   - `SSH_KEY_PATH`;
   - `WORKER_SSH_PORT`;
   - `TS_SERVE_SSH_PORT`;
   - `TS_USERSPACE`;
   - port `2222`;
   - sshd setup.
3. Document `WH_DIR` and its fixed subdirectory layout.
4. Update Docker example.
5. Update Podman example.
6. Update Singularity/Apptainer example:

   ```bash
   apptainer run --nv \
     --env TS_AUTHKEY='<WORKER_TS_AUTHKEY>' \
     --env ORCHESTRATOR_HOST='<orchestrator-tailnet-dns-or-ip>' \
     --env WH_PROXY='socks5://127.0.0.1:1055' \
     --env WH_DIR="$HOME/.local/worker-harness" \
     worker-harness-worker.sif
   ```

7. Explain that Tailscale SSH uses Tailnet port `22` but does not publish host port `22`.
8. Explain that Tailscale SSH policy must allow the runtime worker user.
9. Explain that worker-harness tunnels use SSH local forwarding and are supported by Headscale `action: "accept"` SSH rules.

Acceptance criteria:

- README no longer implies OpenSSH/key-based setup;
- Singularity instructions no longer require overlays or bind mounts for `/var/lib/tailscale`/`/var/run/tailscale`;
- documented commands match the new entrypoint behavior.

### Phase 7 — Tests and validation

Automated tests:

1. Registration/model tests:
   - `ssh_user` defaults to `root` if omitted;
   - registration with explicit `ssh_user` persists correctly;
   - worker summaries expose `ssh_user`.
2. DB tests:
   - new DB schema includes `ssh_user`;
   - no code path requires `ssh_port`.
3. Worker daemon tests:
   - payload includes runtime `ssh_user`;
   - Tailscale IP lookup uses derived socket path;
   - payload does not include SSH port.
4. SSH command tests:
   - target is `<worker.ssh_user>@<worker.worker_ip>`;
   - no command includes `-i`;
   - no command includes non-default port configuration;
   - `ssh -N -L` still uses the same target.
5. Container static checks:
   - worker Dockerfile does not copy `authorized_keys`;
   - worker entrypoint does not call `sshd`;
   - worker entrypoint calls `tailscale up --ssh`;
   - worker entrypoint does not reference `/var/lib/tailscale` or `/var/run/tailscale`.

Manual validation:

1. Docker:
   - build image;
   - run worker;
   - confirm worker registers `ssh_user=root`;
   - start a job;
   - create a tunnel.
2. Podman:
   - run where available;
   - confirm same behavior.
3. Singularity/Apptainer:
   - create `.sif`;
   - run with `WH_DIR`;
   - confirm no writes to rootfs paths;
   - confirm worker registers the invoking user;
   - start a job;
   - create a tunnel.
4. Policy negative test:
   - remove the matching Tailscale SSH `users` entry;
   - confirm SSH/job startup fails with a policy error.
5. Forwarding validation:
   - with the Headscale `action: "accept"` SSH rule, confirm `ssh -N -L` worker-harness tunnel creation succeeds.

## Proposed rollout order

1. Remove key generation from build flow.
2. Add `ssh_user` to registration/model/database.
3. Change worker Tailscale state/socket paths, worker daemon ID, and harness paths to live under `WH_DIR`.
4. Replace worker sshd/serve setup with `tailscale up --ssh`.
5. Simplify orchestrator SSH command construction to Tailscale SSH only.
6. Update Headscale policy example.
7. Update README and runtime examples.
8. Run Docker/Podman/Singularity validation.
9. Delete obsolete code/docs/tests for sshd, keys, SSH port env vars, and key paths.

## Open questions

1. Should `ssh_port` be physically removed from the SQLite schema immediately, or left as an ignored obsolete column until the next DB reset?
2. Should the worker image keep `openssh-client` for debugging, or remove all OpenSSH packages from the worker image?
3. Should orchestrator UI show `ssh_user@worker_ip` prominently in worker lists?
4. For Headscale policy, is `autogroup:nonroot` supported in the deployed Headscale version, or should docs instruct users to enumerate allowed HPC usernames explicitly?
