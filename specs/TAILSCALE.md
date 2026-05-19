# Tailscale/Headscale Migration Plan (Worker Harness)

## 1) Goals

- Replace ZeroTier with Tailscale (Headscale control plane) for both workers and orchestrator.
- Workers must be able to find and reach orchestrator for heartbeat registration.
- Orchestrator must be able to SSH into workers (job execution + tunnel setup).
- Workers must **not** be able to reach other workers or unrelated nodes.
- Orchestrator should be reachable by non-worker nodes, but orchestrator should not initiate connections back to those non-worker nodes.
- Use auth keys passed via env vars; support ephemeral worker/orchestrator nodes.

## 2) Access Model (ACL + tags)

Use tags to model roles:

- `tag:wh-orchestrator`
- `tag:wh-worker`
- (optional) `tag:client` for trusted non-worker nodes

### Required directional ACL rules

1. Worker -> Orchestrator heartbeat API:
   - `tag:wh-worker` -> `tag:wh-orchestrator:<ORCHESTRATOR_PORT>` (default 12888)
2. Orchestrator -> Worker SSH:
   - `tag:wh-orchestrator` -> `tag:wh-worker:22`
3. Non-worker clients -> Orchestrator API/TUI (as needed):
   - e.g. `autogroup:member` or `tag:client` -> `tag:wh-orchestrator:<ports>`

### Explicitly *not* allowed

- Worker -> Worker (no rule)
- Worker -> non-worker nodes (no rule)
- Orchestrator -> non-worker nodes (no rule), unless explicitly needed

> Note: Tailscale/Headscale ACLs are directional. You can allow clients to reach orchestrator without allowing orchestrator to initiate to clients.

## 3) Routing/Tunnel Clarification

- Existing tunnel flow uses SSH local forwards from orchestrator to worker.
- Therefore, workers do **not** need to expose all service ports at ACL layer.
- Required network permission for tunnel capability is still orchestrator -> worker:22.
- The forwarded remote port is opened *inside* SSH; no extra Tailnet ACL rule is needed for that remote port.
- Only add worker service-port ACLs if orchestrator must connect directly (without SSH forwarding).

## 4) Naming/Discovery Strategy

- Remove dependency on static orchestrator IP env values.
- Use Tailscale DNS name (MagicDNS/Headscale DNS) as `ORCHESTRATOR_HOST`, e.g.:
  - `orchestrator.<tailnet-domain>`
- Worker daemon keeps using `ORCHESTRATOR_HOST` + `ORCHESTRATOR_PORT`; value changes from IP to DNS name.

## 5) Worker Container Migration Plan

## 5.1 Replace ZeroTier bootstrap

Current files:
- `worker_container/Dockerfile`
- `worker_container/entrypoint.sh`
- `worker_container/install_zerotier.sh`
- `worker_container/worker_daemon.py`

Planned changes:

1. Dockerfile
   - Remove ZeroTier install steps.
   - Install `tailscale` + runtime deps.
   - Keep `openssh-server`, `tmux`, Python runtime, podman tooling unchanged.

2. Remove `install_zerotier.sh` (or keep deprecated temporarily with clear no-op/deprecation notice).

3. `entrypoint.sh`
   - Remove ZeroTier secret/network join logic.
   - Start `tailscaled`.
   - Run `tailscale up` with env-provided auth key + worker tag(s).
   - Wait until Tailnet IP/hostname available.
   - Start SSH server and worker daemon.

4. `worker_daemon.py`
   - Replace `get_zerotier_ip()` with `get_tailscale_ip()` (e.g. via `tailscale ip -4` or `tailscale status --json`).
   - Keep payload field temporarily backward compatible (see section 7).
   - Keep heartbeat URL construction: `http://{ORCHESTRATOR_HOST}:{ORCHESTRATOR_PORT}/register`.

5. Worker SSH authorized key bootstrap
   - Do not bake a fixed orchestrator key into the worker image.
   - Worker entrypoint waits for orchestrator public key at `ORCHESTRATOR_PUBKEY_PATH` (shared volume), then appends it to `/ssh/authorized_keys`.

## 5.2 Worker env var contract (new)

Required:
- `TS_AUTHKEY` (ephemeral/reusable key from Headscale)
- `TS_TAGS` (e.g. `tag:wh-worker`)
- `TS_HOSTNAME` (optional explicit node name)
- `ORCHESTRATOR_HOST` (Tailnet DNS name)
- `ORCHESTRATOR_PORT` (default `12888`)

Optional hardening:
- `TS_ACCEPT_ROUTES=false`
- `TS_EXTRA_ARGS` for controlled overrides

Remove/deprecate:
- `ZEROTIER_NETWORK_ID`
- `ZEROTIER_SECRET`

## 6) Minimal Orchestrator Container Plan

Create a new minimal image (e.g. `orchestrator_container/`):

Contents:
- Python app + deps (`worker_harness`)
- `tailscale` package
- `openssh-client` (for orchestrator -> worker SSH commands)

Entrypoint behavior:
1. Start `tailscaled`.
2. `tailscale up --authkey=$TS_AUTHKEY --advertise-tags=tag:wh-orchestrator --hostname=$TS_HOSTNAME`.
3. Wait for Tailnet connectivity.
4. Generate orchestrator SSH keypair on first boot (persistent volume).
5. Export orchestrator public key to shared volume path consumed by workers.
6. Set `SSH_KEY_PATH` to generated private key for orchestrator SSH module.
7. Start orchestrator server (`worker-harness serve` or `all`).

Runtime requirements:
- Access to `/dev/net/tun`
- `NET_ADMIN` capability (and platform-specific equivalents)
- Persistent state dir for tailscale (`/var/lib/tailscale`) if non-ephemeral identity desired
- Persistent orchestrator data volume for SSH private key
- Shared read/write volume for orchestrator public key distribution to workers

Network exposure:
- Do not publish heartbeat/API (12888) on the host.
- Access orchestrator API via Tailnet IP/DNS only, controlled by ACLs/tags.

## 7) Data Model + Compatibility Plan

Current code uses `zerotier_ip` in:
- `src/worker_harness/models.py`
- `src/worker_harness/db.py`
- `src/worker_harness/ssh.py`
- `src/worker_harness/heartbeat.py` logging

Migration path:

Phase A (safe transition):
- Keep DB column `zerotier_ip` but semantically treat as overlay IP.
- Worker sends Tailnet IP in same field to avoid immediate schema migration.

Phase B (cleanup):
- Rename API/model/DB field to `overlay_ip` or `tailscale_ip`.
- Add DB migration:
  - new column
  - backfill from `zerotier_ip`
  - code reads new column with fallback
- Update `ssh.py` to target renamed field.

Recommended: use `overlay_ip` to avoid future VPN lock-in.

## 8) Headscale Policy Deliverables

Add policy file artifact (outside container images), including:

1. `tagOwners`
2. ACL rules for:
   - worker -> orchestrator:heartbeat-port
   - orchestrator -> worker:22
   - non-workers -> orchestrator:selected-ports
3. Optional SSH policy section if using Tailscale SSH (not required for current OpenSSH model)

## 9) Security Hardening Checklist

- Use short-lived/ephemeral auth keys for workers.
- Separate key scopes if possible:
  - one for workers (`tag:wh-worker`)
  - one for orchestrator (`tag:wh-orchestrator`)
- Restrict tag ownership to admin identities only.
- Disable route acceptance on workers unless explicitly needed.
- Do not advertise subnet routes or exit nodes from workers.
- Add app-level shared secret on `/register` in a follow-up (VPN-only trust is currently the boundary).

## 10) Rollout Plan (Step-by-step)

1. Create/apply Headscale ACL + tag policy.
2. Build worker image with tailscale bootstrap changes.
3. Build minimal orchestrator image with tailscale bootstrap.
4. Provision shared key volume and mount it read/write on orchestrator, read-only on workers.
5. Deploy one orchestrator test instance with `tag:wh-orchestrator`.
6. Deploy one worker test instance with `tag:wh-worker` and `ORCHESTRATOR_HOST=<tailnet-dns>`.
7. Validate:
   - worker heartbeat succeeds
   - orchestrator can SSH worker and start job
   - port forward via SSH works
   - worker cannot reach any non-orchestrator node
   - worker cannot reach other worker
   - orchestrator cannot initiate to non-worker nodes
8. Migrate remaining workers in batches.
9. Remove ZeroTier env vars/scripts and documentation.
10. Perform field rename/migration (`zerotier_ip` -> `overlay_ip`) after stable operation.

## 11) Validation Commands (acceptance)

From worker container:
- `tailscale status`
- `curl -sf http://$ORCHESTRATOR_HOST:$ORCHESTRATOR_PORT/health`
- Negative tests: attempt connections to forbidden peers should fail.

From orchestrator container:
- `tailscale status`
- `worker-harness worker list` (or API listing)
- SSH job launch + log retrieval
- Tunnel creation API test

From non-worker client:
- Access orchestrator allowed endpoint(s) succeeds.
- Any direct access to workers fails.

## 12) Repository Change List (planned)

Worker-side:
- `worker_container/Dockerfile`
- `worker_container/entrypoint.sh`
- `worker_container/worker_daemon.py`
- remove/deprecate `worker_container/install_zerotier.sh`

Orchestrator-side:
- add `orchestrator_container/Dockerfile`
- add `orchestrator_container/entrypoint.sh`
- (optional) add compose/k8s manifests for required container capabilities

Core code/docs:
- `src/worker_harness/models.py` (future rename)
- `src/worker_harness/db.py` (future migration)
- `src/worker_harness/ssh.py` (future rename usage)
- `SPEC.md` (network section rewrite)
- `README.md` (new runtime and env docs)

## 13) Open Decisions

1. Field naming: `tailscale_ip` vs `overlay_ip` (recommended: `overlay_ip`).
2. Keep OpenSSH model vs migrate to Tailscale SSH later.
3. Whether orchestrator container also serves TUI remotely or only API/CLI.
4. Exact non-worker identity group for orchestrator access (`autogroup:member` vs dedicated `tag:client`).
