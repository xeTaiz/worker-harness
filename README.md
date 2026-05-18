# Worker Harness

Worker Harness manages containerized worker nodes that register to an orchestrator over a private overlay network.

This repository now targets **Tailscale + Headscale** (replacing ZeroTier).

## Networking model

- Workers run with `tag:worker`.
- Orchestrator runs with `tag:orchestrator`.
- Optional user/client nodes run with `tag:client`.

Required ACL directions:

1. `tag:worker` -> `tag:orchestrator:12888` (heartbeat/register API)
2. `tag:orchestrator` -> `tag:worker:22` (SSH job/tunnel control)
3. `tag:client` -> `tag:orchestrator:12888` (optional external access)

Everything else remains denied by default.

See `specs/headscale-policy.example.json` for an example policy.

## Build-time SSH key exchange (separate images)

Use `just` to build both images with a paired SSH key setup:

- `just build`
  - generates `orchestrator_container/ssh/orchestrator_ed25519` only if missing
  - copies the public key to `worker_container/authorized_keys`
  - builds orchestrator and worker images separately
- `just clearkeys`
  - deletes generated orchestrator keypair and `worker_container/authorized_keys`
  - next `just build` creates a fresh keypair

This gives you independent worker/orchestrator images with no runtime shared-volume key distribution required.

## Worker container env vars

Required:

- `TS_AUTHKEY` - Headscale/Tailscale auth key
- `ORCHESTRATOR_HOST` - orchestrator tailnet DNS name

Recommended:

- `TS_TAGS=tag:worker`
- `TS_HOSTNAME=<worker-name>`
- `TS_ACCEPT_ROUTES=false`
- `ORCHESTRATOR_PORT=12888`
- `WORKER_SSH_PORT=22`
- `HEARTBEAT_INTERVAL=60`
- `WORKER_NAME=<display-name>`

## Orchestrator container env vars

Required:

- `TS_AUTHKEY` - Headscale/Tailscale auth key

Recommended:

- `TS_TAGS=tag:orchestrator`
- `TS_HOSTNAME=orchestrator`
- `TS_ACCEPT_ROUTES=false`
- `WH_HB_HOST=0.0.0.0`
- `WH_HB_PORT=12888`
- `WH_DB_PATH=/data/db.sqlite`
- `WH_COMMAND=serve`

SSH key path:

- `SSH_KEY_PATH` defaults to baked key path: `/opt/worker-harness/ssh/orchestrator_ed25519`

## Worker registration field

Worker registration now uses `worker_ip`.
`zerotier_ip` is still accepted as a backward-compatible input alias.

## Runtime requirements for both containers

- `/dev/net/tun` device
- `NET_ADMIN` capability
- Persistent `/var/lib/tailscale` volume if stable identity is desired

See also:
- `specs/TAILSCALE.md`
- `specs/docker-compose.tailscale.example.yml`
