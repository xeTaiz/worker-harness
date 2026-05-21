# Worker Harness

Worker Harness manages containerized worker nodes that register to an orchestrator over a private overlay network.

This repository targets **Tailscale + Headscale**.

## Networking model

- Workers run with `tag:wh-worker`.
- Orchestrator runs with `tag:wh-orchestrator`.
- Optional user/client nodes can run with `tag:client`.

Required ACL directions:

1. `tag:wh-worker` -> `tag:wh-orchestrator:12888` (heartbeat/register API)
2. `tag:wh-orchestrator` -> `tag:wh-worker:22` (SSH job/tunnel control)
3. `tag:client` -> `tag:wh-orchestrator:12888` (optional external access)

Everything else remains denied by default.

See `headscale-policy.example.json` for an example policy.

## Build-time SSH key exchange (separate images)

Use `just` to build both images with a paired SSH key setup:

- `just build`
  - generates `orchestrator_container/ssh/orchestrator_ed25519` only if missing
  - copies the public key to `worker_container/authorized_keys`
  - builds orchestrator and worker images separately
- `just clearkeys`
  - deletes generated orchestrator keypair and `worker_container/authorized_keys`
  - next `just build` creates a fresh keypair

This gives independent worker/orchestrator images with no runtime shared-volume key distribution required.

## Start containers with Docker or Podman (ephemeral runtime)

Build images first:

```bash
just build
```

Run orchestrator (only required env is `TS_AUTHKEY`):

### Docker

```bash
docker run -d \
  --name worker-harness-orchestrator \
  --restart unless-stopped \
  --cap-add NET_ADMIN \
  --device /dev/net/tun:/dev/net/tun \
  -e TS_AUTHKEY='<ORCH_TS_AUTHKEY>' \
  worker-harness/orchestrator:latest
```

### Podman

```bash
podman run -d \
  --name worker-harness-orchestrator \
  --restart unless-stopped \
  --cap-add NET_ADMIN \
  --device /dev/net/tun:/dev/net/tun \
  -e TS_AUTHKEY='<ORCH_TS_AUTHKEY>' \
  worker-harness/orchestrator:latest
```

Run worker (required envs: `TS_AUTHKEY`, `ORCHESTRATOR_HOST`):

### Docker

```bash
docker run -d \
  --name worker-harness-worker-1 \
  --restart unless-stopped \
  --gpus all \
  -e TS_AUTHKEY='<WORKER_TS_AUTHKEY>' \
  -e TS_USERSPACE='true' \
  -e WH_PROXY='socks5://127.0.0.1:1055' \
  -e ORCHESTRATOR_HOST='<orchestrator-tailnet-dns-or-ip>' \
  worker-harness/worker:latest
```

### Podman

```bash
podman run -d \
  --name worker-harness-worker-1 \
  --restart unless-stopped \
  --device nvidia.com/gpu=all \
  -e TS_AUTHKEY='<WORKER_TS_AUTHKEY>' \
  -e TS_USERSPACE='true' \
  -e WH_PROXY='socks5://127.0.0.1:1055' \
  -e ORCHESTRATOR_HOST='<orchestrator-tailnet-dns-or-ip>' \
  worker-harness/worker:latest
```

Notes:

- No keyshare volume is needed with the current build-time key flow (`just build`).
- Do not publish orchestrator API ports to the host; reach it via Tailnet IP/DNS only.
- No Tailscale state volume is needed for ephemeral containers.
- If you want persistent Tailnet identity across restarts, mount `/var/lib/tailscale`.

## Run worker with Singularity/Apptainer

Build the image first (`just build`), then create a `.sif` from the local Docker image:

```bash
apptainer pull worker-harness-worker.sif docker-daemon://worker-harness/worker:latest
```

Run the worker with GPU passthrough and required env vars:

```bash
apptainer run --nv \
  --env TS_AUTHKEY='<WORKER_TS_AUTHKEY>' \
  --env ORCHESTRATOR_HOST='<orchestrator-tailnet-dns-or-ip>' \
  --env TS_USERSPACE='true' \
  --env WH_PROXY='socks5://127.0.0.1:1055' \
  --env WORKER_SSH_PORT='2222' \
  --env TS_SERVE_SSH_PORT='2222' \
  worker-harness-worker.sif
```

Notes:

- `singularity` and `apptainer` CLIs are equivalent for these commands on most systems.
- `WORKER_SSH_PORT` and `TS_SERVE_SSH_PORT` should match so the orchestrator connects to the registered reachable port.

## Worker container env vars

Required:

- `TS_AUTHKEY` - Headscale/Tailscale auth key
- `ORCHESTRATOR_HOST` - orchestrator tailnet DNS name (or tailnet IP)

Defaults (if unset):

- `TS_TAGS=tag:wh-worker`
- `TS_HOST=https://controlplane.tailscale.com` (override for self-hosted Headscale)
- `TS_OPERATOR=worker-harness`
- `TS_HOSTNAME` unset
- `TS_ACCEPT_ROUTES=false`
- `TS_USERSPACE=true`
- `TS_SOCKS5_ADDR=127.0.0.1:1055`
- `TS_SERVE_SSH_PORT=<WORKER_SSH_PORT>`
- `WH_PROXY` unset (auto-defaults to `socks5://$TS_SOCKS5_ADDR` when `TS_USERSPACE=true`)
- `ORCHESTRATOR_PORT=12888`
- `WORKER_SSH_PORT=22`
- `HEARTBEAT_INTERVAL=60`
- `WORKER_NAME=<container hostname>`

## Orchestrator container env vars

Required:

- `TS_AUTHKEY` - Headscale/Tailscale auth key

Defaults (if unset):

- `TS_TAGS=tag:wh-orchestrator`
- `TS_HOST=https://controlplane.tailscale.com` (override for self-hosted Headscale)
- `TS_OPERATOR=worker-harness`
- `TS_HOSTNAME=orchestrator
- `TS_ACCEPT_ROUTES=false`
- `WH_HB_HOST=0.0.0.0`
- `WH_HB_PORT=12888`
- `WH_DB_PATH=~/.config/worker-harness/db.sqlite`
- `WH_COMMAND=serve`
- `SSH_KEY_PATH=/opt/worker-harness/ssh/orchestrator_ed25519`

## Worker registration field

Worker registration now uses `worker_ip`.
`zerotier_ip` is still accepted as a backward-compatible input alias.

## Runtime requirements

- **Orchestrator container:** requires `/dev/net/tun` + `NET_ADMIN` (kernel/TUN mode).
- **Worker container:**
  - `TS_USERSPACE=true`: no `/dev/net/tun` and no `NET_ADMIN` required.
  - `TS_USERSPACE=false`: requires `/dev/net/tun` + `NET_ADMIN`.
- Optional persistent `/var/lib/tailscale` volume if stable identity is desired.

See also:
- `specs/TAILSCALE.md`
- `specs/docker-compose.tailscale.example.yml`
