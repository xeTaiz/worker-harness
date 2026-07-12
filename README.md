# Worker Harness

Worker Harness manages containerized worker nodes that register to an orchestrator over a private overlay network.

This repository targets **Tailscale + Headscale**.

## Networking model

- Workers run with `tag:wh-worker`.
- Orchestrator runs with `tag:wh-orchestrator`.
- Optional user/client nodes can run with `tag:client`.

Required ACL directions:

1. `tag:wh-worker` -> `tag:wh-orchestrator:12888` (heartbeat/register API)
2. `tag:wh-orchestrator` -> `tag:wh-worker:*` (worker control traffic)
3. `tag:client` -> `tag:wh-orchestrator:12888` (optional external access)

Tailscale SSH policy is also required (see `headscale-policy.example.json`).

## Build images

```bash
just build
```

## Start containers with Docker or Podman (ephemeral runtime)

Run orchestrator (required env: `TS_AUTHKEY`):

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
  -e ORCHESTRATOR_HOST='<orchestrator-tailnet-dns-or-ip>' \
  -e SSH_USER="$(id -un)" \
  -e WH_PROXY='socks5://127.0.0.1:1055' \
  worker-harness/worker:latest
```

### Podman

```bash
podman run -d \
  --name worker-harness-worker-1 \
  --restart unless-stopped \
  --device nvidia.com/gpu=all \
  -e TS_AUTHKEY='<WORKER_TS_AUTHKEY>' \
  -e ORCHESTRATOR_HOST='<orchestrator-tailnet-dns-or-ip>' \
  -e SSH_USER="$(id -un)" \
  -e WH_PROXY='socks5://127.0.0.1:1055' \
  worker-harness/worker:latest
```

Notes:

- Worker control is **Tailscale SSH only** (`tailscale up --ssh` on workers).
- For Docker/Podman, pass `SSH_USER="$(id -un)"` so the worker advertises a non-root SSH user.
- No build-time SSH key exchange is required.
- Do not publish orchestrator API ports to the public host network; use Tailnet reachability.

## Run worker with Singularity/Apptainer

Build and convert from local Docker image:

```bash
apptainer pull worker-harness-worker.sif docker-daemon://worker-harness/worker:latest
```

Recommended deploy flow:

```bash
just dist
rsync -a dist/ target:/path/to/worker-harness/
```

Then on the target host:

```bash
cd /path/to/worker-harness
./install-service.sh
```

If you want to run it manually instead of systemd, put env vars in `.env` (or set `WH_ENV_FILE`) and run:

```bash
./start-wh.sh worker-harness-worker.sif
```

Notes:

- `singularity` and `apptainer` CLIs are equivalent on most systems.
- `start-wh.sh` auto-loads env from `WH_ENV_FILE`, `./.env`, `./worker-harness.env`, or `~/.config/worker-harness/worker-harness.env` if present.
- `start-wh.sh` binds a generated `/etc/passwd` and `/etc/group` plus a writable `WH_DIR` at `/var/lib/worker-harness`.
- Worker runtime user is auto-detected and registered as `ssh_user` (fallback `root`).
- `start-wh.sh` uses `--fakeroot` only when subordinate UID/GID ranges exist; override with `WH_FAKEROOT=1` or `0`.
- Tailscale SSH always uses Tailnet port `22`; this does not require publishing host port `22`.
- `just dist` stages a rsync-friendly bundle from the repo `.env` (generated `dist/.env` is gitignored).

### Auto-start on reboot (systemd user service)

If you want the worker to restart automatically after a crash:

```bash
./install-service.sh
```

`install-service.sh` copies `start-wh.sh` and `worker-harness-worker.sif` into `$HOME`, stores the runtime env in `~/.config/worker-harness/worker-harness.env`, and prompts for any missing mandatory values if no bundle `.env` is present.

For boot without login, enable user lingering:

```bash
loginctl enable-linger "$USER"
```

## Worker container env vars

Required:

- `TS_AUTHKEY` - Headscale/Tailscale auth key
- `ORCHESTRATOR_HOST` - orchestrator tailnet DNS name (or tailnet IP)

Defaults (if unset):

- `TS_HOST=https://controlplane.tailscale.com` (override for self-hosted Headscale)
- `TS_HOSTNAME` unset
- `TS_ACCEPT_ROUTES=false`
- `TS_EXTRA_ARGS` unset
- `TS_SOCKS5_ADDR=127.0.0.1:1055`
- `WH_PROXY` defaults to `socks5://$TS_SOCKS5_ADDR`
- `SSH_USER` auto-detected from runtime env/home (set explicitly for Docker/Podman)
- `WH_DIR=$HOME/.local/worker-harness`
  - Tailscale state: `$WH_DIR/tailscale/state`
  - Tailscale socket: `$WH_DIR/tailscale/run/tailscaled.sock`
  - Worker daemon ID: `$WH_DIR/worker-daemon/id`
  - Job/log harness: `$WH_DIR/harness`
- `ORCHESTRATOR_PORT=12888`
- `HEARTBEAT_INTERVAL=60`
- `WORKER_NAME=<container hostname>`
- `WH_OVERLAY` - path to a writable ext3 overlay file (default: `$WH_DIR/overlay.ext3`). Created automatically on first start if the runtime supports it. Lets `apt install` persist across container restarts.
- `WH_OVERLAY_SIZE` - overlay size in MiB (default: `8192` = 8 GB)
- `WH_EXTRA_BINDS` - semicolon-separated `host:container` bind mount pairs (default: empty). e.g. `WH_EXTRA_BINDS="$HOME/Dev:/code;/data/datasets:/data"`
- `WH_MOUNT_HOME_FOLDERS` - set to `0` to disable auto-mounting non-hidden directories from `$HOME` into the container (default: `1`, enabled). Hidden dirs (`.ssh`, `.gnupg`, `.config`, `.aws`, etc.) are excluded automatically by the glob.

## Orchestrator container env vars

Required:

- `TS_AUTHKEY` - Headscale/Tailscale auth key

Defaults (if unset):

- `TS_HOST=https://controlplane.tailscale.com` (override for self-hosted Headscale)
- `TS_HOSTNAME=orchestrator`
- `TS_ACCEPT_ROUTES=false`
- `TS_EXTRA_ARGS` unset
- `WH_HB_HOST=0.0.0.0`
- `WH_HB_PORT=12888`
- `WH_DB_PATH=~/.config/worker-harness/db.sqlite`
- `WH_COMMAND=serve`

## Worker registration fields

Worker registration uses `worker_ip`, `ssh_user`, and `harness_dir`.
`zerotier_ip` is still accepted as a backward-compatible input alias for `worker_ip`.

## Runtime requirements

- **Orchestrator container:** requires `/dev/net/tun` + `NET_ADMIN`.
- **Worker container:** uses Tailscale userspace networking.

See also:
- `specs/TAILSCALE.md`
- `docker-compose.tailscale.example.yml`
- `headscale-policy.example.json`
- `worker_container/CUDA_ENV_GUIDE.md` — how to install CUDA toolkits (nvcc) and Python ML environments on demand inside a worker
