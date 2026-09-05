# Worker Harness

Worker Harness manages containerized worker nodes that register to an orchestrator over a private overlay network.

This repository targets **Tailscale + Headscale**.

## Agent fleet

The control service coordinates one orchestrator, one PM per configured project,
and disposable task worktrees on Herdr client machines. GPU workers remain the
compute plane. Role-scoped bearer tokens protect agent APIs; operator clients
and the Tailnet-only web edge use a separate operator secret.

See [DEPLOYMENT.md](DEPLOYMENT.md) for the paired server/client rollout, pinned
plugin artifacts, credential placement, acceptance gates, and database rollback.
The old router and delegated-container Pi launch paths are removed.

## Networking model

- Workers run with `tag:wh-worker`.
- Orchestrator runs with `tag:wh-orchestrator`.
- Optional user/client nodes can run with `tag:client`.

Required ACL directions:

1. `tag:wh-worker` -> `tag:wh-orchestrator:12888` (heartbeat/register API only)
2. `tag:wh-orchestrator` -> `tag:wh-worker:*` (worker control traffic)
3. Operator/client Tailnet members -> `tag:wh-orchestrator:12889` (privileged control API, including agent fleet control)

`tag:wh-worker` must not be granted access to port `12889`; worker registration
and the operator control plane are deliberately separate services. In
production, the mobile Pi-session webapp is served by the standalone `wh-web`
container on the VPS host's Tailscale IP. `wh-web` proxies only the session UI's
HTTP, SSE, and WebSocket routes to `wh-orch:12889` over a private Docker network.
It uses the existing Tailnet trust boundary—there is no separate browser
credential. The orchestrator can still serve an explicitly configured
`WH_WEB_DIR` for local development, but its production image does not bundle the
web assets. Interactive session terminals continue through the host relay.

## Build images

```bash
just build-orch     # orchestrator only
just build-worker   # worker only
just build-web      # wh-web only

just push-orch      # build and push the orchestrator only
just push-worker    # build and push the worker only
just push-web       # build and push wh-web only
```

Every Docker build receives three tags automatically:
`xetaiz/<image>:latest`, `xetaiz/<image>:<branch>`, and
`xetaiz/<image>:<branch>-<7-character-commit>`. A dirty worktree adds `-dirty`
to the commit tag so an uncommitted image cannot be mistaken for an exact
commit build. For example, `just build-orch` on clean `giga-wh` builds
`xetaiz/wh-orch:latest`, `xetaiz/wh-orch:giga-wh`, and
`xetaiz/wh-orch:giga-wh-<commit>`. Each `push-*` recipe first runs its matching
build, then pushes all three tags, including `:latest`; this prevents Docker Hub's
moving tag from lagging behind the immutable release tag. Set
`WH_IMAGE_NAMESPACE` to override `xetaiz` without editing the `justfile`.
Docker builds use host networking by default so hosts whose resolver is managed
by Tailscale can resolve package registries during the build. Set
`WH_DOCKER_BUILD_NETWORK=default` when the Docker bridge has working DNS.
Root build contexts are allowlisted by `.dockerignore`, excluding local SIFs,
caches, Git history, and deployment data.

## Run the standalone web UI

`docker-compose.web.example.yml` manages only `wh-web`. This is intentional: you
can deploy and test it against the currently running orchestrator before
replacing the orchestrator image.

The existing orchestrator must be attached to a user-defined Docker network and
be resolvable there as `wh-orch`:

```bash
docker network inspect wh-internal >/dev/null 2>&1 || docker network create wh-internal
docker network connect --alias wh-orch wh-internal wh-orch
```

If the orchestrator has a different container name, use that name as the final
argument while retaining the `wh-orch` alias. Connecting a container that is
already on the network is unnecessary.

Start the web container with an explicit host Tailscale address. There is no
`0.0.0.0` default, and the web container needs no persistent volume because its
assets are baked into the image.

```bash
export WH_WEB_BIND_IP="$(tailscale ip -4 | head -n1)"
export WH_WEB_PORT=18080
export WH_DOCKER_NETWORK=wh-internal

docker compose -f docker-compose.web.example.yml up -d --build
```

Verify the separate path before changing the orchestrator deployment:

```bash
curl -fsS "http://${WH_WEB_BIND_IP}:${WH_WEB_PORT}/healthz"
curl -fsS "http://${WH_WEB_BIND_IP}:${WH_WEB_PORT}/"
curl -fsS "http://${WH_WEB_BIND_IP}:${WH_WEB_PORT}/api/v1/pi/sessions"
```

Then open `http://<VPS-TAILSCALE-IP>:18080/` from an authorized Tailnet client
and verify session updates, prompting/configuration, and terminal attachment.
The old orchestrator-served UI remains available during this test, so stopping
`wh-web` is a complete rollback.

Only after that test passes should the no-web orchestrator image be deployed.
The replacement `wh-orch` must join `wh-internal` with the same alias. Restart
`wh-web` after replacing `wh-orch`, because Nginx resolves the upstream container
address when it starts:

```bash
docker compose -f docker-compose.web.example.yml restart wh-web
```

Do not change or share the orchestrator's SQLite or Tailscale state mounts as
part of this web cutover, and never run two orchestrators against the same
Tailscale state directory.

## Start containers with Docker or Podman (ephemeral runtime)

Run orchestrator (required env: `TS_AUTHKEY`):

### Docker

```bash
docker run -d \
  --name wh-orch \
  --restart unless-stopped \
  --cap-add NET_ADMIN \
  --device /dev/net/tun:/dev/net/tun \
  -v worker-harness-orchestrator-tailscale:/var/lib/tailscale \
  -v worker-harness-orchestrator-data:/root/.config/worker-harness \
  -e TS_AUTHKEY='<ORCH_TS_AUTHKEY>' \
  xetaiz/wh-orch:latest
```

### Podman

```bash
podman run -d \
  --name wh-orch \
  --restart unless-stopped \
  --cap-add NET_ADMIN \
  --device /dev/net/tun:/dev/net/tun \
  -v worker-harness-orchestrator-tailscale:/var/lib/tailscale \
  -v worker-harness-orchestrator-data:/root/.config/worker-harness \
  -e TS_AUTHKEY='<ORCH_TS_AUTHKEY>' \
  xetaiz/wh-orch:latest
```

Both orchestrator volumes are required for replacement-safe deployments. The
Tailscale volume preserves the node identity, IP, and stable MagicDNS routing;
the data volume preserves the SQLite worker/session/event registry. Omitting
them creates a fresh Tailnet node and database whenever the container is
recreated, causing transient browser DNS failures and requiring live bridges
and workers to repopulate the registry.

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
  xetaiz/wh-worker:latest
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
  xetaiz/wh-worker:latest
```

Notes:

- Worker control is **Tailscale SSH only** (`tailscale up --ssh` on workers).
- For Docker/Podman, pass `SSH_USER="$(id -un)"` so the worker advertises a non-root SSH user.
- No build-time SSH key exchange is required.
- Do not publish orchestrator API ports to the public host network; use Tailnet reachability.

## Run worker with Singularity/Apptainer

Build and convert from local Docker image:

```bash
apptainer pull worker-harness-worker.sif docker-daemon://xetaiz/wh-worker:latest
```

Recommended deploy flow:

```bash
just deploy target
```

`target` may be a host alias from `~/.ssh/config` or `user@hostname`. The recipe
builds `dist/`, uploads it to a new staging directory, and runs a transactional
migration on the worker:

1. preserve the worker env and rclone credentials in the stage,
2. disable new update/restart triggers and let any active updater finish,
3. acquire the same host lock used by the update and restart helpers,
4. defer any pending `new-image.sif` or restart trigger,
5. stop the worker and each rclone service, confirming old mounts are gone,
6. rename `~/worker-harness` to a timestamped backup and atomically rename the
   stage into its place,
7. migrate regular systemd user units and configs to symlinks, validate rclone
   remotes, and normalize their bind destinations,
8. restart the rclone, worker, and path units, then require both systemd and the
   worker daemon to remain healthy, and
9. automatically restore the old directory, config/unit state, enabled/active
   service state, and pending triggers if any step fails.

To migrate the machine you are on, use the SSH-free variant:

```bash
just deploy-local
```

It stages, swaps, health-checks, and rolls back exactly like a remote
deployment, and refuses to run if `~/worker-harness` is the repository itself.

The successful deployment retains the previous installation at
`~/worker-harness.backup.<transaction>`, including a `.deployment-state`
snapshot. Pending update/restart triggers are retained there rather than
running immediately after deployment. Remove the backup manually after the
worker has been observed in production.

During installation, rclone comes from the official
`https://rclone.org/install.sh` script when it is absent or lacks the SMB
backend. Working network mounts use `/data/shared/datawaha`,
`/data/shared/ibex`, and `/data/shared/ibex_c2324`. Local filesystems mounted
at `/mnt` use `/data/local`; direct child mounts use
`/data/local/<mount-name>`.

Rclone mount activation waits up to 30 seconds by default
(`WH_RCLONE_MOUNT_TIMEOUT`). A failed mount is stopped and disabled, but its
unit remains linked under `~/.config/systemd/user` for inspection or a later
restart; the installer prints recent status and journal output.

Keep the common worker credentials in the gitignored `worker_rclone.conf`;
`just dist` packages it as `dist/rclone.conf`, and that bundled config is
authoritative during deployment. An existing worker config is used only when
the bundle has none and is always retained in the rollback backup.

The transaction covers user services managed under
`~/.config/systemd/user`. System-wide units under `/etc/systemd/system` and
unrelated rclone service names are not migrated. `SIGINT`, `SIGTERM`, and
ordinary command failures roll back automatically; power loss or `SIGKILL`
can still require selecting the timestamped backup manually.

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
- `just dist` stages a deploy bundle from the repo `.env`; generated credentials, `.env`, and `.sif` files under `dist/` are gitignored.

### Auto-start on reboot (systemd user service)

If you want the worker to restart automatically after a crash:

```bash
./install-service.sh
```

`install-service.sh` keeps `start-wh.sh`, `worker-harness-worker.sif`, `rclone.conf`, and all service units in `~/worker-harness`. It creates symlinks from `~/.config/systemd/user/`, `~/.config/worker-harness/`, and `~/.config/rclone/` back into that directory. The runtime env remains mutable at `~/worker-harness/.env` (linked as `~/.config/worker-harness/worker-harness.env`). Updating scripts or units therefore requires no recopy; run `systemctl --user daemon-reload` after unit changes and restart the affected service as needed. Existing copied installations can migrate once with `./migrate-to-symlinks.sh`; it preserves a regular config env as the source of truth and backs up replaced files.

Systemd treats these as linked units. The installer enables them by their
source paths; use `just deploy` or rerun `install-service.sh` rather than
manually running `systemctl enable` against the symlink name.

`~/worker-harness` is host deployment state and is intentionally not mounted
under `/code` or into the container home. Image administration uploads to
`/var/lib/worker-harness/harness/new-image.sif`, backed on the host by
`~/.local/worker-harness/harness/new-image.sif`; the host update service then
swaps `~/worker-harness/worker-harness-worker.sif`. The container home is the
separate persistent `WH_DIR/home/<user>` tree.

For boot without login, enable user lingering:

```bash
loginctl enable-linger "$USER"
```

## Slurm-launched workers

Cluster nodes have no systemd user services and no rclone: a batch job runs
`bash <install-dir>/start-wh.sh` directly, so only the launcher and the Slurm
helpers need updating. Point the recipe at the install directory, either on the
cluster filesystem or through its mount on any worker:

```bash
just deploy-slurm /data/shared/ibex/worker-harness
just deploy-slurm /ibex/user/engeld/worker-harness --with-image
```

Each file is replaced through a temporary name in the same directory, so a job
starting mid-update reads either the old or the new file. Queued and running
jobs are untouched: the batch scripts do not change, and every new job in the
self-chaining sequence picks up the current launcher.

On these nodes the launcher binds the same collections natively:

```text
/ibex/user/<user>      → /code and /data/shared/ibex
/ibex/project/c2324    → /data/shared/ibex_c2324
```

`WH_IBEX_USER_ROOT`, `WH_IBEX_PROJECT_ROOT`, and the semicolon-separated
`WH_IBEX_PROJECTS` override the detected paths. The user filesystem is
deliberately bound twice, because it holds both the repositories and the shared
collection other workers reach through rclone. Hosts without those directories
are unaffected.

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
- `WH_EXTRA_BINDS` - semicolon-separated `host:container` bind mount pairs (default: empty). The installer manages rclone mounts here. Entries whose host source is already covered by automatic code or `/mnt` mapping are ignored; other operator entries are retained.
- `WH_CODE_ROOT` - optional host directory mounted directly at `/code`. By default the launcher chooses `$HOME/Work`, then `$HOME/Dev`; deployment creates `$HOME/Work` only when neither exists.

Data namespace identity:

- `/data/shared/<name>/...` is a deployment-managed network collection. The same full path denotes the same backing collection on every worker advertising it.
- `/data/local/<name>/...` is worker-local. Matching paths on different workers do not imply matching content.
- `list_data` advertises only immediate non-symlink directory children below each configured collection root. It does not recursively index files or advertise empty roots.

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
