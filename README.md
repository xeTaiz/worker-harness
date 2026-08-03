# Worker Harness

Worker Harness manages containerized worker nodes that register to an orchestrator over a private overlay network.

This repository targets **Tailscale + Headscale**.

## Networking model

- Workers run with `tag:wh-worker`.
- Orchestrator runs with `tag:wh-orchestrator`.
- Optional user/client nodes can run with `tag:client`.

Required ACL directions:

1. `tag:wh-worker` -> `tag:wh-orchestrator:12888` (heartbeat/register API only)
2. `tag:wh-orchestrator` -> `tag:wh-worker:*` (worker control traffic)
3. Operator/client Tailnet members -> `tag:wh-orchestrator:12889` (privileged control API, including Pi delegation)

`tag:wh-worker` must not be granted access to port `12889`; worker registration
and the operator control plane are deliberately separate services. In
production, the mobile Pi-session webapp is served by the standalone `wh-web`
container on the VPS host's Tailscale IP. `wh-web` proxies only the session UI's
HTTP, SSE, and WebSocket routes to `wh-orch:12889` over a private Docker network.
It uses the existing Tailnet trust boundary—there is no separate browser
credential. The orchestrator can still serve an explicitly configured
`WH_WEB_DIR` for local development, but its production image does not bundle the
web assets. Working/idle delegated sessions expose a **Terminal preview** tab
through the worker relay on port
`27888`. Ordinary interactive Pi sessions launched inside tmux or Zellij expose
the same tab through an auto-started host relay on the host's Tailnet port
`27888`; random terminals remain non-attachable. The host relay binds only to
loopback and uses Tailscale Serve, so grant the local operator permission once
on each interactive host. Interactive terminal attachment additionally requires
Bun plus the source multiplexer (`tmux`, or Zellij 0.44.2+); missing prerequisites
leave semantic registration available but mark raw terminal attachment unavailable.

```bash
bun --version
sudo tailscale set --operator="$(id -un)"
```

Host and delegated relays allow up to eight concurrent read-write attachments
per Pi session, matching normal single-operator tmux behavior. If a new attach
arrives at capacity, the longest-idle attachment is detached and returned to
its selector, preventing lockout when old clients do not disconnect cleanly.
Connections otherwise remain attached until the client, network, PTY, or route
disconnects. Client activity is tracked only to choose the longest-idle victim;
the most recent client resize controls the shared tmux window. Native clients
prefer the direct Tailnet relay and fall back through the orchestrator gateway;
the PWA uses the same-origin gateway first.

For a native terminal attachment, install the CLI on each operator device and
pick a session:

```bash
uv tool install --editable ~/Dev/worker-harness
wh pi start --name research          # new Pi in the hidden managed tmux backend
wh pi attach                         # interactive fzf picker
wh pi attach <id-prefix-or-name>     # select directly
```

`wh pi start` generates the internal Pi session ID, creates one single-pane
window in a dedicated status-free tmux server, waits for its exact local route,
and attaches over loopback; `--name` is only the human-facing label. The
managed backend retains 50,000 lines per new pane and enables tmux mouse mode,
so scrolling up enters tmux copy mode even through Zellij. Press `Ctrl-]` to
detach without stopping Pi. Tmux sources always stream through a
disposable relay client, including on the source host, so an unrelated outer
tmux keeps its own status and navigation. A same-client local Zellij source is
the sole direct-focus exception because streaming it recursively would render
Zellij inside itself. Remote clients prefer the direct Tailnet relay and fall
back to the orchestrator gateway. `--stream` remains as a compatibility no-op.

The companion tmux dotfiles reserve `Ctrl-a` as a Worker Harness prefix while
leaving tmux's normal `Ctrl-b` prefix unchanged: `Ctrl-a Ctrl-a` opens the
picker, `Ctrl-a Ctrl-j/Ctrl-l` cycles next, `Ctrl-a Ctrl-h/Ctrl-k` cycles
previous, and `Ctrl-a x` detaches. In Zellij, `Alt-a` and `Ctrl-a Ctrl-a` open
the picker in a floating pane. A managed/remote/delegated selection opens one
single-pane tab (`π ● name` working, `π ✓ name` idle, `π ! name` error, `π ?
name` disconnected), while reopening that session focuses its existing tab.
`Ctrl-]` closes the attachment tab without stopping Pi. Same-client plain
Zellij sources still focus their original pane. Picker order is Global, Local
(initial selection; Up selects Global), remote interactive machines, then
delegated workers. `Alt-u/y`, prefix Ctrl-j/l/h/k, and in-stream `Ctrl-^`/
`Ctrl-_` cycle through that same order. Zellij keeps its existing `Ctrl-b`
tmux-emulation mode entry as well.

Tailscale SSH policy is also required (see `headscale-policy.example.json`).

## Build images

```bash
just build          # orchestrator, worker, and wh-web
just build-orch     # orchestrator only
just build-worker   # worker only
just build-web      # wh-web only
```

Every Docker build receives three ready-to-push tags automatically:
`xetaiz/<image>:latest`, `xetaiz/<image>:<branch>`, and
`xetaiz/<image>:<branch>-<7-character-commit>`. A dirty worktree adds `-dirty`
to the commit tag so an uncommitted image cannot be mistaken for an exact
commit build. For example, `just build-orch` on clean `giga-wh` builds
`xetaiz/wh-orch:latest`, `xetaiz/wh-orch:giga-wh`, and
`xetaiz/wh-orch:giga-wh-<commit>`. Set `WH_IMAGE_NAMESPACE` to override
`xetaiz` without editing the `justfile`.

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

`install-service.sh` keeps `start-wh.sh` and `worker-harness-worker.sif` in the install directory, and creates symlinks from `~/.config/systemd/user/` and `~/.config/worker-harness/` back into that directory. The runtime env remains mutable at `~/worker-harness/.env` (linked as `~/.config/worker-harness/worker-harness.env`). Updating scripts or units therefore requires no recopy; run `systemctl --user daemon-reload` after unit changes and restart the affected service as needed. Existing copied installations can migrate once with `./migrate-to-symlinks.sh`; it preserves a regular config env as the source of truth and backs up replaced files.

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
