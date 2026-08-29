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
capture the prepared interactive shell's host runtime before launching Pi:

```bash
uv tool install --editable ~/Dev/worker-harness
wh host setup                        # capture wh/agents/Bun/Node/tmux/Tailscale paths
wh host doctor                       # validate from a clean SSH-like environment
wh start --agent omp --name research # new omp in the hidden managed tmux backend
wh attach                            # interactive fzf picker across agents
wh attach <id-prefix-or-name>        # select directly
wh launch                            # machine/cwd → running/history/new picker
wh resume <exact-id> --cwd /repo     # identity-safe target-local Pi resume
```

`uv tool install` intentionally cannot run project post-install hooks. On an
ordinary fleet host, chain the explicit setup step after a pinned install:

```bash
uv tool install --force --reinstall \
  'git+ssh://git@github.com/xeTaiz/worker-harness.git@<commit>' \
  && wh host setup
```

`wh host setup` writes a private, atomic
`~/.config/worker-harness/host-runtime.json` manifest. It records the absolute
executables and stable PATH needed by non-interactive SSH launches and managed
tmux panes, including Pi's `#!/usr/bin/env node` dependency, omp when present,
and the Bun path used to start the host relay. It does not edit shell profiles.
Rerun setup after moving or upgrading Node, Bun, Pi, omp, tmux, Tailscale, or
the `wh` installation;
`wh host doctor` reports stale paths and exits nonzero. Set
`WH_HOST_RUNTIME_CONFIG` only when an alternate manifest path is required.

`wh start` creates one single-pane window in a dedicated status-free tmux
server, waits for its exact local route, and attaches over loopback. Pi accepts
the generated session ID; omp chooses its own ID, which `wh` resolves from the
registered tmux pane. `--name` is the human-facing label. The
managed backend retains 50,000 lines per new pane and enables tmux mouse mode,
so scrolling up enters tmux copy mode even through Zellij. Press `Ctrl-]` to
detach without stopping Pi. Tmux sources always stream through a
disposable relay client, including on the source host, so an unrelated outer
tmux keeps its own status and navigation. A same-client local Zellij source is
the sole direct-focus exception because streaming it recursively would render
Zellij inside itself. Remote clients prefer the direct Tailnet relay and fall
back to the orchestrator gateway. `--stream` remains as a compatibility no-op.

The companion tmux dotfiles reserve `Ctrl-a` as a Worker Harness prefix while
leaving tmux's normal `Ctrl-b` prefix unchanged. `Ctrl-a Ctrl-a` opens a
transient popup picker, then creates or focuses one dedicated WH-owned window by
exact Pi UUID. `Ctrl-a Ctrl-s` opens the same transient handoff for `wh launch`;
running-session attach, history resume, and new-session launch all finish in the
same dedicated/reused window rather than inside the popup. Only that invoking
tmux client is switched. The window title and Catppuccin status entry retain the
state glyph and use blue/green/red/gray for
working/idle/error/disconnected; ordinary windows are untouched. `Ctrl-a
Ctrl-j/Ctrl-l` cycles next, `Ctrl-a Ctrl-h/Ctrl-k` cycles previous, and `Ctrl-a
x` detaches. A dedicated attachment retries bounded unexpected transport
closures; `Ctrl-]` remains an intentional close. Transport failures report the
direct/gateway path, duration, fallback use, and close code/reason. In Zellij,
`Alt-a` and `Ctrl-a Ctrl-a` open
the picker in a floating pane. A managed/remote/delegated selection opens one
single-pane tab (`π ● name` working, `π ✓ name` idle, `π ! name` error, `π ?
name` disconnected), while reopening that session focuses its existing tab.
`Ctrl-]` closes the attachment tab without stopping Pi. Same-client plain
Zellij sources still focus their original pane. Picker order is Global, Local
(initial selection; Up selects Global), remote interactive machines, then
delegated workers. `Alt-u/y`, prefix Ctrl-j/l/h/k, and in-stream `Ctrl-^`/
`Ctrl-_` cycle through that same order. Zellij keeps its existing `Ctrl-b`
tmux-emulation mode entry as well.

Inside a Herdr pane, `wh attach` uses the same native protocol-v2 stream and
reports the selected Pi/OMP session plus host metadata to Herdr's Agent sidebar.
Working/idle lifecycle comes from the Worker Harness session stream rather than
Herdr screen matching. The client re-applies its current pane dimensions after
the relay backend becomes ready, so Herdr startup and dynamic pane resizes reach
the source TUI. `Ctrl-]` releases Worker Harness lifecycle authority and
metadata before returning to the pane's shell. Managed `wh start` sessions do
not inherit the outer Herdr pane identity; the visible attachment client alone
owns that sidebar projection.

After `wh launch` selects a machine and cwd, its interactive action picker shows
active Worker Harness sessions, inactive target-local Pi histories, and Start
new. Active sessions attach without relaunching. Previous sessions are listed
through the installed Pi `SessionManager.list(cwd)` API, which requires Pi
`>=0.83.0,<1.0.0`; opaque IDs are re-resolved on the target and refused if
already active before Pi is invoked with exact `--session`. Stored names are
preserved by default. SSH errors include the destination and failing phase, and
all operator-controlled paths, names, IDs, and Pi arguments remain argv/shell
quoted.

Tailscale SSH policy is also required (see `headscale-policy.example.json`).

## Build images

```bash
just build          # orchestrator, worker, wh-web, and wh-router
just build-orch     # orchestrator only
just build-worker   # worker only
just build-web      # wh-web only
just build-router   # stateless Pi routing classifier only

just push           # build and push all four images
just push-orch      # build and push the orchestrator only
just push-worker    # build and push the worker only
just push-web       # build and push wh-web only
just push-router    # build and push the router only
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

## Run the global semantic router

The global UI routes operator messages only to active interactive Pi sessions.
Explicit recipients bypass classification; Auto uses the private `wh-router`
sidecar and includes the previous successful route only while it is less than
three minutes old. Every dispatch uses Pi steering, which starts an ordinary
turn when the recipient is idle. The UI records and displays the latest
classification latency.

`wh-router` has no published port or Tailnet identity. It joins `wh-internal`
and uses a dedicated copy of the operator's Pi auth/model configuration. The
directory is writable because OAuth refresh and credential-store locking need
to update it; do not mount the live interactive-agent directory into two
writers:

```bash
install -d -m 0700 "$HOME/.pi/wh-router-agent"
cp -a "$HOME/.pi/agent/." "$HOME/.pi/wh-router-agent/"
export WH_PI_ROUTER_AGENT_DIR="$HOME/.pi/wh-router-agent"
export WH_DOCKER_NETWORK=wh-internal
docker compose -f docker-compose.router.example.yml up -d --build
```

The `wh-orch` container must join the same network and use:

```text
WH_PI_ROUTER_URL=http://wh-router:12900
```

The router model and thinking level are selected from the Global web view and
persist in orchestrator SQLite. Initial intended comparisons are
`openai-codex/gpt-5.3-codex-spark` and `openai-codex/gpt-5.6-luna`; all models
reported as available by the mounted Pi configuration are selectable. The
sidecar receives no filesystem or Worker Harness tools and starts every
classification from a fresh one-message context.

Global **Interrupt** matches Pi's normal Escape behavior through the bridge's
`ctx.abort()`: queued messages are restored into the target Pi editor and the
current operation is aborted. It does not terminate Pi or undo completed tool
side effects.

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

The successful deployment retains the previous installation at
`~/worker-harness.backup.<transaction>`, including a `.deployment-state`
snapshot. Pending update/restart triggers are retained there rather than
running immediately after deployment. Remove the backup manually after the
worker has been observed in production.

During installation, rclone comes from the official
`https://rclone.org/install.sh` script when it is absent or lacks the SMB
backend. Working mounts use `/data_shared`, `/data_ibex`, and
`/data_ibex_c2324`.

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
- `WH_EXTRA_BINDS` - semicolon-separated `host:container` bind mount pairs (default: empty). The installer manages rclone mounts here. Entries whose host source is already managed by automatic home or `/mnt` mapping are ignored to prevent duplicate container trees; other operator entries are retained.
- `WH_MOUNT_HOME_FOLDERS` - set to `0` to disable mapping non-hidden directories from `$HOME` to `/code/<directory>` (default: `1`). `$HOME/mnt`, the live deployment directory, deployment backups/failures, and hidden directories are excluded. Direct host mountpoints at `/mnt` or `/mnt/<name>` are independently mapped in lexical order to `/data`, `/data2`, `/data3`, and so on.

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
