# Multi-user Worker Harness and agent fleet rollout

## D1. Release boundaries

The control server runs Worker Harness in `wh-orch`; it does not run OMP. Client
machines (`desktop`, `framework`, `camel`) run OMP in the `wh` Herdr session.
GPU workers remain a separate runtime: PM/task agents may run experiments on
registered workers, but only the control server has the client-machine SSH key.
The orchestrator agent has no compute actions.

Prepare immutable artifacts from the commits listed in the release report:

- `worker-harness`: control image, web image, shim and bootstrap script.
- `pi-worker-harness` and `pi-local-vault`: version 0.1.4, installed from pinned GitHub commits.
- `agent-orchestrator`: prompts, skills, templates and project registry.
- `dotfiles`: `agent-sandbox/.local/bin/agent-sandbox` and the existing OMP wrapper.

PR #4 (`--json`) is independent; this rollout neither requires nor merges it.
The queue and orchestration changes are published together on `main`. Pull each
owning repository and initialize the harness's pinned plugin submodule. Build
from a clean checkout and record the resulting image digest for rollout.
The agent-orchestrator repository still needs its GitHub remote created; until
then, transfer a `git archive` of its committed `main` to the server and desktop.

The backend, native clients, plugins and web assets are a coordinated API cutover.
Restart existing OMP sessions to load the matching plugins. Browser users need
the matching web edge for login, key management and the shared job board.
No production deployment is implied by publication or a successful local test.

## D2. Before changing services

1. Drain running CPU/GPU jobs and agent turns before activation; save active
   worktrees, pending commands and PRs. Do not auto-stop legacy work to migrate.
   The new job supervisor cannot retroactively supervise existing processes.
2. Stop the control service, then back up the existing SQLite registry with
   SQLite's backup facility (or copy the database and its journals together while
   stopped). Keep a verified backup and the old image/plugin digests for rollback.
   Do not discard history or create a replacement empty registry for an upgrade.
3. Preserve the existing registry and `/var/lib/tailscale` mounts and Tailnet
   settings exactly. Keep all host-managed code/data/storage mounts unchanged;
   this rollout does not provision user homes or change network-storage mounts.
4. Start only one new control process against the preserved registry. Schema
   migration runs on startup; bootstrap below assigns historical resources to
   the first administrator without dropping jobs or resetting pending order.
5. Drain and recreate all legacy unauthenticated Marimo listeners and forwards
   before granting users access. Existing tokenless listeners are not adopted
   as secure managed sessions. New managed notebooks use native tokens and the
   owner-authenticated HTTP/SSE/WebSocket proxy, not a direct notebook URL.

`~/worker-harness` is the installed GPU-worker payload. It is NOT the source
checkout `~/Work/worker-harness`. Do not commit or overwrite source in the
installed payload directory. Updating the GPU worker image is a separate rollout
from installing the host-side agent fleet prerequisites.

## D3. Create server-only credentials and configuration

On the actual server, create a private release/configuration directory outside
all client workspaces. Generate a NEW Ed25519 key there; do not reuse the earlier
desktop test key. Keep the private `wh_herdr` only on the server. Distribute only
its `.pub` to client installers. Replace the old test public-key entries on all
clients during cutover, then remove the old desktop test private key once it is
no longer needed. Never mount a personal `.ssh` directory into the container.

Populate a dedicated SSH directory with `wh_herdr` (mode 0600) and `known_hosts`
containing independently verified host keys for all three machine destinations.
The transport must fail on changed host keys; do not disable verification.

Set `WH_WEB_ORIGIN` in the control-service environment to one exact external
origin, for example `https://harness.example.net` (scheme, host and optional port,
no path or trailing slash). HTTPS is required except explicit development on
`localhost`, `127.0.0.1` or `[::1]`, such as `http://127.0.0.1:12889`.
Terminate HTTPS at the existing trusted ingress in front of `wh-web`; its
internal HTTP listener is not a production browser origin. Preserve Host and
Origin through that ingress. Browser cookies are Secure on HTTPS, HttpOnly and
SameSite=Strict. Harness mutations require the exact Origin and CSRF token.

There is no shared edge credential or default account. On first activation,
run this from the installed release, with the SAME `WH_DB_PATH` and service
environment as the control process (inside the container when containerized):

```sh
uv run python -m worker_harness.orchestrator bootstrap-admin --username admin
```

Enter the password twice at the interactive, no-echo prompt; never pass it in
arguments or environment variables. Bootstrap refuses a second initialization.
Until bootstrap completes, login/health/static assets may be served but protected
requests and dispatch remain denied. The immutable bootstrap administrator owns
migrated resources and alone retains the human Pi workflow.

Sign in to the web UI, create team accounts under `#admin/users`, and issue each
user's ordinary API keys from `#keys` (administrators may issue keys on behalf of
a user). Full keys are shown once. Store them outside Git, project workspaces,
browser storage and sandbox binds in a private file with mode 0600. Native clients
use `WH_ORCHESTRATOR_URL` plus `WH_API_KEY_FILE=/private/path/harness.key`, or
`WH_API_KEY`; the environment value wins over the file only outside internal Pi
roles. Internal agents use only `WH_SESSION_ROLE` and `WH_SESSION_TOKEN`, never
a human-key fallback. Remove old shared-bearer settings and secret mounts from
all services and clients; they no longer authenticate.

C1
```sh
ssh-keygen -t ed25519 -f ssh/wh_herdr -N '' -C wh-service
```

Write the server inventory:

C2
```toml
[machine.desktop]
ssh_target = "dome@desktop.hs.d0me.xyz"
home = "/home/dome"
[machine.framework]
ssh_target = "dome@framework.hs.d0me.xyz"
home = "/home/dome"
[machine.camel]
ssh_target = "engeld@camel.hs.d0me.xyz"
home = "/home/engeld"
```

Mount the pinned agent-orchestrator release read-only at `/opt/agent-orchestrator`.
Its `projects.toml` contains paths on CLIENT machines, not paths inside Docker.
Confirm every project repository exists on the configured machine and has the
expected base branch and GitHub remote. The initial worker-harness project uses
`/home/dome/Work/worker-harness` on desktop. The remote orchestrator working
directory is `/home/dome/Work/agent-orchestrator`; the local prompt source is the
separate read-only container mount.

## D4. Prepare clients, desktop first

For each client, as its intended Unix user:

1. Check Herdr 0.8.2/protocol 20, OMP 18.1.10, mise, Bubblewrap, Git and `gh`.
   Keep existing OMP/model and vault login state; roles share the normal profile.
2. Install the reviewed sandbox script and existing `~/.local/bin/omp` wrapper.
   The wrapper must invoke the sandbox; a raw mise `omp` earlier on PATH is not
   an acceptable substitute. The backend explicitly invokes the absolute wrapper.
3. Install the published plugins through OMP, using the same commit on every
   client. Do not deploy mutable source symlinks. Remove/reinstall old linked
   installations if needed. Confirm `omp plugin list` shows both at 0.1.4,
   then restart OMP sessions.

   C5
   ```sh
   omp plugin install github:xeTaiz/pi-worker-harness#147e501b2cbbb08f3bbf744473b00b5d1421f7f7 --force
   omp plugin install github:xeTaiz/pi-local-vault#e2c580affe5856fc03567d6f563a34e70ee7c858 --force
   ```
4. Install the agent-orchestrator checkout on desktop for its working directory.
   Prompts are copied from the server into private per-session cache directories
   and exposed read-only; other clients do not need a prompt checkout merely to
   run PM/task agents. Ensure project checkouts and required `~/mnt` data exist.
5. Install the shim, forced PUBLIC key and Herdr user unit with the reviewed
   bootstrap script. Run it locally on each client; privileged linger setup may
   require an administrator. It restarts the `wh` session, not `default`.
6. Set ordinary clients' `WH_ORCHESTRATOR_URL` to the HTTPS harness URL and
   `WH_API_KEY_FILE` to the user's protected key file issued from `#keys`.
   Do not put keys in the vault token directory, `~/Work`, OMP project settings,
   or dotfiles Git. Human Pi control is available only in bootstrap's web view.
7. Confirm host-side Git push and `gh` authentication without copying those
   credentials into the agent sandbox.

C3
```sh
WH_HERDR_PUBLIC_KEY=/path/to/wh_herdr.pub scripts/deploy-herdr-machine.sh local
systemctl --user is-active herdr-wh.service
loginctl show-user "$USER" -p Linger --value
omp plugin list
```

The bootstrap script handles `/usr/bin/herdr` versus `~/.local/bin/herdr`.
A successful installation requires both an active unit and `Linger=yes`.
The new session cache binding is required: deploying only the Python launch
changes without the sandbox script would hide role prompts from agents.

## D5. Replace the control server and web edge

`docker-compose.orchestrator.example.yml` is a template for an existing
named-volume deployment. If production uses bind mounts, preserve those exact
binds instead of substituting named volumes. Use the original Tailnet environment
file (`TS_HOST`, `TS_HOSTNAME`, required tags/routes and other existing settings).
Reused Tailscale state normally removes the need for a new join key.

Set the following deployment environment (paths are on the server):

- `WH_ORCHESTRATOR_IMAGE`: immutable control image tag/digest.
- `WH_WEB_IMAGE`: matching immutable web image tag/digest.
- `WH_TAILNET_ENV_FILE`: existing private Tailnet environment file.
- `WH_TAILSCALE_VOLUME`: the existing external Tailscale volume name.
- `WH_REGISTRY_VOLUME`: the preserved external registry volume name. For a truly
  new installation only, create an empty volume with `docker volume create`.
  Never use the Tailscale volume here or replace an upgrade's existing registry.
- `WH_MACHINES_FILE`: absolute path to the inventory above.
- `WH_AGENT_ORCHESTRATOR_DIR`: pinned prompt/config release directory.
- `WH_HERDR_SSH_DIR`: dedicated service-key/verified-known-hosts directory.
- `WH_WEB_ORIGIN`: exact external HTTPS browser origin, e.g. `https://harness.example.net`.
- `WH_TRUSTED_PROXIES`: comma-separated IPs/CIDRs of the web edge (e.g. the
  `WH_DOCKER_NETWORK` subnet). Only behind these peers is the nearest untrusted
  `X-Forwarded-For` hop used as the client address for login rate limiting;
  without it every browser shares the edge's address and one attacker's five
  failed logins per minute would lock out the whole team.
- `WH_FLEET_BASE_URL`: `http://orchestrator.hs.d0me.xyz:12889`.
- `WH_ORCHESTRATOR_MACHINE`: `desktop`.
- `WH_ORCHESTRATOR_CWD`: `/home/dome/Work/agent-orchestrator` on desktop.
- `WH_DOCKER_NETWORK`: existing private network (`wh-internal` by default).
- `WH_WEB_BIND_IP`: server's Tailnet IPv4 address, never `0.0.0.0`.

Stop/remove the old container through its EXISTING deployment manager before
starting the replacement; do not allow two control servers to share SQLite or
Tailscale state. Do not remove volumes. Start the new control first, check schema
migration/health/logs and complete first-run bootstrap, then start/recreate the
web edge so Nginx resolves the new container IP.

C4
```sh
docker compose -f docker-compose.orchestrator.example.yml config --quiet
docker compose -f docker-compose.web.example.yml config --quiet
docker compose -f docker-compose.orchestrator.example.yml up -d
docker compose -f docker-compose.web.example.yml up -d --force-recreate
```

The edge forwards each client's Cookie and Authorization unchanged; it never
injects identity. Keep it Tailnet-only behind HTTPS, and do not publish the control
API ports publicly. Only auth/admin, jobs/queue/usage, owned tunnels/Marimo,
sanitized read-only workers/data, and the existing Pi routes are proxied.
Unrelated `/api/` routes return 404; API responses are no-store. Control `/health`
and edge `/healthz` are public probes. Unauthenticated protected requests always
return 401, including an empty fleet/database; there is no anonymous development
fallback. Ordinary users and API keys have no Pi, account-admin or key-issuance
authority, even if the key belongs to an administrator. Administrators cannot
read, stop or execute another user's private jobs, tunnels or notebooks.

## D6. Acceptance gates before opening the fleet

Run these in order, using the actual production control path:

1. **Health/auth:** control `/health` and edge `/healthz` succeed. Unauthenticated
   requests fail. Browser login, Origin/CSRF, logout and key revocation work.
   Alice/Bob keys see shared sanitized queue rows but not each other's private
   jobs/logs/tunnels/notebooks; admin keys cannot access admin or Pi routes.
   Unrelated edge `/api/v1/_stats` returns 404. Session responses contain no hashes.
2. **Transport:** from the service runtime, verify each machine snapshot, refusal
   of an unknown verb, a leading-option branch and a path outside the allowed
   home/cache roots. Stop desktop's test Herdr unit and confirm one cold recovery.
3. **Desktop canary:** send a harmless global request in the browser. Exactly one
   orchestrator starts. Ask it to route a read-only project request; exactly one
   PM starts. Verify its role prompt and project/cwd, not only process existence.
4. **Isolation:** inside the canary, SSH agent/key, Git credentials and tailscaled
   socket are unavailable; role prompt is readable but its cache is read-only;
   human API credentials are unavailable. Expected data/vault reads work.
5. **Role/API contract:** orchestrator has no compute actions; task has no vault
   write tools or PR/worktree actions. A task cannot send to another project's PM,
   mutate its own role, or bypass role authorization by dropping its bearer.
6. **Mailbox:** ask-PM records blocked/question and queues without preemption;
   the PM answer clears blocked on prompt acknowledgement. Browser roster updates.
7. **Lifecycle:** idle PM stops only without active children/pending messages;
   next request resumes the saved conversation. Failure cache is removed; any
   worktree where an agent may have run is preserved until explicitly reviewed.
8. **PR gate:** use a reviewed, harmless task. Task commits locally; PM reviews;
   host pushes/opens a PR with six sections. Missing sections fail. Teardown with
   no PR refuses. Do not claim post-merge curation passed until a PR really merges.
9. **Remaining clients:** repeat the canary on framework, then camel, before
   enabling their project entries. Do not assume the earlier rsync copies match.

PR #4 can be reviewed separately; do not open duplicate test PRs or tell a PM
that an open PR has merged. After a real merge, observe vault/AGENTS curation and
only then tear down the task worktree containing its evidence.

## D7. Rollback

Stop new launches, drain or reconcile running work, and stop the new control/web
containers. Preserve new worktrees/branches and a backup of the migrated registry.
Restore the pre-migration database backup together with the matching old control,
web and plugin versions; never point an old image at the migrated database.
Keep the SAME Tailscale state, network identity and storage binds. Restore only
the matching edge version; keep it inaccessible to users until its security
boundary is checked. Revoke the new forced public key if abandoning the fleet
rollout. Never reset source checkouts or delete task worktrees as rollback machinery.

The sandbox hardening can remain during an application rollback. Restoring the
old sandbox would reopen the tailscaled socket exposure, so do not do so merely
to restore an older application image.

## D8. Security scope

This deployment is for a trusted team sharing one worker runtime, Unix account,
code and data. API ownership protects private remote endpoints, not shared files
or processes from arbitrary shell commands. `CUDA_VISIBLE_DEVICES` is cooperative
GPU assignment, not a device ACL or isolation boundary. Do not claim hostile-user
multi-tenancy; separate users/containers/VMs would require a different design.
Generic raw TCP tunnels are reachable on the trusted Tailnet and have no per-user
connection authentication: the forwarded service must provide its own. Managed
Marimo instead requires the owner-authenticated proxy plus its private native token.

The forced shim and Bubblewrap reduce agent capabilities, not shared-account
authority. Bash path validation has a check/use race against same-user filesystem
mutations; the service-key Herdr verb can launch panes by design. Keep human and
service credentials outside sandbox binds and retain approved project/data roots.
The direct host-relay terminal endpoint remains a separate Tailnet-trusted
bootstrap workflow, not an ordinary multi-user endpoint. Restrict its reachability.
Likewise, hiding vault write tools does not reduce the scope of the underlying
vault token: use a read-scoped credential if arbitrary task HTTP requests must
also be unable to write. Do not treat tool registration as service authorization.
