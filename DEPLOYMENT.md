# Hierarchical agent fleet rollout

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

The backend and plugins are a coordinated API cutover. Restart existing OMP
sessions to load the new plugins. The web container is optional: leave the old
one stopped and install the matching version when needed.
No production deployment is implied by publication or a successful local test.

## D2. Before changing services

1. Drain agent turns and record active worktrees, pending commands and PRs.
   Stop the old router and delegated-agent runtimes after saving any work.
2. Stop the control service. This rollout intentionally starts with a fresh
   SQLite registry: existing session/job history may be discarded. Use a new
   empty registry volume rather than reusing the old database and its journals.
   Workers and restarted bridges will register again; queued jobs are not restored.
3. Preserve the existing `/var/lib/tailscale` mount and Tailnet settings exactly.
   A fresh database must not create a new Tailnet identity.
4. Keep old image digests for application rollback. Keeping the old registry
   volume is optional, not a migration requirement. Never delete project
   branches/worktrees or the Tailscale state as part of the database reset.

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

Generate an operator secret with 32 random bytes encoded as base64url. Store it
in a private file, not Git, a project workspace, shell history or a browser.
Both the control container and the web edge mount the same file. Human OMP/CLI
clients receive a protected copy at `~/.config/worker-harness/operator.token`,
which is outside the fleet sandbox binds. Set `WH_OPERATOR_TOKEN_FILE` for
ordinary human clients, not for fleet agents. Fleet launches remove inherited
operator credential environment variables and use their own session token.

C1
```sh
python -c 'import secrets; print(secrets.token_urlsafe(32))' > operator.token
chmod 600 operator.token
ssh-keygen -t ed25519 -f ssh/wh_herdr -N '' -C wh-service
```

`wh-web` runs as UID 101. Arrange a read-only secret file mount readable by UID
101 but protected from other host users (for example a root-owned 0700 parent
directory containing the mounted 0444 secret file). Plain Compose bind secrets
do not reliably honor secret `uid`/`mode` overrides; verify file readability
inside the container. The backend runs as root and uses the same token file.

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
6. Set the ordinary client `WH_ORCHESTRATOR_URL` to the control Tailnet URL and
   `WH_OPERATOR_TOKEN_FILE` to its protected token file. Do not put the token
   in the vault token directory, `~/Work`, OMP project settings, or dotfiles Git.
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
- `WH_REGISTRY_VOLUME`: a newly created empty volume for this rollout; create it
  with `docker volume create` before starting Compose. Never use the Tailscale
  volume here.
- `WH_MACHINES_FILE`: absolute path to the inventory above.
- `WH_AGENT_ORCHESTRATOR_DIR`: pinned prompt/config release directory.
- `WH_HERDR_SSH_DIR`: dedicated service-key/verified-known-hosts directory.
- `WH_OPERATOR_TOKEN_FILE`: server operator-secret file.
- `WH_FLEET_BASE_URL`: `http://orchestrator.hs.d0me.xyz:12889`.
- `WH_ORCHESTRATOR_MACHINE`: `desktop`.
- `WH_ORCHESTRATOR_CWD`: `/home/dome/Work/agent-orchestrator` on desktop.
- `WH_DOCKER_NETWORK`: existing private network (`wh-internal` by default).
- `WH_WEB_BIND_IP`: server's Tailnet IPv4 address, never `0.0.0.0`.

Stop/remove the old container through its EXISTING deployment manager before
starting the replacement; do not allow two control servers to share SQLite or
Tailscale state. Do not remove volumes. Start the new control first, check health
and logs, then start/recreate the web edge so Nginx resolves the new container IP.

C4
```sh
docker compose -f docker-compose.orchestrator.example.yml config --quiet
docker compose -f docker-compose.web.example.yml config --quiet
docker compose -f docker-compose.orchestrator.example.yml up -d
docker compose -f docker-compose.web.example.yml up -d --force-recreate
```

The edge injects the operator bearer upstream; it never sends the secret to the
browser. Keep the edge Tailnet-only: anyone allowed to reach it has operator
control. API ports must not be published publicly. `/health` is intentionally
public for health probes. Privileged API requests without credentials return 401
when operator authentication is configured or a machine fleet exists. Empty
fleet/no-secret mode remains only for legacy local development.

## D6. Acceptance gates before opening the fleet

Run these in order, using the actual production control path:

1. **Health/auth:** control `/health` and edge `/healthz` succeed. Unauthenticated
   control session/compute requests fail; the ordinary operator client succeeds.
   Session list/detail/SSE responses contain no `token_hash`.
2. **Transport:** from the service runtime, verify each machine snapshot, refusal
   of an unknown verb, a leading-option branch and a path outside the allowed
   home/cache roots. Stop desktop's test Herdr unit and confirm one cold recovery.
3. **Desktop canary:** send a harmless global request in the browser. Exactly one
   orchestrator starts. Ask it to route a read-only project request; exactly one
   PM starts. Verify its role prompt and project/cwd, not only process existence.
4. **Isolation:** inside the canary, SSH agent/key, Git credentials and tailscaled
   socket are unavailable; role prompt is readable but its cache is read-only;
   operator token environment/file is unavailable. Expected data/vault reads work.
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

Stop new launches and stop the new control/web containers. Preserve any new
worktrees/branches. Restore the old control image with an empty registry (or the
old registry volume, if retained), and restart compatible client plugins.
Keep the SAME Tailscale state and network identity. Leave the web edge stopped
unless restoring its matching version. Revoke the new forced public key if
abandoning the fleet rollout. Never reset source checkouts or delete task worktrees
as rollback machinery. Intentionally discarded database history is not recoverable.

The sandbox hardening can remain during an application rollback. Restoring the
old sandbox would reopen the tailscaled socket exposure, so do not do so merely
to restore an older application image.

## D8. Security scope

The forced shim and Bubblewrap reduce capabilities; they are not adversarial
multi-user isolation. Agents share the client Unix account and approved writable
project roots. Bash path validation has a check/use race against concurrent
same-user filesystem mutations; the service-key Herdr verb can launch panes by
design. Keep service and operator credentials outside every sandbox bind and
restrict the web edge to trusted operators. Hardened separate users/VMs would be
needed for mutually untrusted agents, not just these role prompts.

The preserved direct host-relay terminal endpoint is a separate, pre-existing
network capability; the new operator bearer protects the control gateway, not
that direct endpoint. Restrict host-relay reachability to trusted operators.
Likewise, hiding vault write tools does not reduce the scope of the underlying
vault token: use a read-scoped credential if arbitrary task HTTP requests must
also be unable to write. Do not treat tool registration as service authorization.
