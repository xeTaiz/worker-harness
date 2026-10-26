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

## GPU job queue

PM/task agents and operators can enqueue named jobs through `POST /api/v1/jobs/queue`,
inspect them with `GET /api/v1/jobs/queue`, and update pending jobs through
`PATCH /api/v1/jobs/{job_id}/queue`. The plugin exposes `enqueue`,
`update_queued_job`, and `list_queue`.

Queues are durable and FIFO per worker: a multi-GPU head job waits until its
requested capacity is available rather than allowing smaller jobs to bypass it.
Claimed jobs receive explicit `CUDA_VISIBLE_DEVICES` assignments. Ambiguous SSH
launch results keep those GPUs reserved while recovery checks durable worker
artifacts; they are not immediately retried or treated as free capacity.

Run one control process against the registry. Immediate jobs are outside queue
reservations, and external GPU use is detected through worker telemetry. Do not
delete worker job artifacts while recovering an interrupted launch.

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

Cluster targets are local client configuration, not idle workers. Discover them
through ordinary `list_workers` / `get_worker`, then `enqueue` on
`cluster:<profile-id>` with a shared `slurm.project_path`, explicit bounded
`walltime_seconds`, and optional tested `environment_path`. CPU preparation is
another ordinary queued job on a CPU target; wait for it before submitting GPU
work. The client validates fresh Slurm capacity, reserves a real OFFLINE worker,
persists its ordinary initial queued job, and only then submits one finite batch
allocation through the user's local SSH. No login-node service or container is
required. SSH uses configured aliases and requires working noninteractive
authentication and verified host keys.

The allocation uses the standard WH SIF and existing daemon/queue. It starts
only for attached useful work, preserves logs/results under
`<shared_root>/.wh-slurm/<worker_id>`, and exits when work finishes (or after the
configured bounded failure grace). Diagnose with ordinary `exec` and rerun with
`enqueue` while the allocation remains available; a finished allocation requires
a fresh enqueue. Checkpoint signals request cooperative workload handling, not
proof that a checkpoint was saved. Preemption can provide less or no warning.
The wrapper never submits successors or requeues, and refuses repeated launches.
It removes only its staged enrollment and worker-token copies on exit, retaining
shared workload output and runtime evidence.

Use the same existing shared filesystem on the cluster and at least one WH
worker, exposed at `/data/shared/<storage_id>`. Matching path spelling alone is
not evidence of shared identity; enqueue checks a random probe through ordinary
worker exec and local cluster SSH. Prepare source, lockfiles, data, and uv
environments through ordinary file/exec tools on that storage, not a copied venv
or an experiment-specific SIF. See the configuration below for shared binds.

After changing root `start-wh.sh` or `slurm/wh-allocation.sh`, run
`just package-slurm-client` to regenerate the extension's deterministic
`slurm/runtime-assets.json`. Only these two small root-owned scripts are staged
per allocation; the configured shared SIF and project/environment stay in place.

### Allocation runtime and enrollment security

Slurm mode is selected only by the reserved `WH_SLURM_WORKER_ID` and the staged
`WH_SLURM_SETTINGS_FILE`. It uses the same worker image, entrypoint, daemon,
Tailscale SSH, tmux, and ordinary queue as fleet workers, not a separate
execution service. Each allocation owns `<host_run_path>/runtime`, visible as
`<run_path>/runtime` in the container. Its Tailscale hostname is `whc-<worker_id>`;
its registration name is the exact `worker_name` recorded in the reservation
settings. The launcher forces one-shot `run --cleanenv`, ignores ambient env
files, and neither creates nor attaches a writable overlay. Slurm requires a
modern Apptainer/Singularity supporting `--contain` and
`--no-mount=cwd,hostfs,bind-paths`; unsupported flags fail visibly rather than
exposing ambient host mounts. Only configured shared roots and explicit extra
binds are added, alongside the allocation's isolated home/runtime; the login
node's code/home collections, CWD, `/mnt`, and administrator-configured host
binds are not inherited. The private container `/tmp` is shared by its daemon
and Tailscale SSH children, not by other allocations. Fleet startup keeps its
existing env-file, automatic bind, overlay, and instance defaults.

`TS_AUTHKEY_FILE` and `WH_WORKER_TOKEN_FILE` name staged secret files, never
credential values in container arguments or settings. Both have read-only
explicit binds and private run-directory permissions. Enrollment uses
`tailscale up --auth-key=file:PATH --advertise-tags=tag:wh-cluster-job`.
Provision a **non-reusable, ephemeral** enrollment key restricted to that tag
out of band; do not reuse a fleet/orchestrator enrollment key. Never stage
operator credentials or forward the submitting user's SSH keys into an
allocation. The wrapper removes its known credential copies on exit, but
shared storage and its owner remain trusted; tags are not multi-user isolation.

Each allocation has independent Tailscale state under its runtime directory.
Private short sockets use container-local `/tmp/wh-ts-<id-hash>` and
`/tmp/wh-tmux-<id-hash>` (0700) so long shared-storage paths do not exceed Unix
socket limits. `tailscaled --port=0` avoids fixed UDP ports; its localhost
SOCKS listener is selected dynamically and startup bind failures retry at most
five times. The wrapper's 180-second startup deadline includes container startup
and potentially hanging enrollment. A Tailnet IP alone is not worker readiness:
authenticated registration, resource mapping, and the orchestrator's actual
Tailscale SSH probe must succeed before queue dispatch.

GPU mode requires and preserves Slurm's original `CUDA_VISIBLE_DEVICES` across
`--cleanenv` as `WH_SLURM_CUDA_VISIBLE_DEVICES`; the daemon resolves assigned CUDA
device UUIDs and rejects missing, ambiguous, or unsupported MIG mappings. CPU
mode omits `--nv`, skips NVIDIA/CUDA setup and readiness, reports zero GPUs, and
forces an empty CUDA mask for launches. CPU and memory reports use allocation
limits, not the whole shared node. Jobs retain structured runner output and
descendant-exit evidence beneath `runtime/harness/<job_id>`; missing contact or
a printed `EXIT:` line is not completion proof.

`headscale-policy.example.json` adds a restricted `tag:wh-cluster-job` example:
cluster jobs can reach only orchestrator registration port **12888**, and the
orchestrator can reach cluster workers on Tailnet SSH port **22**. They receive
no permission for privileged control port **12889** and cannot contact one
another through this tag. Issue ephemeral tagged keys separately from ACL
configuration. Review existing broader site rules before applying any example;
the client and runtime never change live Tailnet policy automatically.


### Legacy allocation cutover prerequisite

Before enabling finite enqueue on a cluster with legacy self-chaining jobs, the
operator must identify the **exact owned pending successor job IDs**, verify
their user/dependency/identity, cancel those IDs explicitly, and disable external
resubmitters (including any cron/scrontab or other launch automation). Let running
useful work drain or finish. Do not use broad name-based `scancel`, and do not
cancel unrelated jobs. Removing old repository scripts does not stop batch
scripts already submitted to Slurm. Development and local tests never perform
this live cutover automatically.

### Configured shared storage and local cluster profiles

`start-wh.sh` accepts `WH_SHARED_ROOTS_JSON`, a JSON object mapping a stable
`storage_id` to an **existing absolute host directory**. Configure it on the
cluster and on every participating worker, using each host's actual mount path:

```bash
# Cluster host:
export WH_SHARED_ROOTS_JSON='{"ibex":"/cluster/shared/user"}'
# A participating worker mounting that same filesystem elsewhere:
export WH_SHARED_ROOTS_JSON='{"ibex":"/mnt/cluster-user"}'
```

Both expose that collection as `/data/shared/ibex`. This does not mount a network
filesystem: the host mount is a prerequisite. Storage IDs match
`[A-Za-z0-9][A-Za-z0-9_-]*`. Invalid JSON, duplicate IDs, non-directory/missing
paths, relative paths, control characters, or bind delimiters (`:`, `;`, `,`)
fail startup; symlinks are resolved and their targets validated. Omit the
variable, leave it empty, or use `{}` for no explicit mappings.
Each configured entry joins the existing bind manifest used for data discovery.
It overrides an automatically detected IBEX bind with the same storage ID, while
other automatic binds and unrelated `WH_EXTRA_BINDS` entries remain intact.
Existing managed-source suppression still avoids redundant `WH_EXTRA_BINDS`.

Local profile configuration is read from `WH_CLUSTER_CONFIG`, or
`${XDG_CONFIG_HOME:-$HOME/.config}/worker-harness/clusters.json`. A missing file
means no configured cluster targets; malformed configuration is an explicit
error. The format is `{ "version": 1, "clusters": [...] }`, for example:

```json
{
  "version": 1,
  "clusters": [{
    "id": "ibex-gpu",
    "description": "Example GPU account; confirm site preemption and utilization limits.",
    "ssh_host": "ibex",
    "storage_id": "ibex",
    "shared_root": "/cluster/shared/user",
    "sif_path": "/cluster/shared/user/worker-harness/worker-harness-worker.sif",
    "compute_kind": "gpu",
    "gpu_type": "a100",
    "runtime": "apptainer",
    "runtime_setup": "module load apptainer",
    "account": "YOUR_ACCOUNT",
    "partition": "YOUR_GPU_PARTITION",
    "max_gpus": 1,
    "max_walltime_seconds": 600,
    "failure_grace_seconds": 0,
    "termination_grace_seconds": 20,
    "tailnet_key_file": "/home/YOU/.config/worker-harness/cluster-tailnet.key",
    "tailnet_login_server": "https://YOUR_TAILNET_CONTROL",
    "orchestrator_host": "YOUR_ORCHESTRATOR"
  }]
}
```

These are illustrative paths, capacity, account, partition and runtime setup,
not detected IBEX settings. `ssh_host` is a local SSH alias; credentials stay
local. `runtime_setup` is a trusted shell prelude, not untrusted workload input.
`storage_id` defaults to `id`, and
`WH_SHARED_ROOTS_JSON[storage_id]` on the cluster must match `shared_root`.
Profiles for different partitions on one filesystem use the same `storage_id`
and `shared_root`, including a separate CPU preparation profile with
`compute_kind: "cpu"`, `max_gpus: 0`, no `gpu_type`, and the actual configured
CPU partition. Optional `qos`, `constraint`, `max_cpus` and `max_memory_mb` describe
site policy. `warning_seconds`, `orchestrator_registration_port` and
`poll_seconds` default to 120, 12888 and 15 respectively.
Set the required `failure_grace_seconds` to 0 when idle allocation holding is
forbidden; document preemption, utilization and account restrictions in
`description`.

#### Verify storage identity, not just path spelling

Use `list_data` to find an existing participating worker (a nonempty project
directory makes the collection discoverable). Matching `/data/shared/ibex`
paths alone are not proof that it and the cluster see the same files. Run this
through **ordinary WH exec** on that worker:

```bash
python3 - <<'PY'
import json
import secrets
from pathlib import Path

probe = Path("/data/shared/ibex") / (".wh-storage-probe-" + secrets.token_hex(16))
token = secrets.token_hex(32)
with probe.open("x") as handle:
    handle.write(token + "\n")
print(json.dumps({"probe_name": probe.name, "token": token}))
PY
```

Copy the returned random name and token into the local client shell, then read
the exact file through the profile's **local cluster SSH**, not through a worker
SSH hop. For the example mapping above:

```bash
probe_name='PASTE_RETURNED_PROBE_NAME'
expected='PASTE_RETURNED_TOKEN'
actual=$(ssh -o BatchMode=yes -o StrictHostKeyChecking=yes -o ConnectTimeout=10 \
  ibex "cat -- '/cluster/shared/user/$probe_name'") &&
  test "$actual" = "$expected"
```

Require that exact token comparison to succeed before reusing the storage.
Regardless of success, remove the specific probe through ordinary WH exec on
the same worker (`rm -- /data/shared/ibex/RETURNED_PROBE_NAME`). A failed read or
different token is a mount/identity error, not permission to assume equivalence.

#### Reuse the standard SIF and project-local uv environments

Prepare and test projects through ordinary exec on a participating worker under
`/data/shared/<storage_id>/projects/...`. The standard WH SIF already includes
`uv`; ML/CUDA experiment dependencies belong in the project's `pyproject.toml`
and `uv.lock`, not in a newly rebuilt image for each experiment.

Use the SHA-256 of the **exact SIF used by that worker** (obtain it with
`sha256sum` on its host image), the lockfile hash, architecture and container
Python major/minor as the environment key. For example, inside that SIF:

```bash
set -eu
cd /data/shared/ibex/projects/my-project
SIF_SHA256='PASTE_SHA256_OF_THE_EXACT_WORKER_SIF'
lock_sha256=$(sha256sum uv.lock | cut -d ' ' -f 1)
architecture=$(/usr/bin/python3 -c 'import platform; print(platform.machine())')
python_version=$(/usr/bin/python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
export UV_PROJECT_ENVIRONMENT="$PWD/.venvs/$SIF_SHA256-$lock_sha256-$architecture-$python_version"
export UV_CACHE_DIR=/data/shared/ibex/.cache/uv
export UV_LINK_MODE=copy
mkdir -p "$PWD/.venvs" "$UV_CACHE_DIR"
(
  flock -x 9
  # A completed keyed environment is immutable; do not sync it during a run.
  if [ ! -f "$UV_PROJECT_ENVIRONMENT/.wh-uv-ready" ]; then
    uv sync --locked --python /usr/bin/python3
    touch "$UV_PROJECT_ENVIRONMENT/.wh-uv-ready"
  fi
) 9>"$UV_PROJECT_ENVIRONMENT.lock"
"$UV_PROJECT_ENVIRONMENT/bin/python" -c 'import sys; print(sys.executable)'
```

Keep the lockfile/worktree unchanged throughout preparation; the adjacent
`flock` serializes creators, and the readiness marker is written only after a
successful sync. Use the resulting environment directly for runs, without an
implicit `uv run` resync. Never modify an environment used by a running job:
changed code/dependencies use a separate shared worktree/output directory and a
new keyed environment. Keep the SIF, Python, architecture and absolute container
paths identical wherever the environment is reused; hashes are compatibility
evidence, not an image or dependency snapshot service.

A test worker need not mount this filesystem. Transfer the relevant source,
lockfile and data to a participating worker using existing `upload_file` or
`data_copy`, then prepare there. CPU preparation can also be an ordinary queued
job on a configured CPU target when cluster enqueue is available; wait for it
to finish before dependent GPU work. There is no separate preparation API or
dependency scheduler. **Do not copy a venv** built at another absolute path or
architecture. Projects without uv supply their own tested environment. This
Slurm runtime release requires a one-time SIF rebuild from the updated image
definition: older images contain the old entrypoint and worker daemon and do not
support this allocation workflow. Thereafter, rebuild for image runtime or base
dependency changes, not individual experiments. Retain logs/checkpoints in
separate shared output directories after jobs exit.

## Worker container env vars

Required:

- `TS_AUTHKEY` or `TS_AUTHKEY_FILE` - Headscale/Tailscale auth key, or readable mounted key file (Slurm requires the file form)
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
- `WH_EXTRA_BINDS` - semicolon-separated `host:container` bind mount pairs (default: empty). The installer manages rclone mounts here. Entries whose host source is already covered by automatic code, `/mnt`, or configured shared-root mapping are ignored; other operator entries are retained.
- `WH_SHARED_ROOTS_JSON` - JSON object mapping `storage_id` to existing absolute host directory, bound at `/data/shared/<storage_id>` and recorded in the bind manifest (default: `{}`). Invalid entries fail startup; see the shared-storage workflow above.
- `WH_CODE_ROOT` - optional host directory mounted directly at `/code`. By default the launcher chooses `$HOME/Work`, then `$HOME/Dev`; deployment creates `$HOME/Work` only when neither exists.

Slurm launches additionally receive the wrapper-owned `WH_SLURM_WORKER_ID`,
`WH_SLURM_SETTINGS_FILE`, `WH_WORKER_TOKEN_FILE`, and isolated `WH_DIR`.
`WH_SLURM_COMPUTE_KIND` and `WH_SLURM_CUDA_VISIBLE_DEVICES` are set by the launcher
from the settings and original allocation environment; they are not overrides
for requesting resources. In this mode `WH_OVERLAY`, `WH_LAUNCH_MODE`,
`TS_SOCKS5_ADDR`, `WH_PROXY`, and `TS_EXTRA_ARGS` cannot override the finite
launch, dynamic networking, or enrollment tag policy.

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

### Reserved Slurm workers and job events

Slurm allocations use ordinary worker rows and the ordinary job queue. The
control API reserves an `offline` worker with
`POST /api/v1/workers/reservations`; the client then queues its initial task
with `submission_key` equal to the reservation's `request_id`. Repeating an
identical request returns the same worker/job; conflicting key reuse returns
409. Reservation and scheduler-observation routes require a valid operator or
compute-session bearer even when ordinary anonymous development access is enabled.

Only the reservation's scoped token can register that worker on port 12888.
The server stores its SHA-256 hash, checks immutable allocation capacity and
ordered GPU UUIDs, and requires a successful orchestrator-to-worker SSH probe
before dispatch. The scoped token cannot authorize control API calls. Ordinary
worker registration is unchanged. CPU reservations admit zero-GPU queued jobs
and execute them serially; zero-GPU queue requests remain invalid on ordinary
workers and GPU allocations. Diagnostic immediate jobs do not extend failure grace.
Grace is measured from the failed runner's recorded finish time. Only an actual
start of newer queued work clears it—not admission or a queue claim. Durable
start/finish evidence prevents delayed results or reconnects from restarting an
older grace period; same-boot ordering uses monotonic timestamps.

Job-scope cancellation of an empty allocation uses `cancel_requested: true`
with `cancel_if_empty: true` on the observation route. The server atomically
declines it if useful queued/running work appeared or startup already occurred;
the client scancels only when the returned `cancel_requested` is true.
Explicit allocation-scope cancellation closes admission unconditionally.

`PATCH /api/v1/workers/{id}/slurm` records timestamped scheduler observations
and validated recovered job results. Terminal accounting cannot be undone by
an older poll or overwrite an already confirmed job result. Reserved workers
and their job history survive stale-worker pruning. Draining or finished
allocations reject new work with `409 SLURM_WORKER_FINISHED`; enqueue a fresh
allocation rather than assigning work to retained history.

Ordinary job creation accepts `notify_recipient_id`; cluster jobs inherit their
worker's default when omitted. Authenticated
`GET /api/v1/job-events?recipient_id=...&after=N` replays and tails persisted SSE
events with sequence cursors and keepalives. Job transitions and events commit
together. Recipients are single-user routing identifiers, **not privacy or
ownership boundaries**. Delivery does not acknowledge agent action, and failure
deadlines never wait for a client. The separate multi-user design is unchanged.

## Runtime requirements

- **Orchestrator container:** requires `/dev/net/tun` + `NET_ADMIN`.
- **Worker container:** uses Tailscale userspace networking.

See also:
- `specs/TAILSCALE.md`
- `docker-compose.tailscale.example.yml`
- `headscale-policy.example.json`
