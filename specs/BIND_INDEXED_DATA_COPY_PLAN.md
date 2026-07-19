# Bind-Indexed Data Discovery and Secure Copy Plan

**Status:** Proposed — implementation sequence

## Decision

Worker Harness data sharing will use host-mounted data plus direct, ephemeral
rsync copies.  It will not use rclone/FUSE inside worker SIFs, static
Apptainer FUSE mounts, rclone SFTP serving, or worker-to-worker Tailscale SSH.

The model is:

```text
host NAS / host rclone mount / host-local data
        │
        └── WH_EXTRA_BINDS (host path → chosen container path)
                                      │
                                      ▼
                                worker sees path
                                      │
                   heartbeat advertises immediate directories
                                      │
                                      ▼
                         agent finds source worker + path
                                      │
                                      ▼
                    wh_data_copy(source, path, target, path)
                                      │
             temporary read-only rsync daemon + Tailnet TCP forwarding
                                      │
                                      ▼
                         target materializes an rsync copy
```

Host-side mounts remain the operator's responsibility.  A host that has NAS
credentials may use rclone, native SMB, or any other mechanism; its credentials
and rclone configuration never enter the SIF.  A worker only sees the resulting
bind-mounted path.

## Security model

- No normal worker receives peer SSH access.
- Normal worker-to-worker ACL is restricted to a reserved TCP data range
  (`22000-22999`) only.
- Each copy creates a temporary, read-only rsync daemon exposing exactly the
  requested source path, with a random one-transfer credential and an expiry.
- The destination connects with `tailscale nc` through its existing userspace
  Tailscale socket; it does not need a kernel TUN device or peer SSH.
- The orchestrator remains the control-plane coordinator.  It starts and
  cleans up source/target helpers using its existing Tailscale SSH authority.
- The worker registration endpoint and privileged orchestration API must be
  separated before this facility is relied on across different trust domains.

This supersedes the rclone/FUSE design in `SHARED_DATA_ACCESS.md`; that file is
removed in Phase 1.

## Preconditions and operational recovery

1. KW60898 must be stable before another Worker Harness deployment.
2. Remove the FUSE probe marker if present:
   ```bash
   rm -f ~/.local/worker-harness/data/fuse-probe.request
   ```
3. No FUSE experiment, image swap, or launcher change is made while the worker
   is in a restart loop or loop-device constrained state.
4. The corrected worker update service/path unit must be active on target
   workers before a new SIF is deployed.
5. Current leaked loop devices are a host-maintenance concern.  This plan must
   not depend on cleaning them before normal copy-only functionality works.

## Phase 1 — Remove in-SIF rclone/FUSE work

### Goal

Return the worker image and launcher to a FUSE-free, credential-free state.
The existing rclone/FUSE implementation was exploratory and must not remain as
a latent capability or deployment risk.

### Remove

- `worker_container/wh-data-serve`
- `worker_container/wh-data-receive`
- rclone and fuse3 package installation from:
  - `worker_container/Dockerfile`
  - `worker_container/Singularity.def`
- helper copies/permissions in the two image definitions
- `/run/worker-harness` creation that exists solely for rclone configuration
- all rclone configuration binding and `RCLONE_CONFIG` handling in `start-wh.sh`
- all `/dev/fuse` handling, `WH_FUSE_AVAILABLE`, and the temporary
  `fuse-probe.request` / `--fusemount` code from `start-wh.sh`
- `WH_FUSE_AVAILABLE` persistence/export from `worker_container/entrypoint.sh`
- rclone mount/copy/serve lease state under `$WH_DIR/data/leases` and
  `$WH_DIR/data/cache`
- `src/worker_harness/data.py` transfer-command builders and `POST
  /api/v1/data/fetch`
- `DataFetchRequest` and corresponding HTTP API documentation
- FUSE/rclone-specific tests and `SHARED_DATA_ACCESS.md`

### Retain

- `data_paths` in registration, worker model, and SQLite worker row.
- `GET /api/v1/data`, rewritten in Phase 2 to use bind-manifest paths.
- the `22000-22999` worker-to-worker normal ACL rule.  It is used in Phase 3,
  not for SSH.

### Tests

- Existing job, file-transfer, and worker registration tests remain green.
- A worker starts with no `/dev/fuse` bind, no rclone config bind, and no
  FUSE-related environment variable.
- The rebuilt image has no `rclone`, `fusermount3`, or `wh-data-*` helpers.

## Phase 2 — Bind-path discovery

### Goal

Advertise the immediate data directories inside each data-visible container
path chosen by the operator in `WH_EXTRA_BINDS`. Do not recursively index
content or scan outside those configured bind destinations.

### Bind manifest

`start-wh.sh` already parses the semicolon-delimited `WH_EXTRA_BINDS` value and
passes each pair to Apptainer/Singularity.  During that same parsing it writes
an atomic manifest in the host `WH_DIR`:

```text
$WH_DIR/data/bind-paths.json
```

Example host configuration:

```bash
WH_EXTRA_BINDS="/mnt/institution/datasets:/data/institution;/srv/project:/code/project"
```

Manifest visible in the SIF:

```json
{
  "paths": ["/data/institution", "/code/project"]
}
```

The manifest contains only **container destination paths**.  It never exposes
host source paths, rclone remotes, NAS hostnames, credentials, or mount options.

The existing `WH_EXTRA_BINDS` syntax remains unchanged.  Its current practical
contract is `host:container` or `host:container:ro`; host paths containing
literal `:` are unsupported and need not be added in this phase.

### Worker daemon behavior

The worker daemon reads `bind-paths.json` on each heartbeat and reports the
normalized, deduplicated **immediate non-symlink directory children** of each
configured destination in `data_paths`. For example, a configured `/data`
bind with `/data/ds1` and `/data/ds2` reports those two paths, not `/data`.

It does not:

- recursively scan below those immediate children;
- list files, calculate size, hash content, or parse datasets;
- report a bind root, a path outside a bind root, or a host-side source path;
- follow or advertise symlinks.

A missing or unreadable bind is simply absent from that heartbeat.

The operator chooses a collection-level bind destination:

```text
/mnt/nas/datasets   → /data                # advertises /data/imagenet, /data/coco, …
/mnt/nas/projects   → /code                # advertises /code/project-a, /code/project-b, …
```

Each returned directory is a copyable unit. Agents may inspect it with their
existing worker shell access (`ls`, `find`, `du`, application-specific
commands). No content-inspection endpoint is added.

### Orchestrator behavior

Persist `data_paths` as JSON on the existing worker record.  `GET /api/v1/data`
returns the exact reverse map of online workers:

```json
{
  "/data/imagenet": [
    {"worker_id": "…", "worker_name": "KW60898"}
  ],
  "/code/project": [
    {"worker_id": "…", "worker_name": "KW60995"}
  ]
}
```

There is no data topology table, dataset model, versioning, conflict handling,
manifest, checksum, or automatic interpretation.

Expose a read-only Worker Harness extension action, e.g.:

```text
wh_read({ action: "list_data" })
```

### Tests

- launcher manifest extraction for ordinary, read-only, and multiple bind pairs;
- daemon ignores missing/invalid manifests and reports only immediate valid directories;
- DB migration, insert, update, and row decoding of `data_paths`;
- endpoint excludes offline workers by default and has an explicit
  `include_offline` option;
- no filesystem recursion is performed.

## Phase 3 — Secure direct copy

### Goal

Provide one mutating tool:

```text
wh_data_copy(src_worker, src_path, dst_worker, dst_path)
```

The agent decides source and destination using the simple path map and its own
inspection.  Worker Harness handles only transport lifecycle and cleanup.

### Image dependency

Add `rsync` to both worker image definitions.  Do not re-add rclone or fuse3.

### API and job model

Add:

```http
POST /api/v1/data/copy

{
  "src_worker": "KW60898",
  "src_path": "/data/imagenet",
  "dst_worker": "KW60995",
  "dst_path": "/data/imagenet"
}
```

The response returns a normal target Worker Harness job ID plus a transfer ID.
The existing job list/log/stop routes remain the primary progress interface.

Validate only operational invariants:

- both worker references resolve and are online;
- source/destination paths are normalized absolute paths and not `/`;
- source and destination workers differ;
- target destination parent can be created by the runtime user;
- source equals an advertised source directory or is below one of those
  directories (an explicit future `allow_unindexed_source` flag may be added;
  default false).

This is not intended as a path authorization system: agents with worker
control already have shell access.  Validation prevents accidental broad
exports, shell injection, and malformed orchestration requests.

### Source helper

The orchestrator starts a source-only helper over its existing Tailscale SSH
control channel.  The helper:

1. validates the requested source directory;
2. chooses an unused port from `22000-22999`;
3. creates an rsync daemon configuration with exactly one module rooted at the
   requested directory, `read only = yes`, `list = no`;
4. generates a random transfer username/password and stores the daemon secret
   in a restrictive temporary file;
5. starts `rsync --daemon --no-detach` bound to localhost;
6. publishes that localhost TCP listener with the source worker's userspace
   Tailscale daemon using `tailscale --socket=<WH_DIR socket> serve --tcp`;
7. returns endpoint, module, credential, port, PID, and expiry to the
   orchestrator; and
8. removes the rsync daemon, Tailnet Serve rule, temporary config, and secret
   when explicitly cleaned up or when TTL expires.

The source server is an internal detail; no public `wh_data_serve` tool exists.

### Destination helper and userspace transport

Workers use Tailscale userspace networking, so a normal process cannot assume
that connecting directly to a Tailnet IP works.  The destination rsync daemon
connection must use the local Tailscale socket, not ordinary TCP routing.

rsync supports `RSYNC_CONNECT_PROG`, which replaces its direct daemon socket
connection.  The destination helper uses the equivalent of:

```bash
RSYNC_CONNECT_PROG='tailscale --socket=/var/lib/worker-harness/tailscale/run/tailscaled.sock nc %H 22000' \
rsync -a --partial --append-verify \
  rsync://transfer@SOURCE_TAILNET_IP/module/ \
  /requested/destination/
```

The actual source port replaces `22000` in the generated command.  rsync's
`%H` escape is substituted with the source host by rsync.  The destination
uses a temporary password file/environment scoped to the copy job; it never
writes the source credential to heartbeat data, SQLite, or the API response.

This is a required feasibility spike before the copy endpoint is implemented:

1. verify that `tailscale serve --tcp` publishes a localhost rsync daemon from
   a userspace-networking worker;
2. verify that target `tailscale --socket … nc <source> <port>` transports a
   complete rsync daemon session;
3. verify the Headscale worker-to-worker `22000-22999` normal ACL permits the
   connection but does not grant peer SSH;
4. verify source cleanup removes both the daemon and its `tailscale serve`
   publication on success, cancellation, TTL expiry, and worker restart.

If this spike fails, do **not** add peer Tailscale SSH as a silent fallback.
The explicit fallback decision is either an orchestrator-relayed tar/rsync
stream or a host-LAN-only transfer implementation, documented separately.

### Control-plane cleanup

A copy can outlive its HTTP request.  Store a minimal `data_transfers` record
in SQLite:

```text
id, source_worker_id, destination_worker_id, source_lease_id,
target_job_id, source_port, created_at, expires_at, cleaned_at, status
```

A reaper/poller observes the target job state.  On done/failed/stopped/expiry,
it SSHes to the source helper's cleanup operation and marks the transfer
cleaned.  Cleanup is idempotent.  The random credential is never stored in the
database; it is needed only while starting the destination job.

### Headscale policy for Phase 3

Keep the existing normal ACL rule:

```json
{
  "action": "accept",
  "src": ["tag:wh-worker"],
  "dst": ["tag:wh-worker:22000-22999"]
}
```

Do **not** add:

```json
"tag:wh-worker:22"
```

and do **not** add a worker-to-worker entry to the `ssh` policy.  The source
rsync daemon is the only peer data-plane service.

### Tests

- request validation and source/destination resolution;
- generated rsync daemon config is read-only and rooted at the exact source;
- generated target command uses `RSYNC_CONNECT_PROG` with the worker's
  Tailscale socket and not a peer SSH command;
- source failure, target startup failure, cancellation, completion, and expiry
  all invoke idempotent cleanup;
- no random credential appears in logs, database rows, or API response;
- live two-worker userspace-Tailscale rsync spike before enabling the endpoint.

## Phase 4 — Separate worker registration from control API

### Goal

Prevent a network-compromised worker from using its heartbeat reachability to
invoke privileged orchestration routes on other workers.

Today both worker registration and all `/api/v1/*` control routes share port
`12888`, while policy permits `tag:wh-worker -> tag:wh-orchestrator:12888`.
The HTTP application does not distinguish worker heartbeats from privileged
agent requests.  This is a fleet-control vulnerability independent of data
copying.

### Target topology

Run two FastAPI applications/servers using the same database:

| Service | Default port | Routes | Allowed callers |
|---|---:|---|---|
| registration | `12888` | `POST /register`, `GET /health` | `tag:wh-worker` |
| control | `12889` | `/api/v1/*` | operator/client nodes, orchestrator-local tools |

Add `WH_CONTROL_PORT` (default `12889`) to orchestrator configuration.  The
worker daemon continues using only `ORCHESTRATOR_PORT=12888`; it requires no
new secret or control credential in this phase.

Refactor the current monolithic `create_app` into route/app factories with
shared dependency setup.  Registration app does not import/register control
routes.  Control app does not expose `/register`.

### ACL changes

Retain worker registration reachability only:

```json
{
  "action": "accept",
  "src": ["tag:wh-worker"],
  "dst": ["tag:wh-orchestrator:12888"]
}
```

Give trusted client/user nodes access only to `12889`, not all orchestrator
ports.  Exact client tags/groups depend on the deployed Headscale policy; the
example policy must stop using broad `autogroup:member -> tag:wh-orchestrator:*`.

The Worker Harness PI extension/config defaults and HTTP API documentation
must use the control URL/port.  Existing clients must be migrated before
removing `/api/v1/*` from port 12888.

### Tests

- worker source can register/health-check on 12888 but receives 404/connection
  refusal for control routes there;
- trusted control client reaches 12889;
- all existing control API tests run against the control app;
- registration tests run against the registration app;
- policy example has no worker-to-control-port path.

## Rollout order

1. Recover and stabilize KW60898; no more FUSE attempts.
2. Implement and test Phase 1 locally.  Build a clean non-rclone/non-FUSE SIF.
3. Deploy that clean image only after worker update path units are confirmed
   healthy; verify normal jobs on KW60898.
4. Implement and test Phase 2; deploy only after the bind manifest is reviewed.
5. Run Phase 3 transport feasibility spike on two disposable/idle workers.
6. Implement the Phase 3 copy endpoint only if the spike succeeds.
7. Implement and deploy Phase 4 before treating copy as safe across separate
   worker trust domains.

No phase silently substitutes broader peer SSH access, an in-SIF FUSE mount,
or a relay through the orchestrator without an explicit new decision.
