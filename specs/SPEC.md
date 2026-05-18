# Worker Harness — Specification v0.1

## Overview

A tool for managing a fleet of worker machines that run coding/machine-learning jobs.
Two components:

1. **Worker** — a container image that self-registers into a ZeroTier VPN and exposes
   a Podman socket, SSH, and a minimal heartbeat daemon.
2. **Orchestrator** — a native process on a head node that discovers workers via their
   heartbeats, stores state in SQLite, and exposes both a TUI and a CLI for full
   control (run jobs, stream logs, open ports, monitor resources).

---

## 1. Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        ZeroTier Mesh VPN                         │
│                                                                  │
│  ┌──────────────────────────┐   ┌──────────────────────────┐   │
│  │   Orchestrator (native)  │   │     Worker N (container)  │   │
│  │                          │   │   ┌────────────────────┐  │   │
│  │  ┌────────────────────┐  │   │   │  Python daemon    │  │   │
│  │  │ HTTP API (heartbeat│◄─┼───┼───┼─► POST /register  │  │   │
│  │  │ & commands)        │  │   │   │  Minimal: 1 proc  │  │   │
│  │  └────────────────────┘  │   │   └────────┬───────────┘  │   │
│  │  ┌────────────────────┐  │   │            │              │   │
│  │  │ SSH Client         │◄─┼───┼────────────┼──────────────┼───┐
│  │  │ (job exec, shell,  │  │   │  Podman socket (host)   │   │  │
│  │  │  port forward)     │  │   │  SSH server (container) │   │  │
│  │  └────────────────────┘  │   │  tmux (job sessions)    │   │  │
│  │  ┌────────────────────┐  │   │  ZeroTier daemon        │   │  │
│  │  │ SQLite DB          │  │   └─────────────────────────│───┘   │
│  │  └────────────────────┘  │                                   │
│  │  ┌────────────────────┐  │   ┌──────────────────────────┐   │
│  │  │ TUI + CLI          │  │   │     Worker 2 (container)  │   │
│  │  └────────────────────┘  │   │  ... same structure ...   │   │
│  └──────────────────────────┘   └──────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────┘
```

**Key design decisions:**

- Workers **push** heartbeats to the orchestrator's HTTP API. The orchestrator has a
  well-known ZeroTier IP that is baked into the worker image config.
- The orchestrator **pulls** job results by SSHing into workers, creating/managing tmux
  sessions, and reading tmux capture files.
- ZeroTier lives **inside** the container, bootstrapped from env vars at start time.
- The worker container runs a **minimal Python daemon** (single async process) for
  heartbeats and nothing else. All "real work" runs in tmux managed by the orchestrator.
- Podman socket is exposed via host bind-mount, enabling nested containers inside jobs.

---

## 2. Network & Registration

### ZeroTier Setup

- Worker container starts with env vars:
  - `ZEROTIER_NETWORK_ID` — the ZeroTier network to join
  - `ZEROTIER_SECRET` — the ZeroTier identity secret (base64), optional. If omitted, the
    daemon generates a fresh identity on first start.
- A startup script in the container:
  1. Writes the secret to `/var/lib/zerotier-one/identity.secret` (if provided)
  2. Starts `zerotier-one`
  3. Joins `ZEROTIER_NETWORK_ID`
  4. Waits until the node has an IP on the network
  5. Starts the worker daemon

- Orchestrator also runs ZeroTier (native install or container) and gets a **fixed,
  pre-allocated IP** on the ZeroTier network (via ZeroTier's IP assignment).

- Worker containers can be pre-built with a static ZeroTier IP pool or use DHCP on the
  ZeroTier network. For predictability, workers should be pre-authorized in the ZeroTier
  central console so they always get the same IP based on their identity.

### Worker Registration (Heartbeat)

Every **60 seconds** (configurable), the worker daemon POSTs to:
```
http://<ORCHESTRATOR_ZEROTIER_IP>:<ORCHESTRATOR_PORT>/register
```

Payload:
```json
{
  "worker_id": "uuid-v4",
  "name": "gpu-rig-1",           // from env WORKER_NAME or hostname
  "zerotier_ip": "10.147.17.x",
  "ssh_port": 22,                // container SSH port (passed at start)
  "gpu_count": 2,
  "gpus": [
    {"name": "NVIDIA RTX 4090", "vram_total_gb": 24, "vram_used_gb": 8},
    {"name": "NVIDIA RTX 4090", "vram_total_gb": 24, "vram_used_gb": 0}
  ],
  "cpu_cores": 32,
    "used_ram_gb": 64,
    "total_disk_gb": 2000,
    "used_disk_gb": 340
  },
  "active_ports": [
    {"local": 6006, "remote": 6006, "service": "tensorboard"}
  ],
  "active_jobs": [
    {"job_id": "uuid", "tmux_session": "wh_job_abc123", "status": "running"}
  ]
}
```

If the orchestrator restarts, workers will re-register on their next heartbeat and the
orchestrator picks them back up (existing worker_id + zerotier_ip is the identity key).

---

## 3. Worker Container

### Image Contents

```
FROM fedora:41  # or ubuntu, choose based on GPU driver compatibility

# ZeroTier
RUN curl -s 'https://pkg.zerotier.com/zt.gpg' | rpm --import - && \
    curl -s https://www.zerotier.com/download/packages/zt_6_4.deb -o /tmp/zt.deb && \
    apt install /tmp/zt.deb || ( # or yum/dnf equivalent )

# SSH server
RUN dnf install -y openssh-server tmux git curl wget vim jq && \
    ssh-keygen -A && \
    mkdir -p /run/sshd

# Harness directory for job scripts and logs (mount from host for persistence)
RUN mkdir -p /harness && chmod 1777 /harness
# Podman socket (container-in-container support)
# Note: socket is mounted from host at runtime

WORKDIR /workspace
```

### Entrypoint / Startup Script

```bash
#!/bin/bash
# 1. ZeroTier bootstrap
if [ -n "$ZEROTIER_SECRET" ]; then
    mkdir -p /var/lib/zerotier-one
    echo "$ZEROTIER_SECRET" > /var/lib/zerotier-one/identity.secret
fi
zerotier-one &
sleep 5
zerotier-one join "$ZEROTIER_NETWORK_ID"

# Wait for IP
while ! ip addr show zt* | grep -q "inet "; do sleep 1; done

# 2. SSH server (port from env WORKER_SSH_PORT, default 22)
sed -i "s/#Port 22/Port ${WORKER_SSH_PORT:-22}/" /etc/ssh/sshd_config
/usr/sbin/sshd

# 3. Harness directory for job scripts and logs
mkdir -p /harness && chmod 1777 /harness

# 4. Start the worker daemon (Python)
exec python -m worker.daemon
```

### Worker Daemon

A single-file (or minimal module) async Python process. Responsibilities:
- Send heartbeat POST every 60s to orchestrator
- On startup, do a full registration POST immediately
- Optionally: receive commands via a Unix socket (`/run/worker-daemon.sock`) for
  things that need in-container execution (e.g. `podman ps`, `zerotier-cli status`)

That's it. No job management, no scheduling. Keep it tiny.

### Host-Side Requirements

On each worker **host machine** (not inside the container):
- Podman rootless socket must be running: `systemctl --user enable --now podman.socket`
- The socket is passed into the container: `-v /run/podman/podman.sock:/run/podman/podman.sock`
- NVIDIA Container Toolkit / runtime installed
- ZeroTier client (if orchestrator runs natively)

---

## 4. Orchestrator

### Directory Structure

```
worker-harness/
├── pyproject.toml
├── src/
│   └── worker_harness/
│       ├── __init__.py
│       ├── models.py          # Pydantic data models
│       ├── db.py              # SQLite repository layer
│       ├── ssh.py             # SSH client (subprocess + async wrappers)
│       ├── heartbeat.py       # HTTP server for worker heartbeats
│       ├── job.py             # Job execution via tmux
│       ├── port_forward.py    # SSH tunnel management
│       ├── worker_info.py     # GPU/CPU info collection
│       ├── config.py          # Config loading
│       ├── cli/
│       │   ├── __init__.py
│       │   ├── app.py          # Typer CLI root
│       │   ├── workers.py      # worker subcommands
│       │   ├── jobs.py         # job subcommands
│       │   └── tunnels.py      # port tunnel subcommands
│       └── tui/
│           ├── __init__.py
│           └── app.py          # Textual TUI
├── SPEC.md
└── README.md
```

### Database Schema (SQLite)

```sql
CREATE TABLE workers (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    zerotier_ip TEXT NOT NULL,
    ssh_port INTEGER NOT NULL DEFAULT 22,
    gpu_count INTEGER DEFAULT 0,
    gpu_names TEXT,            -- JSON list
    gpu_vram_gb TEXT,          -- JSON list (total per GPU)
    gpu_used_vram_gb TEXT,     -- JSON list (current usage per GPU)
    cpu_cores INTEGER,
    total_ram_gb REAL,
    used_ram_gb REAL,
    total_disk_gb REAL,
    used_disk_gb REAL,
    status TEXT DEFAULT 'online',  -- online, offline, draining
    last_heartbeat_ts INTEGER,
    created_at INTEGER
);

CREATE TABLE jobs (
    id TEXT PRIMARY KEY,
    worker_id TEXT REFERENCES workers(id),
    tmux_session TEXT,
    command TEXT NOT NULL,
    status TEXT DEFAULT 'pending',  -- pending, running, done, failed
    exit_code INTEGER,
    started_at INTEGER,
    finished_at INTEGER,
    pty_enabled INTEGER DEFAULT 1  -- whether to use PTY for ANSI/tqdm support
);

CREATE TABLE port_forwards (
    id TEXT PRIMARY KEY,
    worker_id TEXT REFERENCES workers(id),
    local_port INTEGER NOT NULL,
    remote_port INTEGER NOT NULL,
    service_name TEXT,  -- e.g. "tensorboard", "jupyter", "gradio"
    pid INTEGER,         -- SSH tunnel process ID
    created_at INTEGER
);

CREATE TABLE failures (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT REFERENCES jobs(id),
    worker_id TEXT REFERENCES workers(id),
    exit_code INTEGER,
    timestamp INTEGER,
    summary TEXT  -- one-line extracted from job output
);
```

### HTTP API (Worker → Orchestrator)

| Method | Path | Description |
|--------|------|-------------|
| POST | `/register` | Worker heartbeat + full registration |
| POST | `/worker/<id>/heartbeat` | Periodic heartbeat (lighter payload) |

### SSH Client Module (`ssh.py`)

Wraps the `ssh` CLI. Key operations:

```python
async def ssh_run(worker_ip: str, ssh_port: int, command: str, timeout=30) -> CompletedProcess
async def ssh_run_pty(worker_ip: str, ssh_port: int, command: str) -> None  # for interactive
async def ssh_tmux_send(worker_ip: str, ssh_port: int, session: str, keys: str) -> None
async def ssh_tmux_capture(worker_ip: str, ssh_port: int, session: str) -> str
async def ssh_tunnel(worker_ip: str, ssh_port: int, local_port: int, remote_port: int) -> Popen
def read_tmux_log(worker_ip: str, ssh_port: int, session: str) -> str  # reads .log file
```

**PTY handling:** Jobs that use tqdm or other TUI libraries need a PTY. The `pty_enabled`

Job logs are written to `/harness/<job_id>/output.log` on the worker (persistent,
mount a host volume at `/harness` for durability). Each job also stores its script
at `/harness/<job_id>/script.sh` for full command history auditability.

Job execution: the orchestrator base64-encodes a bash script and passes it via a
single SSH call — the script writes to `/harness/<job_id>/script.sh`, then runs it
under `tmux` for interactive inspection. The tmux session stays open for 60s after
the job completes, allowing `tmux attach` to inspect the finished session.

| Argument | Behavior |
|----------|----------|
| (default) | `tail -n 10` — last 10 lines, keeps output small |
| `--tail NUM` | Show last NUM lines (default: 10) |
| `--head NUM` | Show first NUM lines (setup/environment info). Mutually exclusive with `--tail`. |
| `--tail 0` | Suppress output, show only exit status |
| `--follow` / `-f` | Stream new lines as they appear (Ctrl+C to stop). Mutually exclusive with `--head`. |
| `--follow` / `-f` | Stream new lines as they appear (Ctrl+C to stop) |

Exit status is appended as `EXIT:<code>` to the log file when the job finishes.

> **Note**: No raw full-file retrieval. If the full log is needed, use `--tail 100000`.
> This is intentional — agents should work with summary output by default.

### Job Execution

1. `job start <worker_id> <command> [--no-pty]`
   - Generate `job_id = uuid4()`
   - SSH into worker:
     ```bash
     tmux new -d -s "wh_<job_id>" 'command 2>&1 | tee /tmp/wh_<job_id>.log; echo "EXIT:$?" >> /tmp/wh_<job_id>.log'
     ```
   - Record job in DB as `running`
2. `job list [--worker <id>]`
   - Query DB; merge with live data from tmux for running jobs (via `tmux display-message`)
3. `job logs <job_id>`
   - SSH `tail /tmp/wh_<job_id>.log` with `--tail`/`--head` flags (see Log Retrieval above)
4. `job stop <job_id>`
   - `ssh tmux kill-session -t wh_<job_id>`

### Port Forwarding

```bash
# Start tunnel: worker:remote_port → localhost:local_port
ssh -N -L <local_port>:localhost:<remote_port> <worker_ip> -p <ssh_port>
```

The orchestrator tracks all active tunnels in SQLite (pid, ports). On restart, tunnels
are not auto-resumed (agent/human must re-start them — simple and predictable).

---

## 5. CLI Interface

All commands are agent-friendly: structured output (JSON available), stable exit codes,
useful help text.

```
worker-harness --output json workers list
worker-harness --output json workers show <id>
worker-harness --output json workers status

worker-harness job start <worker_id> <command> [--name <label>]
worker-harness job list [--worker <id>] [--status running|done|failed]
worker-harness job logs <job_id> [--tail NUM] [--head NUM] [--follow]
worker-harness job stop <job_id>

worker-harness tunnel add <worker_id> <local_port> <remote_port> [--name tensorboard]
worker-harness tunnel list
worker-harness tunnel remove <tunnel_id>

worker-harness agent workers   # JSON summary for AI agents
worker-harness agent free-gpus  # list workers with available GPUs
```

Output format: default is human-readable, `--output json` emits machine-parseable JSON.

---

## 6. TUI (Textual)

Layout:

```
┌─ Worker Harness ─────────────────────────────────────────────────┐
│ [Workers: 4 | Jobs: 7 running | Tunnels: 3]       [cmd: help]   │
├──────────────────────────────────────────────────────────────────┤
│ WORKERS                          │ SELECTED WORKER: gpu-rig-1    │
│ ───────────────────────────────  │ ───────────────────────────  │
│ ● gpu-rig-1   10.147.17.5  2/2  │ GPUs: [████░░] 18GB [████░░]│
│ ○ gpu-rig-2   10.147.17.6  1/2  │ RAM: [██████░░] 64/128GB    │
│ ○ storage-1   10.147.17.10 0/1  │ DISK: 340GB / 2TB           │
│ ● cpu-node-1  10.147.17.15 0/32 │ Jobs: 2 running              │
│                                   │   ● wh_abc123: python train │
├──────────────────────────────────│   ● wh_def456: tensorboard   │
│ ACTIVE JOBS                      │ Tunnels:                     │
│ ───────────────────────────────  │   ↗ localhost:6006 → 6006  │
│ ● wh_abc  gpu-rig-1  running     │                             │
│ ○ wh_def  gpu-rig-1  done(0)     │ [B] Start job  [S] Stop      │
│ ● wh_ghi  gpu-rig-2  running     │ [L] Logs  [T] Tunnel  [I] Shell│
│ ○ wh_jkl  storage-1   failed(1)  │                             │
└──────────────────────────────────┴─────────────────────────────┘
```

- Left panel: scrollable worker list with status indicators (●=online, ○=offline)
- Right panel: detail view of selected worker (live GPU/RAM bars, job list, tunnels)
- Bottom bar: key bindings for common actions
- Floating modal for: start job dialog, log viewer, tunnel manager, interactive shell

Key binding ideas:
- `j/k` — navigate workers
- `Enter` — select worker detail
- `l` — view logs for selected job
- `s` — stop selected job
- `t` — add tunnel
- `i` — interactive SSH shell to worker (via Textual subprocess widget)
- `r` — refresh worker data (force heartbeat pull)
- `Ctrl+L` — log viewer with live tail

---

## 7. Open Questions / Future Considerations

These are intentionally **not** in scope for v1 but noted for later:

1. **Authentication** — Currently no auth on the orchestrator HTTP endpoint. For single-user
   this is fine (VPN is the security boundary). For multi-user: add a shared secret or
   certificate-based auth on the heartbeat endpoint.
2. **Job queues** — No queueing system. Jobs are fire-and-forget. A simple queue (SQLite-backed)
   with workers picking up work could be added later.
3. **Multi-orchestrator** — One orchestrator per ZeroTier network. For HA: use a proper DB
   (PostgreSQL) instead of SQLite and run multiple orchestrators with leader election.
4. **File transfer** — No `scp`/`rsync` built in yet. Workers have SSH so `rsync` over the
   ZeroTier network works manually. A `worker-harness sync` command could wrap this.
5. **NFS server** — The spec mentions potentially running an NFS server in workers for shared
   data. The tool will provide the *capability* but not automate the setup (agent responsibility).
6. **GPU scheduling hints** — v1 just exposes GPU info. A future version could have a simple
   constraint language: `--require "gpu_vram >= 16"` and the CLI/TUI highlights matching workers.

---

## 8. Implementation Order

### Phase 1 — Foundation
1. Worker container Dockerfile + ZeroTier bootstrap script
2. Worker daemon (heartbeat only, minimal)
3. Orchestrator HTTP server (register + heartbeat endpoints)
4. SQLite schema + repository layer
5. SSH client module (basic command execution)
6. `worker-harness workers list` CLI command

### Phase 2 — Job Execution
7. Job start / list / stop via tmux
8. Job log retrieval from tmux capture files
9. PTY support for tqdm/ANSI output
10. Job status polling and exit code capture
11. Failure tracking table

### Phase 3 — CLI Completeness
12. All CLI subcommands (`workers`, `jobs`, `tunnels`, `agent`)
13. JSON output mode for agents
14. `--follow` log tailing

### Phase 4 — TUI
15. Textual TUI with worker list + detail view
16. Job start/stop from TUI
17. Log viewer with live tail
18. Interactive shell widget
19. Tunnel management in TUI

### Phase 5 — Polish
20. Completion for bash/zsh
21. Config file support (`~/.config/worker-harness/config.toml`)
22. Documentation and examples
