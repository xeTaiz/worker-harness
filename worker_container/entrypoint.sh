#!/bin/bash
set -euo pipefail

slurm_mode=0
if [ -n "${WH_SLURM_WORKER_ID:-}" ]; then
  slurm_mode=1
  umask 077
  : "${WH_SLURM_COMPUTE_KIND:?Slurm compute kind is required}"
  case "$WH_SLURM_COMPUTE_KIND" in
    cpu) export CUDA_VISIBLE_DEVICES="" NVIDIA_VISIBLE_DEVICES=void ;;
    gpu)
      : "${WH_SLURM_CUDA_VISIBLE_DEVICES:?Original Slurm CUDA visibility is required}"
      export CUDA_VISIBLE_DEVICES="$WH_SLURM_CUDA_VISIBLE_DEVICES"
      ;;
    *) echo "[entrypoint] ERROR: invalid Slurm compute kind" >&2; exit 1 ;;
  esac
fi

# Fleet/GPU launches retain the normal CUDA tool and driver setup. CPU
# allocations must not depend on a host NVIDIA stack.
if (( ! slurm_mode )) || [ "$WH_SLURM_COMPUTE_KIND" = gpu ]; then
  export PATH="/usr/local/cuda/bin:/usr/local/nvidia/bin:${PATH}"
  export LD_LIBRARY_PATH="/.singularity.d/libs${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
fi
export TAILSCALE_SSH_DEFAULT_PATH="${PATH}"

echo "[entrypoint] Starting worker bootstrap..."

# ── 1. Tailscale bootstrap (userspace mode only) ─────────────────────
TS_AUTHKEY="${TS_AUTHKEY:-}"
TS_HOST="${TS_HOST:-https://controlplane.tailscale.com}"
TS_HOSTNAME="${TS_HOSTNAME:-}"
TS_ACCEPT_ROUTES="${TS_ACCEPT_ROUTES:-false}"
TS_EXTRA_ARGS="${TS_EXTRA_ARGS:-}"
TS_SOCKS5_ADDR="${TS_SOCKS5_ADDR:-127.0.0.1:1055}"
WH_DIR="${WH_DIR:-$HOME/.local/worker-harness}"
TS_STATE_DIR="${WH_DIR}/tailscale/state"
TS_SOCKET_DIR="${WH_DIR}/tailscale/run"
if (( slurm_mode )); then
  # AF_UNIX paths have a small fixed limit; shared run paths need not. Keep
  # sockets in this container's private /tmp, keyed by the reserved UUID.
  socket_key="$(python3 - "$WH_SLURM_WORKER_ID" <<'PY'
import hashlib
import os
import stat
import sys
key = hashlib.sha256(sys.argv[1].encode()).hexdigest()[:16]
for prefix in ("wh-ts-", "wh-tmux-"):
    path = "/tmp/" + prefix + key
    try:
        os.mkdir(path, 0o700)
    except FileExistsError:
        pass
    info = os.lstat(path)
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid():
        raise ValueError("unsafe allocation socket directory")
    os.chmod(path, 0o700)
print(key)
PY
)"
  TS_SOCKET_DIR="/tmp/wh-ts-$socket_key"
fi
TS_SOCKET="${TS_SOCKET_DIR}/tailscaled.sock"
HARNESS_DIR="${WH_DIR}/harness"
JOB_TMUX_TMPDIR="${HARNESS_DIR}/job-tmux"
if (( slurm_mode )); then
  JOB_TMUX_TMPDIR="/tmp/wh-tmux-$socket_key"
  export JOB_TMUX_DIR="$JOB_TMUX_TMPDIR"
  trap 'rmdir "$TS_SOCKET_DIR" "$JOB_TMUX_TMPDIR" 2>/dev/null || true' EXIT
fi
WORKER_DAEMON_DIR="${WH_DIR}/worker-daemon"
TMUX_TMPDIR="$JOB_TMUX_TMPDIR"
SSH_HOME_DIR=""

_detect_ssh_user() {
  if [ -n "${SSH_USER:-}" ]; then
    printf '%s\n' "$SSH_USER"
    return 0
  fi

  # Prefer the username implied by HOME (/home/<user>) when available.
  if [ -n "${HOME:-}" ]; then
    case "$HOME" in
      /home/*)
        candidate="${HOME#/home/}"
        if [ -n "$candidate" ] && [ "$candidate" != "root" ]; then
          printf '%s\n' "$candidate"
          return 0
        fi
        ;;
    esac
  fi

  for var in SINGULARITY_USER APPTAINER_USER SUDO_USER LOGNAME USER; do
    eval "candidate=\${$var:-}"
    if [ -n "$candidate" ] && [ "$candidate" != "root" ]; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done

  printf '%s\n' "root"
}

SSH_USER="${SSH_USER:-$(_detect_ssh_user)}"
if [ "$SSH_USER" = "root" ]; then
  SSH_HOME_DIR="/root"
else
  SSH_HOME_DIR="${WH_DIR}/home/${SSH_USER}"
fi

if (( slurm_mode )) && [ ! -r "${TS_AUTHKEY_FILE:-}" ]; then
  echo "[entrypoint] ERROR: Slurm enrollment requires readable TS_AUTHKEY_FILE" >&2
  exit 1
elif [ -z "$TS_AUTHKEY" ] && [ ! -r "${TS_AUTHKEY_FILE:-}" ]; then
  echo "[entrypoint] ERROR: TS_AUTHKEY or readable TS_AUTHKEY_FILE is required" >&2
  exit 1
fi

SSH_HOME_DIR="${WH_DIR}/home/${SSH_USER}"

if ! getent passwd "$SSH_USER" >/dev/null 2>&1; then
  if [ "$(id -u)" -eq 0 ] && [ -w /etc/passwd ] && command -v useradd >/dev/null 2>&1; then
    if ! getent group "$SSH_USER" >/dev/null 2>&1; then
      groupadd "$SSH_USER" >/dev/null 2>&1 || true
    fi
    useradd -m -d "$SSH_HOME_DIR" -s /bin/bash -g "$SSH_USER" "$SSH_USER" >/dev/null 2>&1 || \
      useradd -m -d "$SSH_HOME_DIR" -s /bin/bash "$SSH_USER"
  else
    echo "[entrypoint] ERROR: missing passwd entry for SSH user '$SSH_USER'. Bind generated /etc/passwd and /etc/group (see start-wh.sh), or run in a writable rootful container."
    exit 1
  fi
fi

SSH_HOME_DIR="$(getent passwd "$SSH_USER" | cut -d: -f6)"
if [ -z "$SSH_HOME_DIR" ]; then
  SSH_HOME_DIR="${WH_DIR}/home/${SSH_USER}"
fi

mkdir -p "$TS_STATE_DIR" "$TS_SOCKET_DIR" "$HARNESS_DIR" "$JOB_TMUX_TMPDIR" "$WORKER_DAEMON_DIR" "$TMUX_TMPDIR" "$SSH_HOME_DIR"
# tmux tmpdir must be writable by the SSH user's uid, which may differ from
# the entrypoint's uid on workers without fakeroot. 1777 (like /tmp) lets
# any uid create its tmux-<uid> subdirectory.
chown "$SSH_USER":"$SSH_USER" "$TMUX_TMPDIR" 2>/dev/null || chown "$SSH_USER" "$TMUX_TMPDIR" 2>/dev/null || true
if (( slurm_mode )); then
  chmod 700 "$TMUX_TMPDIR" "$TS_SOCKET_DIR"
else
  chmod 1777 "$TMUX_TMPDIR" 2>/dev/null || true
fi
export HOME="$SSH_HOME_DIR"
export USER="$SSH_USER"
export LOGNAME="$SSH_USER"
export SSH_USER
export TMUX_TMPDIR

tailscaled_pid=""
daemon_pid=""
cleanup_slurm() {
  local status=$?
  trap - EXIT
  if [ -n "$daemon_pid" ]; then
    kill -TERM "$daemon_pid" 2>/dev/null || true
    wait "$daemon_pid" 2>/dev/null || true
  fi
  if [ -n "$tailscaled_pid" ]; then
    kill -TERM "$tailscaled_pid" 2>/dev/null || true
    for ((poll=0; poll<20; poll++)); do
      kill -0 "$tailscaled_pid" 2>/dev/null || break
      sleep 0.1
    done
    kill -KILL "$tailscaled_pid" 2>/dev/null || true
    wait "$tailscaled_pid" 2>/dev/null || true
  fi
  rm -f "$TS_SOCKET"
  rmdir "$TS_SOCKET_DIR" "$JOB_TMUX_TMPDIR" 2>/dev/null || true
  exit "$status"
}

if (( slurm_mode )); then
  trap cleanup_slurm EXIT
  trap 'exit 143' TERM INT HUP
  # Host networking is shared by allocations. Reserve an ephemeral candidate,
  # then verify tailscaled actually bound it; retry the close/bind race.
  socks_ready=0
  for attempt in 1 2 3 4 5; do
    TS_SOCKS5_ADDR="$(python3 - <<'PY'
import socket
with socket.socket() as listener:
    listener.bind(("127.0.0.1", 0))
    print(f"127.0.0.1:{listener.getsockname()[1]}")
PY
)"
    rm -f "$TS_SOCKET"
    tailscaled \
      --statedir="$TS_STATE_DIR" \
      --state="$TS_STATE_DIR/tailscaled.state" \
      --socket="$TS_SOCKET" \
      --tun=userspace-networking \
      --port=0 \
      --socks5-server="$TS_SOCKS5_ADDR" >>"$WH_DIR/tailscaled.log" 2>&1 &
    tailscaled_pid=$!
    for ((poll=0; poll<50; poll++)); do
      kill -0 "$tailscaled_pid" 2>/dev/null || break
      if [ -S "$TS_SOCKET" ] && python3 - "$TS_SOCKS5_ADDR" <<'PY'
import socket
import sys
host, port = sys.argv[1].rsplit(":", 1)
try:
    with socket.create_connection((host, int(port)), timeout=0.1):
        pass
except OSError:
    sys.exit(1)
PY
      then
        socks_ready=1
        break
      fi
      sleep 0.1
    done
    (( socks_ready )) && break
    kill -TERM "$tailscaled_pid" 2>/dev/null || true
    wait "$tailscaled_pid" 2>/dev/null || true
    tailscaled_pid=""
  done
  if (( ! socks_ready )); then
    echo "[entrypoint] ERROR: tailscaled SOCKS startup failed after 5 attempts; see tailscaled.log" >&2
    exit 1
  fi
else
  echo "[entrypoint] Starting tailscaled in userspace mode (SOCKS5: ${TS_SOCKS5_ADDR})..."
  tailscaled \
    --statedir="${TS_STATE_DIR}" \
    --state="${TS_STATE_DIR}/tailscaled.state" \
    --socket="${TS_SOCKET}" \
    --tun=userspace-networking \
    --socks5-server="${TS_SOCKS5_ADDR}" &
  sleep 2
fi

echo "[entrypoint] Joining tailnet with Tailscale SSH enabled..."
UP_ARGS=(
  # The worker exclusively owns this userspace tailscaled state. Reset makes
  # restarts idempotent when a prior partial `tailscale up` persisted flags.
  --reset
  --login-server="$TS_HOST"
  --accept-routes="$TS_ACCEPT_ROUTES"
  --ssh
)
if [ -n "${TS_AUTHKEY_FILE:-}" ]; then
  UP_ARGS+=(--auth-key="file:$TS_AUTHKEY_FILE")
else
  UP_ARGS+=(--authkey="$TS_AUTHKEY")
fi
if (( slurm_mode )); then
  UP_ARGS+=(--advertise-tags=tag:wh-cluster-job)
fi

if [ -n "$TS_HOSTNAME" ]; then
  UP_ARGS+=(--hostname="$TS_HOSTNAME")
fi

if (( ! slurm_mode )) && [ -n "$TS_EXTRA_ARGS" ]; then
  # shellcheck disable=SC2206
  EXTRA_ARGS=( $TS_EXTRA_ARGS )
  UP_ARGS+=("${EXTRA_ARGS[@]}")
fi

tailscale --socket="$TS_SOCKET" up "${UP_ARGS[@]}"

echo "[entrypoint] Waiting for Tailnet IP..."
for i in $(seq 1 30); do
  TS_IP="$(tailscale --socket="$TS_SOCKET" ip -4 2>/dev/null | head -n1 || true)"
  if [ -n "$TS_IP" ]; then
    echo "[entrypoint] Tailnet IP assigned: $TS_IP"
    break
  fi
  echo "[entrypoint] Waiting for Tailnet IP... ($i/30)"
  sleep 2
done
if (( slurm_mode )) && [ -z "${TS_IP:-}" ]; then
  echo "[entrypoint] ERROR: Tailnet IP was not assigned" >&2
  exit 1
fi

# ── 2. Harness directory ─────────────────────────────────────────────
chmod 1777 "$HARNESS_DIR"
if (( slurm_mode )); then
  chmod 700 "$JOB_TMUX_TMPDIR"
else
  # Fleet SSH and delegated jobs share the writable harness tmux root.
  chmod 1777 "$JOB_TMUX_TMPDIR"
fi
echo "[entrypoint] Harness directory ready at $HARNESS_DIR"

# ── 3. Worker daemon ─────────────────────────────────────────────────
DAEMON_WH_PROXY="${WH_PROXY:-}"
if [ -z "$DAEMON_WH_PROXY" ]; then
  DAEMON_WH_PROXY="socks5://${TS_SOCKS5_ADDR}"
fi

export WH_DIR
export TS_SOCKET

unset TMUX TMUX_PANE

echo "[entrypoint] Starting worker daemon..."
if (( slurm_mode )); then
  # Keep bootstrap-owned tailscaled finite as well; workload lifecycle remains
  # entirely in the existing daemon. The batch wrapper bounds hanging startup,
  # including tailscale up, with its original 180-second deadline.
  env WH_PROXY="$DAEMON_WH_PROXY" python3 /worker_daemon.py &
  daemon_pid=$!
  status=0
  wait "$daemon_pid" || status=$?
  daemon_pid=""
  exit "$status"
fi
exec env WH_PROXY="$DAEMON_WH_PROXY" python3 /worker_daemon.py
