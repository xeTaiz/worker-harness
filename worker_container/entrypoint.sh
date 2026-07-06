#!/bin/bash
set -euo pipefail

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
TS_SOCKET="${TS_SOCKET_DIR}/tailscaled.sock"
HARNESS_DIR="${WH_DIR}/harness"
WORKER_DAEMON_DIR="${WH_DIR}/worker-daemon"
TMUX_TMPDIR="${WH_DIR}/tmux"
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

if [ -z "$TS_AUTHKEY" ]; then
  echo "[entrypoint] ERROR: TS_AUTHKEY is required"
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

mkdir -p "$TS_STATE_DIR" "$TS_SOCKET_DIR" "$HARNESS_DIR" "$WORKER_DAEMON_DIR" "$TMUX_TMPDIR" "$SSH_HOME_DIR"
chown "$SSH_USER":"$SSH_USER" "$TMUX_TMPDIR" 2>/dev/null || chown "$SSH_USER" "$TMUX_TMPDIR" 2>/dev/null || true
chmod 700 "$TMUX_TMPDIR" 2>/dev/null || true
export HOME="$SSH_HOME_DIR"
export USER="$SSH_USER"
export LOGNAME="$SSH_USER"
export SSH_USER
export TMUX_TMPDIR

echo "[entrypoint] Starting tailscaled in userspace mode (SOCKS5: ${TS_SOCKS5_ADDR})..."
tailscaled \
  --statedir="${TS_STATE_DIR}" \
  --state="${TS_STATE_DIR}/tailscaled.state" \
  --socket="${TS_SOCKET}" \
  --tun=userspace-networking \
  --socks5-server="${TS_SOCKS5_ADDR}" &

sleep 2

echo "[entrypoint] Joining tailnet with Tailscale SSH enabled..."
UP_ARGS=(
  --login-server="$TS_HOST"
  --authkey="$TS_AUTHKEY"
  --accept-routes="$TS_ACCEPT_ROUTES"
  --ssh
)

if [ -n "$TS_HOSTNAME" ]; then
  UP_ARGS+=(--hostname="$TS_HOSTNAME")
fi

if [ -n "$TS_EXTRA_ARGS" ]; then
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

# ── 2. Harness directory ─────────────────────────────────────────────
chmod 1777 "$HARNESS_DIR"
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
exec env WH_PROXY="$DAEMON_WH_PROXY" python3 /worker_daemon.py
