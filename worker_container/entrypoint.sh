#!/bin/bash
set -euo pipefail

echo "[entrypoint] Starting worker bootstrap..."

TS_SOCKET="/var/run/tailscale/tailscaled.sock"

# ── 1. Tailscale bootstrap ─────────────────────────────────────────
TS_AUTHKEY="${TS_AUTHKEY:-}"
TS_HOST="${TS_HOST:-https://controlplane.tailscale.com}"
TS_HOSTNAME="${TS_HOSTNAME:-}"
TS_ACCEPT_ROUTES="${TS_ACCEPT_ROUTES:-false}"
TS_EXTRA_ARGS="${TS_EXTRA_ARGS:-}"
TS_USERSPACE="$(printf '%s' "${TS_USERSPACE:-true}" | tr '[:upper:]' '[:lower:]')"
TS_SOCKS5_ADDR="${TS_SOCKS5_ADDR:-127.0.0.1:1055}"
TS_SERVE_SSH_PORT="${TS_SERVE_SSH_PORT:-${WORKER_SSH_PORT:-22}}"

if [ -z "$TS_AUTHKEY" ]; then
    echo "[entrypoint] ERROR: TS_AUTHKEY is required"
    exit 1
fi

mkdir -p /var/lib/tailscale /var/run/tailscale

if [ "$TS_USERSPACE" = "true" ]; then
  echo "[entrypoint] Starting tailscaled in userspace mode (SOCKS5: ${TS_SOCKS5_ADDR})..."
  tailscaled \
    --state=/var/lib/tailscale/tailscaled.state \
    --socket="$TS_SOCKET" \
    --tun=userspace-networking \
    --socks5-server="$TS_SOCKS5_ADDR" &
else
  echo "[entrypoint] Starting tailscaled in kernel/TUN mode..."
  tailscaled \
    --state=/var/lib/tailscale/tailscaled.state \
    --socket="$TS_SOCKET" &
fi

sleep 2

echo "[entrypoint] Joining tailnet..."
UP_ARGS=(
  --login-server="$TS_HOST"
  --authkey="$TS_AUTHKEY"
  --accept-routes="$TS_ACCEPT_ROUTES"
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

if [ "$TS_USERSPACE" = "true" ]; then
  local_target="127.0.0.1:${WORKER_SSH_PORT:-22}"
  echo "[entrypoint] Configuring userspace Tailnet TCP forwarding ${TS_SERVE_SSH_PORT} -> ${local_target}"
  if tailscale --socket="$TS_SOCKET" serve --bg "tcp:${TS_SERVE_SSH_PORT}" "tcp://${local_target}" >/dev/null 2>&1; then
    echo "[entrypoint] tailscale serve configured (tcp:<port> syntax)"
  elif tailscale --socket="$TS_SOCKET" serve --bg --tcp "${TS_SERVE_SSH_PORT}" "${local_target}" >/dev/null 2>&1; then
    echo "[entrypoint] tailscale serve configured (--tcp syntax)"
  else
    echo "[entrypoint] ERROR: failed to configure userspace SSH forwarding"
    tailscale --socket="$TS_SOCKET" serve status || true
    exit 1
  fi
fi

# ── 2. SSH server ──────────────────────────────────────────────────
echo "[entrypoint] Configuring SSH server on port ${WORKER_SSH_PORT:-22}..."
sed -i "s/#Port 22/Port ${WORKER_SSH_PORT:-22}/" /etc/ssh/sshd_config

if [ ! -s /ssh/authorized_keys ]; then
  echo "[entrypoint] ERROR: /ssh/authorized_keys is missing or empty"
  echo "[entrypoint] Ensure worker image was built via 'just build' or mount a valid authorized_keys file"
  exit 1
fi

chmod 600 /ssh/authorized_keys
mkdir -p /run/sshd
/usr/sbin/sshd
echo "[entrypoint] SSH server running on port ${WORKER_SSH_PORT:-22}"

# ── 2b. Harness directory ───────────────────────────────────────────
mkdir -p /harness
chmod 1777 /harness
echo "[entrypoint] Harness directory ready at /harness"

# ── 3. Worker daemon ────────────────────────────────────────────────
DAEMON_WH_PROXY="${WH_PROXY:-}"
if [ "$TS_USERSPACE" = "true" ] && [ -z "$DAEMON_WH_PROXY" ]; then
  DAEMON_WH_PROXY="socks5://${TS_SOCKS5_ADDR}"
fi

echo "[entrypoint] Starting worker daemon..."
if [ -n "$DAEMON_WH_PROXY" ]; then
  exec env WH_PROXY="$DAEMON_WH_PROXY" python3 /worker_daemon.py
fi
exec python3 /worker_daemon.py
