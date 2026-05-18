#!/bin/bash
set -euo pipefail

echo "[entrypoint] Starting worker bootstrap..."

# ── 1. Tailscale bootstrap ─────────────────────────────────────────
TS_AUTHKEY="${TS_AUTHKEY:-}"
TS_TAGS="${TS_TAGS:-tag:worker}"
TS_HOSTNAME="${TS_HOSTNAME:-}"
TS_ACCEPT_ROUTES="${TS_ACCEPT_ROUTES:-false}"
TS_EXTRA_ARGS="${TS_EXTRA_ARGS:-}"

if [ -z "$TS_AUTHKEY" ]; then
    echo "[entrypoint] ERROR: TS_AUTHKEY is required"
    exit 1
fi

mkdir -p /var/lib/tailscale /var/run/tailscale

echo "[entrypoint] Starting tailscaled..."
tailscaled \
  --state=/var/lib/tailscale/tailscaled.state \
  --socket=/var/run/tailscale/tailscaled.sock &

sleep 2

echo "[entrypoint] Joining tailnet with tags: $TS_TAGS"
UP_ARGS=(
  --socket=/var/run/tailscale/tailscaled.sock
  --authkey="$TS_AUTHKEY"
  --advertise-tags="$TS_TAGS"
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

tailscale up "${UP_ARGS[@]}"

echo "[entrypoint] Waiting for Tailnet IP..."
for i in $(seq 1 30); do
  TS_IP="$(tailscale --socket=/var/run/tailscale/tailscaled.sock ip -4 2>/dev/null | head -n1 || true)"
  if [ -n "$TS_IP" ]; then
    echo "[entrypoint] Tailnet IP assigned: $TS_IP"
    break
  fi
  echo "[entrypoint] Waiting for Tailnet IP... ($i/30)"
  sleep 2
done

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
echo "[entrypoint] Starting worker daemon..."
exec python3 /worker_daemon.py
