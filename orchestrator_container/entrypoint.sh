#!/bin/bash
set -euo pipefail

echo "[entrypoint] Starting orchestrator bootstrap..."

TS_AUTHKEY="${TS_AUTHKEY:-}"
TS_HOST="${TS_HOST:-https://controlplane.tailscale.com}"
TS_HOSTNAME="${TS_HOSTNAME:-orchestrator}"
TS_ACCEPT_ROUTES="${TS_ACCEPT_ROUTES:-false}"
TS_EXTRA_ARGS="${TS_EXTRA_ARGS:-}"
WH_COMMAND="${WH_COMMAND:-serve}"
SSH_KEY_PATH="${SSH_KEY_PATH:-/opt/worker-harness/ssh/orchestrator_ed25519}"

if [ -z "$TS_AUTHKEY" ]; then
  echo "[entrypoint] ERROR: TS_AUTHKEY is required"
  exit 1
fi

mkdir -p /var/lib/tailscale /var/run/tailscale /root/.config/worker-harness

if [ ! -f "$SSH_KEY_PATH" ]; then
  echo "[entrypoint] ERROR: SSH private key not found at $SSH_KEY_PATH"
  echo "[entrypoint] Build orchestrator image via 'just build' so key is baked in"
  exit 1
fi
chmod 600 "$SSH_KEY_PATH"
export SSH_KEY_PATH

echo "[entrypoint] Starting tailscaled..."
tailscaled \
  --state=/var/lib/tailscale/tailscaled.state \
  --socket=/var/run/tailscale/tailscaled.sock &

sleep 2

echo "[entrypoint] Joining tailnet..."
UP_ARGS=(
  --login-server="$TS_HOST"
  --authkey="$TS_AUTHKEY"
  --hostname="$TS_HOSTNAME"
  --accept-routes="$TS_ACCEPT_ROUTES"
)

if [ -n "$TS_EXTRA_ARGS" ]; then
  # shellcheck disable=SC2206
  EXTRA_ARGS=( $TS_EXTRA_ARGS )
  UP_ARGS+=("${EXTRA_ARGS[@]}")
fi

tailscale --socket=/var/run/tailscale/tailscaled.sock up "${UP_ARGS[@]}"

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

echo "[entrypoint] Starting orchestrator: python -m worker_harness.orchestrator $WH_COMMAND"
exec python -m worker_harness.orchestrator "$WH_COMMAND"
