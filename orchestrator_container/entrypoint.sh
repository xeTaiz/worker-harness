#!/bin/bash
set -euo pipefail

echo "[entrypoint] Starting orchestrator bootstrap..."

TS_AUTHKEY="${TS_AUTHKEY:-}"
TS_HOST="${TS_HOST:-https://controlplane.tailscale.com}"
TS_HOSTNAME="${TS_HOSTNAME:-orchestrator}"
TS_ACCEPT_ROUTES="${TS_ACCEPT_ROUTES:-false}"
TS_EXTRA_ARGS="${TS_EXTRA_ARGS:-}"
TS_UP_TIMEOUT="${TS_UP_TIMEOUT:-30}"
WH_COMMAND="${WH_COMMAND:-serve}"
TS_STATE_FILE="/var/lib/tailscale/tailscaled.state"
TS_SOCKET="/var/run/tailscale/tailscaled.sock"

if [ -z "$TS_AUTHKEY" ] && [ ! -s "$TS_STATE_FILE" ]; then
  echo "[entrypoint] ERROR: TS_AUTHKEY is required for initial Tailnet enrollment"
  exit 1
fi

mkdir -p /var/lib/tailscale /var/run/tailscale /root/.config/worker-harness

echo "[entrypoint] Starting tailscaled..."
tailscaled \
  --state="$TS_STATE_FILE" \
  --socket="$TS_SOCKET" &
TAILSCALED_PID=$!

bootstrap_tailnet() {
  echo "[entrypoint] Tailnet bootstrap running asynchronously"
  for attempt in $(seq 1 30); do
    if tailscale --socket="$TS_SOCKET" status --json >/dev/null 2>&1; then
      break
    fi
    if ! kill -0 "$TAILSCALED_PID" 2>/dev/null; then
      echo "[entrypoint] ERROR: tailscaled exited before its local API became ready"
      return 1
    fi
    sleep 1
  done

  UP_ARGS=(
    --login-server="$TS_HOST"
    --hostname="$TS_HOSTNAME"
    --accept-routes="$TS_ACCEPT_ROUTES"
  )
  if [ -n "$TS_AUTHKEY" ]; then
    UP_ARGS+=(--authkey="$TS_AUTHKEY")
  fi
  if [ -n "$TS_EXTRA_ARGS" ]; then
    # shellcheck disable=SC2206
    EXTRA_ARGS=( $TS_EXTRA_ARGS )
    UP_ARGS+=("${EXTRA_ARGS[@]}")
  fi

  while kill -0 "$TAILSCALED_PID" 2>/dev/null; do
    echo "[entrypoint] Joining or restoring Tailnet identity..."
    if timeout "$TS_UP_TIMEOUT" tailscale --socket="$TS_SOCKET" up "${UP_ARGS[@]}"; then
      for attempt in $(seq 1 30); do
        TS_IP="$(tailscale --socket="$TS_SOCKET" ip -4 2>/dev/null | head -n1 || true)"
        if [ -n "$TS_IP" ]; then
          echo "[entrypoint] Tailnet ready: $TS_HOSTNAME at $TS_IP"
          return 0
        fi
        sleep 1
      done
      echo "[entrypoint] WARN: tailscale up succeeded but no IPv4 address appeared"
    else
      echo "[entrypoint] WARN: Tailnet join did not complete within ${TS_UP_TIMEOUT}s; retrying in 5s"
    fi
    sleep 5
  done
  echo "[entrypoint] ERROR: tailscaled exited during Tailnet bootstrap"
  return 1
}

# Keep this shell as the direct parent of tailscaled, bootstrap, and Python so
# it can supervise and reap each child. Tini remains PID 1 and subreaps any
# orphaned SSH ProxyCommand descendants.
shutdown_children() {
  status=$?
  trap - EXIT INT TERM

  pids=("${ORCH_PID:-}" "${BOOTSTRAP_PID:-}" "$TAILSCALED_PID")
  for pid in "${pids[@]}"; do
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
      kill -TERM "$pid" 2>/dev/null || true
    fi
  done

  for _ in $(seq 1 50); do
    live=false
    for pid in "${pids[@]}"; do
      if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        live=true
      fi
    done
    if [ "$live" = false ]; then
      break
    fi
    sleep 0.1
  done

  for pid in "${pids[@]}"; do
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
      kill -KILL "$pid" 2>/dev/null || true
    fi
    if [ -n "$pid" ]; then
      wait "$pid" 2>/dev/null || true
    fi
  done
  exit "$status"
}

trap 'exit 143' TERM
trap 'exit 130' INT
trap shutdown_children EXIT

# Tailnet control-plane latency must not gate the local registration/control
# servers. Once tailscaled restores the persisted identity, the already-running
# HTTP services become reachable immediately on the same Tailnet IP.
bootstrap_tailnet &
BOOTSTRAP_PID=$!

echo "[entrypoint] Starting orchestrator immediately: python -m worker_harness.orchestrator $WH_COMMAND"
python -m worker_harness.orchestrator "$WH_COMMAND" &
ORCH_PID=$!

set +e
wait -n -p EXITED_PID "$ORCH_PID" "$TAILSCALED_PID"
status=$?
set -e

if [ "$EXITED_PID" = "$TAILSCALED_PID" ]; then
  echo "[entrypoint] ERROR: tailscaled exited; stopping orchestrator"
else
  echo "[entrypoint] Orchestrator exited with status $status"
fi
exit "$status"
