#!/bin/bash
set -euo pipefail

echo "[entrypoint] Starting worker bootstrap..."

# ── 1. ZeroTier bootstrap ──────────────────────────────────────────
if [ -n "${ZEROTIER_SECRET:-}" ]; then
    echo "[entrypoint] Writing ZeroTier identity secret..."
    mkdir -p /var/lib/zerotier-one
    echo "$ZEROTIER_SECRET" > /var/lib/zerotier-one/identity.secret
    chmod 600 /var/lib/zerotier-one/identity.secret
fi

if [ -n "${ZEROTIER_NETWORK_ID:-}" ]; then
    echo "[entrypoint] Starting ZeroTier and joining network $ZEROTIER_NETWORK_ID..."
    zerotier-one &
    ZT_PID=$!

    # Wait for ZeroTier to initialize
    sleep 3

    # Join the network (auto-authorized if the network has certificate policy,
    # otherwise authorize via ZeroTier Central)
    zerotier-one join "$ZEROTIER_NETWORK_ID"

    # Wait for an IP to be assigned
    echo "[entrypoint] Waiting for ZeroTier IP..."
    for i in $(seq 1 30); do
        ZT_IP=$(zerotier-cli -j listnetworks 2>/dev/null | jq -r \
            '.[] | select(.nwid == "'"$ZEROTIER_NETWORK_ID"'") | .assignedAddresses[0]' 2>/dev/null || true)
        if [ -n "$ZT_IP" ] && [ "$ZT_IP" != "null" ]; then
            echo "[entrypoint] ZeroTier IP assigned: $ZT_IP"
            break
        fi
        echo "[entrypoint] Waiting for IP... ($i/30)"
        sleep 2
    done
else
    echo "[entrypoint] WARNING: ZEROTIER_NETWORK_ID not set, skipping ZeroTier setup."
fi

# ── 2. SSH server ──────────────────────────────────────────────────
echo "[entrypoint] Configuring SSH server on port ${WORKER_SSH_PORT:-22}..."
sed -i "s/#Port 22/Port ${WORKER_SSH_PORT:-22}/" /etc/ssh/sshd_config
mkdir -p /ssh
chmod 700 /ssh

# Ensure sshd dir exists
mkdir -p /run/sshd

# Start SSH server in background
echo "[entrypoint] Starting SSH server..."
/usr/sbin/sshd

echo "[entrypoint] SSH server running on port ${WORKER_SSH_PORT:-22}"

# ── 3. Worker daemon ────────────────────────────────────────────────
echo "[entrypoint] Starting worker daemon..."
exec python3 /worker_daemon.py
