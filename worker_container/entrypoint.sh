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
    sleep 3

    zerotier-cli join "$ZEROTIER_NETWORK_ID"

    # Wait for an IP: parse zerotier-cli output
    # Line format: 200 listnetworks <nwid> <name> <mac> <status> <type> <dev> <IP/NM>
    # IP is the last field. Strip /NETMASK suffix before matching.
    echo "[entrypoint] Waiting for ZeroTier IP..."
    for i in $(seq 1 30); do
        ZT_IP="$(
            zerotier-cli listnetworks 2>/dev/null | \
            awk -v netid="$ZEROTIER_NETWORK_ID" '
                $2 == "listnetworks" && $3 == netid {
                    for (n = NF; n >= 1; n--) {
                        gsub(/\/.*/, "", $n)
                        if ($n ~ /^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$/) {
                            print $n
                            exit
                        }
                    }
                }'
        )"
        if [ -n "$ZT_IP" ]; then
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
mkdir -p /run/sshd
/usr/sbin/sshd
echo "[entrypoint] SSH server running on port ${WORKER_SSH_PORT:-22}"

# ── 3. Worker daemon ────────────────────────────────────────────────
echo "[entrypoint] Starting worker daemon..."
exec python3 /worker_daemon.py