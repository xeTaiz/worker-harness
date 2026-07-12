#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
bundle_mode=0
if [ ! -d "$script_dir/.git" ]; then
  bundle_mode=1
fi

service_src=""
for candidate in \
  "$script_dir/worker-harness.service" \
  "$script_dir/systemd/worker-harness.service"; do
  if [ -f "$candidate" ]; then
    service_src="$candidate"
    break
  fi
done

env_src=""
for candidate in \
  "$script_dir/.env" \
  "$script_dir/worker-harness.env"; do
  if [ -f "$candidate" ]; then
    env_src="$candidate"
    break
  fi
done

launcher_src="$script_dir/start-wh.sh"
image_src="$script_dir/worker-harness-worker.sif"

unit_dir="$HOME/.config/systemd/user"
config_dir="$HOME/.config/worker-harness"
service_dst="$unit_dir/worker-harness.service"
env_dst="$config_dir/worker-harness.env"

for path in "$service_src" "$launcher_src" "$image_src"; do
  if [ ! -f "$path" ]; then
    echo "[install-service] ERROR: missing $path" >&2
    exit 1
  fi
done

mkdir -p "$unit_dir" "$config_dir"
cp -f "$service_src" "$service_dst"
chmod +x "$launcher_src"

# Install update + restart path units (optional — only if source files exist)
for unit_src in \
  "$script_dir/worker-harness-update.path" \
  "$script_dir/worker-harness-update.service" \
  "$script_dir/worker-harness-restart.path" \
  "$script_dir/worker-harness-restart.service"; do
  if [ -f "$unit_src" ]; then
    cp -f "$unit_src" "$unit_dir/$(basename "$unit_src")"
  fi
done

# Copy the .env as-is to the config location, then prompt for any missing
# required values and patch them in place. This preserves all user-set vars
# (WH_EXTRA_BINDS, WH_MOUNT_HOME_FOLDERS, etc.) without lossy re-emission.
if [ -n "$env_src" ]; then
  cp -f "$env_src" "$env_dst"
else
  touch "$env_dst"
fi
chmod 600 "$env_dst"

# Source the installed env to check for missing required values
set -a
# shellcheck disable=SC1090
. "$env_dst"
set +a

_needed_patch=0
if [ -z "${TS_AUTHKEY:-${WORKER_TS_KEY:-}}" ]; then
  read -r -s -p "TS_AUTHKEY: " TS_AUTHKEY
  echo
  echo "export TS_AUTHKEY='$TS_AUTHKEY'" >> "$env_dst"
  _needed_patch=1
fi
if [ -z "${ORCHESTRATOR_HOST:-}" ]; then
  read -r -p "ORCHESTRATOR_HOST [orchestrator.hs.d0me.xyz]: " ORCHESTRATOR_HOST
  ORCHESTRATOR_HOST="${ORCHESTRATOR_HOST:-orchestrator.hs.d0me.xyz}"
  echo "export ORCHESTRATOR_HOST='$ORCHESTRATOR_HOST'" >> "$env_dst"
  _needed_patch=1
fi

if ! command -v systemctl >/dev/null 2>&1; then
  echo "[install-service] ERROR: systemctl not found" >&2
  exit 1
fi

systemctl --user daemon-reload
systemctl --user enable --now worker-harness.service

# Enable path units for image updates and restart triggers
systemctl --user enable worker-harness-update.path 2>/dev/null && systemctl --user start worker-harness-update.path 2>/dev/null || true
systemctl --user enable worker-harness-restart.path 2>/dev/null && systemctl --user start worker-harness-restart.path 2>/dev/null || true

echo "[install-service] installed: $service_dst"
echo "[install-service] env:       $env_dst"
echo "[install-service] launcher:  $launcher_src"
echo "[install-service] image:     $image_src"
echo "[install-service] path units: update + restart watchers enabled"

if command -v loginctl >/dev/null 2>&1; then
  if loginctl enable-linger "$USER" >/dev/null 2>&1; then
    echo "[install-service] linger enabled for $USER"
  else
    echo "[install-service] linger not enabled (you may need sudo); service will start on login only" >&2
  fi
fi

echo "[install-service] done"
