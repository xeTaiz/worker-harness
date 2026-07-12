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
launcher_dst="$HOME/start-wh.sh"
image_dst="$HOME/worker-harness-worker.sif"

for path in "$service_src" "$launcher_src" "$image_src"; do
  if [ ! -f "$path" ]; then
    echo "[install-service] ERROR: missing $path" >&2
    exit 1
  fi
done

mkdir -p "$unit_dir" "$config_dir"
cp -f "$service_src" "$service_dst"
cp -f "$launcher_src" "$launcher_dst"
cp -f "$image_src" "$image_dst"
chmod +x "$launcher_dst"

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

if [ -n "$env_src" ]; then
  set -a
  # shellcheck disable=SC1090
  . "$env_src"
  set +a
fi

TS_AUTHKEY="${TS_AUTHKEY:-${WORKER_TS_KEY:-}}"
if [ -z "${TS_AUTHKEY:-}" ]; then
  read -r -s -p "TS_AUTHKEY: " TS_AUTHKEY
  echo
fi
if [ -z "${ORCHESTRATOR_HOST:-}" ]; then
  read -r -p "ORCHESTRATOR_HOST [orchestrator.hs.d0me.xyz]: " ORCHESTRATOR_HOST
  ORCHESTRATOR_HOST="${ORCHESTRATOR_HOST:-orchestrator.hs.d0me.xyz}"
fi
TS_HOST="${TS_HOST:-https://headscale.d0me.xyz}"
WH_DIR="${WH_DIR:-$HOME/.local/worker-harness}"

cat > "$env_dst" <<EOF
export TS_AUTHKEY='${TS_AUTHKEY}'
export ORCHESTRATOR_HOST='${ORCHESTRATOR_HOST}'
export TS_HOST='${TS_HOST}'
export WH_DIR="${WH_DIR}"
EOF
chmod 600 "$env_dst"

if [ "$bundle_mode" -eq 1 ] && [ -n "$env_src" ] && [ "$env_src" != "$env_dst" ]; then
  rm -f "$env_src"
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
echo "[install-service] launcher:  $launcher_dst -> $launcher_src"
echo "[install-service] image:     $image_dst -> $image_src"
echo "[install-service] path units: update + restart watchers enabled"

if command -v loginctl >/dev/null 2>&1; then
  if loginctl enable-linger "$USER" >/dev/null 2>&1; then
    echo "[install-service] linger enabled for $USER"
  else
    echo "[install-service] linger not enabled (you may need sudo); service will start on login only" >&2
  fi
fi

echo "[install-service] done"
