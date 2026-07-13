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

# Keep host-side service configuration as links into ~/worker-harness rather
# than copies. Updating a tracked script or unit there then needs only a
# daemon-reload/restart, never another install/re-copy pass. Preserve any
# pre-existing unmanaged file so migration cannot silently discard settings.
link_file() {
  local src="$1" dst="$2" backup
  if [ -e "$dst" ] && [ ! -L "$dst" ]; then
    backup="${dst}.pre-symlink.$(date +%Y%m%d%H%M%S)"
    mv "$dst" "$backup"
    echo "[install-service] backed up $dst -> $backup"
  fi
  ln -sfnT "$src" "$dst"
}

find_optional_source() {
  local name="$1" candidate
  for candidate in "$script_dir/$name" "$script_dir/systemd/$name"; do
    if [ -f "$candidate" ]; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done
  return 1
}

for path in "$service_src" "$launcher_src" "$image_src"; do
  if [ ! -f "$path" ]; then
    echo "[install-service] ERROR: missing $path" >&2
    exit 1
  fi
done

mkdir -p "$unit_dir" "$config_dir"
link_file "$service_src" "$service_dst"
chmod +x "$launcher_src"

# Link update + restart units (when supplied by either a dist bundle or the
# repository's systemd/ directory).
for unit_name in \
  worker-harness-update.path \
  worker-harness-update.service \
  worker-harness-restart.path \
  worker-harness-restart.service; do
  if unit_src="$(find_optional_source "$unit_name")"; then
    link_file "$unit_src" "$unit_dir/$unit_name"
  fi
done

# Link the scripts referenced by the path-triggered services. Their stable
# ~/.config paths remain unchanged; the source of truth is ~/worker-harness.
for script_name in worker-harness-update.sh worker-harness-restart.sh; do
  if script_src="$(find_optional_source "$script_name")"; then
    chmod +x "$script_src"
    link_file "$script_src" "$config_dir/$script_name"
  fi
done

# Keep the env file in ~/worker-harness too. It is intentionally mutable and
# ignored by git, so prompts below update the linked source file in place.
# A pre-symlink config env is authoritative: it may contain local edits or
# secrets made before this migration, so preserve it as the source of truth.
if [ -z "$env_src" ]; then
  env_src="$script_dir/.env"
fi
if [ -e "$env_dst" ] && [ ! -L "$env_dst" ]; then
  if [ -e "$env_src" ]; then
    backup="${env_src}.pre-config-migration.$(date +%Y%m%d%H%M%S)"
    mv "$env_src" "$backup"
    echo "[install-service] backed up $env_src -> $backup"
  fi
  mv "$env_dst" "$env_src"
  echo "[install-service] migrated $env_dst -> $env_src"
elif [ ! -e "$env_src" ]; then
  touch "$env_src"
fi
chmod 600 "$env_src"
link_file "$env_src" "$env_dst"

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
