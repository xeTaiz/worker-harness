#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
service_src="$repo_dir/systemd/worker-harness.service"
env_src="$repo_dir/systemd/worker-harness.env"
launcher_src="$repo_dir/start-wh.sh"
image_src="$repo_dir/worker-harness-worker.sif"

unit_dir="$HOME/.config/systemd/user"
config_dir="$HOME/.config/worker-harness"
service_dst="$unit_dir/worker-harness.service"
env_dst="$config_dir/worker-harness.env"
launcher_dst="$HOME/start-wh.sh"
image_dst="$HOME/worker-harness-worker.sif"

if [ ! -f "$service_src" ]; then
  echo "[install-service] ERROR: missing $service_src" >&2
  exit 1
fi
if [ ! -f "$env_src" ]; then
  echo "[install-service] ERROR: missing $env_src" >&2
  exit 1
fi
if [ ! -f "$launcher_src" ]; then
  echo "[install-service] ERROR: missing $launcher_src" >&2
  exit 1
fi
if [ ! -f "$image_src" ]; then
  echo "[install-service] ERROR: missing $image_src" >&2
  exit 1
fi

mkdir -p "$unit_dir" "$config_dir"
cp "$service_src" "$service_dst"
cp "$env_src" "$env_dst"
chmod 600 "$env_dst"

ln -sfn "$launcher_src" "$launcher_dst"
ln -sfn "$image_src" "$image_dst"

if ! command -v systemctl >/dev/null 2>&1; then
  echo "[install-service] ERROR: systemctl not found" >&2
  exit 1
fi

systemctl --user daemon-reload
systemctl --user enable --now worker-harness.service

echo "[install-service] installed: $service_dst"
echo "[install-service] env:       $env_dst"
echo "[install-service] launcher:  $launcher_dst -> $launcher_src"
echo "[install-service] image:     $image_dst -> $image_src"

if command -v loginctl >/dev/null 2>&1; then
  if loginctl enable-linger "$USER" >/dev/null 2>&1; then
    echo "[install-service] linger enabled for $USER"
  else
    echo "[install-service] linger not enabled (you may need sudo); service will start on login only" >&2
  fi
fi

echo "[install-service] done"
