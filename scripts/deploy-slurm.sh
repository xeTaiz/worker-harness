#!/usr/bin/env bash
set -euo pipefail

# Update a Slurm-launched worker installation. These nodes have no systemd user
# services and no rclone: a job runs `bash <dir>/start-wh.sh` directly, so only
# the launcher and the Slurm helpers need to be current. The directory is a
# plain path, which may be the cluster filesystem itself or its mount on any
# worker (for example /data/shared/ibex/worker-harness).

target="${1:?usage: deploy-slurm.sh <install-dir> [--with-image]}"
with_image=0
if [ "${2:-}" = "--with-image" ]; then
  with_image=1
elif [ -n "${2:-}" ]; then
  echo "[deploy-slurm] ERROR: unknown argument: $2" >&2
  exit 1
fi

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"

if [ ! -d "$target" ]; then
  echo "[deploy-slurm] ERROR: install directory does not exist: $target" >&2
  exit 1
fi

# Replace each file through a temporary name in the destination directory so a
# job starting mid-update reads either the old or the new file, never a partial
# one. Slurm nodes may be reading these paths right now.
install_file() {
  local src="$1" dst="$2" tmp
  tmp="$(mktemp "${dst}.deploy-XXXXXX")"
  cat "$src" > "$tmp"
  chmod 755 "$tmp"
  mv -f "$tmp" "$dst"
  echo "[deploy-slurm] updated $dst"
}

install_file start-wh.sh "$target/start-wh.sh"

mkdir -p "$target/slurm"
for helper in slurm/*.sh; do
  install_file "$helper" "$target/slurm/$(basename "$helper")"
done

if [ "$with_image" -eq 1 ]; then
  if [ ! -f worker-harness-worker.sif ]; then
    echo "[deploy-slurm] ERROR: worker-harness-worker.sif is missing; run just build-singularity" >&2
    exit 1
  fi
  # A queued job may hold the current image open; write beside it and swap.
  tmp_image="$(mktemp "$target/worker-harness-worker.sif.deploy-XXXXXX")"
  cat worker-harness-worker.sif > "$tmp_image"
  chmod 644 "$tmp_image"
  mv -f "$tmp_image" "$target/worker-harness-worker.sif"
  echo "[deploy-slurm] updated $target/worker-harness-worker.sif"
fi

echo "[deploy-slurm] done; already-running jobs keep their current mounts until they chain"
