#!/usr/bin/env bash
# One-time host-side migration for worker-harness installations that predate
# symlinked systemd configuration. Run from ~/worker-harness on the host.
set -euo pipefail

install_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
unit_dir="$HOME/.config/systemd/user"
config_dir="$HOME/.config/worker-harness"
mkdir -p "$unit_dir" "$config_dir"

find_source() {
  local name="$1" candidate
  for candidate in "$install_dir/$name" "$install_dir/systemd/$name"; do
    if [ -f "$candidate" ]; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done
  return 1
}

link_file() {
  local src="$1" dst="$2" backup
  if [ -e "$dst" ] && [ ! -L "$dst" ]; then
    backup="${dst}.pre-symlink.$(date +%Y%m%d%H%M%S)"
    mv "$dst" "$backup"
    echo "[migrate] backed up $dst -> $backup"
  fi
  ln -sfnT "$src" "$dst"
  echo "[migrate] linked $dst -> $src"
}

# A regular config env is authoritative: preserve local edits/secrets by
# moving it into the install directory before replacing its old path with a
# symlink. Existing install-dir env is retained as a timestamped backup.
env_src="$(find_source .env || true)"
if [ -z "$env_src" ]; then
  env_src="$install_dir/.env"
fi
if [ -e "$config_dir/worker-harness.env" ] && [ ! -L "$config_dir/worker-harness.env" ]; then
  if [ -e "$env_src" ]; then
    backup="${env_src}.pre-config-migration.$(date +%Y%m%d%H%M%S)"
    mv "$env_src" "$backup"
    echo "[migrate] backed up $env_src -> $backup"
  fi
  mv "$config_dir/worker-harness.env" "$env_src"
  echo "[migrate] migrated config env -> $env_src"
elif [ ! -e "$env_src" ]; then
  touch "$env_src"
fi
chmod 600 "$env_src"
link_file "$env_src" "$config_dir/worker-harness.env"

for unit_name in \
  worker-harness.service \
  worker-harness-update.path \
  worker-harness-update.service \
  worker-harness-restart.path \
  worker-harness-restart.service; do
  if unit_src="$(find_source "$unit_name")"; then
    link_file "$unit_src" "$unit_dir/$unit_name"
  else
    echo "[migrate] WARN: source unit missing: $unit_name" >&2
  fi
done

# The updated update script is expected in the install directory. The restart
# script may exist only under old ~/.config; migrate that copy if necessary.
if update_src="$(find_source worker-harness-update.sh)"; then
  chmod +x "$update_src"
  link_file "$update_src" "$config_dir/worker-harness-update.sh"
else
  echo "[migrate] ERROR: worker-harness-update.sh is missing from $install_dir" >&2
  exit 1
fi

if ! restart_src="$(find_source worker-harness-restart.sh)"; then
  if [ -f "$config_dir/worker-harness-restart.sh" ] && [ ! -L "$config_dir/worker-harness-restart.sh" ]; then
    restart_src="$install_dir/worker-harness-restart.sh"
    mv "$config_dir/worker-harness-restart.sh" "$restart_src"
    echo "[migrate] migrated restart script -> $restart_src"
  else
    echo "[migrate] WARN: restart script source missing" >&2
    restart_src=""
  fi
fi
if [ -n "$restart_src" ]; then
  chmod +x "$restart_src"
  link_file "$restart_src" "$config_dir/worker-harness-restart.sh"
fi

systemctl --user daemon-reload
systemctl --user restart worker-harness.service || echo "[migrate] WARN: worker service restart returned nonzero; checking settled state"
for path_unit in worker-harness-update.path worker-harness-restart.path; do
  systemctl --user restart "$path_unit" || echo "[migrate] WARN: could not restart $path_unit"
done

sleep 3
if systemctl --user is-active --quiet worker-harness.service; then
  echo "[migrate] complete: worker-harness.service is active"
else
  echo "[migrate] ERROR: worker-harness.service is not active after migration" >&2
  exit 1
fi
