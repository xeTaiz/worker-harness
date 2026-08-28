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

rclone_config_src="$script_dir/rclone.conf"
rclone_config_dst="$HOME/.config/rclone/rclone.conf"

shopt -s nullglob
rclone_units=("$script_dir"/rclone-*.service)
if [ "${#rclone_units[@]}" -eq 0 ]; then
  rclone_units=("$script_dir"/systemd/rclone-*.service)
fi
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

run_as_root() {
  if [ "$EUID" -eq 0 ]; then
    "$@"
  elif command -v sudo >/dev/null 2>&1; then
    sudo "$@"
  else
    echo "[install-service] ERROR: root access is required to install host dependencies" >&2
    return 1
  fi
}

install_host_packages() {
  local packages=("$@")
  [ "${#packages[@]}" -gt 0 ] || return 0

  if command -v apt-get >/dev/null 2>&1; then
    run_as_root apt-get update
    run_as_root env DEBIAN_FRONTEND=noninteractive apt-get install -y "${packages[@]}"
  elif command -v pacman >/dev/null 2>&1; then
    run_as_root pacman -S --needed --noconfirm "${packages[@]}"
  elif command -v dnf >/dev/null 2>&1; then
    run_as_root dnf install -y "${packages[@]}"
  elif command -v zypper >/dev/null 2>&1; then
    run_as_root zypper --non-interactive install "${packages[@]}"
  else
    echo "[install-service] ERROR: no supported package manager found for host dependencies" >&2
    return 1
  fi
}

install_rclone_dependencies() {
  local packages=()
  command -v curl >/dev/null 2>&1 || packages+=(curl)
  command -v unzip >/dev/null 2>&1 || packages+=(unzip)
  command -v fusermount3 >/dev/null 2>&1 || packages+=(fuse3)
  install_host_packages "${packages[@]}"

  if ! command -v rclone >/dev/null 2>&1 || ! rclone help backend smb >/dev/null 2>&1; then
    echo "[install-service] installing the official rclone release"
    curl -fsSL https://rclone.org/install.sh | run_as_root bash
  fi

  command -v rclone >/dev/null 2>&1
  rclone help backend smb >/dev/null 2>&1
  command -v fusermount3 >/dev/null 2>&1
}

rclone_bind_destination() {
  local mount_name
  mount_name="$(basename "$1")"
  case "$mount_name" in
    datawaha) printf '/data_shared\n' ;;
    ibex) printf '/data_ibex\n' ;;
    ibex_c2324) printf '/data_ibex_c2324\n' ;;
    *) printf '/data_%s\n' "$mount_name" ;;
  esac
}

append_semicolon_value() {
  local current="$1" value="$2"
  if [ -z "$current" ]; then
    printf '%s\n' "$value"
  elif [[ ";$current;" == *";$value;"* ]]; then
    printf '%s\n' "$current"
  else
    printf '%s;%s\n' "$current" "$value"
  fi
}

write_extra_binds() {
  local value="$1" tmp line
  tmp="$(mktemp "${env_src}.tmp.XXXXXX")"
  while IFS= read -r line || [ -n "$line" ]; do
    if [[ "$line" =~ ^[[:space:]]*(export[[:space:]]+)?WH_EXTRA_BINDS= ]]; then
      continue
    fi
    printf '%s\n' "$line" >> "$tmp"
  done < "$env_src"
  printf 'export WH_EXTRA_BINDS=%q\n' "$value" >> "$tmp"
  chmod 600 "$tmp"
  mv "$tmp" "$env_src"
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

valid_rclone_units=()
configured_rclone_mounts=()
if [ "${#rclone_units[@]}" -gt 0 ]; then
  if [ ! -f "$rclone_config_src" ]; then
    echo "[install-service] WARNING: rclone services found without rclone.conf; skipping them" >&2
  else
    install_rclone_dependencies
    mkdir -p "$(dirname "$rclone_config_dst")"
    chmod 600 "$rclone_config_src"
    link_file "$rclone_config_src" "$rclone_config_dst"

    for unit_src in "${rclone_units[@]}"; do
      unit_name="$(basename "$unit_src")"
      exec_start=""
      while IFS= read -r line; do
        if [[ "$line" == ExecStart=* ]]; then
          exec_start="${line#ExecStart=}"
          break
        fi
      done < "$unit_src"

      read -r rclone_bin rclone_action remote_spec mount_spec _ <<< "$exec_start"
      if [ "$rclone_action" != "mount" ] || [ -z "${remote_spec:-}" ] || [ -z "${mount_spec:-}" ]; then
        echo "[install-service] WARNING: cannot parse $unit_name; skipping it" >&2
        continue
      fi

      mount_dir="${mount_spec//\%h/$HOME}"
      configured_rclone_mounts+=("$mount_dir")
      mkdir -p "$mount_dir"
      echo "[install-service] checking $remote_spec for $unit_name"
      if rclone lsf --config="$rclone_config_dst" --max-depth 1 "$remote_spec" >/dev/null; then
        link_file "$unit_src" "$unit_dir/$unit_name"
        valid_rclone_units+=("$unit_name|$mount_dir")
      else
        echo "[install-service] WARNING: $remote_spec is unavailable; skipping $unit_name" >&2
        systemctl --user disable --now "$unit_name" >/dev/null 2>&1 || true
      fi
    done
  fi
fi

systemctl --user daemon-reload

working_rclone_binds=()
for unit_and_mount in "${valid_rclone_units[@]}"; do
  unit_name="${unit_and_mount%%|*}"
  mount_dir="${unit_and_mount#*|}"
  systemctl --user enable --now "$unit_name"
  for _attempt in {1..20}; do
    mountpoint -q "$mount_dir" && break
    sleep 0.25
  done
  if systemctl --user is-active --quiet "$unit_name" && mountpoint -q "$mount_dir"; then
    container_dir="$(rclone_bind_destination "$mount_dir")"
    working_rclone_binds+=("$mount_dir:$container_dir")
    echo "[install-service] mounted: $mount_dir -> $container_dir"
  else
    echo "[install-service] WARNING: $unit_name did not mount $mount_dir; disabling it" >&2
    systemctl --user disable --now "$unit_name" >/dev/null 2>&1 || true
  fi
done

extra_binds="${WH_EXTRA_BINDS:-}"
if [ "${#configured_rclone_mounts[@]}" -gt 0 ]; then
  filtered_extra_binds=""
  IFS=';' read -ra existing_bind_pairs <<< "$extra_binds"
  for bind_pair in "${existing_bind_pairs[@]}"; do
    [ -n "$bind_pair" ] || continue
    keep_bind=1
    for mount_dir in "${configured_rclone_mounts[@]}"; do
      if [ "${bind_pair%%:*}" = "$mount_dir" ]; then
        keep_bind=0
        break
      fi
    done
    if [ "$keep_bind" -eq 1 ]; then
      filtered_extra_binds="$(append_semicolon_value "$filtered_extra_binds" "$bind_pair")"
    fi
  done
  extra_binds="$filtered_extra_binds"
fi
for bind_pair in "${working_rclone_binds[@]}"; do
  extra_binds="$(append_semicolon_value "$extra_binds" "$bind_pair")"
done
if [ "${#configured_rclone_mounts[@]}" -gt 0 ]; then
  write_extra_binds "$extra_binds"
fi

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
