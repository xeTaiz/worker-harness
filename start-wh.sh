#!/usr/bin/env bash
set -euo pipefail

runtime="${WH_RUNTIME:-}"
if [ -z "$runtime" ]; then
  if command -v singularity >/dev/null 2>&1; then
    runtime=singularity
  elif command -v apptainer >/dev/null 2>&1; then
    runtime=apptainer
  else
    echo "[start-wh] ERROR: need singularity or apptainer in PATH" >&2
    exit 1
  fi
fi

load_env_file() {
  local env_file="$1"
  if [ -n "$env_file" ] && [ -f "$env_file" ]; then
    # shellcheck disable=SC1090
    set -a
    . "$env_file"
    set +a
    return 0
  fi
  return 1
}

if ! load_env_file "${WH_ENV_FILE:-}"; then
  load_env_file "$PWD/.env" || true
  load_env_file "$PWD/worker-harness.env" || true
  load_env_file "$HOME/.config/worker-harness/worker-harness.env" || true
fi

image="${WH_IMAGE:-${1:-worker-harness-worker.sif}}"
wh_dir_host="${WH_DIR:-$HOME/.local/worker-harness}"
wh_dir_container="${WH_CONTAINER_DIR:-/var/lib/worker-harness}"
ssh_user="${SSH_USER:-$(id -un)}"
ssh_uid="$(id -u)"
ssh_gid="$(id -g)"
ssh_shell="${SSH_SHELL:-/bin/bash}"
ssh_home_container="${wh_dir_container}/home/${ssh_user}"
compat_dir="${wh_dir_host}/compat"
passwd_file="${compat_dir}/passwd"
group_file="${compat_dir}/group"
launch_mode="${WH_LAUNCH_MODE:-instance}"
instance_name="${WH_INSTANCE_NAME:-wh-${ssh_user}}"
fakeroot_flag=""
if [ -n "${WH_FAKEROOT:-}" ]; then
  case "${WH_FAKEROOT}" in
    1|true|yes|on) fakeroot_flag="--fakeroot" ;;
    0|false|no|off) fakeroot_flag="" ;;
    *) echo "[start-wh] ERROR: WH_FAKEROOT must be 0/1 or false/true" >&2; exit 1 ;;
  esac
elif grep -q "^${ssh_user}:" /etc/subuid 2>/dev/null && grep -q "^${ssh_user}:" /etc/subgid 2>/dev/null; then
  fakeroot_flag="--fakeroot"
fi

TS_AUTHKEY="${TS_AUTHKEY:-${WORKER_TS_KEY:-}}"

if [ -z "${TS_AUTHKEY:-}" ]; then
  echo "[start-wh] ERROR: TS_AUTHKEY is required" >&2
  exit 1
fi
if [ -z "${ORCHESTRATOR_HOST:-}" ]; then
  echo "[start-wh] ERROR: ORCHESTRATOR_HOST is required" >&2
  exit 1
fi

mkdir -p "$wh_dir_host" "$compat_dir" "${wh_dir_host}/home/${ssh_user}"

# ── Writable overlay (persistent apt installs across restarts) ───────
overlay_file="${WH_OVERLAY:-$wh_dir_host/overlay.ext3}"
overlay_size="${WH_OVERLAY_SIZE:-8192}"   # MiB (8 GB default)
if [ ! -f "$overlay_file" ]; then
  if "$runtime" overlay create --size "$overlay_size" "$overlay_file" 2>/dev/null; then
    echo "[start-wh] Created ${overlay_size}MiB writable overlay at $overlay_file"
  else
    echo "[start-wh] WARNING: could not create overlay ($overlay_file). Continuing without it." >&2
    echo "[start-wh]   (Needs fakeroot/root + mkfs.ext3. Set WH_OVERLAY to a pre-created file to skip this.)" >&2
    overlay_file=""
  fi
fi

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

base_passwd="${tmpdir}/passwd"
base_group="${tmpdir}/group"
"$runtime" exec --cleanenv "$image" cat /etc/passwd >"$base_passwd"
"$runtime" exec --cleanenv "$image" cat /etc/group >"$base_group"

python3 - "$base_passwd" "$base_group" "$passwd_file" "$group_file" "$ssh_user" "$ssh_uid" "$ssh_gid" "$ssh_home_container" "$ssh_shell" "$(id -G)" <<'PY'
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

base_passwd = Path(sys.argv[1])
base_group = Path(sys.argv[2])
out_passwd = Path(sys.argv[3])
out_group = Path(sys.argv[4])
user = sys.argv[5]
uid = sys.argv[6]
gid = sys.argv[7]
home = sys.argv[8]
shell = sys.argv[9]
host_group_ids = [g for g in sys.argv[10].split() if g]

passwd_lines = []
for line in base_passwd.read_text().splitlines():
    if not line:
        continue
    if line.split(":", 1)[0] == user:
        continue
    passwd_lines.append(line)
passwd_lines.append(f"{user}:x:{uid}:{gid}:Worker Harness User:{home}:{shell}")
out_passwd.write_text("\n".join(passwd_lines) + "\n")

group_lines = []
by_gid: dict[str, int] = {}
for idx, line in enumerate(base_group.read_text().splitlines()):
    if not line:
        continue
    parts = line.split(":")
    if len(parts) < 4:
        group_lines.append(line)
        continue
    line_gid = parts[2]
    by_gid[line_gid] = idx
    group_lines.append(line)

for group_id in host_group_ids:
    try:
        resolved = subprocess.check_output(["getent", "group", group_id], text=True).strip()
    except subprocess.CalledProcessError:
        resolved = ""
    group_name = resolved.split(":", 1)[0] if resolved else f"whg{group_id}"

    if group_id in by_gid:
        idx = by_gid[group_id]
        parts = group_lines[idx].split(":")
        while len(parts) < 4:
            parts.append("")
        members = [m for m in parts[3].split(",") if m]
        if user not in members:
            members.append(user)
        parts[3] = ",".join(members)
        group_lines[idx] = ":".join(parts[:4])
    else:
        group_lines.append(f"{group_name}:x:{group_id}:{user}")

out_group.write_text("\n".join(group_lines) + "\n")
PY

mount_args=($fakeroot_flag --nv \
  --home "$wh_dir_host/home/$ssh_user:$ssh_home_container" \
  --bind "$wh_dir_host:$wh_dir_container" \
  --bind "$passwd_file:/etc/passwd" \
  --bind "$group_file:/etc/group" \
  --workdir "$ssh_home_container")

# Add writable overlay if available (allows persistent apt installs)
if [ -n "$overlay_file" ] && [ -f "$overlay_file" ]; then
  mount_args+=(--overlay "$overlay_file")
fi

# Extra bind mounts (colon-separated host:container pairs)
# e.g. WH_EXTRA_BINDS="$HOME/Dev:/code:/data:/data"
if [ -n "${WH_EXTRA_BINDS:-}" ]; then
  IFS=':' read -ra _extra_pairs <<< "$WH_EXTRA_BINDS"
  for _pair in "${_extra_pairs[@]}"; do
    mount_args+=(--bind "$_pair")
  done
fi

# Auto-mount non-hidden home directories at the same path inside the container.
# The glob */ naturally excludes .ssh, .gnupg, .config, .aws, .cache, etc.
# Enable with WH_MOUNT_HOME_FOLDERS=1
if [ "${WH_MOUNT_HOME_FOLDERS:-1}" = "1" ]; then
  for _dir in "$HOME"/*/; do
    _name="$(basename "$_dir")"
    mount_args+=(--bind "${_dir%/}:$HOME/$_name")
  done
fi

exec_env_args=(
  --env TS_AUTHKEY="$TS_AUTHKEY"
  --env TS_HOST="${TS_HOST:-https://headscale.d0me.xyz}"
  --env ORCHESTRATOR_HOST="$ORCHESTRATOR_HOST"
  --env SSH_USER="$ssh_user"
  --env USER="$ssh_user"
  --env LOGNAME="$ssh_user"
  --env WH_DIR="$wh_dir_container"
  --env WH_PROXY="${WH_PROXY:-socks5://127.0.0.1:1055}"
)

if [ "$launch_mode" = "instance" ]; then
  # Stop any leftover instance from a previous run (crash, restart, etc.)
  "$runtime" instance stop "$instance_name" 2>/dev/null || true
  echo "[start-wh] Starting instance $instance_name using $runtime..."
  "$runtime" instance start --cleanenv "${mount_args[@]}" "$image" "$instance_name"
  exec "$runtime" exec --cleanenv "${exec_env_args[@]}" instance://"$instance_name" /entrypoint.sh
fi

echo "[start-wh] Starting one-shot container using $runtime..."
exec "$runtime" run --cleanenv "${mount_args[@]}" "${exec_env_args[@]}" "$image"
