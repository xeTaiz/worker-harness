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
    # Variables the caller already exported (e.g. a Slurm script's WH_DIR)
    # must win over this file — it only supplies defaults. Snapshot every
    # currently-exported name, source the file, then force-restore those
    # names to their pre-source values/export state.
    local prior
    prior="$(declare -p $(compgen -e) 2>/dev/null)"
    prior="${prior//declare -/declare -g}"
    set -a
    # shellcheck disable=SC1090
    . "$env_file"
    set +a
    eval "$prior"
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
# Keep the SIF home backed by WH_DIR (rather than the host's full home), while
# exposing it at the conventional /home/<user> path. Non-hidden host home
# directories are separately exposed under /code.
ssh_home_container="/home/${ssh_user}"
compat_dir="${wh_dir_host}/compat"
passwd_file="${compat_dir}/passwd"
group_file="${compat_dir}/group"
launch_mode="${WH_LAUNCH_MODE:-instance}"
instance_name="${WH_INSTANCE_NAME:-wh-${ssh_user}}"
# Fakeroot remains opt-in because it changes the container's UID/GID mapping.
# It is required on rootless Apptainer installations where inner tailscaled
# otherwise lacks CAP_SETGID for its SSH credential switch. SUID-capable
# installations generally do not need it. Validate host mappings before use.
fakeroot_flag=""
if [ -n "${WH_FAKEROOT:-}" ]; then
  case "${WH_FAKEROOT}" in
    1|true|yes|on) fakeroot_flag="--fakeroot" ;;
    0|false|no|off) fakeroot_flag="" ;;
    *) echo "[start-wh] ERROR: WH_FAKEROOT must be 0/1 or false/true" >&2; exit 1 ;;
  esac
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

# Build the standard host layout, then append operator-provided binds.
effective_binds=""
append_effective_bind() {
  local pair="$1"
  if [ -z "$effective_binds" ]; then
    effective_binds="$pair"
  elif [[ ";$effective_binds;" != *";$pair;"* ]]; then
    effective_binds="$effective_binds;$pair"
  fi
}

managed_bind_sources=""
managed_bind_roots=""
append_semicolon_item() {
  local current="$1" item="$2"
  if [ -z "$current" ]; then
    printf '%s\n' "$item"
  elif [[ ";$current;" == *";$item;"* ]]; then
    printf '%s\n' "$current"
  else
    printf '%s;%s\n' "$current" "$item"
  fi
}

append_managed_source() {
  managed_bind_sources="$(append_semicolon_item "$managed_bind_sources" "$1")"
}

append_managed_root() {
  managed_bind_roots="$(append_semicolon_item "$managed_bind_roots" "$1")"
}

is_managed_source() {
  local source="$1" root
  [[ ";$managed_bind_sources;" == *";$source;"* ]] && return 0
  IFS=';' read -ra _managed_roots <<< "$managed_bind_roots"
  for root in "${_managed_roots[@]}"; do
    if [ "$source" = "$root" ] || [[ "$source" == "$root/"* ]]; then
      return 0
    fi
  done
  return 1
}

normalize_host_source() {
  local source="$1"
  case "$source" in
    '~') source="$HOME" ;;
    '~/'*) source="$HOME/${source#\~/}" ;;
    '$HOME') source="$HOME" ;;
    '$HOME/'*) source="$HOME/${source#\$HOME/}" ;;
  esac
  while [ "$source" != "/" ] && [[ "$source" == */ ]]; do
    source="${source%/}"
  done
  if [ -e "$source" ]; then
    source="$(readlink -f "$source")"
  fi
  printf '%s\n' "$source"
}

# Workers use one host code collection. Prefer ~/Work, fall back to ~/Dev, and
# allow an explicit WH_CODE_ROOT path. Deployment creates ~/Work only when
# neither conventional directory exists.
code_root="${WH_CODE_ROOT:-}"
if [ -z "$code_root" ]; then
  if [ -d "$HOME/Work" ]; then
    code_root="$HOME/Work"
  elif [ -d "$HOME/Dev" ]; then
    code_root="$HOME/Dev"
  fi
fi
code_root="$(normalize_host_source "$code_root")"
if [ -d "$code_root" ]; then
  append_managed_source "$HOME"
  append_managed_root "$code_root"
  append_effective_bind "$code_root:/code"
fi

# A filesystem mounted directly at /mnt becomes /data/local. Otherwise each
# direct child mount gets a stable basename under /data/local.
mapfile -t _mnt_targets < <(
  findmnt --raw --noheadings --output TARGET 2>/dev/null | LC_ALL=C sort -u
)
_mnt_root=0
for _mount_dir in "${_mnt_targets[@]}"; do
  if [ "$_mount_dir" = "/mnt" ]; then
    _mnt_root=1
    break
  fi
done
if [ "$_mnt_root" -eq 1 ]; then
  append_effective_bind "/mnt:/data/local"
  append_managed_root "/mnt"
else
  for _mount_dir in "${_mnt_targets[@]}"; do
    _mount_suffix="${_mount_dir#/mnt/}"
    [[ "$_mount_dir" == /mnt/* && "$_mount_suffix" != */* ]] || continue
    append_effective_bind "$_mount_dir:/data/local/$(basename "$_mount_dir")"
    append_managed_root "$_mount_dir"
  done
fi

if [ -n "${WH_EXTRA_BINDS:-}" ]; then
  IFS=';' read -ra _extra_pairs <<< "$WH_EXTRA_BINDS"
  for _pair in "${_extra_pairs[@]}"; do
    [ -n "$_pair" ] || continue
    _extra_source="$(normalize_host_source "${_pair%%:*}")"
    is_managed_source "$_extra_source" && continue
    append_effective_bind "$_pair"
  done
fi

# Record every container-visible data root for worker path discovery.
bind_manifest="$wh_dir_host/data/bind-paths.json"
mkdir -p "$(dirname "$bind_manifest")"
python3 - "$bind_manifest" "$effective_binds" <<'PY'
import json
import os
import sys

manifest, raw = sys.argv[1:]
paths = []
for pair in filter(None, raw.split(";")):
    # Bind syntax is host:container[:ro]. Literal ':' in the host source is
    # intentionally unsupported.
    fields = pair.split(":")
    if len(fields) < 2:
        continue
    destination = fields[1]
    if destination.startswith("/") and destination != "/" and ".." not in destination.split("/"):
        paths.append(destination.rstrip("/"))
paths = sorted(set(paths))
temporary = f"{manifest}.tmp-{os.getpid()}"
with open(temporary, "w", encoding="utf-8") as handle:
    json.dump({"paths": paths}, handle, separators=(",", ":"))
    handle.write("\n")
os.replace(temporary, manifest)
PY

if [ -n "$effective_binds" ]; then
  IFS=';' read -ra _extra_pairs <<< "$effective_binds"
  for _pair in "${_extra_pairs[@]}"; do
    mount_args+=(--bind "$_pair")
  done
fi

exec_env_args=(
  --env TS_AUTHKEY="$TS_AUTHKEY"
  --env TS_HOST="${TS_HOST:-https://headscale.d0me.xyz}"
  --env TS_HOSTNAME="${TS_HOSTNAME:-}"
  --env TS_SOCKS5_ADDR="${TS_SOCKS5_ADDR:-127.0.0.1:1055}"
  --env ORCHESTRATOR_HOST="$ORCHESTRATOR_HOST"
  --env ORCHESTRATOR_PORT="${ORCHESTRATOR_PORT:-12888}"
  --env SSH_USER="$ssh_user"
  --env USER="$ssh_user"
  --env LOGNAME="$ssh_user"
  --env WH_DIR="$wh_dir_container"
  --env WH_PROXY="${WH_PROXY:-socks5://127.0.0.1:1055}"
  --env WH_PI_INGEST_BASE_URL="${WH_PI_INGEST_BASE_URL:-}"
  --env WH_PI_RELAY_PORT="${WH_PI_RELAY_PORT:-27888}"
  --env WH_PI_JOB_SOCKET="${WH_PI_JOB_SOCKET:-}"
  --env WH_PI_COMMAND="${WH_PI_COMMAND:-}"
)

# Opt-in override for worker identity. Left unset, the container falls back
# to its inherited hostname (unchanged default behavior for existing fleet
# workers). Set WORKER_NAME (and TS_HOSTNAME above) when co-locating
# multiple instances on one physical host (e.g. several SLURM jobs sharing
# a multi-GPU node), so each gets a distinct orchestrator/Tailscale identity.
if [ -n "${WORKER_NAME:-}" ]; then
  exec_env_args+=(--env WORKER_NAME="$WORKER_NAME")
fi

if [ "$launch_mode" = "instance" ]; then
  # Stop any leftover instance from a previous run (crash, restart, etc.)
  "$runtime" instance stop "$instance_name" 2>/dev/null || true
  echo "[start-wh] Starting instance $instance_name using $runtime..."
  # `instance start` runs the SIF startscript (which is /entrypoint.sh), so
  # its environment must be supplied here rather than only to a later exec.
  "$runtime" instance start --cleanenv "${mount_args[@]}" "${exec_env_args[@]}" "$image" "$instance_name"

  # Keep this systemd service attached to the instance without starting a
  # second entrypoint (and thus a second tailscaled/worker daemon).
  # If the instance exits, return failure so Restart=always recreates it.
  trap '"$runtime" instance stop "$instance_name" 2>/dev/null || true' EXIT INT TERM
  while "$runtime" instance list "$instance_name" | awk -v name="$instance_name" '$1 == name { found = 1 } END { exit !found }'; do
    sleep 1
  done
  echo "[start-wh] ERROR: instance $instance_name exited" >&2
  exit 1
fi

echo "[start-wh] Starting one-shot container using $runtime..."
exec "$runtime" run --cleanenv "${mount_args[@]}" "${exec_env_args[@]}" "$image"
