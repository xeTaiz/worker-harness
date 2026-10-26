#!/usr/bin/env bash
set -euo pipefail

slurm_mode=0
if [ -n "${WH_SLURM_WORKER_ID:-}" ]; then
  slurm_mode=1
  # Allocation settings, not a login-node .env, define this launch.
  umask 077
fi

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

if (( ! slurm_mode )); then
  if ! load_env_file "${WH_ENV_FILE:-}"; then
    load_env_file "$PWD/.env" || true
    load_env_file "$PWD/worker-harness.env" || true
    load_env_file "$HOME/.config/worker-harness/worker-harness.env" || true
  fi
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
if (( slurm_mode )); then
  : "${WH_DIR:?Slurm launches require their isolated runtime directory}"
  : "${WH_SLURM_SETTINGS_FILE:?Slurm launches require allocation settings}"
  # The wrapper gives container paths for secrets/settings. Read only the
  # corresponding staged host settings; never source JSON as shell code.
  slurm_config="$(python3 - "$wh_dir_host" "$WH_SLURM_WORKER_ID" "$WH_SLURM_SETTINGS_FILE" <<'PY'
import json
import os
import re
import sys
from pathlib import Path

runtime = Path(sys.argv[1]).resolve(strict=True)
settings = json.loads((runtime.parent / "settings.json").read_text())
worker_id, container_settings = sys.argv[2:]
if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", worker_id) or settings["worker_id"] != worker_id:
    raise ValueError("invalid Slurm worker identity")
run_path = settings["run_path"]
if not isinstance(run_path, str) or not run_path.startswith("/") or any(c in run_path for c in "\x00\n\r:;,"):
    raise ValueError("invalid container run path")
if runtime != Path(settings["host_run_path"]).resolve(strict=True) / "runtime":
    raise ValueError("WH_DIR must be the reserved allocation runtime directory")
if container_settings != run_path + "/settings.json":
    raise ValueError("settings must use the allocation's container path")
kind = settings["compute_kind"]
if kind not in ("cpu", "gpu"):
    raise ValueError("invalid compute_kind")
worker_name = settings["worker_name"]
if not isinstance(worker_name, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,199}", worker_name):
    raise ValueError("invalid reserved worker name")
for name in ("tailnet.key", "worker.token"):
    path = runtime.parent / name
    if path.is_symlink() or not path.is_file() or not os.access(path, os.R_OK):
        raise ValueError("missing staged allocation credential file")
print(kind)
print(run_path)
print(worker_name)
PY
)"
  mapfile -t _slurm_config <<< "$slurm_config"
  slurm_kind="${_slurm_config[0]}"
  slurm_run_path="${_slurm_config[1]}"
  wh_dir_container="$slurm_run_path/runtime"
  launch_mode=run
  TS_HOSTNAME="whc-$WH_SLURM_WORKER_ID"
  WORKER_NAME="${_slurm_config[2]}"
  TS_AUTHKEY_FILE="$slurm_run_path/tailnet.key"
  WH_WORKER_TOKEN_FILE="$slurm_run_path/worker.token"
  if [ "$slurm_kind" = gpu ]; then
    : "${CUDA_VISIBLE_DEVICES:?Slurm GPU launches require the original assigned CUDA visibility}"
    slurm_cuda_visible_devices="$CUDA_VISIBLE_DEVICES"
  else
    slurm_cuda_visible_devices=""
  fi
fi
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

if (( ! slurm_mode )) && [ -z "$TS_AUTHKEY" ] && [ ! -r "${TS_AUTHKEY_FILE:-}" ]; then
  echo "[start-wh] ERROR: TS_AUTHKEY or readable TS_AUTHKEY_FILE is required" >&2
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
if (( slurm_mode )); then
  overlay_file=""
elif [ ! -f "$overlay_file" ]; then
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

mount_args=($fakeroot_flag \
  --home "$wh_dir_host/home/$ssh_user:$ssh_home_container" \
  --bind "$wh_dir_host:$wh_dir_container" \
  --bind "$passwd_file:/etc/passwd" \
  --bind "$group_file:/etc/group" \
  --workdir "$ssh_home_container")
if (( ! slurm_mode )) || [ "$slurm_kind" = gpu ]; then
  mount_args+=(--nv)
fi
if (( slurm_mode )); then
  # Do not inherit login-node CWD or site-wide host bind configuration.
  # /tmp is private to this one container, shared by daemon and SSH children.
  mount_args+=(--contain --no-mount=cwd,hostfs,bind-paths)
  mount_args+=(--bind "${wh_dir_host%/runtime}/tailnet.key:$TS_AUTHKEY_FILE:ro"
    --bind "${wh_dir_host%/runtime}/worker.token:$WH_WORKER_TOKEN_FILE:ro")
fi

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

if (( ! slurm_mode )); then
# Cluster nodes reach the shared collections natively instead of through
# rclone. The user filesystem is the same directory other workers mount as
# /data/shared/ibex, and it also holds the checked-out repositories.
ibex_user_root="${WH_IBEX_USER_ROOT-/ibex/user/$(id -un)}"
[ -d "$ibex_user_root" ] || ibex_user_root=""

# Workers use one host code collection. Prefer ~/Work, fall back to ~/Dev or
# the cluster user filesystem, and allow an explicit WH_CODE_ROOT path.
# Deployment creates ~/Work only when neither conventional directory exists.
code_root="${WH_CODE_ROOT:-}"
if [ -z "$code_root" ]; then
  if [ -d "$HOME/Work" ]; then
    code_root="$HOME/Work"
  elif [ -d "$HOME/Dev" ]; then
    code_root="$HOME/Dev"
  else
    code_root="$ibex_user_root"
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
fi

# Explicit shared storage uses the same bind list and discovery manifest as
# automatic mounts. Validate the entire mapping before adding any entries.
# Keep parsing in a command substitution: a validation failure must stop startup,
# unlike a failed producer behind process substitution.
shared_bind_pairs="$(python3 - "${WH_SHARED_ROOTS_JSON:-}" <<'PY'
import json
import os
import re
import sys


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate storage_id: {key!r}")
        result[key] = value
    return result


try:
    roots = json.loads(sys.argv[1] or "{}", object_pairs_hook=unique_object)
    if not isinstance(roots, dict):
        raise ValueError("expected a JSON object mapping storage_id to host directory")
    binds = []
    for storage_id, root in roots.items():
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", storage_id):
            raise ValueError(f"invalid storage_id: {storage_id!r}")
        if not isinstance(root, str) or not os.path.isabs(root):
            raise ValueError(f"{storage_id}: host directory must be an absolute path")
        if any(character in ":;," or ord(character) < 32 or ord(character) == 127 for character in root):
            raise ValueError(f"{storage_id}: host directory contains an unsupported bind delimiter or control character")
        root = os.path.realpath(root)
        if not os.path.isdir(root):
            raise ValueError(f"{storage_id}: host directory does not exist: {root}")
        if any(character in ":;," or ord(character) < 32 or ord(character) == 127 for character in root):
            raise ValueError(f"{storage_id}: resolved host directory contains an unsupported bind delimiter or control character")
        binds.append(f"{root}:/data/shared/{storage_id}")
    print(";".join(binds))
except (ValueError, OSError) as error:
    print(f"[start-wh] ERROR: WH_SHARED_ROOTS_JSON: {error}", file=sys.stderr)
    sys.exit(1)
PY
)"
declare -A configured_shared_roots=()
IFS=';' read -ra _shared_pairs <<< "$shared_bind_pairs"
for _pair in "${_shared_pairs[@]}"; do
  [ -n "$_pair" ] || continue
  _shared_source="${_pair%%:*}"
  _storage_id="${_pair##*/}"
  configured_shared_roots["$_storage_id"]="$_shared_source"
  append_effective_bind "$_pair"
  append_managed_root "$_shared_source"
done

if (( ! slurm_mode )); then
if [ -n "$ibex_user_root" ] && [ -z "${configured_shared_roots[ibex]+set}" ]; then
  ibex_user_root="$(normalize_host_source "$ibex_user_root")"
  append_effective_bind "$ibex_user_root:/data/shared/ibex"
  append_managed_root "$ibex_user_root"
fi

# Project filesystems keep the same collection names the rclone mounts use,
# so /data/shared/ibex_c2324 means one directory across the whole fleet.
IFS=';' read -ra _ibex_projects <<< "${WH_IBEX_PROJECTS-c2324}"
for _project in "${_ibex_projects[@]}"; do
  [ -n "$_project" ] || continue
  [ -z "${configured_shared_roots[ibex_$_project]+set}" ] || continue
  _project_dir="${WH_IBEX_PROJECT_ROOT-/ibex/project}/$_project"
  [ -d "$_project_dir" ] || continue
  append_effective_bind "$_project_dir:/data/shared/ibex_$_project"
  append_managed_root "$_project_dir"
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
  --env TS_HOST="${TS_HOST:-https://headscale.d0me.xyz}"
  --env TS_HOSTNAME="${TS_HOSTNAME:-}"
  --env ORCHESTRATOR_HOST="$ORCHESTRATOR_HOST"
  --env ORCHESTRATOR_PORT="${ORCHESTRATOR_PORT:-12888}"
  --env SSH_USER="$ssh_user"
  --env USER="$ssh_user"
  --env LOGNAME="$ssh_user"
  --env WH_DIR="$wh_dir_container"
)

if (( slurm_mode )); then
  exec_env_args+=(
    --env TS_AUTHKEY_FILE="$TS_AUTHKEY_FILE"
    --env WH_WORKER_TOKEN_FILE="$WH_WORKER_TOKEN_FILE"
    --env WH_SLURM_WORKER_ID="$WH_SLURM_WORKER_ID"
    --env WH_SLURM_SETTINGS_FILE="$WH_SLURM_SETTINGS_FILE"
    --env WH_SLURM_COMPUTE_KIND="$slurm_kind"
    --env WH_SLURM_CUDA_VISIBLE_DEVICES="$slurm_cuda_visible_devices"
    --env CUDA_VISIBLE_DEVICES="$slurm_cuda_visible_devices"
    --env TS_SOCKS5_ADDR=""
    --env WH_PROXY=""
  )
  if [ "$slurm_kind" = cpu ]; then
    exec_env_args+=(--env NVIDIA_VISIBLE_DEVICES=void)
  fi
else
  exec_env_args+=(
    --env TS_SOCKS5_ADDR="${TS_SOCKS5_ADDR:-127.0.0.1:1055}"
    --env WH_PROXY="${WH_PROXY:-socks5://127.0.0.1:1055}"
  )
  if [ -n "${TS_AUTHKEY_FILE:-}" ]; then
    mount_args+=(--bind "$TS_AUTHKEY_FILE:$TS_AUTHKEY_FILE:ro")
    exec_env_args+=(--env TS_AUTHKEY_FILE="$TS_AUTHKEY_FILE")
  else
    exec_env_args+=(--env TS_AUTHKEY="$TS_AUTHKEY")
  fi
fi

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
