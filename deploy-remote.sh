#!/usr/bin/env bash
set -Eeuo pipefail

stage="${1:?stage directory required}"
txid="${2:?transaction id required}"
case "$stage" in
  "$HOME"/.worker-harness.stage.*) ;;
  *) echo "[deploy-remote] ERROR: invalid stage path: $stage" >&2; exit 1 ;;
esac
if [[ ! "$txid" =~ ^[A-Za-z0-9_.-]+$ ]]; then
  echo "[deploy-remote] ERROR: invalid transaction id" >&2
  exit 1
fi

live="$HOME/worker-harness"
backup="$HOME/worker-harness.backup.$txid"
failed="$HOME/worker-harness.failed.$txid"
state="$HOME/.worker-harness.state.$txid"
unit_dir="$HOME/.config/systemd/user"
worker_config_dir="$HOME/.config/worker-harness"
rclone_config_dir="$HOME/.config/rclone"
lock_file="$HOME/.worker-harness-deploy.lock"

if [ ! -x "$stage/install-service.sh" ]; then
  echo "[deploy-remote] ERROR: incomplete stage: $stage" >&2
  exit 1
fi
if [ -e "$backup" ] || [ -e "$failed" ] || [ -e "$state" ]; then
  echo "[deploy-remote] ERROR: transaction paths already exist for $txid" >&2
  exit 1
fi

exec 9>"$lock_file"

shopt -s nullglob
candidate_rclone_units=()
for unit in "$stage"/rclone-*.service; do
  candidate_rclone_units+=("$(basename "$unit")")
done
managed_units=(
  worker-harness.service
  worker-harness-update.path
  worker-harness-update.service
  worker-harness-restart.path
  worker-harness-restart.service
  "${candidate_rclone_units[@]}"
)
worker_config_names=(
  worker-harness.env
  worker-harness-update.sh
  worker-harness-restart.sh
)

mkdir -p "$state/systemd" "$state/worker-config" "$state/rclone-config" "$state/pending"
active_units=()
enabled_units=()
for unit_name in "${managed_units[@]}"; do
  unit_path="$unit_dir/$unit_name"
  if [ -e "$unit_path" ] || [ -L "$unit_path" ]; then
    cp -a "$unit_path" "$state/systemd/$unit_name"
  fi
  if systemctl --user is-active --quiet "$unit_name"; then
    active_units+=("$unit_name")
  fi
  if systemctl --user is-enabled --quiet "$unit_name"; then
    enabled_units+=("$unit_name")
  fi
done
for config_name in "${worker_config_names[@]}"; do
  config_path="$worker_config_dir/$config_name"
  if [ -e "$config_path" ] || [ -L "$config_path" ]; then
    cp -a "$config_path" "$state/worker-config/$config_name"
  fi
done
if [ -e "$rclone_config_dir/rclone.conf" ] || [ -L "$rclone_config_dir/rclone.conf" ]; then
  cp -a "$rclone_config_dir/rclone.conf" "$state/rclone-config/rclone.conf"
fi
printf '%s\n' "${active_units[@]}" > "$state/active-units"
printf '%s\n' "${enabled_units[@]}" > "$state/enabled-units"

# The remote worker environment is authoritative. The bundled common rclone
# config is authoritative when present; an existing config is only a fallback.
preserve_into_stage() {
  local destination="$1"
  shift
  local source
  for source in "$@"; do
    if [ -f "$source" ]; then
      cp -pL "$source" "$destination"
      return 0
    fi
  done
}
preserve_into_stage "$stage/.env" \
  "$worker_config_dir/worker-harness.env" \
  "$live/.env" || true
if [ ! -f "$stage/rclone.conf" ]; then
  preserve_into_stage "$stage/rclone.conf" \
    "$rclone_config_dir/rclone.conf" \
    "$live/rclone.conf" || true
fi
chmod 600 "$stage/.env"
if [ -f "$stage/rclone.conf" ]; then
  chmod 600 "$stage/rclone.conf"
fi

activated=0
had_live=0
deferred_trigger=0

lock_acquired=0
restore_path_set() {
  local source_dir="$1" destination_dir="$2"
  shift 2
  local name
  mkdir -p "$destination_dir"
  for name in "$@"; do
    rm -f "$destination_dir/$name"
    if [ -e "$source_dir/$name" ] || [ -L "$source_dir/$name" ]; then
      cp -a "$source_dir/$name" "$destination_dir/$name"
    fi
  done
}

enable_unit_file() {
  local unit_name="$1" unit_path="$unit_dir/$1"
  if [ -L "$unit_path" ]; then
    unit_path="$(readlink -f "$unit_path" 2>/dev/null || true)"
  fi
  if [ -n "$unit_path" ] && [ -f "$unit_path" ]; then
    systemctl --user enable "$unit_path"
  else
    systemctl --user enable "$unit_name"
  fi
}

restore_service_state() {
  local unit_name
  systemctl --user daemon-reload || true
  for unit_name in "${enabled_units[@]}"; do
    enable_unit_file "$unit_name" >/dev/null 2>&1 || true
  done
  for unit_name in "${active_units[@]}"; do
    systemctl --user start "$unit_name" >/dev/null 2>&1 || \
      echo "[deploy-remote] WARNING: rollback could not start $unit_name" >&2
  done
}

restore_pending_triggers() {
  local harness_dir="$HOME/.local/worker-harness/harness" name
  mkdir -p "$harness_dir"
  for name in new-image.sif restart-trigger; do
    if [ -e "$state/pending/$name" ]; then
      mv "$state/pending/$name" "$harness_dir/$name"
    fi
  done
}

stop_unit_safely() {
  local unit_name="$1"
  if systemctl --user cat "$unit_name" >/dev/null 2>&1; then
    systemctl --user stop "$unit_name"
    if systemctl --user is-active --quiet "$unit_name"; then
      echo "[deploy-remote] ERROR: failed to stop $unit_name" >&2
      return 1
    fi
  fi
}

wait_unit_inactive() {
  local unit_name="$1"
  for _attempt in {1..60}; do
    if ! systemctl --user is-active --quiet "$unit_name"; then
      return 0
    fi
    sleep 1
  done
  echo "[deploy-remote] ERROR: timed out waiting for $unit_name" >&2
  return 1
}

restore_prelock_paths() {
  local path_unit value
  for path_unit in worker-harness-update.path worker-harness-restart.path; do
    for value in "${enabled_units[@]}"; do
      if [ "$value" = "$path_unit" ]; then
        enable_unit_file "$path_unit" >/dev/null 2>&1 || true
      fi
    done
    for value in "${active_units[@]}"; do
      if [ "$value" = "$path_unit" ]; then
        systemctl --user start "$path_unit" >/dev/null 2>&1 || true
      fi
    done
  done
}

rollback() {
  local status="$1" unit_name
  trap - ERR EXIT INT TERM
  set +e
  if [ "$lock_acquired" -eq 0 ]; then
    restore_prelock_paths
    rm -rf "$stage" "$state"
    echo "[deploy-remote] rollback complete; live installation was not changed" >&2
    exit "$status"
  fi
  echo "[deploy-remote] ERROR: deployment failed; rolling back" >&2
  for unit_name in "${managed_units[@]}"; do
    systemctl --user stop "$unit_name" >/dev/null 2>&1 || true
  done
  # Disable the failed deployment before restoring saved unit-file links:
  # systemd may delete out-of-search-path links while disabling.
  for unit_name in "${managed_units[@]}"; do
    systemctl --user disable "$unit_name" >/dev/null 2>&1 || true
  done
  restore_path_set "$state/systemd" "$unit_dir" "${managed_units[@]}"
  restore_path_set "$state/worker-config" "$worker_config_dir" "${worker_config_names[@]}"
  restore_path_set "$state/rclone-config" "$rclone_config_dir" rclone.conf
  if [ "$activated" -eq 1 ] && [ -e "$live" ]; then
    mv "$live" "$failed"
  fi
  if [ "$had_live" -eq 1 ] && [ -e "$backup" ]; then
    mv "$backup" "$live"
  fi
  restore_pending_triggers
  restore_service_state
  rm -rf "$stage" "$state"
  if [ "$activated" -eq 1 ]; then
    echo "[deploy-remote] rollback complete; failed deployment retained at $failed" >&2
  else
    echo "[deploy-remote] rollback complete; live installation was not changed" >&2
  fi
  exit "$status"
}
trap 'rollback $?' ERR
trap 'rollback 130' INT TERM

# Disable new triggers first, then let an update/restart already in its critical
# section finish. Killing the old updater between its image rename operations
# would make the rollback source incomplete.
stop_unit_safely worker-harness-update.path
stop_unit_safely worker-harness-restart.path
wait_unit_inactive worker-harness-update.service
wait_unit_inactive worker-harness-restart.service

# New updater/restart helpers take this same lock. With their path units off and
# old invocations settled, the live SIF and service links are now quiescent.
flock 9
lock_acquired=1
for trigger_name in new-image.sif restart-trigger; do
  trigger_path="$HOME/.local/worker-harness/harness/$trigger_name"
  if [ -e "$trigger_path" ]; then
    mv "$trigger_path" "$state/pending/$trigger_name"
    deferred_trigger=1
    echo "[deploy-remote] deferred pending trigger: $trigger_name"
  fi
done
stop_unit_safely worker-harness.service
for unit_name in "${candidate_rclone_units[@]}"; do
  installed_unit="$unit_dir/$unit_name"
  stop_unit_safely "$unit_name"
  unit_definition="$installed_unit"
  if [ ! -f "$unit_definition" ]; then
    unit_definition="$stage/$unit_name"
  fi
  if [ -f "$unit_definition" ]; then
    mount_dir=""
    while IFS= read -r line; do
      if [[ "$line" == ExecStart=* ]]; then
        read -r _ _ _ mount_dir _ <<< "${line#ExecStart=}"
        break
      fi
    done < "$unit_definition"
    mount_dir="${mount_dir//\%h/$HOME}"
    if [ -n "$mount_dir" ] && mountpoint -q "$mount_dir"; then
      fusermount3 -uz "$mount_dir" || true
    fi
    if [ -n "$mount_dir" ] && mountpoint -q "$mount_dir"; then
      echo "[deploy-remote] ERROR: $unit_name still mounts $mount_dir" >&2
      false
    fi
  fi
done

if [ -e "$live" ]; then
  had_live=1
  mv "$live" "$backup"
fi
mv "$stage" "$live"
activated=1

cd "$live"
WH_MIGRATION_SUFFIX="$txid" WH_USE_BUNDLED_RCLONE_CONFIG=1 ./install-service.sh

# Match the image updater's health contract: both systemd and the actual worker
# daemon must remain live for several consecutive polls.
health_streak=0
health_required="${WH_DEPLOY_HEALTH_STREAK:-5}"
health_attempts="${WH_DEPLOY_HEALTH_ATTEMPTS:-20}"
health_interval="${WH_DEPLOY_HEALTH_INTERVAL:-1}"
for ((attempt = 1; attempt <= health_attempts; attempt++)); do
  if systemctl --user is-active --quiet worker-harness.service && \
     pgrep -f '^python3? /worker_daemon.py$' >/dev/null; then
    health_streak=$((health_streak + 1))
    if [ "$health_streak" -ge "$health_required" ]; then
      break
    fi
  else
    health_streak=0
  fi
  sleep "$health_interval"
done
if [ "$health_streak" -lt "$health_required" ]; then
  echo "[deploy-remote] ERROR: worker failed post-deploy health check" >&2
  false
fi

trap - ERR INT TERM
if [ "$had_live" -eq 1 ] || [ "$deferred_trigger" -eq 1 ]; then
  mkdir -p "$backup"
  mv "$state" "$backup/.deployment-state"
else
  rm -rf "$state"
fi
systemctl --user reset-failed >/dev/null 2>&1 || true
echo "[deploy-remote] deployment healthy"
if [ "$had_live" -eq 1 ]; then
  echo "[deploy-remote] previous installation retained at $backup"
fi
if [ "$deferred_trigger" -eq 1 ]; then
  echo "[deploy-remote] pending update/restart trigger retained in the deployment backup"
fi
