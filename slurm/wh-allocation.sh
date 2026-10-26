#!/usr/bin/env bash
# One explicit allocation, one launch. All workload lifecycle decisions remain
# in the existing worker daemon; this wrapper owns batch signals and deadlines.
set -euo pipefail
umask 077

stop_requested=0
checkpoint_requested=0
runtime_dir=""
child=""
owned_launch=0
stop_deadline=0
exit_override=0

now() { date +%s; }

persist_control() {
  [ -n "$runtime_dir" ] || return 0
  (( owned_launch )) || return 0
  local action=checkpoint signal="$checkpoint_signal"
  if (( stop_requested )); then action=stop; signal=TERM; fi
  python3 - "$runtime_dir/control.json" "$action" "$signal" <<'PY'
import datetime
import fcntl
import json
import os
import sys
from pathlib import Path
path = Path(sys.argv[1])
action, signal = sys.argv[2:]
with path.with_name("control.lock").open("a") as lock:
    fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
    if path.exists() and json.loads(path.read_text())["action"] == "stop":
        sys.exit(0)
    value = {"action": action, "signal": signal,
             "requested_at": datetime.datetime.now(datetime.timezone.utc).isoformat()}
    temporary = path.with_name(f".control-{os.getpid()}.json")
    with temporary.open("x") as handle:
        json.dump(value, handle, separators=(",", ":"))
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
PY
}

request_stop() {
  stop_requested=1
  persist_control
}
request_checkpoint() {
  checkpoint_requested=1
  (( stop_requested )) || persist_control
}
# Install before parsing settings: a signal during startup is not discarded.
trap request_stop TERM INT HUP
trap request_checkpoint USR1

settings="${1:?usage: wh-allocation.sh <settings.json>}"
# Emit NUL-separated values, never source settings or interpolate JSON as shell.
# A regular temporary file makes validation failure propagate (not process
# substitution, where mapfile would hide the Python exit status).
configuration="$(mktemp)"
trap 'rm -f "$configuration"' EXIT
python3 - "$settings" >"$configuration" <<'PY'
import json
import os
import re
import sys
from pathlib import Path
settings_path = Path(sys.argv[1]).resolve(strict=True)
s = json.loads(settings_path.read_text())
if s.get("version") != 1:
    raise ValueError("unsupported allocation settings version")
worker_id = s["worker_id"]
if not isinstance(worker_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", worker_id):
    raise ValueError("invalid reserved worker ID")
kind = s["compute_kind"]
if kind not in ("cpu", "gpu"):
    raise ValueError("invalid compute_kind")
for key in ("gpu_count", "cpus", "memory_mb", "failure_grace_seconds", "termination_grace_seconds", "warning_seconds"):
    value = s[key]
    if type(value) is not int or value < (1 if key in ("cpus", "memory_mb") else 0):
        raise ValueError(f"invalid {key}")
if (kind == "cpu" and s["gpu_count"] != 0) or (kind == "gpu" and s["gpu_count"] < 1):
    raise ValueError("compute_kind does not match GPU count")
if s["runtime"] not in ("apptainer", "singularity"):
    raise ValueError("invalid runtime")
if s["checkpoint_signal"] not in ("USR1", "TERM"):
    raise ValueError("invalid checkpoint signal")
for key in ("host_run_path", "run_path", "shared_root", "sif_path", "tailnet_key_file", "worker_token_file"):
    value = s[key]
    if not isinstance(value, str) or not value.startswith("/") or any(c in value for c in "\x00\n\r:;,"):
        raise ValueError(f"invalid {key}")
run = Path(s["host_run_path"]).resolve(strict=True)
shared = Path(s["shared_root"]).resolve(strict=True)
relative_run = run.relative_to(shared)
if run == shared or settings_path != run / "settings.json":
    raise ValueError("settings and run directory must be beneath shared_root")
if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", s["storage_id"]):
    raise ValueError("invalid storage_id")
expected = Path("/data/shared") / s["storage_id"] / relative_run
if Path(s["run_path"]) != expected:
    raise ValueError("run_path does not match shared storage mapping")
for key, name in (("tailnet_key_file", "tailnet.key"), ("worker_token_file", "worker.token")):
    path = Path(s[key])
    if path != run / name or path.is_symlink() or not path.is_file():
        raise ValueError(f"{key} must be a staged run credential")
    os.chmod(path, 0o600)
port = s["orchestrator_registration_port"]
if type(port) is not int or not 1 <= port <= 65535:
    raise ValueError("invalid registration port")
for key in ("tailnet_login_server", "orchestrator_host"):
    if not isinstance(s[key], str) or not s[key] or any(c in s[key] for c in "\x00\n\r"):
        raise ValueError(f"invalid {key}")
values = [worker_id, str(run), s["run_path"], s["shared_root"], s["storage_id"], s["sif_path"],
          s["runtime"], str(s["termination_grace_seconds"]), s["checkpoint_signal"],
          s["tailnet_login_server"], s["orchestrator_host"], str(port)]
for value in values:
    sys.stdout.buffer.write(value.encode() + b"\0")
PY
mapfile -d '' -t config <"$configuration"
rm -f "$configuration"
worker_id="${config[0]}"
host_run_path="${config[1]}"
run_path="${config[2]}"
shared_root="${config[3]}"
storage_id="${config[4]}"
image="${config[5]}"
runtime="${config[6]}"
termination_grace="${config[7]}"
checkpoint_signal="${config[8]}"
runtime_dir="$host_run_path/runtime"

mkdir -p "$runtime_dir"
chmod 700 "$runtime_dir" "$host_run_path"
# mkdir is atomic across nodes on shared storage. Never remove this fence.
if ! mkdir "$runtime_dir/launch-started" 2>/dev/null; then
  echo '[wh-allocation] Refusing duplicate allocation launch' >&2
  exit 1
fi
owned_launch=1

cleanup() {
  local status=$?
  trap - EXIT
  if (( owned_launch )); then
    if [ -n "$child" ] && kill -0 "$child" 2>/dev/null; then
      kill -KILL -- "-$child" 2>/dev/null || true
      wait "$child" 2>/dev/null || true
    fi
    # Only known credential copies owned by this run; logs/results stay shared.
    rm -f "$host_run_path/tailnet.key" "$host_run_path/worker.token"
    printf '{"exit_code":%s,"finished_at_epoch":%s}\n' "$status" "$(now)" >"$runtime_dir/wrapper-result.json.tmp"
    mv "$runtime_dir/wrapper-result.json.tmp" "$runtime_dir/wrapper-result.json"
  fi
}
trap cleanup EXIT
if [[ ! "${SLURM_RESTART_COUNT:-0}" =~ ^0+$ ]]; then
  echo '[wh-allocation] Refusing a requeued allocation' >&2
  exit 1
fi

export WH_SLURM_WORKER_ID="$worker_id"
export WH_SLURM_SETTINGS_FILE="$run_path/settings.json"
export WH_DIR="$runtime_dir"
export WH_LAUNCH_MODE=run WH_LOAD_ENV_FILE=0 WH_AUTO_OVERLAY=0
export WH_RUNTIME="$runtime" WH_IMAGE="$image"
export TS_HOSTNAME="whc-$worker_id" WORKER_NAME="whc-$worker_id"
export TS_AUTHKEY_FILE="$run_path/tailnet.key"
export WH_WORKER_TOKEN_FILE="$run_path/worker.token"
export TS_HOST="${config[9]}" ORCHESTRATOR_HOST="${config[10]}" ORCHESTRATOR_PORT="${config[11]}"
export WH_SHARED_ROOTS_JSON
WH_SHARED_ROOTS_JSON="$(python3 - "$storage_id" "$shared_root" <<'PY'
import json
import sys
print(json.dumps({sys.argv[1]: sys.argv[2]}, separators=(",", ":")))
PY
)"

started_at="$(now)"
startup_deadline=$((started_at + 180))
printf '{"started_at_epoch":%s,"deadline_epoch":%s}\n' "$started_at" "$startup_deadline" >"$runtime_dir/startup.json"
# Replay controls delivered before the daemon exists. An already persisted stop
# also wins over a later checkpoint, including after a batch-shell interruption.
if (( stop_requested || checkpoint_requested )); then persist_control; fi
if [ -f "$runtime_dir/control.json" ]; then
  action="$(python3 - "$runtime_dir/control.json" <<'PY'
import json
import sys
print(json.load(open(sys.argv[1]))["action"])
PY
)"
  if [ "$action" = stop ]; then exit 143; fi
fi

# setsid isolates the launcher and its descendants for the final bounded kill.
# The daemon watches control.json, so startup signals need not hit an unready
# container process (where a default USR1 handler could kill enrollment).
setsid bash "$host_run_path/start-wh.sh" "$image" &
child=$!
while kill -0 "$child" 2>/dev/null; do
  current="$(now)"
  if (( ! stop_requested )) && [ ! -f "$runtime_dir/ready" ] && (( current >= startup_deadline )); then
    echo '[wh-allocation] Startup readiness timed out after 180 seconds' >&2
    exit_override=124
    request_stop
  fi
  if (( stop_requested )); then
    if (( stop_deadline == 0 )); then stop_deadline=$((current + termination_grace)); fi
    if (( current >= stop_deadline )); then
      kill -KILL -- "-$child" 2>/dev/null || true
      break
    fi
  fi
  # wait on a short background timer so traps interrupt immediately; waiting
  # directly for the launcher would otherwise hide readiness/deadline changes.
  sleep 0.2 &
  timer=$!
  wait "$timer" || true
done
status=0
wait "$child" || status=$?
child=""
if (( exit_override )); then exit "$exit_override"; fi
if (( stop_requested )); then exit 143; fi
exit "$status"
