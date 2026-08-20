# wh-slurm-common.sh — shared Slurm bootstrap for worker-harness jobs.
#
# Source this from a per-cluster job script, after the #SBATCH directives
# and two plain variables mirroring them:
#
#   #SBATCH --constraint=v100
#   #SBATCH --account=pi-violai
#   ...
#   GPUTYPE=v100        # mirror --constraint
#   ACCOUNT=pi-violai   # mirror --account; leave "" if you don't pass --account
#
#   source /path/to/wh-slurm-common.sh
#   wh_slurm_bootstrap "$@"
#
# wh_slurm_bootstrap:
#   - renames the running job to wh_<GPUTYPE>[_pi] (visible in squeue
#     immediately; propagated to every self-resubmitted successor's
#     --job-name, so their %x-based --output log names match too). The
#     suffix is the literal "_pi", not the account value, whenever ACCOUNT
#     is non-empty.
#   - picks a per-job, discardable WH_DIR: node-local /local scratch,
#     falling back to the Weka user FS — never $HOME (NFS; causes
#     overlay.ext3 lock contention when multiple jobs share it)
#   - exports WH_INSTANCE_NAME / WORKER_NAME / TS_HOSTNAME
#   - re-submits itself with a jittered --begin so a multi-day allocation
#     chains indefinitely, guarded against double-chaining on requeue
#   - refuses to queue a successor (breaking the chain) when either:
#       * $HOME/.local/state/worker-harness/stop-<job_name> exists — drop
#         this file to end a chain cleanly; the currently running job still
#         finishes, it just won't queue another link. rm it to resume.
#       * the previous run in this chain lasted under WH_MIN_RUNTIME_SECONDS
#         (default 120s) — crash-loop guard. Since the dependency chain
#         means a job can't start until its predecessor has fully exited,
#         this reliably stops runaway chaining within one extra job.
#   - enables `set -uo pipefail` for the remainder of the caller's script
#
# Use slurm/wh-submit.sh to submit the *first* job in a chain so its name
# (and log filename) matches from the start instead of only from the first
# resubmission onward.

wh_slurm_bootstrap() {
  : "${GPUTYPE:?GPUTYPE must be set before calling wh_slurm_bootstrap (mirror your #SBATCH --constraint)}"
  : "${SLURM_JOB_ID:?wh_slurm_bootstrap must run inside a Slurm job}"

  local job_name state stop_file runtime_file
  job_name="wh_${GPUTYPE}"
  if [ -n "${ACCOUNT:-}" ]; then
    job_name="${job_name}_pi"
  fi
  scontrol update JobId="${SLURM_JOB_ID}" JobName="${job_name}" >/dev/null 2>&1 || true

  state="$HOME/.local/state/worker-harness"
  mkdir -p "$state"
  stop_file="$state/stop-${job_name}"
  runtime_file="$state/last-runtime-${job_name}"

  export XDG_RUNTIME_DIR=/tmp
  local node user
  node="$(hostname -s)"
  user="$(whoami)"

  # Per-job, isolated, discardable worker-harness state. Prefer node-local
  # NVMe scratch (SLURM auto-purges it at job end); not every node has
  # /local, so fall back to the WekaIO user FS rather than $HOME (NFS —
  # causes overlay.ext3 lock contention across nodes/jobs).
  _wh_dir_is_temp=0
  if mkdir -p "/local/${SLURM_JOB_ID}/worker-harness" 2>/dev/null; then
    export WH_DIR="/local/${SLURM_JOB_ID}/worker-harness"
  else
    export WH_DIR="/ibex/user/${user}/.local/worker-harness-${SLURM_JOB_ID}"
    _wh_dir_is_temp=1
  fi
  _wh_start_epoch="$(date +%s)"
  _wh_runtime_file="$runtime_file"
  trap _wh_slurm_on_exit EXIT

  export WH_INSTANCE_NAME="wh-${user}-${SLURM_JOB_ID}"
  export WORKER_NAME="wh-${user}-${node}-${SLURM_JOB_ID}"
  export TS_HOSTNAME="wh-${user}-${SLURM_JOB_ID}"

  set -uo pipefail

  local script slot marker jitter_minutes begin submitted attempt next_job
  script="$(readlink -f "$0")"
  slot="${1:-0}"

  # Avoid branching the chain if this job is requeued/restarted.
  marker="$state/submitted-${SLURM_JOB_ID}"

  local min_runtime skip_reason last_runtime
  min_runtime="${WH_MIN_RUNTIME_SECONDS:-120}"
  skip_reason=""
  if [ -f "$stop_file" ]; then
    skip_reason="stop file present: $stop_file (rm it to resume chaining)"
  elif [ -f "$runtime_file" ]; then
    last_runtime="$(cat "$runtime_file" 2>/dev/null || echo 0)"
    case "$last_runtime" in (''|*[!0-9]*) last_runtime=0 ;; esac
    if [ "$last_runtime" -lt "$min_runtime" ]; then
      skip_reason="previous run in this chain lasted only ${last_runtime}s (< ${min_runtime}s) — looks like a crash loop"
    fi
  fi

  if [ -n "$skip_reason" ]; then
    echo "wh_slurm_bootstrap: NOT queuing a successor: $skip_reason" >&2
  elif mkdir "$marker" 2>/dev/null; then
    # This only matters if the current job dies quickly. After a normal
    # multi-day run, the begin time will already have passed.
    jitter_minutes=$((15 + RANDOM % 31))
    begin="$(date -d "+${jitter_minutes} minutes" '+%Y-%m-%dT%H:%M:%S')"

    submitted=false
    for attempt in 1 2 3 4 5; do
      if next_job="$(
          sbatch --parsable \
              --job-name="$job_name" \
              --dependency="afterany:${SLURM_JOB_ID}" \
              --begin="$begin" \
              "$script" "$slot"
      )"; then
          printf '%s\n' "$next_job" >"$marker/next-job"
          echo "Queued successor $next_job ($job_name) for slot $slot"
          submitted=true
          break
      fi
      sleep $((attempt * 15))
    done

    if ! $submitted; then
      echo "WARNING: failed to submit successor" >&2
      rmdir "$marker"
    fi
  fi
}

# Exit trap registered by wh_slurm_bootstrap: records this run's wall-clock
# duration (read by the *next* job in the chain to detect crash loops) and
# cleans up the Weka-fallback WH_DIR. Relies on the globals wh_slurm_bootstrap
# sets (_wh_start_epoch, _wh_runtime_file, _wh_dir_is_temp, WH_DIR) — plain
# (non-local) assignments, so they still exist here after the function that
# set them has returned.
_wh_slurm_on_exit() {
  if [ -n "${_wh_runtime_file:-}" ] && [ -n "${_wh_start_epoch:-}" ]; then
    local elapsed
    elapsed=$(( $(date +%s) - _wh_start_epoch ))
    printf '%s\n' "$elapsed" >"$_wh_runtime_file" 2>/dev/null || true
  fi
  if [ "${_wh_dir_is_temp:-0}" = 1 ] && [ -n "${WH_DIR:-}" ]; then
    rm -rf "$WH_DIR"
  fi
}
