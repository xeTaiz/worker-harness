#!/usr/bin/env bash
# wh-rename-existing.sh — one-off bulk rename for jobs submitted before you
# adopted the wh_<GPUTYPE>[_pi] naming convention (or under mismatched
# GPUTYPE/ACCOUNT script vars). Renames every worker-harness job you own,
# pending or running, to match the name wh_slurm_bootstrap would give it,
# derived from each job's actual --constraint (Features=) and --account
# (Account=) record rather than trusting the job's current display name.
#
# For PENDING jobs this also fixes the eventual %x-based --output log path.
# For RUNNING jobs it only fixes the squeue display — their log file was
# already opened under the old name.
#
# Usage: slurm/wh-rename-existing.sh [job_id ...]
#   No args: operates on every job you own reported by `squeue -u $USER`.
set -euo pipefail

ids=("$@")
if [ ${#ids[@]} -eq 0 ]; then
  mapfile -t ids < <(squeue -u "$(whoami)" -h -o '%A')
fi

for jobid in "${ids[@]}"; do
  info="$(scontrol show job "$jobid" 2>/dev/null || true)"
  if [ -z "$info" ]; then
    echo "skip $jobid: not found (already finished?)" >&2
    continue
  fi
  gputype="$(printf '%s' "$info" | sed -n 's/.*Features=\([^ ]*\).*/\1/p' | head -n1)"
  account="$(printf '%s' "$info" | sed -n 's/.*Account=\([^ ]*\).*/\1/p' | head -n1)"
  [ "$gputype" = "(null)" ] && gputype=""
  [ "$account" = "(null)" ] && account=""

  if [ -z "$gputype" ]; then
    echo "skip $jobid: no --constraint/Features on record, can't derive GPUTYPE" >&2
    continue
  fi

  job_name="wh_${gputype}"
  [ -n "$account" ] && job_name="${job_name}_pi"

  old_name="$(printf '%s' "$info" | sed -n 's/.*JobName=\([^ ]*\).*/\1/p' | head -n1)"
  if [ "$old_name" = "$job_name" ]; then
    echo "$jobid already $job_name"
    continue
  fi

  if scontrol update JobId="$jobid" JobName="$job_name" 2>/dev/null; then
    echo "$jobid: $old_name -> $job_name"
  else
    echo "FAILED to rename $jobid (not yours, or field locked by site policy)" >&2
  fi
done
