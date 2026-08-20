#!/usr/bin/env bash
# wh-submit.sh — submit a worker-harness job script with the same
# wh_<GPUTYPE>[_pi] name that wh_slurm_bootstrap gives its self-resubmitted
# successors (literal "_pi" suffix whenever ACCOUNT is non-empty, not the
# account value itself — kept in sync with wh-slurm-common.sh).
#
# Reads the literal `GPUTYPE=...` / `ACCOUNT=...` assignments out of the
# target script (the same lines wh_slurm_bootstrap reads at runtime) so the
# *first* job in a chain gets a matching name and %x-based --output log
# filename from the start, instead of only from the first resubmission on.
#
# Usage: slurm/wh-submit.sh <job-script> [extra sbatch args...] [-- slot]
set -euo pipefail

if [ $# -lt 1 ]; then
  echo "usage: $0 <job-script> [sbatch-args...]" >&2
  exit 1
fi
script="$1"
shift

strip_quotes() { local v="$1"; v="${v%\"}"; v="${v#\"}"; v="${v%\'}"; v="${v#\'}"; printf '%s' "$v"; }
gputype="$(strip_quotes "$(sed -n 's/^GPUTYPE=//p' "$script" | head -n1)")"
account="$(strip_quotes "$(sed -n 's/^ACCOUNT=//p' "$script" | head -n1)")"
: "${gputype:?GPUTYPE=... not found in $script}"

job_name="wh_${gputype}"
if [ -n "$account" ]; then
  job_name="${job_name}_pi"
fi

exec sbatch --job-name="$job_name" "$script" "$@"
