#!/usr/bin/env bash
set -euo pipefail

remote="${1:?usage: deploy-worker.sh <ssh-target>|local}"
repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"

if [ ! -d dist ] || [ ! -x dist/deploy-remote.sh ]; then
  echo "[deploy] ERROR: dist bundle is missing or incomplete; run just dist" >&2
  exit 1
fi

# Deploying to this machine uses the same transaction, minus SSH. Keep it an
# explicit target so a real host named "local" is never assumed.
local_mode=0
case "$remote" in
  local|--local) local_mode=1 ;;
esac

txid="$(date +%Y%m%d%H%M%S)-$$"
stage_name=".worker-harness.stage.$txid"

if [ "$local_mode" -eq 1 ]; then
  stage_dir="$HOME/$stage_name"
  cleanup_stage() { rm -rf -- "$stage_dir"; }
  trap cleanup_stage EXIT

  # The transaction renames ~/worker-harness aside. Anything inside that tree —
  # including this repository and the running script — would move with it.
  live_dir="$(cd "$HOME" && mkdir -p worker-harness && cd worker-harness && pwd -P)"
  repo_real="$(cd "$repo_dir" && pwd -P)"
  if [ "$repo_real" = "$live_dir" ] || [[ "$repo_real" == "$live_dir/"* ]]; then
    echo "[deploy] ERROR: repository lives inside $live_dir; deployment would move it" >&2
    exit 1
  fi

  echo "[deploy] staging dist/ locally in $stage_dir"
  mkdir -p "$stage_dir"
  rsync -a --delete dist/ "$stage_dir/"

  bash "$stage_dir/deploy-remote.sh" "$stage_dir" "$txid"

  trap - EXIT
  echo "[deploy] completed: $(hostname)"
  exit 0
fi

cleanup_stage() {
  ssh -- "$remote" "rm -rf -- \"\$HOME/$stage_name\"" >/dev/null 2>&1 || true
}
trap cleanup_stage EXIT

echo "[deploy] staging dist/ on $remote"
ssh -- "$remote" "mkdir -p \"\$HOME/$stage_name\""
rsync -az --delete -e ssh dist/ "$remote:$stage_name/"

ssh -t -- "$remote" \
  "bash \"\$HOME/$stage_name/deploy-remote.sh\" \"\$HOME/$stage_name\" \"$txid\""

trap - EXIT
echo "[deploy] completed: $remote"
