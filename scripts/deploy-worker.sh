#!/usr/bin/env bash
set -euo pipefail

remote="${1:?usage: deploy-worker.sh <ssh-target>}"
repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"

if [ ! -d dist ] || [ ! -x dist/deploy-remote.sh ]; then
  echo "[deploy] ERROR: dist bundle is missing or incomplete; run just dist" >&2
  exit 1
fi

txid="$(date +%Y%m%d%H%M%S)-$$"
stage_name=".worker-harness.stage.$txid"

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
