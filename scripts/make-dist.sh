#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"

if [ ! -f .env ]; then
  echo "[make-dist] ERROR: missing .env" >&2
  exit 1
fi

set -a
. ./.env
set +a

: "${WORKER_TS_KEY:?WORKER_TS_KEY missing from .env}"
: "${ORCHESTRATOR_HOST:?ORCHESTRATOR_HOST missing from .env}"
TS_HOST="${TS_HOST:-https://headscale.d0me.xyz}"

rm -rf dist
mkdir -p dist

cp start-wh.sh dist/start-wh.sh
cp install-service.sh dist/install-service.sh
cp systemd/worker-harness.service dist/worker-harness.service
cp systemd/worker-harness-update.path dist/worker-harness-update.path
cp systemd/worker-harness-update.service dist/worker-harness-update.service
cp systemd/worker-harness-restart.path dist/worker-harness-restart.path
cp systemd/worker-harness-restart.service dist/worker-harness-restart.service

# Copy the repo .env as-is — install-service.sh handles it on the target.
# Any WH_* vars in the repo .env are preserved automatically.
cp -f .env dist/.env

if [ -f worker-harness-worker.sif ]; then
  cp worker-harness-worker.sif dist/worker-harness-worker.sif
fi

chmod +x dist/start-wh.sh dist/install-service.sh

cat > dist/README.md <<'EOF'
# worker-harness dist

Generated deploy bundle for a single worker host.

Contents:
- `start-wh.sh`
- `install-service.sh`
- `worker-harness.service` — main service (Restart=always)
- `worker-harness-update.path` / `.service` — auto-swap new image + restart
- `worker-harness-restart.path` / `.service` — restart on trigger file
- `.env`
- `worker-harness-worker.sif` (if built)

Usage:
1. `rsync -a dist/ target:/path/to/worker-harness/`
2. On target: `cd /path/to/worker-harness && ./install-service.sh`
3. If needed: `loginctl enable-linger "$USER"`

The generated `.env` is derived from the repo `.env` and contains the runtime worker env.
You can add extra vars (e.g. `WH_EXTRA_BINDS`, `WH_MOUNT_HOME_FOLDERS`) to this file before running install-service.sh — they will be preserved in the installed config.
All `WH_*` variables are automatically carried through.
EOF

cat > dist/.gitignore <<'EOF'
*.env
*.sif
EOF

echo "[make-dist] bundle ready in $repo_dir/dist"
