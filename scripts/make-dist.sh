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
cp migrate-to-symlinks.sh dist/migrate-to-symlinks.sh
cp deploy-remote.sh dist/deploy-remote.sh
cp systemd/worker-harness.service dist/worker-harness.service
cp systemd/worker-harness-update.path dist/worker-harness-update.path
cp systemd/worker-harness-update.service dist/worker-harness-update.service
cp systemd/worker-harness-update.sh dist/worker-harness-update.sh
cp systemd/worker-harness-restart.path dist/worker-harness-restart.path
cp systemd/worker-harness-restart.service dist/worker-harness-restart.service
cp systemd/worker-harness-restart.sh dist/worker-harness-restart.sh
for rclone_unit in systemd/rclone-*.service; do
  [ -e "$rclone_unit" ] || continue
  cp "$rclone_unit" "dist/$(basename "$rclone_unit")"
done

# This host-specific credential file is intentionally untracked. The bundle
# uses rclone's conventional filename so it can be linked into ~/.config.
if [ -f worker_rclone.conf ]; then
  cp worker_rclone.conf dist/rclone.conf
  chmod 600 dist/rclone.conf
fi
chmod +x dist/worker-harness-update.sh dist/worker-harness-restart.sh

# Copy the repo .env as-is — install-service.sh handles it on the target.
# Any WH_* vars in the repo .env are preserved automatically.
cp -f .env dist/.env

if [ -f worker-harness-worker.sif ]; then
  cp worker-harness-worker.sif dist/worker-harness-worker.sif
fi

chmod +x dist/start-wh.sh dist/install-service.sh dist/migrate-to-symlinks.sh dist/deploy-remote.sh

cat > dist/README.md <<'EOF'
# worker-harness dist

Generated deploy bundle for a single worker host.

Contents:
- `start-wh.sh`
- `install-service.sh`
- `migrate-to-symlinks.sh` — one-time migration for existing installs
- `deploy-remote.sh` — transactional activation and automatic rollback
- `worker-harness.service` — main service (Restart=always)
- `worker-harness-update.path` / `.service` — auto-swap new image + restart
- `worker-harness-restart.path` / `.service` — restart on trigger file
- `.env`
- `rclone.conf` and `rclone-*.service` (when configured)
- `worker-harness-worker.sif` (if built)

Usage:
1. From the repository, run `just deploy target`.
2. `target` may be an SSH config host or `user@hostname`.
3. If needed: `loginctl enable-linger "$USER"`

The generated `.env` is derived from the repo `.env` and contains the runtime worker env.
`install-service.sh` links units, scripts, config, and runtime env from `~/.config/...` back to this directory. The common rclone config is authoritative; network mounts bind below `/data/shared/<name>` and local `/mnt` storage below `/data/local`. Deployment chooses existing `~/Work`, then `~/Dev`, or creates `~/Work`, and mounts that single collection at `/code`. `list_data` shallowly advertises immediate directories inside each collection.
Linked systemd units are enabled by their source paths; rerun the installer instead of enabling the symlink names manually.
All `WH_*` variables are automatically carried through.

`just deploy` stages this bundle outside the live installation, locks out image/restart helpers, stops the worker and packaged rclone mounts, atomically swaps the install directory, and performs a health check. Failure restores the previous directory, configs, units, service state, and deferred triggers. A successful migration retains `~/worker-harness.backup.<transaction>` for manual cleanup. System-wide units and unrelated rclone service names remain outside the transaction.
EOF

cat > dist/.gitignore <<'EOF'
*.env
*.sif
rclone.conf
EOF

echo "[make-dist] bundle ready in $repo_dir/dist"
