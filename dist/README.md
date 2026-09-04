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
Rclone mounts get 30 seconds to become ready by default. Failed mounts are disabled but remain linked for diagnostics and later restart.
Linked systemd units are enabled by their source paths; rerun the installer instead of enabling the symlink names manually.
All `WH_*` variables are automatically carried through.

`just deploy` stages this bundle outside the live installation, locks out image/restart helpers, stops the worker and packaged rclone mounts, atomically swaps the install directory, and performs a health check. Failure restores the previous directory, configs, units, service state, and deferred triggers. A successful migration retains `~/worker-harness.backup.<transaction>` for manual cleanup. System-wide units and unrelated rclone service names remain outside the transaction.
