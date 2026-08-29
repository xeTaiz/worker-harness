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
`install-service.sh` links the units, scripts, rclone config, and runtime env from `~/.config/...` back to this directory. It installs the official rclone release when needed, validates each remote, and binds successful mounts at `/data_shared`, `/data_ibex`, and `/data_ibex_c2324`. The launcher maps home directories under `/code` and direct `/mnt` mountpoints to `/data`, `/data2`, and so on.
All `WH_*` variables are automatically carried through.

`just deploy` stages this bundle outside the live installation, locks out image/restart helpers, stops the worker and packaged rclone mounts, atomically swaps the install directory, and performs a health check. Failure restores the previous directory, configs, units, service state, and deferred triggers. A successful migration retains `~/worker-harness.backup.<transaction>` for manual cleanup. System-wide units and unrelated rclone service names remain outside the transaction.
