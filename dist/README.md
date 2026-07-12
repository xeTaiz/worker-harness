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
