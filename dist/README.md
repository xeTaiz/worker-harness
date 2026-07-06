# worker-harness dist

Generated deploy bundle for a single worker host.

Contents:
- `start-wh.sh`
- `install-service.sh`
- `worker-harness.service`
- `.env`
- `worker-harness-worker.sif` (if built)

Usage:
1. `rsync -a dist/ target:/path/to/worker-harness/`
2. On target: `cd /path/to/worker-harness && ./install-service.sh`
3. If needed: `loginctl enable-linger "$USER"`

The generated `.env` is derived from the repo `.env` and contains the runtime worker env.
