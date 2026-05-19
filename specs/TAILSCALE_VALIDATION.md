# Tailscale Migration Validation Checklist

## Run date

- Automated smoke checks executed during migration implementation.

## Checks

- [x] Python syntax compile check
  - Command: `python -m compileall src worker_container orchestrator_container`
  - Result: pass (no syntax errors)

- [x] Worker container bootstrap switched to Tailscale
  - `worker_container/Dockerfile` uses `tailscale.com/install.sh`
  - `worker_container/entrypoint.sh` starts `tailscaled` and runs `tailscale up`

- [x] Worker daemon uses Tailnet IP lookup
  - `worker_container/worker_daemon.py` now uses `get_tailscale_ip()`
  - Registration still sends transitional `zerotier_ip` field for compatibility

- [x] Orchestrator container added
  - `orchestrator_container/Dockerfile`
  - `orchestrator_container/entrypoint.sh`

- [x] Container-native SSH key bootstrap added
  - orchestrator generates persistent SSH key on first start
  - orchestrator exports public key to shared volume
  - worker imports orchestrator public key into `/ssh/authorized_keys`

- [x] Headscale ACL policy artifact added
  - `specs/headscale-policy.example.json`

- [x] Runtime compose example added (tun + NET_ADMIN + persistent TS state)
  - `specs/docker-compose.tailscale.example.yml`

## Remaining ZeroTier references (expected follow-up)

- CLI/help text and labels still mention ZeroTier in:
  - `src/worker_harness/cli/app.py`
  - `src/worker_harness/cli/workers.py`
  - `pyproject.toml` description
- Historical planning notes in `TODO.md` still reference ZeroTier.

These are non-blocking for runtime, but should be cleaned up in a final wording pass.
