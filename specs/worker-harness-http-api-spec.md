# Worker-Harness HTTP API Specification

## Services

| Service | Default URL | Purpose |
|---|---|---|
| Registration | `http://<orchestrator>:12888` | Worker-only `POST /register`, `GET /health` |
| Control | `http://<orchestrator>:12889/api/v1` | Operator and agent API |

Workers must only receive ACL access to the registration service. Control API
access is intentionally separate.

## Control endpoints

### Workers
- GET /api/v1/workers
- GET /api/v1/workers/:id
- DELETE /api/v1/workers/prune
- GET /api/v1/workers/summary

### Data
- GET /api/v1/data — advertised immediate data-directory → online-worker map
- POST /api/v1/data/copy — start a direct, ephemeral rsync copy

### Jobs
- POST /api/v1/jobs
- GET /api/v1/jobs
- GET /api/v1/jobs/:id/logs
- GET /api/v1/jobs/:id/logs/stream
- DELETE /api/v1/jobs/:id

### Tunnels
- POST /api/v1/tunnels
- GET /api/v1/tunnels
- DELETE /api/v1/tunnels/:id

### Events
- GET /api/v1/events
