# Worker-Harness HTTP API Specification

## Base URL
http://localhost:8765/api/v1

## Endpoints
### Workers
- GET /api/v1/workers
- GET /api/v1/workers/:id  
- DELETE /api/v1/workers/prune
- GET /api/v1/workers/summary

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
