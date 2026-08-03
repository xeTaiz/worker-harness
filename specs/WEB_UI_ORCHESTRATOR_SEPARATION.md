---
title: Worker Harness Web UI / Orchestrator Separation
status: implemented-locally
created: 2026-08-03
updated: 2026-08-03
baseline_worker_harness: giga-wh@17ecd1a
related:
  - specs/DISTRIBUTED_PI_SESSION_FABRIC.md
  - specs/PI_ATTACH_UX_NEXT.md
---

# Worker Harness Web UI / Orchestrator Separation

## 1. Phase 1 goal

Phase 1 is deployment separation only:

- build the existing PWA as an independent `wh-web` container;
- serve it only on the VPS host's Tailscale IPv4 address;
- proxy its existing same-origin session HTTP, SSE, and WebSocket traffic to the already-running orchestrator container `wh-orch:12889` over a private Docker network;
- preserve the current Tailnet trust boundary and browser behavior;
- test `wh-web` against the currently deployed orchestrator image before deploying an orchestrator image that no longer bundles `web/`.

Phase 1 does not add a second browser/API origin. The browser sees one HTTP origin owned by `wh-web`.

## 2. Explicit non-goals

The following are deferred to a later public-edge/security phase:

- public Internet exposure;
- Authelia or application accounts;
- HTTPS, HSTS, CSP, or public-edge headers;
- Origin/CSRF checks or `WH_WEB_ALLOWED_ORIGINS`;
- CORS;
- gateway-only browser transport;
- runtime `config.js` transport policy;
- authentication-expiry handling;
- service-worker security/cache redesign;
- changes to native CLI, tmux, or Zellij transport behavior.

The current gateway-first/direct-relay-fallback browser behavior and current service worker remain unchanged.

## 3. Topology

```text
Tailnet browser
  -> http://<VPS_TAILSCALE_IP>:18080
       wh-web:8080
         / and static assets
           -> baked web/ directory
         /api/v1/pi/sessions...
           -> private Docker network `wh-internal`
                -> wh-orch:12889
                     -> existing host/worker relay :27888

native CLI/tmux/Zellij
  -> existing private orchestrator Tailnet address :12889
  -> direct relay :27888 first, gateway fallback second
```

`wh-web` has no Tailscale daemon, Tailnet state, database, credentials, or persistent volume. Docker publishes its port only on the explicitly supplied `WH_WEB_BIND_IP`; there is no wildcard-bind default.

## 4. Container and network contract

Files:

- `web_container/Dockerfile` — unprivileged Nginx image with `web/` baked in;
- `web_container/nginx.conf` — static serving plus the session proxy;
- `docker-compose.web.example.yml` — standalone `wh-web` deployment;
- `orchestrator_container/Dockerfile` — production orchestrator without bundled web assets.

The compose file intentionally manages only `wh-web`. This lets it attach to the current production orchestrator without recreating or upgrading that container.

The operator creates an external user-defined network and attaches the existing orchestrator with alias `wh-orch`:

```bash
docker network inspect wh-internal >/dev/null 2>&1 || docker network create wh-internal
docker network connect --alias wh-orch wh-internal <existing-orchestrator-container>
```

`wh-web` joins the same network. Port `12889` is not published from `wh-orch` onto the VPS host for this connection.

The web container uses:

- unprivileged UID/GID `101:101`;
- read-only root filesystem;
- all Linux capabilities dropped;
- `no-new-privileges`;
- temporary writable filesystems only for `/tmp` and `/var/cache/nginx`;
- no named or bind-mounted volumes.

## 5. Proxy contract

Only `/api/v1/pi/sessions...` is proxied. Other `/api/` paths return `404` at `wh-web`; worker, job, tunnel, file, data, bridge, delegation, metrics, and admin endpoints remain available only through the private orchestrator endpoint.

Separate Nginx locations handle:

- ordinary session HTTP requests;
- `/stream` SSE with buffering/cache disabled and a timeout above the server's 15-second heartbeat;
- `/attach-gateway` WebSocket Upgrade with HTTP/1.1, buffering disabled, and long read/send timeouts.

The proxy preserves the original URI and query string. It forwards the browser-visible `Host` including a non-default port and sets `X-Forwarded-Proto` from the actual incoming scheme. This keeps the existing orchestrator-generated `gateway_websocket_url` on the `wh-web` origin.

The current PWA remains root-scoped. `/sw.js` is served with `Service-Worker-Allowed: /`; the manifest receives `application/manifest+json`.

## 6. Clean orchestrator separation

The production orchestrator image no longer copies `web/` or sets `WH_WEB_DIR=/app/web`. Its root route is therefore absent while `/health` and control APIs remain available.

`src/worker_harness/heartbeat.py` retains optional static serving when `WH_WEB_DIR` points to a real directory. This supports repository development and explicit local tests without coupling production images.

Build targets:

```bash
just build-web
just build
```

`just build` builds orchestrator, worker, and web images.

## 7. Safe deployment sequence

### Stage A — deploy only `wh-web`

1. Keep the current orchestrator image running, including its old bundled UI.
2. Create `wh-internal` and attach the current orchestrator with alias `wh-orch`.
3. Set `WH_WEB_BIND_IP` to the VPS host's Tailscale IPv4 address.
4. Start only `docker-compose.web.example.yml`.
5. Verify static assets, session list, replay/SSE, prompt/configure, and interactive/delegated terminal attachment through the new URL.
6. Keep the old orchestrator-served UI available as rollback throughout this stage.

Stopping `wh-web` returns immediately to the old UI; no orchestrator state or container changes are involved.

### Stage B — deploy the no-web orchestrator

Only after Stage A passes:

1. deploy the new orchestrator image with the existing SQLite and Tailscale state mounts unchanged;
2. attach the replacement to `wh-internal` with alias `wh-orch`;
3. restart `wh-web` so Nginx resolves the replacement container address;
4. repeat browser and native-client checks;
5. confirm the orchestrator's `/` is `404`, while `/health` and `/api/v1/pi/sessions` remain healthy.

Never run two orchestrator containers against the same Tailscale state directory.

## 8. Acceptance

Local/container acceptance must prove:

1. `wh-web` builds and runs as UID 101 with a read-only root filesystem.
2. `/healthz`, `/`, JS, CSS, manifest, service worker, icons, and vendor assets are served.
3. `/api/v1/pi/sessions` and session HTTP mutations proxy successfully.
4. the browser-visible Host and port survive through `attach-info`.
5. SSE crosses without buffering.
6. WebSocket Upgrade crosses and terminal frames can round-trip.
7. unrelated `/api/` paths return `404` without reaching the orchestrator.
8. compose rejects a missing `WH_WEB_BIND_IP` and defines no volume.
9. `wh-web` works against an orchestrator image that still bundles and serves `web/`.
10. the no-web orchestrator image contains no `/app/web`, returns `404` at `/`, and still returns `200` for health/control APIs.
11. existing Python and JavaScript regression checks remain green.

Live VPS acceptance adds:

- positive access from the intended operator Tailnet client;
- negative access from the public Internet and a tagged worker identity;
- real interactive and delegated terminal attachment through `wh-web`;
- native clients remain unchanged.

The negative Tailnet test validates parity with the current network boundary; it does not introduce application authentication.

## 9. Rollback

Before Stage B, stop or remove `wh-web` and use the old orchestrator URL.

After Stage B, either:

- roll back only `wh-web` to its previous immutable image tag; or
- stop the new orchestrator and restart the prior orchestrator image with the same state directories, then restart `wh-web`.

Do not alter the orchestrator database or Tailscale state as part of web rollback.

## 10. Deferred phase 2

A later, separately approved plan may add a public HTTPS edge, Authelia, exact Origin enforcement, gateway-only browser transport, service-worker cache migration, and a strict public route/method allowlist. None of those changes are prerequisites for Phase 1 Tailnet-only separation.
