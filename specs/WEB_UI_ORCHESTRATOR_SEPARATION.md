---
title: Worker Harness Web UI / Orchestrator Separation
status: planned
created: 2026-08-03
baseline_worker_harness: giga-wh@0bbeabb
related:
  - specs/DISTRIBUTED_PI_SESSION_FABRIC.md
  - specs/PI_ATTACH_UX_NEXT.md
---

# Worker Harness Web UI / Orchestrator Separation

## 1. Goal

Split the existing PWA from the production orchestrator image and deploy it as an independently versioned static client without creating a second API origin.

Target topology:

```text
public browser
  -> HTTPS Nginx + Authelia on a Tailnet-joined VPS
       / and static assets
         -> loopback/private standalone PWA container
       explicitly allowlisted Pi HTTP/SSE/WS routes
         -> private Tailnet orchestrator :12889
              -> existing host/worker relay :27888

native CLI/tmux/Zellij
  -> private Tailnet orchestrator :12889
  -> direct Tailnet relay :27888 first, gateway fallback second
```

The public browser is a deliberate exception to the private Tailnet-client boundary: Authelia authenticates the browser at the edge, while the orchestrator and relays remain unreachable from the Internet. The browser never connects directly to a host or worker relay.

Separation means an independent static image, deployment, health check, and release lifecycle. A new repository is not required; the PWA remains in this monorepo initially.

## 2. Fixed constraints

- Keep `:12888` worker registration and `:12889` operator control private. Never publish either port on an Internet interface.
- Keep relay port `27888` private to the Tailnet. A public browser must not attempt a direct `ws://<tailnet-host>:27888` fallback.
- Use one public HTTPS origin. `/` serves the PWA; only an explicit subset of `/api/v1/pi` is proxied to `:12889`.
- Authelia must protect the document, static assets, API requests, SSE requests, and WebSocket handshake.
- Do not add CORS or a distinct API origin.
- Preserve root PWA scope: `/sw.js`, manifest `scope: "/"`, and `start_url: "/"`.
- Keep the orchestrator gateway a raw, bounded byte proxy. Do not add SQLite work or terminal interpretation to the per-frame path.
- Native clients retain direct-first behavior and may omit `Origin`.
- Optional local development serving through `WH_WEB_DIR` may remain. The production orchestrator image must stop bundling `web/` after cutover.
- The static container contains no Tailscale daemon or credentials; the existing VPS host/edge owns Tailnet reachability.
- Do not add public delegation, bridge, worker, job, tunnel, file, data, metrics, or admin routes.

## 3. Current coupling and security gates

### 3.1 Current coupling

- `src/worker_harness/heartbeat.py::create_app()` mounts `WH_WEB_DIR`, or the repository `web/` directory, at `/` when present.
- `orchestrator_container/Dockerfile` copies `web/` into `/app/web` and sets `WH_WEB_DIR=/app/web`.
- `web/app.js` already uses same-origin relative HTTP and SSE paths.
- `web/app.js::connectTerminal()` currently tries the orchestrator gateway and then a direct Tailnet relay URL.
- `attach-info` builds its absolute gateway URL from `X-Forwarded-Proto` plus the request `Host`.
- `web/sw.js` currently precaches and serves the shell cache-first.
- No standalone PWA image or edge example exists.

### 3.2 P0 gates before Internet exposure

1. The edge is default-deny and proxies only the paths and methods in §5.
2. Authelia protects every allowed static/API/SSE/WS entry point. The internal Authelia authorization subrequest is not publicly addressable.
3. Browser mutations and WebSocket handshakes require the exact configured public `Origin`.
4. Browser terminal mode is gateway-only and uses a same-origin `wss://` URL derived from `window.location`; it never attempts `:27888`.
5. TLS and HSTS are enabled at the public edge.
6. Off-Tailnet negative probes confirm `12888`, `12889`, `27888`, the PWA container port, orchestrator health, and metrics are not Internet-reachable.

### 3.3 P1 hardening

- The orchestrator rejects a present but unapproved `Origin` on browser mutation routes and the gateway WebSocket while continuing to accept missing `Origin` from private native clients.
- The edge strips client-supplied `X-Forwarded-*` and `X-Agent-Name`, then writes its own forwarding metadata. `X-Agent-Name` only selects a rate-limit bucket in the current orchestrator and carries no authorization weight.
- CSP restricts connections to the same origin and disallows objects, framing, and base-tag changes.
- The static container is non-root, read-only, capability-free, and loopback/private-network bound.
- Public health is a minimal edge/static-container `200`; orchestrator health and `_stats` remain private.
- The service worker cannot serve a cached application shell after logout or while offline.

## 4. Browser runtime contract

### 4.1 Runtime configuration

Add a non-cached classic script (not `type="module"`, `async`, or `defer`) loaded before `app.js`:

```js
window.WH_WEB_CONFIG = Object.freeze({
  terminalTransport: "gateway-direct-fallback"
});
```

The committed `web/config.js` uses `gateway-direct-fallback` for the existing local/Tailnet development experience. The standalone container generates `/tmp/config.js` at startup from `WH_WEB_TERMINAL_TRANSPORT`, accepting only:

- `gateway-only` — production default for the public image;
- `gateway-direct-fallback` — explicit private/local development override.

Nginx serves `/config.js` from `/tmp` with `Cache-Control: no-store`; this exact location/alias must take precedence over the committed `web/config.js` in the static root. The entrypoint uses a fixed `case` statement and fixed templates rather than interpolating arbitrary JavaScript. No secret belongs in browser configuration.

A runtime file is preferred to inferring policy from HTTPS: transport authorization is a deployment decision, not a scheme heuristic, and the setting must be changeable without rebuilding application assets.

### 4.2 Gateway URL

The browser always derives the gateway candidate from its own origin:

```text
http page  -> ws://<window.location.host>/api/v1/pi/sessions/<id>/attach-gateway
https page -> wss://<window.location.host>/api/v1/pi/sessions/<id>/attach-gateway
```

It does not consume the server-generated absolute gateway URL. This removes browser dependence on forwarded-host/proto correctness. In `gateway-only` mode it creates exactly one candidate. In private fallback mode it appends the current direct relay candidate after the same-origin gateway.

The existing absolute `gateway_websocket_url` remains in `attach-info` for native CLI compatibility. Direct relay metadata is not secret, but CSP and the browser mode prevent its use by the public client.

### 4.3 Authentication expiry

The edge returns a plain `401` for unauthenticated API/SSE/WS requests; it redirects top-level/static navigation to Authelia.

- `api()` treats `401` as an expired browser session and navigates to `/`, triggering the authenticated document flow.
- SSE `error` performs a small same-origin authentication probe. A `401` navigates to `/`; otherwise normal EventSource recovery continues.
- A failed gateway-only WebSocket handshake performs the same probe before rendering a transport error.
- Authelia authenticates a WebSocket at handshake time. An already-open stream is not re-authenticated mid-frame; revocation takes effect on the next reconnect. This is an explicit residual property.

General transient WebSocket exponential-backoff is not part of this separation slice; it remains an M8 follow-up unless implementation testing shows it is required for cutover correctness.

## 5. Public edge allowlist

The edge must match normalized paths exactly and reject every other `/api` path. Publicly addressable session IDs are UUID/slug values and cannot contain `/` or `:`; validate this assumption against generated and bridge-provided IDs in the edge contract tests. Nginx must decode and anchor normalized single-segment paths, and the reference configuration must explicitly reject encoded slash/traversal variants such as `%2f`, `%2e`, and `%2e%2e` before proxying.

| Method/upgrade | Allowed path | Purpose |
|---|---|---|
| `GET` | `/api/v1/pi/sessions` | session directory |
| `GET` | `/api/v1/pi/sessions/<id>` | selected session |
| `GET` | `/api/v1/pi/sessions/<id>/attach-info` | attachment capability |
| `GET` | `/api/v1/pi/sessions/<id>/events` | bounded replay |
| `GET` | `/api/v1/pi/sessions/<id>/stream` | SSE tail |
| `GET` + WebSocket Upgrade | `/api/v1/pi/sessions/<id>/attach-gateway` | terminal gateway |
| `POST` | `/api/v1/pi/sessions/<id>:prompt` | prompt/steer |
| `POST` | `/api/v1/pi/sessions/<id>:configure` | model/thinking configuration |

`cancel` is intentionally excluded because the current PWA does not use it. It can be added later only with matching UI, Origin protection, and acceptance coverage.

Explicitly excluded include:

- `/api/v1/pi/bridge/*`;
- `/api/v1/pi/delegations*`;
- `/api/v1/pi/sessions/<id>:cancel`;
- `/api/v1/_stats` and `/api/v1/events`;
- workers, jobs, tunnels, data, files, summary, prune, and all other control APIs;
- every registration endpoint on `:12888`.

Use separate Nginx locations for ordinary HTTP, SSE, and WebSocket because buffering, timeouts, and Upgrade handling differ. A broad `location /api/v1/pi/` proxy is prohibited.

For the WebSocket location, explicitly set HTTP/1.1 and resend `Upgrade`/`Connection`. For SSE, disable buffering and cache, clear `Connection`, and use an inactivity timeout comfortably above the server's 15-second comment heartbeat. Preserve public `Host`, set `X-Forwarded-Proto https`, and replace rather than append untrusted forwarding headers.

## 6. Origin and CSRF contract

### 6.1 Edge enforcement

- Static and top-level navigation are authenticated by Authelia.
- Allowed API, SSE, and WebSocket handshakes use Authelia `auth_request` and return `401` rather than an HTML login redirect.
- Exact-origin checks apply to `POST :prompt`, `POST :configure`, and the WebSocket handshake.
- The accepted value is the one configured public HTTPS origin, including scheme and non-default port if any.
- A missing, mismatched, or malformed Origin receives `403` before proxying. Missing-Origin compatibility exists only on the private orchestrator path for native clients; the public edge is browser-only and requires Origin.

Same-origin JSON plus exact Origin is the browser CSRF defense. CORS remains disabled.

### 6.2 Orchestrator defense in depth

Add `WH_WEB_ALLOWED_ORIGINS` as a comma-separated exact-origin allowlist. Empty means backward-compatible allow-all.

For `:prompt`, `:configure`, and `attach-gateway`:

- if `Origin` is absent, allow the request for native/private compatibility;
- if `Origin` is present and exactly allowed, continue;
- if `Origin` is present and not allowed, return HTTP `403`; for WebSocket, reject the HTTP handshake before `accept()` (the browser observes a failed handshake and runs the authentication probe).

During shadow rollout, configure both the public HTTPS origin and any still-supported private browser origin. After cutover, keep only the public origin. Native CLI/tmux/Zellij requests continue without Origin.

Do not add global CORS. Do not trust arbitrary `X-Forwarded-Host`; browser-derived gateway URLs make it unnecessary.

## 7. Service worker and cache policy

Public asset authentication and offline shell fallback conflict: a service worker that serves cached documents/assets without a network check bypasses the desired online authentication experience. Offline operation is not a requirement.

Replace the v9 cache-first shell with a network-only service worker that:

- remains registered at `/sw.js` with root scope for installability;
- deletes all prior `wh-pi-shell-*` caches during activation;
- returns from the `fetch` handler without calling `respondWith` for non-GET requests and every `/api/` path, so API and EventSource streaming bypass the worker entirely;
- performs only an ordinary network fetch for navigations/assets, with no document or asset fallback while offline;
- uses `skipWaiting()` and `clients.claim()` so the migration takes effect promptly.

The static server uses:

- `/`, `/index.html`, `/config.js`: `Cache-Control: no-store`;
- `/sw.js`, `/app.js`, `/app.css`, manifest, icons, vendors, and fonts: `Cache-Control: private, no-cache` so online revalidation passes through the authenticated edge;
- no immutable unauthenticated browser-cache window.

This deliberately trades offline startup and maximal asset caching for an unambiguous authenticated public boundary.

## 8. Static PWA image

Add `web_container/`:

- `Dockerfile` based on a pinned `nginxinc/nginx-unprivileged:stable-alpine` digest or pinned release;
- `nginx.conf` listening on unprivileged port `8080`;
- `entrypoint.sh` validating `WH_WEB_TERMINAL_TRANSPORT`, writing fixed `/tmp/config.js`, then executing Nginx;
- internal `/healthz` with a minimal `200`;
- `/config.js` aliased from `/tmp/config.js` and all assets served at root;
- `Service-Worker-Allowed: /` on `/sw.js`.

Runtime hardening:

```yaml
user: "101:101"
read_only: true
cap_drop: [ALL]
security_opt: ["no-new-privileges:true"]
tmpfs:
  - /tmp
  - /var/cache/nginx
  - /var/run
ports:
  - "127.0.0.1:18080:8080"
```

The exact unprivileged UID and writable paths must be verified against the pinned image rather than assumed. No Tailnet state, device, capability, database, provider key, or orchestrator credential is mounted into this container.

Add `just build-web`; include the image in the aggregate build without coupling its release tag to the orchestrator image tag. Publish immutable and moving tags such as `xetaiz/wh-web:giga-wh-<commit>` and `xetaiz/wh-web:giga-wh`.

## 9. Edge artifacts

The operator already has Nginx and Authelia. Do not add an Authelia user database or a second identity deployment to this repository.

Add reference-only artifacts:

- `deploy/nginx/worker-harness-pwa.conf.example` — vhost/upstreams, exact allowlist, Authelia include placeholders, Origin map, SSE/WS handling, CSP/security headers, minimal health;
- `docker-compose.pwa.example.yml` — standalone static container bound to loopback;
- deployment documentation listing required values: public FQDN, existing Authelia auth endpoint/include, loopback PWA address, private orchestrator MagicDNS/IP, and allowed Origin.

Recommended response headers include HSTS at the TLS edge, `X-Content-Type-Options: nosniff`, strict referrer/permissions policy, and CSP:

```text
default-src 'self';
script-src 'self';
style-src 'self' 'unsafe-inline';
img-src 'self' data:;
font-src 'self';
connect-src 'self';
worker-src 'self';
manifest-src 'self';
object-src 'none';
base-uri 'none';
frame-ancestors 'none'
```

`unsafe-inline` is currently required by inline style attributes emitted by KaTeX-rendered math and retained through sanitization. Removing scripted CSS property assignments alone would not eliminate it; tightening this directive requires changing the KaTeX rendering/style strategy or a compatible nonce/hash approach.

## 10. Production orchestrator cutover

Keep the optional `WH_WEB_DIR` mount in `heartbeat.py` for repository development and explicit local tests.

After the standalone image and real edge pass the acceptance matrix:

- remove `COPY web /app/web` and `ENV WH_WEB_DIR=/app/web` from `orchestrator_container/Dockerfile`;
- build a production orchestrator image and assert `/` is `404` while `/health` and private control APIs remain healthy;
- leave SQLite and Tailscale volume mounts unchanged;
- do not run a second `tailscaled` against the orchestrator's state volume.

The separation is incomplete until the production orchestrator image no longer contains or serves the PWA.

## 11. Ordered implementation commits

### Commit 1 — Browser transport and auth-expiry contract

Files:

- `web/config.js` (new);
- `web/index.html`;
- `web/app.js`;
- frontend contract tests.

Implement validated runtime mode handling, same-origin browser-derived gateway URLs, gateway-only candidate suppression, and 401 probes for fetch/SSE/WS. Preserve private fallback mode.

Gate: a gateway-only contract test proves that no candidate contains `:27888`, a Tailnet address, or server-provided direct URL.

### Commit 2 — Auth-safe service worker and cache migration

Files:

- `web/sw.js`;
- service-worker contract tests.

Replace shell caching with network-only behavior and purge all v9/older caches. Do not cache `config.js` or API responses.

Gate: after installation, a logged-out or offline top-level reload cannot render the Worker Harness shell from service-worker cache, and `/api/.../stream` is not intercepted by `respondWith`.

### Commit 3 — Standalone PWA container and build wiring

Files:

- `web_container/Dockerfile`;
- `web_container/nginx.conf`;
- `web_container/entrypoint.sh`;
- `docker-compose.pwa.example.yml`;
- `justfile`;
- container smoke tests/scripts.

Gate: image serves root-scope assets and fixed gateway-only config, reports minimal health, runs non-root/read-only, and emits the cache headers in §7.

### Commit 4 — Orchestrator present-Origin checks

Files:

- `src/worker_harness/heartbeat.py`;
- `tests/test_pi_sessions_api.py`;
- configuration documentation.

Add `WH_WEB_ALLOWED_ORIGINS` checks to prompt, configure, and gateway routes. Missing Origin remains valid. No CORS middleware is added.

Gate: allowed, disallowed, and missing Origin tests pass for HTTP and WebSocket; native gateway tests remain green.

### Commit 5 — Existing-edge Nginx/Authelia reference configuration

Files:

- `deploy/nginx/worker-harness-pwa.conf.example`;
- deployment documentation;
- allowlist contract tests.

Add the default-deny route/method table, Authelia protection, exact Origin, same-origin proxy headers, SSE no-buffering, WS Upgrade, CSP, and minimal health. Do not add identity secrets or an Authelia deployment.

Gate: `nginx -t` passes in the pinned test image; allowlisted requests reach mock upstreams; excluded paths/methods never do; unauthorized static/API/SSE/WS cases have the intended redirect/401 behavior.

### Commit 6 — Shadow deployment and real HTTPS acceptance

No cutover yet. Publish the PWA image, bind it to VPS loopback, configure the existing authenticated edge, set `WH_WEB_ALLOWED_ORIGINS`, and execute §12. The old orchestrator-served PWA remains available privately as rollback.

Gate: every P0 row is green, including real interactive and delegated gateway attach, before removing assets from the orchestrator image. The deployed orchestrator must report/configure a non-empty `WH_WEB_ALLOWED_ORIGINS`; empty allow-all is prohibited for the public deployment.

### Commit 7 — Production image cutover

Files:

- `orchestrator_container/Dockerfile`;
- static-serving tests made explicit through `WH_WEB_DIR`;
- README/spec status.

Stop copying assets into the production orchestrator image. Keep optional local serving in code.

Gate: public PWA remains healthy, production orchestrator `/` is absent, local `WH_WEB_DIR` serving still passes, and native clients remain unchanged.

## 12. Acceptance matrix

| # | Scenario | Expected result |
|---|---|---|
| 1 | Anonymous browser requests `/` and an asset | Redirect/challenge by Authelia; no PWA shell is served |
| 2 | Anonymous HTTP API, SSE, and WS handshake | `401`/handshake rejection; no orchestrator request or byte stream opens |
| 3 | Authenticated HTTPS load | One origin; shell, manifest, icons, and sessions load; service worker controls `/` |
| 4 | Browser network inspection | HTTP/SSE use same-origin HTTPS; terminal uses same-origin WSS; no request targets `:12888`, `:12889`, `:27888`, or a Tailnet relay address |
| 5 | Interactive Pi terminal | Existing gateway streams input/output/resize through the edge; detach leaves Pi alive |
| 6 | Delegated Pi terminal | Gateway reaches worker relay privately; input/output/resize and close reasons propagate |
| 7 | Semantic prompt/configure | Exact-origin JSON mutations succeed; missing or forged Origin receives `403` at the public edge; replay/SSE converges |
| 8 | SSE idle for at least 45 seconds | At least two 15-second comments cross promptly; no proxy buffering or idle close |
| 9 | Auth expires | New fetch/SSE/WS handshake goes to login; an already-open WS is documented to persist only until reconnect |
| 10 | Service-worker migration/logout/offline | Old `wh-pi-shell-*` caches are deleted; logged-out or offline navigation cannot render cached shell; `/api/.../stream` bypasses service-worker interception |
| 11 | Edge allowlist negative matrix | Bridge, delegations, cancel, stats, jobs, workers, tunnels, data, files, summary, prune, wrong methods, encoded-slash bypasses, and unknown `/api` paths do not reach the orchestrator |
| 12 | Private-port negative tests from off Tailnet | `12888`, `12889`, `27888`, PWA `8080/18080`, orchestrator health, and metrics are unreachable |
| 13 | Native CLI/tmux/Zellij | Direct relay remains first; private gateway fallback works; missing Origin remains accepted |
| 14 | Attachment bounds through edge | `4408` idle and `4410` replacement propagate; session survives; newest client is admitted at cap |
| 15 | Slow browser/downstream | Existing send watchdog bounds the gateway; no unbounded memory or SQLite starvation |
| 16 | Static container hardening | Non-root UID, read-only root, no capabilities/secrets/Tailnet state; only declared tmpfs paths writable |
| 17 | Cutover image | Standalone PWA stays available; production orchestrator no longer contains/serves `web/`; private health/control remain healthy |
| 18 | Rollback | Previous PWA image or old orchestrator image restores browser UI without changing Tailscale/SQLite volumes |

## 13. Automated verification

Add tests/scripts for:

- frontend candidate construction in both runtime modes;
- 401/auth-expiry state transitions;
- network-only service-worker behavior and old-cache deletion;
- static image headers, root scope, health, UID, read-only filesystem;
- HTTP/WS present-Origin allow/deny/missing behavior;
- Nginx syntax plus a mock-auth/mock-static/mock-orchestrator route matrix;
- existing gateway frame fidelity, unavailable-upstream, idle, replacement, and SSE cursor tests;
- explicit local static mount through `WH_WEB_DIR`;
- production orchestrator image assertion that `/` is absent.

Run the complete non-live Python suite, JavaScript syntax checks, container build/smoke, `nginx -t`, and diff checks before shadow deployment.

Real-browser acceptance must inspect the network panel or capture edge access logs to prove gateway-only transport; successful rendering alone is insufficient.

## 14. Deployment and rollback

### Deployment order

1. Land browser mode, service-worker migration, static image, Origin defense, tests, and edge examples while the orchestrator still bundles the old PWA.
2. Build and publish immutable PWA and pre-cutover orchestrator images.
3. Configure a non-empty `WH_WEB_ALLOWED_ORIGINS` on the private orchestrator, assert the deployed value/readiness before enabling the edge, and restart it with the same SQLite and `/var/lib/tailscale` volumes.
4. Deploy the PWA container on VPS loopback/private Docker network with `gateway-only` default.
5. Validate Nginx/Authelia configuration, then enable the HTTPS vhost.
6. Execute all P0 and live acceptance rows. Keep the private old PWA available during this shadow phase.
7. Remove web assets from the production orchestrator image, deploy it with unchanged persistent volumes, and repeat health/browser/native tests.

### Rollback

- Edge/PWA failure: roll the standalone PWA image back by immutable tag; the control plane and native clients remain private and unaffected.
- Edge configuration failure before cutover: route `/` back to the still-bundled private orchestrator PWA or disable the new vhost.
- Post-cutover static failure: redeploy the previous orchestrator image using the existing Tailscale and SQLite volumes, never concurrently with the replacement, or restore the prior PWA image.
- Origin misconfiguration: disable the public vhost first, then restore the prior allowed-origin list; do not temporarily expose `:12889` publicly.
- Service-worker rollback: ship a higher-revision network-only worker that purges the bad cache; never rely on downgrading the worker script because browsers may retain the newer revision.

## 15. Explicit non-goals

- Moving the PWA into a separate repository.
- Adding CORS or a second browser/API origin.
- Publishing the orchestrator, registration service, or relays directly.
- Browser direct-to-relay attachment.
- Replacing Authelia with application accounts/RBAC.
- Public access to delegation, bridge, jobs, workers, tunnels, data, files, metrics, or admin APIs.
- Reworking terminal protocol v2, multi-attach semantics, or the gateway pump.
- Adding general transient-terminal reconnect in this separation slice.
- Adding the global semantic router.
- Offline PWA operation.

## 16. External configuration references

The edge example should cite and follow:

- Nginx `auth_request`: https://nginx.org/en/docs/http/ngx_http_auth_request_module.html
- Nginx proxy/WebSocket directives: https://nginx.org/en/docs/http/ngx_http_proxy_module.html
- Authelia Nginx integration: https://www.authelia.com/integration/proxies/nginx/
- Authelia forwarded-auth validation: https://www.authelia.com/reference/guides/validating-forwarded-authentication/
- MDN SSE: https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events
- MDN Service Worker API: https://developer.mozilla.org/en-US/docs/Web/API/Service_Worker_API
- MDN CSP `connect-src`: https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Content-Security-Policy/connect-src
