FROM oven/bun:1.3.14-alpine

WORKDIR /app
COPY router_service/package.json router_service/bun.lock* ./
RUN bun install --frozen-lockfile
COPY router_service/router.ts ./router.ts

ENV WH_PI_ROUTER_HOST=0.0.0.0 \
    WH_PI_ROUTER_PORT=12900 \
    PI_AGENT_DIR=/pi-agent
EXPOSE 12900
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=5 \
  CMD wget -qO- http://127.0.0.1:12900/healthz >/dev/null 2>&1 || exit 1
CMD ["bun", "run", "router.ts"]
