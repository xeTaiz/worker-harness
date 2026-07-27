FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    openssh-client \
    iproute2 \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN id -u worker-harness >/dev/null 2>&1 || useradd -m worker-harness

# Tailscale userspace + daemon
RUN curl -fsSL https://tailscale.com/install.sh | sh

WORKDIR /app

COPY pyproject.toml /app/pyproject.toml
COPY src /app/src
COPY web /app/web
ENV WH_WEB_DIR=/app/web
RUN pip install --no-cache-dir .

COPY orchestrator_container/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=12 \
    CMD curl -fsS "http://127.0.0.1:${WH_CONTROL_PORT:-12889}/health" || exit 1

ENTRYPOINT ["/entrypoint.sh"]
