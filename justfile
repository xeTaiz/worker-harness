set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

orchestrator_image := "worker-harness/orchestrator:latest"
worker_image := "worker-harness/worker:latest"

build:
    @mkdir -p orchestrator_container/ssh
    @if [ ! -f orchestrator_container/ssh/orchestrator_ed25519 ] || [ ! -f orchestrator_container/ssh/orchestrator_ed25519.pub ]; then \
      echo "[just build] Generating orchestrator SSH keypair..."; \
      ssh-keygen -t ed25519 -N "" -f orchestrator_container/ssh/orchestrator_ed25519 -C "worker-harness-orchestrator" >/dev/null; \
    else \
      echo "[just build] Reusing existing orchestrator SSH keypair"; \
    fi
    @chmod 600 orchestrator_container/ssh/orchestrator_ed25519
    @chmod 644 orchestrator_container/ssh/orchestrator_ed25519.pub
    @cp orchestrator_container/ssh/orchestrator_ed25519.pub worker_container/authorized_keys
    @chmod 600 worker_container/authorized_keys
    @echo "[just build] Building orchestrator image: {{orchestrator_image}}"
    @docker build -t {{orchestrator_image}} -f orchestrator_container/Dockerfile .
    @echo "[just build] Building worker image: {{worker_image}}"
    @docker build -t {{worker_image}} -f worker_container/Dockerfile worker_container

clearkeys:
    @rm -f orchestrator_container/ssh/orchestrator_ed25519 orchestrator_container/ssh/orchestrator_ed25519.pub worker_container/authorized_keys
    @echo "[just clearkeys] Removed orchestrator SSH keys and worker authorized_keys"
