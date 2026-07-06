set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

orchestrator_image := "worker-harness/orchestrator:latest"
worker_image := "worker-harness/worker:latest"

build:
    @echo "[just build] Building orchestrator image: {{orchestrator_image}}"
    @docker build -t {{orchestrator_image}} -f orchestrator_container/Dockerfile .
    @echo "[just build] Building worker image: {{worker_image}}"
    @docker build -t {{worker_image}} -f worker_container/Dockerfile worker_container

build-orch:
    @echo "[just build-orch] Building orchestrator image: {{orchestrator_image}}"
    @docker build -t {{orchestrator_image}} -f orchestrator_container/Dockerfile .

build-singularity output="worker-harness-worker.sif":
    @echo "[just build-singularity] Building Singularity image: {{output}}"
    @mkdir -p .apptainer-tmp .apptainer-cache .pip-cache
    @TMPDIR="$PWD/.apptainer-tmp" APPTAINER_TMPDIR="$PWD/.apptainer-tmp" APPTAINER_CACHEDIR="$PWD/.apptainer-cache" PIP_CACHE_DIR="$PWD/.pip-cache" apptainer build --force {{output}} worker_container/Singularity.def

build-singularity-from-docker output="worker-harness-worker.sif":
    @echo "[just build-singularity-from-docker] Pulling Singularity image from docker-daemon://{{worker_image}} -> {{output}}"
    @mkdir -p .apptainer-tmp .apptainer-cache
    @TMPDIR="$PWD/.apptainer-tmp" APPTAINER_TMPDIR="$PWD/.apptainer-tmp" APPTAINER_CACHEDIR="$PWD/.apptainer-cache" apptainer pull --force {{output}} docker-daemon://{{worker_image}}

dist:
    @./scripts/make-dist.sh
