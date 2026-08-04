set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

image_namespace := env_var_or_default("WH_IMAGE_NAMESPACE", "xetaiz")
git_branch := `branch="$(git branch --show-current)"; printf '%s' "${branch:-detached}" | sed 's/[^A-Za-z0-9_.-]/-/g'`
git_sha := `git rev-parse --short=7 HEAD`
git_dirty := `if test -n "$(git status --porcelain)"; then printf '%s' '-dirty'; fi`
release_tag := git_branch + "-" + git_sha + git_dirty

orchestrator_repo := image_namespace + "/wh-orch"
worker_repo := image_namespace + "/wh-worker"
web_repo := image_namespace + "/wh-web"
router_repo := image_namespace + "/wh-router"

orchestrator_image := orchestrator_repo + ":latest"
worker_image := worker_repo + ":latest"
web_image := web_repo + ":latest"
router_image := router_repo + ":latest"

build: build-orch build-worker build-web build-router
    @echo "[just build] Built orchestrator, worker, web, and router images"

# Build everything (docker containers + singularity .sif) then produce dist bundle.
all: build build-singularity dist
    @echo "[just all] Done. dist bundle ready in ./dist"

build-orch:
    @echo "[just build-orch] Building {{orchestrator_repo}} with tags latest, {{git_branch}}, and {{release_tag}}"
    @docker build \
        -t {{orchestrator_repo}}:latest \
        -t {{orchestrator_repo}}:{{git_branch}} \
        -t {{orchestrator_repo}}:{{release_tag}} \
        -f orchestrator_container/Dockerfile .

build-worker:
    @echo "[just build-worker] Building {{worker_repo}} with tags latest, {{git_branch}}, and {{release_tag}}"
    @docker build \
        -t {{worker_repo}}:latest \
        -t {{worker_repo}}:{{git_branch}} \
        -t {{worker_repo}}:{{release_tag}} \
        -f worker_container/Dockerfile worker_container

build-web:
    @echo "[just build-web] Building {{web_repo}} with tags latest, {{git_branch}}, and {{release_tag}}"
    @docker build \
        -t {{web_repo}}:latest \
        -t {{web_repo}}:{{git_branch}} \
        -t {{web_repo}}:{{release_tag}} \
        -f web_container/Dockerfile .

build-router:
    @echo "[just build-router] Building {{router_repo}} with tags latest, {{git_branch}}, and {{release_tag}}"
    @docker build \
        -t {{router_repo}}:latest \
        -t {{router_repo}}:{{git_branch}} \
        -t {{router_repo}}:{{release_tag}} \
        -f router_service/Dockerfile .

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
