# Setting up CUDA / nvcc inside a worker container

The worker image ships with the CUDA **runtime** (libs + driver interface) but
**not** the CUDA toolkit — no `nvcc`, no headers.  This keeps the `.sif` small.

When a job needs to **compile CUDA kernels**, install the toolkit on the fly
from pip wheels.  This is cached per-worker in `$WH_DIR/harness/` so only the
first job pays the install cost.

## Prerequisites (already in the image)

- `uv` — fast Python package installer (static binary)
- `build-essential` (gcc/g++/make), `cmake`, `ninja-build`, `ccache` — host
  compiler toolchain that nvcc shells out to

## Quick start

```bash
# 1. One-time install (cached in $WH_DIR/harness/cuda-venv + cuda-root)
wh-cuda-env install

# 2. Activate in the current shell
eval "$(wh-cuda-env env)"

# 3. Compile
nvcc my_kernel.cu -o my_kernel
# or with cmake:
cmake -DCMAKE_CUDA_COMPILER=nvcc .
make -j
```

## What `wh-cuda-env install` does

1. Creates a uv venv at `$WH_DIR/harness/cuda-venv`.
2. Installs three pip wheels:
   - `nvidia-cuda-nvcc-cu12` — the compiler (`nvcc`, `ptxas`, `fatbinary`, …)
   - `nvidia-cuda-runtime-cu12` — `cuda_runtime.h`, `cuda.h`, `libcudart.so`
   - `nvidia-cuda-cccl-cu12` — thrust / cub headers
3. Builds a symlink tree at `$WH_DIR/harness/cuda-root` that mimics a
   conventional `$CUDA_HOME` layout, so any build system (cmake, Makefile,
   `setup.py`) finds it without extra flags.

After activation, `CUDA_HOME`, `PATH`, `LD_LIBRARY_PATH`, `CPATH`, and
`LIBRARY_PATH` are all set correctly.

## Choosing a specific CUDA version

By default you get the latest CUDA 12.x wheels.  To pin a version:

```bash
wh-cuda-env install nvidia-cuda-nvcc-cu12==12.4.127 nvidia-cuda-runtime-cu12==12.4.127 nvidia-cuda-cccl-cu12==12.4.127
```

The version you compile with is **independent of the image's runtime CUDA
(12.6.3)**.  Your binary links the venv's `libcudart.so`, so run it with the
env activated (or ensure that lib dir is on `LD_LIBRARY_PATH`).

## Adding more CUDA libraries

Install additional NVIDIA wheels the same way — just append them:

```bash
wh-cuda-env install nvidia-cuda-nvcc-cu12 nvidia-cuda-runtime-cu12 nvidia-cuda-cccl-cu12 \
  nvidia-cudnn-cu12 nvidia-cublas-cu12 nvidia-cusolver-cu12 nvidia-cufft-cu12
```

After re-running `install` with extra packages, re-run `eval "$(wh-cuda-env env)"`.

## Installing system packages (apt)

The image root filesystem is read-only (SIF).  For `apt install` that needs to
persist, a **writable overlay** is bound at container start (see `start-wh.sh`
→ `WH_OVERLAY`).  Packages installed via `apt` inside the container persist
across restarts for that worker:

```bash
apt-get update && apt-get install -y ffmpeg libopenslide-dev
```

If `WH_OVERLAY` is not configured, apt installs are ephemeral (lost on
container exit).

## Python environments for ML jobs

The image does **not** ship torch/transformers/etc.  Use `uv` to create
per-job Python environments:

```bash
uv venv $WH_DIR/harness/myjob-env
source $WH_DIR/harness/myjob-env/bin/activate
uv pip install torch torchvision transformers --index-url https://download.pytorch.org/whl/cu124
```

These venvs persist in `$WH_DIR/harness/` across jobs on the same worker.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `g++ not found` | Rebuild image — `build-essential` is baked in, not installable on the fly without overlay |
| `cuda_runtime.h: No such file` | You only installed `nvidia-cuda-nvcc-cu12` — also install `nvidia-cuda-runtime-cu12` |
| `thrust/...: No such file` | Also install `nvidia-cuda-cccl-cu12` |
| `nvcc: cannot find libnvvm` | Run `wh-cuda-env install` again to rebuild the shim (nvvm symlink may be stale) |
| `libcudart.so: cannot open shared object` | You forgot to activate: `eval "$(wh-cuda-env env)"` |
