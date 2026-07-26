#!/usr/bin/env bash
# Build a self-contained, immutable Pi runtime release for direct worker push.
# The source runtime/config stays on the operator/orchestrator; workers never
# clone dotfiles or fetch Git themselves.
set -euo pipefail

source_bun="${PI_BUN_HOME:-$HOME/.bun}"
release_id="${1:?usage: build-pi-release.sh <release-id> [output-dir]}"
output_root="${2:-$PWD/dist/pi-releases}"
release_dir="$output_root/$release_id"
mkdir -p "$output_root"

bun="$source_bun/bin/bun"
global_modules="$source_bun/install/global/node_modules"
pi_cli="$global_modules/@earendil-works/pi-coding-agent/dist/cli.js"
worker_extension="$(CDPATH= cd -- "$(dirname -- "$0")/../worker_container" && pwd)/pi_worker_bash.ts"
[[ -x "$bun" ]] || { echo "missing Bun binary: $bun" >&2; exit 1; }
[[ -f "$pi_cli" ]] || { echo "missing Pi CLI: $pi_cli" >&2; exit 1; }
[[ -f "$worker_extension" ]] || { echo "missing worker bash extension: $worker_extension" >&2; exit 1; }

rm -rf "$release_dir"
mkdir -p "$release_dir/bin" "$release_dir/runtime" "$release_dir/extensions"
install -m 0644 "$worker_extension" "$release_dir/extensions/pi-worker-bash.ts"
install -m 0755 "$bun" "$release_dir/runtime/bun"
cp -a "$global_modules" "$release_dir/runtime/node_modules"

cat >"$release_dir/bin/pi" <<'EOF'
#!/usr/bin/env sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
exec "$root/runtime/bun" "$root/runtime/node_modules/@earendil-works/pi-coding-agent/dist/cli.js" "$@"
EOF
chmod 0755 "$release_dir/bin/pi"

# Delegated children are started in an isolated tool profile. In particular,
# auto-discovered host extensions (wh_ tools, vault, planners, subagents) are
# never inherited. The sole explicit extension replaces the builtin `bash`
# implementation with the worker-private durable job executor.
cat >"$release_dir/bin/pi-worker" <<'EOF'
#!/usr/bin/env sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
exec "$root/bin/pi" --no-extensions --no-skills \
  --extension "$root/extensions/pi-worker-bash.ts" \
  --tools read,write,edit,bash,grep,find,ls "$@"
EOF
chmod 0755 "$release_dir/bin/pi-worker"

# Config is intentionally optional and copied only from an explicit,
# operator-controlled source. Secrets must be injected separately, never saved
# in this release tree or manifest.
if [[ -n "${PI_CONFIG_SOURCE:-}" ]]; then
  [[ -d "$PI_CONFIG_SOURCE" ]] || { echo "PI_CONFIG_SOURCE is not a directory" >&2; exit 1; }
  mkdir -p "$release_dir/agent-config"
  # Never ship interaction history, crash logs, extensions, or arbitrary host
  # files. The allowlist is deliberately the small Pi provider/model surface.
  for name in auth.json settings.json models.json models-store.json opencode-keys.json; do
    [[ ! -f "$PI_CONFIG_SOURCE/$name" ]] || cp -a "$PI_CONFIG_SOURCE/$name" "$release_dir/agent-config/$name"
  done
  # A normal interactive profile may declare npm packages for extensions. The
  # delegated launcher intentionally has neither npm nor extensions, so retain
  # its model defaults but strip package declarations before release.
  if [[ -f "$release_dir/agent-config/settings.json" ]]; then
    SETTINGS_PATH="$release_dir/agent-config/settings.json" python3 - <<'PY'
import json, os
from pathlib import Path
path = Path(os.environ["SETTINGS_PATH"])
settings = json.loads(path.read_text(encoding="utf-8"))
settings.pop("packages", None)
provider = os.environ.get("PI_DELEGATED_PROVIDER", "").strip()
model = os.environ.get("PI_DELEGATED_MODEL", "").strip()
if bool(provider) != bool(model):
    raise SystemExit("PI_DELEGATED_PROVIDER and PI_DELEGATED_MODEL must be set together")
if provider:
    settings["defaultProvider"] = provider
    settings["defaultModel"] = model
path.write_text(json.dumps(settings, indent=2) + "\n", encoding="utf-8")
PY
  fi
fi

pi_version=$("$release_dir/bin/pi" --version)
RELEASE_DIR="$release_dir" RELEASE_ID="$release_id" PI_VERSION="$pi_version" python3 - <<'PY'
import hashlib, json, os
from pathlib import Path
root = Path(os.environ["RELEASE_DIR"])
files = {}
for path in sorted(p for p in root.rglob("*") if p.is_file()):
    if path.name == "manifest.json":
        continue
    files[str(path.relative_to(root))] = hashlib.sha256(path.read_bytes()).hexdigest()
(root / "manifest.json").write_text(json.dumps({
    "release_id": os.environ["RELEASE_ID"],
    "pi_version": os.environ["PI_VERSION"],
    "files": files,
}, indent=2) + "\n")
PY

tar -C "$output_root" -czf "$output_root/$release_id.tar.gz" "$release_id"
printf 'built %s\n' "$output_root/$release_id.tar.gz"
