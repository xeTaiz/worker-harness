"""Isolated host-runtime manifest for Worker Harness.

The host runtime manifest pins the absolute lexical paths of the external
executables (and a deterministic, minimal ``PATH``) that Worker Harness
sessions rely on. The manifest lives under the user's ``XDG_CONFIG_HOME`` so
the values survive across shell profiles, SSH logins, and containerized
launches. It is written atomically with restrictive permissions and can be
validated by ``wh host doctor`` or by downstream code that wants to launch a Pi
session with the exact same interpreter set.
"""

from __future__ import annotations

import dataclasses
import datetime as _dt
import json
import os
import shutil
import stat
import subprocess
import tempfile
from pathlib import Path
from typing import Iterable, Mapping, Sequence

SCHEMA_VERSION = 1
REQUIRED_EXECUTABLES: tuple[str, ...] = (
    "wh",
    "pi",
    "bun",
    "node",
    "tmux",
    "tailscale",
)
OPTIONAL_EXECUTABLES: tuple[str, ...] = ("zellij",)
STANDARD_PATH_FALLBACK: tuple[str, ...] = ("/usr/local/bin", "/usr/bin", "/bin")
VERSION_ARGUMENTS: Mapping[str, tuple[str, ...]] = {
    "wh": ("--help",),
    "pi": ("--version",),
    "bun": ("--version",),
    "node": ("--version",),
    "tmux": ("-V",),
    "tailscale": ("version",),
    "zellij": ("--version",),
}
VALIDATION_TIMEOUT_SECONDS = 5.0
CLEAN_ENV_KEYS: tuple[str, ...] = (
    "HOME",
    "USER",
    "LOGNAME",
    "LANG",
    "LC_ALL",
    "TERM",
)


class HostRuntimeError(RuntimeError):
    """Raised for manifest validation, capture, and atomic-write failures."""


@dataclasses.dataclass(frozen=True)
class HostRuntime:
    """Immutable view of a parsed host-runtime manifest."""

    path: tuple[str, ...]
    executables: Mapping[str, str | None]
    _schema_version: int
    _generated_at: str

    def executable(self, name: str) -> str | None:
        return self.executables.get(name)

    def environment(self, base: Mapping[str, str] | None = None) -> dict[str, str]:
        """Return a copy of ``base`` (or the current process env) with PATH pinned."""

        source = dict(os.environ if base is None else base)
        source["PATH"] = os.pathsep.join(self.path)
        return source

    @property
    def schema_version(self) -> int:
        return self._schema_version

    @property
    def generated_at(self) -> str:
        return self._generated_at


@dataclasses.dataclass(frozen=True)
class ToolCheck:
    """Result of a single read-only version probe."""

    name: str
    required: bool
    available: bool
    exit_code: int | None
    timed_out: bool
    output: str
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.available and not self.timed_out and (self.exit_code == 0)

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "required": self.required,
            "available": self.available,
            "ok": self.ok,
            "exit_code": self.exit_code,
            "timed_out": self.timed_out,
            "output": self.output,
            "error": self.error,
        }


def default_manifest_path() -> Path:
    """Return the canonical manifest path honouring ``WH_HOST_RUNTIME_CONFIG``."""

    override = os.environ.get("WH_HOST_RUNTIME_CONFIG", "").strip()
    if override:
        return Path(override).expanduser()
    xdg = os.environ.get("XDG_CONFIG_HOME", "").strip()
    if xdg:
        base = Path(xdg).expanduser()
    else:
        base = Path.home() / ".config"
    return base / "worker-harness" / "host-runtime.json"


def _is_absolute_nonempty(value: str) -> bool:
    return bool(value) and os.path.isabs(value)


def _temp_root() -> str:
    try:
        return os.path.realpath(tempfile.gettempdir())
    except OSError:
        return tempfile.gettempdir()


def _is_under(value: str, root: str) -> bool:
    if not value or not root:
        return False
    try:
        common = os.path.commonpath([value, root])
    except ValueError:
        return False
    return common == root


def _dedupe_preserving_order(entries: Iterable[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for entry in entries:
        if entry in seen:
            continue
        seen.add(entry)
        normalized.append(entry)
    return normalized


def _capture_path(
    required_bins: Sequence[tuple[str, str]],
    optional_bins: Sequence[tuple[str, str | None]],
    existing_path_entries: Sequence[str],
) -> list[str]:
    """Build the deterministic PATH stored in the manifest."""

    primary: list[str] = []
    temp_root = _temp_root()
    for _, resolved in required_bins:
        if not resolved:
            continue
        directory = os.path.dirname(resolved)
        if directory and directory not in primary:
            primary.append(directory)
    for _, resolved in optional_bins:
        if not resolved:
            continue
        directory = os.path.dirname(resolved)
        if directory and directory not in primary:
            primary.append(directory)
    # Append absolute existing PATH entries (skipping temp-root noise and
    # duplicates), keeping the order returned by the surrounding PATH lookup.
    filtered_existing: list[str] = []
    seen: set[str] = set(primary)
    for entry in existing_path_entries:
        if not entry or not os.path.isabs(entry):
            continue
        if not os.path.isdir(entry):
            continue
        if entry in seen:
            continue
        if _is_under(entry, temp_root):
            continue
        seen.add(entry)
        filtered_existing.append(entry)
    final = primary + filtered_existing
    for fallback in STANDARD_PATH_FALLBACK:
        if not fallback or not os.path.isdir(fallback):
            continue
        if fallback in seen:
            continue
        seen.add(fallback)
        final.append(fallback)
    return final


def capture_host_runtime() -> HostRuntime:
    """Discover required (and any optional) executables and build a ``HostRuntime``."""

    discovered_required: list[tuple[str, str]] = []
    discovered_optional: list[tuple[str, str | None]] = []
    missing_required: list[str] = []
    for name in REQUIRED_EXECUTABLES:
        resolved = shutil.which(name)
        if resolved:
            discovered_required.append((name, os.path.abspath(resolved)))
        else:
            missing_required.append(name)
    for name in OPTIONAL_EXECUTABLES:
        resolved = shutil.which(name)
        discovered_optional.append((name, os.path.abspath(resolved)) if resolved else (name, None))
    if missing_required:
        joined = ", ".join(missing_required)
        raise HostRuntimeError(
            f"required host executables missing from PATH: {joined}"
        )
    path_entries = [entry for entry in os.environ.get("PATH", "").split(os.pathsep) if entry]
    normalized_path = _capture_path(discovered_required, discovered_optional, path_entries)
    executables: dict[str, str | None] = {
        name: resolved for name, resolved in discovered_required
    }
    for name, resolved in discovered_optional:
        executables[name] = resolved
    generated_at = _dt.datetime.now(tz=_dt.timezone.utc).replace(microsecond=0).isoformat()
    return HostRuntime(
        path=tuple(normalized_path),
        executables=executables,
        _schema_version=SCHEMA_VERSION,
        _generated_at=generated_at,
    )


def _validate_loaded_payload(
    payload: object,
) -> tuple[list[str], dict[str, str | None], int, str]:
    if not isinstance(payload, dict):
        raise HostRuntimeError("manifest must decode to a JSON object")
    schema_version = payload.get("schema_version")
    if schema_version != SCHEMA_VERSION:
        raise HostRuntimeError(
            f"unsupported manifest schema_version: {schema_version!r} (expected {SCHEMA_VERSION})"
        )
    generated_at = payload.get("generated_at")
    if not isinstance(generated_at, str) or not generated_at:
        raise HostRuntimeError("manifest missing or invalid generated_at")
    manifest_path_value = payload.get("path")
    if not isinstance(manifest_path_value, list):
        raise HostRuntimeError("manifest 'path' must be a list of strings")
    path_entries: list[str] = []
    for entry in manifest_path_value:
        if not isinstance(entry, str):
            raise HostRuntimeError("manifest 'path' entries must be strings")
        if not _is_absolute_nonempty(entry):
            raise HostRuntimeError(
                f"manifest 'path' entries must be absolute and non-empty: {entry!r}"
            )
        path_entries.append(entry)
    executables = payload.get("executables")
    if not isinstance(executables, dict):
        raise HostRuntimeError("manifest 'executables' must be a JSON object")
    normalized_executables: dict[str, str | None] = {}
    for required in REQUIRED_EXECUTABLES:
        value = executables.get(required)
        if not isinstance(value, str) or not _is_absolute_nonempty(value):
            raise HostRuntimeError(
                f"required executable {required!r} missing or not an absolute path"
            )
        normalized_executables[required] = value
    for optional in OPTIONAL_EXECUTABLES:
        if optional not in executables:
            normalized_executables[optional] = None
            continue
        value = executables[optional]
        if value is None:
            normalized_executables[optional] = None
            continue
        if not isinstance(value, str) or not _is_absolute_nonempty(value):
            raise HostRuntimeError(
                f"optional executable {optional!r} must be null or an absolute path"
            )
        normalized_executables[optional] = value
    return path_entries, normalized_executables, schema_version, generated_at


def _validate_executables_on_disk(executables: Mapping[str, str | None]) -> None:
    for name, path in executables.items():
        if path is None:
            continue
        candidate = Path(path)
        if not candidate.is_file():
            raise HostRuntimeError(f"executable {name!r} not a regular file: {path}")
        mode = candidate.stat().st_mode
        if not mode or not (mode & 0o111):
            raise HostRuntimeError(f"executable {name!r} lacks execute permission: {path}")


def load_host_runtime(required: bool = False, *, path: Path | None = None) -> HostRuntime | None:
    """Load and validate a manifest from disk."""

    manifest_path = Path(path) if path is not None else default_manifest_path()
    if not manifest_path.exists():
        if required:
            raise HostRuntimeError(f"host runtime manifest not found: {manifest_path}")
        return None
    try:
        metadata = manifest_path.lstat()
        parent_metadata = manifest_path.parent.stat()
    except OSError as exc:
        raise HostRuntimeError(f"cannot inspect manifest permissions: {exc}") from exc
    if not stat.S_ISREG(metadata.st_mode):
        raise HostRuntimeError(f"manifest is not a regular file: {manifest_path}")
    if metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) != 0o600:
        raise HostRuntimeError(
            f"manifest must be owned by the current user with mode 0600: {manifest_path}"
        )
    if (
        not stat.S_ISDIR(parent_metadata.st_mode)
        or parent_metadata.st_uid != os.getuid()
        or stat.S_IMODE(parent_metadata.st_mode) != 0o700
    ):
        raise HostRuntimeError(
            f"manifest directory must be owned by the current user with mode 0700: "
            f"{manifest_path.parent}"
        )
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf8"))
    except json.JSONDecodeError as exc:
        raise HostRuntimeError(f"manifest is not valid JSON: {exc}") from exc
    except OSError as exc:
        raise HostRuntimeError(f"cannot read manifest: {exc}") from exc
    path_entries, executables, schema_version, generated_at = _validate_loaded_payload(payload)
    _validate_executables_on_disk(executables)
    return HostRuntime(
        path=tuple(_dedupe_preserving_order(path_entries)),
        executables=executables,
        _schema_version=schema_version,
        _generated_at=generated_at,
    )


def _fsync_directory(directory: Path) -> None:
    fd = os.open(str(directory), os.O_RDONLY)
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def write_host_runtime(
    runtime: HostRuntime,
    *,
    path: Path | None = None,
) -> Path:
    """Atomically write the manifest JSON to disk."""

    manifest_path = Path(path) if path is not None else default_manifest_path()
    manifest_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(manifest_path.parent, 0o700)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": runtime._generated_at,
        "path": list(runtime.path),
        "executables": dict(runtime.executables),
    }
    temp_path: Path | None = None
    try:
        fd, temp_name = tempfile.mkstemp(
            prefix=f".{manifest_path.name}.",
            suffix=".tmp",
            dir=str(manifest_path.parent),
        )
        temp_path = Path(temp_name)
        with os.fdopen(fd, "w", encoding="utf8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temp_path, 0o600)
        os.replace(temp_path, manifest_path)
    except BaseException as exc:
        if temp_path is not None:
            try:
                temp_path.unlink()
            except OSError:
                pass
        if isinstance(exc, HostRuntimeError):
            raise
        if isinstance(exc, Exception):
            raise HostRuntimeError(f"cannot write host runtime manifest: {exc}") from exc
        raise
    os.chmod(manifest_path, 0o600)
    try:
        _fsync_directory(manifest_path.parent)
    except OSError:
        pass
    return manifest_path


def _clean_environment(runtime: HostRuntime) -> dict[str, str]:
    environment: dict[str, str] = {}
    for key in CLEAN_ENV_KEYS:
        value = os.environ.get(key)
        if value:
            environment[key] = value
    environment["PATH"] = os.pathsep.join(runtime.path)
    return environment


def _probe_tool(runtime: HostRuntime, name: str, required: bool) -> ToolCheck:
    executable = runtime.executable(name)
    if not executable:
        return ToolCheck(
            name=name,
            required=required,
            available=False,
            exit_code=None,
            timed_out=False,
            output="",
            error=None,
        )
    args = VERSION_ARGUMENTS.get(name, ("--version",))
    command = [executable, *args]
    try:
        result = subprocess.run(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=VALIDATION_TIMEOUT_SECONDS,
            check=False,
            env=_clean_environment(runtime),
        )
    except FileNotFoundError as exc:
        return ToolCheck(
            name=name,
            required=required,
            available=False,
            exit_code=None,
            timed_out=False,
            output="",
            error=str(exc),
        )
    except subprocess.TimeoutExpired as exc:
        stdout = (exc.stdout or b"").decode("utf8", errors="replace")
        stderr = (exc.stderr or b"").decode("utf8", errors="replace")
        return ToolCheck(
            name=name,
            required=required,
            available=True,
            exit_code=None,
            timed_out=True,
            output=stdout or stderr,
            error=f"timeout after {VALIDATION_TIMEOUT_SECONDS:g}s",
        )
    stdout = result.stdout.decode("utf8", errors="replace")
    stderr = result.stderr.decode("utf8", errors="replace")
    return ToolCheck(
        name=name,
        required=required,
        available=True,
        exit_code=result.returncode,
        timed_out=False,
        output=(stdout or stderr).strip(),
        error=None,
    )


def validate_host_runtime(runtime: HostRuntime) -> dict[str, ToolCheck]:
    """Run bounded read-only version probes for every required (and optional) tool."""

    results: dict[str, ToolCheck] = {}
    for name in REQUIRED_EXECUTABLES:
        results[name] = _probe_tool(runtime, name, True)
    for name in OPTIONAL_EXECUTABLES:
        if runtime.executable(name):
            results[name] = _probe_tool(runtime, name, False)
    return results


def validation_failures(results: Mapping[str, ToolCheck]) -> list[ToolCheck]:
    """Return required-tool probes that did not succeed."""

    failures: list[ToolCheck] = []
    for check in results.values():
        if check.required and not check.ok:
            failures.append(check)
    return failures


def validation_warnings(results: Mapping[str, ToolCheck]) -> list[ToolCheck]:
    """Return optional-tool probes that did not succeed."""

    return [check for check in results.values() if (not check.required) and (not check.ok)]


__all__ = [
    "CLEAN_ENV_KEYS",
    "HostRuntime",
    "HostRuntimeError",
    "OPTIONAL_EXECUTABLES",
    "REQUIRED_EXECUTABLES",
    "SCHEMA_VERSION",
    "STANDARD_PATH_FALLBACK",
    "ToolCheck",
    "VALIDATION_TIMEOUT_SECONDS",
    "VERSION_ARGUMENTS",
    "capture_host_runtime",
    "default_manifest_path",
    "load_host_runtime",
    "validate_host_runtime",
    "validation_failures",
    "validation_warnings",
    "write_host_runtime",
]