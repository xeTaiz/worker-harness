"""Path discovery and safe command construction for shared data copies."""

from __future__ import annotations

import shlex
from collections import defaultdict
from pathlib import PurePosixPath
from typing import Iterable

from .models import Worker, WorkerStatus


class DataPathError(ValueError):
    """Raised when a requested data path is unsafe."""


def validate_data_path(path: str) -> str:
    """Accept a normalized, non-root absolute POSIX path."""
    if not path or not path.startswith("/"):
        raise DataPathError("path must be absolute")
    parsed = PurePosixPath(path)
    normalized = str(parsed)
    if normalized in {"/", "."} or ".." in parsed.parts:
        raise DataPathError("path must not be the filesystem root or contain '..'")
    return normalized


def is_advertised_data_path(path: str, advertised_paths: Iterable[str]) -> bool:
    """Return whether *path* is an advertised directory or one of its children.

    Workers advertise only immediate directories below configured bind roots.
    A copy may select one of those advertised directories, or a descendant
    file/directory, but cannot select an unadvertised sibling or bind root.
    """
    candidate = validate_data_path(path)
    for root in advertised_paths:
        try:
            normalized_root = validate_data_path(root)
        except DataPathError:
            continue
        if candidate == normalized_root or candidate.startswith(normalized_root + "/"):
            return True
    return False


def reverse_data_paths(
    workers: Iterable[Worker], *, include_offline: bool = False
) -> dict[str, list[dict[str, str]]]:
    """Build the intentionally minimal exact-path -> online-workers map."""
    paths: dict[str, list[dict[str, str]]] = defaultdict(list)
    for worker in workers:
        if not include_offline and worker.status != WorkerStatus.ONLINE:
            continue
        for path in sorted(set(worker.data_paths)):
            try:
                normalized = validate_data_path(path)
            except DataPathError:
                continue
            paths[normalized].append({"worker_id": worker.id, "worker_name": worker.name})
    return dict(sorted(paths.items()))


def with_worker_dir(worker: Worker, command: str) -> str:
    """Run a helper with the worker's persistent WH_DIR available."""
    wh_dir = str(PurePosixPath(worker.harness_dir).parent)
    return f"WH_DIR={shlex.quote(wh_dir)} {command}"


def source_export_command(path: str, transfer_id: str, ttl_seconds: int) -> str:
    return " ".join(
        [
            "/usr/local/lib/worker-harness/wh-data-export",
            "--path", shlex.quote(validate_data_path(path)),
            "--transfer-id", shlex.quote(transfer_id),
            "--ttl-seconds", str(ttl_seconds),
        ]
    )


def source_cleanup_command(transfer_id: str) -> str:
    return " ".join(
        [
            "/usr/local/lib/worker-harness/wh-data-export",
            "--cleanup",
            "--transfer-id", shlex.quote(transfer_id),
        ]
    )


def destination_copy_command(
    source_host: str,
    source_port: int,
    destination: str,
    username: str,
    password_file: str,
) -> str:
    if not 22000 <= source_port <= 22999:
        raise ValueError("source port is outside the reserved data range")
    return " ".join(
        [
            "/usr/local/lib/worker-harness/wh-data-import",
            "--host", shlex.quote(source_host),
            "--port", str(source_port),
            "--destination", shlex.quote(validate_data_path(destination)),
            "--username", shlex.quote(username),
            "--password-file", shlex.quote(password_file),
        ]
    )
