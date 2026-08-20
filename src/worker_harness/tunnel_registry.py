"""In-memory registry for persistent SSH port-forward subprocesses.

Tunnel subprocesses deliberately do not occupy WorkerLanes after setup. This
registry gives them an explicit owner so delete, shutdown, and the background
reaper can clean them up deterministically.
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass

from .ssh import _process_group_exists, _terminate_popen_process_group


@dataclass
class TunnelProcess:
    id: str
    worker_id: str
    local_port: int
    remote_port: int
    proc: subprocess.Popen
    created_at: int


class TunnelRegistry:
    def __init__(self) -> None:
        self._entries: dict[str, TunnelProcess] = {}

    def add(self, entry: TunnelProcess) -> None:
        self._entries[entry.id] = entry

    def get(self, tunnel_id: str) -> TunnelProcess | None:
        return self._entries.get(tunnel_id)

    def remove(self, tunnel_id: str) -> TunnelProcess | None:
        return self._entries.pop(tunnel_id, None)

    def reap_dead(self) -> int:
        """Reap dead tunnel leaders and terminate any surviving descendants."""
        dead = [entry for entry in self._entries.values() if entry.proc.poll() is not None]
        for entry in dead:
            self.stop(entry, grace_seconds=0.1)
            self._entries.pop(entry.id, None)
        return len(dead)

    def stats(self) -> dict:
        by_worker: dict[str, int] = {}
        live = 0
        dead = 0
        for entry in self._entries.values():
            if entry.proc.poll() is None:
                live += 1
                by_worker[entry.worker_id] = by_worker.get(entry.worker_id, 0) + 1
            else:
                dead += 1
        return {"live": live, "dead_registered": dead, "by_worker": by_worker}

    @staticmethod
    def stop(entry: TunnelProcess, grace_seconds: float = 5.0) -> bool:
        """Terminate and reap one tunnel's complete process group."""
        proc = entry.proc
        was_live = proc.returncode is None or _process_group_exists(proc.pid)
        _terminate_popen_process_group(proc, grace_seconds)
        return was_live

    def shutdown(self, grace_seconds: float = 5.0) -> int:
        """Terminate every managed tunnel, escalating to SIGKILL if needed."""
        entries = list(self._entries.values())
        self._entries.clear()
        return sum(self.stop(entry, grace_seconds) for entry in entries)