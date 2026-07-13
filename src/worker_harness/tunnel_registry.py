"""In-memory registry for persistent SSH port-forward subprocesses.

Tunnel subprocesses deliberately do not occupy WorkerLanes after setup. This
registry gives them an explicit owner so delete, shutdown, and the background
reaper can clean them up deterministically.
"""

from __future__ import annotations

import os
import signal
import subprocess
from dataclasses import dataclass


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
        """Drop registry entries for tunnel processes that have already died."""
        dead = [tunnel_id for tunnel_id, entry in self._entries.items() if entry.proc.poll() is not None]
        for tunnel_id in dead:
            self._entries.pop(tunnel_id, None)
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
        """Terminate one tunnel's complete process group. Returns whether it
        was live when stop began."""
        proc = entry.proc
        if proc.poll() is not None:
            return False
        try:
            # ssh_port_forward creates a new session, so terminate the whole
            # group rather than leaving an SSH child behind.
            os.killpg(proc.pid, signal.SIGTERM)
            proc.wait(timeout=grace_seconds)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait(timeout=1)
            except (subprocess.TimeoutExpired, ProcessLookupError, PermissionError):
                pass
        except (ProcessLookupError, PermissionError):
            pass
        return True

    def shutdown(self, grace_seconds: float = 5.0) -> int:
        """Terminate every managed tunnel, escalating to SIGKILL if needed."""
        entries = list(self._entries.values())
        self._entries.clear()
        return sum(self.stop(entry, grace_seconds) for entry in entries)