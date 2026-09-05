"""Project streamed Worker Harness attachments into a Herdr pane."""

from __future__ import annotations

import asyncio
import logging
import os
import re
import subprocess
import time
from collections.abc import Mapping
from typing import Any

from worker_harness.agents import row_agent

log = logging.getLogger(__name__)

_STATE_SOURCE = "worker-harness:attachment"
_METADATA_SOURCE = "worker-harness:attachment-display"
_CLI_TIMEOUT_SECONDS = 2.0
_STATE_MAP = {
    "working": "working",
    "idle": "idle",
    "error": "unknown",
    "disconnected": "unknown",
}

_DEFAULT_NAME = "Pi"
_TAB_OR_NEWLINE_RE = re.compile(r"[\t\n\r\f\v\u0085\u2028\u2029]+")
_INTERNAL_WHITESPACE_RE = re.compile(r"[ \u00a0\u1680\u2000-\u200a\u202f\u205f\u3000]+")


def sanitize_name(name: str | None) -> str:
    """Collapse tabs/newlines/whitespace and fall back to :data:`_DEFAULT_NAME`."""

    if name is None:
        return _DEFAULT_NAME
    text = _TAB_OR_NEWLINE_RE.sub(" ", str(name))
    text = _INTERNAL_WHITESPACE_RE.sub(" ", text)
    text = text.strip()
    return text or _DEFAULT_NAME


class HerdrAttachmentReporter:
    """Own Herdr lifecycle metadata while ``wh attach`` occupies one pane."""

    def __init__(
        self,
        binary: str | None,
        pane_id: str | None,
        environment: Mapping[str, str],
    ) -> None:
        self.binary = binary
        self.pane_id = pane_id
        self.environment = dict(environment)
        self._session_id: str | None = None
        self._agent: str | None = None
        self._last_state: str | None = None
        self._sequence = time.time_ns() // 1_000

    @classmethod
    def from_environment(
        cls,
        environment: Mapping[str, str] | None = None,
    ) -> HerdrAttachmentReporter:
        values = dict(os.environ if environment is None else environment)
        if values.get("HERDR_ENV") != "1" or not values.get("HERDR_SOCKET_PATH"):
            return cls(None, None, values)
        return cls(values.get("HERDR_BIN_PATH"), values.get("HERDR_PANE_ID"), values)

    @property
    def enabled(self) -> bool:
        return bool(self.binary and self.pane_id)

    async def bind(self, session: dict[str, Any]) -> None:
        """Bind the pane to one selected fabric session and report its initial state."""

        if not self.enabled:
            return
        session_id = str(session.get("id") or "")
        agent = row_agent(session)
        if not session_id:
            return
        if self._session_id == session_id and self._agent == agent:
            return
        await self.release()
        self._session_id = session_id
        self._agent = agent
        self._last_state = None
        await self.update_state(str(session.get("state") or "disconnected"))
        await self._report_metadata(session)

    async def update_state(self, state: str) -> None:
        """Publish one authoritative lifecycle transition for the bound attachment."""

        if not self.enabled or not self._agent or not self._session_id:
            return
        mapped = _STATE_MAP.get(state, "unknown")
        if mapped == self._last_state:
            return
        self._last_state = mapped
        await self._run_cli(
            "pane",
            "report-agent",
            self.pane_id or "",
            "--source",
            _STATE_SOURCE,
            "--agent",
            self._agent,
            "--state",
            mapped,
            "--seq",
            str(self._next_sequence()),
        )

    async def release(self) -> None:
        """Release lifecycle authority and display metadata from the current pane."""

        agent = self._agent
        if not self.enabled or not agent:
            self._clear_binding()
            return
        await self._run_cli(
            "pane",
            "report-metadata",
            self.pane_id or "",
            "--source",
            _METADATA_SOURCE,
            "--agent",
            agent,
            "--applies-to-source",
            _STATE_SOURCE,
            "--clear-display-agent",
            "--clear-token",
            "wh_session_id",
            "--clear-token",
            "wh_host",
            "--seq",
            str(self._next_sequence()),
        )
        await self._run_cli(
            "pane",
            "release-agent",
            self.pane_id or "",
            "--source",
            _STATE_SOURCE,
            "--agent",
            agent,
            "--seq",
            str(self._next_sequence()),
        )
        self._clear_binding()

    async def _report_metadata(self, session: dict[str, Any]) -> None:
        if not self._agent or not self._session_id:
            return
        name = sanitize_name(str(session.get("name") or session.get("task") or self._agent))
        marker = "π" if self._agent == "pi" else self._agent
        arguments = [
            "pane",
            "report-metadata",
            self.pane_id or "",
            "--source",
            _METADATA_SOURCE,
            "--agent",
            self._agent,
            "--applies-to-source",
            _STATE_SOURCE,
            "--display-agent",
            f"{marker} {name}",
            "--token",
            f"wh_session_id={self._session_id}",
        ]
        host = sanitize_name(str(session.get("host") or ""))
        if host and host != "Pi":
            arguments.extend(["--token", f"wh_host={host}"])
        arguments.extend(["--seq", str(self._next_sequence())])
        await self._run_cli(*arguments)

    async def _run_cli(self, *arguments: str) -> None:
        if not self.binary:
            return
        process: asyncio.subprocess.Process | None = None
        try:
            process = await asyncio.create_subprocess_exec(
                self.binary,
                *arguments,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                env=self.environment,
            )
            returncode = await asyncio.wait_for(process.wait(), timeout=_CLI_TIMEOUT_SECONDS)
            if returncode != 0:
                log.debug("Herdr attachment report exited with status %s", returncode)
        except asyncio.TimeoutError:
            if process is not None:
                process.kill()
                await process.wait()
            log.debug("Herdr attachment report timed out")
        except OSError as exc:
            log.debug("Herdr attachment report unavailable: %s", exc)

    def _next_sequence(self) -> int:
        self._sequence += 1
        return self._sequence

    def _clear_binding(self) -> None:
        self._session_id = None
        self._agent = None
        self._last_state = None


__all__ = ["HerdrAttachmentReporter", "sanitize_name"]
