"""Authenticated control-API transport for native clients; never local authority."""

from __future__ import annotations

import json as json_module
import os
from pathlib import Path

import httpx


class HarnessHTTPError(RuntimeError):
    """A control-API error retaining its HTTP status and optional conflict code."""

    def __init__(self, status_code: int, message: str, code: str | None = None):
        self.status_code = status_code
        self.code = code
        self.message = message
        super().__init__(f"control API returned {status_code}: "
                         f"{code + ': ' if code else ''}{message}")


def base_url() -> str:
    configured = os.environ.get("WH_ORCHESTRATOR_URL")
    if configured:
        return configured.rstrip("/")
    bridge_config = Path.home() / ".pi" / "worker-harness" / "config.json"
    try:
        configured = str(json_module.loads(bridge_config.read_text(encoding="utf8"))
                         .get("orchestratorUrl") or "")
        if configured.startswith(("http://", "https://")):
            return configured.rstrip("/")
    except (OSError, ValueError):
        pass
    from worker_harness.config import Config
    return Config.load().control.url.rstrip("/")


def authorization_headers() -> dict[str, str]:
    if "WH_SESSION_ROLE" in os.environ:
        token = os.environ.get("WH_SESSION_TOKEN", "").strip()
        if not token:
            raise RuntimeError("WH_SESSION_ROLE requires WH_SESSION_TOKEN; human API keys are not a fallback")
    else:
        token = os.environ.get("WH_API_KEY", "").strip()
        token_file = os.environ.get("WH_API_KEY_FILE", "").strip()
        if not token and token_file:
            try:
                token = Path(token_file).expanduser().read_text(encoding="utf8").strip()
            except OSError as exc:
                raise RuntimeError("Unable to read WH_API_KEY_FILE") from exc
        if not token:
            raise RuntimeError("Authentication required: set WH_API_KEY or WH_API_KEY_FILE")
    return {"Authorization": f"Bearer {token}"}


async def request(method: str, path: str, *, json: dict | None = None,
                  params: dict | None = None) -> object:
    headers = authorization_headers()
    try:
        async with httpx.AsyncClient(base_url=base_url(), timeout=30.0) as client:
            response = await client.request(method, path, json=json, params=params, headers=headers)
    except httpx.HTTPError as exc:
        raise RuntimeError(f"control API unavailable: {exc}") from exc
    if response.is_error:
        code = None
        try:
            detail = response.json().get("detail")
        except (ValueError, AttributeError):
            detail = None
        if isinstance(detail, dict):
            code = detail.get("code")
            message = str(detail.get("message") or response.reason_phrase)
        elif isinstance(detail, str):
            message = detail
        else:
            message = response.reason_phrase
        raise HarnessHTTPError(response.status_code, message, code)
    if response.status_code == 204 or not response.content:
        return None
    return response.json()
