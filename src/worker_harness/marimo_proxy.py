"""Streaming, owner-authorized transport to private loopback Marimo listeners."""
from __future__ import annotations

import asyncio
import base64
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass, field
from urllib.parse import unquote, urljoin, urlsplit

import httpx
import websockets
from fastapi import APIRouter, HTTPException, Request, WebSocket
from starlette.requests import HTTPConnection
from starlette.responses import Response, StreamingResponse
from websockets.exceptions import ConnectionClosed

from .marimo import proxy_base_path


@dataclass(frozen=True)
class ProxyTarget:
    upstream: str
    base_path: str
    token: str = field(repr=False)
    credential_kind: str


_HOP_HEADERS = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailer", "transfer-encoding", "upgrade", "proxy-connection",
}
_SAFE_METHODS = {"GET", "HEAD", "OPTIONS"}


def _headers(headers, *, response: bool = False) -> dict[str, str]:
    denied = _HOP_HEADERS | {
        part.strip().lower() for part in headers.get("connection", "").split(",")
    }
    # The serving ASGI server sets its own Date/Server headers.
    denied |= {"set-cookie", "www-authenticate", "date", "server"} if response else {
        "authorization", "cookie", "host", "forwarded", "x-real-ip",
        "sec-websocket-key", "sec-websocket-version", "sec-websocket-extensions",
        "sec-websocket-protocol", "content-length",
    }
    return {
        key: value for key, value in headers.items()
        if key.lower() not in denied and not key.lower().startswith("x-forwarded-")
    }


def _upstream_url(target: ProxyTarget, session_id: str, path: str, query: str) -> str:
    parsed = urlsplit(target.upstream)
    if (parsed.scheme != "http" or parsed.hostname != "127.0.0.1"
            or parsed.username or parsed.password or parsed.path not in ("", "/")
            or parsed.query or parsed.fragment or not parsed.port
            or target.base_path != proxy_base_path(session_id)):
        raise HTTPException(502, "Invalid notebook upstream")
    # Decoded dot segments could otherwise escape the authorized base prefix.
    decoded = unquote(path)
    if "\\" in decoded or any(part in {".", ".."} for part in decoded.split("/")):
        raise HTTPException(400, "Invalid notebook path")
    url = target.upstream.rstrip("/") + target.base_path + "/" + path
    return url + ("?" + query if query else "")


def _redirect_allowed(location: str, request_url: str, web_origin: str, base: str) -> bool:
    if "\\" in location or any(ord(c) < 32 for c in location):
        return False
    # Resolve against the configured external origin, not an untrusted Host or
    # the private reverse-proxy address seen by the application server.
    request_path = urlsplit(request_url).path
    resolved = urlsplit(urljoin(web_origin.rstrip("/") + request_path, location))
    origin = urlsplit(web_origin)
    path = unquote(resolved.path)
    return (
        resolved.scheme == origin.scheme and resolved.netloc == origin.netloc
        and not resolved.username and not resolved.password
        and (path == base or path.startswith(base + "/"))
        and not any(part in {".", ".."} for part in path.split("/"))
    )


class _RevocableStreamingResponse(StreamingResponse):
    """Check authority independently of upstream silence or client backpressure."""

    def __init__(self, *args, revalidate, interval, **kwargs):
        super().__init__(*args, **kwargs)
        self.revalidate = revalidate
        self.interval = interval

    async def stream_response(self, send):
        async def watch():
            while True:
                await asyncio.sleep(self.interval)
                try:
                    await self.revalidate()
                except HTTPException:
                    return

        body = asyncio.create_task(super().stream_response(send))
        watchdog = asyncio.create_task(watch())
        try:
            done, _ = await asyncio.wait({body, watchdog}, return_when=asyncio.FIRST_COMPLETED)
            if body in done:
                await body
            else:
                body.cancel()
                await asyncio.gather(body, return_exceptions=True)
                with suppress(OSError, RuntimeError, TimeoutError):
                    await asyncio.wait_for(
                        send({"type": "http.response.body", "body": b"", "more_body": False}),
                        timeout=self.interval,
                    )
        finally:
            body.cancel()
            watchdog.cancel()
            await asyncio.gather(body, watchdog, return_exceptions=True)
            await self.body_iterator.aclose()


def create_marimo_proxy_router(
    *, authorize: Callable[[HTTPConnection, str], Awaitable[ProxyTarget]],
    web_origin: str, recheck_seconds: float = 5.0,
) -> APIRouter:
    if recheck_seconds <= 0 or recheck_seconds > 5:
        raise ValueError("recheck_seconds must be positive and at most five seconds")
    router = APIRouter()
    prefix = "/api/v1/marimo/{session_id}/proxy"

    async def perform_http_proxy(request: Request, session_id: str, path: str = ""):
        target = await authorize(request, session_id)
        if target.credential_kind == "web_session" and request.method not in _SAFE_METHODS:
            if (request.headers.get("origin") != web_origin
                    or request.headers.get("sec-fetch-site") != "same-origin"):
                raise HTTPException(403, "Same-origin notebook request required")
        url = _upstream_url(target, session_id, path, request.url.query)
        client = httpx.AsyncClient(
            trust_env=False, follow_redirects=False,
            timeout=httpx.Timeout(30, read=None), auth=httpx.BasicAuth("wh", target.token),
        )
        try:
            upstream = await client.send(client.build_request(
                request.method, url, headers=_headers(request.headers), content=request.stream(),
            ), stream=True)
        except BaseException as exc:
            await client.aclose()
            if isinstance(exc, httpx.HTTPError):
                raise HTTPException(502, "Notebook upstream unavailable") from None
            raise
        if (upstream.status_code in {401, 403} or (
            upstream.is_redirect and upstream.headers.get("location") is not None
            and not _redirect_allowed(
                upstream.headers["location"], str(request.url), web_origin, target.base_path,
            )
        )):
            await upstream.aclose()
            await client.aclose()
            return Response(status_code=502, headers={"Cache-Control": "no-store"})

        async def stream():
            try:
                async for chunk in upstream.aiter_raw():
                    yield chunk
            finally:
                await upstream.aclose()
                await client.aclose()

        headers = _headers(upstream.headers, response=True)
        headers.update({"Cache-Control": "no-store", "X-Accel-Buffering": "no"})
        return _RevocableStreamingResponse(
            stream(), status_code=upstream.status_code, headers=headers,
            revalidate=lambda: authorize(request, session_id), interval=recheck_seconds,
        )

    async def http_proxy(request: Request, session_id: str, path: str = ""):
        try:
            return await perform_http_proxy(request, session_id, path)
        except HTTPException as exc:
            exc.headers = {**(exc.headers or {}), "Cache-Control": "no-store"}
            raise

    async def websocket_proxy(websocket: WebSocket, session_id: str, path: str = ""):
        try:
            target = await authorize(websocket, session_id)
            if target.credential_kind == "web_session" and (
                websocket.headers.get("origin") != web_origin
                or websocket.headers.get("sec-fetch-site") not in (None, "same-origin")
            ):
                await websocket.close(code=4403)
                return
            url = _upstream_url(target, session_id, path, websocket.url.query)
        except HTTPException as exc:
            await websocket.close(code=4404 if exc.status_code == 404 else 4401)
            return
        headers = _headers(websocket.headers)
        headers["Authorization"] = "Basic " + base64.b64encode(
            f"wh:{target.token}".encode("utf-8")
        ).decode("ascii")
        protocols = websocket.headers.get("sec-websocket-protocol", "")
        subprotocols = [item.strip() for item in protocols.split(",") if item.strip()]
        try:
            async with websockets.connect(
                "ws" + url[4:], extra_headers=headers, subprotocols=subprotocols or None,
                max_queue=4, open_timeout=10, close_timeout=2, ping_timeout=20,
            ) as upstream:
                await websocket.accept(subprotocol=upstream.subprotocol)

                async def to_upstream():
                    while True:
                        message = await websocket.receive()
                        if message["type"] == "websocket.disconnect":
                            return
                        data = message.get("bytes")
                        if data is None:
                            data = message.get("text", "")
                        await asyncio.wait_for(upstream.send(data), timeout=10)

                async def to_client():
                    async for message in upstream:
                        send = websocket.send_text if isinstance(message, str) else websocket.send_bytes
                        await asyncio.wait_for(send(message), timeout=10)

                async def revalidate():
                    while True:
                        await asyncio.sleep(recheck_seconds)
                        try:
                            await authorize(websocket, session_id)
                        except HTTPException as exc:
                            await websocket.close(code=4404 if exc.status_code == 404 else 4401)
                            return

                tasks = [asyncio.create_task(fn()) for fn in (to_upstream, to_client, revalidate)]
                try:
                    done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                    for task in done:
                        task.result()
                finally:
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
        except (ConnectionClosed, OSError, TimeoutError, RuntimeError, websockets.InvalidHandshake):
            pass
        finally:
            with suppress(RuntimeError, OSError):
                await websocket.close()

    for route in (prefix, prefix + "/{path:path}"):
        router.add_api_route(
            route, http_proxy, methods=["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "TRACE"],
        )
        router.add_api_websocket_route(route, websocket_proxy)
    return router
