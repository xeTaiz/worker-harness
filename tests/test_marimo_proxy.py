"""Exercise the proxy over real loopback HTTP, SSE and WebSocket sockets."""
import asyncio
import base64
from contextlib import asynccontextmanager, suppress
import socket
import unittest

from fastapi import FastAPI, HTTPException, Request, WebSocket
import httpx
from starlette.responses import JSONResponse, RedirectResponse, Response, StreamingResponse
import uvicorn
import websockets
from websockets.exceptions import ConnectionClosed, InvalidStatusCode

from worker_harness.marimo import wait_until_ready
from worker_harness.marimo_proxy import ProxyTarget, create_marimo_proxy_router

BASE = "/api/v1/marimo/test-session/proxy"
ORIGIN = "http://harness.test"
TOKEN = "private-native-token"
BASIC = "Basic " + base64.b64encode(f"wh:{TOKEN}".encode()).decode()


@asynccontextmanager
async def live_server(app):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen(128)
    port = sock.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, log_level="critical", lifespan="off"))
    task = asyncio.create_task(server.serve(sockets=[sock]))
    try:
        async with asyncio.timeout(5):
            while not server.started:
                if task.done():
                    await task
                await asyncio.sleep(0.01)
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        try:
            await asyncio.wait_for(task, 5)
        except TimeoutError:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        sock.close()


class MarimoProxyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.revoked = False
        self.kind = "api_key"
        self.contacts = 0
        self.auth_calls = 0
        self.stream_closed = asyncio.Event()
        self.next_event = asyncio.Event()
        upstream = FastAPI()

        @upstream.middleware("http")
        async def auth(request, call_next):
            self.contacts += 1
            if request.headers.get("authorization") != BASIC:
                return Response(status_code=401)
            return await call_next(request)

        @upstream.get(BASE + "/health")
        async def health():
            return Response(status_code=200)

        @upstream.get(BASE + "/api/status")
        async def status():
            return Response(status_code=200)

        @upstream.get(BASE + "/unauthorized")
        async def unauthorized():
            return Response(status_code=401, headers={"WWW-Authenticate": "Basic"})

        @upstream.get(BASE + "/challenge")
        async def challenge():
            return Response("ok", headers={"WWW-Authenticate": "Basic", "Set-Cookie": "native=secret"})

        @upstream.get(BASE + "/redirect")
        async def redirect(request: Request):
            return RedirectResponse(request.query_params["to"])

        @upstream.api_route(BASE + "/echo", methods=["GET", "POST"])
        async def echo(request: Request):
            body = await request.body()
            return JSONResponse({"headers": dict(request.headers), "body": body.decode(), "query": str(request.url.query)})

        @upstream.get(BASE + "/sse")
        async def sse():
            async def events():
                try:
                    yield b"data: first\n\n"
                    await self.next_event.wait()
                    yield b"data: second\n\n"
                    await asyncio.Event().wait()
                finally:
                    self.stream_closed.set()
            return StreamingResponse(events(), media_type="text/event-stream")

        @upstream.websocket(BASE + "/ws")
        async def ws(websocket: WebSocket):
            self.contacts += 1
            if websocket.headers.get("authorization") != BASIC or websocket.headers.get("cookie"):
                await websocket.close()
                return
            await websocket.accept()
            try:
                while True:
                    message = await websocket.receive()
                    if message["type"] == "websocket.disconnect":
                        return
                    if message.get("bytes") is not None:
                        await websocket.send_bytes(message["bytes"])
                    else:
                        await websocket.send_text(message["text"])
            finally:
                self.stream_closed.set()

        self.upstream_context = live_server(upstream)
        self.upstream = await self.upstream_context.__aenter__()

        async def authorize(conn, session_id):
            self.auth_calls += 1
            if self.revoked:
                raise HTTPException(401, "Revoked")
            if session_id != "test-session":
                raise HTTPException(404, "Not found")
            return ProxyTarget(self.upstream, BASE, TOKEN, self.kind)

        app = FastAPI()
        app.include_router(create_marimo_proxy_router(
            authorize=authorize, web_origin=ORIGIN, recheck_seconds=0.05,
        ))
        self.proxy_context = live_server(app)
        self.proxy = await self.proxy_context.__aenter__()
        self.client = httpx.AsyncClient(base_url=self.proxy, trust_env=False, timeout=3)

    async def asyncTearDown(self):
        await self.client.aclose()
        await self.proxy_context.__aexit__(None, None, None)
        await self.upstream_context.__aexit__(None, None, None)

    async def test_header_stripping_and_basic_injection(self):
        response = await self.client.post(BASE + "/echo?x=1", content="payload", headers={
            "Authorization": "Bearer human-key", "Cookie": "wh_session=private; other=private",
            "Forwarded": "for=bad", "X-Forwarded-For": "bad", "X-Real-IP": "bad",
            "Connection": "x-private", "X-Private": "bad",
            "Marimo-Session-Id": "kernel-id", "Content-Type": "application/json",
            "Last-Event-Id": "event-id",
        })
        self.assertEqual(response.status_code, 200)
        data = response.json()
        headers = data["headers"]
        self.assertEqual(headers["authorization"], BASIC)
        for name in ("cookie", "forwarded", "x-forwarded-for", "x-real-ip", "x-private"):
            self.assertNotIn(name, headers)
        self.assertEqual(headers["marimo-session-id"], "kernel-id")
        self.assertEqual(headers["last-event-id"], "event-id")
        self.assertEqual(data["body"], "payload")
        self.assertEqual(data["query"], "x=1")
        self.assertEqual(response.headers["cache-control"], "no-store")
        response = await self.client.get(BASE + "/challenge")
        self.assertNotIn("set-cookie", response.headers)
        self.assertNotIn("www-authenticate", response.headers)
        self.assertEqual((await self.client.get(BASE + "/unauthorized")).status_code, 502)

    async def test_redirects_stay_inside_owned_prefix(self):
        for location in ("https://evil.test/", "/api/v1/marimo/other/proxy/", "//evil.test/path", "../../outside"):
            response = await self.client.get(BASE + "/redirect", params={"to": location})
            self.assertEqual(response.status_code, 502, location)
        for location in (BASE + "/health", ORIGIN + BASE + "/health", "health"):
            response = await self.client.get(BASE + "/redirect", params={"to": location})
            self.assertEqual(response.status_code, 307, location)
            self.assertEqual(response.headers["location"], location)

    async def test_authorization_failure_never_contacts_upstream(self):
        response = await self.client.get("/api/v1/marimo/foreign/proxy/echo")
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.headers["cache-control"], "no-store")
        self.assertEqual(self.contacts, 0)
        with self.assertRaises(InvalidStatusCode):
            async with websockets.connect(self.proxy.replace("http:", "ws:") + "/api/v1/marimo/foreign/proxy/ws"):
                self.fail("foreign websocket accepted")
        self.assertEqual(self.contacts, 0)

    async def test_sse_incremental_and_revocation_closes_quiet_stream(self):
        async with self.client.stream("GET", BASE + "/sse") as response:
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.headers["x-accel-buffering"], "no")
            chunks = response.aiter_bytes()
            self.assertEqual(await asyncio.wait_for(anext(chunks), 1), b"data: first\n\n")
            self.next_event.set()
            self.assertEqual(await asyncio.wait_for(anext(chunks), 1), b"data: second\n\n")
            self.revoked = True
            with self.assertRaises(StopAsyncIteration):
                await asyncio.wait_for(anext(chunks), 1)
        await asyncio.wait_for(self.stream_closed.wait(), 1)
        self.assertGreaterEqual(self.auth_calls, 2)

    async def test_client_disconnect_cancels_upstream(self):
        async with self.client.stream("GET", BASE + "/sse") as response:
            await anext(response.aiter_bytes())
        await asyncio.wait_for(self.stream_closed.wait(), 1)

    async def test_websocket_relays_and_revocation_closes(self):
        async with websockets.connect(
            self.proxy.replace("http:", "ws:") + BASE + "/ws",
            extra_headers={"Authorization": "Bearer key", "Cookie": "wh_session=private"},
        ) as websocket:
            await websocket.send("hello")
            self.assertEqual(await websocket.recv(), "hello")
            await websocket.send(b"binary")
            self.assertEqual(await websocket.recv(), b"binary")
            self.revoked = True
            with self.assertRaises(ConnectionClosed) as closed:
                await asyncio.wait_for(websocket.recv(), 1)
            self.assertEqual(closed.exception.code, 4401)
        await asyncio.wait_for(self.stream_closed.wait(), 1)

    async def test_cookie_origin_and_fetch_metadata(self):
        self.kind = "web_session"
        for headers in ({}, {"Origin": ORIGIN}, {"Origin": ORIGIN, "Sec-Fetch-Site": "cross-site"}):
            self.assertEqual((await self.client.post(BASE + "/echo", headers=headers)).status_code, 403)
        self.assertEqual(self.contacts, 0)
        response = await self.client.post(BASE + "/echo", headers={"Origin": ORIGIN, "Sec-Fetch-Site": "same-origin"})
        self.assertEqual(response.status_code, 200)
        before = self.contacts
        for headers in ({}, {"Origin": "https://evil.test"}, {"Origin": ORIGIN, "Sec-Fetch-Site": "cross-site"}):
            with self.assertRaises(InvalidStatusCode):
                async with websockets.connect(self.proxy.replace("http:", "ws:") + BASE + "/ws", extra_headers=headers):
                    self.fail("cross-site websocket accepted")
        self.assertEqual(self.contacts, before)
        async with websockets.connect(self.proxy.replace("http:", "ws:") + BASE + "/ws", origin=ORIGIN) as websocket:
            await websocket.send("allowed")
            self.assertEqual(await websocket.recv(), "allowed")

    async def test_authenticated_prefixed_readiness_probe(self):
        port = int(self.upstream.rsplit(":", 1)[1])
        await wait_until_ready("127.0.0.1", port, 1, base_url=BASE, token=TOKEN)
        with self.assertRaises(TimeoutError):
            await wait_until_ready("127.0.0.1", port, 0.1, base_url=BASE, token="wrong")
