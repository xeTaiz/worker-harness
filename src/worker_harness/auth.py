"""Human identity: passwords, API keys, browser sessions and request principals.

Every authenticated request resolves to a concrete :class:`Principal` (never a
``None`` authority). Bearer credentials are checked first and never fall back
to the browser cookie. Ordinary API keys reach only the ordinary job, queue,
usage, worker-read, tunnel and Marimo surface; browser-only routes (keys,
administration) reject every bearer credential.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import os
import re
import secrets
import time
from collections import deque
from dataclasses import dataclass, field
from ipaddress import IPv4Network, IPv6Network, ip_address, ip_network
from typing import Literal
from urllib.parse import urlsplit

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError, VerifyMismatchError
from fastapi import HTTPException
from starlette.requests import HTTPConnection

from .db import AuditActor, Database
from .models import PiSession, PiSessionState

SESSION_COOKIE = "wh_session"
SESSION_TTL_SECONDS = 12 * 60 * 60
API_KEY_PREFIX = "whk_"
USERNAME_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{2,63}$")
PASSWORD_MIN = 12
PASSWORD_MAX = 1024
SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})
LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "[::1]", "::1"})
MARIMO_PROXY_PATH = re.compile(r"^/api/v1/marimo/[^/]+/proxy(/|$)")

CredentialKind = Literal["api_key", "web_session", "pi_session"]


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def new_api_key() -> tuple[str, str, str]:
    """Return (token, sha256 hash, nonsecret display prefix)."""
    token = API_KEY_PREFIX + secrets.token_urlsafe(32)
    return token, hash_token(token), token[: len(API_KEY_PREFIX) + 6]


def normalize_username(value: str) -> str:
    username = value.strip().lower()
    if not USERNAME_PATTERN.fullmatch(username):
        raise ValueError("username must match [a-z0-9][a-z0-9_-]{2,63}")
    return username


def validate_password(value: str) -> str:
    if not PASSWORD_MIN <= len(value) <= PASSWORD_MAX:
        raise ValueError(f"password must be {PASSWORD_MIN}-{PASSWORD_MAX} characters")
    return value


def validate_display_name(value: str) -> str:
    if not 1 <= len(value.strip()) <= 100:
        raise ValueError("display_name must be 1-100 characters")
    return value.strip()


def load_web_origin() -> str:
    """Return the single exact external origin from WH_WEB_ORIGIN."""
    raw = os.environ.get("WH_WEB_ORIGIN", "").strip()
    if not raw:
        raise RuntimeError("WH_WEB_ORIGIN must be set to the exact external web origin")
    parts = urlsplit(raw)
    if parts.scheme not in {"https", "http"} or not parts.netloc or parts.path not in {"", "/"} or parts.query:
        raise RuntimeError("WH_WEB_ORIGIN must be an origin like https://harness.example")
    if parts.scheme == "http" and (parts.hostname or "") not in {"localhost", "127.0.0.1", "::1"}:
        raise RuntimeError("WH_WEB_ORIGIN must use https except for loopback development")
    return f"{parts.scheme}://{parts.netloc}"


class PasswordService:
    """Argon2id hashing off the event loop with a constant-shape unknown-user path."""

    def __init__(self) -> None:
        self._hasher = PasswordHasher()
        self._dummy_hash = self._hasher.hash(secrets.token_urlsafe(24))

    async def hash(self, password: str) -> str:
        return await asyncio.to_thread(self._hasher.hash, password)

    def _verify(self, password_hash: str, password: str) -> bool:
        try:
            return self._hasher.verify(password_hash, password)
        except (VerifyMismatchError, VerificationError, InvalidHashError):
            return False

    async def verify(self, password_hash: str | None, password: str) -> bool:
        if password_hash is None:
            await asyncio.to_thread(self._verify, self._dummy_hash, password)
            return False
        return await asyncio.to_thread(self._verify, password_hash, password)

    def needs_rehash(self, password_hash: str) -> bool:
        return self._hasher.check_needs_rehash(password_hash)


class LoginRateLimiter:
    """At most five failed logins per minute per username and per client address.

    Attempts are reserved before any await, so concurrent requests count
    against the limit while their password verification is in flight.
    """

    def __init__(self, limit: int = 5, window_seconds: float = 60.0) -> None:
        self.limit = limit
        self.window = window_seconds
        self._failures: dict[str, deque[float]] = {}
        self._in_flight: dict[str, int] = {}

    def _prune(self, key: str, now: float) -> deque[float]:
        bucket = self._failures.setdefault(key, deque())
        while bucket and now - bucket[0] >= self.window:
            bucket.popleft()
        if not bucket:
            self._failures.pop(key, None)
            bucket = self._failures.setdefault(key, deque())
        return bucket

    def reserve(self, keys: list[str], now: float | None = None) -> int:
        """Reserve one attempt on every key; return Retry-After seconds when refused."""
        now = time.monotonic() if now is None else now
        waits = []
        for key in keys:
            bucket = self._prune(key, now)
            if len(bucket) + self._in_flight.get(key, 0) >= self.limit:
                waits.append(self.window - (now - bucket[0]) if bucket else self.window)
        if waits:
            return max(1, int(max(waits) + 0.999))
        for key in keys:
            self._in_flight[key] = self._in_flight.get(key, 0) + 1
        return 0

    def finish(self, keys: list[str], *, failed: bool, now: float | None = None) -> None:
        """Release a reservation, recording it as a failure when it failed."""
        now = time.monotonic() if now is None else now
        for key in keys:
            remaining = self._in_flight.get(key, 0) - 1
            if remaining > 0:
                self._in_flight[key] = remaining
            else:
                self._in_flight.pop(key, None)
            if failed:
                self._prune(key, now).append(now)


@dataclass(frozen=True)
class ProxyTrust:
    """How to find the browser's address for login rate limiting.

    Either ``networks`` (WH_TRUSTED_PROXIES): peers inside these IPs/CIDRs are
    proxies, and the nearest X-Forwarded-For hop outside them is the client.
    Or ``hops`` (WH_TRUSTED_PROXY_HOPS): exactly this many appending reverse
    proxies always sit in front of the API, so the client is the hop-th entry
    from the right. Hop counting needs no addresses but is only sound when the
    API cannot be reached without passing through all of those proxies.
    """

    networks: tuple[IPv4Network | IPv6Network, ...] = ()
    hops: int = 0


def load_proxy_trust() -> ProxyTrust:
    raw_networks = os.environ.get("WH_TRUSTED_PROXIES", "").strip()
    raw_hops = os.environ.get("WH_TRUSTED_PROXY_HOPS", "").strip()
    if raw_networks and raw_hops:
        raise RuntimeError("Set either WH_TRUSTED_PROXIES or WH_TRUSTED_PROXY_HOPS, not both")
    if raw_hops:
        if not raw_hops.isdecimal() or not 1 <= int(raw_hops) <= 8:
            raise RuntimeError("WH_TRUSTED_PROXY_HOPS must be a number of proxies from 1 to 8")
        return ProxyTrust(hops=int(raw_hops))
    try:
        networks = tuple(ip_network(item.strip(), strict=False) for item in raw_networks.split(",") if item.strip())
    except ValueError as exc:
        raise RuntimeError("WH_TRUSTED_PROXIES must list IP addresses or CIDR networks") from exc
    return ProxyTrust(networks=networks)


def client_address(connection: HTTPConnection, trust: ProxyTrust) -> str:
    """Peer address, or the browser's address as forwarded by trusted proxies."""

    def valid(value: str) -> str | None:
        try:
            return str(ip_address(value))
        except ValueError:
            return None

    peer = connection.client.host if connection.client else "unknown"
    hops = [hop.strip() for hop in connection.headers.get("x-forwarded-for", "").split(",") if hop.strip()]
    if trust.hops:
        # Entries left of the first proxy's appended hop are client-supplied.
        if len(hops) < trust.hops:
            return peer
        return valid(hops[-trust.hops]) or peer

    def is_trusted(value: str) -> bool:
        address = valid(value)
        return address is not None and any(ip_address(address) in network for network in trust.networks)

    if not trust.networks or not is_trusted(peer):
        return peer
    for hop in reversed(hops):
        if not is_trusted(hop):
            return valid(hop) or peer
    return peer


@dataclass(frozen=True)
class Principal:
    user_id: str
    credential_kind: CredentialKind
    actor_id: str
    pi_session: PiSession | None = None
    username: str = ""
    display_name: str = ""
    is_admin: bool = False
    is_bootstrap: bool = False
    web_session_id: str | None = None
    csrf_token: str = field(default="", repr=False)
    # Hash of the presented credential, used only to revalidate long streams.
    credential_hash: str = field(default="", repr=False)

    @property
    def is_bootstrap_web(self) -> bool:
        return self.credential_kind == "web_session" and self.is_bootstrap

    def audit_actor(self) -> AuditActor:
        return AuditActor(user_id=self.user_id, display_name=self.display_name, actor_id=self.actor_id)


_INACTIVE_PI_STATES = {PiSessionState.STOPPED, PiSessionState.FAILED, PiSessionState.TERMINATION_UNKNOWN}


async def _principal_for_user(
    db: Database, user: dict, *, kind: CredentialKind, actor_id: str, credential_hash: str,
    pi_session: PiSession | None = None, web_session: dict | None = None,
) -> Principal:
    bootstrap = await db.bootstrap_user_id()
    return Principal(
        user_id=user["id"],
        credential_kind=kind,
        actor_id=actor_id,
        pi_session=pi_session,
        username=user["username"],
        display_name=user["display_name"],
        is_admin=bool(user["is_admin"]),
        is_bootstrap=bootstrap == user["id"],
        web_session_id=web_session["id"] if web_session else None,
        csrf_token=web_session["csrf_token"] if web_session else "",
        credential_hash=credential_hash,
    )


async def _resolve_bearer(db: Database, token: str) -> Principal | None:
    token_hash = hash_token(token)
    now = int(time.time())
    if token.startswith(API_KEY_PREFIX):
        resolved = await db.resolve_api_key(token_hash, now=now)
        if resolved is not None:
            key, user = resolved
            return await _principal_for_user(
                db, user, kind="api_key", actor_id=f"api_key:{key['id']}", credential_hash=token_hash,
            )
    session = await db.get_pi_session_by_token_hash(token_hash)
    if session is None or not session.token_hash or session.state in _INACTIVE_PI_STATES:
        return None
    user = await db.get_user(session.owner_user_id) if session.owner_user_id else None
    if user is None or user["disabled"]:
        return None
    return await _principal_for_user(
        db, user, kind="pi_session", actor_id=f"pi_session:{session.id}",
        credential_hash=token_hash, pi_session=session,
    )


async def resolve_principal(connection: HTTPConnection) -> Principal:
    """Resolve the request's credential; raise 401 when none is valid."""
    db: Database = connection.app.state.db
    authorization = connection.headers.get("authorization")
    if authorization is not None:
        scheme, separator, token = authorization.partition(" ")
        token = token.strip()
        if separator != " " or scheme.lower() != "bearer" or not token or not token.isascii():
            raise HTTPException(status_code=401, detail="invalid bearer token")
        principal = await _resolve_bearer(db, token)
        if principal is None:
            raise HTTPException(status_code=401, detail="invalid or revoked bearer token")
        return principal
    cookie = connection.cookies.get(SESSION_COOKIE)
    if cookie:
        token_hash = hash_token(cookie)
        resolved = await db.resolve_web_session(token_hash, now=int(time.time()))
        if resolved is not None:
            session, user = resolved
            return await _principal_for_user(
                db, user, kind="web_session", actor_id=f"web_session:{session['id']}",
                credential_hash=token_hash, web_session=session,
            )
    raise HTTPException(status_code=401, detail="authentication required")


async def principal_still_valid(db: Database, principal: Principal) -> bool:
    """Recheck revocation, expiry and account status for long-lived streams."""
    now = int(time.time())
    if principal.credential_kind == "api_key":
        resolved = await db.resolve_api_key(principal.credential_hash, now=now)
        return resolved is not None and resolved[1]["id"] == principal.user_id
    if principal.credential_kind == "web_session":
        resolved = await db.resolve_web_session(principal.credential_hash, now=now)
        return resolved is not None and resolved[1]["id"] == principal.user_id
    session = await db.get_pi_session_by_token_hash(principal.credential_hash)
    if session is None or session.state in _INACTIVE_PI_STATES:
        return False
    user = await db.get_user(session.owner_user_id)
    return user is not None and not user["disabled"]


def origin_matches(connection: HTTPConnection, web_origin: str) -> bool:
    origin = connection.headers.get("origin")
    return origin is not None and hmac.compare_digest(origin, web_origin)


def enforce_browser_mutation(request: HTTPConnection, principal: Principal, web_origin: str) -> None:
    """Cookie-authenticated harness mutations need exact Origin and CSRF token."""
    if principal.credential_kind != "web_session" or request.scope.get("method", "GET") in SAFE_METHODS:
        return
    if MARIMO_PROXY_PATH.match(request.url.path):
        # The native notebook frontend cannot send the harness CSRF header;
        # the proxy enforces exact Origin + Sec-Fetch-Site instead.
        return
    if not origin_matches(request, web_origin):
        raise HTTPException(status_code=403, detail="origin not allowed")
    token = request.headers.get("x-csrf-token", "")
    if not token or not hmac.compare_digest(token, principal.csrf_token):
        raise HTTPException(status_code=403, detail="missing or invalid CSRF token")


# Ordinary job/queue/usage/worker-read/tunnel/Marimo surface, shared by API
# keys and every browser user. Everything else is privileged.
_ORDINARY_ROUTES: tuple[tuple[frozenset[str], re.Pattern[str]], ...] = tuple(
    (frozenset(methods), re.compile(pattern))
    for methods, pattern in (
        ({"GET"}, r"^/api/v1/workers$"),
        ({"GET"}, r"^/api/v1/workers/summary$"),
        ({"GET"}, r"^/api/v1/workers/[^/]+$"),
        ({"GET"}, r"^/api/v1/data$"),
        ({"GET", "POST"}, r"^/api/v1/jobs$"),
        ({"GET", "POST"}, r"^/api/v1/jobs/queue$"),
        ({"GET"}, r"^/api/v1/jobs/queue/history$"),
        ({"GET", "PATCH", "DELETE"}, r"^/api/v1/jobs/[^/]+$"),
        ({"PATCH"}, r"^/api/v1/jobs/[^/]+/queue$"),
        ({"POST"}, r"^/api/v1/jobs/[^/]+/queue/(preview|move)$"),
        ({"GET"}, r"^/api/v1/jobs/[^/]+/logs$"),
        ({"GET"}, r"^/api/v1/jobs/[^/]+/logs/stream$"),
        ({"GET"}, r"^/api/v1/usage$"),
        ({"GET", "POST"}, r"^/api/v1/tunnels$"),
        ({"DELETE"}, r"^/api/v1/tunnels/[^/]+$"),
        ({"GET", "POST"}, r"^/api/v1/marimo$"),
        ({"GET", "DELETE"}, r"^/api/v1/marimo/[^/]+$"),
    )
)


def is_ordinary_route(method: str, path: str) -> bool:
    if MARIMO_PROXY_PATH.match(path):
        return True
    return any(method in methods and pattern.match(path) for methods, pattern in _ORDINARY_ROUTES)


def enforce_capability(principal: Principal, method: str, path: str) -> None:
    """Server-side route capability boundary for every credential kind."""
    if path.startswith("/api/v1/auth/") or path.startswith("/api/v1/admin/"):
        if principal.credential_kind != "web_session":
            raise HTTPException(status_code=403, detail="browser session required")
        return
    if path.startswith("/api/v1/pi/"):
        if principal.credential_kind == "api_key":
            raise HTTPException(status_code=403, detail="Pi workflow is not available to API keys")
        if principal.credential_kind == "web_session" and not principal.is_bootstrap:
            raise HTTPException(status_code=403, detail="Pi workflow belongs to the bootstrap administrator")
        return
    if principal.credential_kind == "pi_session" and principal.pi_session.role == "orchestrator":
        raise HTTPException(status_code=403, detail="caller role is not allowed")
    if is_ordinary_route(method, path):
        return
    if principal.credential_kind == "api_key":
        raise HTTPException(status_code=403, detail="operation is not available to API keys")
    if principal.credential_kind == "web_session" and not principal.is_bootstrap:
        raise HTTPException(status_code=403, detail="operation requires the bootstrap administrator")


async def require_principal(connection: HTTPConnection) -> Principal:
    principal = getattr(connection.state, "principal", None)
    if principal is None:
        principal = await resolve_principal(connection)
        connection.state.principal = principal
    return principal


async def require_web_user(connection: HTTPConnection) -> Principal:
    principal = await require_principal(connection)
    if principal.credential_kind != "web_session":
        raise HTTPException(status_code=403, detail="browser session required")
    return principal


async def require_web_admin(connection: HTTPConnection) -> Principal:
    principal = await require_web_user(connection)
    if not principal.is_admin:
        raise HTTPException(status_code=403, detail="administrator required")
    return principal


async def require_bootstrap_web_user(connection: HTTPConnection) -> Principal:
    principal = await require_web_user(connection)
    if not principal.is_bootstrap:
        raise HTTPException(status_code=403, detail="bootstrap administrator required")
    return principal
