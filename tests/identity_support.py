"""Real-credential helpers for API tests (Argon2 hashing, cookies, API keys)."""

from __future__ import annotations

import os
import time

from fastapi.testclient import TestClient

from worker_harness.auth import PasswordService
from worker_harness.db import Database

ORIGIN = os.environ["WH_WEB_ORIGIN"]
PASSWORD = "correct horse battery staple"


async def bootstrap(db: Database, username: str = "admin") -> dict:
    password_hash = await PasswordService().hash(PASSWORD)
    return await db.bootstrap_admin(
        username=username, display_name=username, password_hash=password_hash, now=int(time.time()),
    )


def login(client: TestClient, username: str, password: str = PASSWORD) -> dict[str, str]:
    """Log a browser client in; return headers required for cookie mutations."""
    response = client.post(
        "/api/v1/auth/login", json={"username": username, "password": password}, headers={"Origin": ORIGIN},
    )
    assert response.status_code == 200, response.text
    return {"Origin": ORIGIN, "X-CSRF-Token": response.json()["csrf_token"]}


def create_user(client: TestClient, admin_headers: dict[str, str], username: str, *, is_admin: bool = False) -> dict:
    response = client.post(
        "/api/v1/admin/users",
        json={"username": username, "display_name": username.title(), "password": PASSWORD, "is_admin": is_admin},
        headers=admin_headers,
    )
    assert response.status_code == 201, response.text
    return response.json()


def issue_key(client: TestClient, headers: dict[str, str], label: str = "test") -> str:
    response = client.post("/api/v1/auth/keys", json={"label": label}, headers=headers)
    assert response.status_code == 201, response.text
    return response.json()["token"]


def bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}
