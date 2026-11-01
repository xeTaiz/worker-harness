"""Human identity, browser sessions, API keys and the capability boundary."""

from __future__ import annotations

import asyncio
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from worker_harness.db import Database, IdentityConflict
from worker_harness.heartbeat import create_app
from worker_harness.models import GPUInfo, Job, JobStatus, PiSession, PiSessionState, PiSessionType, WorkerRegistration
from worker_harness.ssh import SSHResult

from identity_support import ORIGIN, PASSWORD, bearer, bootstrap, create_user, issue_key, login


def registration(worker_id: str = "w1", gpu_count: int = 1) -> WorkerRegistration:
    return WorkerRegistration(
        worker_id=worker_id, name=f"worker-{worker_id}", worker_ip="100.64.0.2", gpu_count=gpu_count,
        gpus=[GPUInfo(index=i, name="GPU", vram_total_gb=24, vram_used_gb=0, busy=False) for i in range(gpu_count)],
    )


class IdentityTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "db.sqlite"
        self.loop = asyncio.new_event_loop()
        self.db = Database(self.path)
        self.run_async(self.db.connect())
        self.app = create_app(self.db)

    def tearDown(self) -> None:
        self.run_async(self.db.close())
        self.loop.close()
        self.tmp.cleanup()

    def run_async(self, coroutine):
        return self.loop.run_until_complete(coroutine)

    def client(self) -> TestClient:
        return TestClient(self.app)

    def bootstrap_admin(self) -> tuple[TestClient, dict[str, str]]:
        self.admin = self.run_async(bootstrap(self.db))
        admin = self.client()
        return admin, login(admin, "admin")


class UnauthenticatedTests(IdentityTestCase):
    def test_empty_database_denies_everything_but_login_health_and_static(self) -> None:
        client = self.client()
        self.assertEqual(client.get("/api/v1/workers").status_code, 401)
        self.assertEqual(client.get("/api/v1/jobs/queue").status_code, 401)
        self.assertEqual(client.get("/api/v1/auth/me").status_code, 401)
        health = client.get("/health")
        self.assertEqual(health.status_code, 200)
        self.assertEqual(set(health.json()), {"status", "ts"})
        failed = client.post(
            "/api/v1/auth/login", json={"username": "admin", "password": PASSWORD}, headers={"Origin": ORIGIN},
        )
        self.assertEqual(failed.status_code, 401)

    def test_invalid_bearer_never_falls_back_to_cookie(self) -> None:
        admin, _headers = self.bootstrap_admin()
        self.assertEqual(admin.get("/api/v1/auth/me").status_code, 200)
        self.assertEqual(admin.get("/api/v1/workers", headers=bearer("whk_not-a-real-key")).status_code, 401)
        self.assertEqual(admin.get("/api/v1/workers", headers={"Authorization": "Basic abc"}).status_code, 401)

    def test_second_bootstrap_is_refused(self) -> None:
        self.run_async(bootstrap(self.db))
        with self.assertRaises(IdentityConflict):
            self.run_async(bootstrap(self.db, "other"))


class BrowserSessionTests(IdentityTestCase):
    def test_login_requires_origin_and_is_generic_and_rate_limited(self) -> None:
        self.bootstrap_admin()
        client = self.client()
        no_origin = client.post("/api/v1/auth/login", json={"username": "admin", "password": PASSWORD})
        self.assertEqual(no_origin.status_code, 403)
        unknown = client.post(
            "/api/v1/auth/login", json={"username": "nobody", "password": "x" * 12}, headers={"Origin": ORIGIN},
        )
        wrong = client.post(
            "/api/v1/auth/login", json={"username": "admin", "password": "y" * 12}, headers={"Origin": ORIGIN},
        )
        self.assertEqual((unknown.status_code, wrong.status_code), (401, 401))
        self.assertEqual(unknown.json(), wrong.json())
        for _ in range(4):
            client.post(
                "/api/v1/auth/login", json={"username": "admin", "password": "z" * 12}, headers={"Origin": ORIGIN},
            )
        limited = client.post(
            "/api/v1/auth/login", json={"username": "admin", "password": PASSWORD}, headers={"Origin": ORIGIN},
        )
        self.assertEqual(limited.status_code, 429)
        self.assertGreaterEqual(int(limited.headers["Retry-After"]), 1)

    def test_cookie_is_httponly_strict_and_auth_responses_are_no_store(self) -> None:
        self.bootstrap_admin()
        client = self.client()
        response = client.post(
            "/api/v1/auth/login", json={"username": "admin", "password": PASSWORD}, headers={"Origin": ORIGIN},
        )
        cookie = response.headers["set-cookie"].lower()
        self.assertIn("wh_session=", cookie)
        self.assertIn("httponly", cookie)
        self.assertIn("samesite=strict", cookie)
        self.assertEqual(response.headers["cache-control"], "no-store")
        me = client.get("/api/v1/auth/me").json()
        self.assertTrue(me["is_bootstrap"])
        self.assertNotIn("password_hash", me["user"])

    def test_cookie_mutations_require_origin_and_csrf(self) -> None:
        admin, headers = self.bootstrap_admin()
        body = {"label": "laptop"}
        self.assertEqual(admin.post("/api/v1/auth/keys", json=body).status_code, 403)
        self.assertEqual(
            admin.post("/api/v1/auth/keys", json=body, headers={"Origin": ORIGIN}).status_code, 403,
        )
        self.assertEqual(
            admin.post(
                "/api/v1/auth/keys", json=body,
                headers={"Origin": "https://evil.example", "X-CSRF-Token": headers["X-CSRF-Token"]},
            ).status_code,
            403,
        )
        created = admin.post("/api/v1/auth/keys", json=body, headers=headers)
        self.assertEqual(created.status_code, 201)
        self.assertTrue(created.json()["token"].startswith("whk_"))
        self.assertNotIn("token_hash", created.json()["key"])
        listed = admin.get("/api/v1/auth/keys").json()
        self.assertEqual([key["label"] for key in listed], ["laptop"])
        self.assertNotIn("token", listed[0])

    def test_expired_cookie_and_logout_are_rejected(self) -> None:
        admin, headers = self.bootstrap_admin()
        self.run_async(self.db._db.execute("UPDATE web_sessions SET expires_at=?", (int(time.time()) - 1,)))
        self.run_async(self.db._db.commit())
        self.assertEqual(admin.get("/api/v1/auth/me").status_code, 401)
        headers = login(admin, "admin")
        self.assertEqual(admin.post("/api/v1/auth/logout", headers=headers).status_code, 204)
        self.assertEqual(admin.get("/api/v1/auth/me").status_code, 401)

    def test_password_reset_and_change_revoke_browser_sessions(self) -> None:
        admin, admin_headers = self.bootstrap_admin()
        bob = create_user(admin, admin_headers, "bob")
        bob_client = self.client()
        login(bob_client, "bob")
        reset = admin.post(
            f"/api/v1/admin/users/{bob['id']}/password", json={"password": "another long password"},
            headers=admin_headers,
        )
        self.assertEqual(reset.status_code, 204)
        self.assertEqual(bob_client.get("/api/v1/auth/me").status_code, 401)
        bob_headers = login(bob_client, "bob", "another long password")
        changed = bob_client.post(
            "/api/v1/auth/password",
            json={"current_password": "another long password", "new_password": PASSWORD}, headers=bob_headers,
        )
        self.assertEqual(changed.status_code, 204)
        self.assertEqual(bob_client.get("/api/v1/auth/me").status_code, 401)

    def test_validation_errors_do_not_echo_secrets(self) -> None:
        admin, headers = self.bootstrap_admin()
        response = admin.post(
            "/api/v1/admin/users",
            json={"username": "carol", "display_name": "Carol", "password": "shortsecret", "is_admin": False},
            headers=headers,
        )
        self.assertEqual(response.status_code, 422)
        self.assertNotIn("shortsecret", response.text)


class CapabilityTests(IdentityTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.admin_client, self.admin_headers = self.bootstrap_admin()
        self.alice = create_user(self.admin_client, self.admin_headers, "alice")
        alice_client = self.client()
        self.alice_key = issue_key(alice_client, login(alice_client, "alice"))
        self.admin_key = issue_key(self.admin_client, self.admin_headers, "admin-cli")
        self.run_async(self.db.upsert_worker(registration()))

    def test_api_key_reaches_ordinary_surface_only(self) -> None:
        client = self.client()
        key = bearer(self.alice_key)
        self.assertEqual(client.get("/api/v1/workers", headers=key).status_code, 200)
        worker = client.get("/api/v1/workers/w1", headers=key).json()
        for private in ("worker_ip", "ssh_user", "harness_dir", "dns_name"):
            self.assertNotIn(private, worker)
        self.assertEqual(client.get("/api/v1/jobs/queue", headers=key).status_code, 200)
        self.assertEqual(client.get("/api/v1/usage", headers=key).status_code, 200)
        for method, path in (
            ("get", "/api/v1/auth/keys"),
            ("get", "/api/v1/admin/users"),
            ("get", "/api/v1/pi/sessions"),
            ("get", "/api/v1/_stats"),
            ("get", "/api/v1/events"),
            ("get", "/api/v1/workers/w1/files?path=/etc/hostname"),
            ("delete", "/api/v1/workers/prune"),
        ):
            with self.subTest(path=path):
                self.assertEqual(getattr(client, method)(path, headers=key).status_code, 403)

    def test_admin_owned_key_cannot_administer(self) -> None:
        client = self.client()
        key = bearer(self.admin_key)
        self.assertEqual(client.get("/api/v1/admin/users", headers=key).status_code, 403)
        self.assertEqual(client.post("/api/v1/auth/keys", json={"label": "x"}, headers=key).status_code, 403)
        self.assertEqual(client.get("/api/v1/pi/sessions", headers=key).status_code, 403)

    def test_ordinary_browser_user_cannot_use_pi_or_admin(self) -> None:
        alice = self.client()
        login(alice, "alice")
        self.assertEqual(alice.get("/api/v1/pi/sessions").status_code, 403)
        self.assertEqual(alice.get("/api/v1/admin/users").status_code, 403)
        self.assertFalse(alice.get("/api/v1/auth/me").json()["is_bootstrap"])
        self.assertEqual(self.admin_client.get("/api/v1/pi/sessions").status_code, 200)

    def test_revoked_key_and_disabled_user_lose_access_immediately(self) -> None:
        client = self.client()
        key_id = self.client().get("/api/v1/workers", headers=bearer(self.alice_key))
        self.assertEqual(key_id.status_code, 200)
        keys = self.admin_client.get(f"/api/v1/admin/users/{self.alice['id']}/keys").json()
        revoked = self.admin_client.delete(
            f"/api/v1/admin/users/{self.alice['id']}/keys/{keys[0]['id']}", headers=self.admin_headers,
        )
        self.assertEqual(revoked.status_code, 204)
        self.assertEqual(client.get("/api/v1/workers", headers=bearer(self.alice_key)).status_code, 401)

        second_key = self.admin_client.post(
            f"/api/v1/admin/users/{self.alice['id']}/keys", json={"label": "ci"}, headers=self.admin_headers,
        ).json()["token"]
        alice_browser = self.client()
        login(alice_browser, "alice")
        disabled = self.admin_client.patch(
            f"/api/v1/admin/users/{self.alice['id']}", json={"disabled": True}, headers=self.admin_headers,
        )
        self.assertEqual(disabled.status_code, 200)
        self.assertEqual(client.get("/api/v1/workers", headers=bearer(second_key)).status_code, 401)
        self.assertEqual(alice_browser.get("/api/v1/auth/me").status_code, 401)
        self.assertEqual(
            self.admin_client.post(
                f"/api/v1/admin/users/{self.alice['id']}/keys", json={"label": "x"}, headers=self.admin_headers,
            ).status_code,
            409,
        )

    def test_disable_cancels_pending_jobs_but_not_running_jobs(self) -> None:
        launch = AsyncMock(return_value=SSHResult(stdout="started", stderr="", returncode=0))
        key = bearer(self.alice_key)
        client = self.client()
        with patch("worker_harness.job.ssh_job_launch", new=launch):
            running = client.post("/api/v1/jobs/queue", headers=key, json={
                "worker_id": "w1", "command": "train", "name": "train", "expected_seconds": 600, "gpu_count": 1,
            }).json()
            pending = client.post("/api/v1/jobs/queue", headers=key, json={
                "worker_id": "w1", "command": "eval", "name": "eval", "expected_seconds": 60, "gpu_count": 1,
            }).json()
        self.assertEqual((running["status"], pending["status"]), ("running", "pending"))
        self.admin_client.patch(
            f"/api/v1/admin/users/{self.alice['id']}", json={"disabled": True}, headers=self.admin_headers,
        )
        still_running = self.run_async(self.db.get_job(running["id"]))
        cancelled = self.run_async(self.db.get_job(pending["id"]))
        self.assertEqual(still_running.status, JobStatus.RUNNING)
        self.assertEqual(cancelled.status, JobStatus.FAILED)
        self.assertEqual(cancelled.termination_reason.value, "user_disabled")

    def test_bootstrap_and_last_admin_protections(self) -> None:
        bootstrap_id = self.admin["id"]
        for change in ({"disabled": True}, {"is_admin": False}):
            response = self.admin_client.patch(
                f"/api/v1/admin/users/{bootstrap_id}", json=change, headers=self.admin_headers,
            )
            self.assertEqual(response.status_code, 409)
        duplicate = self.admin_client.post(
            "/api/v1/admin/users",
            json={"username": "ALICE", "display_name": "Again", "password": PASSWORD, "is_admin": False},
            headers=self.admin_headers,
        )
        self.assertEqual(duplicate.status_code, 409)
        extra = self.admin_client.patch(
            f"/api/v1/admin/users/{self.alice['id']}", json={"username": "x"}, headers=self.admin_headers,
        )
        self.assertEqual(extra.status_code, 422)

    def test_users_manage_only_their_own_keys(self) -> None:
        alice = self.client()
        login(alice, "alice")
        admin_keys = self.admin_client.get("/api/v1/auth/keys").json()
        response = alice.delete(
            f"/api/v1/auth/keys/{admin_keys[0]['id']}",
            headers={"Origin": ORIGIN, "X-CSRF-Token": alice.get("/api/v1/auth/me").json()["csrf_token"]},
        )
        self.assertEqual(response.status_code, 404)


class GatewayAndPiTests(IdentityTestCase):
    def test_attach_gateway_requires_bootstrap_cookie_with_origin(self) -> None:
        admin, _headers = self.bootstrap_admin()
        key = issue_key(admin, _headers)
        anonymous = self.client()
        for headers in ({}, bearer(key), {"Authorization": "Bearer legacy-operator-token-" + "x" * 30}):
            with self.subTest(headers=list(headers)):
                with anonymous.websocket_connect(
                    "/api/v1/pi/sessions/missing/attach-gateway", headers=headers,
                ) as websocket:
                    with self.assertRaises(WebSocketDisconnect) as closed:
                        websocket.receive_json()
                    self.assertIn(closed.exception.code, {4401, 4403})
        with admin.websocket_connect(
            "/api/v1/pi/sessions/missing/attach-gateway", headers={"Origin": "https://evil.example"},
        ) as websocket:
            with self.assertRaises(WebSocketDisconnect) as closed:
                websocket.receive_json()
            self.assertEqual(closed.exception.code, 4403)
        with admin.websocket_connect(
            "/api/v1/pi/sessions/missing/attach-gateway", headers={"Origin": ORIGIN},
        ) as websocket:
            # Authorized: the request reaches session resolution.
            self.assertEqual(websocket.receive_json()["code"], "session_not_found")

    def test_internal_pi_token_uses_its_persisted_owner(self) -> None:
        import hashlib

        admin_record = self.run_async(bootstrap(self.db))
        self.run_async(self.db.upsert_worker(registration()))
        token = "internal-task-token-" + "a" * 40
        session = PiSession(
            role="task", session_type=PiSessionType.INTERACTIVE, state=PiSessionState.WORKING,
            token_hash=hashlib.sha256(token.encode()).hexdigest(), owner_user_id=admin_record["id"],
        )
        self.run_async(self.db.insert_pi_session(session))
        launch = AsyncMock(return_value=SSHResult(stdout="started", stderr="", returncode=0))
        with patch("worker_harness.job.ssh_job_launch", new=launch):
            created = self.client().post("/api/v1/jobs", headers=bearer(token), json={
                "worker_id": "w1", "command": "pip install x", "name": "setup", "expected_seconds": 30,
            })
        self.assertEqual(created.status_code, 200, created.text)
        job = created.json()
        self.assertEqual(job["owner_user_id"], admin_record["id"])
        self.assertEqual(job["origin_session_id"], session.id)
        self.assertEqual(job["submitted_by_actor_id"], f"pi_session:{session.id}")
        self.assertEqual(self.client().get("/api/v1/auth/keys", headers=bearer(token)).status_code, 403)


class LoginHardeningTests(unittest.IsolatedAsyncioTestCase):
    def test_in_flight_attempts_count_against_the_limit(self) -> None:
        from worker_harness.auth import LoginRateLimiter

        limiter = LoginRateLimiter(limit=5, window_seconds=60)
        keys = ["user:alice", "client:10.0.0.1"]
        self.assertEqual([limiter.reserve(keys, now=0) for _ in range(5)], [0] * 5)
        self.assertGreater(limiter.reserve(keys, now=0), 0)  # six concurrent guesses
        limiter.finish(keys, failed=False, now=1)  # a success frees its slot
        self.assertEqual(limiter.reserve(keys, now=1), 0)
        for _ in range(5):
            limiter.finish(keys, failed=True, now=2)
        self.assertGreater(limiter.reserve(["user:alice"], now=30), 0)
        self.assertEqual(limiter.reserve(["user:alice"], now=63), 0)

    def test_client_address_uses_forwarded_hop_only_behind_trusted_proxies(self) -> None:
        from ipaddress import ip_network
        from types import SimpleNamespace

        from worker_harness.auth import ProxyTrust, client_address

        def connection(peer: str, forwarded: str):
            return SimpleNamespace(client=SimpleNamespace(host=peer), headers={"x-forwarded-for": forwarded})

        trusted = ProxyTrust(networks=(ip_network("172.20.0.0/16"),))
        spoofed = "1.2.3.4, 198.51.100.7"
        self.assertEqual(client_address(connection("172.20.0.5", spoofed), trusted), "198.51.100.7")
        self.assertEqual(client_address(connection("203.0.113.9", spoofed), trusted), "203.0.113.9")
        self.assertEqual(client_address(connection("172.20.0.5", spoofed), ProxyTrust()), "172.20.0.5")

    def test_hop_count_takes_the_first_proxys_appended_address(self) -> None:
        from types import SimpleNamespace

        from worker_harness.auth import ProxyTrust, client_address

        def connection(forwarded: str):
            return SimpleNamespace(client=SimpleNamespace(host="172.31.250.11"), headers={"x-forwarded-for": forwarded})

        # Browser -> SWAG (appends the browser) -> web edge (appends SWAG) -> API.
        two = ProxyTrust(hops=2)
        self.assertEqual(client_address(connection("198.51.100.7, 172.20.0.10"), two), "198.51.100.7")
        # Client-supplied entries stay left of the address SWAG appended.
        self.assertEqual(client_address(connection("9.9.9.9, 1.2.3.4, 198.51.100.7, 172.20.0.10"), two), "198.51.100.7")
        # Fewer hops than configured means the chain is not the expected one.
        self.assertEqual(client_address(connection("172.20.0.10"), two), "172.31.250.11")
        self.assertEqual(client_address(connection("not-an-ip, 172.20.0.10"), two), "172.31.250.11")

    def test_proxy_trust_configuration_is_validated(self) -> None:
        from worker_harness.auth import load_proxy_trust

        for env in (
            {"WH_TRUSTED_PROXY_HOPS": "0"},
            {"WH_TRUSTED_PROXY_HOPS": "two"},
            {"WH_TRUSTED_PROXIES": "PROXY_SUBNET"},
            {"WH_TRUSTED_PROXIES": "10.0.0.0/8", "WH_TRUSTED_PROXY_HOPS": "2"},
        ):
            with self.subTest(env=env), patch.dict("os.environ", env):
                with self.assertRaises(RuntimeError):
                    load_proxy_trust()
        with patch.dict("os.environ", {"WH_TRUSTED_PROXY_HOPS": "2"}):
            self.assertEqual(load_proxy_trust().hops, 2)

    async def test_login_cannot_issue_a_session_after_a_concurrent_reset(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = Database(Path(tmp) / "db.sqlite")
            await db.connect()
            try:
                admin = await bootstrap(db)
                old_hash = (await db.get_user(admin["id"]))["password_hash"]
                # The login verified `old_hash`; a reset commits before it issues.
                await db.set_password_hash(admin["id"], "replacement-hash", now=int(time.time()))
                issued = await db.create_web_session(
                    user_id=admin["id"], token_hash="h", csrf_token="c", now=int(time.time()),
                    expires_at=int(time.time()) + 60, password_hash=old_hash,
                )
                self.assertIsNone(issued)
                self.assertFalse(await db.rehash_password(admin["id"], "rehash", expected_hash=old_hash))
                self.assertEqual((await db.get_user(admin["id"]))["password_hash"], "replacement-hash")
            finally:
                await db.close()


class LegacyMigrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_bootstrap_backfills_owners_without_reordering(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = Database(Path(tmp) / "db.sqlite")
            await db.connect()
            try:
                await db.upsert_worker(registration())
                for index, job_id in enumerate(("first", "second"), 1):
                    await db._db.execute(
                        """INSERT INTO jobs (id, worker_id, name, command, status, queue_managed, gpu_count,
                           queue_order, expected_seconds) VALUES (?, 'w1', ?, 'x', 'pending', 1, 1, ?, 60)""",
                        (job_id, job_id, index),
                    )
                await db._db.commit()
                await db.close()
                await db.connect()  # legacy rows gain submission metadata on migration
                admin = await bootstrap(db)
                jobs = await db.list_queued_jobs("w1")
                self.assertEqual([job.id for job in jobs], ["first", "second"])
                self.assertTrue(all(job.owner_user_id == admin["id"] for job in jobs))
                self.assertTrue(all(job.submitted_by_actor_id == f"migration:{admin['id']}" for job in jobs))
                self.assertEqual([job.submission_sequence for job in jobs], [1, 2])
                usage = await db.usage(now=int(time.time()))
                self.assertEqual(usage, [])
            finally:
                await db.close()


if __name__ == "__main__":
    unittest.main()
