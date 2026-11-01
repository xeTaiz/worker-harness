import asyncio
import itertools
import shlex
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from identity_support import ORIGIN, bearer, bootstrap, create_user, issue_key, login
from worker_harness.db import Database
from worker_harness.heartbeat import create_app
from worker_harness.marimo import allocate_local_port, build_launch_command, validate_absolute_path
from worker_harness.models import GPUInfo, JobStatus, WorkerRegistration
from worker_harness.ssh import JobProbe, SSHResult


class FakeProcess:
    pid = 4242
    returncode = None

    def poll(self):
        return self.returncode


class MarimoHelpersTests(unittest.TestCase):
    def test_launch_command_is_quoted_and_native_token_helper_receives_values(self):
        command = build_launch_command(
            notebook_path="/code/project with spaces/demo.py",
            environment="/code/project with spaces/.venv",
            port=12345,
            token_file="/harness/private tokens/native.token",
            base_url="/api/v1/marimo/session-test/proxy",
        )
        self.assertEqual(shlex.split(command), [
            "/usr/local/lib/worker-harness/wh-marimo-launch",
            "--notebook", "/code/project with spaces/demo.py",
            "--environment", "/code/project with spaces/.venv",
            "--port", "12345",
            "--token-file", "/harness/private tokens/native.token",
            "--base-url", "/api/v1/marimo/session-test/proxy",
        ])

    def test_paths_must_be_normalized_absolute_paths(self):
        for value in ("relative.py", "/code/../secret", "/code/demo\n.py"):
            with self.assertRaises(ValueError):
                validate_absolute_path(value, "path")

    def test_local_port_is_allocated_from_acl_scoped_range(self):
        with patch.dict("os.environ", {"WH_MARIMO_PORT_MIN": "28000", "WH_MARIMO_PORT_MAX": "28010"}):
            port = allocate_local_port("127.0.0.1")
        self.assertGreaterEqual(port, 28000)
        self.assertLessEqual(port, 28010)


class MarimoApiTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.tmp.name) / "harness.sqlite")
        asyncio.run(self.db.connect())
        self.admin = asyncio.run(bootstrap(self.db))
        asyncio.run(self.db.upsert_worker(WorkerRegistration(
            worker_id="w-test",
            name="worker-test",
            worker_ip="100.64.0.9",
            dns_name="worker-test.tailnet",
            ssh_user="testuser",
            harness_dir="/var/lib/worker-harness/harness",
            gpu_count=1,
            gpus=[GPUInfo(index=0, name="GPU0", vram_total_gb=24, vram_used_gb=0)],
        )))
        self.app = create_app(self.db)
        self.admin_client = TestClient(self.app)
        self.admin_headers = login(self.admin_client, "admin")
        self.alice = create_user(self.admin_client, self.admin_headers, "alice")
        self.bob = create_user(self.admin_client, self.admin_headers, "bob")
        self.browser = TestClient(self.app)
        self.browser_headers = login(self.browser, "alice")
        alice_token = issue_key(self.browser, self.browser_headers)
        self.alice_key = self.browser.get("/api/v1/auth/keys").json()[0]
        self.bob_browser = TestClient(self.app)
        bob_headers = login(self.bob_browser, "bob")
        bob_token = issue_key(self.bob_browser, bob_headers)
        self.client = TestClient(self.app, headers=bearer(alice_token))
        self.bob_client = TestClient(self.app, headers=bearer(bob_token))
        self.patches = ExitStack()
        self.patches.enter_context(patch.dict("os.environ", {"WH_TAILNET_BIND_HOST": "100.64.0.4"}))
        self.worker_port = self.patches.enter_context(patch(
            "worker_harness.heartbeat.allocate_worker_port", new=AsyncMock(return_value=2718),
        ))
        self.local_port = self.patches.enter_context(patch(
            "worker_harness.heartbeat.allocate_local_port", side_effect=lambda host, **_kwargs: next(self.ports),
        ))
        self.ports = itertools.count(18123)
        self.launch = self.patches.enter_context(patch(
            "worker_harness.job.ssh_job_launch", new=AsyncMock(return_value=SSHResult(stdout="started", stderr="", returncode=0)),
        ))
        self.probe = self.patches.enter_context(patch(
            "worker_harness.job.ssh_job_probe", new=AsyncMock(return_value=JobProbe("running")),
        ))
        self.stop = self.patches.enter_context(patch(
            "worker_harness.job.ssh_job_stop",
            new=AsyncMock(return_value=JobProbe("finished", exit_code=-15, termination_reason="user_cancelled")),
        ))
        self.forward = self.patches.enter_context(patch(
            "worker_harness.heartbeat.ssh_port_forward", new=AsyncMock(side_effect=lambda *args, **kwargs: FakeProcess()),
        ))
        self.ready = self.patches.enter_context(patch("worker_harness.heartbeat.wait_until_ready", new=AsyncMock()))
        self.upload = self.patches.enter_context(patch(
            "worker_harness.heartbeat.upload_remote_token", new=AsyncMock(return_value=SSHResult(stdout="", stderr="", returncode=0)),
        ))
        self.delete_remote = self.patches.enter_context(patch(
            "worker_harness.heartbeat.delete_remote_token", new=AsyncMock(return_value=SSHResult(stdout="", stderr="", returncode=0)),
        ))
        self.tunnel_stop = self.patches.enter_context(patch("worker_harness.heartbeat.TunnelRegistry.stop", return_value=True))

    def tearDown(self):
        for client in (self.client, self.bob_client, self.browser, self.bob_browser, self.admin_client):
            client.close()
        self.patches.close()
        asyncio.run(self.db.close())
        self.tmp.cleanup()

    def create_session(self, client=None, *, headers=None):
        response = (client or self.client).post("/api/v1/marimo", json={
            "worker_id": "worker-test",
            "notebook_path": "/code/project/demo.py",
            "environment": "/code/project/.venv",
        }, headers=headers)
        self.assertEqual(response.status_code, 201, response.text)
        return response.json()

    def resources(self, body):
        session = asyncio.run(self.db.get_marimo_session(body["id"]))
        job = asyncio.run(self.db.get_job(session.job_id))
        forward = asyncio.run(self.db.get_port_forward(session.tunnel_id))
        return session, job, forward

    def test_create_and_delete_marimo_session(self):
        body = self.create_session()
        session, job, forward = self.resources(body)
        self.assertEqual(set(body), {
            "id", "worker_id", "worker_name", "notebook_path", "environment", "job_id",
            "status", "url", "owner_user_id", "created_at",
        })
        self.assertEqual(body["url"], f"{ORIGIN}/api/v1/marimo/{body['id']}/proxy/")
        self.assertEqual(body["status"], "ready")
        self.assertEqual(body["owner_user_id"], self.alice["id"])
        actor_id = f"api_key:{self.alice_key['id']}"
        for resource in (session, forward):
            self.assertEqual(resource.owner_user_id, self.alice["id"])
            self.assertEqual(resource.created_by_actor_id, actor_id)
            self.assertEqual(resource.bind_host, "127.0.0.1")
        self.assertEqual(job.owner_user_id, self.alice["id"])
        self.assertEqual(job.submitted_by_actor_id, actor_id)
        self.assertEqual(job.gpu_count, 0)
        self.assertEqual(job.gpu_indices, [])
        self.assertFalse(job.queue_managed)
        self.assertFalse(job.pty_enabled)
        self.launch.assert_awaited_once()
        self.assertEqual(self.launch.await_args.args[1], job.id)
        self.assertEqual(self.launch.await_args.kwargs["gpu_indices"], [])
        self.assertIn("wh-marimo-launch", self.launch.await_args.args[2])
        self.assertIn(f"--base-url /api/v1/marimo/{session.id}/proxy", job.command)
        self.assertIn(f"/.marimo/{session.id}/native.token", job.command)
        self.assertEqual(self.local_port.call_args.args, ("127.0.0.1",))
        self.assertEqual(self.forward.await_args.kwargs["bind_host"], "127.0.0.1")
        self.assertEqual(self.app.state.tunnels.get(forward.id).owner_user_id, self.alice["id"])
        token_path = Path(session.upstream_token_path)
        token = token_path.read_text()
        self.assertTrue(token)
        self.assertEqual(token_path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(token_path.parent.stat().st_mode & 0o777, 0o700)
        self.assertNotIn(token, str(body))
        self.assertNotIn(token, job.command)
        self.assertEqual(self.upload.await_args.args[1:], (session.id, token))
        self.assertEqual(self.ready.await_args.args[:2], ("127.0.0.1", forward.local_port))
        self.assertEqual(self.ready.await_args.kwargs, {"base_url": f"/api/v1/marimo/{session.id}/proxy", "token": token})
        self.assertEqual(self.client.get("/api/v1/marimo").json(), [body])
        self.assertEqual(self.client.get(f"/api/v1/marimo/{session.id}").json(), body)

        deleted = self.client.delete(f"/api/v1/marimo/{session.id}")
        self.assertEqual(deleted.status_code, 200, deleted.text)
        self.assertEqual(deleted.json(), {"session_id": session.id, "removed": True})
        self.assertEqual(self.stop.await_args.args[1:], (job.id, "user_cancelled"))
        self.assertEqual(self.tunnel_stop.call_args.args[0].id, forward.id)
        self.assertFalse(token_path.exists())
        self.assertEqual(self.delete_remote.await_args.args[1], session.id)
        self.assertEqual(self.client.get("/api/v1/marimo").json(), [])
        self.assertIsNone(asyncio.run(self.db.get_port_forward(forward.id)))
        self.assertIsNone(self.app.state.tunnels.get(forward.id))

    def test_lists_and_foreign_ids_are_private_even_from_admin(self):
        alice_body = self.create_session()
        bob_body = self.create_session(self.bob_client)
        self.assertEqual(self.client.get("/api/v1/marimo").json(), [alice_body])
        self.assertEqual(self.client.get("/api/v1/marimo?worker_id=w-test").json(), [alice_body])
        self.assertEqual(self.browser.get("/api/v1/marimo").json(), [alice_body])
        self.assertEqual(self.bob_client.get("/api/v1/marimo").json(), [bob_body])
        self.assertEqual(self.admin_client.get("/api/v1/marimo").json(), [])
        for client, body, headers in (
            (self.bob_client, alice_body, None),
            (self.client, bob_body, None),
            (self.admin_client, alice_body, self.admin_headers),
            (self.admin_client, bob_body, self.admin_headers),
        ):
            with self.subTest(user=client, session=body["id"]):
                session, job, forward = self.resources(body)
                self.assertEqual(client.get(f"/api/v1/marimo/{session.id}").status_code, 404)
                self.assertEqual(client.delete(f"/api/v1/marimo/{session.id}", headers=headers).status_code, 404)
                self.assertEqual(client.delete(f"/api/v1/jobs/{job.id}", headers=headers).status_code, 404)
                self.assertEqual(client.delete(f"/api/v1/tunnels/{forward.id}", headers=headers).status_code, 404)
                self.assertIsNotNone(asyncio.run(self.db.get_marimo_session(session.id)))
                self.assertIsNotNone(self.app.state.tunnels.get(forward.id))
                self.assertTrue(Path(session.upstream_token_path).exists())
                self.assertEqual(asyncio.run(self.db.get_job(job.id)).status, JobStatus.RUNNING)
        self.stop.assert_not_awaited()
        self.tunnel_stop.assert_not_called()
        self.delete_remote.assert_not_awaited()
        self.assertEqual(self.bob_client.get(f"/api/v1/marimo/{bob_body['id']}").json(), bob_body)
        response = self.bob_client.delete(f"/api/v1/marimo/{bob_body['id']}")
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(self.bob_client.get("/api/v1/marimo").json(), [])
        self.assertEqual(self.client.get("/api/v1/marimo").json(), [alice_body])
        self.assertIsNotNone(asyncio.run(self.db.get_marimo_session(alice_body["id"])))

    def test_browser_owner_can_create_get_and_delete(self):
        body = self.create_session(self.browser, headers=self.browser_headers)
        session, job, forward = self.resources(body)
        self.assertTrue(session.created_by_actor_id.startswith("web_session:"))
        self.assertEqual(job.submitted_by_actor_id, session.created_by_actor_id)
        self.assertEqual(forward.created_by_actor_id, session.created_by_actor_id)
        self.assertEqual(self.browser.get(f"/api/v1/marimo/{session.id}").status_code, 200)
        response = self.browser.delete(f"/api/v1/marimo/{session.id}", headers=self.browser_headers)
        self.assertEqual(response.status_code, 200, response.text)
        self.stop.assert_awaited_once()

    def test_generic_delete_cannot_remove_managed_forward(self):
        body = self.create_session()
        session, job, forward = self.resources(body)
        self.assertEqual(self.client.get("/api/v1/tunnels").json(), [])
        response = self.client.delete(f"/api/v1/tunnels/{forward.id}")
        self.assertEqual(response.status_code, 409, response.text)
        self.assertEqual(response.json()["detail"]["code"], "MANAGED_BY_MARIMO")
        self.assertIsNotNone(asyncio.run(self.db.get_port_forward(forward.id)))
        self.assertIsNotNone(asyncio.run(self.db.get_marimo_session(session.id)))
        self.assertIsNotNone(self.app.state.tunnels.get(forward.id))
        self.stop.assert_not_awaited()
        self.tunnel_stop.assert_not_called()

    def test_owner_stopping_linked_cpu_job_releases_session(self):
        body = self.create_session()
        session, job, forward = self.resources(body)
        response = self.client.delete(f"/api/v1/jobs/{job.id}")
        self.assertEqual(response.status_code, 200, response.text)
        self.assertTrue(response.json()["stopped"])
        self.assertIsNone(asyncio.run(self.db.get_marimo_session(session.id)))
        self.assertIsNone(asyncio.run(self.db.get_port_forward(forward.id)))
        self.assertFalse(Path(session.upstream_token_path).exists())
        self.tunnel_stop.assert_called_once()
        self.delete_remote.assert_awaited_once()

    def test_delete_preserves_resources_until_supervisor_confirms_finished(self):
        for probe in (None, JobProbe("running"), JobProbe("starting"), JobProbe("lost"), JobProbe("absent")):
            with self.subTest(probe=probe):
                body = self.create_session()
                session, job, forward = self.resources(body)
                self.stop.return_value = probe
                response = self.client.delete(f"/api/v1/marimo/{session.id}")
                self.assertEqual(response.status_code, 503, response.text)
                self.assertEqual(response.json()["detail"]["code"], "STOP_UNCONFIRMED")
                self.assertIsNotNone(asyncio.run(self.db.get_marimo_session(session.id)))
                self.assertIsNotNone(asyncio.run(self.db.get_port_forward(forward.id)))
                self.assertIsNotNone(self.app.state.tunnels.get(forward.id))
                self.assertTrue(Path(session.upstream_token_path).exists())
                self.assertEqual(asyncio.run(self.db.get_job(job.id)).status, JobStatus.RUNNING)
        self.tunnel_stop.assert_not_called()
        self.delete_remote.assert_not_awaited()

    def test_startup_failure_rolls_back_only_new_resources(self):
        existing = self.create_session(self.bob_client)
        old_session, old_job, old_forward = self.resources(existing)
        self.ready.side_effect = TimeoutError("no health")
        response = self.client.post("/api/v1/marimo", json={
            "worker_id": "w-test", "notebook_path": "/code/failing.py", "environment": "/code/.venv",
        })
        self.assertEqual(response.status_code, 502, response.text)
        new_job_id = self.launch.await_args.args[1]
        self.assertNotEqual(new_job_id, old_job.id)
        self.assertEqual(self.stop.await_args.args[1], new_job_id)
        self.tunnel_stop.assert_called_once()
        self.assertNotEqual(self.tunnel_stop.call_args.args[0].id, old_forward.id)
        self.assertEqual([s.id for s in asyncio.run(self.db.list_marimo_sessions())], [old_session.id])
        self.assertEqual([f.id for f in asyncio.run(self.db.list_port_forwards())], [old_forward.id])
        self.assertEqual(asyncio.run(self.db.get_job(old_job.id)).status, JobStatus.RUNNING)
        self.assertIsNotNone(self.app.state.tunnels.get(old_forward.id))
        self.assertTrue(Path(old_session.upstream_token_path).exists())
        failed_session_id = self.upload.await_args.args[1]
        self.assertEqual(self.delete_remote.await_args.args[1], failed_session_id)
        self.assertFalse((Path(self.tmp.name) / "marimo-secrets" / failed_session_id / "native.token").exists())

    def test_listener_collision_returns_409_without_cleaning_existing_session(self):
        existing = self.create_session(self.bob_client)
        session, job, forward = self.resources(existing)
        self.local_port.side_effect = None
        self.local_port.return_value = forward.local_port
        response = self.client.post("/api/v1/marimo", json={
            "worker_id": "w-test", "notebook_path": "/code/another.py", "environment": "/code/.venv",
        })
        self.assertEqual(response.status_code, 409, response.text)
        self.forward.assert_awaited_once()
        self.stop.assert_awaited_once()
        self.assertNotEqual(self.stop.await_args.args[1], job.id)
        self.tunnel_stop.assert_not_called()
        self.assertEqual([f.id for f in asyncio.run(self.db.list_port_forwards())], [forward.id])
        self.assertEqual([s.id for s in asyncio.run(self.db.list_marimo_sessions())], [session.id])
        self.assertEqual(asyncio.run(self.db.get_job(job.id)).status, JobStatus.RUNNING)
        self.assertIsNotNone(self.app.state.tunnels.get(forward.id))
        self.assertTrue(Path(session.upstream_token_path).exists())


if __name__ == "__main__":
    unittest.main()
