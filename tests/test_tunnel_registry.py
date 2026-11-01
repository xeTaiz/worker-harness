import asyncio
import os
import subprocess
import tempfile
import time
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from identity_support import bearer, bootstrap, create_user, issue_key, login
from worker_harness.db import Database
from worker_harness.heartbeat import create_app
from worker_harness.models import WorkerRegistration
from worker_harness.tunnel_registry import TunnelProcess, TunnelRegistry


class FakeProcess:
    pid = 4242
    returncode = None

    def poll(self):
        return self.returncode


class TunnelRegistryTests(unittest.TestCase):
    def _entry(self, tunnel_id: str) -> TunnelProcess:
        proc = subprocess.Popen(["sleep", "30"], start_new_session=True)
        return TunnelProcess(
            id=tunnel_id,
            worker_id="worker-a",
            local_port=18000,
            remote_port=8000,
            proc=proc,
            created_at=int(time.time()),
            owner_user_id="alice",
        )

    def test_shutdown_kills_complete_tunnel_process_group(self):
        registry = TunnelRegistry()
        entry = self._entry("tunnel-1")
        registry.add(entry)
        self.assertEqual(registry.get(entry.id).owner_user_id, "alice")
        self.assertEqual(registry.stats()["live"], 1)
        self.assertEqual(registry.shutdown(grace_seconds=0.1), 1)
        self.assertIsNotNone(entry.proc.poll())
        self.assertEqual(registry.stats()["live"], 0)

    def test_stop_kills_descendant_after_tunnel_leader_exits(self):
        with tempfile.TemporaryDirectory() as tmp:
            child_file = Path(tmp) / "child.pid"
            proc = subprocess.Popen(
                ["sh", "-c", f"sleep 30 & echo $! > {child_file}; exit 0"],
                start_new_session=True,
            )
            proc.wait(timeout=1)
            child_pid = int(child_file.read_text().strip())
            entry = TunnelProcess(
                id="tunnel-orphan",
                worker_id="worker-a",
                local_port=18001,
                remote_port=8001,
                proc=proc,
                created_at=int(time.time()),
                owner_user_id="alice",
            )

            self.assertTrue(TunnelRegistry.stop(entry, grace_seconds=0.1))
            with self.assertRaises(ProcessLookupError):
                os.kill(child_pid, 0)

    def test_reap_dead_removes_stale_registry_entry(self):
        registry = TunnelRegistry()
        entry = self._entry("tunnel-2")
        registry.add(entry)
        os.killpg(entry.proc.pid, 15)
        entry.proc.wait(timeout=1)
        self.assertEqual(registry.reap_dead(), ["tunnel-2"])
        self.assertEqual(registry.stats()["live"], 0)


class OrphanForwardTests(unittest.TestCase):
    """Dead or restart-orphaned forwards must not reserve listener ports forever."""

    def setUp(self):
        from worker_harness.models import PortForward

        self.tmp = tempfile.TemporaryDirectory()
        self.loop = asyncio.new_event_loop()
        self.db = Database(Path(self.tmp.name) / "harness.sqlite")
        self.loop.run_until_complete(self.db.connect())
        owner = self.loop.run_until_complete(bootstrap(self.db))["id"]
        self.loop.run_until_complete(self.db.upsert_worker(WorkerRegistration(
            worker_id="w1", name="w1", worker_ip="100.64.0.2",
        )))
        self.generic = PortForward(worker_id="w1", local_port=15001, remote_port=80, owner_user_id=owner,
                                   bind_host="100.64.0.10")
        self.managed = PortForward(worker_id="w1", local_port=18000, remote_port=81, owner_user_id=owner,
                                   bind_host="127.0.0.1", marimo_session_id="notebook")
        for forward in (self.generic, self.managed):
            self.loop.run_until_complete(self.db.insert_port_forward(forward))
        self.app = create_app(self.db)

    def tearDown(self):
        self.loop.run_until_complete(self.db.close())
        self.loop.close()
        self.tmp.cleanup()

    def forward_ids(self) -> set[str]:
        return {pf.id for pf in self.loop.run_until_complete(self.db.list_port_forwards())}

    def test_startup_releases_generic_rows_but_keeps_notebook_rows(self):
        with TestClient(self.app):
            self.assertEqual(self.forward_ids(), {self.managed.id})
        from worker_harness.models import PortForward

        # The released listener can be reserved again.
        again = PortForward(worker_id="w1", local_port=15001, remote_port=82, owner_user_id="x",
                            bind_host="100.64.0.10")
        self.loop.run_until_complete(self.db.insert_port_forward(again))

    def test_reaper_releases_rows_of_dead_generic_tunnels(self):
        from worker_harness.reaper import _reap_once

        proc = subprocess.Popen(["true"], start_new_session=True)
        proc.wait(timeout=5)
        self.app.state.tunnels.add(TunnelProcess(
            id=self.generic.id, worker_id="w1", local_port=15001, remote_port=80, proc=proc,
            created_at=int(time.time()), owner_user_id=self.generic.owner_user_id,
        ))
        live = subprocess.Popen(["sleep", "30"], start_new_session=True)
        self.addCleanup(lambda: (live.kill(), live.wait()))
        self.app.state.tunnels.add(TunnelProcess(
            id=self.managed.id, worker_id="w1", local_port=18000, remote_port=81, proc=live,
            created_at=int(time.time()), owner_user_id=self.managed.owner_user_id,
        ))
        self.loop.run_until_complete(_reap_once(self.app))
        self.assertEqual(self.forward_ids(), {self.managed.id})

    def test_notebook_port_allocation_skips_reserved_listeners(self):
        from worker_harness.marimo import allocate_local_port

        with patch.dict(os.environ, {"WH_MARIMO_PORT_MIN": "18000", "WH_MARIMO_PORT_MAX": "18999"}):
            port = allocate_local_port("127.0.0.1", reserved={18000})
        self.assertNotEqual(port, 18000)


class TunnelApiCleanupTests(unittest.TestCase):
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
            gpu_count=0,
            gpus=[],
        )))
        self.app = create_app(self.db)
        self.admin_client = TestClient(self.app, raise_server_exceptions=False)
        self.admin_headers = login(self.admin_client, "admin")
        self.alice = create_user(self.admin_client, self.admin_headers, "alice")
        self.bob = create_user(self.admin_client, self.admin_headers, "bob")
        self.browser = TestClient(self.app, raise_server_exceptions=False)
        self.browser_headers = login(self.browser, "alice")
        alice_token = issue_key(self.browser, self.browser_headers)
        self.alice_key = self.browser.get("/api/v1/auth/keys").json()[0]
        self.bob_browser = TestClient(self.app, raise_server_exceptions=False)
        bob_headers = login(self.bob_browser, "bob")
        bob_token = issue_key(self.bob_browser, bob_headers)
        self.client = TestClient(self.app, headers=bearer(alice_token), raise_server_exceptions=False)
        self.bob_client = TestClient(self.app, headers=bearer(bob_token), raise_server_exceptions=False)
        self.patches = ExitStack()
        self.patches.enter_context(patch.dict("os.environ", {"WH_TAILNET_BIND_HOST": "100.64.0.4"}))
        self.forward = self.patches.enter_context(patch(
            "worker_harness.heartbeat.ssh_port_forward", new=AsyncMock(side_effect=lambda *args, **kwargs: FakeProcess()),
        ))
        self.real_tunnel_stop = TunnelRegistry.stop
        self.tunnel_stop = self.patches.enter_context(patch("worker_harness.heartbeat.TunnelRegistry.stop", return_value=True))

    def tearDown(self):
        for client in (self.client, self.bob_client, self.browser, self.bob_browser, self.admin_client):
            client.close()
        self.patches.close()
        asyncio.run(self.db.close())
        self.tmp.cleanup()

    def create_tunnel(self, client=None, *, local_port=18002, headers=None):
        response = (client or self.client).post("/api/v1/tunnels", json={
            "worker_id": "worker-test",
            "local_port": local_port,
            "remote_port": 8002,
            "name": "Private service",
        }, headers=headers)
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()

    def test_generic_forward_uses_configured_tailnet_bind_and_records_owner_actor(self):
        body = self.create_tunnel()
        self.assertEqual(set(body), {
            "id", "worker_id", "worker_name", "local_port", "remote_port", "service_name",
            "bind_host", "owner_user_id", "created_at",
        })
        self.assertEqual(body["worker_id"], "w-test")
        self.assertEqual(body["worker_name"], "worker-test")
        self.assertEqual(body["bind_host"], "100.64.0.4")
        self.assertEqual(body["owner_user_id"], self.alice["id"])
        self.forward.assert_awaited_once()
        self.assertEqual(self.forward.await_args.args[1:], (18002, 8002))
        self.assertEqual(self.forward.await_args.kwargs, {"bind_host": "100.64.0.4"})
        forward = asyncio.run(self.db.get_port_forward(body["id"]))
        self.assertEqual(forward.owner_user_id, self.alice["id"])
        self.assertEqual(forward.created_by_actor_id, f"api_key:{self.alice_key['id']}")
        self.assertIsNone(forward.marimo_session_id)
        self.assertEqual(self.app.state.tunnels.get(forward.id).owner_user_id, self.alice["id"])
        self.assertEqual(self.client.get("/api/v1/tunnels").json(), [body])
        response = self.client.delete(f"/api/v1/tunnels/{forward.id}")
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json(), {"tunnel_id": forward.id, "removed": True})
        self.tunnel_stop.assert_called_once()
        self.assertEqual(self.tunnel_stop.call_args.args[0].id, forward.id)
        self.assertIsNone(asyncio.run(self.db.get_port_forward(forward.id)))
        self.assertIsNone(self.app.state.tunnels.get(forward.id))
        self.assertEqual(self.client.get("/api/v1/tunnels").json(), [])

    def test_lists_are_owner_scoped_and_foreign_delete_never_cleans_up(self):
        alice_body = self.create_tunnel()
        bob_body = self.create_tunnel(self.bob_client, local_port=18003)
        self.assertEqual(self.client.get("/api/v1/tunnels").json(), [alice_body])
        self.assertEqual(self.browser.get("/api/v1/tunnels").json(), [alice_body])
        self.assertEqual(self.bob_client.get("/api/v1/tunnels").json(), [bob_body])
        self.assertEqual(self.admin_client.get("/api/v1/tunnels").json(), [])
        for client, body, headers in (
            (self.bob_client, alice_body, None),
            (self.client, bob_body, None),
            (self.admin_client, alice_body, self.admin_headers),
            (self.admin_client, bob_body, self.admin_headers),
        ):
            with self.subTest(tunnel=body["id"], client=client):
                response = client.delete(f"/api/v1/tunnels/{body['id']}", headers=headers)
                self.assertEqual(response.status_code, 404, response.text)
                self.assertIsNotNone(asyncio.run(self.db.get_port_forward(body["id"])))
                self.assertIsNotNone(self.app.state.tunnels.get(body["id"]))
        self.tunnel_stop.assert_not_called()
        self.assertEqual(self.client.get("/api/v1/tunnels").json(), [alice_body])
        self.assertEqual(self.bob_client.get("/api/v1/tunnels").json(), [bob_body])
        response = self.bob_client.delete(f"/api/v1/tunnels/{bob_body['id']}")
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(self.bob_client.get("/api/v1/tunnels").json(), [])
        self.assertEqual(self.client.get("/api/v1/tunnels").json(), [alice_body])

    def test_browser_owner_can_create_and_delete_forward(self):
        body = self.create_tunnel(self.browser, headers=self.browser_headers)
        forward = asyncio.run(self.db.get_port_forward(body["id"]))
        self.assertEqual(forward.owner_user_id, self.alice["id"])
        self.assertTrue(forward.created_by_actor_id.startswith("web_session:"))
        self.assertEqual(self.browser.get("/api/v1/tunnels").json(), [body])
        response = self.browser.delete(f"/api/v1/tunnels/{body['id']}", headers=self.browser_headers)
        self.assertEqual(response.status_code, 200, response.text)
        self.tunnel_stop.assert_called_once()
        self.assertEqual(self.client.get("/api/v1/tunnels").json(), [])

    def test_listener_collision_is_409_without_touching_existing_owner(self):
        existing = self.create_tunnel(self.bob_client)
        response = self.client.post("/api/v1/tunnels", json={
            "worker_id": "w-test", "local_port": existing["local_port"], "remote_port": 9000,
        })
        self.assertEqual(response.status_code, 409, response.text)
        self.forward.assert_awaited_once()
        self.tunnel_stop.assert_not_called()
        self.assertEqual([f.id for f in asyncio.run(self.db.list_port_forwards())], [existing["id"]])
        self.assertIsNotNone(self.app.state.tunnels.get(existing["id"]))
        self.assertEqual(self.client.get("/api/v1/tunnels").json(), [])
        self.assertEqual(self.bob_client.get("/api/v1/tunnels").json(), [existing])

    def test_failed_reservation_never_starts_ssh(self):
        with patch.object(self.db, "insert_port_forward", new=AsyncMock(side_effect=RuntimeError("database unavailable"))):
            response = self.client.post("/api/v1/tunnels", json={
                "worker_id": "worker-test", "local_port": 18002, "remote_port": 8002,
            })
        self.assertEqual(response.status_code, 500)
        self.forward.assert_not_awaited()
        self.tunnel_stop.assert_not_called()
        self.assertEqual(asyncio.run(self.db.list_port_forwards()), [])

    def test_failed_pid_insert_terminates_unregistered_process_and_preserves_existing(self):
        existing = self.create_tunnel(self.bob_client, local_port=18003)
        proc = subprocess.Popen(["sleep", "30"], start_new_session=True)
        try:
            self.forward.side_effect = None
            self.forward.return_value = proc
            with patch.object(
                self.db, "update_port_forward_pid", new=AsyncMock(side_effect=RuntimeError("database unavailable")),
            ), patch("worker_harness.heartbeat.TunnelRegistry.stop", new=self.real_tunnel_stop):
                response = self.client.post("/api/v1/tunnels", json={
                    "worker_id": "worker-test", "local_port": 18002, "remote_port": 8002,
                })
            self.assertEqual(response.status_code, 500)
            self.assertIsNotNone(proc.poll())
            self.assertEqual([f.id for f in asyncio.run(self.db.list_port_forwards())], [existing["id"]])
            self.assertIsNotNone(self.app.state.tunnels.get(existing["id"]))
            self.assertEqual(self.bob_client.get("/api/v1/tunnels").json(), [existing])
        finally:
            if proc.poll() is None:
                os.killpg(proc.pid, 9)
                proc.wait(timeout=1)

    def test_failed_forward_rolls_back_only_new_reservation(self):
        existing = self.create_tunnel(self.bob_client, local_port=18003)
        dead_proc = FakeProcess()
        dead_proc.returncode = 1
        self.forward.side_effect = None
        self.forward.return_value = dead_proc
        response = self.client.post("/api/v1/tunnels", json={
            "worker_id": "worker-test", "local_port": 18002, "remote_port": 8002,
        })
        self.assertEqual(response.status_code, 502, response.text)
        self.tunnel_stop.assert_called_once()
        self.assertIs(self.tunnel_stop.call_args.args[0].proc, dead_proc)
        self.assertNotEqual(self.tunnel_stop.call_args.args[0].id, existing["id"])
        self.assertEqual([f.id for f in asyncio.run(self.db.list_port_forwards())], [existing["id"]])
        self.assertIsNotNone(self.app.state.tunnels.get(existing["id"]))

    def test_generic_bind_must_be_in_tailnet_range(self):
        for host in ("127.0.0.1", "192.168.0.1", "0.0.0.0"):
            with self.subTest(host=host), patch.dict("os.environ", {"WH_TAILNET_BIND_HOST": host}):
                response = self.client.post("/api/v1/tunnels", json={
                    "worker_id": "worker-test", "local_port": 18002, "remote_port": 8002,
                })
                self.assertEqual(response.status_code, 503, response.text)
        self.forward.assert_not_awaited()
        self.assertEqual(asyncio.run(self.db.list_port_forwards()), [])
