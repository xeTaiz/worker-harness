"""Tests for the worker-local Pi relay and its Tailscale Serve lifecycle."""

from __future__ import annotations

import asyncio
import importlib.util
import os
import subprocess
import sqlite3
import sys
import tempfile
import textwrap
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from worker_harness.db import Database
from worker_harness.models import WorkerRegistration

WORKER_CONTAINER = Path(__file__).parents[1] / "worker_container"
RELAY_PATH = WORKER_CONTAINER / "pi_relay.py"
DAEMON_PATH = WORKER_CONTAINER / "worker_daemon.py"


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class _OfflineIngestClient:
    def __init__(self, **_kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    async def post(self, *_args, **_kwargs):
        raise RuntimeError("offline")


class _CapturingIngestClient:
    payloads: list[dict] = []
    init_kwargs: list[dict] = []

    def __init__(self, **kwargs):
        self.init_kwargs.append(kwargs)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    async def post(self, _url, *, json):
        self.payloads.append(json)
        return SimpleNamespace(raise_for_status=lambda: None)


class PiRelayTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.relay = load_module("pi_relay_for_test", RELAY_PATH)

    def test_health_session_lifecycle_and_real_tmux_websocket_attach(self):
        with tempfile.TemporaryDirectory() as tmp:
            app = self.relay.create_relay_app(
                sessions_root=Path(tmp),
                pi_command="/bin/sh",
                default_cwd=Path(tmp),
            )
            with TestClient(app) as client:
                health = client.get("/healthz")
                self.assertEqual(health.status_code, 200)
                self.assertEqual(
                    health.json(),
                    {"status": "healthy", "protocol_version": 2, "session_count": 0},
                )

                created = client.post(
                    "/v1/sessions",
                    json={"session_id": "delegate-1", "parent_session_id": "parent-1"},
                )
                self.assertEqual(created.status_code, 201)
                self.assertEqual(created.json()["state"], "working")
                self.assertEqual(client.get("/v1/sessions").json()[0]["session_id"], "delegate-1")

                with client.websocket_connect("/v1/sessions/delegate-1/attach") as websocket:
                    self.assertEqual(
                        websocket.receive_json(),
                        {
                            "type": "status",
                            "session_id": "delegate-1",
                            "state": "working",
                            "terminal": "ready",
                            "protocol_version": 2,
                        },
                    )
                    websocket.send_bytes(b"printf 'relay-pty-ok\\n'\\n")
                    output = b""
                    for _ in range(20):
                        output += websocket.receive_bytes()
                        if b"relay-pty-ok" in output:
                            break
                    self.assertIn(b"relay-pty-ok", output)

                cancelled = client.post("/v1/sessions/delegate-1:cancel")
                self.assertEqual(cancelled.status_code, 200)
                self.assertEqual(cancelled.json()["state"], "stopped")

                with client.websocket_connect("/v1/sessions/missing/attach") as websocket:
                    self.assertEqual(
                        websocket.receive_json(),
                        {"type": "error", "code": "session_not_found", "session_id": "missing"},
                    )
                    with self.assertRaises(WebSocketDisconnect) as closed:
                        websocket.receive_text()
                    self.assertEqual(closed.exception.code, 4404)

    def test_relay_injects_only_trusted_child_job_identity(self):
        async def run() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                marker = Path(tmp) / "child-env"
                command = f"/bin/sh -c 'printf \"%s|%s\" \"$WH_PI_SESSION_ID\" \"$WH_PI_JOB_URL\" > {marker}; exec sleep 60'"
                state = self.relay.RelayState(
                    root=Path(tmp) / "sessions",
                    command=command,
                    default_cwd=Path(tmp),
                    tmux_tmpdir=Path(tmp) / "pi-tmux",
                    job_url="http://127.0.0.1:27889",
                )
                await state.create(self.relay.SessionCreate(session_id="child"))
                for _ in range(20):
                    if marker.exists():
                        break
                    await asyncio.sleep(0.05)
                self.assertEqual(marker.read_text(encoding="utf-8"), "child|http://127.0.0.1:27889")
                await state.cancel("child")

        asyncio.run(run())

    def test_relay_server_rejects_invalid_port(self):
        with self.assertRaisesRegex(ValueError, "1..65535"):
            self.relay.RelayServer(0)

    def test_state_refresh_uploads_terminated_state_to_orchestrator(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmux_mock = AsyncMock(return_value=SimpleNamespace(returncode=1, stdout="", stderr="not running"))
            upload_mock = AsyncMock()
            with patch.object(self.relay.RelayState, "_tmux", new=tmux_mock), \
                 patch.object(self.relay.RelayState, "_upload_state", new=upload_mock):
                state = self.relay.RelayState(
                    root=Path(tmp),
                    command="/bin/sh",
                    default_cwd=Path(tmp),
                    orchestrator_url="http://orchestrator:12888",
                    worker_id="wkr",
                )
                asyncio.run(state.create(self.relay.SessionCreate(session_id="child")))
                upload_mock.assert_awaited()
                args = upload_mock.await_args
                record, event_type = args.args
                self.assertEqual(event_type, "create-failed")
                self.assertEqual(record.state, "failed")
                self.assertTrue(record.detail)

    def test_background_observer_reports_unattended_tmux_exit(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = self.relay.RelayState(root=Path(tmp), command="/bin/sh", default_cwd=Path(tmp))
            record = self.relay.SessionRecord(
                session_id="child",
                cwd=tmp,
                tmux_session="wh_pi_child",
                state="working",
                created_at=10,
                updated_at=10,
            )
            state.sessions[record.session_id] = record
            state._persist(record)
            tmux_mock = AsyncMock(return_value=SimpleNamespace(returncode=1, stdout="", stderr="gone"))
            upload_mock = AsyncMock()
            with patch.object(state, "_tmux", new=tmux_mock), patch.object(state, "_upload_state", new=upload_mock):
                asyncio.run(state._observe_sessions())
            self.assertEqual(record.state, "stopped")
            self.assertEqual(record.detail, "tmux session exited")
            self.assertEqual(upload_mock.await_args.args[1], "tmux-session-exited")

    def test_failed_ingest_is_persisted_and_replayed_in_order(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = self.relay.RelayState(
                root=Path(tmp),
                command="/bin/sh",
                default_cwd=Path(tmp),
                orchestrator_url="http://orchestrator:12888",
                worker_id="wkr",
                proxy="socks5://127.0.0.1:1055",
            )
            record = self.relay.SessionRecord(
                session_id="child",
                cwd=tmp,
                tmux_session="wh_pi_child",
                state="working",
                detail="Pi process started",
                created_at=10,
                updated_at=11,
            )
            state.sessions[record.session_id] = record
            state._persist(record)

            with patch.object(self.relay.httpx, "AsyncClient", _OfflineIngestClient):
                asyncio.run(state._upload_state(record, "create-working"))
            self.assertEqual(len(record.outbox), 1)
            event_id = record.outbox[0].id

            # A new RelayState proves the durable JSON record, rather than
            # memory, owns retry state after a daemon restart.
            reloaded = self.relay.RelayState(
                root=Path(tmp),
                command="/bin/sh",
                default_cwd=Path(tmp),
                orchestrator_url="http://orchestrator:12888",
                worker_id="wkr",
                proxy="socks5://127.0.0.1:1055",
            )
            _CapturingIngestClient.payloads.clear()
            _CapturingIngestClient.init_kwargs.clear()
            with patch.object(self.relay.httpx, "AsyncClient", _CapturingIngestClient):
                asyncio.run(reloaded._flush_all_outboxes())
            replayed = reloaded.sessions["child"]
            self.assertEqual(replayed.outbox, [])
            self.assertEqual(len(_CapturingIngestClient.payloads), 1)
            self.assertEqual(_CapturingIngestClient.payloads[0]["events"][0]["id"], event_id)
            self.assertEqual(_CapturingIngestClient.payloads[0]["events"][0]["event_type"], "create-working")
            self.assertEqual(_CapturingIngestClient.init_kwargs[0]["proxy"], "socks5://127.0.0.1:1055")


class PiRelayPublicationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        # worker_daemon imports sibling pi_relay by its production module name.
        cls.relay = load_module("pi_relay", RELAY_PATH)
        cls.daemon = load_module("worker_daemon_for_pi_relay_test", DAEMON_PATH)

    def test_heartbeat_capability_persists_in_worker_database(self):
        async def check() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                db = Database(Path(tmp) / "workers.sqlite")
                await db.connect()
                try:
                    await db.upsert_worker(
                        WorkerRegistration(
                            worker_id="worker-1",
                            name="worker-1",
                            worker_ip="100.64.0.1",
                            pi_relay_port=27888,
                            pi_relay_available=True,
                            pi_relay_protocol_version=1,
                        )
                    )
                    worker = await db.get_worker("worker-1")
                    assert worker is not None
                    self.assertEqual(worker.pi_relay_port, 27888)
                    self.assertTrue(worker.pi_relay_available)
                    self.assertEqual(worker.pi_relay_protocol_version, 1)
                finally:
                    await db.close()

        asyncio.run(check())

    def test_reconcile_repairs_a_missing_serve_rule_and_withholds_dead_relay(self):
        class RunningRelay:
            is_running = True

        class DeadRelay:
            is_running = False

        with patch.object(self.daemon, "is_pi_relay_published", return_value=False), patch.object(
            self.daemon, "publish_pi_relay", return_value=True
        ) as publish:
            self.assertTrue(self.daemon.reconcile_pi_relay(RunningRelay()))
            publish.assert_called_once_with(self.daemon.PI_RELAY_PORT)
        with patch.object(self.daemon, "publish_pi_relay") as publish:
            self.assertFalse(self.daemon.reconcile_pi_relay(DeadRelay()))
            publish.assert_not_called()

    def test_sigterm_runs_daemon_cleanup_before_exit(self):
        temp_dir = Path(tempfile.mkdtemp())
        marker = temp_dir / "cleanup"
        ready = temp_dir / "ready"
        script = textwrap.dedent(
            f"""
            import asyncio
            import importlib.util
            import sys
            from pathlib import Path

            relay_spec = importlib.util.spec_from_file_location("pi_relay", {str(RELAY_PATH)!r})
            relay = importlib.util.module_from_spec(relay_spec)
            sys.modules["pi_relay"] = relay
            relay_spec.loader.exec_module(relay)
            daemon_spec = importlib.util.spec_from_file_location("worker_daemon_sigterm_test", {str(DAEMON_PATH)!r})
            daemon = importlib.util.module_from_spec(daemon_spec)
            daemon_spec.loader.exec_module(daemon)

            class Client:
                async def __aenter__(self): return self
                async def __aexit__(self, *args): return False
            class Relay:
                is_running = True
                state = object()
                def __init__(self, port, **kwargs): pass
                async def start(self): pass
                async def stop(self): pass
            class Jobs:
                def __init__(self, *args, **kwargs): pass
                async def start(self): pass
                async def stop(self): pass

            daemon.ORCHESTRATOR_HOST = "orchestrator"
            daemon.HEARTBEAT_INTERVAL = 60
            daemon.RelayServer = Relay
            daemon.PiJobServer = Jobs
            daemon.PiJobService = lambda **kwargs: object()
            daemon.build_http_client = lambda: Client()
            daemon.get_worker_id = lambda: "worker"
            daemon.get_tailscale_identity = lambda: ("100.64.0.1", "worker")
            daemon.publish_pi_relay = lambda port: (Path({str(ready)!r}).write_text("ready"), True)[1]
            daemon.is_pi_relay_published = lambda port: True
            daemon.send_heartbeat = lambda *args, **kwargs: asyncio.sleep(0)
            daemon.unpublish_pi_relay = lambda port: Path({str(marker)!r}).write_text("done")
            asyncio.run(daemon.main())
            """
        )
        environment = dict(os.environ)
        environment["PYTHONPATH"] = str(WORKER_CONTAINER) + os.pathsep + environment.get("PYTHONPATH", "")
        proc = subprocess.Popen([sys.executable, "-c", script], env=environment)
        try:
            for _ in range(100):
                if ready.exists():
                    break
                time.sleep(0.05)
            self.assertTrue(ready.exists(), "daemon never reached signal-handler setup")
            proc.terminate()
            self.assertEqual(proc.wait(timeout=5), 0)
            self.assertEqual(marker.read_text(encoding="utf-8"), "done")
        finally:
            if proc.poll() is None:
                proc.kill()

    def test_legacy_database_gains_relay_columns(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "legacy.sqlite"
            legacy = sqlite3.connect(db_path)
            legacy.execute(
                """CREATE TABLE workers (
                    id TEXT PRIMARY KEY, name TEXT NOT NULL, worker_ip TEXT NOT NULL,
                    status TEXT DEFAULT 'offline', last_heartbeat_ts INTEGER DEFAULT 0,
                    created_at INTEGER DEFAULT 0
                )"""
            )
            legacy.execute("INSERT INTO workers (id, name, worker_ip) VALUES ('legacy', 'legacy', '100.64.0.2')")
            legacy.commit()
            legacy.close()

            async def check() -> None:
                db = Database(db_path)
                await db.connect()
                try:
                    columns = await db._db.execute_fetchall("PRAGMA table_info(workers)")  # noqa: SLF001
                    names = {column["name"] for column in columns}
                    self.assertTrue(
                        {"pi_relay_port", "pi_relay_available", "pi_relay_protocol_version"}.issubset(names)
                    )
                    row = await db._db.execute_fetchall(  # noqa: SLF001
                        "SELECT pi_relay_port, pi_relay_available, pi_relay_protocol_version FROM workers WHERE id='legacy'"
                    )
                    self.assertEqual(tuple(row[0]), (0, 0, 0))
                finally:
                    await db.close()

            asyncio.run(check())

    def test_publish_and_unpublish_use_only_the_relay_tcp_rule(self):
        completed = SimpleNamespace(returncode=0, stderr="")
        with patch.object(self.daemon.subprocess, "run", return_value=completed) as run:
            self.assertTrue(self.daemon.publish_pi_relay(27888))
            self.daemon.unpublish_pi_relay(27888)

        self.assertEqual(
            run.call_args_list[0].args[0],
            [
                "tailscale",
                f"--socket={self.daemon.TS_SOCKET}",
                "serve",
                "--bg",
                "--yes",
                "--tcp=27888",
                "tcp://127.0.0.1:27888",
            ],
        )
        self.assertEqual(
            run.call_args_list[1].args[0],
            [
                "tailscale",
                f"--socket={self.daemon.TS_SOCKET}",
                "serve",
                "--tcp=27888",
                "off",
            ],
        )

    def test_publication_failure_is_reported_and_heartbeat_advertises_capability(self):
        failed = SimpleNamespace(returncode=1, stderr="serve is unavailable")
        with patch.object(self.daemon.subprocess, "run", return_value=failed):
            self.assertFalse(self.daemon.publish_pi_relay(27888))

        with patch.object(self.daemon, "get_gpu_info", return_value={"gpu_count": 0, "gpus": []}), patch.object(
            self.daemon, "get_system_info", return_value={}
        ), patch.object(self.daemon, "get_active_jobs", return_value=[]), patch.object(
            self.daemon, "get_data_paths", return_value=[]
        ):
            payload = self.daemon.build_payload(
                "worker-1", "100.64.0.1", "worker-1.hs.d0me.xyz", pi_relay_available=True
            )
        self.assertEqual(payload["pi_relay_port"], self.daemon.PI_RELAY_PORT)
        self.assertTrue(payload["pi_relay_available"])
        self.assertEqual(payload["pi_relay_protocol_version"], 2)


if __name__ == "__main__":
    unittest.main()
