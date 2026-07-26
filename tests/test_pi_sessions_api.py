"""Control API regression tests for delegated Pi session lifecycle."""

from __future__ import annotations

import asyncio
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from worker_harness.db import Database
from worker_harness.heartbeat import create_app, create_registration_app, sweep_expired_pi_delegations
from worker_harness.models import PiDelegation, PiSession, PiSessionState, PiSessionType, WorkerRegistration


class _Response:
    def __init__(self, payload: dict):
        self.status_code = 200
        self._payload = payload
        self.text = ""

    def json(self):
        return self._payload


class _RelayClient:
    calls: list[tuple[str, str, dict | None]] = []

    def __init__(self, **_kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def request(self, method: str, url: str, json=None):
        self.calls.append((method, url, json))
        session_id = (json or {}).get("session_id", "child")
        if url.endswith(":cancel"):
            state, detail = "stopped", "cancelled"
        elif url.endswith(":prompt"):
            state, detail = "working", "prompt delivered"
        else:
            state, detail = "working", "Pi process started"
        return _Response(
            {
                "session_id": session_id,
                "state": state,
                "tmux_session": "wh_pi_child",
                "detail": detail,
                "updated_at": 123,
            }
        )


class PiSessionsApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
        self.tmp.close()
        self.db = Database(self.tmp.name)
        asyncio.run(self.db.connect())
        asyncio.run(
            self.db.upsert_worker(
                WorkerRegistration(
                    worker_id="archdome",
                    name="archdome",
                    worker_ip="100.64.0.89",
                    pi_relay_port=27888,
                    pi_relay_available=True,
                    pi_relay_protocol_version=2,
                )
            )
        )
        self.app = create_app(self.db)

    def tearDown(self) -> None:
        asyncio.run(self.db.close())
        Path(self.tmp.name).unlink(missing_ok=True)

    def test_delegate_prompt_cancel_and_event_history(self):
        _RelayClient.calls.clear()
        with patch("worker_harness.heartbeat.httpx.AsyncClient", _RelayClient), TestClient(self.app) as client:
            created = client.post(
                "/api/v1/pi/delegations",
                json={"worker_id": "archdome", "task": "inspect this repository", "parent_session_id": "parent"},
            )
            self.assertEqual(created.status_code, 201, created.text)
            body = created.json()
            child = body["child_session_id"]
            self.assertEqual(body["state"], "working")
            self.assertEqual(_RelayClient.calls[0][1], "http://100.64.0.89:27888/v1/sessions")

            listed = client.get("/api/v1/pi/sessions")
            self.assertEqual(listed.status_code, 200)
            self.assertEqual(listed.json()[0]["id"], child)

            prompted = client.post(f"/api/v1/pi/sessions/{child}:prompt", json={"message": "continue"})
            self.assertEqual(prompted.status_code, 200)
            self.assertEqual(prompted.json()["state"], "working")

            cancelled = client.post(f"/api/v1/pi/sessions/{child}:cancel")
            self.assertEqual(cancelled.status_code, 200)
            self.assertEqual(cancelled.json()["state"], "stopped")

            events = client.get(f"/api/v1/pi/sessions/{child}/events")
            self.assertEqual([event["event_type"] for event in events.json()], ["starting", "working", "prompt", "cancelled"])


class PiWorkerIngestTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
        self.tmp.close()
        self.db = Database(self.tmp.name)
        asyncio.run(self.db.connect())
        asyncio.run(
            self.db.upsert_worker(
                WorkerRegistration(
                    worker_id="archdome",
                    name="archdome",
                    worker_ip="100.64.0.89",
                    pi_relay_port=27888,
                    pi_relay_available=True,
                    pi_relay_protocol_version=2,
                )
            )
        )
        asyncio.run(self.db.upsert_worker(
            WorkerRegistration(
                worker_id="kwworker",
                name="kwworker",
                worker_ip="100.64.0.99",
                pi_relay_port=27888,
                pi_relay_available=True,
                pi_relay_protocol_version=2,
            )
        ))
        self.app = create_registration_app(self.db)

    def tearDown(self) -> None:
        asyncio.run(self.db.close())
        Path(self.tmp.name).unlink(missing_ok=True)

    def _seed_child(self) -> str:
        from worker_harness.models import PiSession, PiSessionState, PiSessionType
        from uuid import uuid4
        sid = str(uuid4())
        asyncio.run(self.db.insert_pi_session(PiSession(
            id=sid,
            worker_id="archdome",
            parent_session_id="parent",
            session_type=PiSessionType.DELEGATED,
            state=PiSessionState.WORKING,
            task="t",
            cwd="/tmp",
            tmux_session="wh_pi_x",
            detail="started",
            created_at=1,
            updated_at=1,
        )))
        return sid

    def test_worker_can_ingest_state_and_events(self):
        sid = self._seed_child()
        with TestClient(self.app) as client:
            resp = client.post(
                f"/pi/worker/archdome/sessions/{sid}/events",
                json={
                    "session_id": sid,
                    "state": "idle",
                    "detail": "model returned",
                    "events": [
                        {"event_type": "idle", "payload": {"reason": "completed"}},
                        {"event_type": "working", "payload": {"reason": "follow-up"}},
                    ],
                },
            )
            self.assertEqual(resp.status_code, 200, resp.text)
            self.assertEqual(resp.json()["events_persisted"], 2)

            session = asyncio.run(self.db.get_pi_session(sid))
            self.assertEqual(session.state.value, "idle")
            self.assertEqual(session.detail, "model returned")

            events = asyncio.run(self.db.list_pi_session_events(sid))
            types = [e.event_type for e in events]
            self.assertIn("idle", types)
            self.assertIn("working", types)

    def test_ingest_deduplicates_retried_event_ids(self):
        sid = self._seed_child()
        payload = {
            "session_id": sid,
            "state": "working",
            "events": [{"id": "stable-event", "event_type": "working", "payload": {}}],
        }
        with TestClient(self.app) as client:
            first = client.post(f"/pi/worker/archdome/sessions/{sid}/events", json=payload)
            retry = client.post(f"/pi/worker/archdome/sessions/{sid}/events", json=payload)
            self.assertEqual(first.status_code, 200, first.text)
            self.assertEqual(first.json()["events_persisted"], 1)
            self.assertEqual(retry.status_code, 200, retry.text)
            self.assertEqual(retry.json()["events_persisted"], 0)
        self.assertEqual(len(asyncio.run(self.db.list_pi_session_events(sid))), 1)

    def test_worker_job_reports_link_to_origin_session_and_ignore_replay(self):
        sid = self._seed_child()
        report = {
            "id": "delegated-job-1",
            "origin_session_id": sid,
            "tmux_session": "wh_delegated_job_1",
            "command": "printf hello",
            "status": "running",
            "pty_enabled": True,
            "started_at": 10,
            "finished_at": 0,
            "report_revision": 1,
        }
        with TestClient(self.app) as client:
            first = client.post("/pi/worker/archdome/jobs", json={"jobs": [report]})
            replay = client.post("/pi/worker/archdome/jobs", json={"jobs": [report]})
            self.assertEqual(first.status_code, 200, first.text)
            self.assertEqual(first.json()["jobs_applied"], 1)
            self.assertEqual(replay.status_code, 200, replay.text)
            self.assertEqual(replay.json()["jobs_applied"], 0)

            report.update({"status": "done", "exit_code": 0, "finished_at": 12, "report_revision": 2})
            completed = client.post("/pi/worker/archdome/jobs", json={"jobs": [report]})
            self.assertEqual(completed.status_code, 200, completed.text)
            self.assertEqual(completed.json()["jobs_applied"], 1)

        job = asyncio.run(self.db.get_job("delegated-job-1"))
        self.assertEqual(job.origin_session_id, sid)
        self.assertEqual(job.kind.value, "delegated")
        self.assertEqual(job.status.value, "done")
        self.assertEqual(job.report_revision, 2)

        with TestClient(create_app(self.db)) as client:
            listed = client.get("/api/v1/jobs", params={"origin_session_id": sid})
            self.assertEqual(listed.status_code, 200, listed.text)
            self.assertEqual([item["id"] for item in listed.json()], ["delegated-job-1"])
            self.assertEqual(listed.json()[0]["origin_session_id"], sid)

    def test_worker_job_report_rejects_foreign_origin_session(self):
        sid = self._seed_child()
        report = {
            "id": "foreign-job",
            "origin_session_id": sid,
            "tmux_session": "wh_foreign_job",
            "command": "true",
            "status": "running",
            "started_at": 10,
            "report_revision": 1,
        }
        with TestClient(self.app) as client:
            response = client.post("/pi/worker/kwworker/jobs", json={"jobs": [report]})
            self.assertEqual(response.status_code, 404, response.text)

    def test_ingest_rejects_wrong_worker(self):
        sid = self._seed_child()
        with TestClient(self.app) as client:
            resp = client.post(
                f"/pi/worker/kwworker/sessions/{sid}/events",
                json={"session_id": sid, "state": "idle", "events": []},
            )
            self.assertEqual(resp.status_code, 404, resp.text)

    def test_ingest_rejects_session_id_mismatch(self):
        sid = self._seed_child()
        with TestClient(self.app) as client:
            resp = client.post(
                f"/pi/worker/archdome/sessions/{sid}/events",
                json={"session_id": "wrong", "state": "idle", "events": []},
            )
            self.assertEqual(resp.status_code, 422, resp.text)


class PiSyncDelegationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
        self.tmp.close()
        self.db = Database(self.tmp.name)
        asyncio.run(self.db.connect())
        asyncio.run(
            self.db.upsert_worker(
                WorkerRegistration(
                    worker_id="archdome",
                    name="archdome",
                    worker_ip="100.64.0.89",
                    pi_relay_port=27888,
                    pi_relay_available=True,
                    pi_relay_protocol_version=2,
                )
            )
        )
        self.app = create_app(self.db)

    def tearDown(self) -> None:
        asyncio.run(self.db.close())
        Path(self.tmp.name).unlink(missing_ok=True)

    def test_sync_waits_for_settled_state(self):
        _RelayClient.calls.clear()
        holder: dict = {}

        def run_client() -> None:
            with patch("worker_harness.heartbeat.httpx.AsyncClient", _RelayClient), TestClient(self.app) as client:
                holder["resp"] = client.post(
                    "/api/v1/pi/delegations",
                    json={"worker_id": "archdome", "task": "do x", "sync": True, "timeout_seconds": 30},
                )

        thread = threading.Thread(target=run_client)
        thread.start()
        time.sleep(1.5)

        async def settle() -> None:
            other = Database(self.tmp.name)
            await other.connect()
            sessions = await other.list_pi_sessions()
            session = sessions[0]
            session.state = PiSessionState.IDLE
            await other.update_pi_session(session)
            await other.close()

        asyncio.run(settle())
        thread.join(timeout=30)
        resp = holder["resp"]
        self.assertEqual(resp.status_code, 201, resp.text)
        body = resp.json()
        self.assertTrue(body["settled"])
        self.assertEqual(body["state"], "idle")
        self.assertEqual(body["session"]["id"], body["child_session_id"])
        self.assertTrue(body["delegation"]["id"])

    def test_sync_applies_timeout_gate_before_returning(self):
        _RelayClient.calls.clear()
        with patch("worker_harness.heartbeat.httpx.AsyncClient", _RelayClient), TestClient(self.app) as client:
            started = time.time()
            resp = client.post(
                "/api/v1/pi/delegations",
                json={"worker_id": "archdome", "task": "long task", "sync": True, "timeout_seconds": 2},
            )
            self.assertLess(time.time() - started, 15)
            self.assertEqual(resp.status_code, 201, resp.text)
            body = resp.json()
            self.assertTrue(body["settled"])
            self.assertEqual(body["state"], "stopped")
            self.assertEqual(body["session"]["detail"], "delegation timed out")
            self.assertTrue(any(url.endswith(":cancel") for _, url, _ in _RelayClient.calls))

    def test_sync_reports_unknown_when_timeout_cancel_is_unacknowledged(self):
        class _UnreachableCancelClient(_RelayClient):
            async def request(self, method: str, url: str, json=None):
                if url.endswith(":cancel"):
                    raise RuntimeError("worker unreachable")
                return await super().request(method, url, json)

        _UnreachableCancelClient.calls.clear()
        with patch("worker_harness.heartbeat.httpx.AsyncClient", _UnreachableCancelClient), TestClient(self.app) as client:
            resp = client.post(
                "/api/v1/pi/delegations",
                json={"worker_id": "archdome", "task": "long task", "sync": True, "timeout_seconds": 2},
            )
            self.assertEqual(resp.status_code, 201, resp.text)
            body = resp.json()
            self.assertFalse(body["settled"])
            self.assertEqual(body["state"], "termination_unknown")
            self.assertEqual(body["session"]["detail"], "delegation timed out; worker unreachable")


class PiDelegationTimeoutTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
        self.tmp.close()
        self.db = Database(self.tmp.name)
        asyncio.run(self.db.connect())
        asyncio.run(
            self.db.upsert_worker(
                WorkerRegistration(
                    worker_id="archdome",
                    name="archdome",
                    worker_ip="100.64.0.89",
                    pi_relay_port=27888,
                    pi_relay_available=True,
                    pi_relay_protocol_version=2,
                )
            )
        )
        self.session_id = "child-1"
        asyncio.run(self.db.insert_pi_session(PiSession(
            id=self.session_id,
            worker_id="archdome",
            session_type=PiSessionType.DELEGATED,
            state=PiSessionState.WORKING,
            task="t",
            created_at=100,
            updated_at=100,
        )))
        self.delegation = PiDelegation(
            id="del-1",
            worker_id="archdome",
            child_session_id=self.session_id,
            task="t",
            state=PiSessionState.WORKING,
            timeout_seconds=60,
            created_at=100,
        )
        asyncio.run(self.db.insert_pi_delegation(self.delegation))

    def tearDown(self) -> None:
        asyncio.run(self.db.close())
        Path(self.tmp.name).unlink(missing_ok=True)

    def test_unexpired_delegation_is_untouched(self):
        async def relay(_worker, _method, _path, _payload=None):
            raise AssertionError("relay must not be called")

        asyncio.run(sweep_expired_pi_delegations(self.db, relay, now=120))
        session = asyncio.run(self.db.get_pi_session(self.session_id))
        self.assertEqual(session.state, PiSessionState.WORKING)

    def test_acknowledged_timeout_stops_session(self):
        calls = []

        async def relay(_worker, method, path, _payload=None):
            calls.append((method, path))
            return {"state": "stopped"}

        asyncio.run(sweep_expired_pi_delegations(self.db, relay, now=200))
        session = asyncio.run(self.db.get_pi_session(self.session_id))
        delegation = asyncio.run(self.db.get_pi_delegation("del-1"))
        self.assertEqual(session.state, PiSessionState.STOPPED)
        self.assertEqual(session.detail, "delegation timed out")
        self.assertEqual(delegation.state, PiSessionState.STOPPED)
        self.assertEqual(calls, [("POST", f"/v1/sessions/{self.session_id}:cancel")])
        events = asyncio.run(self.db.list_pi_session_events(self.session_id))
        self.assertEqual(events[-1].event_type, "timeout")

    def test_unacknowledged_timeout_is_termination_unknown(self):
        async def relay(_worker, _method, _path, _payload=None):
            raise RuntimeError("worker unreachable")

        asyncio.run(sweep_expired_pi_delegations(self.db, relay, now=200))
        session = asyncio.run(self.db.get_pi_session(self.session_id))
        delegation = asyncio.run(self.db.get_pi_delegation("del-1"))
        self.assertEqual(session.state, PiSessionState.TERMINATION_UNKNOWN)
        self.assertEqual(delegation.state, PiSessionState.TERMINATION_UNKNOWN)
        events = asyncio.run(self.db.list_pi_session_events(self.session_id))
        self.assertEqual(events[-1].event_type, "timeout_unknown")

    def test_timeout_seconds_round_trips_through_delegation(self):
        delegation = asyncio.run(self.db.get_pi_delegation("del-1"))
        self.assertEqual(delegation.timeout_seconds, 60)


if __name__ == "__main__":
    unittest.main()
