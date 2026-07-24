"""Control API regression tests for delegated Pi session lifecycle."""

from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from worker_harness.db import Database
from worker_harness.heartbeat import create_app, create_registration_app
from worker_harness.models import WorkerRegistration


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


if __name__ == "__main__":
    unittest.main()
