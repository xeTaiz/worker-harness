"""Control API regression tests for delegated Pi session lifecycle."""

from __future__ import annotations

import asyncio
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from worker_harness.db import Database
from worker_harness.heartbeat import (
    create_app,
    create_registration_app,
    stream_pi_session_events,
    sweep_expired_pi_delegations,
)
from worker_harness.models import (
    PiBridgeRegister,
    PiDelegation,
    PiSession,
    PiSessionState,
    PiSessionType,
    WorkerJobReport,
    WorkerRegistration,
)


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

    def test_mobile_webapp_is_served_by_control_app(self):
        with TestClient(self.app) as client:
            page = client.get("/")
            manifest = client.get("/manifest.webmanifest")
            script = client.get("/app.js")
        self.assertEqual(page.status_code, 200)
        self.assertIn("Pi sessions", page.text)
        self.assertEqual(manifest.status_code, 200)
        self.assertEqual(manifest.json()["display"], "standalone")
        self.assertIn("EventSource", script.text)

    def test_interactive_bridge_register_events_prompt_and_ack(self):
        session_id = "plain-pi-session"
        first_incarnation = "incarnation-1"
        with TestClient(self.app) as client:
            registered = client.post("/api/v1/pi/bridge/register", json={
                "session_id": session_id,
                "incarnation": first_incarnation,
                "cwd": "/home/dome/project",
                "name": "project-agent",
                "host": "archdome",
            })
            self.assertEqual(registered.status_code, 200, registered.text)
            self.assertEqual(registered.json()["session_type"], "interactive")
            self.assertEqual(registered.json()["state"], "idle")

            event_payload = {
                "incarnation": first_incarnation,
                "state": "working",
                "events": [{"id": "interactive-start", "event_type": "agent-start"}],
            }
            first = client.post(f"/api/v1/pi/bridge/{session_id}/events", json=event_payload)
            replay = client.post(f"/api/v1/pi/bridge/{session_id}/events", json=event_payload)
            self.assertEqual(first.json()["events_persisted"], 1)
            self.assertEqual(replay.json()["events_persisted"], 0)

            prompted = client.post(
                f"/api/v1/pi/sessions/{session_id}:prompt",
                json={"message": "continue here", "deliver_as": "steer"},
            )
            self.assertEqual(prompted.status_code, 200, prompted.text)
            command_id = prompted.json()["command_id"]
            commands = client.get(
                f"/api/v1/pi/bridge/{session_id}/commands",
                params={"incarnation": first_incarnation, "wait_seconds": 0},
            )
            self.assertEqual(commands.status_code, 200, commands.text)
            self.assertEqual(commands.json()[0]["id"], command_id)
            self.assertEqual(commands.json()[0]["deliver_as"], "steer")
            ack = client.post(
                f"/api/v1/pi/bridge/{session_id}/commands/{command_id}:ack",
                json={"incarnation": first_incarnation},
            )
            self.assertEqual(ack.status_code, 200, ack.text)
            empty = client.get(
                f"/api/v1/pi/bridge/{session_id}/commands",
                params={"incarnation": first_incarnation, "wait_seconds": 0},
            )
            self.assertEqual(empty.json(), [])

            replayed = client.get(
                f"/api/v1/pi/sessions/{session_id}/events",
                params={"after": 1, "limit": 10},
            )
            self.assertEqual([event["sequence"] for event in replayed.json()], [2, 3])

            configured = client.post(
                f"/api/v1/pi/sessions/{session_id}:configure",
                json={"provider": "openai-codex", "model": "gpt-5.6-luna", "thinking_level": "high"},
            )
            self.assertEqual(configured.status_code, 200, configured.text)
            control_commands = client.get(
                f"/api/v1/pi/bridge/{session_id}/commands",
                params={"incarnation": first_incarnation, "wait_seconds": 0},
            ).json()
            self.assertEqual(control_commands[0]["kind"], "configure")
            self.assertEqual(control_commands[0]["message"], "")
            self.assertEqual(control_commands[0]["payload"], {
                "provider": "openai-codex", "model": "gpt-5.6-luna", "thinking_level": "high",
            })

        session = asyncio.run(self.db.get_pi_session(session_id))
        self.assertEqual(session.state, PiSessionState.WORKING)
        events = asyncio.run(self.db.list_pi_session_events(session_id))
        self.assertEqual(
            [event.event_type for event in events],
            ["bridge-registered", "agent-start", "prompt-queued", "configure-queued"],
        )
        self.assertEqual([event.sequence for event in events], [1, 2, 3, 4])

    def test_interactive_registration_backfills_latest_exchange_once(self):
        session_id = "history-session"
        initial_events = [
            {
                "id": "history-user-start",
                "event_type": "message-start",
                "payload": {"message_id": "user:100:message", "role": "user", "timestamp": 100},
                "created_at": 1,
            },
            {
                "id": "history-user-end",
                "event_type": "message-end",
                "payload": {
                    "message_id": "user:100:message",
                    "message": {"role": "user", "timestamp": 100, "content": [{"type": "text", "text": "question"}]},
                },
                "created_at": 1,
            },
            {
                "id": "history-assistant-start",
                "event_type": "message-start",
                "payload": {"message_id": "assistant:200:message", "role": "assistant", "timestamp": 200},
                "created_at": 2,
            },
            {
                "id": "history-assistant-end",
                "event_type": "message-end",
                "payload": {
                    "message_id": "assistant:200:message",
                    "message": {"role": "assistant", "timestamp": 200, "content": [{"type": "text", "text": "answer"}]},
                },
                "created_at": 2,
            },
        ]
        with TestClient(self.app) as client:
            first = client.post("/api/v1/pi/bridge/register", json={
                "session_id": session_id,
                "incarnation": "history-incarnation-1",
                "initial_events": initial_events,
            })
            self.assertEqual(first.status_code, 200, first.text)

            # A replacement incarnation may assign different event IDs, but
            # stable message IDs still prevent duplicate transcript bubbles.
            replacement_events = [
                {**event, "id": f"replacement-{index}"}
                for index, event in enumerate(initial_events)
            ]
            second = client.post("/api/v1/pi/bridge/register", json={
                "session_id": session_id,
                "incarnation": "history-incarnation-2",
                "initial_events": replacement_events,
            })
            self.assertEqual(second.status_code, 200, second.text)
            replayed = client.get(f"/api/v1/pi/sessions/{session_id}/events").json()

        message_events = [event for event in replayed if event["event_type"].startswith("message-")]
        self.assertEqual([event["event_type"] for event in message_events], [
            "message-start", "message-end", "message-start", "message-end",
        ])
        self.assertEqual(message_events[1]["payload"]["message"]["content"][0]["text"], "question")
        self.assertEqual(message_events[3]["payload"]["message"]["content"][0]["text"], "answer")

    def test_pi_session_sse_replays_from_durable_cursor(self):
        session_id = "stream-session"
        asyncio.run(self.db.register_interactive_pi_session(
            PiBridgeRegister(session_id=session_id, incarnation="inc"), now=100,
        ))
        from worker_harness.models import PiSessionEvent
        asyncio.run(self.db.insert_pi_session_event(PiSessionEvent(
            id="first-stream-event", session_id=session_id,
            event_type="message-start", payload={"message_id": "m1"}, created_at=101,
        )))

        class ConnectedRequest:
            async def is_disconnected(self):
                return False

        async def read_one():
            stream = stream_pi_session_events(ConnectedRequest(), self.db, session_id)
            try:
                return await anext(stream)
            finally:
                await stream.aclose()

        frame = asyncio.run(read_one())
        self.assertIn("id: 1\n", frame)
        self.assertIn("event: pi-event\n", frame)
        self.assertIn('"event_type":"message-start"', frame)
        self.assertIn('"sequence":1', frame)

    def test_interactive_bridge_new_incarnation_rejects_stale_client(self):
        session_id = "reload-session"
        with TestClient(self.app) as client:
            for incarnation in ("old", "new"):
                response = client.post("/api/v1/pi/bridge/register", json={
                    "session_id": session_id, "incarnation": incarnation,
                })
                self.assertEqual(response.status_code, 200, response.text)
            stale_event = client.post(f"/api/v1/pi/bridge/{session_id}/events", json={
                "incarnation": "old", "state": "idle", "events": [],
            })
            stale_poll = client.get(
                f"/api/v1/pi/bridge/{session_id}/commands",
                params={"incarnation": "old", "wait_seconds": 0},
            )
        self.assertEqual(stale_event.status_code, 409, stale_event.text)
        self.assertEqual(stale_poll.status_code, 409, stale_poll.text)

    def test_stale_interactive_bridge_is_reaped(self):
        asyncio.run(self.db.register_interactive_pi_session(
            PiBridgeRegister(session_id="stale-session", incarnation="inc"), now=100,
        ))
        reaped = asyncio.run(self.db.sweep_stale_interactive_pi_sessions(101, now=200))
        self.assertEqual(reaped, ["stale-session"])
        session = asyncio.run(self.db.get_pi_session("stale-session"))
        self.assertEqual(session.state, PiSessionState.STOPPED)
        self.assertEqual(session.detail, "bridge heartbeat expired")

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
            delegation_id = body["delegation_id"]
            self.assertEqual(body["state"], "working")
            self.assertEqual(_RelayClient.calls[0][1], "http://100.64.0.89:27888/v1/sessions")

            listed = client.get("/api/v1/pi/sessions")
            self.assertEqual(listed.status_code, 200)
            self.assertEqual(listed.json()[0]["id"], child)

            prompted = client.post(f"/api/v1/pi/sessions/{child}:prompt", json={"message": "continue"})
            self.assertEqual(prompted.status_code, 200)
            self.assertEqual(prompted.json()["state"], "working")

            unsupported = client.post(
                f"/api/v1/pi/sessions/{child}:configure", json={"thinking_level": "low"},
            )
            self.assertEqual(unsupported.status_code, 409, unsupported.text)

            cancelled = client.post(f"/api/v1/pi/sessions/{child}:cancel")
            self.assertEqual(cancelled.status_code, 200)
            self.assertEqual(cancelled.json()["state"], "stopped")

            events = client.get(f"/api/v1/pi/sessions/{child}/events")
            self.assertEqual([event["event_type"] for event in events.json()], ["starting", "working", "prompt", "cancelled"])

        delegation = asyncio.run(self.db.get_pi_delegation(delegation_id))
        self.assertEqual(delegation.state, PiSessionState.STOPPED)
        self.assertGreater(delegation.completed_at, 0)


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
        asyncio.run(self.db.insert_pi_delegation(PiDelegation(
            id="ingest-delegation",
            worker_id="archdome",
            child_session_id=sid,
            task="t",
            state=PiSessionState.WORKING,
            created_at=1,
        )))
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
            delegation = asyncio.run(self.db.get_pi_delegation("ingest-delegation"))
            self.assertEqual(delegation.state, PiSessionState.IDLE)
            self.assertGreater(delegation.completed_at, 0)

    def test_late_ingest_cannot_resurrect_terminal_projection(self):
        sid = self._seed_child()
        session = asyncio.run(self.db.get_pi_session(sid))
        session.state = PiSessionState.TERMINATION_UNKNOWN
        session.detail = "timeout unacknowledged"
        asyncio.run(self.db.update_pi_session(session))
        with TestClient(self.app) as client:
            stale = client.post(
                f"/pi/worker/archdome/sessions/{sid}/events",
                json={
                    "session_id": sid,
                    "state": "working",
                    "detail": "delayed working report",
                    "events": [{"id": "late-working", "event_type": "working"}],
                },
            )
            self.assertEqual(stale.status_code, 200, stale.text)
        unchanged = asyncio.run(self.db.get_pi_session(sid))
        self.assertEqual(unchanged.state, PiSessionState.TERMINATION_UNKNOWN)
        self.assertEqual(unchanged.detail, "timeout unacknowledged")
        self.assertEqual(
            [event.id for event in asyncio.run(self.db.list_pi_session_events(sid))],
            ["late-working"],
        )

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

    def test_delegated_job_listing_skips_generic_ssh_refresh(self):
        sid = self._seed_child()
        asyncio.run(self.db.upsert_reported_worker_job(
            "archdome",
            WorkerJobReport(
                id="reported-running-job",
                origin_session_id=sid,
                tmux_session="wh_reported_running_job",
                command="sleep 60",
                status="running",
                started_at=10,
                report_revision=1,
            ),
        ))
        refresh = AsyncMock()
        with patch("worker_harness.heartbeat.JobManager.refresh_job_status", new=refresh), TestClient(create_app(self.db)) as client:
            response = client.get("/api/v1/jobs", params={"origin_session_id": sid})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()[0]["status"], "running")
        refresh.assert_not_awaited()

    def test_concurrent_worker_job_reports_preserve_highest_revision(self):
        sid = self._seed_child()

        async def report(revision: int, status: str, finished_at: int = 0):
            return await self.db.upsert_reported_worker_job(
                "archdome",
                WorkerJobReport(
                    id="concurrent-job",
                    origin_session_id=sid,
                    tmux_session="wh_concurrent_job",
                    command="true",
                    status=status,
                    exit_code=0 if status == "done" else None,
                    started_at=10,
                    finished_at=finished_at,
                    report_revision=revision,
                ),
            )

        async def submit_both():
            await asyncio.gather(report(1, "running"), report(2, "done", 12))

        asyncio.run(submit_both())
        job = asyncio.run(self.db.get_job("concurrent-job"))
        self.assertEqual(job.report_revision, 2)
        self.assertEqual(job.status.value, "done")

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

    def test_ingest_marks_missing_session_projection_gone(self):
        with TestClient(self.app) as client:
            event_response = client.post(
                "/pi/worker/archdome/sessions/missing/events",
                json={"session_id": "missing", "state": "idle", "events": []},
            )
            job_response = client.post(
                "/pi/worker/archdome/jobs",
                json={"jobs": [{
                    "id": "missing-origin-job",
                    "origin_session_id": "missing",
                    "tmux_session": "wh_missing_origin_job",
                    "command": "true",
                    "status": "running",
                    "started_at": 10,
                    "report_revision": 1,
                }]},
            )
        self.assertEqual(event_response.status_code, 410, event_response.text)
        self.assertEqual(job_response.status_code, 410, job_response.text)


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
