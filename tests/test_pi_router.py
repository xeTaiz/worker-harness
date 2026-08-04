from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from worker_harness.db import Database
from worker_harness.heartbeat import create_app
from worker_harness.models import (
    PiRouterConfig,
    PiSession,
    PiSessionEvent,
    PiSessionState,
    PiSessionType,
)
from worker_harness.pi_router import (
    RouterClassification,
    active_interactive_sessions,
    build_candidates,
    build_classifier_prompt,
    parse_router_output,
    summarize_session_events,
)


class FakeRouter:
    def __init__(self, output: str = "1", latency_ms: int = 17):
        self.output = output
        self.latency_ms = latency_ms
        self.calls: list[tuple[str, PiRouterConfig]] = []

    async def list_models(self):
        return [
            {"provider": "openai-codex", "id": "gpt-5.3-codex-spark", "name": "Spark", "reasoning": True},
            {"provider": "openai-codex", "id": "gpt-5.6-luna", "name": "Luna", "reasoning": True},
        ]

    async def classify(self, prompt: str, config: PiRouterConfig):
        self.calls.append((prompt, config))
        return RouterClassification(
            output=self.output,
            latency_ms=self.latency_ms,
            provider=config.provider,
            model=config.model,
            thinking_level=config.thinking_level,
        )


class PiRouterPureTests(unittest.TestCase):
    def test_candidates_are_active_interactive_only(self):
        sessions = [
            PiSession(id="a", session_type=PiSessionType.INTERACTIVE, state=PiSessionState.IDLE,
                      bridge_incarnation="inc", name="alpha"),
            PiSession(id="b", session_type=PiSessionType.INTERACTIVE, state=PiSessionState.STOPPED,
                      bridge_incarnation="inc", name="beta"),
            PiSession(id="c", session_type=PiSessionType.DELEGATED, state=PiSessionState.WORKING,
                      bridge_incarnation="inc", name="child"),
            PiSession(id="d", session_type=PiSessionType.INTERACTIVE, state=PiSessionState.WORKING,
                      bridge_incarnation=None, name="missing"),
            PiSession(id="e", session_type=PiSessionType.INTERACTIVE, state=PiSessionState.WORKING,
                      bridge_incarnation="inc", name="subagent-scout-1"),
        ]
        self.assertEqual([item.id for item in active_interactive_sessions(sessions)], ["a"])

    def test_summary_and_prompt_are_bounded_and_include_recent_route(self):
        events = [
            PiSessionEvent(session_id="a", event_type="message-end", sequence=1, created_at=10,
                           payload={"message": {"role": "user", "content": [{"type": "text", "text": "fix routing"}]}}),
            PiSessionEvent(session_id="a", event_type="message-delta", sequence=2, created_at=11,
                           payload={"delta": "working on it"}),
            PiSessionEvent(session_id="a", event_type="tool-start", sequence=3, created_at=12,
                           payload={"tool_call_id": "t", "tool_name": "bash"}),
        ]
        summary = summarize_session_events(events)
        self.assertEqual(summary["latest_user_prompt"], "fix routing")
        self.assertEqual(summary["assistant_tail"], "working on it")
        self.assertEqual(summary["current_tool"], "bash")
        sessions = [PiSession(id="a", session_type=PiSessionType.INTERACTIVE, state=PiSessionState.WORKING,
                              bridge_incarnation="inc", name="router", host="camel", cwd="/repo")]
        candidates = build_candidates(sessions, {"a": summary})
        prompt = build_classifier_prompt("continue", candidates, recent_session_id="a", recent_message="fix routing")
        self.assertIn("RECENT ROUTE (<3 minutes", prompt)
        self.assertIn("recipient: 1", prompt)
        self.assertIn("latest_user_prompt='fix routing'", prompt)

    def test_standalone_web_proxy_allowlists_router_surface(self):
        nginx = (Path(__file__).resolve().parents[1] / "web_container" / "nginx.conf").read_text()
        self.assertIn("/api/v1/pi/router", nginx)
        self.assertIn(":dispatch", nginx)
        self.assertIn("requests/[^/]+", nginx)
        self.assertIn('location /api/ {', nginx)

    def test_numeric_output_fails_closed(self):
        self.assertEqual(parse_router_output("2", 3), 2)
        for output in ["0", "4", "recipient 2", "2\nthanks", "-1", ""]:
            self.assertEqual(parse_router_output(output, 3), 0)


class PiRouterApiTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
        self.tmp.close()
        self.db = Database(self.tmp.name)
        asyncio.run(self.db.connect())
        self.router = FakeRouter()
        self.app = create_app(self.db, router_client=self.router)
        self.client = TestClient(self.app)
        self.client.__enter__()
        self._register("alpha", "inc-a", "/repo/alpha", "camel")
        self._register("beta", "inc-b", "/repo/beta", "archdome")

    def tearDown(self):
        self.client.__exit__(None, None, None)
        asyncio.run(self.db.close())
        Path(self.tmp.name).unlink(missing_ok=True)

    def _register(self, session_id: str, incarnation: str, cwd: str, host: str):
        response = self.client.post("/api/v1/pi/bridge/register", json={
            "session_id": session_id,
            "incarnation": incarnation,
            "cwd": cwd,
            "name": session_id,
            "host": host,
        })
        self.assertEqual(response.status_code, 200, response.text)

    def test_model_config_models_auto_dispatch_and_latency(self):
        configured = self.client.put("/api/v1/pi/router/config", json={
            "provider": "openai-codex", "model": "gpt-5.6-luna", "thinking_level": "minimal",
        })
        self.assertEqual(configured.status_code, 200)
        self.assertEqual(self.client.get("/api/v1/pi/router/models").status_code, 200)

        response = self.client.post("/api/v1/pi/router:dispatch", json={"message": "work on alpha"})
        self.assertEqual(response.status_code, 200, response.text)
        result = response.json()
        self.assertEqual(result["status"], "dispatched")
        self.assertEqual(result["selected_session_id"], "alpha")
        self.assertEqual(result["latency_ms"], 17)
        self.assertEqual(result["model"], "gpt-5.6-luna")
        self.assertEqual(len(self.router.calls), 1)
        self.assertIn("work on alpha", self.router.calls[0][0])

        commands = self.client.get("/api/v1/pi/bridge/alpha/commands", params={
            "incarnation": "inc-a", "wait_seconds": 0,
        }).json()
        self.assertEqual(commands[0]["kind"], "prompt")
        self.assertEqual(commands[0]["deliver_as"], "steer")
        self.assertEqual(commands[0]["payload"]["router_request_id"], result["id"])

        snapshot = self.client.get("/api/v1/pi/router/snapshot").json()
        self.assertEqual(snapshot["latest_route"]["latency_ms"], 17)
        self.assertEqual(snapshot["config"]["thinking_level"], "minimal")

    def test_explicit_dispatch_bypasses_router_and_is_idempotent(self):
        payload = {"message": "send explicitly", "target_session_id": "beta", "request_id": "same-request"}
        first = self.client.post("/api/v1/pi/router:dispatch", json=payload)
        second = self.client.post("/api/v1/pi/router:dispatch", json=payload)
        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.json()["id"], "same-request")
        self.assertEqual(first.json()["selected_session_id"], "beta")
        self.assertEqual(self.router.calls, [])
        commands = self.client.get("/api/v1/pi/bridge/beta/commands", params={
            "incarnation": "inc-b", "wait_seconds": 0,
        }).json()
        self.assertEqual(len(commands), 1)

    def test_zero_and_stale_explicit_target_need_selection(self):
        self.router.output = "0"
        auto = self.client.post("/api/v1/pi/router:dispatch", json={"message": "ambiguous"}).json()
        self.assertEqual(auto["status"], "needs_target")
        self.assertEqual(auto["selected_session_id"], None)
        explicit = self.client.post("/api/v1/pi/router:dispatch", json={
            "message": "no", "target_session_id": "missing",
        }).json()
        self.assertEqual(explicit["status"], "needs_target")

    def test_recent_route_is_included_only_within_three_minutes(self):
        first = self.client.post("/api/v1/pi/router:dispatch", json={
            "message": "first alpha", "target_session_id": "alpha",
        }).json()
        self.router.output = "1"
        second = self.client.post("/api/v1/pi/router:dispatch", json={"message": "continue"}).json()
        self.assertIn("RECENT ROUTE (<3 minutes", self.router.calls[-1][0])
        for request_id in (first["id"], second["id"]):
            request = asyncio.run(self.db.get_pi_router_request(request_id))
            request.completed_at -= 181
            asyncio.run(self.db.update_pi_router_request(request))
        self.client.post("/api/v1/pi/router:dispatch", json={"message": "unrelated"})
        self.assertNotIn("RECENT ROUTE (<3 minutes", self.router.calls[-1][0])

    def test_interrupt_and_pending_state_are_durable_bridge_commands(self):
        response = self.client.post("/api/v1/pi/sessions/alpha:interrupt")
        self.assertEqual(response.status_code, 200)
        commands = self.client.get("/api/v1/pi/bridge/alpha/commands", params={
            "incarnation": "inc-a", "wait_seconds": 0,
        }).json()
        self.assertEqual(commands[0]["kind"], "interrupt")
        updated = self.client.post("/api/v1/pi/bridge/alpha/events", json={
            "incarnation": "inc-a", "has_pending_messages": True, "events": [],
        })
        self.assertEqual(updated.status_code, 200)
        session = self.client.get("/api/v1/pi/sessions/alpha").json()
        self.assertTrue(session["has_pending_messages"])


if __name__ == "__main__":
    unittest.main()
