"""Role-aware fleet session API regression tests."""

from __future__ import annotations

import asyncio
import hashlib
import os
import tempfile
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from worker_harness.db import Database
from worker_harness.heartbeat import create_app
from worker_harness.machines import Machine
from worker_harness.models import (
    PiBridgeEventBatch, PiBridgeRegister, PiSession, PiSessionCommand,
    PiSessionEvent, PiSessionState, PiSessionType,
)
from worker_harness.pr import PrRejected
from worker_harness.projects import Project


class PiSessionsApiTests(unittest.TestCase):
    def setUp(self) -> None:
        operator_env = patch.dict(os.environ, {"WH_OPERATOR_TOKEN": "o" * 43})
        operator_env.start()
        self.addCleanup(operator_env.stop)
        temporary = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
        temporary.close()
        self.path = Path(temporary.name)
        self.db = Database(self.path)
        asyncio.run(self.db.connect())
        self.sessions = {
            "orchestrator": self._insert("orchestrator", "orchestrator", "orch-token"),
            "pm-one": self._insert("pm-one", "pm", "pm-one-token", project="one"),
            "pm-two": self._insert("pm-two", "pm", "pm-two-token", project="two"),
            "task-one": self._insert(
                "task-one", "task", "task-one-token", project="one", parent="pm-one"
            ),
            "task-two": self._insert(
                "task-two", "task", "task-two-token", project="two", parent="pm-two"
            ),
        }
        self.app = create_app(self.db)
        self.app.state.projects = {
            "one": Project("one", "desktop", "/home/user/one", "owner/one"),
            "two": Project("two", "desktop", "/home/user/two", "owner/two"),
        }
        self.app.state.machines = {
            "desktop": Machine("desktop", "user@desktop", "/home/user")
        }
        self.app.state.orchestrator = AsyncMock()
        @asynccontextmanager
        async def manager_session(project):
            yield self.sessions[f"pm-{project}"]
        self.app.state.orchestrator.pm_session = manager_session

    def client(self) -> TestClient:
        return TestClient(self.app, headers=self.auth("o" * 43))

    def tearDown(self) -> None:
        asyncio.run(self.db.close())
        self.path.unlink(missing_ok=True)

    def _insert(
        self,
        session_id: str,
        role: str,
        token: str,
        *,
        project: str = "",
        parent: str | None = None,
    ) -> PiSession:
        meta = {"project": project} if project else {}
        session = PiSession(
            id=session_id,
            parent_session_id=parent,
            session_type=PiSessionType.INTERACTIVE,
            state=PiSessionState.IDLE,
            role=role,
            token_hash=hashlib.sha256(token.encode()).hexdigest(),
            meta=meta,
            name=session_id,
            bridge_incarnation=f"{session_id}-inc",
            created_at=1,
            updated_at=1,
        )
        asyncio.run(self.db.insert_pi_session(session))
        return session

    @staticmethod
    def auth(token: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {token}"}

    def commands(self, session_id: str) -> list[dict]:
        commands = asyncio.run(
            self.db.claim_pi_session_commands(
                session_id, f"{session_id}-inc", now=100, lease_seconds=30
            )
        )
        return [command.model_dump(mode="json") for command in commands]

    def test_session_list_authorization_and_pm_scope(self):
        with self.client() as client:
            operator = client.get("/api/v1/pi/sessions")
            unknown = client.get(
                "/api/v1/pi/sessions", headers=self.auth("not-a-session-token")
            )
            task = client.get(
                "/api/v1/pi/sessions", headers=self.auth("task-one-token")
            )
            manager = client.get(
                "/api/v1/pi/sessions", headers=self.auth("pm-one-token")
            )
        self.assertEqual(operator.status_code, 200, operator.text)
        self.assertEqual(unknown.status_code, 401, unknown.text)
        self.assertEqual(task.status_code, 403, task.text)
        self.assertEqual(
            {row["id"] for row in manager.json()},
            {"orchestrator", "pm-one", "task-one"},
        )

    def test_send_delivery_and_relationship_checks_are_server_owned(self):
        with self.client() as client:
            operator = client.post(
                "/api/v1/pi/sessions/task-one:send",
                json={"message": "operator message", "deliver_as": "followUp"},
            )
            orchestrator = client.post(
                "/api/v1/pi/sessions/pm-one:send",
                headers=self.auth("orch-token"),
                json={"message": "orchestrator message"},
            )
            wrong_role = client.post(
                "/api/v1/pi/sessions/task-one:send",
                headers=self.auth("orch-token"),
                json={"message": "not allowed"},
            )
            manager = client.post(
                "/api/v1/pi/sessions/task-one:send",
                headers=self.auth("pm-one-token"),
                json={"message": "manager answer"},
            )
            escalated = client.post(
                "/api/v1/pi/sessions/orchestrator:send",
                headers=self.auth("pm-one-token"),
                json={"message": "Need operator input"},
            )
            cross_project = client.post(
                "/api/v1/pi/sessions/task-two:send",
                headers=self.auth("pm-one-token"),
                json={"message": "not mine"},
            )
        self.assertEqual(operator.status_code, 200, operator.text)
        self.assertEqual(orchestrator.status_code, 200, orchestrator.text)
        self.assertEqual(manager.status_code, 200, manager.text)
        self.assertEqual(escalated.status_code, 200, escalated.text)
        self.assertEqual(wrong_role.status_code, 403, wrong_role.text)
        self.assertEqual(cross_project.status_code, 403, cross_project.text)
        self.assertEqual(
            [command["deliver_as"] for command in self.commands("task-one")],
            ["steer", "steer"],
        )
        self.assertEqual(self.commands("pm-one")[0]["deliver_as"], "steer")
        self.assertEqual(self.commands("orchestrator")[0]["deliver_as"], "steer")

    def test_interrupt_relationships_and_operator_only_configuration(self):
        with self.client() as client:
            orchestrator_interrupt = client.post(
                "/api/v1/pi/sessions/pm-one:interrupt",
                headers=self.auth("orch-token"),
            )
            manager_interrupt = client.post(
                "/api/v1/pi/sessions/task-one:interrupt",
                headers=self.auth("pm-one-token"),
            )
            manager_cannot_interrupt_orchestrator = client.post(
                "/api/v1/pi/sessions/orchestrator:interrupt",
                headers=self.auth("pm-one-token"),
            )
            configured = client.post(
                "/api/v1/pi/sessions/task-one:configure",
                json={
                    "provider": "openai-codex",
                    "model": "gpt-5.6",
                    "thinking_level": "high",
                },
            )
            manager_cannot_configure = client.post(
                "/api/v1/pi/sessions/task-one:configure",
                headers=self.auth("pm-one-token"),
                json={"thinking_level": "low"},
            )
            incomplete_model = client.post(
                "/api/v1/pi/sessions/task-one:configure",
                json={"provider": "openai-codex"},
            )
        self.assertEqual(orchestrator_interrupt.status_code, 200, orchestrator_interrupt.text)
        self.assertEqual(manager_interrupt.status_code, 200, manager_interrupt.text)
        self.assertEqual(
            manager_cannot_interrupt_orchestrator.status_code,
            403,
            manager_cannot_interrupt_orchestrator.text,
        )
        self.assertEqual(configured.status_code, 200, configured.text)
        self.assertEqual(manager_cannot_configure.status_code, 403, manager_cannot_configure.text)
        self.assertEqual(incomplete_model.status_code, 422, incomplete_model.text)
        task_commands = self.commands("task-one")
        self.assertEqual([command["kind"] for command in task_commands], ["interrupt", "configure"])
        self.assertEqual(task_commands[1]["payload"], {
            "provider": "openai-codex",
            "model": "gpt-5.6",
            "thinking_level": "high",
        })
        self.assertEqual(self.commands("pm-one")[0]["kind"], "interrupt")

    def test_ask_pm_blocks_task_and_answer_ack_clears_question(self):
        with self.client() as client:
            asked = client.post(
                "/api/v1/pi/sessions/task-one:ask-pm",
                headers=self.auth("task-one-token"),
                json={"question": "Which database?"},
            )
            heartbeat = client.post(
                "/api/v1/pi/bridge/task-one/events",
                headers=self.auth("task-one-token"),
                json={"incarnation": "task-one-inc", "state": "idle", "events": []},
            )
            spoofed = client.post(
                "/api/v1/pi/sessions/task-two:ask-pm",
                headers=self.auth("task-one-token"),
                json={"question": "May I spoof?"},
            )
            answered = client.post(
                "/api/v1/pi/sessions/task-one:send",
                headers=self.auth("pm-one-token"),
                json={"message": "Use SQLite"},
            )
            command_id = answered.json()["command_id"]
            polled = client.get(
                "/api/v1/pi/bridge/task-one/commands",
                params={"incarnation": "task-one-inc", "wait_seconds": 0},
            )
            acknowledged = client.post(
                f"/api/v1/pi/bridge/task-one/commands/{command_id}:ack",
                json={"incarnation": "task-one-inc"},
            )
        self.assertEqual(asked.status_code, 200, asked.text)
        self.assertEqual(heartbeat.json()["state"], "blocked")
        self.assertEqual(spoofed.status_code, 403, spoofed.text)
        self.assertEqual(polled.status_code, 200, polled.text)
        self.assertEqual(acknowledged.status_code, 200, acknowledged.text)
        blocked = asyncio.run(self.db.get_pi_session("task-one"))
        self.assertEqual(blocked.state, PiSessionState.WORKING)
        self.assertEqual(blocked.question, "")
        events = asyncio.run(self.db.list_pi_session_events("task-one"))
        self.assertIn("blocked", [event.event_type for event in events])
        pm_commands = self.commands("pm-one")
        self.assertEqual(pm_commands[0]["deliver_as"], "followUp")
        self.assertIn("Which database?", pm_commands[0]["message"])

    def test_notify_pm_is_non_interrupting_without_blocking(self):
        with self.client() as client:
            response = client.post(
                "/api/v1/pi/sessions/task-one:notify-pm",
                headers=self.auth("task-one-token"),
                json={"note": "Ready for review"},
            )
        self.assertEqual(response.status_code, 200, response.text)
        task = asyncio.run(self.db.get_pi_session("task-one"))
        self.assertEqual(task.state, PiSessionState.IDLE)
        command = self.commands("pm-one")[0]
        self.assertEqual(command["deliver_as"], "followUp")
        self.assertIn("Ready for review", command["message"])

    def test_projects_and_lazy_pm_send(self):
        with self.client() as client:
            projects = client.get(
                "/api/v1/pi/projects", headers=self.auth("orch-token")
            )
            forbidden = client.get(
                "/api/v1/pi/projects", headers=self.auth("pm-one-token")
            )
            sent = client.post(
                "/api/v1/pi/projects/one:send",
                headers=self.auth("orch-token"),
                json={"message": "Please investigate"},
            )
            missing = client.post(
                "/api/v1/pi/projects/missing:send",
                json={"message": "No such project"},
            )
        self.assertEqual({project["name"] for project in projects.json()}, {"one", "two"})
        self.assertEqual(forbidden.status_code, 403, forbidden.text)
        self.assertEqual(sent.status_code, 200, sent.text)
        self.assertEqual(missing.status_code, 404, missing.text)
        self.assertEqual(self.commands("pm-one")[0]["message"], "Please investigate")

    def test_task_launch_and_teardown_enforce_pm_project_and_parent(self):
        launched = PiSession(
            id="new-task",
            role="task",
            parent_session_id="pm-one",
            meta={"project": "one"},
        )
        self.app.state.orchestrator.launch_task.return_value = launched
        with self.client() as client:
            created = client.post(
                "/api/v1/pi/projects/one/tasks",
                headers=self.auth("pm-one-token"),
                json={"branch": "feature/test", "briefing": "Implement it"},
            )
            cross_project = client.post(
                "/api/v1/pi/projects/two/tasks",
                headers=self.auth("pm-one-token"),
                json={"branch": "feature/bad", "briefing": "Wrong project"},
            )
            torn_down = client.post(
                "/api/v1/pi/sessions/task-one:teardown",
                headers=self.auth("pm-one-token"),
                json={"force": True},
            )
            foreign = client.post(
                "/api/v1/pi/sessions/task-two:teardown",
                headers=self.auth("pm-one-token"),
                json={"force": True},
            )
        self.assertEqual(created.status_code, 201, created.text)
        self.assertEqual(cross_project.status_code, 403, cross_project.text)
        self.assertEqual(torn_down.status_code, 200, torn_down.text)
        self.assertEqual(foreign.status_code, 403, foreign.text)
        self.app.state.orchestrator.launch_task.assert_awaited_once()
        self.app.state.orchestrator.teardown_task.assert_awaited_once_with(
            self.sessions["task-one"], force=True
        )

    def test_submit_pr_validates_ownership_and_persists_url(self):
        task = asyncio.run(self.db.get_pi_session("task-one"))
        task.meta.update(
            machine="desktop", branch="feature/test", worktree="/home/user/worktree"
        )
        asyncio.run(self.db.update_pi_session(task))
        with patch(
            "worker_harness.heartbeat.open_pr",
            new=AsyncMock(return_value="https://github.com/owner/one/pull/1"),
        ) as open_pr:
            with self.client() as client:
                response = client.post(
                    "/api/v1/pi/sessions/task-one:submit-pr",
                    headers=self.auth("pm-one-token"),
                    json={"summary": "complete summary"},
                )
        self.assertEqual(response.status_code, 200, response.text)
        open_pr.assert_awaited_once()
        persisted = asyncio.run(self.db.get_pi_session("task-one"))
        self.assertEqual(persisted.meta["pr_url"], "https://github.com/owner/one/pull/1")

        with patch(
            "worker_harness.heartbeat.open_pr",
            new=AsyncMock(side_effect=PrRejected(["## Outcome"])),
        ):
            with self.client() as client:
                rejected = client.post(
                    "/api/v1/pi/sessions/task-one:submit-pr",
                    headers=self.auth("pm-one-token"),
                    json={"summary": "incomplete"},
                )
        self.assertEqual(rejected.status_code, 422, rejected.text)
        self.assertEqual(rejected.json()["detail"]["missing"], ["## Outcome"])

    def test_bridge_registration_preserves_fleet_identity_and_metadata(self):
        before = asyncio.run(self.db.get_pi_session("task-one"))
        with self.client() as client:
            registered = client.post(
                "/api/v1/pi/bridge/register",
                json={
                    "session_id": "task-one",
                    "incarnation": "replacement",
                    "cwd": "/home/user/worktree",
                    "name": "task-one",
                    "agent": "omp",
                },
            )
        self.assertEqual(registered.status_code, 200, registered.text)
        after = asyncio.run(self.db.get_pi_session("task-one"))
        self.assertEqual(after.role, "task")
        self.assertEqual(after.token_hash, before.token_hash)
        self.assertEqual(after.parent_session_id, "pm-one")
        self.assertEqual(after.meta, {"project": "one"})
        self.assertEqual(after.bridge_incarnation, "replacement")

    def test_bridge_registration_keeps_harness_identity_when_bridge_values_are_empty(self):
        fleet_session = PiSession(
            id="fleet-pm",
            session_type=PiSessionType.INTERACTIVE,
            state=PiSessionState.STARTING,
            role="pm",
            name="pm-worker-harness",
            host="desktop",
            created_at=1,
            updated_at=1,
        )
        asyncio.run(self.db.insert_pi_session(fleet_session))

        with self.client() as client:
            registered = client.post(
                "/api/v1/pi/bridge/register",
                json={
                    "session_id": "fleet-pm",
                    "incarnation": "fleet-incarnation",
                    "cwd": "/home/user/worker-harness",
                    "name": "",
                    "host": "",
                    "agent": "omp",
                },
            )

        self.assertEqual(registered.status_code, 200, registered.text)
        persisted = asyncio.run(self.db.get_pi_session("fleet-pm"))
        self.assertEqual(persisted.name, "pm-worker-harness")
        self.assertEqual(persisted.host, "desktop")

    def test_global_router_registration_preserves_type_and_claims_commands(self):
        token = "global-router-token"
        global_session = PiSession(
            id="global-router",
            session_type=PiSessionType.GLOBAL_ROUTER,
            state=PiSessionState.STARTING,
            role="orchestrator",
            token_hash=hashlib.sha256(token.encode()).hexdigest(),
            name="orchestrator",
            host="desktop",
            created_at=1,
            updated_at=1,
        )
        asyncio.run(self.db.insert_pi_session(global_session))

        with self.client() as client:
            registered = client.post(
                "/api/v1/pi/bridge/register",
                headers=self.auth(token),
                json={
                    "session_id": "global-router",
                    "incarnation": "global-incarnation",
                    "cwd": "/home/user/agent-orchestrator",
                    "name": "",
                    "host": "",
                    "agent": "omp",
                },
            )
            queued = client.post(
                "/api/v1/pi/sessions/global-router:send",
                json={"message": "route this task"},
            )
            claimed = client.get(
                "/api/v1/pi/bridge/global-router/commands",
                headers=self.auth(token),
                params={"incarnation": "global-incarnation", "wait_seconds": 0},
            )

        self.assertEqual(registered.status_code, 200, registered.text)
        self.assertEqual(registered.json()["session_type"], "global-router")
        self.assertEqual(queued.status_code, 200, queued.text)
        self.assertEqual(claimed.status_code, 200, claimed.text)
        self.assertEqual([command["message"] for command in claimed.json()], ["route this task"])
        persisted = asyncio.run(self.db.get_pi_session("global-router"))
        self.assertEqual(persisted.session_type, PiSessionType.GLOBAL_ROUTER)

        swept = asyncio.run(
            self.db.sweep_stale_interactive_pi_sessions(cutoff_ts=10**12, now=200)
        )
        self.assertIn("global-router", swept)
        persisted = asyncio.run(self.db.get_pi_session("global-router"))
        self.assertEqual(persisted.state, PiSessionState.STOPPED)

    def test_operator_auth_cannot_be_bypassed_by_omitting_bearer(self):
        with TestClient(self.app) as client:
            for method, path, payload in (
                ("GET", "/api/v1/pi/sessions", None),
                ("GET", "/api/v1/workers", None),
                ("POST", "/api/v1/jobs", {"worker_id": "desktop", "command": "id"}),
                ("POST", "/api/v1/pi/bridge/register", {
                    "session_id": "pm-one", "incarnation": "hijack",
                }),
            ):
                with self.subTest(path=path):
                    response = client.request(method, path, json=payload)
                    self.assertEqual(response.status_code, 401, response.text)
            self.assertEqual(client.get("/health").status_code, 200)
        self.app.state.operator_token = ""
        with TestClient(self.app) as client:
            self.assertEqual(client.get("/api/v1/workers").status_code, 401)

    def test_orchestrator_cannot_access_worker_compute_or_admin_routes(self):
        with self.client() as client:
            for method, path, payload in (
                ("GET", "/api/v1/workers", None),
                ("POST", "/api/v1/jobs", {"worker_id": "desktop", "command": "id"}),
                ("GET", "/api/v1/marimo", None),
                ("POST", "/api/v1/tunnels", {}),
                ("GET", "/api/v1/workers/desktop/files?path=/etc/passwd", None),
            ):
                with self.subTest(path=path):
                    response = client.request(
                        method, path, json=payload, headers=self.auth("orch-token")
                    )
                    self.assertEqual(response.status_code, 403, response.text)
            self.assertEqual(client.get(
                "/api/v1/workers", headers=self.auth("task-one-token")
            ).status_code, 200)
            self.assertEqual(client.delete(
                "/api/v1/workers/prune", headers=self.auth("pm-one-token")
            ).status_code, 403)

    def test_private_session_reads_enforce_parent_and_project(self):
        self._insert("wrong-project", "task", "wrong-token", project="two", parent="pm-one")
        with self.client() as client:
            for suffix in ("", "/events", "/stream"):
                for target, token in (
                    ("task-two", "pm-one-token"),
                    ("wrong-project", "pm-one-token"),
                    ("pm-one", "task-one-token"),
                ):
                    with self.subTest(suffix=suffix, target=target, token=token):
                        response = client.get(
                            f"/api/v1/pi/sessions/{target}{suffix}",
                            headers=self.auth(token),
                        )
                        self.assertEqual(response.status_code, 403, response.text)
            roster = client.get("/api/v1/pi/sessions", headers=self.auth("pm-one-token"))
            self.assertNotIn("wrong-project", {row["id"] for row in roster.json()})
            own = client.get(
                "/api/v1/pi/sessions/task-one", headers=self.auth("task-one-token")
            )
            self.assertEqual(own.status_code, 200, own.text)

    def test_session_hashes_never_leave_api_but_still_authenticate(self):
        self.app.state.orchestrator.ensure_orchestrator.return_value = self.sessions["orchestrator"]
        self.app.state.orchestrator.ensure_pm.return_value = self.sessions["pm-one"]
        self.app.state.orchestrator.launch_task.return_value = self.sessions["task-one"]
        with self.client() as client:
            responses = [
                client.get("/api/v1/pi/sessions"),
                client.get("/api/v1/pi/sessions?include_attach_info=true"),
                client.get("/api/v1/pi/sessions/task-one"),
                client.get("/api/v1/pi/orchestrator"),
                client.post("/api/v1/pi/orchestrator:send", json={"message": "hello"}),
                client.post("/api/v1/pi/projects/one:send", json={"message": "hello"}),
                client.post(
                    "/api/v1/pi/projects/one/tasks", headers=self.auth("pm-one-token"),
                    json={"branch": "task/secret", "briefing": "work"},
                ),
                client.post(
                    "/api/v1/pi/bridge/register", headers=self.auth("task-one-token"),
                    json={"session_id": "task-one", "incarnation": "new"},
                ),
            ]
        for response in responses:
            self.assertIn(response.status_code, {200, 201}, response.text)
            self.assertNotIn("token_hash", response.text)
            for session in self.sessions.values():
                self.assertNotIn(session.token_hash, response.text)
        persisted = asyncio.run(self.db.get_pi_session_by_token_hash(
            hashlib.sha256(b"task-one-token").hexdigest()
        ))
        self.assertEqual(persisted.id, "task-one")

    def test_agent_terminal_gateway_is_operator_only(self):
        with self.client() as client:
            response = client.get(
                "/api/v1/pi/sessions/task-one/attach-info",
                headers=self.auth("task-one-token"),
            )
            self.assertEqual(response.status_code, 403)
            with client.websocket_connect(
                "/api/v1/pi/sessions/task-one/attach-gateway",
                headers=self.auth("task-one-token"),
            ) as socket:
                with self.assertRaises(WebSocketDisconnect) as caught:
                    socket.receive_json()
            self.assertEqual(caught.exception.code, 4403)

    def test_bridge_token_cannot_impersonate_another_session_or_revive_stopped_agent(self):
        with self.client() as client:
            spoofed = client.post(
                "/api/v1/pi/bridge/register", headers=self.auth("task-one-token"),
                json={"session_id": "task-two", "incarnation": "hijack"},
            )
            self.assertEqual(spoofed.status_code, 403, spoofed.text)
            stopped = client.post(
                "/api/v1/pi/bridge/task-one/events", headers=self.auth("task-one-token"),
                json={"incarnation": "task-one-inc", "state": "stopped"},
            )
            self.assertEqual(stopped.status_code, 200, stopped.text)
            revived = client.post(
                "/api/v1/pi/bridge/register", headers=self.auth("task-one-token"),
                json={"session_id": "task-one", "incarnation": "revived"},
            )
            self.assertEqual(revived.status_code, 401, revived.text)

    def test_blocked_task_can_exit_and_clears_question(self):
        with self.client() as client:
            client.post(
                "/api/v1/pi/sessions/task-one:ask-pm", headers=self.auth("task-one-token"),
                json={"question": "Need advice"},
            )
            stopped = client.post(
                "/api/v1/pi/bridge/task-one/events", headers=self.auth("task-one-token"),
                json={"incarnation": "task-one-inc", "state": "stopped"},
            )
        self.assertEqual(stopped.json()["state"], "stopped")
        persisted = asyncio.run(self.db.get_pi_session("task-one"))
        self.assertEqual(persisted.question, "")

    def test_terminal_task_cannot_be_reblocked_by_an_inflight_question(self):
        async def exercise():
            await self.db.finish_pi_session("task-one", PiSessionState.STOPPED, "finished")
            with self.assertRaises(ValueError):
                await self.db.set_pi_session_blocked("task-one", "late question")
            session = await self.db.get_pi_session("task-one")
            self.assertEqual(session.state, PiSessionState.STOPPED)
            self.assertEqual(session.question, "")
            self.assertEqual(await self.db.list_pi_session_events("task-one"), [])
        asyncio.run(exercise())

    def test_duplicate_prompt_ack_does_not_clear_a_later_question(self):
        async def exercise():
            command = PiSessionCommand(session_id="task-one", message="initial answer")
            await self.db.enqueue_pi_session_command(command)
            await self.db.claim_pi_session_commands("task-one", "task-one-inc")
            await self.db.set_pi_session_blocked("task-one", "first question")
            self.assertTrue(await self.db.ack_pi_session_command(
                "task-one", command.id, "task-one-inc",
            ))
            await self.db.set_pi_session_blocked("task-one", "second question")
            self.assertFalse(await self.db.ack_pi_session_command(
                "task-one", command.id, "task-one-inc",
            ))
            session = await self.db.get_pi_session("task-one")
            self.assertEqual(session.state, PiSessionState.BLOCKED)
            self.assertEqual(session.question, "second question")
        asyncio.run(exercise())

    def test_delegated_database_cutover_removes_dependents_and_is_idempotent(self):
        async def migrate():
            legacy = PiSession(id="legacy", state=PiSessionState.WORKING)
            await self.db.insert_pi_session(legacy)
            await self.db.insert_pi_session_event(PiSessionEvent(
                id="legacy-event", session_id="legacy", event_type="message-end",
            ))
            await self.db.enqueue_pi_session_command(PiSessionCommand(
                id="legacy-command", session_id="legacy", message="old work",
            ))
            await self.db.insert_pi_session_event(PiSessionEvent(
                id="kept-event", session_id="task-one", event_type="message-end",
            ))
            await self.db._db.execute(
                "UPDATE pi_sessions SET session_type='delegated' WHERE id='legacy'"
            )
            await self.db._db.execute(
                "UPDATE pi_sessions SET parent_session_id='legacy' WHERE id='task-one'"
            )
            await self.db._db.commit()
            await self.db._db.execute("PRAGMA foreign_keys=ON")
            await self.db._init_schema()
            await self.db._init_schema()
            self.assertIsNone(await self.db.get_pi_session("legacy"))
            self.assertEqual(await self.db.list_pi_session_events("legacy"), [])
            commands = await self.db._db.execute_fetchall(
                "SELECT id FROM pi_session_commands WHERE session_id='legacy'"
            )
            self.assertEqual(commands, [])
            task = await self.db.get_pi_session("task-one")
            self.assertIsNone(task.parent_session_id)
            self.assertEqual(
                [event.id for event in await self.db.list_pi_session_events("task-one")],
                ["kept-event"],
            )
            self.assertEqual(len(await self.db.list_pi_sessions()), 5)

        asyncio.run(migrate())

    def test_transcript_path_survives_projection_and_cannot_escape_session_directory(self):
        asyncio.run(self.db.update_pi_session_meta("task-one", {"machine": "desktop"}))
        resume_path = "/home/user/.omp/agent/sessions/project/session.jsonl"
        with self.client() as client:
            accepted = client.post(
                "/api/v1/pi/bridge/register", headers=self.auth("task-one-token"),
                json={"session_id": "task-one", "incarnation": "new", "resume_path": resume_path},
            )
            self.assertEqual(accepted.status_code, 200, accepted.text)
            denied = client.post(
                "/api/v1/pi/bridge/register", headers=self.auth("task-one-token"),
                json={"session_id": "task-one", "incarnation": "bad", "resume_path": "/etc/passwd"},
            )
            self.assertEqual(denied.status_code, 403, denied.text)
        asyncio.run(self.db.next_pi_session_pane_sequence("task-one"))
        asyncio.run(self.db.update_pi_session_meta("task-one", {"pr_url": "https://github.com/o/r/pull/1"}))
        persisted = asyncio.run(self.db.get_pi_session("task-one"))
        self.assertEqual(persisted.meta["resume_path"], resume_path)
        self.assertEqual(persisted.bridge_incarnation, "new")

    def test_heartbeat_does_not_reset_idle_clock_but_mailbox_activity_does(self):
        async def exercise():
            session, _ = await self.db.apply_interactive_pi_events(
                "pm-one", PiBridgeEventBatch(incarnation="pm-one-inc", state=PiSessionState.IDLE),
                now=100,
            )
            self.assertEqual(session.updated_at, 1)
            self.assertEqual(session.last_seen, 100)
            await self.db.enqueue_pi_session_command(PiSessionCommand(
                session_id="pm-one", message="review", created_at=101,
            ))
            session, _ = await self.db.apply_interactive_pi_events(
                "pm-one", PiBridgeEventBatch(
                    incarnation="pm-one-inc", state=PiSessionState.IDLE,
                    has_pending_messages=False,
                ), now=200,
            )
            self.assertEqual(session.updated_at, 101)
            self.assertTrue(session.has_pending_messages)
        asyncio.run(exercise())

    def test_operator_token_file_and_invalid_configuration(self):
        secret_file = self.path.with_suffix(".token")
        self.addCleanup(secret_file.unlink, missing_ok=True)
        secret_file.write_text("f" * 43 + "\n")
        with patch.dict(os.environ, {
            "WH_OPERATOR_TOKEN": "", "WH_OPERATOR_TOKEN_FILE": str(secret_file),
        }):
            app = create_app(self.db)
            with TestClient(app) as client:
                self.assertEqual(client.get("/api/v1/workers").status_code, 401)
                self.assertEqual(client.get(
                    "/api/v1/workers", headers=self.auth("f" * 43),
                ).status_code, 200)
            secret_file.write_text("weak")
            with self.assertRaises(ValueError):
                create_app(self.db)


if __name__ == "__main__":
    unittest.main()
