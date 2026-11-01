"""Native clients use authenticated HTTP only, including jobs and shared metadata."""

from __future__ import annotations

import io
import json
import os
import tempfile
import unittest
from functools import partial
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
from rich.console import Console
from typer.testing import CliRunner

from worker_harness import client
from worker_harness.cli import jobs, tunnels
from worker_harness.cli.app import _state


class NativeTransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_credential_precedence_and_authorization(self):
        real_client = httpx.AsyncClient
        seen = []

        def handle(request):
            seen.append(request)
            self.assertEqual(request.extensions["timeout"]["read"], 30.0)
            return httpx.Response(200, json={"ok": True})

        with tempfile.TemporaryDirectory() as directory:
            token_file = Path(directory) / "key"
            token_file.write_text("file-key\n")
            base = {"WH_ORCHESTRATOR_URL": "http://test", "WH_API_KEY_FILE": str(token_file)}
            cases = [({}, "file-key"), ({"WH_API_KEY": "env-key"}, "env-key"),
                     ({"WH_SESSION_ROLE": "task", "WH_SESSION_TOKEN": "session-key", "WH_API_KEY": "env-key"}, "session-key")]
            for credentials, expected in cases:
                with (patch.dict(os.environ, {**base, **credentials}, clear=True),
                      patch.object(client.httpx, "AsyncClient", partial(real_client, transport=httpx.MockTransport(handle)))):
                    result = await client.request("POST", "/api/v1/test", json={"hello": "world"}, params={"a": 1})
                self.assertEqual(result, {"ok": True})
                self.assertEqual(seen[-1].headers["Authorization"], f"Bearer {expected}")
                self.assertEqual(seen[-1].url.params["a"], "1")
                self.assertEqual(json.loads(seen[-1].content), {"hello": "world"})

    async def test_missing_session_token_never_uses_human_credentials(self):
        for role in ("task", ""):
            with (patch.dict(os.environ, {"WH_SESSION_ROLE": role, "WH_API_KEY": "human-key"}, clear=True),
                  patch.object(client.httpx, "AsyncClient") as transport):
                with self.assertRaisesRegex(RuntimeError, "requires WH_SESSION_TOKEN"):
                    await client.request("GET", "/api/v1/jobs")
                transport.assert_not_called()

    async def test_missing_or_unreadable_credential_fails_without_network(self):
        for credentials in ({}, {"WH_API_KEY_FILE": "/nonexistent-native-test/key"}):
            with (patch.dict(os.environ, credentials, clear=True),
                  patch.object(client.httpx, "AsyncClient") as transport):
                with self.assertRaises(RuntimeError):
                    await client.request("GET", "/api/v1/jobs")
                transport.assert_not_called()

    async def test_structured_and_plain_errors_and_empty_success(self):
        real_client = httpx.AsyncClient
        cases = [(409, {"detail": {"code": "MANAGED_BY_MARIMO", "message": "Stop the notebook"}}, "MANAGED_BY_MARIMO", "Stop the notebook"),
                 (404, {"detail": "Not found"}, None, "Not found"),
                 (422, {"detail": [{"loc": ["body"], "input": "do-not-echo"}]}, None, "Unprocessable Entity")]
        with patch.dict(os.environ, {"WH_API_KEY": "test-key", "WH_ORCHESTRATOR_URL": "http://test"}, clear=True):
            for status, payload, code, message in cases:
                def handle(request):
                    return httpx.Response(status, json=payload)
                with patch.object(client.httpx, "AsyncClient", partial(real_client, transport=httpx.MockTransport(handle))):
                    with self.assertRaises(client.HarnessHTTPError) as raised:
                        await client.request("GET", "/api/v1/test")
                self.assertEqual(raised.exception.status_code, status)
                self.assertEqual(raised.exception.code, code)
                self.assertEqual(raised.exception.message, message)
                self.assertNotIn("do-not-echo", str(raised.exception))
            with patch.object(client.httpx, "AsyncClient", partial(real_client, transport=httpx.MockTransport(lambda r: httpx.Response(204)))):
                self.assertIsNone(await client.request("DELETE", "/api/v1/test"))


class NativeJobCliTests(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()
        self.environment = patch.dict(os.environ, {"WH_API_KEY": "test-key", "WH_ORCHESTRATOR_URL": "http://test"}, clear=True)
        self.environment.start()
        _state.clear()
        self.real_client = httpx.AsyncClient

    def tearDown(self):
        self.environment.stop()
        _state.clear()

    def transport(self, handler):
        return patch.object(client.httpx, "AsyncClient", partial(self.real_client, transport=httpx.MockTransport(handler)))

    def test_start_routes_cpu_and_gpu_and_preserves_metadata(self):
        seen = []

        def handle(request):
            seen.append(request)
            return httpx.Response(200, json={"id": "new-job", "status": "pending" if request.url.path.endswith("queue") else "running"})

        for count, path in [(0, "/api/v1/jobs"), (2, "/api/v1/jobs/queue")]:
            with self.transport(handle):
                result = self.runner.invoke(jobs.app, ["start", "worker", "sleep 5", "--name", "Experiment", "--expected-seconds", "120", "--gpu-count", str(count), "--priority", "time_critical", "--priority-note", "deadline", "--deadline-at", "1800000000", "--no-pty"])
            self.assertEqual(result.exit_code, 0, result.output)
            request = seen[-1]
            self.assertEqual(request.url.path, path)
            self.assertEqual(request.headers["authorization"], "Bearer test-key")
            payload = json.loads(request.content)
            self.assertEqual(payload["name"], "Experiment")
            self.assertEqual(payload["expected_seconds"], 120)
            self.assertEqual(payload["priority"], "time_critical")
            self.assertEqual(payload["deadline_at"], 1800000000)
            self.assertTrue(payload["no_pty"])
            if count:
                self.assertEqual(payload["gpu_count"], count)
            else:
                self.assertNotIn("gpu_count", payload)
            self.assertNotIn("owner_user_id", payload)

    def test_start_requires_public_name_positive_duration_and_priority_note(self):
        for flags in ([], ["--name", "A"], ["--expected-seconds", "10"],
                      ["--name", " ", "--expected-seconds", "10"],
                      ["--name", "A", "--expected-seconds", "0"],
                      ["--name", "A", "--expected-seconds", "10", "--priority", "time_critical"]):
            with patch.object(client.httpx, "AsyncClient") as transport:
                result = self.runner.invoke(jobs.app, ["start", "worker", "sleep 1", *flags])
            self.assertNotEqual(result.exit_code, 0, result.output)
            transport.assert_not_called()

    def test_queue_renders_signed_delay_and_unknown_estimate(self):
        base = {"created_at": 1700000000, "moved_job_name": "Experiment B", "moved_job_owner_display_name": "Bob",
                "actor_display_name": "Alice", "reason": "Team agreement"}
        row = dict(id="job-A", name="Experiment A", owner_display_name="Alice", gpu_count=1, position=2,
                   priority="normal", status="pending", expected_seconds=120, estimate_status="unknown",
                   estimate_explanation="FIFO head has unknown finish", delay_history=[
                       {**base, "delay_seconds": 61}, {**base, "delay_seconds": None},
                       {**base, "moved_job_name": "No positive delay", "delay_seconds": -10}])
        board = {"workers": [{"worker_id": "w", "worker_name": "Worker", "revision": 2, "jobs": [row], "cpu_jobs": []}]}
        stream = io.StringIO()
        with (self.transport(lambda request: httpx.Response(200, json=board)),
              patch.object(jobs, "console", Console(file=stream, width=400, markup=False))):
            result = self.runner.invoke(jobs.app, ["queue", "--worker", "w"])
        self.assertEqual(result.exit_code, 0, result.output)
        output = stream.getvalue()
        self.assertIn("+2 minutes estimated delay because of Experiment B by Bob", output)
        self.assertIn("Moved by Alice", output)
        self.assertIn("Team agreement", output)
        self.assertIn("Moved later; delay estimate unavailable", output)
        self.assertIn("unknown: FIFO head has unknown finish", output)
        self.assertNotIn("No positive delay", output)

    def test_logs_follow_stops_on_revoked_or_foreign_access(self):
        for status in (401, 403, 404):
            calls = []

            def handle(request):
                calls.append(request)
                if len(calls) == 1:
                    return httpx.Response(200, json={"logs": "line one\n"})
                return httpx.Response(status, json={"detail": "Access denied"})

            with (self.transport(handle), patch.object(jobs.asyncio, "sleep", new=AsyncMock())):
                result = self.runner.invoke(jobs.app, ["logs", "owned", "--follow"])
            self.assertEqual(result.exit_code, 1, result.output)
            self.assertIn("line one", result.output)
            self.assertIn("Access denied", result.output)
            self.assertEqual(len(calls), 2)
            self.assertTrue(all(r.headers["authorization"] == "Bearer test-key" for r in calls))

    def test_tunnel_conflict_keeps_structured_message(self):
        with self.transport(lambda r: httpx.Response(409, json={"detail": {"code": "MANAGED_BY_MARIMO", "message": "Stop the managed notebook instead"}})):
            result = self.runner.invoke(tunnels.app, ["add", "worker", "12000", "8080"])
        self.assertEqual(result.exit_code, 1)
        self.assertIn("MANAGED_BY_MARIMO", result.output)
        self.assertIn("Stop the managed notebook instead", result.output)


class NativeTuiTests(unittest.IsolatedAsyncioTestCase):
    async def test_shared_job_cannot_fetch_logs_or_stop(self):
        from worker_harness.tui.app import WorkerHarnessTUI
        calls = []
        worker = dict(id="w", name="Worker", status="online", gpu_count=1, cpu_cores=4, gpu_names=["GPU"])
        shared = dict(id="foreign", name="Public label", owner_display_name="Bob", status="running", gpu_count=1,
                      priority="normal", expected_seconds=100, position=0, estimate_status="unknown")

        async def api(method, path, **kwargs):
            calls.append((method, path))
            if path == "/api/v1/workers":
                return [worker]
            if path == "/api/v1/jobs/queue":
                return {"workers": [{"jobs": [shared], "cpu_jobs": []}]}
            if path == "/api/v1/jobs":
                return []
            self.fail(f"Private route called for shared row: {method} {path}")

        app = WorkerHarnessTUI(api)
        async with app.run_test() as pilot:
            screen = app.screen
            screen._selected_worker_id = "w"
            await screen._load_jobs("w")
            screen._selected_job_id = "foreign"
            await screen.action_show_log()
            await screen.action_stop_job()
            self.assertEqual(screen._owned, {})
        self.assertFalse(any("foreign" in path for _, path in calls))
