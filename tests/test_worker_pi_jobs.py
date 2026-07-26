"""Regression tests for the private delegated-Pi tmux job service."""

from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fastapi.testclient import TestClient

WORKER_CONTAINER = Path(__file__).parents[1] / "worker_container"
RELAY_PATH = WORKER_CONTAINER / "pi_relay.py"
JOB_PATH = WORKER_CONTAINER / "pi_job_server.py"


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class _OfflineClient:
    def __init__(self, **_kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    async def post(self, *_args, **_kwargs):
        raise RuntimeError("offline")


class _CapturingClient:
    sent: list[dict] = []

    def __init__(self, **_kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    async def post(self, _url, json):
        self.sent.append(json)
        return SimpleNamespace(raise_for_status=lambda: None)


class PiJobServiceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        # pi_job_server imports the production module name.
        cls.relay = load_module("pi_relay", RELAY_PATH)
        cls.jobs = load_module("pi_job_server_for_test", JOB_PATH)

    async def _make_service(self, tmp: Path, *, ingest: bool = False):
        sessions_root = tmp / "pi" / "sessions"
        relay = self.relay.RelayState(
            root=sessions_root,
            command="/bin/sh",
            default_cwd=tmp,
            tmux_tmpdir=tmp / "pi-tmux",
        )
        await relay.create(self.relay.SessionCreate(session_id="child"))
        service = self.jobs.PiJobService(
            sessions=relay,
            sessions_root=sessions_root,
            harness_dir=tmp / "harness",
            tmux_tmpdir=tmp / "tmux",
            orchestrator_url="http://orchestrator:12888" if ingest else None,
            worker_id="worker" if ingest else None,
            proxy="socks5://127.0.0.1:1055" if ingest else None,
        )
        return relay, service

    async def _cleanup(self, relay, service, job_id: str | None = None) -> None:
        if job_id:
            record = service.jobs.get(job_id)
            if record:
                await service._tmux("kill-session", "-t", record.tmux_session)
        await service.stop()
        await relay.cancel("child")

    def test_private_service_runs_tmux_job_with_canonical_log_path(self):
        async def run() -> None:
            with tempfile.TemporaryDirectory() as raw:
                tmp = Path(raw)
                relay, service = await self._make_service(tmp)
                try:
                    record, output = await service.run(
                        "child",
                        self.jobs.DelegatedBashRequest(command="printf 'delegated-ok\\n'", cwd=str(tmp)),
                    )
                    self.assertEqual(record.status, "done")
                    self.assertEqual(record.exit_code, 0)
                    self.assertEqual(output, "delegated-ok")
                    self.assertTrue(record.tmux_session.startswith("wh_"))
                    self.assertEqual(
                        (tmp / "harness" / record.id / "output.log").read_text(encoding="utf-8"),
                        "delegated-ok\nEXIT:0\n",
                    )
                    manifest = tmp / "pi" / "sessions" / "child" / "jobs" / record.id / "job.json"
                    self.assertTrue(manifest.is_file())
                    self.assertEqual(json.loads(manifest.read_text())["origin_session_id"], "child")
                finally:
                    await self._cleanup(relay, service, locals().get("record", None).id if "record" in locals() else None)

        asyncio.run(run())

    def test_stdout_exit_marker_cannot_finish_job_early(self):
        async def run() -> None:
            with tempfile.TemporaryDirectory() as raw:
                tmp = Path(raw)
                relay, service = await self._make_service(tmp)
                try:
                    started = asyncio.get_running_loop().time()
                    record, output = await service.run(
                        "child",
                        self.jobs.DelegatedBashRequest(
                            command="printf 'EXIT:0\\n'; sleep 0.4; printf 'after-sleep\\n'",
                            cwd=str(tmp),
                        ),
                    )
                    self.assertGreaterEqual(asyncio.get_running_loop().time() - started, 0.3)
                    self.assertEqual(record.status, "done")
                    self.assertIn("after-sleep", output)
                    self.assertEqual((tmp / "harness" / record.id / "exit-code").read_text().strip(), "0")
                finally:
                    await self._cleanup(relay, service, locals().get("record", None).id if "record" in locals() else None)

        asyncio.run(run())

    def test_recovered_job_enforces_persisted_timeout_deadline(self):
        async def run() -> None:
            with tempfile.TemporaryDirectory() as raw:
                tmp = Path(raw)
                relay, service = await self._make_service(tmp)
                reloaded = None
                try:
                    record = await service.create_job(
                        "child",
                        self.jobs.DelegatedBashRequest(command="sleep 30", cwd=str(tmp), timeout=0.2),
                    )
                    self.assertGreater(record.deadline_at, 0)
                    await asyncio.sleep(0.3)
                    reloaded = self.jobs.PiJobService(
                        sessions=relay,
                        sessions_root=tmp / "pi" / "sessions",
                        harness_dir=tmp / "harness",
                        tmux_tmpdir=tmp / "tmux",
                    )
                    await reloaded.start()
                    for _ in range(20):
                        current = reloaded.jobs[record.id]
                        if current.status == "failed":
                            break
                        await asyncio.sleep(0.1)
                    current = reloaded.jobs[record.id]
                    self.assertEqual(current.status, "failed")
                    self.assertEqual(current.exit_code, 124)
                finally:
                    if reloaded:
                        await reloaded.stop()
                    await self._cleanup(relay, service, locals().get("record", None).id if "record" in locals() else None)

        asyncio.run(run())

    def test_job_report_outbox_survives_restart_and_uses_proxy(self):
        async def run() -> None:
            with tempfile.TemporaryDirectory() as raw:
                tmp = Path(raw)
                relay, service = await self._make_service(tmp, ingest=True)
                try:
                    with patch.object(self.jobs.httpx, "AsyncClient", _OfflineClient):
                        record, _ = await service.run(
                            "child", self.jobs.DelegatedBashRequest(command="true", cwd=str(tmp))
                        )
                    self.assertGreaterEqual(len(record.outbox), 2)  # running + terminal snapshots

                    reloaded = self.jobs.PiJobService(
                        sessions=relay,
                        sessions_root=tmp / "pi" / "sessions",
                        harness_dir=tmp / "harness",
                        tmux_tmpdir=tmp / "tmux",
                        orchestrator_url="http://orchestrator:12888",
                        worker_id="worker",
                        proxy="socks5://127.0.0.1:1055",
                    )
                    _CapturingClient.sent.clear()
                    with patch.object(self.jobs.httpx, "AsyncClient", _CapturingClient):
                        await reloaded._flush_all_outboxes()
                    flushed = reloaded.jobs[record.id]
                    self.assertEqual(flushed.outbox, [])
                    self.assertEqual(len(_CapturingClient.sent), 2)
                    self.assertEqual(
                        [item["jobs"][0]["report_revision"] for item in _CapturingClient.sent], [1, 2]
                    )
                finally:
                    await self._cleanup(relay, service, locals().get("record", None).id if "record" in locals() else None)

        asyncio.run(run())

    def test_private_state_bridge_reports_idle_without_public_job_route(self):
        async def setup(raw: str):
            return await self._make_service(Path(raw))

        with tempfile.TemporaryDirectory() as raw:
            relay, service = asyncio.run(setup(raw))
            try:
                app = self.jobs.create_job_app(service)
                with TestClient(app) as client:
                    response = client.post(
                        "/v1/sessions/child/state",
                        json={"state": "idle", "event_type": "agent-settled"},
                    )
                self.assertEqual(response.status_code, 200, response.text)
                session = asyncio.run(relay.get("child"))
                self.assertEqual(session.state, "idle")
                self.assertEqual(session.outbox[-1].event_type, "agent-settled")
            finally:
                asyncio.run(self._cleanup(relay, service))

    def test_public_terminal_relay_has_no_job_execution_route(self):
        with tempfile.TemporaryDirectory() as raw:
            app = self.relay.create_relay_app(
                sessions_root=Path(raw) / "sessions", pi_command="/bin/sh", default_cwd=Path(raw)
            )
            with TestClient(app) as client:
                response = client.post("/v1/sessions/child/jobs", json={"command": "echo unsafe"})
            self.assertEqual(response.status_code, 404)


if __name__ == "__main__":
    unittest.main()
