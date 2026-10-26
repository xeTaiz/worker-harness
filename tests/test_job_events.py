"""Durable ordinary-job events and authenticated SSE replay (no Pi consumer)."""

import asyncio
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from worker_harness.db import Database
from worker_harness.heartbeat import create_app
from worker_harness.job import JobManager
from worker_harness.models import JobStatus, PiSession, PiSessionState, WorkerRegistration
from worker_harness.slurm import stream_job_events
from worker_harness.ssh import SSHResult


class ConnectedRequest:
    async def is_disconnected(self):
        return False


class JobEventTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.tmp.name) / "events.sqlite")
        await self.db.connect()
        self.worker = await self.db.upsert_worker(WorkerRegistration(
            worker_id="ordinary", name="ordinary", worker_ip="100.64.0.2",
        ))

    async def asyncTearDown(self):
        await self.db.close()
        self.tmp.cleanup()

    async def test_real_ordinary_transitions_are_atomic_deduplicated_and_routed(self):
        manager = JobManager(self.db)
        with patch("worker_harness.job.ssh_tmux_new", new=AsyncMock(
            return_value=SSHResult(stdout="started", stderr="", returncode=0))):
            first = await manager.start_job(self.worker, "true", notify_recipient_id="one")
            second = await manager.start_job(self.worker, "false", notify_recipient_id="two")
            await manager.start_job(self.worker, "unrouted")
        first.status = JobStatus.DONE
        first.finished_at = 100
        first.exit_code = 0
        await self.db.update_job(first)
        await self.db.update_job(first)
        second.status = JobStatus.FAILED
        second.finished_at = 101
        second.exit_code = 1
        await self.db.update_job(second)
        await self.db.update_job(second)
        one = await self.db.list_job_events("one")
        two = await self.db.list_job_events("two")
        self.assertEqual([event.event_type for event in one], ["job_started", "job_finished"])
        self.assertEqual([event.event_type for event in two], ["job_started", "job_failed"])
        self.assertTrue(all(event.job_id == first.id for event in one))
        self.assertEqual(await self.db.list_job_events("unknown"), [])
        self.assertEqual([event.sequence for event in await self.db.list_job_events(
            "one", after=one[0].sequence)], [one[1].sequence])
        self.assertEqual(len(await self.db.list_job_events("one", limit=1)), 1)
        self.assertEqual((await self.db.get_job(first.id)).status, JobStatus.DONE)

    async def test_sse_replays_cursor_and_tails_persisted_events(self):
        manager = JobManager(self.db)
        with patch("worker_harness.job.ssh_tmux_new", new=AsyncMock(
            return_value=SSHResult(stdout="started", stderr="", returncode=0))):
            job = await manager.start_job(self.worker, "true", notify_recipient_id="recipient")
        events = await self.db.list_job_events("recipient")
        stream = stream_job_events(ConnectedRequest(), self.db, "recipient")
        first = await anext(stream)
        self.assertIn(f"id: {events[0].sequence}\nevent: job-event\n", first)
        event_data = json.loads(first.split("data: ", 1)[1])
        self.assertEqual(event_data["job_id"], job.id)
        await stream.aclose()
        job.status = JobStatus.DONE
        job.exit_code = 0
        job.finished_at = 100
        await self.db.update_job(job)
        replay = stream_job_events(ConnectedRequest(), self.db, "recipient", events[0].sequence)
        finished = await anext(replay)
        self.assertEqual(json.loads(finished.split("data: ", 1)[1])["event_type"], "job_finished")
        await replay.aclose()
        await self.db.close()
        await self.db.connect()
        self.assertEqual(len(await self.db.list_job_events("recipient")), 2)

    async def test_sse_keepalive_and_disconnect(self):
        stream = stream_job_events(ConnectedRequest(), self.db, "empty")
        with patch("worker_harness.slurm.asyncio.sleep", new=AsyncMock()), \
             patch("worker_harness.slurm.asyncio.get_running_loop") as loop:
            loop.return_value.time.side_effect = [0.0, 16.0]
            self.assertEqual(await anext(stream), ": keep-alive\n\n")
        await stream.aclose()
        disconnected = ConnectedRequest()
        disconnected.is_disconnected = AsyncMock(return_value=True)
        closed = stream_job_events(disconnected, self.db, "empty")
        with self.assertRaises(StopAsyncIteration):
            await anext(closed)


class JobEventAuthTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.tmp.name) / "auth.sqlite")
        asyncio.run(self.db.connect())
        self.environment = patch.dict("os.environ", {"WH_OPERATOR_TOKEN": "o" * 43})
        self.environment.start()
        self.transports = []
        for target in ("_exec_ssh", "_exec_ssh_bytes"):
            transport = AsyncMock(side_effect=AssertionError("unexpected SSH transport"))
            boundary = patch(f"worker_harness.ssh.{target}", new=transport)
            boundary.start()
            self.addCleanup(boundary.stop)
            self.transports.append(transport)
        self.client = TestClient(create_app(self.db))

    def tearDown(self):
        self.client.close()
        self.environment.stop()
        asyncio.run(self.db.close())
        self.tmp.cleanup()
        for transport in self.transports:
            transport.assert_not_awaited()

    def test_event_stream_requires_valid_operator_or_compute_session(self):
        async def finite_stream(*_args, **_kwargs):
            yield ": keep-alive\n\n"

        for role in ("pm", "task", "orchestrator"):
            asyncio.run(self.db.insert_pi_session(PiSession(
                id=role, role=role, state=PiSessionState.IDLE,
                token_hash=hashlib.sha256(role.encode()).hexdigest(),
            )))
        with patch("worker_harness.slurm.stream_job_events", new=finite_stream):
            path = "/api/v1/job-events?recipient_id=client&after=0"
            self.assertEqual(self.client.get(path).status_code, 401)
            self.assertEqual(self.client.get(path, headers={"Authorization": "Bearer wrong"}).status_code, 401)
            for token in ("o" * 43, "pm", "task"):
                response = self.client.get(path, headers={"Authorization": f"Bearer {token}"})
                self.assertEqual(response.status_code, 200, response.text)
                self.assertIn("text/event-stream", response.headers["content-type"])
                self.assertEqual(response.headers["cache-control"], "no-cache")
            self.assertEqual(self.client.get(path, headers={
                "Authorization": "Bearer orchestrator"}).status_code, 403)
            self.assertEqual(self.client.get(path.replace("after=0", "after=-1"), headers={
                "Authorization": "Bearer pm"}).status_code, 422)
