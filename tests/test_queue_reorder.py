"""Shared preview/commit reordering, revisions, estimates and atomicity."""

from __future__ import annotations

import asyncio
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from worker_harness.db import AuditActor, Database, QueueConflict
from worker_harness.heartbeat import create_app
from worker_harness.models import GPUInfo, Job, JobStatus, WorkerRegistration
from worker_harness.ssh import SSHResult

from identity_support import bearer, bootstrap, create_user, issue_key, login


def registration(worker_id: str, gpu_count: int = 1, busy: list[bool] | None = None) -> WorkerRegistration:
    busy = busy or [False] * gpu_count
    return WorkerRegistration(
        worker_id=worker_id, name=f"worker-{worker_id}", worker_ip="100.64.0.2", gpu_count=gpu_count,
        gpus=[GPUInfo(index=i, name="GPU", vram_total_gb=24, vram_used_gb=0, busy=busy[i]) for i in range(gpu_count)],
    )


class ReorderApiTests(unittest.TestCase):
    """One GPU: R has 600 s left; pending A=1200 s (Alice) then B=300 s (Bob)."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.loop = asyncio.new_event_loop()
        self.db = Database(Path(self.tmp.name) / "db.sqlite")
        self.wait(self.db.connect())
        self.wait(self.db.upsert_worker(registration("w1")))
        self.wait(self.db.upsert_worker(registration("w2")))
        self.app = create_app(self.db)
        self.wait(bootstrap(self.db))
        admin = TestClient(self.app)
        admin_headers = login(admin, "admin")
        self.users, self.keys = {}, {}
        for name in ("alice", "bob", "carol"):
            self.users[name] = create_user(admin, admin_headers, name)
            browser = TestClient(self.app)
            self.keys[name] = bearer(issue_key(browser, login(browser, name)))
        self.client = TestClient(self.app)
        launch = patch(
            "worker_harness.job.ssh_job_launch",
            new=AsyncMock(return_value=SSHResult(stdout="started", stderr="", returncode=0)),
        )
        launch.start()
        self.addCleanup(launch.stop)
        self.running = self.enqueue("carol", "R", 1200)
        # R started 600 s ago: 600 s remain.
        self.wait(self.db._db.execute(
            "UPDATE jobs SET started_at=? WHERE id=?", (int(time.time()) - 600, self.running["id"]),
        ))
        self.wait(self.db._db.commit())
        self.job_a = self.enqueue("alice", "A", 1200)
        self.job_b = self.enqueue("bob", "B", 300)

    def tearDown(self) -> None:
        self.wait(self.db.close())
        self.loop.close()
        self.tmp.cleanup()

    def wait(self, coroutine):
        return self.loop.run_until_complete(coroutine)

    def enqueue(self, user: str, name: str, seconds: int, worker_id: str = "w1") -> dict:
        response = self.client.post("/api/v1/jobs/queue", headers=self.keys[user], json={
            "worker_id": worker_id, "command": f"run {name}", "name": name,
            "expected_seconds": seconds, "gpu_count": 1,
        })
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()

    def worker_board(self, worker_id: str = "w1") -> dict:
        board = self.client.get("/api/v1/jobs/queue", headers=self.keys["carol"]).json()
        return next(item for item in board["workers"] if item["worker_id"] == worker_id)

    def preview(self, user: str, job_id: str, before: str | None, revision: int, reason: str = "deadline"):
        return self.client.post(
            f"/api/v1/jobs/{job_id}/queue/preview", headers=self.keys[user],
            json={"before_job_id": before, "expected_revision": revision, "reason": reason},
        )

    def test_preview_attributes_delay_and_commit_moves_exactly_once(self) -> None:
        board = self.worker_board()
        estimates = {row["id"]: row for row in board["jobs"]}
        now = int(time.time())
        self.assertAlmostEqual(estimates[self.job_a["id"]]["estimated_start_at"], now + 600, delta=3)
        self.assertAlmostEqual(estimates[self.job_b["id"]]["estimated_start_at"], now + 1800, delta=3)

        response = self.preview("carol", self.job_b["id"], self.job_a["id"], board["revision"], "paper deadline")
        self.assertEqual(response.status_code, 200, response.text)
        preview = response.json()
        self.assertTrue(preview["changed"])
        self.assertEqual(preview["after_order"], [self.job_b["id"], self.job_a["id"]])
        impacts = {impact["job_id"]: impact for impact in preview["impacts"]}
        self.assertEqual(impacts[self.job_a["id"]]["delay_seconds"], 300)
        self.assertEqual(impacts[self.job_b["id"]]["delay_seconds"], -1200)
        self.assertEqual(impacts[self.job_a["id"]]["owner_display_name"], "Alice")
        # Previews never change order.
        self.assertEqual([row["id"] for row in self.worker_board()["jobs"][1:]], [self.job_a["id"], self.job_b["id"]])

        commit = self.client.post(
            f"/api/v1/jobs/{self.job_b['id']}/queue/move", headers=self.keys["carol"],
            json={"preview_id": preview["preview_id"], "expected_revision": preview["revision"]},
        )
        self.assertEqual(commit.status_code, 200, commit.text)
        event = commit.json()["event"]
        self.assertEqual(event["actor_display_name"], "Carol")
        self.assertEqual(event["job_owner_display_name"], "Bob")
        self.assertEqual(event["reason"], "paper deadline")
        self.assertNotIn("run B", commit.text)
        repeat = self.client.post(
            f"/api/v1/jobs/{self.job_b['id']}/queue/move", headers=self.keys["carol"],
            json={"preview_id": preview["preview_id"], "expected_revision": preview["revision"]},
        )
        self.assertEqual(repeat.json()["event"]["id"], event["id"])
        history = self.client.get("/api/v1/jobs/queue/history", headers=self.keys["alice"]).json()["events"]
        self.assertEqual(len([item for item in history if item["event_type"] == "reorder"]), 1)

        board = self.worker_board()
        self.assertEqual([row["id"] for row in board["jobs"][1:]], [self.job_b["id"], self.job_a["id"]])
        delayed = next(row for row in board["jobs"] if row["id"] == self.job_a["id"])
        self.assertEqual(delayed["delay_history"][0]["delay_seconds"], 300)
        self.assertEqual(delayed["delay_history"][0]["moved_job_name"], "B")
        self.assertEqual(delayed["delay_history"][0]["actor_display_name"], "Carol")
        owned = self.client.get(f"/api/v1/jobs/{self.job_a['id']}", headers=self.keys["alice"]).json()
        self.assertEqual(owned["delay_history"][0]["reason"], "paper deadline")

    def test_stale_and_expired_previews_never_partially_apply(self) -> None:
        revision = self.worker_board()["revision"]
        first = self.preview("carol", self.job_b["id"], self.job_a["id"], revision).json()
        second = self.preview("alice", self.job_a["id"], None, revision).json()
        self.assertEqual(
            self.client.post(
                f"/api/v1/jobs/{self.job_b['id']}/queue/move", headers=self.keys["carol"],
                json={"preview_id": first["preview_id"], "expected_revision": revision},
            ).status_code,
            200,
        )
        stale = self.client.post(
            f"/api/v1/jobs/{self.job_a['id']}/queue/move", headers=self.keys["alice"],
            json={"preview_id": second["preview_id"], "expected_revision": revision},
        )
        self.assertEqual(stale.status_code, 409)
        self.assertEqual(stale.json()["detail"]["code"], "QUEUE_CHANGED")
        events = self.wait(self.db.list_queue_events(worker_id="w1"))
        self.assertEqual(len([event for event in events if event["event_type"] == "reorder"]), 1)

        board = self.worker_board()
        expiring = self.preview("alice", self.job_a["id"], self.job_b["id"], board["revision"]).json()
        self.wait(self.db._db.execute("UPDATE queue_previews SET expires_at=0 WHERE id=?", (expiring["preview_id"],)))
        self.wait(self.db._db.commit())
        expired = self.client.post(
            f"/api/v1/jobs/{self.job_a['id']}/queue/move", headers=self.keys["alice"],
            json={"preview_id": expiring["preview_id"], "expected_revision": board["revision"]},
        )
        self.assertEqual(expired.json()["detail"]["code"], "PREVIEW_EXPIRED")
        self.assertEqual([row["id"] for row in self.worker_board()["jobs"][1:]], [self.job_b["id"], self.job_a["id"]])

    def test_previews_bind_to_the_credential_that_created_them(self) -> None:
        revision = self.worker_board()["revision"]
        preview = self.preview("carol", self.job_b["id"], self.job_a["id"], revision).json()
        other_user = self.client.post(
            f"/api/v1/jobs/{self.job_b['id']}/queue/move", headers=self.keys["bob"],
            json={"preview_id": preview["preview_id"], "expected_revision": revision},
        )
        self.assertEqual(other_user.status_code, 404)
        carol_browser = TestClient(self.app)
        headers = login(carol_browser, "carol")
        same_user_other_credential = carol_browser.post(
            f"/api/v1/jobs/{self.job_b['id']}/queue/move", headers=headers,
            json={"preview_id": preview["preview_id"], "expected_revision": revision},
        )
        self.assertEqual(same_user_other_credential.status_code, 404)

    def test_preview_validation(self) -> None:
        revision = self.worker_board()["revision"]
        other = self.enqueue("bob", "elsewhere", 60, worker_id="w2")
        other_pending = self.enqueue("bob", "elsewhere-2", 60, worker_id="w2")
        self.assertEqual(self.preview("carol", self.job_a["id"], self.job_a["id"], revision).status_code, 422)
        self.assertEqual(self.preview("carol", self.job_a["id"], other_pending["id"], revision).status_code, 422)
        self.assertEqual(self.preview("carol", self.job_a["id"], "missing", revision).status_code, 422)
        self.assertEqual(self.preview("carol", self.job_a["id"], None, revision, reason="  ").status_code, 422)
        self.assertEqual(self.preview("carol", self.job_a["id"], None, revision - 1).status_code, 409)
        self.assertEqual(self.preview("carol", self.running["id"], None, revision).status_code, 409)
        self.assertEqual(self.preview("carol", other["id"], None, 0).status_code, 409)
        no_op = self.preview("carol", self.job_b["id"], None, revision)
        self.assertEqual(no_op.json(), {"changed": False})

    def test_revision_tracks_scheduling_changes_but_not_heartbeats_or_priority(self) -> None:
        revision = self.worker_board()["revision"]
        self.wait(self.db.upsert_worker(registration("w1")))
        self.assertEqual(self.worker_board()["revision"], revision)
        priority = self.client.patch(
            f"/api/v1/jobs/{self.job_b['id']}/queue", headers=self.keys["bob"],
            json={"priority": "time_critical", "priority_note": "camera ready"},
        )
        self.assertEqual(priority.status_code, 200)
        board = self.worker_board()
        self.assertEqual(board["revision"], revision)
        self.assertEqual([row["id"] for row in board["jobs"][1:]], [self.job_a["id"], self.job_b["id"]])
        estimate = self.client.patch(
            f"/api/v1/jobs/{self.job_b['id']}/queue", headers=self.keys["bob"], json={"expected_seconds": 200},
        )
        self.assertEqual(estimate.status_code, 200)
        self.assertGreater(self.worker_board()["revision"], revision)
        revision = self.worker_board()["revision"]
        self.enqueue("alice", "C", 60)
        self.assertGreater(self.worker_board()["revision"], revision)


class EstimateBoardTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.tmp.name) / "db.sqlite")
        await self.db.connect()
        self.owner = (await bootstrap(self.db))["id"]

    async def asyncTearDown(self) -> None:
        await self.db.close()
        self.tmp.cleanup()

    def job(self, name: str, gpus: int, seconds: int) -> Job:
        return Job(
            worker_id="w1", name=name, command="x", expected_seconds=seconds, gpu_count=gpus,
            owner_user_id=self.owner, submitted_by_actor_id="api_key:test",
        )

    async def test_unknown_head_makes_every_later_job_unknown(self) -> None:
        from worker_harness.queue_board import forecast_worker

        # GPU 1 is busy outside the harness with an unknown finish.
        worker = await self.db.upsert_worker(registration("w1", 2, busy=[False, True]))
        a = await self.db.insert_queued_job(self.job("A", 2, 60))
        b = await self.db.insert_queued_job(self.job("B", 1, 60))
        estimates = forecast_worker(worker, await self.db.list_queued_jobs("w1"), int(time.time()))
        self.assertEqual(estimates[a.id].status, "unknown")
        self.assertEqual(estimates[b.id].status, "unknown")
        self.assertIsNone(estimates[b.id].estimated_start_at)

    async def test_known_one_gpu_head_before_unknown_job(self) -> None:
        from worker_harness.queue_board import forecast_worker

        worker = await self.db.upsert_worker(registration("w1", 2, busy=[False, True]))
        head = await self.db.insert_queued_job(self.job("head", 1, 60))
        later = await self.db.insert_queued_job(self.job("A", 2, 60))
        estimates = forecast_worker(worker, await self.db.list_queued_jobs("w1"), int(time.time()))
        self.assertEqual(estimates[head.id].status, "known")
        self.assertEqual(estimates[later.id].status, "unknown")


class CrossConnectionAtomicityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        path = Path(self.tmp.name) / "db.sqlite"
        self.first = Database(path)
        self.second = Database(path)
        await self.first.connect()
        await self.second.connect()
        self.owner = (await bootstrap(self.first))["id"]
        await self.first.upsert_worker(registration("w1", 2))
        self.actor = AuditActor(user_id=self.owner, display_name="admin", actor_id="api_key:x")

    async def asyncTearDown(self) -> None:
        await self.first.close()
        await self.second.close()
        self.tmp.cleanup()

    async def enqueue(self, name: str) -> Job:
        return await self.first.insert_queued_job(Job(
            worker_id="w1", name=name, command="x", expected_seconds=60, gpu_count=1,
            owner_user_id=self.owner, submitted_by_actor_id="api_key:x",
        ))

    async def test_two_connections_cannot_claim_overlapping_gpus(self) -> None:
        head = await self.enqueue("head")
        await self.enqueue("next")
        now = int(time.time())
        results = await asyncio.gather(
            self.first.claim_queued_job(head.id, [0], now, worker_id="w1"),
            self.second.claim_queued_job(head.id, [0], now, worker_id="w1"),
        )
        self.assertEqual(sum(result is not None for result in results), 1)
        self.assertEqual(await self.first.running_queue_gpu_indices("w1"), {0})

    async def test_claim_racing_a_reorder_commit_is_serialized(self) -> None:
        a = await self.enqueue("A")
        b = await self.enqueue("B")
        revision = (await self.first.queue_revisions())["w1"]
        preview = await self.first.create_queue_preview(
            b.id, before_job_id=a.id, expected_revision=revision, reason="urgent",
            actor=self.actor, now=int(time.time()),
        )

        async def commit():
            try:
                return await self.second.commit_queue_move(
                    b.id, preview_id=preview["preview_id"], expected_revision=revision,
                    actor=self.actor, now=int(time.time()),
                )
            except QueueConflict as exc:
                return exc

        claim, committed = await asyncio.gather(
            self.first.claim_queued_job(a.id, [0], int(time.time()), worker_id="w1"), commit(),
        )
        events = await self.first.list_queue_events(worker_id="w1")
        if claim is not None:
            # The claim won: the stale preview must not apply or audit.
            self.assertIsInstance(committed, QueueConflict)
            self.assertEqual(events, [])
            self.assertEqual((await self.first.get_job(b.id)).queue_order, 2)
        else:
            # The move won: A is no longer the head, so its claim failed.
            self.assertNotIsInstance(committed, QueueConflict)
            self.assertEqual(len(events), 1)
            self.assertEqual((await self.first.get_job(b.id)).queue_order, 1)


if __name__ == "__main__":
    unittest.main()
