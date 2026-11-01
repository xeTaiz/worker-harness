"""Private job ownership on a shared public board."""

from __future__ import annotations

import asyncio
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from worker_harness.db import Database
from worker_harness.heartbeat import create_app
from worker_harness.models import GPUInfo, Job, JobStatus, WorkerRegistration, WorkerStatus
from worker_harness.ssh import JobProbe, SSHResult

from identity_support import bearer, bootstrap, create_user, issue_key, login

SECRET_COMMAND = "python train.py --token=supersecret"


def registration(worker_id: str, gpu_count: int = 1) -> WorkerRegistration:
    return WorkerRegistration(
        worker_id=worker_id, name=f"worker-{worker_id}", worker_ip="100.64.0.2", gpu_count=gpu_count,
        gpus=[GPUInfo(index=i, name="GPU", vram_total_gb=24, vram_used_gb=0, busy=False) for i in range(gpu_count)],
    )


class SharedBoardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.loop = asyncio.new_event_loop()
        self.db = Database(Path(self.tmp.name) / "db.sqlite")
        self.wait(self.db.connect())
        self.wait(self.db.upsert_worker(registration("w1")))
        self.wait(self.db.upsert_worker(registration("w2")))
        self.app = create_app(self.db)
        self.wait(bootstrap(self.db))
        self.admin = TestClient(self.app)
        self.admin_headers = login(self.admin, "admin")
        self.users = {}
        self.keys = {}
        for name in ("alice", "bob"):
            self.users[name] = create_user(self.admin, self.admin_headers, name)
            browser = TestClient(self.app)
            self.keys[name] = bearer(issue_key(browser, login(browser, name)))
        self.client = TestClient(self.app)
        self.launch = AsyncMock(return_value=SSHResult(stdout="started", stderr="", returncode=0))
        patcher = patch("worker_harness.job.ssh_job_launch", new=self.launch)
        patcher.start()
        self.addCleanup(patcher.stop)

    def tearDown(self) -> None:
        self.wait(self.db.close())
        self.loop.close()
        self.tmp.cleanup()

    def wait(self, coroutine):
        return self.loop.run_until_complete(coroutine)

    def enqueue(self, user: str, name: str, **extra) -> dict:
        body = {"worker_id": "w1", "command": SECRET_COMMAND, "name": name, "expected_seconds": 600, "gpu_count": 1}
        body.update(extra)
        response = self.client.post("/api/v1/jobs/queue", json=body, headers=self.keys[user])
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()

    def board(self, user: str) -> dict:
        response = self.client.get("/api/v1/jobs/queue", headers=self.keys[user])
        self.assertEqual(response.status_code, 200)
        return response.json()

    def test_other_users_see_public_rows_but_never_private_details(self) -> None:
        running = self.enqueue("alice", "alice-train")
        pending = self.enqueue("alice", "alice-eval", priority="blocking")
        self.assertEqual((running["status"], pending["status"]), ("running", "pending"))
        board = self.client.get("/api/v1/jobs/queue", headers=self.keys["bob"])
        self.assertNotIn("supersecret", board.text)
        rows = {row["id"]: row for worker in board.json()["workers"] for row in worker["jobs"]}
        self.assertEqual(rows[pending["id"]]["owner_display_name"], "Alice")
        self.assertEqual(rows[pending["id"]]["position"], 1)
        self.assertEqual(rows[pending["id"]]["priority"], "blocking")
        self.assertNotIn("command", rows[pending["id"]])
        for method, path, body in (
            ("get", f"/api/v1/jobs/{pending['id']}", None),
            ("get", f"/api/v1/jobs/{running['id']}/logs", None),
            ("patch", f"/api/v1/jobs/{pending['id']}/queue", {"expected_seconds": 1}),
            ("patch", f"/api/v1/jobs/{running['id']}", {"expected_seconds": 1}),
            ("delete", f"/api/v1/jobs/{running['id']}", None),
        ):
            with self.subTest(method=method, path=path):
                kwargs = {"json": body} if body is not None else {}
                response = self.client.request(method.upper(), path, headers=self.keys["bob"], **kwargs)
                self.assertEqual(response.status_code, 404)
        self.assertEqual(self.client.get("/api/v1/jobs", headers=self.keys["bob"]).json(), [])
        owned = self.client.get("/api/v1/jobs", headers=self.keys["alice"]).json()
        self.assertEqual({job["id"] for job in owned}, {running["id"], pending["id"]})
        self.assertTrue(all(job["command"] == SECRET_COMMAND for job in owned))
        # Administrators have no override over private jobs.
        self.assertEqual(self.admin.get(f"/api/v1/jobs/{pending['id']}").status_code, 404)
        stop = self.admin.delete(f"/api/v1/jobs/{running['id']}", headers=self.admin_headers)
        self.assertEqual(stop.status_code, 404)

    def test_owner_field_and_unknown_fields_are_rejected(self) -> None:
        body = {
            "worker_id": "w1", "command": "x", "name": "x", "expected_seconds": 60, "gpu_count": 1,
            "owner_user_id": self.users["bob"]["id"],
        }
        self.assertEqual(self.client.post("/api/v1/jobs/queue", json=body, headers=self.keys["alice"]).status_code, 422)
        moved = {"position": 1}
        job = self.enqueue("alice", "a")
        self.assertEqual(
            self.client.patch(f"/api/v1/jobs/{job['id']}/queue", json=moved, headers=self.keys["alice"]).status_code,
            422,
        )

    def test_submission_contract_validation(self) -> None:
        missing_note = {"worker_id": "w1", "command": "x", "name": "x", "expected_seconds": 60,
                        "gpu_count": 1, "priority": "time_critical"}
        self.assertEqual(
            self.client.post("/api/v1/jobs/queue", json=missing_note, headers=self.keys["alice"]).status_code, 422,
        )
        missing_name = {"worker_id": "w1", "command": "x", "expected_seconds": 60}
        self.assertEqual(self.client.post("/api/v1/jobs", json=missing_name, headers=self.keys["alice"]).status_code, 422)
        zero_duration = {"worker_id": "w1", "command": "x", "name": "x", "expected_seconds": 0}
        self.assertEqual(self.client.post("/api/v1/jobs", json=zero_duration, headers=self.keys["alice"]).status_code, 422)
        cpu_gpu = {"worker_id": "w1", "command": "x", "name": "x", "expected_seconds": 60, "gpu_count": 1}
        self.assertEqual(self.client.post("/api/v1/jobs", json=cpu_gpu, headers=self.keys["alice"]).status_code, 422)
        critical = self.enqueue("alice", "urgent", priority="time_critical", priority_note="paper deadline")
        self.assertEqual(critical["priority_note"], "paper deadline")

    def test_cpu_setup_runs_while_gpu_is_occupied_and_offline_cpu_is_refused(self) -> None:
        self.enqueue("alice", "gpu-holder")
        waiting = self.enqueue("bob", "gpu-waiter")
        cpu = self.client.post("/api/v1/jobs", headers=self.keys["bob"], json={
            "worker_id": "w1", "command": "pip install torch", "name": "setup", "expected_seconds": 120,
        })
        self.assertEqual(cpu.status_code, 200, cpu.text)
        self.assertEqual(cpu.json()["status"], "running")
        self.assertEqual(cpu.json()["gpu_count"], 0)
        self.assertEqual(self.launch.await_args.kwargs["gpu_indices"], [])
        board = self.board("alice")
        worker = next(item for item in board["workers"] if item["worker_id"] == "w1")
        self.assertEqual([row["id"] for row in worker["cpu_jobs"]], [cpu.json()["id"]])
        self.assertNotIn("pip install", str(board))
        self.assertEqual(self.wait(self.db.get_job(waiting["id"])).status, JobStatus.PENDING)

        self.wait(self.db.set_worker_status("w2", WorkerStatus.OFFLINE))
        offline = self.client.post("/api/v1/jobs", headers=self.keys["bob"], json={
            "worker_id": "w2", "command": "x", "name": "x", "expected_seconds": 60,
        })
        self.assertEqual(offline.status_code, 409)
        self.assertEqual(offline.json()["detail"]["code"], "WORKER_OFFLINE")
        queued_offline = self.client.post("/api/v1/jobs/queue", headers=self.keys["bob"], json={
            "worker_id": "w2", "command": "x", "name": "x", "expected_seconds": 60, "gpu_count": 1,
        })
        self.assertEqual(queued_offline.json()["status"], "pending")
        estimates = next(item for item in self.board("bob")["workers"] if item["worker_id"] == "w2")
        self.assertEqual(estimates["jobs"][0]["estimate_status"], "unknown")

    def test_owner_edits_and_worker_moves_are_audited_without_delay_attribution(self) -> None:
        self.enqueue("alice", "holder")
        pending = self.enqueue("alice", "mover")
        edit = self.client.patch(
            f"/api/v1/jobs/{pending['id']}/queue", json={"expected_seconds": 900, "name": "renamed"},
            headers=self.keys["alice"],
        )
        self.assertEqual(edit.status_code, 200, edit.text)
        self.assertEqual((edit.json()["expected_seconds"], edit.json()["name"]), (900, "renamed"))
        move = self.client.patch(
            f"/api/v1/jobs/{pending['id']}/queue", json={"worker_id": "w2"}, headers=self.keys["alice"],
        )
        self.assertEqual(move.json()["worker_id"], "w2")
        history = self.client.get(
            "/api/v1/jobs/queue/history", params={"job_id": pending["id"]}, headers=self.keys["bob"],
        ).json()["events"]
        self.assertEqual([event["event_type"] for event in history], ["worker_move", "job_updated"])
        self.assertTrue(all(event["impacts"] == [] for event in history))
        self.assertEqual(self.client.get(f"/api/v1/jobs/{pending['id']}", headers=self.keys["alice"]).json()["delay_history"], [])

    def test_running_owner_can_revise_only_estimates_and_priority(self) -> None:
        running = self.enqueue("alice", "train")
        ok = self.client.patch(
            f"/api/v1/jobs/{running['id']}", json={"expected_seconds": 7200, "deadline_at": None},
            headers=self.keys["alice"],
        )
        self.assertEqual(ok.status_code, 200)
        self.assertEqual(ok.json()["expected_seconds"], 7200)
        for body in ({"gpu_count": 2}, {"worker_id": "w2"}, {"command": "rm -rf /"}):
            response = self.client.patch(f"/api/v1/jobs/{running['id']}", json=body, headers=self.keys["alice"])
            self.assertEqual(response.status_code, 422)

    def test_stop_reports_success_only_after_confirmed_termination(self) -> None:
        running = self.enqueue("alice", "train")
        waiting = self.enqueue("bob", "next")
        with patch("worker_harness.job.ssh_job_stop", new=AsyncMock(return_value=None)):
            unconfirmed = self.client.delete(f"/api/v1/jobs/{running['id']}", headers=self.keys["alice"])
        self.assertEqual(unconfirmed.status_code, 503)
        held = self.wait(self.db.get_job(running["id"]))
        self.assertEqual((held.status, held.execution_state.value), (JobStatus.RUNNING, "stopping"))
        self.assertEqual(self.wait(self.db.get_job(waiting["id"])).status, JobStatus.PENDING)
        with patch(
            "worker_harness.job.ssh_job_stop",
            new=AsyncMock(return_value=JobProbe(state="finished", exit_code=143, termination_reason="user_cancelled")),
        ), patch("worker_harness.job.ssh_read_log", new=AsyncMock(return_value="")):
            confirmed = self.client.delete(f"/api/v1/jobs/{running['id']}", headers=self.keys["alice"])
        self.assertEqual(confirmed.status_code, 200, confirmed.text)
        stopped = self.wait(self.db.get_job(running["id"]))
        self.assertEqual((stopped.status, stopped.termination_reason.value), (JobStatus.FAILED, "user_cancelled"))
        self.assertEqual(self.wait(self.db.get_job(waiting["id"])).status, JobStatus.RUNNING)

    def _stream_until_revoked(self, job_id: str, log: bytearray, grow, *, tail: int) -> str:
        """Stream a job log whose content ``grow`` extends on every read.

        The test transport buffers the body, so the (otherwise endless) stream
        is ended by revoking the key from a separate SQLite connection.
        """
        import sqlite3
        import threading

        async def read_range(_worker, _job_id, start, **_kwargs):
            grow(log)
            first = max(0, len(log) - (1 << 20)) if start is None else min(start, len(log))
            return first, len(log), bytes(log[first:])

        def revoke() -> None:
            with sqlite3.connect(self.db.path) as conn:
                conn.execute("UPDATE api_keys SET revoked_at=?", (int(time.time()),))

        timer = threading.Timer(0.6, revoke)
        timer.start()
        self.addCleanup(timer.cancel)
        with patch("worker_harness.job.ssh_read_log_range", new=AsyncMock(side_effect=read_range)):
            response = self.client.get(
                f"/api/v1/jobs/{job_id}/logs/stream", params={"poll_seconds": 0.05, "tail": tail},
                headers=self.keys["alice"], timeout=10,
            )
        self.assertEqual(response.status_code, 200)
        return response.text

    def test_log_stream_ends_when_the_key_is_revoked(self) -> None:
        running = self.enqueue("alice", "train")
        text = self._stream_until_revoked(running["id"], bytearray(b"line\n"), lambda log: None, tail=50)
        self.assertEqual(text, "line\n")

    def test_log_stream_keeps_emitting_after_the_log_outgrows_tail(self) -> None:
        running = self.enqueue("alice", "train")
        counter = iter(range(10_000))

        def grow(log: bytearray) -> None:
            index = next(counter)
            # Finish the previous read's partial line, add a whole line, and
            # leave a new partial line to be completed by the next read.
            if index:
                log.extend(b"ial %d\n" % (index - 1))
            log.extend(b"step %d\npart" % index)

        initial = bytearray(b"".join(b"old %d\n" % index for index in range(5)))
        text = self._stream_until_revoked(running["id"], initial, grow, tail=2)
        lines = text.splitlines()
        # Initial window is the last two complete lines; afterwards every line.
        self.assertEqual(lines[:4], ["old 4", "step 0", "partial 0", "step 1"])
        steps = [line for line in lines if line.startswith("step ")]
        self.assertGreater(len(steps), 5)  # far beyond tail=2
        self.assertEqual(steps, [f"step {index}" for index in range(len(steps))])
        partials = [line for line in lines if line.startswith("part")]
        self.assertEqual(partials, [f"partial {index}" for index in range(len(partials))])
        self.assertNotIn("old 3", lines)

    def test_rate_limit_is_per_credential_and_ignores_polled_reads(self) -> None:
        # Every request here arrives from the same peer, like browsers behind nginx.
        for _ in range(120):
            self.assertEqual(self.client.get("/api/v1/jobs/queue", headers=self.keys["alice"]).status_code, 200)
        for _ in range(50):
            self.assertEqual(
                self.client.get("/api/v1/jobs/missing/logs", headers=self.keys["alice"]).status_code, 404,
            )
        self.assertEqual(self.client.get("/api/v1/jobs/missing/logs", headers=self.keys["alice"]).status_code, 429)
        self.assertEqual(self.client.get("/api/v1/jobs/missing/logs", headers=self.keys["bob"]).status_code, 404)

    def test_usage_is_allocated_time_per_owner_and_worker(self) -> None:
        now = int(time.time())
        alice = self.users["alice"]["id"]
        for job in (
            Job(worker_id="w1", command="x", status=JobStatus.DONE, gpu_count=2, queue_managed=True,
                started_at=now - 300, finished_at=now - 180, owner_user_id=alice, submitted_by_actor_id="api_key:a"),
            Job(worker_id="w1", command="x", status=JobStatus.DONE, gpu_count=0,
                started_at=now - 100, finished_at=now - 70, owner_user_id=alice, submitted_by_actor_id="api_key:a"),
            Job(worker_id="w1", command="x", status=JobStatus.FAILED, gpu_count=1, queue_managed=True,
                started_at=0, finished_at=now, owner_user_id=alice, submitted_by_actor_id="api_key:a"),
        ):
            self.wait(self.db.insert_job(job))
        first = self.client.get("/api/v1/usage", headers=self.keys["bob"]).json()
        second = self.client.get("/api/v1/usage", headers=self.keys["bob"]).json()
        rows = [row for row in first["rows"] if row["owner_user_id"] == alice]
        self.assertEqual(len(rows), 1)
        self.assertEqual(
            (rows[0]["allocated_gpu_seconds"], rows[0]["cpu_job_seconds"], rows[0]["job_seconds"]), (240, 30, 150),
        )
        self.assertFalse(rows[0]["provisional"])
        self.assertEqual(first["rows"], second["rows"])
        self.wait(self.db.delete_worker("w1"))
        after_prune = self.client.get("/api/v1/usage", headers=self.keys["bob"]).json()["rows"]
        self.assertEqual(after_prune[0]["allocated_gpu_seconds"], 240)
        self.assertEqual(after_prune[0]["worker_name"], "worker-w1")


if __name__ == "__main__":
    unittest.main()
