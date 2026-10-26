"""Reserved allocation API, authentication, admission and readiness regressions."""

import asyncio
import copy
import hashlib
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from worker_harness.db import Database
from worker_harness.heartbeat import create_app, create_registration_app
from worker_harness.models import JobStatus, WorkerRegistration
from worker_harness.ssh import SSHResult


OPERATOR_TOKEN = "o" * 43
WORKER_TOKEN = "allocation-scoped-token"


def reservation(request_id="request-one", *, gpu=False):
    return {
        "request_id": request_id, "name": "allocation-one",
        "gpu_count": 1 if gpu else 0, "recipient_id": "client-one",
        "registration_token_hash": hashlib.sha256(WORKER_TOKEN.encode()).hexdigest(),
        "slurm": {
            "cluster_id": "test-gpu" if gpu else "test-cpu", "storage_id": "test",
            "compute_kind": "gpu" if gpu else "cpu", "cpus": 2,
            "memory_mb": 4096, "walltime_seconds": 600,
            "failure_grace_seconds": 60, "warning_seconds": 120,
            "termination_grace_seconds": 10,
        },
    }


def registration(worker_id, *, gpu=False):
    return {
        "worker_id": worker_id, "name": "allocation-one", "worker_ip": "100.64.0.2",
        "gpu_count": 1 if gpu else 0, "cpu_cores": 2, "total_ram_gb": 4,
        "gpus": [{"index": 0, "name": "GPU", "vram_total_gb": 24,
                  "vram_used_gb": 0, "busy": False, "uuid": "GPU-unique"}] if gpu else [],
        "slurm": {},
    }


def runner_result(exit_code=0):
    return {"exit_code": exit_code, "descendants_exited": True,
            "finished_at": time.time(), "finished_monotonic": 1.0, "boot_id": "boot-one"}


async def successful_probe(_worker, command, **_kwargs):
    return SSHResult(stdout=command.split("'")[-2], stderr="", returncode=0)


class SlurmWorkerApiTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.tmp.name) / "test.sqlite")
        asyncio.run(self.db.connect())
        self.environment = patch.dict("os.environ", {"WH_OPERATOR_TOKEN": OPERATOR_TOKEN})
        self.environment.start()
        # Mock expected probes/launches at their call sites. Any missed path is
        # blocked at both central transports, never sent to a real fleet host.
        self.ssh_transport = AsyncMock(side_effect=AssertionError("unexpected SSH transport"))
        self.ssh_bytes_transport = AsyncMock(side_effect=AssertionError("unexpected SSH byte transport"))
        for target, transport in (("_exec_ssh", self.ssh_transport),
                                  ("_exec_ssh_bytes", self.ssh_bytes_transport)):
            boundary = patch(f"worker_harness.ssh.{target}", new=transport)
            boundary.start()
            self.addCleanup(boundary.stop)
        self.runner_stage = AsyncMock(
            return_value=SSHResult(stdout="staged", stderr="", returncode=0),
        )
        staging = patch("worker_harness.heartbeat.ssh_stage_job_runner", new=self.runner_stage)
        staging.start()
        self.addCleanup(staging.stop)
        self.control = TestClient(create_app(self.db))
        self.register = TestClient(create_registration_app(self.db))
        self.headers = {"Authorization": f"Bearer {OPERATOR_TOKEN}"}
        self.worker_headers = {"Authorization": f"Bearer {WORKER_TOKEN}"}

    def tearDown(self):
        self.control.close()
        self.register.close()
        self.environment.stop()
        asyncio.run(self.db.close())
        self.tmp.cleanup()
        self.ssh_transport.assert_not_awaited()
        self.ssh_bytes_transport.assert_not_awaited()

    def reserve(self, *, gpu=False, request_id="request-one"):
        response = self.control.post("/api/v1/workers/reservations",
                                     json=reservation(request_id, gpu=gpu), headers=self.headers)
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()["worker_id"]

    def enqueue(self, worker_id, *, key="request-one", gpu_count=0, command="true"):
        body = {"worker_id": worker_id, "command": command, "name": "task",
                "expected_seconds": 60, "gpu_count": gpu_count}
        if key is not None:
            body["submission_key"] = key
        return self.control.post("/api/v1/jobs/queue", json=body, headers=self.headers)

    def observe(self, worker_id, **values):
        return self.control.patch(f"/api/v1/workers/{worker_id}/slurm",
                                  json={"observed_at": time.time(), **values}, headers=self.headers)

    def test_new_routes_require_real_credentials_even_anonymous_mode(self):
        with patch.dict("os.environ", {"WH_OPERATOR_TOKEN": "", "WH_OPERATOR_TOKEN_FILE": ""}):
            anonymous = TestClient(create_app(self.db))
        try:
            for method, path, body in (
                ("POST", "/api/v1/workers/reservations", reservation()),
                ("PATCH", "/api/v1/workers/missing/slurm", {"observed_at": time.time()}),
                ("GET", "/api/v1/job-events?recipient_id=client-one", None),
            ):
                self.assertEqual(anonymous.request(method, path, json=body).status_code, 401)
                self.assertEqual(anonymous.request(method, path, json=body,
                                 headers=self.worker_headers).status_code, 401)
        finally:
            anonymous.close()

    def test_reservation_and_initial_queue_are_idempotent_and_offline(self):
        worker_id = self.reserve()
        self.assertEqual(self.reserve(), worker_id)
        changed = reservation()
        changed["slurm"]["cpus"] = 3
        self.assertEqual(self.control.post("/api/v1/workers/reservations", json=changed,
                                          headers=self.headers).status_code, 409)
        first = self.enqueue(worker_id)
        self.assertEqual(first.status_code, 200, first.text)
        self.assertEqual(first.json()["status"], "pending")
        self.assertEqual(self.enqueue(worker_id).json()["id"], first.json()["id"])
        self.assertEqual(self.enqueue(worker_id, command="different").status_code, 409)
        worker = self.control.get(f"/api/v1/workers/{worker_id}", headers=self.headers).json()
        self.assertEqual(worker["status"], "offline")
        self.assertEqual(worker["slurm"]["initial_job_id"], first.json()["id"])
        self.assertEqual(first.json()["notify_recipient_id"], "client-one")
        self.assertNotIn("registration_token_hash", str(worker))

    def test_scoped_registration_requires_auth_mapping_and_real_ssh(self):
        worker_id = self.reserve(gpu=True)
        queued = self.enqueue(worker_id, gpu_count=1).json()
        payload = registration(worker_id, gpu=True)
        for headers in ({}, self.headers, {"Authorization": "Bearer wrong"}):
            self.assertEqual(self.register.post("/register", json=payload,
                                               headers=headers).status_code, 401)
        for field, value in (("name", "changed"), ("gpu_count", 2), ("cpu_cores", 3),
                             ("total_ram_gb", 8), ("gpus", [])):
            invalid = copy.deepcopy(payload)
            invalid[field] = value
            result = self.register.post("/register", json=invalid, headers=self.worker_headers)
            self.assertIn(result.status_code, (409, 422), result.text)
        invalid = copy.deepcopy(payload)
        invalid["gpus"][0]["uuid"] = ""
        self.assertIn(self.register.post("/register", json=invalid,
                                        headers=self.worker_headers).status_code, (409, 422))
        launch = AsyncMock(return_value=SSHResult(stdout="started", stderr="", returncode=0))
        with patch("worker_harness.heartbeat.async_ssh_run", new=AsyncMock(
            return_value=SSHResult(stdout="", stderr="not ready", returncode=255))), \
             patch("worker_harness.job.ssh_tmux_new", new=launch):
            response = self.register.post("/register", json=payload, headers=self.worker_headers)
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()["worker_status"], "offline")
            self.assertEqual(response.json()["queued_jobs"][0]["id"], queued["id"])
            launch.assert_not_awaited()
            self.runner_stage.assert_not_awaited()
        with patch("worker_harness.heartbeat.async_ssh_run", new=successful_probe), \
             patch("worker_harness.job.ssh_tmux_new", new=launch):
            response = self.register.post("/register", json=payload, headers=self.worker_headers)
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()["worker_status"], "online")
            self.assertEqual(response.json()["running_jobs"][0]["id"], queued["id"])
            self.register.post("/register", json=payload, headers=self.worker_headers)
        launch.assert_awaited_once()
        self.runner_stage.assert_awaited_once()

    def test_legacy_report_and_retention_protect_reserved_workers(self):
        worker_id = self.reserve()
        queued = self.enqueue(worker_id).json()
        response = self.register.post(f"/pi/worker/{worker_id}/jobs", json={"jobs": []})
        self.assertEqual(response.status_code, 403)
        asyncio.run(self.db.prune_workers(int(time.time()) + 1000))
        self.assertIsNotNone(asyncio.run(self.db.get_worker(worker_id)))
        self.assertIsNotNone(asyncio.run(self.db.get_job(queued["id"])))
        with self.assertRaises(ValueError):
            asyncio.run(self.db.delete_worker(worker_id))

    def test_cpu_admission_and_frozen_initial_resource_edits(self):
        worker_id = self.reserve()
        initial = self.enqueue(worker_id).json()
        self.assertEqual(self.enqueue(worker_id, key=None, gpu_count=1).status_code, 422)
        ordinary = WorkerRegistration(worker_id="ordinary", name="ordinary", worker_ip="100.64.0.3")
        asyncio.run(self.db.upsert_worker(ordinary))
        self.assertEqual(self.enqueue("ordinary", key=None).status_code, 422)
        gpu_id = self.reserve(gpu=True, request_id="gpu-reservation")
        self.assertEqual(self.enqueue(gpu_id, key=None).status_code, 422)
        self.assertEqual(self.observe(worker_id, submission_attempted=True).status_code, 200)
        for body in ({"worker_id": gpu_id}, {"gpu_count": 1}):
            response = self.control.patch(f"/api/v1/jobs/{initial['id']}/queue",
                                          json=body, headers=self.headers)
            self.assertEqual(response.status_code, 409, response.text)
        self.assertEqual(self.control.patch(f"/api/v1/jobs/{initial['id']}/queue",
                         json={"name": "renamed"}, headers=self.headers).status_code, 200)

    def test_terminal_recovery_precedes_failure_and_closes_admission(self):
        worker_id = self.reserve()
        first = self.enqueue(worker_id).json()
        second = self.enqueue(worker_id, key=None, command="second").json()
        response = self.observe(worker_id, slurm_job_id="123", scheduler_state="COMPLETED",
                                recovered_results=[{"job_id": first["id"], "result": runner_result()}])
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(asyncio.run(self.db.get_job(first["id"])).status, JobStatus.DONE)
        self.assertEqual(asyncio.run(self.db.get_job(second["id"])).status, JobStatus.FAILED)
        self.assertEqual(self.observe(worker_id, slurm_job_id="124").status_code, 409)
        self.assertEqual(self.observe(worker_id, scheduler_state="RUNNING").status_code, 200)
        worker = asyncio.run(self.db.get_worker(worker_id))
        self.assertEqual(worker.slurm.scheduler_state, "COMPLETED")
        for path, body in (("/api/v1/jobs/queue", {"worker_id": worker_id, "command": "new",
                           "name": "new", "expected_seconds": 10, "gpu_count": 0}),
                           ("/api/v1/jobs", {"worker_id": worker_id, "command": "true"})):
            rejected = self.control.post(path, json=body, headers=self.headers)
            self.assertEqual(rejected.status_code, 409, rejected.text)
            self.assertEqual(rejected.json()["detail"]["code"], "SLURM_WORKER_FINISHED")

    def test_results_require_worker_linkage_and_proof(self):
        worker_id = self.reserve()
        other = self.reserve(request_id="other")
        foreign = self.enqueue(other, key="other").json()
        response = self.observe(worker_id, recovered_results=[
            {"job_id": foreign["id"], "result": runner_result()}])
        self.assertIn(response.status_code, (409, 422))
        result = runner_result()
        result["descendants_exited"] = False
        self.assertEqual(self.observe(other, recovered_results=[
            {"job_id": foreign["id"], "result": result}]).status_code, 422)
        self.assertEqual(asyncio.run(self.db.get_job(foreign["id"])).status, JobStatus.PENDING)

    def test_draining_heartbeat_cannot_reopen_worker(self):
        worker_id = self.reserve()
        queued = self.enqueue(worker_id).json()
        self.assertFalse(asyncio.run(self.db.drain_slurm_worker(worker_id)))
        recovered = self.observe(worker_id, recovered_results=[
            {"job_id": queued["id"], "result": runner_result()},
        ])
        self.assertEqual(recovered.status_code, 200, recovered.text)
        self.assertTrue(asyncio.run(self.db.drain_slurm_worker(worker_id)))
        response = self.register.post("/register", json=registration(worker_id),
                                      headers=self.worker_headers)
        self.assertEqual(response.status_code, 200, response.text)
        self.assertTrue(response.json()["draining"])
        self.assertEqual(self.enqueue(worker_id, key=None).status_code, 409)
        self.ssh_transport.assert_not_awaited()
        self.ssh_bytes_transport.assert_not_awaited()

    def test_heartbeat_failure_grace_signal_events_and_rerun(self):
        worker_id = self.reserve()
        first = self.enqueue(worker_id).json()
        payload = registration(worker_id)
        launch = AsyncMock(return_value=SSHResult(stdout="started", stderr="", returncode=0))
        with patch("worker_harness.heartbeat.async_ssh_run", new=successful_probe), \
             patch("worker_harness.job.ssh_tmux_new", new=launch):
            self.register.post("/register", json=payload, headers=self.worker_headers)
            result = runner_result(1)
            payload["slurm"] = {"results": [{"job_id": first["id"], "result": result}]}
            failed = self.register.post("/register", json=payload, headers=self.worker_headers)
            self.assertEqual(failed.status_code, 200, failed.text)
            self.assertEqual(failed.json()["failure_deadline"], result["finished_at"] + 60)
            self.assertEqual(failed.json()["worker_status"], "online")
            self.register.post("/register", json=payload, headers=self.worker_headers)
            events = asyncio.run(self.db.list_job_events("client-one"))
            self.assertEqual(sum(event.event_type == "job_failed" for event in events), 1)
            rerun = self.enqueue(worker_id, key=None, command="fixed")
            self.assertEqual(rerun.status_code, 200, rerun.text)
            self.assertEqual(rerun.json()["status"], "running")
            self.assertIsNone(asyncio.run(self.db.get_worker(worker_id)).slurm.failure_deadline)
            payload["slurm"] = {"checkpoint_requested": True, "draining": True}
            for _ in range(2):
                response = self.register.post("/register", json=payload, headers=self.worker_headers)
                self.assertEqual(response.status_code, 200, response.text)
                self.assertTrue(response.json()["draining"])
        events = asyncio.run(self.db.list_job_events("client-one"))
        self.assertEqual(sum(event.event_type == "worker_ready" for event in events), 1)
        self.assertEqual(sum(event.event_type == "checkpoint_requested" for event in events), 1)
        self.assertEqual(asyncio.run(self.db.get_job(first["id"])).status, JobStatus.FAILED)

    def test_submission_refusal_and_terminal_reassignment_reject_work(self):
        worker_id = self.reserve()
        pending = self.enqueue(worker_id).json()
        refused = self.observe(worker_id, submission_attempted=True, submission_failed=True)
        self.assertEqual(refused.status_code, 200, refused.text)
        self.assertEqual(asyncio.run(self.db.get_job(pending["id"])).status, JobStatus.FAILED)
        self.assertEqual(self.enqueue(worker_id, key=None).status_code, 409)
        other = self.reserve(request_id="other")
        waiting = self.enqueue(other, key="other").json()
        response = self.control.patch(f"/api/v1/jobs/{waiting['id']}/queue",
                                      json={"worker_id": worker_id}, headers=self.headers)
        self.assertEqual(response.status_code, 409, response.text)
        self.assertEqual(response.json()["detail"]["code"], "SLURM_WORKER_FINISHED")

    def test_startup_failure_and_terminal_history_events_are_deduplicated(self):
        worker_id = self.reserve()
        self.enqueue(worker_id)
        payload = registration(worker_id)
        payload["slurm"] = {"startup_error": "SSH enrollment failed"}
        with patch("worker_harness.heartbeat.async_ssh_run", new=AsyncMock(
            return_value=SSHResult(stdout="", stderr="not ready", returncode=255))):
            for _ in range(2):
                response = self.register.post("/register", json=payload, headers=self.worker_headers)
                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(response.json()["worker_status"], "offline")
        for _ in range(2):
            self.assertEqual(self.observe(worker_id, scheduler_state="FAILED").status_code, 200)
        events = asyncio.run(self.db.list_job_events("client-one"))
        self.assertEqual(sum(event.event_type == "worker_start_failed" for event in events), 1)
        self.assertEqual(sum(event.event_type == "worker_stopped" for event in events), 1)
        asyncio.run(self.db.prune_workers(int(time.time()) + 1000))
        self.assertIsNotNone(asyncio.run(self.db.get_worker(worker_id)))

    def test_conditional_cancellation_only_closes_empty_prestart_allocation(self):
        worker_id = self.reserve()
        pending = self.enqueue(worker_id).json()
        invalid = self.observe(worker_id, cancel_if_empty=True)
        self.assertEqual(invalid.status_code, 422, invalid.text)
        occupied = self.observe(worker_id, cancel_requested=True, cancel_if_empty=True)
        self.assertEqual(occupied.status_code, 200, occupied.text)
        self.assertFalse(occupied.json()["slurm"]["cancel_requested"])
        stopped = self.control.delete(f"/api/v1/jobs/{pending['id']}", headers=self.headers)
        self.assertEqual(stopped.status_code, 200, stopped.text)
        empty = self.observe(worker_id, cancel_requested=True, cancel_if_empty=True)
        self.assertEqual(empty.status_code, 200, empty.text)
        self.assertTrue(empty.json()["slurm"]["cancel_requested"])
        self.assertEqual(empty.json()["status"], "draining")

        started_id = self.reserve(request_id="started")
        self.enqueue(started_id, key="started")
        with patch("worker_harness.heartbeat.async_ssh_run", new=successful_probe), \
             patch("worker_harness.job.ssh_tmux_new", new=AsyncMock(
                 return_value=SSHResult(stdout="started", stderr="", returncode=0))):
            response = self.register.post("/register", json=registration(started_id),
                                          headers=self.worker_headers)
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["worker_status"], "online")
        running = self.observe(started_id, cancel_requested=True, cancel_if_empty=True)
        self.assertEqual(running.status_code, 200, running.text)
        self.assertFalse(running.json()["slurm"]["cancel_requested"])
        self.assertEqual(running.json()["status"], "online")

    def test_runner_staging_failure_keeps_initial_work_pending_until_retry(self):
        worker_id = self.reserve()
        queued = self.enqueue(worker_id).json()
        payload = registration(worker_id)
        self.runner_stage.side_effect = [
            SSHResult(stdout="", stderr="staging refused", returncode=1),
            RuntimeError("staging connection lost"),
        ]
        launch = AsyncMock(return_value=SSHResult(stdout="started", stderr="", returncode=0))
        with patch("worker_harness.heartbeat.async_ssh_run", new=successful_probe), \
             patch("worker_harness.job.ssh_tmux_new", new=launch):
            for _ in range(2):
                response = self.register.post("/register", json=payload, headers=self.worker_headers)
                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(response.json()["worker_status"], "offline")
                self.assertEqual(response.json()["queued_jobs"][0]["id"], queued["id"])
                self.assertEqual(asyncio.run(self.db.get_job(queued["id"])).status, JobStatus.PENDING)
                launch.assert_not_awaited()
            self.runner_stage.side_effect = None
            response = self.register.post("/register", json=payload, headers=self.worker_headers)
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()["worker_status"], "online")
            self.assertEqual(response.json()["running_jobs"][0]["id"], queued["id"])
            self.assertIsNotNone(asyncio.run(self.db.get_worker(worker_id)).slurm.started_at)
            self.register.post("/register", json=payload, headers=self.worker_headers)
        self.assertEqual(self.runner_stage.await_count, 3)
        launch.assert_awaited_once()
