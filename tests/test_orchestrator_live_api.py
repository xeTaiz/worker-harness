import os
import socket
import time
import unittest
from typing import Any
from urllib.parse import urlparse

import httpx


ORCH_URL = os.getenv("ORCHESTRATOR_URL", "http://orchestrator.hs.d0me.xyz:12888")
PREFERRED_WORKER_HINT = os.getenv("ORCHESTRATOR_WORKER_HINT", "userspace").strip().lower()
TIMEOUT = 20.0


class OrchestratorLiveApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.client = httpx.Client(base_url=ORCH_URL, timeout=TIMEOUT, trust_env=False)
        deadline = time.time() + 30
        last_err = None
        while time.time() < deadline:
            try:
                resp = cls.client.get("/health")
                if resp.status_code == 200:
                    return
            except Exception as err:
                last_err = err
            time.sleep(1)
        raise RuntimeError(f"Orchestrator not reachable at {ORCH_URL}: {last_err}")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.client.close()

    def _get_workers(self) -> list[dict[str, Any]]:
        deadline = time.time() + 45
        workers: list[dict[str, Any]] = []
        while time.time() < deadline:
            resp = self.client.get("/api/v1/workers")
            self.assertEqual(resp.status_code, 200, resp.text)
            workers = resp.json()
            self.assertIsInstance(workers, list)
            if workers:
                return workers
            time.sleep(2)
        self.skipTest("No workers registered in orchestrator; live worker required for API integration flow")
        return workers

    def _pick_worker(self) -> dict[str, Any]:
        workers = self._get_workers()
        online = [w for w in workers if w.get("status") == "online"]
        pool = online or workers
        if PREFERRED_WORKER_HINT:
            for worker in pool:
                name = str(worker.get("name", "")).lower()
                worker_id = str(worker.get("id", "")).lower()
                if PREFERRED_WORKER_HINT in name or PREFERRED_WORKER_HINT in worker_id:
                    return worker
        return pool[0]

    def _start_job(self, worker_id: str, command: str, name: str) -> dict[str, Any]:
        resp = self.client.post(
            "/api/v1/jobs",
            json={"worker_id": worker_id, "command": command, "name": name},
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        self.assertIn("id", body)
        return body

    def _wait_for_logs_contains(self, job_id: str, marker: str, timeout_s: int = 60) -> str:
        deadline = time.time() + timeout_s
        last = ""
        while time.time() < deadline:
            resp = self.client.get(f"/api/v1/jobs/{job_id}/logs")
            if resp.status_code == 200:
                logs = resp.json().get("logs", "")
                last = logs
                if marker in logs:
                    return logs
            time.sleep(1)
        self.fail(f"Marker '{marker}' not found in logs for job {job_id}. Last logs:\n{last}")

    def _wait_for_job_terminal(self, job_id: str, timeout_s: int = 60) -> dict[str, Any]:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            resp = self.client.get("/api/v1/jobs")
            self.assertEqual(resp.status_code, 200, resp.text)
            jobs = resp.json()
            for job in jobs:
                if job.get("id") == job_id and job.get("status") in ("done", "failed"):
                    return job
            time.sleep(1)
        self.fail(f"Job {job_id} did not become terminal in {timeout_s}s")

    def _free_local_port(self) -> int:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
        s.close()
        return port

    def test_full_orchestrator_api_flow(self):
        worker = self._pick_worker()
        worker_id = worker["id"]
        worker_name = worker["name"]

        created_jobs: list[str] = []
        created_tunnels: list[str] = []

        try:
            # /health
            resp = self.client.get("/health")
            self.assertEqual(resp.status_code, 200, resp.text)
            health = resp.json()
            self.assertEqual(health.get("status"), "healthy")
            self.assertIn("ts", health)

            # workers + summary + get by id/name + prune validation + unknown worker
            workers = self._get_workers()
            self.assertTrue(any(w.get("id") == worker_id for w in workers))

            resp = self.client.get("/api/v1/workers/summary")
            self.assertEqual(resp.status_code, 200, resp.text)
            summary = resp.json()
            self.assertGreaterEqual(summary.get("total", 0), 1)

            resp = self.client.get(f"/api/v1/workers/{worker_id}")
            self.assertEqual(resp.status_code, 200, resp.text)
            self.assertEqual(resp.json().get("id"), worker_id)

            resp = self.client.get(f"/api/v1/workers/{worker_name}")
            self.assertEqual(resp.status_code, 200, resp.text)
            self.assertEqual(resp.json().get("id"), worker_id)

            resp = self.client.get("/api/v1/workers/does-not-exist")
            self.assertEqual(resp.status_code, 404, resp.text)

            resp = self.client.delete("/api/v1/workers/prune", params={"minutes": 10000})
            self.assertEqual(resp.status_code, 200, resp.text)
            self.assertIn("removed", resp.json())

            resp = self.client.delete("/api/v1/workers/prune", params={"minutes": -1})
            self.assertEqual(resp.status_code, 422, resp.text)

            # start log-producing job by worker_id
            marker = f"LIVE_MARKER_{int(time.time())}"
            log_job = self._start_job(
                worker_id,
                command=f"bash -lc 'echo {marker}; sleep 120'",
                name="live-log-job",
            )
            log_job_id = log_job["id"]
            created_jobs.append(log_job_id)

            # start by worker_name (API resolves names)
            name_job = self._start_job(
                worker_name,
                command="bash -lc 'echo worker-name-lookup-ok; sleep 2'",
                name="live-name-lookup-job",
            )
            created_jobs.append(name_job["id"])

            # jobs list + filter + invalid status
            resp = self.client.get("/api/v1/jobs")
            self.assertEqual(resp.status_code, 200, resp.text)
            jobs = resp.json()
            self.assertTrue(any(j.get("id") == log_job_id for j in jobs))

            resp = self.client.get("/api/v1/jobs", params={"worker_id": worker_id})
            self.assertEqual(resp.status_code, 200, resp.text)

            resp = self.client.get("/api/v1/jobs", params={"status": "running"})
            self.assertEqual(resp.status_code, 200, resp.text)

            resp = self.client.get("/api/v1/jobs", params={"status": "bad-status"})
            self.assertEqual(resp.status_code, 400, resp.text)

            # logs endpoint (+ stopped-job logs later)
            logs = self._wait_for_logs_contains(log_job_id, marker)
            self.assertIn(marker, logs)

            resp = self.client.get(f"/api/v1/jobs/{log_job_id}/logs", params={"tail": 1, "head": 1})
            self.assertEqual(resp.status_code, 400, resp.text)

            resp = self.client.get("/api/v1/jobs/does-not-exist/logs")
            self.assertEqual(resp.status_code, 404, resp.text)

            # logs stream endpoint
            with self.client.stream(
                "GET",
                f"/api/v1/jobs/{log_job_id}/logs/stream",
                params={"poll_seconds": 1.0, "tail": 50},
            ) as stream_resp:
                self.assertEqual(stream_resp.status_code, 200)
                first_line = None
                for line in stream_resp.iter_lines():
                    if line:
                        first_line = line
                        break
                self.assertIsNotNone(first_line)

            resp = self.client.get(
                f"/api/v1/jobs/{log_job_id}/logs/stream",
                params={"poll_seconds": 0},
            )
            self.assertEqual(resp.status_code, 422, resp.text)

            # stop running job
            resp = self.client.delete(f"/api/v1/jobs/{log_job_id}")
            self.assertEqual(resp.status_code, 200, resp.text)
            stop_body = resp.json()
            self.assertEqual(stop_body.get("job_id"), log_job_id)
            self.assertTrue(stop_body.get("stopped"))

            # read logs of not-running job
            self._wait_for_job_terminal(log_job_id)
            resp = self.client.get(f"/api/v1/jobs/{log_job_id}/logs")
            self.assertEqual(resp.status_code, 200, resp.text)
            self.assertIn(marker, resp.json().get("logs", ""))

            # idempotent stop of terminal job
            resp = self.client.delete(f"/api/v1/jobs/{log_job_id}")
            self.assertEqual(resp.status_code, 200, resp.text)
            self.assertTrue(resp.json().get("already_terminal"))

            # start HTTP server job for tunnel verification
            service_port = 18080
            ready_marker = "Serving HTTP on"
            http_job = self._start_job(
                worker_id,
                command=(
                    "bash -lc '"
                    f"if command -v python3 >/dev/null 2>&1; then python3 -u -m http.server {service_port} --bind 127.0.0.1; "
                    f"else python -u -m http.server {service_port} --bind 127.0.0.1; fi"
                    "'"
                ),
                name="live-http-service-job",
            )
            http_job_id = http_job["id"]
            created_jobs.append(http_job_id)
            self._wait_for_logs_contains(http_job_id, ready_marker)

            # open tunnel
            local_port = self._free_local_port()
            resp = self.client.post(
                "/api/v1/tunnels",
                json={
                    "worker_id": worker_id,
                    "local_port": local_port,
                    "remote_port": service_port,
                    "name": "live-http-test",
                },
            )
            self.assertEqual(resp.status_code, 200, resp.text)
            tunnel = resp.json()
            tunnel_id = tunnel["id"]
            created_tunnels.append(tunnel_id)

            # conflict + unknown worker
            resp = self.client.post(
                "/api/v1/tunnels",
                json={
                    "worker_id": worker_id,
                    "local_port": local_port,
                    "remote_port": service_port,
                    "name": "live-http-test-conflict",
                },
            )
            self.assertEqual(resp.status_code, 409, resp.text)

            resp = self.client.post(
                "/api/v1/tunnels",
                json={"worker_id": "does-not-exist", "local_port": self._free_local_port(), "remote_port": service_port},
            )
            self.assertEqual(resp.status_code, 404, resp.text)

            # list tunnels
            resp = self.client.get("/api/v1/tunnels")
            self.assertEqual(resp.status_code, 200, resp.text)
            tunnels = resp.json()
            self.assertTrue(any(t.get("id") == tunnel_id for t in tunnels))

            # verify tunnel
            # If orchestrator is local, validate through localhost:<local_port>.
            # If orchestrator is remote, local port is bound on orchestrator host, so
            # verify tunnel creation/list metadata only.
            orch_host = (urlparse(ORCH_URL).hostname or "").lower()
            if orch_host in {"localhost", "127.0.0.1"}:
                tunnel_resp = None
                deadline = time.time() + 30
                with httpx.Client(timeout=5.0, trust_env=False) as local_client:
                    while time.time() < deadline:
                        try:
                            tunnel_resp = local_client.get(f"http://127.0.0.1:{local_port}/")
                            if tunnel_resp.status_code == 200:
                                break
                        except Exception:
                            pass
                        time.sleep(1)
                self.assertIsNotNone(tunnel_resp)
                self.assertEqual(tunnel_resp.status_code, 200)
                self.assertIn("Directory listing", tunnel_resp.text)
            else:
                self.assertEqual(tunnel.get("local_port"), local_port)
                self.assertEqual(tunnel.get("remote_port"), service_port)
                self.assertIn("pid", tunnel)

            # close tunnel + missing delete
            resp = self.client.delete(f"/api/v1/tunnels/{tunnel_id}")
            self.assertEqual(resp.status_code, 200, resp.text)
            self.assertTrue(resp.json().get("removed"))
            created_tunnels.remove(tunnel_id)

            resp = self.client.delete(f"/api/v1/tunnels/{tunnel_id}")
            self.assertEqual(resp.status_code, 404, resp.text)

            # events endpoint
            resp = self.client.get("/api/v1/events", params={"limit": 10})
            self.assertEqual(resp.status_code, 200, resp.text)
            events = resp.json()
            self.assertIsInstance(events, list)
            if events:
                sample = events[0]
                self.assertIn("type", sample)
                self.assertIn("timestamp", sample)

        finally:
            for tunnel_id in list(created_tunnels):
                try:
                    self.client.delete(f"/api/v1/tunnels/{tunnel_id}")
                except Exception:
                    pass
            for job_id in created_jobs:
                try:
                    self.client.delete(f"/api/v1/jobs/{job_id}")
                except Exception:
                    pass


if __name__ == "__main__":
    unittest.main()
