import tempfile
import unittest
from pathlib import Path

from worker_harness.db import Database
from worker_harness.models import Job, JobStatus, WorkerRegistration
from worker_harness.orchestrator import prune_stale_workers_once

from identity_support import bootstrap


class AutomaticWorkerPruningTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.tmp.name) / "worker-harness.db")
        await self.db.connect()
        self.owner = await bootstrap(self.db)

    async def asyncTearDown(self) -> None:
        await self.db.close()
        self.tmp.cleanup()

    @staticmethod
    def registration(worker_id: str) -> WorkerRegistration:
        return WorkerRegistration(
            worker_id=worker_id,
            name=worker_id,
            worker_ip="100.64.0.1",
            ssh_user="root",
        )

    async def test_prunes_only_stale_registration_and_preserves_job_history(self) -> None:
        stale = self.registration("stale-worker")
        fresh = self.registration("fresh-worker")
        await self.db.upsert_worker(stale)
        await self.db.upsert_worker(fresh)
        await self.db.insert_job(
            Job(
                id="historical-job", worker_id=stale.worker_id, command="true",
                owner_user_id=self.owner["id"],
                submitted_by_actor_id=f"migration:{self.owner['id']}",
            )
        )
        await self.db._db.execute(
            "UPDATE workers SET last_heartbeat_ts = ? WHERE id = ?",
            (700, stale.worker_id),
        )
        await self.db._db.execute(
            "UPDATE workers SET last_heartbeat_ts = ? WHERE id = ?",
            (900, fresh.worker_id),
        )
        await self.db._db.commit()

        removed = await prune_stale_workers_once(self.db, cutoff_seconds=180, now=1000)

        self.assertEqual(removed, 1)
        self.assertIsNone(await self.db.get_worker(stale.worker_id))
        self.assertIsNotNone(await self.db.get_worker(fresh.worker_id))
        historical = await self.db.get_job("historical-job")
        self.assertIsNotNone(historical)
        self.assertEqual(historical.worker_id, stale.worker_id)

        restored = await self.db.upsert_worker(stale)
        self.assertEqual(restored.id, stale.worker_id)
        self.assertIsNotNone(await self.db.get_worker(stale.worker_id))
        self.assertIsNotNone(await self.db.get_job("historical-job"))

    async def test_delete_worker_retains_owned_history_and_allocated_usage(self) -> None:
        worker = await self.db.upsert_worker(self.registration("deleted-worker"))
        job = Job(
            id="completed-job", worker_id=worker.id, command="private command",
            owner_user_id=self.owner["id"],
            submitted_by_actor_id=f"migration:{self.owner['id']}",
            status=JobStatus.DONE, started_at=100, finished_at=220,
            gpu_count=2, gpu_indices=[0, 1],
            worker_name_at_submission=worker.name,
        )
        await self.db.insert_job(job)
        before = await self.db.usage(now=300)

        self.assertTrue(await self.db.delete_worker(worker.id))

        self.assertIsNone(await self.db.get_worker(worker.id))
        historical = await self.db.get_job(job.id)
        self.assertIsNotNone(historical)
        self.assertEqual(historical.owner_user_id, self.owner["id"])
        self.assertEqual(historical.worker_name_at_submission, worker.name)
        after = await self.db.usage(now=400)
        self.assertEqual(after, before)
        self.assertEqual(after[0]["allocated_gpu_seconds"], 240)


if __name__ == "__main__":
    unittest.main()
