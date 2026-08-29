import tempfile
import unittest
from pathlib import Path

from worker_harness.db import Database
from worker_harness.models import Job, WorkerRegistration
from worker_harness.orchestrator import prune_stale_workers_once


class AutomaticWorkerPruningTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.tmp.name) / "worker-harness.db")
        await self.db.connect()

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
            Job(id="historical-job", worker_id=stale.worker_id, command="true")
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


if __name__ == "__main__":
    unittest.main()
