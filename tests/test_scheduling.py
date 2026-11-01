"""Pure FIFO GPU estimates and attributable admission-time changes."""

import unittest

from worker_harness.scheduling import (
    Estimate,
    PendingRequest,
    RunningClaim,
    WorkerResources,
    delays,
    estimate_worker,
)


class SchedulingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.now = 10_000
        self.resources = WorkerResources("worker", True, 1)

    def test_one_gpu_fifo_and_reorder_delays(self) -> None:
        running = [RunningClaim("R", (0,), self.now - 100, 700, True)]
        a = PendingRequest("A", 1, 1200)
        b = PendingRequest("B", 1, 300)

        before = estimate_worker(self.now, self.resources, running, [a, b])
        after = estimate_worker(self.now, self.resources, running, [b, a])

        self.assertEqual(set(before), {"R", "A", "B"})
        self.assertEqual(before["R"].estimated_start_at, self.now - 100)
        self.assertEqual(before["R"].estimated_finish_at, self.now + 600)
        self.assertEqual(before["A"].estimated_start_at, self.now + 600)
        self.assertEqual(before["A"].estimated_finish_at, self.now + 1800)
        self.assertEqual(before["B"].estimated_start_at, self.now + 1800)
        self.assertEqual(before["B"].estimated_finish_at, self.now + 2100)
        self.assertEqual(after["B"].estimated_start_at, self.now + 600)
        self.assertEqual(after["A"].estimated_start_at, self.now + 900)
        self.assertEqual(delays(before, after, iter(["A", "B"])), {"A": 300, "B": -1200})
        self.assertTrue(all(item.status == "known" for item in before.values()))
        self.assertIn("GPU 0", before["A"].explanation)
        self.assertIn("job R", before["A"].explanation)

    def test_unknown_release_blocks_head_and_prevents_backfill(self) -> None:
        resources = WorkerResources("worker", True, 2)
        running = [RunningClaim("R", (1,), self.now - 10, 100, False)]
        pending = [PendingRequest("A", 2, 100), PendingRequest("B", 1, 50)]
        estimates = estimate_worker(self.now, resources, running, pending)

        self.assert_unknown_start(estimates["A"])
        self.assert_unknown_start(estimates["B"])
        self.assertIn("job R", estimates["A"].explanation)
        self.assertIn("earlier queued job A", estimates["B"].explanation)

        estimates = estimate_worker(
            self.now, resources, running, [PendingRequest("first", 1, 10), *pending]
        )
        self.assertEqual(estimates["first"].estimated_start_at, self.now)
        self.assertEqual(estimates["first"].estimated_finish_at, self.now + 10)
        self.assert_unknown_start(estimates["A"])
        self.assert_unknown_start(estimates["B"])

    def test_overrun_is_unknown_not_an_immediate_release(self) -> None:
        running = [RunningClaim("R", (0,), self.now - 101, 100, True)]
        estimates = estimate_worker(
            self.now, self.resources, running, [PendingRequest("A", 1, 50)]
        )
        self.assertEqual(estimates["R"].estimated_start_at, self.now - 101)
        self.assertEqual(estimates["R"].status, "unknown")
        self.assertIsNone(estimates["R"].estimated_finish_at)
        self.assertIn("past its estimate", estimates["R"].explanation)
        self.assert_unknown_start(estimates["A"])
        self.assertIn("past its estimate", estimates["A"].explanation)

    def test_release_exactly_now_is_known(self) -> None:
        estimates = estimate_worker(
            self.now,
            self.resources,
            [RunningClaim("R", (0,), self.now - 100, 100, True)],
            [PendingRequest("A", 1, 50)],
        )
        self.assertEqual(estimates["R"].estimated_finish_at, self.now)
        self.assertEqual(estimates["R"].status, "known")
        self.assertEqual(estimates["A"].estimated_start_at, self.now)

    def test_starting_stopping_and_unknown_duration_keep_claims(self) -> None:
        for release_known, duration in [(False, 100), (True, 0)]:
            with self.subTest(release_known=release_known, duration=duration):
                estimates = estimate_worker(
                    self.now,
                    self.resources,
                    [RunningClaim("R", (0,), self.now - 10, duration, release_known)],
                    [PendingRequest("A", 1, 50)],
                )
                self.assertIsNone(estimates["R"].estimated_finish_at)
                self.assertEqual(estimates["R"].status, "unknown")
                self.assert_unknown_start(estimates["A"])

    def test_offline_worker_has_no_pending_eta(self) -> None:
        estimates = estimate_worker(
            self.now,
            WorkerResources("worker", False, 2),
            [RunningClaim("R", (0,), self.now - 10, 100, True)],
            [PendingRequest("A", 1, 50), PendingRequest("B", 1, 50)],
        )
        for job_id in ["A", "B"]:
            self.assert_unknown_start(estimates[job_id])
            self.assertIn("worker offline", estimates[job_id].explanation)
        self.assertEqual(estimates["R"].estimated_finish_at, self.now + 90)

    def test_external_busy_gpu_has_unknown_release(self) -> None:
        estimates = estimate_worker(
            self.now,
            WorkerResources("worker", True, 2, frozenset({0})),
            [],
            [PendingRequest("A", 1, 50), PendingRequest("B", 2, 50)],
        )
        self.assertEqual(estimates["A"].estimated_start_at, self.now)
        self.assert_unknown_start(estimates["B"])
        self.assertIn("GPU 0", estimates["B"].explanation)
        self.assertIn("outside harness", estimates["B"].explanation)

    def test_insufficient_capacity_blocks_later_jobs(self) -> None:
        for gpu_count in [0, 1]:
            with self.subTest(gpu_count=gpu_count):
                estimates = estimate_worker(
                    self.now,
                    WorkerResources("worker", True, gpu_count),
                    [],
                    [PendingRequest("A", 2, 50), PendingRequest("B", 1, 50)],
                )
                self.assert_unknown_start(estimates["A"])
                self.assert_unknown_start(estimates["B"])
                self.assertIn("more GPUs", estimates["A"].explanation)

    def test_lowest_available_gpu_index_is_allocated(self) -> None:
        # Both GPUs are initially free. A must take index 0, not index 1.
        estimates = estimate_worker(
            self.now,
            WorkerResources("worker", True, 2),
            [],
            [
                PendingRequest("A", 1, 100),
                PendingRequest("B", 1, 200),
                PendingRequest("C", 1, 50),
                PendingRequest("D", 2, 10),
            ],
        )
        self.assertEqual(estimates["A"].estimated_start_at, self.now)
        self.assertEqual(estimates["B"].estimated_start_at, self.now)
        self.assertEqual(estimates["C"].estimated_start_at, self.now + 100)
        self.assertIn("GPU 0 released by job A", estimates["C"].explanation)
        self.assertEqual(estimates["D"].estimated_start_at, self.now + 200)
        self.assertIn("GPU 0 released by job C", estimates["D"].explanation)
        self.assertIn("GPU 1 released by job B", estimates["D"].explanation)

    def test_fifo_admission_never_moves_backwards_in_time(self) -> None:
        # A waits for two slots at t+100. B must not backfill the spare
        # GPU that was already free at t=now before A was admitted.
        estimates = estimate_worker(
            self.now,
            WorkerResources("worker", True, 4),
            [
                RunningClaim("R1", (0,), self.now, 100, True),
                RunningClaim("R2", (1,), self.now, 100, True),
                RunningClaim("R3", (2,), self.now, 100, True),
            ],
            [PendingRequest("A", 2, 0), PendingRequest("B", 1, 10)],
        )
        self.assertEqual(estimates["A"].estimated_start_at, self.now + 100)
        self.assertIsNone(estimates["A"].estimated_finish_at)
        self.assertEqual(estimates["B"].estimated_start_at, self.now + 100)

    def test_unknown_pending_duration_preserves_start_but_not_release(self) -> None:
        estimates = estimate_worker(
            self.now,
            self.resources,
            [],
            [PendingRequest("A", 1, 0), PendingRequest("B", 1, 50)],
        )
        self.assertEqual(estimates["A"].estimated_start_at, self.now)
        self.assertIsNone(estimates["A"].estimated_finish_at)
        self.assertEqual(estimates["A"].status, "unknown")
        self.assert_unknown_start(estimates["B"])
        self.assertIn("job A has no runtime estimate", estimates["B"].explanation)
        self.assertEqual(delays(estimates, estimates, ["A", "B"]), {"A": 0, "B": None})

    def test_cpu_running_claim_does_not_consume_gpu_capacity(self) -> None:
        estimates = estimate_worker(
            self.now,
            self.resources,
            [RunningClaim("CPU", (), self.now - 10, 0, True)],
            [PendingRequest("GPU", 1, 50)],
        )
        self.assertEqual(estimates["GPU"].estimated_start_at, self.now)
        self.assertIsNone(estimates["CPU"].estimated_finish_at)

    def test_deterministic_and_does_not_mutate_inputs(self) -> None:
        resources = WorkerResources("worker", True, 3, frozenset({1, 2}))
        running = [RunningClaim("R", (0,), self.now - 10, 100, True)]
        pending = [PendingRequest("A", 1, 50), PendingRequest("B", 2, 50)]
        snapshot = (resources, running.copy(), pending.copy())
        first = estimate_worker(self.now, resources, running, pending)
        second = estimate_worker(self.now, resources, running, pending)
        self.assertEqual(first, second)
        self.assertEqual(list(first), ["R", "A", "B"])
        self.assertEqual((resources, running, pending), snapshot)

    def test_delays_unknown_in_either_direction(self) -> None:
        known = Estimate("A", self.now, self.now + 10, "known", "available")
        unknown = Estimate("A", None, None, "unknown", "unknown release")
        self.assertEqual(delays({"A": known}, {"A": unknown}, ["A"]), {"A": None})
        self.assertEqual(delays({"A": unknown}, {"A": known}, ["A"]), {"A": None})

    def assert_unknown_start(self, estimate: Estimate) -> None:
        self.assertEqual(estimate.status, "unknown")
        self.assertIsNone(estimate.estimated_start_at)
        self.assertIsNone(estimate.estimated_finish_at)
