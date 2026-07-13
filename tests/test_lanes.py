import asyncio
import unittest

from worker_harness.lanes import LaneTimeout, WorkerLanes


class WorkerLanesTests(unittest.TestCase):
    def test_fifo_queue_bounds_and_independent_workers(self):
        async def run():
            lanes = WorkerLanes(max_concurrent=1, max_queue=1)
            first_entered = asyncio.Event()
            release_first = asyncio.Event()
            order: list[str] = []

            async def first():
                async with lanes.acquire("slow", timeout=1):
                    order.append("first")
                    first_entered.set()
                    await release_first.wait()

            async def second():
                async with lanes.acquire("slow", timeout=1):
                    order.append("second")

            first_task = asyncio.create_task(first())
            await first_entered.wait()
            second_task = asyncio.create_task(second())
            await asyncio.sleep(0.02)

            self.assertEqual(lanes.stats()["slow"]["in_use"], 1)
            self.assertEqual(lanes.stats()["slow"]["queue_depth"], 1)

            # Queue depth is capped: a third slow-worker request fails fast.
            with self.assertRaises(LaneTimeout) as ctx:
                async with lanes.acquire("slow", timeout=0.1):
                    pass
            self.assertTrue(ctx.exception.queue_full)

            # A different worker is unaffected by slow's full lane/queue.
            async with lanes.acquire("fast", timeout=0.1):
                order.append("fast")

            release_first.set()
            await asyncio.gather(first_task, second_task)
            self.assertEqual(order, ["first", "fast", "second"])
            self.assertEqual(lanes.stats()["slow"]["in_use"], 0)
            self.assertEqual(lanes.stats()["slow"]["queue_depth"], 0)

        asyncio.run(run())

    def test_wait_timeout_removes_waiter_and_releases_no_slot(self):
        async def run():
            lanes = WorkerLanes(max_concurrent=1, max_queue=4)
            hold = asyncio.Event()
            entered = asyncio.Event()

            async def holder():
                async with lanes.acquire("w", timeout=1):
                    entered.set()
                    await hold.wait()

            task = asyncio.create_task(holder())
            await entered.wait()
            with self.assertRaises(LaneTimeout) as ctx:
                async with lanes.acquire("w", timeout=0.02):
                    pass
            self.assertFalse(ctx.exception.queue_full)
            self.assertEqual(lanes.stats()["w"]["queue_depth"], 0)
            self.assertEqual(lanes.stats()["w"]["in_use"], 1)

            hold.set()
            await task
            self.assertEqual(lanes.stats()["w"]["in_use"], 0)

        asyncio.run(run())

    def test_cancelled_waiter_is_removed(self):
        async def run():
            lanes = WorkerLanes(max_concurrent=1, max_queue=2)
            entered = asyncio.Event()
            release = asyncio.Event()

            async def holder():
                async with lanes.acquire("w", timeout=1):
                    entered.set()
                    await release.wait()

            async def waiter():
                async with lanes.acquire("w", timeout=10):
                    return "never"

            holder_task = asyncio.create_task(holder())
            await entered.wait()
            waiter_task = asyncio.create_task(waiter())
            await asyncio.sleep(0.01)
            waiter_task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await waiter_task
            self.assertEqual(lanes.stats()["w"]["queue_depth"], 0)
            release.set()
            await holder_task
            self.assertEqual(lanes.stats()["w"]["in_use"], 0)

        asyncio.run(run())
