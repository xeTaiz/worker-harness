import asyncio
import unittest

from worker_harness.cache import TTLCache


class TTLCacheTests(unittest.TestCase):
    def test_get_set_expiry_and_invalidation(self):
        async def run():
            cache = TTLCache()
            self.assertIsNone(await cache.get("missing"))
            await cache.set("workers:list", {"n": 1}, ttl_seconds=60)
            self.assertEqual(await cache.get("workers:list"), {"n": 1})
            self.assertTrue(await cache.invalidate("workers:list"))
            self.assertIsNone(await cache.get("workers:list"))
            self.assertFalse(await cache.invalidate("workers:list"))

            await cache.set("workers:list", 1, ttl_seconds=60)
            await cache.set("workers:summary", 2, ttl_seconds=60)
            await cache.set("tunnels:list", 3, ttl_seconds=60)
            self.assertEqual(await cache.invalidate_prefix("workers:"), 2)
            self.assertIsNone(await cache.get("workers:list"))
            self.assertEqual(await cache.get("tunnels:list"), 3)

            await cache.set("expired", "value", ttl_seconds=0)
            await asyncio.sleep(0)
            self.assertIsNone(await cache.get("expired"))

            stats = cache.stats()
            self.assertGreaterEqual(stats["hits"], 2)
            self.assertGreaterEqual(stats["misses"], 3)
            self.assertGreaterEqual(stats["evictions"], 3)

        asyncio.run(run())
