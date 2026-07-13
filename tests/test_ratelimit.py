import unittest
from unittest.mock import patch

from worker_harness.ratelimit import AgentRateLimiter, RateLimited, TokenBucket, resolve_agent_name


class RateLimitTests(unittest.TestCase):
    def test_burst_exhaustion_and_refill(self):
        with patch("worker_harness.ratelimit.time.monotonic", side_effect=[100.0, 100.0, 100.0, 100.0, 101.0]):
            bucket = TokenBucket(capacity=2, refill_rate=1.0)
            self.assertEqual(bucket.try_consume(), (True, 0.0))
            self.assertEqual(bucket.try_consume(), (True, 0.0))
            ok, retry = bucket.try_consume()
            self.assertFalse(ok)
            self.assertAlmostEqual(retry, 1.0)
            self.assertEqual(bucket.try_consume(), (True, 0.0))

    def test_agents_have_isolated_buckets(self):
        limiter = AgentRateLimiter(capacity=1, refill_rate=0.01)
        limiter.check("compute-manager")
        with self.assertRaises(RateLimited):
            limiter.check("compute-manager")
        # A different agent gets its own burst capacity.
        limiter.check("data-expert")
        stats = limiter.stats()
        self.assertIn("compute-manager", stats)
        self.assertIn("data-expert", stats)

    def test_header_identity_and_peer_fallback(self):
        self.assertEqual(resolve_agent_name({"x-agent-name": "tester"}, "127.0.0.1"), "tester")
        self.assertEqual(resolve_agent_name({}, "127.0.0.1"), "ip:127.0.0.1")
