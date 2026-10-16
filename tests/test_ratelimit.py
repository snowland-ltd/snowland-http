"""Unit tests for the RPM / TPM rate-limit modes (v0.3.2).

Run from the project root::

    python -m unittest tests.test_ratelimit

These tests are backend-independent (they exercise the limiter in isolation).
"""

import asyncio
import time
import unittest

from snowland_http import RateLimitConfig
from snowland_http.ratelimit import RateLimiter


class RpmTpmTest(unittest.TestCase):
    def test_rpm_burst_then_throttle(self):
        # rpm=60 == 1 request/s, burst of 60. First 60 are free, the 61st waits ~1s.
        limiter = RateLimiter(RateLimitConfig(rpm=60))
        start = time.monotonic()
        for _ in range(60):
            limiter.acquire()
        self.assertLess(time.monotonic() - start, 0.2)

        start = time.monotonic()
        limiter.acquire()  # 61st
        elapsed = time.monotonic() - start
        self.assertGreater(elapsed, 0.7)
        self.assertLess(elapsed, 1.8)

    def test_tpm_cost_consumes_tokens(self):
        # tpm=120 == 2 tokens/s, capacity 120. Two cost=60 calls drain it instantly;
        # a third cost=1 call must wait ~0.5s for one token to refill.
        limiter = RateLimiter(RateLimitConfig(tpm=120))
        start = time.monotonic()
        limiter.acquire(cost=60)
        limiter.acquire(cost=60)
        self.assertLess(time.monotonic() - start, 0.2)

        start = time.monotonic()
        limiter.acquire(cost=1)
        elapsed = time.monotonic() - start
        self.assertGreater(elapsed, 0.3)
        self.assertLess(elapsed, 1.5)

    def test_request_bucket_ignores_cost(self):
        # Regression: a request-rate limit (max_rate) must count ONE request per
        # call regardless of ``cost``. With cost=1000 it must NOT wait ~100s; it
        # should throttle purely by max_rate (10/s -> ~0.1s between calls).
        limiter = RateLimiter(RateLimitConfig(max_rate=10, burst=1))
        start = time.monotonic()
        limiter.acquire(cost=1000)
        limiter.acquire(cost=1000)
        elapsed = time.monotonic() - start
        self.assertGreater(elapsed, 0.05)
        self.assertLess(elapsed, 0.6)

    def test_multi_bucket_and_request_binding(self):
        # max_rate huge (effectively unlimited) + tpm=60: the TPM bucket is the
        # binding constraint. 60 cost=1 calls are free, the 61st waits ~1s.
        limiter = RateLimiter(RateLimitConfig(max_rate=1000, tpm=60))
        start = time.monotonic()
        for _ in range(60):
            limiter.acquire(cost=1)
        self.assertLess(time.monotonic() - start, 0.2)

        start = time.monotonic()
        limiter.acquire(cost=1)
        elapsed = time.monotonic() - start
        self.assertGreater(elapsed, 0.7)
        self.assertLess(elapsed, 1.8)

    def test_enabled_flags(self):
        self.assertTrue(RateLimitConfig(rpm=30).enabled())
        self.assertTrue(RateLimitConfig(tpm=30).enabled())
        self.assertTrue(RateLimitConfig(max_rate=1, rpm=0, tpm=0).enabled())
        self.assertFalse(RateLimitConfig().enabled())
        self.assertFalse(RateLimitConfig(max_rate=0, rpm=0, tpm=0).enabled())

    def test_disabled_rpm_tpm_is_nonblocking(self):
        limiter = RateLimiter(RateLimitConfig(rpm=0, tpm=0))
        self.assertFalse(limiter.enabled)
        start = time.monotonic()
        for _ in range(100):
            limiter.acquire(cost=50)
        self.assertLess(time.monotonic() - start, 0.1)

    def test_async_rpm_throttle(self):
        async def run():
            limiter = RateLimiter(RateLimitConfig(rpm=60))
            start = time.monotonic()
            for _ in range(60):
                await limiter.acquire_async()
            instant = time.monotonic() - start
            self.assertLess(instant, 0.2)
            start = time.monotonic()
            await limiter.acquire_async()  # 61st
            return time.monotonic() - start

        elapsed = asyncio.run(run())
        self.assertGreater(elapsed, 0.7)
        self.assertLess(elapsed, 1.8)


if __name__ == "__main__":
    unittest.main()
