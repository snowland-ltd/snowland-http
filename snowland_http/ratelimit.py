"""Token-bucket based rate limiter.

Provides both a blocking (thread-safe) :meth:`RateLimiter.acquire` and an
async :meth:`RateLimiter.acquire_async`. The two share the same token counter,
so the effective rate is honoured whether the client is used from threads or
from an asyncio event loop.
"""

import asyncio
import threading
import time
from dataclasses import dataclass


@dataclass
class RateLimitConfig:
    """Configuration for the rate limiter.

    ``max_rate`` is the maximum number of requests allowed per second.
    ``burst`` is the maximum number of requests that may be sent back-to-back
    before the limiter starts throttling. Set ``max_rate <= 0`` to disable
    rate limiting entirely.
    """

    max_rate: float = 0.0
    burst: int = 1

    def enabled(self) -> bool:
        return self.max_rate > 0


class RateLimiter:
    """A thread-safe / asyncio-safe token bucket rate limiter."""

    def __init__(self, config: RateLimitConfig):
        if config.max_rate <= 0:
            raise ValueError("RateLimiter requires max_rate > 0")
        self.rate = float(config.max_rate)
        self.capacity = float(max(int(config.burst), 1))
        self._tokens = self.capacity
        self._last = time.monotonic()
        self._lock = threading.Lock()
        self._async_lock = None  # created lazily inside a running loop

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last
        if elapsed > 0:
            self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
            self._last = now

    def acquire(self) -> None:
        """Blocking acquire: waits (sleeps) until a token is available."""
        with self._lock:
            self._refill()
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return
            wait = (1.0 - self._tokens) / self.rate
            self._tokens = 0.0
            # Advance the reference time to when the token actually arrives,
            # otherwise the sleeping time would be counted as refill too.
            self._last += wait
        time.sleep(wait)

    async def acquire_async(self) -> None:
        """Async acquire: awaits (via asyncio.sleep) until a token is available."""
        if self._async_lock is None:
            self._async_lock = asyncio.Lock()
        async with self._async_lock:
            self._refill()
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return
            wait = (1.0 - self._tokens) / self.rate
            self._tokens = 0.0
            self._last += wait
        await asyncio.sleep(wait)
