"""Token-bucket based rate limiter.

Supports three optional limit dimensions, each ANDed together (a call must
satisfy every enabled bucket):

  * per-second request rate  (``max_rate`` / ``burst``) - requests per second
  * per-minute request rate  (``rpm``)                - Requests Per Minute
  * per-minute token rate    (``tpm``)                - Tokens Per Minute (LLM-style)

Pass a per-call ``cost`` to :meth:`RateLimiter.acquire` so each call consumes that
many units (e.g. the token count of an LLM call) from every enabled bucket.

Provides both a blocking (thread-safe) :meth:`RateLimiter.acquire` and an
async :meth:`RateLimiter.acquire_async`. The two share the same counters, so the
effective rate is honoured whether the client is used from threads or from an
asyncio event loop.
"""

import asyncio
import threading
import time
from dataclasses import dataclass


@dataclass
class RateLimitConfig:
    """Configuration for the rate limiter.

    ``max_rate`` is the maximum number of requests per second; ``burst`` is the
    maximum number of requests that may be sent back-to-back before throttling.
    ``rpm`` is the maximum number of requests per minute (Requests Per Minute).
    ``tpm`` is the maximum number of tokens per minute (Tokens Per Minute); pass a
    per-call ``cost`` to :meth:`RateLimiter.acquire` so each call consumes that
    many tokens.

    Setting every limit to ``<= 0`` disables rate limiting entirely.
    """

    max_rate: float = 0.0
    burst: int = 1
    rpm: float = 0.0
    tpm: float = 0.0

    def enabled(self) -> bool:
        return self.max_rate > 0 or self.rpm > 0 or self.tpm > 0


class _Bucket:
    """A single token bucket refilled at ``rate`` units per second.

    When ``per_token`` is False the bucket counts *requests* (every call consumes
    exactly 1, regardless of ``cost``); when True it counts *tokens* and consumes
    ``cost`` units per call. This lets a request-rate limit and a TPM limit share
    one ``acquire(cost=...)`` call with different units.
    """

    __slots__ = ("capacity", "rate", "tokens", "last", "per_token")

    def __init__(self, capacity: float, rate: float, per_token: bool = False):
        self.capacity = float(capacity)
        self.rate = float(rate)
        self.per_token = per_token
        self.tokens = float(capacity)
        self.last = time.monotonic()

    def refill(self, now: float) -> None:
        elapsed = now - self.last
        if elapsed > 0:
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.last = now

    def deficit(self, cost: float) -> float:
        """Seconds to wait before the required units are available (0 if ready)."""
        needed = cost if self.per_token else 1.0
        if self.tokens >= needed:
            return 0.0
        return (needed - self.tokens) / self.rate

    def consume(self, cost: float) -> None:
        needed = cost if self.per_token else 1.0
        self.tokens = max(0.0, self.tokens - needed)


class RateLimiter:
    """A thread-safe / asyncio-safe token-bucket rate limiter."""

    def __init__(self, config: RateLimitConfig):
        self.enabled = config.enabled()
        self._buckets: list = []
        if config.max_rate > 0:
            self._buckets.append(
                _Bucket(capacity=max(float(config.burst), 1.0), rate=float(config.max_rate))
            )
        if config.rpm > 0:
            self._buckets.append(
                _Bucket(capacity=float(config.rpm), rate=float(config.rpm) / 60.0)
            )
        if config.tpm > 0:
            self._buckets.append(
                _Bucket(
                    capacity=float(config.tpm),
                    rate=float(config.tpm) / 60.0,
                    per_token=True,
                )
            )
        self._lock = threading.Lock()
        self._async_lock = None  # created lazily inside a running loop

    def _acquire_common(self, cost: float) -> float:
        """Refill buckets, return the wait time (seconds) required for ``cost``.

        Caller must hold the appropriate lock. Returns 0.0 when no wait is needed.
        """
        now = time.monotonic()
        for b in self._buckets:
            b.refill(now)
        wait = max((b.deficit(cost) for b in self._buckets), default=0.0)
        if wait > 0:
            time.sleep(wait)
            now = time.monotonic()
            for b in self._buckets:
                b.refill(now)
        for b in self._buckets:
            b.consume(cost)
        return wait

    def acquire(self, cost: float = 1.0) -> None:
        """Blocking acquire.

        Consumes ``cost`` units from every enabled bucket (per-second request
        rate, RPM, and TPM). ``cost`` defaults to 1, so a plain ``acquire()``
        keeps the old behaviour; for TPM limiting pass the number of tokens the
        call spends.

        When rate limiting is disabled this returns immediately.
        """
        if not self.enabled or not self._buckets:
            return
        with self._lock:
            self._acquire_common(cost)

    async def acquire_async(self, cost: float = 1.0) -> None:
        """Async acquire. See :meth:`acquire`."""
        if not self.enabled or not self._buckets:
            return
        if self._async_lock is None:
            self._async_lock = asyncio.Lock()
        async with self._async_lock:
            # Run the (blocking) token math in a worker thread so the event loop
            # is never stalled by the sleep inside _acquire_common.
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._acquire_common, cost)
