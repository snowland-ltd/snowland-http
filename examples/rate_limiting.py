"""Rate limiting: token bucket, burst, disabling, and sharing across workers.

The limiter is attached to the client, so every parallel worker (thread or
coroutine) shares the same bucket and the *aggregate* rate stays under the
configured ceiling.

Run:
    python examples/rate_limiting.py
"""

import os
import sys
import time

_EXAMPLES_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _EXAMPLES_DIR)  # for `from _demo_server import DemoServer`
sys.path.insert(0, os.path.dirname(_EXAMPLES_DIR))  # for `import snowland_http` from a checkout

from _demo_server import DemoServer

from snowland_http import HttpClient, RateLimitConfig, RateLimiter


def timed(label, func):
    """Run func and print how long it took."""
    start = time.perf_counter()
    result = func()
    elapsed = time.perf_counter() - start
    print("%-52s %6.2fs" % (label, elapsed))
    return elapsed, result


def burst_then_throttle(base):
    """burst requests go out immediately, afterwards the rate kicks in."""
    print("=== burst vs. sustained rate ===")
    urls = [base + "/hello"] * 6

    client = HttpClient(backend="stdlib", rate_limit=RateLimitConfig(max_rate=2, burst=3))
    timed("burst=3, max_rate=2, 6 requests (expect ~1.5s)", lambda: client.get_many(urls, max_workers=6))

    client = HttpClient(backend="stdlib", rate_limit=RateLimitConfig(max_rate=2, burst=1))
    timed("burst=1, max_rate=2, 6 requests (expect ~2.5s)", lambda: client.get_many(urls, max_workers=6))


def disabled_limiter(base):
    """max_rate <= 0 disables throttling entirely (non-blocking no-op)."""
    print("\n=== disabled rate limit ===")
    urls = [base + "/hello"] * 20

    unlimited = HttpClient(backend="stdlib")
    elapsed_off, _ = timed("no RateLimitConfig, 20 requests", lambda: unlimited.get_many(urls, max_workers=8))

    zero = HttpClient(backend="stdlib", rate_limit=RateLimitConfig(max_rate=0))
    elapsed_zero, _ = timed("max_rate=0 (disabled), 20 requests", lambda: zero.get_many(urls, max_workers=8))

    slow = HttpClient(backend="stdlib", rate_limit=RateLimitConfig(max_rate=5, burst=1))
    elapsed_on, _ = timed("max_rate=5, burst=1, 20 requests", lambda: slow.get_many(urls, max_workers=8))

    print("disabled adds no overhead: %.2fs vs %.2fs; limiting costs %.2fs" % (elapsed_zero, elapsed_off, elapsed_on))


def shared_across_threads(base):
    """The bucket is shared: 10 workers at max_rate=5 still take ~2s, not ~0s."""
    print("\n=== shared bucket across threads ===")
    urls = [base + "/hello"] * 10
    client = HttpClient(backend="stdlib", rate_limit=RateLimitConfig(max_rate=5, burst=1))
    elapsed, results = timed("10 requests / max_rate=5 (expect ~1.8s)", lambda: client.get_many(urls, max_workers=10))
    print("all ok:", all(not isinstance(r, Exception) and r.ok for r in results))


def standalone_limiter():
    """RateLimiter can also be used on its own, with or without a loop."""
    print("\n=== standalone RateLimiter ===")
    limiter = RateLimiter(RateLimitConfig(max_rate=10, burst=2))
    start = time.perf_counter()
    for _ in range(2):
        limiter.acquire()
    print("first 2 (burst) acquires : %.3fs" % (time.perf_counter() - start))
    start = time.perf_counter()
    for _ in range(3):
        limiter.acquire()
    print("next 3 acquires @10/s    : %.3fs" % (time.perf_counter() - start))

    off = RateLimiter(RateLimitConfig())
    print("disabled limiter.enabled :", off.enabled)


def rpm_tpm_demo():
    """Requests-Per-Minute (RPM) and Tokens-Per-Minute (TPM) limiting, plus ANDing."""
    print("\n=== RPM / TPM limiting ===")

    # RPM: 60 requests per minute == 1 per second, burst of 60.
    rpm = RateLimiter(RateLimitConfig(rpm=60))
    t0 = time.perf_counter()
    for _ in range(60):
        rpm.acquire()
    burst = time.perf_counter() - t0
    t0 = time.perf_counter()
    rpm.acquire()  # 61st must wait ~1s for the bucket to refill 1 request
    wait = time.perf_counter() - t0
    print("RPM burst of 60    : %.3fs (instant)" % burst)
    print("RPM 61st acquire   : %.3fs (expect ~1.0s)" % wait)

    # TPM: 60 tokens per minute == 1 token/s, burst of 60 tokens.
    # Every call costs 1 token here (cost=1).
    tpm = RateLimiter(RateLimitConfig(tpm=60))
    t0 = time.perf_counter()
    for _ in range(60):
        tpm.acquire(cost=1)
    burst = time.perf_counter() - t0
    t0 = time.perf_counter()
    tpm.acquire(cost=1)  # 61st token must wait ~1s to refill 1 token
    wait = time.perf_counter() - t0
    print("TPM burst of 60 tok: %.3fs (instant)" % burst)
    print("TPM 61st token     : %.3fs (expect ~1.0s)" % wait)

    # A single call can cost many tokens. With tpm=60 (1 token/s), one cost=30
    # call spends half the per-minute budget, so a third 30-token call would block
    # ~30s (we don't actually wait that long in the demo).
    big = RateLimiter(RateLimitConfig(tpm=60))
    big.acquire(cost=30)  # instant (60 -> 30 left)
    t0 = time.perf_counter()
    big.acquire(cost=30)  # instant (30 -> 0 left)
    print("TPM two 30-tok     : %.3fs (instant; 3rd would wait ~30s)" % (time.perf_counter() - t0))

    # Both dimensions enabled -> ANDed. Here the TPM limit is the binding one
    # (max_rate=1000 is effectively unlimited for requests).
    both = RateLimiter(RateLimitConfig(max_rate=1000, tpm=60))
    for _ in range(60):
        both.acquire(cost=1)
    t0 = time.perf_counter()
    both.acquire(cost=1)  # 61st token -> wait ~1s
    print("AND(max_rate+TPM)  : %.3fs (expect ~1.0s, TPM binding)" % (time.perf_counter() - t0))


def main():
    with DemoServer() as server:
        base = server.base_url
        print("demo server:", base)
        burst_then_throttle(base)
        disabled_limiter(base)
        shared_across_threads(base)
    standalone_limiter()
    rpm_tpm_demo()


if __name__ == "__main__":
    main()
