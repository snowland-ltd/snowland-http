"""Orchestrator execution modes: thread, process, coroutine and semaphore.

The Orchestrator owns a pool of HttpClient instances and spreads work over it.
Which `mode` you pick decides *how* the work is driven:

    mode="thread"     ThreadPoolExecutor, sync backend (portable, GIL bound)
    mode="process"    ProcessPoolExecutor, sync backend (CPU/GIL bound work)
    mode="coroutine"  asyncio.gather, async backend (httpx / aiohttp)
    mode="semaphore"  asyncio.gather + Semaphore, async backend

Run:
    python examples/orchestrator_modes.py

Note: on Windows/macOS (spawn) every script using mode="process" must keep its
entry point under `if __name__ == "__main__":` - see the bottom of this file.
"""

import asyncio
import importlib.util
import os
import sys
import time

_EXAMPLES_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _EXAMPLES_DIR)  # for `from _demo_server import DemoServer`
sys.path.insert(0, os.path.dirname(_EXAMPLES_DIR))  # for `import snowland_http` from a checkout

from _demo_server import DemoServer

from snowland_http import (
    Orchestrator,
    RateLimitConfig,
    execute_async_requests,
    execute_parallel_requests,
)


def _timed(label, func):
    start = time.perf_counter()
    result = func()
    print("%-46s %6.2fs" % (label, time.perf_counter() - start))
    return result


def thread_mode(base):
    """Threads spread requests over the connection pool."""
    print("=== mode='thread' ===")
    urls = [base + "/delay/200"] * 8
    with Orchestrator(num_connections=4, backend="stdlib") as orch:
        results = _timed(
            "8x 200ms over 4 connections",
            lambda: orch.get_many_parallel(urls, mode="thread", return_exceptions=True),
        )
    print("statuses:", [r.status_code for r in results])


def process_mode(base):
    """Processes: one HttpClient is built per worker process and reused."""
    print("\n=== mode='process' ===")
    urls = [base + "/delay/200"] * 8
    with Orchestrator(num_connections=2, backend="stdlib") as orch:
        results = _timed(
            "8x 200ms over 2 worker processes",
            lambda: orch.get_many_parallel(urls, mode="process", max_workers=4, return_exceptions=True),
        )
    print("statuses:", [getattr(r, "status_code", r) for r in results])
    print("the pool initializer builds one HttpClient per process, so the")
    print("connection (and its token bucket) survives across tasks.")


def standalone_functions(base):
    """One-shot helpers: they build and dispose of the Orchestrator for you."""
    print("\n=== standalone helpers ===")
    items = [
        {"method": "GET", "url": base + "/items/1"},
        ("GET", base + "/items/2", {}),
        {"method": "POST", "url": base + "/echo", "json": {"hello": "world"}},
    ]
    results = execute_parallel_requests(
        items,
        num_connections=2,
        backend="stdlib",
        mode="thread",
        rate_limit=RateLimitConfig(max_rate=20, burst=4),
        return_exceptions=True,
    )
    for item, result in zip(items, results):
        label = item["url"] if isinstance(item, dict) else item[1]
        print(label, "->", getattr(result, "status_code", result))


def _async_backend():
    """Return an installed async backend name, or None."""
    for name in ("httpx", "aiohttp"):
        if importlib.util.find_spec(name) is not None:
            return name
    return None


async def async_modes(base, backend):
    """coroutine = fire everything; semaphore = cap the concurrency."""
    print("\n=== async modes (backend=%s) ===" % backend)
    urls = [base + "/delay/200"] * 8

    async with Orchestrator(num_connections=4, backend=backend) as orch:
        start = time.perf_counter()
        results = await orch.get_many_async(urls, mode="coroutine", return_exceptions=True)
        print("coroutine (all at once)          %6.2fs" % (time.perf_counter() - start))
        print("statuses:", [getattr(r, "status_code", r) for r in results])

        start = time.perf_counter()
        results = await orch.get_many_async(
            urls, mode="semaphore", max_concurrency=2, return_exceptions=True
        )
        print("semaphore (max_concurrency=2)    %6.2fs" % (time.perf_counter() - start))
        print("statuses:", [getattr(r, "status_code", r) for r in results])

    items = [{"method": "GET", "url": base + "/items/%d" % i} for i in range(1, 4)]
    results = await execute_async_requests(
        items, num_connections=2, backend=backend, mode="coroutine", return_exceptions=True
    )
    print("execute_async_requests ->", [getattr(r, "status_code", r) for r in results])


def main():
    with DemoServer() as server:
        base = server.base_url
        print("demo server:", base)
        thread_mode(base)
        process_mode(base)
        standalone_functions(base)

        backend = _async_backend()
        if backend is None:
            print("\n=== async modes skipped ===")
            print("no async transport installed; run: pip install httpx")
        else:
            asyncio.run(async_modes(base, backend))


if __name__ == "__main__":
    main()
