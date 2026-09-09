"""Scheduling strategies: how tasks are spread over the connection pool.

    scheduling="round_robin"  (default) task i always uses connection
                              i % num_connections. Deterministic, but when
                              durations differ, slow tasks can pile up on the
                              same connection while others sit idle.
    scheduling="idle"         every task takes the next *free* connection; a
                              connection serves one task at a time, so the
                              concurrency ceiling is num_connections.

The strategy can be set on the constructor (instance default) and overridden
per call. It is ignored (with a warning log) in mode="process".

Run:
    python examples/scheduling_strategies.py
"""

import asyncio
import importlib.util
import logging
import os
import sys
import time

_EXAMPLES_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _EXAMPLES_DIR)  # for `from _demo_server import DemoServer`
sys.path.insert(0, os.path.dirname(_EXAMPLES_DIR))  # for `import snowland_http` from a checkout

from _demo_server import DemoServer

from snowland_http import (
    SCHEDULING_IDLE,
    SCHEDULING_ROUND_ROBIN,
    HttpClient,
    Orchestrator,
)

NUM_CONNECTIONS = 4


def _stats(base):
    """Read the demo server counters (peak concurrent requests, ...)."""
    with HttpClient(backend="stdlib") as client:
        return client.get(base + "/stats").json()


def _reset_stats(base):
    with HttpClient(backend="stdlib") as client:
        client.get(base + "/stats/reset")


def defaults_and_validation(base):
    """Where the strategy can be set, and what happens on a bad value."""
    print("=== defaults and validation ===")
    orch = Orchestrator(num_connections=2, backend="stdlib")
    print("default scheduling      :", orch.scheduling)
    orch.close_all()

    idle_orch = Orchestrator(num_connections=2, backend="stdlib", scheduling=SCHEDULING_IDLE)
    print("constructor scheduling  :", idle_orch.scheduling)
    idle_orch.close_all()

    try:
        Orchestrator(num_connections=2, backend="stdlib", scheduling="fastest")
    except ValueError as exc:
        print("invalid value raises    :", exc)

    print("constants               : %r / %r" % (SCHEDULING_ROUND_ROBIN, SCHEDULING_IDLE))


def _run(base, strategy, durations, workers):
    """Run a workload; return (elapsed, peak concurrency, statuses)."""
    _reset_stats(base)
    items = [{"method": "GET", "url": "%s/delay/%d" % (base, ms)} for ms in durations]
    with Orchestrator(num_connections=NUM_CONNECTIONS, backend="stdlib", scheduling=strategy) as orch:
        start = time.perf_counter()
        results = orch.execute_parallel(
            items, mode="thread", max_workers=workers, return_exceptions=True
        )
        elapsed = time.perf_counter() - start
    return elapsed, _stats(base)["max_in_flight"], [getattr(r, "status_code", r) for r in results]


def concurrency_ceiling(base):
    """The guaranteed difference: how many requests hit the server at once."""
    print("\n=== peak concurrency (%d connections, 8 workers, 12x150ms) ===" % NUM_CONNECTIONS)
    durations = [150] * 12

    rr_time, rr_peak, rr_status = _run(base, SCHEDULING_ROUND_ROBIN, durations, workers=8)
    print("round_robin : peak=%2d  wall=%.2fs  statuses=%s" % (rr_peak, rr_time, set(rr_status)))

    idle_time, idle_peak, idle_status = _run(base, SCHEDULING_IDLE, durations, workers=8)
    print("idle        : peak=%2d  wall=%.2fs  statuses=%s" % (idle_peak, idle_time, set(idle_status)))

    print("round_robin only maps task i to connection i %% %d; nothing stops" % NUM_CONNECTIONS)
    print("two workers from using the same connection, so the peak follows max_workers.")
    print("idle leases a connection exclusively, so the peak is capped at")
    print("num_connections - pick it when one connection is a scarce resource")
    print("(one account, one proxy, a per-connection quota, ...).")


def unbalanced_workload(base):
    """Unequal durations: what each strategy does with the same workload."""
    print("\n=== unequal durations (2x1000ms + 10x100ms, 4 connections) ===")
    durations = [1000, 1000] + [100] * 10

    rr_time, rr_peak, rr_status = _run(base, SCHEDULING_ROUND_ROBIN, durations, workers=4)
    print("round_robin : wall=%.2fs  peak=%d  statuses=%s" % (rr_time, rr_peak, set(rr_status)))

    idle_time, idle_peak, idle_status = _run(base, SCHEDULING_IDLE, durations, workers=4)
    print("idle        : wall=%.2fs  peak=%d  statuses=%s" % (idle_time, idle_peak, set(idle_status)))

    print("with max_workers == num_connections both take about the same time;")
    print("the real difference is the guarantee shown above: idle never puts two")
    print("requests on the same connection, round_robin does not care.")


def method_level_override(base):
    """A per-call argument wins over the instance default."""
    print("\n=== per-call override ===")
    with Orchestrator(num_connections=3, backend="stdlib", scheduling=SCHEDULING_ROUND_ROBIN) as orch:
        urls = [base + "/items/%d" % i for i in range(1, 7)]

        rr = orch.get_many_parallel(urls, mode="thread")
        print("instance default (round_robin) ->", [r.status_code for r in rr])

        idle = orch.get_many_parallel(urls, mode="thread", scheduling=SCHEDULING_IDLE)
        print("override scheduling='idle'     ->", [r.status_code for r in idle])

        explicit = orch.get_many_parallel(urls, mode="thread", scheduling=SCHEDULING_ROUND_ROBIN)
        print("override back to round_robin   ->", [r.status_code for r in explicit])


def process_mode_warning(base):
    """mode='process' ignores the strategy and logs a warning."""
    print("\n=== process mode ignores scheduling (warning is logged) ===")
    logging.basicConfig(level=logging.WARNING, format="  [%(levelname)s] %(message)s")
    with Orchestrator(num_connections=2, backend="stdlib") as orch:
        results = orch.get_many_parallel(
            [base + "/items/1", base + "/items/2", base + "/items/3"],
            mode="process",
            scheduling=SCHEDULING_IDLE,
            return_exceptions=True,
        )
    print("results:", [getattr(r, "status_code", r) for r in results])
    print("worker processes already pull from a shared call queue, so idle-first")
    print("dispatch happens naturally there and connections cannot be shared.")


async def async_idle(base, backend):
    """The same switch works for the async paths."""
    print("\n=== async idle (backend=%s) ===" % backend)
    urls = [base + "/delay/100"] * 6
    async with Orchestrator(num_connections=3, backend=backend, scheduling=SCHEDULING_IDLE) as orch:
        start = time.perf_counter()
        results = await orch.get_many_async(urls, mode="coroutine", return_exceptions=True)
        print("coroutine + idle : %6.2fs" % (time.perf_counter() - start))

        results = await orch.get_many_async(
            urls, mode="semaphore", max_concurrency=6, scheduling=SCHEDULING_IDLE, return_exceptions=True
        )
        print("semaphore + idle :", [getattr(r, "status_code", r) for r in results])
    print("with idle, concurrency is capped at num_connections even if")
    print("max_concurrency is set higher.")


def main():
    with DemoServer() as server:
        base = server.base_url
        print("demo server:", base)
        defaults_and_validation(base)
        concurrency_ceiling(base)
        unbalanced_workload(base)
        method_level_override(base)
        process_mode_warning(base)

        backend = next(
            (n for n in ("httpx", "aiohttp") if importlib.util.find_spec(n) is not None), None
        )
        if backend is None:
            print("\n=== async idle skipped ===")
            print("no async transport installed; run: pip install httpx")
        else:
            asyncio.run(async_idle(base, backend))


if __name__ == "__main__":
    main()
