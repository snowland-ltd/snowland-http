"""Error handling: return_exceptions, status checks, timeouts and retries.

Rules of thumb:

* an HTTP error status (4xx/5xx) is NOT an exception - inspect `resp.ok` or
  call `resp.raise_for_status()`;
* transport failures (DNS, refused connection, timeout) ARE exceptions;
* parallel helpers default to return_exceptions=True, so one failure never
  kills the whole batch. Set it to False to fail fast.

Run:
    python examples/error_handling.py
"""

import os
import sys

_EXAMPLES_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _EXAMPLES_DIR)  # for `from _demo_server import DemoServer`
sys.path.insert(0, os.path.dirname(_EXAMPLES_DIR))  # for `import snowland_http` from a checkout

from _demo_server import DemoServer

from snowland_http import HttpClient, Orchestrator, Task


def return_exceptions_switch(base):
    """True collects failures in the result list, False re-raises them."""
    print("=== return_exceptions ===")
    client = HttpClient(backend="stdlib")
    urls = [base + "/hello", "http://127.0.0.1:1/nope", base + "/hello"]

    results = client.get_many(urls, max_workers=3, return_exceptions=True)
    for url, result in zip(urls, results):
        if isinstance(result, Exception):
            print("  %-40s -> %s: %s" % (url, type(result).__name__, result))
        else:
            print("  %-40s -> %d" % (url, result.status_code))

    try:
        client.get_many(urls, max_workers=3, return_exceptions=False)
    except Exception as exc:
        print("  return_exceptions=False raised:", type(exc).__name__)


def status_vs_exception(base):
    """A 500 is a normal response; only check `ok` when you care."""
    print("\n=== HTTP status vs. transport exception ===")
    client = HttpClient(backend="stdlib")
    resp = client.get(base + "/status/503")
    print("status:", resp.status_code, "ok:", resp.ok)
    print("body  :", resp.text)
    try:
        resp.raise_for_status()
    except RuntimeError as exc:
        print("raise_for_status:", exc)


def timeouts(base):
    """timeout=<seconds> is forwarded to the transport."""
    print("\n=== timeout ===")
    client = HttpClient(backend="stdlib")
    try:
        client.get(base + "/delay/500", timeout=0.05)
    except Exception as exc:
        print("slow endpoint + timeout=0.05 ->", type(exc).__name__)

    print("generous timeout ->", client.get(base + "/delay/100", timeout=5).status_code)


def classify(base):
    """A reusable triage of a mixed result list."""
    print("\n=== classify a mixed batch ===")
    urls = [
        base + "/items/1",
        base + "/status/500",
        "http://127.0.0.1:1/nope",
        base + "/items/2",
        base + "/status/404",
    ]
    with Orchestrator(num_connections=3, backend="stdlib") as orch:
        results = orch.get_many_parallel(urls, mode="thread", return_exceptions=True)

    ok, http_error, failed = [], [], []
    for url, result in zip(urls, results):
        if isinstance(result, Exception):
            failed.append((url, result))
        elif result.ok:
            ok.append(url)
        else:
            http_error.append((url, result.status_code))

    print("ok         :", len(ok))
    print("http errors:", http_error)
    print("transport  :", [(u, type(e).__name__) for u, e in failed])


def retry_failed(base):
    """Retry only the entries that failed, on a flaky endpoint."""
    print("\n=== retry only the failures ===")
    urls = [base + "/flaky?fail_rate=0.6" for _ in range(10)]

    def run(batch):
        with Orchestrator(num_connections=4, backend="stdlib") as orch:
            return orch.get_many_parallel(batch, mode="thread", return_exceptions=True)

    results = run(urls)
    pending = [(i, u) for i, (u, r) in enumerate(zip(urls, results)) if not _good(r)]
    print("first pass : %d/%d ok, %d to retry" % (len(urls) - len(pending), len(urls), len(pending)))

    for attempt in (1, 2):
        if not pending:
            break
        retry_urls = [u for _, u in pending]
        retry_results = run(retry_urls)
        still = [(i, u) for (i, u), r in zip(pending, retry_results) if not _good(r)]
        for (idx, _), result in zip(pending, retry_results):
            if _good(result):
                results[idx] = result
        print("attempt %d  : %d recovered, %d left" % (attempt + 1, len(pending) - len(still), len(still)))
        pending = still

    print("final      : %d/%d ok" % (sum(1 for r in results if _good(r)), len(urls)))


def _good(result):
    return not isinstance(result, Exception) and result.ok


def retry_inside_task(base):
    """Retry inside post_process is not possible; retry around the task."""
    print("\n=== retry a single task ===")

    def fetch_with_retry(client, url, attempts=5):
        last = None
        for _ in range(attempts):
            last = client.get(url)
            if last.ok:
                return last
        return last

    flaky = base + "/flaky?fail_rate=0.4"
    with HttpClient(backend="stdlib") as client:
        resp = fetch_with_retry(client, flaky)
        print("status after up to 5 attempts:", resp.status_code)

    task = Task(
        request_params={"method": "GET", "url": flaky},
        post_process=lambda r: r.status_code,
        name="flaky",
    )
    with Orchestrator(num_connections=1, backend="stdlib") as orch:
        for attempt in range(5):
            result = orch.execute_tasks_parallel([task], mode="thread", return_exceptions=True)[0]
            if result == 200:
                break
    print("task status after %d attempts: %s" % (attempt + 1, result))
    print("a retry loop raises the success odds but never guarantees it -")
    print("always keep a final branch for the permanent failure.")


def main():
    with DemoServer() as server:
        base = server.base_url
        print("demo server:", base)
        return_exceptions_switch(base)
        status_vs_exception(base)
        timeouts(base)
        classify(base)
        retry_failed(base)
        retry_inside_task(base)


if __name__ == "__main__":
    main()
