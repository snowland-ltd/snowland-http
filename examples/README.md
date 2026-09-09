# Examples

Every example here runs **offline** and **dependency-free**: each one starts a
minimal local HTTP server (`_demo_server.py`, bound to a random free port on
127.0.0.1) and talks to it through the zero-dependency `stdlib` (urllib)
backend. So:

- no internet access and no third-party site such as httpbin is required;
- `requests` / `httpx` / `aiohttp` do not have to be installed;
- async sections skip themselves (with a hint) when no async backend is found.

See the repository root [README.md](../README.md) and
[doc/API_0_3_1.md](../doc/API_0_3_1.md) for the full API reference.

## Running the examples

From the project root (Windows `cmd`):

```bat
cd /d d:\astar\code\python\snowland-http

.venv\Scripts\python.exe examples\basic_client.py
.venv\Scripts\python.exe examples\rate_limiting.py
.venv\Scripts\python.exe examples\orchestrator_example.py
.venv\Scripts\python.exe examples\orchestrator_modes.py
.venv\Scripts\python.exe examples\scheduling_strategies.py
.venv\Scripts\python.exe examples\task_pipeline.py
.venv\Scripts\python.exe examples\error_handling.py
```

They can also be run as modules (same working directory):

```bat
.venv\Scripts\python.exe -m examples.basic_client
```

Both forms work because every example bootstraps `sys.path` at the top, so
neither `PYTHONPATH` nor `pip install -e .` is needed.

## Overview

| File | Topic | Main API |
| --- | --- | --- |
| `basic_client.py` | Client basics: a single request, `HttpResponse` fields, POST JSON, error status codes, thread-pool batches, lifecycle, backend detection | `HttpClient` / `HttpResponse` |
| `rate_limiting.py` | Token bucket: `burst` vs. sustained rate, `max_rate <= 0` disables it, one bucket shared by all threads, using `RateLimiter` standalone | `RateLimitConfig` / `RateLimiter` |
| `orchestrator_example.py` | End-to-end tour of `Orchestrator`: thread / process / coroutine / semaphore, mixed request-item shapes, manual connection management, and the `Task` pre/post-processing variants | `Orchestrator` / `Task` / `execute_parallel_requests` / `execute_async_requests` |
| `orchestrator_modes.py` | The four execution modes (thread / process / coroutine / semaphore) and the standalone helpers | `Orchestrator` / `execute_parallel_requests` / `execute_async_requests` |
| `scheduling_strategies.py` | `round_robin` vs. `idle`: instance default, per-call override, **measured concurrency ceiling** (`round_robin` follows `max_workers`, `idle` is capped at `num_connections`), unbalanced durations, ignored + warned in process mode | `Orchestrator(scheduling=...)` / `SCHEDULING_IDLE` / `SCHEDULING_ROUND_ROBIN` |
| `task_pipeline.py` | Task pipelines: `pre_process` adds auth headers, `post_process` parses/reduces, paged fetch + aggregation, sequential discovery then parallel fan-out | `Task` |
| `error_handling.py` | `return_exceptions` on/off, HTTP status is not an exception, timeouts, classifying results, retrying only the failures | `HttpClient` / `Orchestrator` / `Task` |

## The demo server: `_demo_server.py`

A minimal server shared by the examples. It can also be started on its own for
manual poking (`python examples\_demo_server.py`, alive for 30 seconds):

| Route | Description |
| --- | --- |
| `GET /hello` | returns `{"message": "hello"}` |
| `GET /items/<id>` | returns `{"id": ..., "name": "item-<id>", "stock": n}` |
| `GET /page?page=1&size=3` | returns `{"page": n, "size": k, "items": [...], "has_more": bool}` (3 pages) |
| `GET /delay/<ms>` | sleeps for the given milliseconds, used to create duration skew |
| `GET /status/<code>` | replies with the given HTTP status code, used for 4xx/5xx |
| `GET /flaky?fail_rate=0.5` | fails with HTTP 500 at the given probability, used for retries |
| `ANY /echo` | echoes method, path, query, headers and body |
| `GET /stats` | returns the concurrency counters `{"in_flight": n, "max_in_flight": n, "requests": n}` |
| `GET /stats/reset` | zeroes the counters above |

Usage:

```python
from _demo_server import DemoServer

with DemoServer() as server:
    url = server.base_url + "/hello"
```

Bodies are plain ASCII JSON, so they are unaffected by the encoding resolution
of `HttpResponse` (when no charset is declared it falls back to ISO-8859-1 per
RFC 7231; see the root README).

## Backends and dependencies

| Backend | Install | Sync | Async |
| --- | --- | --- | --- |
| `stdlib` (urllib) | none, always available | yes | no |
| `requests` | `pip install requests` | yes | no |
| `httpx` | `pip install httpx` | yes | yes |
| `aiohttp` | `pip install aiohttp` | no | yes |

The sync examples pass `backend="stdlib"` explicitly so they also run in a bare
Python environment; async sections probe `httpx` then `aiohttp` and print a hint
when neither is installed.

## Notes

- **Process mode on Windows / macOS (spawn)**: the main module is re-imported in
  the worker processes, so the entry point must live under
  `if __name__ == "__main__":` - all examples are written that way.
- **An async section is skipped**: install `pip install httpx` and re-run.
- **Port conflicts**: `DemoServer()` binds a random free port (`port=0`), so no
  fixed port is claimed.
- **`scheduling="idle"` is not about speed**: it guarantees that one connection
  serves only one task at a time (ceiling = `num_connections`), which protects a
  scarce resource - one account, one proxy, a per-connection quota.
  `scheduling_strategies.py` measures this with `/stats`: `round_robin` peaks at
  `max_workers` (8 workers -> peak 8) while `idle` stays at `num_connections`
  (4). With `idle` in thread mode keep `max_workers >= num_connections`,
  otherwise some connections are never used.
- **`scheduling` is ignored in process mode**: expected, a warning is logged
  (worker processes already pull work from a shared call queue).
