# snowland-http

[![PyPI version](https://img.shields.io/pypi/v/snowland-http.svg)](https://pypi.org/project/snowland-http/)
[![PyPI downloads](https://img.shields.io/pypi/dm/snowland-http.svg?cacheSeconds=86400)](https://pypi.org/project/snowland-http/)
[![License](https://img.shields.io/badge/license-BSD%203--Clause-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/downloads/)
[![CI](https://github.com/snowland-ltd/snowland-http/actions/workflows/test.yml/badge.svg)](https://github.com/snowland-ltd/snowland-http/actions/workflows/test.yml)

A rate-limited, parallel HTTP client with pluggable `requests` / `httpx` / `aiohttp` / **zero-dependency `stdlib` (urllib)** backends.

## Features

- **Pluggable transport**: `requests` (sync only), `httpx` (sync + async), `aiohttp` (async only), and `stdlib` (sync, built on the Python standard library `urllib` — requires **no third-party install**). Select via `backend=` or use `backend="auto"` to auto-detect (preference: httpx > aiohttp > requests > **stdlib**).
- **Global rate limiting**: a token-bucket limiter shared by all parallel workers, so the aggregate request rate never exceeds the configured ceiling. Provides both a blocking `acquire()` and an async `acquire_async()`.
- **Parallel requests**: thread pool (`ThreadPoolExecutor`) for sync, and `asyncio.gather` + `Semaphore` for async.
- **Orchestrator**: manage multiple HTTP connections with multi-process or coroutine-based parallel/concurrent execution for improved throughput.
- **Connection lifecycle**: explicit `open()` / `close()` (and async counterparts), with context-manager support that opens on enter and closes on exit.

## Installation

The three third-party transports (`requests` / `httpx` / `aiohttp`) are **optional dependencies**, independent of each other — none is required for the package to import (backends are imported lazily). When none is installed, the client automatically falls back to the built-in `stdlib` backend (pure `urllib`), so it works in a bare Python environment with zero installs. Install at least one third-party transport to use the corresponding backend:

```bash
# Option A: install a transport library directly
pip install requests          # or httpx / aiohttp — install at least one

# Option B: install via extras (recommended)
pip install ".[requests]"     # sync backend only
pip install ".[httpx]"        # sync + async backend (recommended)
pip install ".[aiohttp]"      # async backend only
pip install ".[all]"          # everything
# ".[stdlib]" is a no-op extra: it documents the always-available urllib backend.
```

With `backend="auto"`, the client detects installed libraries in the order httpx > aiohttp > requests, and finally falls back to `stdlib` (no install needed).

## Quick start

### Sync + rate limiting + parallel

```python
from snowland_http import HttpClient, RateLimitConfig

client = HttpClient(
    backend="requests",
    rate_limit=RateLimitConfig(max_rate=5, burst=2),  # <=5 req/s, burst of 2
)

resp = client.get("https://example.com")
print(resp.status_code, resp.json())

# parallel GET
results = client.get_many(["https://example.com/1", "https://example.com/2"])
for r in results:
    print(r if isinstance(r, Exception) else r.status_code)
```

### Async + rate limiting + parallel

```python
import asyncio
from snowland_http import HttpClient, RateLimitConfig

async def main():
    client = HttpClient(
        backend="httpx",
        rate_limit=RateLimitConfig(max_rate=10, burst=5),
    )
    async with client:  # open_async on enter, close_async on exit
        results = await client.get_many_async(["https://example.com/1", "https://example.com/2"])
        for r in results:
            print(r.status_code)

asyncio.run(main())
```

### Zero-dependency (stdlib / urllib)

No third-party package needed — works with a stock Python:

```bash
pip install snowland-http   # nothing else required
```

```python
from snowland_http import HttpClient

# backend="auto" falls back to stdlib when requests/httpx/aiohttp are absent,
# or pick it explicitly:
client = HttpClient(backend="stdlib")
resp = client.get("https://example.com")
print(resp.status_code, resp.text)
```

## API

`HttpClient(backend="auto", rate_limit=None, max_workers=10, max_concurrency=10)`

The `backend` argument accepts `"requests"`, `"httpx"`, `"aiohttp"`, `"stdlib"` (or `"urllib"`), or `"auto"`. With `"auto"` the client prefers httpx > aiohttp > requests and finally falls back to the dependency-free `stdlib` backend.

| Method | Description |
| --- | --- |
| `request(method, url, **kwargs)` | Single sync request |
| `get/post/put/delete/head/patch(url, **kwargs)` | Sync convenience methods |
| `request_many(items, max_workers, return_exceptions)` | Sync parallel (thread pool) |
| `get_many(urls, method="GET", ...)` | Sync parallel GET |
| `request_async(method, url, **kwargs)` | Single async request |
| `get_async/...` | Async convenience methods |
| `request_many_async(items, max_concurrency, return_exceptions)` | Async parallel |
| `get_many_async(urls, ...)` | Async parallel GET |
| `open()` / `open_async()` | Open / establish connection resources |
| `close()` / `close_async()` | Close connection resources |

- Each element of `items` may be a `dict` (`{"method": ..., "url": ..., ...}`) or a `(method, url, kwargs_dict)` tuple.
- Parallel methods default to `return_exceptions=True`: a single failure is returned as an exception object in the result list rather than aborting the rest. Set it to `False` to raise immediately.

### Rate limiting

`RateLimitConfig(max_rate, burst)`

- `max_rate`: maximum requests per second. `max_rate <= 0` **disables** rate limiting entirely (the limiter becomes a no-op and never blocks); it does **not** raise.
- `burst`: how many requests may be sent back-to-back before smoothing kicks in.

### Response encoding

`HttpResponse.text` is decoded according to the HTTP rules, **not** a hard-coded UTF-8:

- the `charset` declared in the `Content-Type` header wins (e.g. `text/html; charset=gbk`);
- when no `charset` is present, the default is **ISO-8859-1** (latin-1) per RFC 7231;
- decoding is strict (no silent `errors="replace"`): an invalid body raises `UnicodeDecodeError` so mojibake is never hidden.

You can override the resolution by passing `encoding=` when constructing a response (used internally by the backends).

### Backend constraints

- `requests` supports **sync** APIs only (calling `request_async` raises `AsyncRequiredError`).
- `aiohttp` supports **async** APIs only (calling `request` raises `AsyncRequiredError`).
- `httpx` supports both.
- `stdlib` (urllib) supports **sync** APIs only (calling `request_async` raises `AsyncRequiredError`).

### Orchestrator (Multi-connection parallel/concurrent execution)

The `Orchestrator` class manages a pool of `HttpClient` instances and provides high-level APIs for executing requests across multiple connections:

```python
from snowland_http import Orchestrator, RateLimitConfig

# Create orchestrator with 4 connections
orch = Orchestrator(
    num_connections=4,
    backend="httpx",
    rate_limit=RateLimitConfig(max_rate=10, burst=5),
)

# Execute in parallel (multi-process or multi-thread)
urls = ["https://api.example.com/data/1", "https://api.example.com/data/2"]
results = orch.execute_parallel(urls, mode="process")  # or mode="thread"

# Execute concurrently (asyncio)
results = await orch.execute_async(urls, mode="coroutine")  # or mode="semaphore"
```

**Execution modes:**

- **Parallel (sync)**:
  - `mode="process"`: Multi-process execution using `ProcessPoolExecutor`
  - `mode="thread"`: Multi-threaded execution with load balancing across connections
- **Concurrent (async)**:
  - `mode="coroutine"`: `asyncio.gather` with load balancing
  - `mode="semaphore"`: Controlled concurrency using `asyncio.Semaphore`

**Convenience methods:**

```python
# Parallel GET requests
results = orch.get_many_parallel(urls, mode="thread")

# Async concurrent GET requests
results = await orch.get_many_async(urls, mode="coroutine")
```

**Standalone functions:**

```python
from snowland_http import execute_parallel_requests, execute_async_requests

# Parallel execution
results = execute_parallel_requests(items, num_connections=4, mode="process")

# Async execution
results = await execute_async_requests(items, num_connections=4, mode="coroutine")
```

**Context manager support:**

```python
# Sync context manager
with Orchestrator(num_connections=4) as orch:
    results = orch.get_many_parallel(urls)

# Async context manager
async with Orchestrator(num_connections=4) as orch:
    results = await orch.get_many_async(urls)
```

### Task (Pre-processing and Post-processing)

The `Task` class allows you to define HTTP requests with optional pre-processing and post-processing hooks:

```python
from snowland_http import Task

# Define pre-processing function
def add_auth(params):
    params["headers"] = {"Authorization": "Bearer token"}

# Define post-processing function
def extract_json(response):
    return response.json()

# Create a task
task = Task(
    request_params={"method": "GET", "url": "https://api.example.com/data"},
    pre_process=add_auth,
    post_process=extract_json,
    name="fetch_data",
)

# Execute task
result = task.execute(client)
```

**Executing tasks with Orchestrator:**

```python
# Create multiple tasks
tasks = [
    Task(
        request_params={"method": "GET", "url": f"https://api.example.com/data/{i}"},
        pre_process=add_auth,
        post_process=extract_json,
    )
    for i in range(10)
]

# Execute tasks in parallel
results = orch.execute_tasks_parallel(tasks, mode="thread")

# Execute tasks concurrently
results = await orch.execute_tasks_async(tasks, mode="coroutine")
```

**Task execution flow:**

1. **Pre-processing** (optional): Modify request parameters before sending
2. **HTTP Request**: Execute the HTTP request
3. **Post-processing** (optional): Process the response before returning

This allows you to:
- Add authentication headers dynamically
- Transform request parameters
- Extract and transform response data
- Implement custom error handling
- Add logging or monitoring

## Development & CI

- Tests run on `master` and `dev` branches (see `.github/workflows/test.yml`), across Python 3.8–3.12, installing `.[all]` so functional/parallel tests execute.
- Publishing to PyPI happens on GitHub Release (`release: published`) via `.github/workflows/release.yml`, authenticating with the `PYPI_API_TOKEN` repository secret.

Run the test suite locally:

```bash
pip install -e ".[all]"
python -m unittest discover -s tests -v
```

## License

BSD 3-Clause. See [LICENSE](LICENSE).
