# snowland-http

A rate-limited, parallel HTTP client with pluggable `requests` / `httpx` / `aiohttp` backends.

## Features

- **Pluggable transport**: `requests` (sync only), `httpx` (sync + async), `aiohttp` (async only). Select via `backend=` or use `backend="auto"` to auto-detect (preference: httpx > aiohttp > requests).
- **Global rate limiting**: a token-bucket limiter shared by all parallel workers, so the aggregate request rate never exceeds the configured ceiling. Provides both a blocking `acquire()` and an async `acquire_async()`.
- **Parallel requests**: thread pool (`ThreadPoolExecutor`) for sync, and `asyncio.gather` + `Semaphore` for async.
- **Connection lifecycle**: explicit `open()` / `close()` (and async counterparts), with context-manager support that opens on enter and closes on exit.

## Installation

All three transports (`requests` / `httpx` / `aiohttp`) are **optional dependencies**, independent of each other — none is required for the package to import (backends are imported lazily). Install at least one to use the corresponding backend:

```bash
# Option A: install a transport library directly
pip install requests          # or httpx / aiohttp — install at least one

# Option B: install via extras (recommended)
pip install ".[requests]"     # sync backend only
pip install ".[httpx]"        # sync + async backend (recommended)
pip install ".[aiohttp]"      # async backend only
pip install ".[all]"          # everything
```

With `backend="auto"`, the client detects installed libraries in the order httpx > aiohttp > requests.

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

## API

`HttpClient(backend="auto", rate_limit=None, max_workers=10, max_concurrency=10)`

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

- `max_rate`: maximum requests per second (`<= 0` disables limiting).
- `burst`: how many requests may be sent back-to-back before smoothing kicks in.

### Backend constraints

- `requests` supports **sync** APIs only (calling `request_async` raises `AsyncRequiredError`).
- `aiohttp` supports **async** APIs only (calling `request` raises `AsyncRequiredError`).
- `httpx` supports both.

## Development & CI

- Tests run on `master` and `dev` branches (see `.github/workflows/test.yml`), across Python 3.8–3.12, installing `.[all]` so functional/parallel tests execute.
- Publishing to PyPI happens on GitHub Release (`release: published`) via `.github/workflows/release.yml`, using PyPI Trusted Publishing (OIDC) by default.

Run the test suite locally:

```bash
pip install -e ".[all]"
python -m unittest discover -s tests -v
```

## License

BSD 3-Clause. See [LICENSE](LICENSE).
