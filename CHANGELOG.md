# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.2] - 2026-10-16

### Added

- **RPM / TPM rate-limit modes**: `RateLimitConfig` gains `rpm` (requests per
  minute) and `tpm` (tokens per minute) fields, alongside the existing
  `max_rate` / `burst`. All configured limits are combined with a logical AND —
  every call must satisfy every enabled limit. Request-based limits
  (`max_rate`, `rpm`) count exactly one request per call; the token-based limit
  (`tpm`) consumes `cost` tokens per call, where `cost` is supplied via the new
  `rate_limit_cost` argument on `HttpClient.request` / `request_async` / `get` /
  `get_many` / `post` / `post_many` (and as `cost` to `RateLimiter.acquire`).
  `RateLimiter` now holds one token bucket per enabled limit and blocks until the
  tightest one admits the call.

## [0.3.1] - 2026-09-09

### Added

- **Scheduling strategy for `Orchestrator`**: a new `scheduling` argument
  accepting `"round_robin"` (default, keeps the existing task-index-modulo
  behaviour) or `"idle"` (each task runs on the next idle connection, and a
  connection serves a single task at a time). It can be set on
  `Orchestrator(...)` as the instance default and overridden per call on
  `execute_parallel` / `execute_tasks_parallel` / `execute_async` /
  `execute_tasks_async`, as well as `get_many_parallel` / `get_many_async`.
  New exported constants: `SCHEDULING_ROUND_ROBIN`, `SCHEDULING_IDLE`.

### Changed

- **Process mode reuses `HttpClient`**: `mode="process"` now builds one
  `HttpClient` per worker process via `ProcessPoolExecutor(initializer=...)`
  and reuses it, fixing the previous "new client per task" behaviour that
  prevented connection/session reuse and kept resetting the rate-limiter
  token bucket.
- Passing a non-default `scheduling` in `mode="process"` is no longer silently
  ignored: a warning is logged explaining why it has no effect there.

### Fixed

- **Thread-safe connection rotation in `Orchestrator.get_connection()`**: the
  round-robin index (`_current_index`) is now incremented under a lock, preventing
  lost-update races when `get_connection()` is called concurrently from multiple
  threads.

## [0.3.0] - 2026-09-01

### Added

- **Orchestrator module**: a new high-level API for managing multiple HTTP connections
  with parallel/concurrent execution. Supports:
  - Multi-process parallel execution using `ProcessPoolExecutor`
  - Multi-threaded execution with load balancing across connections
  - Asyncio-based concurrent execution with `asyncio.gather` or `Semaphore`
  - Context manager support for both sync and async workflows
  - Standalone functions `execute_parallel_requests()` and `execute_async_requests()`
- **Task class**: a new abstraction for HTTP requests with pre-processing and post-processing hooks.
  - Supports custom pre-processing functions to modify request parameters before sending
  - Supports custom post-processing functions to transform responses after receiving
  - Can be executed directly with `HttpClient` or in batches with `Orchestrator`
  - Provides `execute_tasks_parallel()` and `execute_tasks_async()` methods in `Orchestrator`
- **Example code**: `examples/orchestrator_example.py` demonstrating Orchestrator and Task usage.
- **API documentation for v0.3** (`doc/API_0.3.md`).

## [0.2.0] - 2026-08-09

### Added

- **Zero-dependency `stdlib` backend**: a new synchronous transport built on the
  Python standard library `urllib`. It requires **no third-party install** and is
  automatically selected by `backend="auto"` when `requests` / `httpx` / `aiohttp`
  are not installed. Select it explicitly with `HttpClient(backend="stdlib")`
  (alias `backend="urllib"`). Added the empty `stdlib` extra for
  documentation/discoverability.
- **API documentation for v0.2** (`doc/API_0.2.md`).

### Changed

- **Response text encoding** now follows HTTP rules instead of assuming UTF-8:
  the `charset` of `Content-Type` is honoured, and when absent the default is
  ISO-8859-1 (RFC 7231). Decoding is strict — no silent `errors="replace"` — so
  invalid bodies raise `UnicodeDecodeError` rather than producing mojibake.

### Fixed

- **Rate limiter robustness**: `RateLimitConfig` with `max_rate <= 0` now
  *disables* rate limiting (the limiter becomes a non-blocking no-op) instead of
  raising `ValueError`. This also avoids a divide-by-zero on `rate == 0`.

## [0.1.0] - 2026-07-29

Initial release of `snowland-http`.

### Added

- **Rate limiting**: a global token-bucket limiter shared by all parallel workers,
  with both a blocking `acquire()` and an async `acquire_async()`.
- **Parallel requests**: thread pool (`ThreadPoolExecutor`) for sync, and
  `asyncio.gather` + `Semaphore` for async.
- **Pluggable transport backends**: `requests` (sync only),
  `httpx` (sync + async, recommended), and `aiohttp` (async only).
  Select via `backend=`, or use `backend="auto"` to auto-detect
  (preference: httpx > aiohttp > requests).
- **Optional dependencies**: none of the transport libraries is required for
  the package to import (backends are imported lazily). Install via extras
  `requests` / `httpx` / `aiohttp` / `all`.
- **Connection lifecycle**: explicit `open()` / `close()` (and async
  counterparts) with sync/async context-manager support.
- **Unified response model** (`HttpResponse`) that hides backend differences.
- **0.1 API documentation** (`doc/API_0.1.md`).
- **CI pipelines**:
  - `test.yml` runs the test suite on `master` and `dev` branches
    (Python 3.8–3.12, installing `.[all]`).
  - `release.yml` publishes to PyPI on GitHub Release
    (`release: published`) via PyPI Trusted Publishing (OIDC).

### License

BSD 3-Clause.

[0.3.2]: https://github.com/snowland-ltd/snowland-http/releases/tag/v0.3.2
[0.3.1]: https://github.com/snowland-ltd/snowland-http/releases/tag/v0.3.1
[0.3.0]: https://github.com/snowland-ltd/snowland-http/releases/tag/v0.3.0
[0.2.0]: https://github.com/snowland-ltd/snowland-http/releases/tag/v0.2.0
[0.1.0]: https://github.com/snowland-ltd/snowland-http/releases/tag/v0.1.0
