# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[0.2.0]: https://github.com/snowland-ltd/snowland-http/releases/tag/v0.2.0
[0.1.0]: https://github.com/snowland-ltd/snowland-http/releases/tag/v0.1.0
