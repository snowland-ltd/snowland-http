# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[0.1.0]: https://github.com/snowland-ltd/snowland-http/releases/tag/v0.1.0
