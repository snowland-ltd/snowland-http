"""High-level HTTP client with rate limiting and parallel execution.

Features
--------
* Pluggable transport: ``requests`` (sync), ``httpx`` (sync + async),
  ``aiohttp`` (async), and ``stdlib`` (sync, built on :mod:`urllib` with NO
  third-party dependencies). Select via ``backend=`` or let it auto-detect
  among requests/httpx/aiohttp.
* Global token-bucket rate limiting applied across *all* parallel workers,
  so the aggregate request rate stays under the configured ceiling.
* Parallel requests via a thread pool (sync) or ``asyncio.gather`` +
  semaphore (async).

Quick start
-----------
    from snowland_http import HttpClient, RateLimitConfig

    client = HttpClient(backend="requests",
                        rate_limit=RateLimitConfig(max_rate=5, burst=2))
    resp = client.get("https://example.com")

    # parallel
    results = client.get_many(["https://a.com", "https://b.com"])

Async usage
-----------
    import asyncio
    from snowland_http import HttpClient, RateLimitConfig

    async def main():
        client = HttpClient(backend="httpx",
                            rate_limit=RateLimitConfig(max_rate=10, burst=5))
        async with client:
            results = await client.get_many_async(["https://a.com", "https://b.com"])

    asyncio.run(main())

Zero-dependency usage
---------------------
When no third-party HTTP library is installed, the standard-library backend
works out of the box (sync only)::

    from snowland_http import HttpClient

    client = HttpClient(backend="stdlib")
    resp = client.get("https://example.com")
    print(resp.status_code, resp.text)

The ``stdlib`` backend is synchronous-only and supports ``method``, ``url``,
``headers``, ``params``, ``data`` (bytes/str/dict), ``json`` and ``timeout``.
When no third-party transport is installed, ``backend="auto"`` falls back to it
automatically; you can also pass ``backend="stdlib"`` explicitly.
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Iterable, List, Optional, Sequence, Union

from snowland_http.backends import build_backend, detect_backend
from snowland_http.backends.base import BaseBackend
from snowland_http.exceptions import AsyncRequiredError
from snowland_http.ratelimit import RateLimitConfig, RateLimiter
from snowland_http.response import HttpResponse


class HttpClient:
    def __init__(
        self,
        backend: str = "auto",
        rate_limit: Optional[RateLimitConfig] = None,
        max_workers: int = 10,
        max_concurrency: int = 10,
    ):
        self.backend: BaseBackend = self._build_backend(backend)
        self.rate_config = rate_limit or RateLimitConfig()
        self.limiter: Optional[RateLimiter] = (
            RateLimiter(self.rate_config) if self.rate_config.enabled() else None
        )
        self.max_workers = max_workers
        self.max_concurrency = max_concurrency

    # ------------------------------------------------------------------ #
    # Backend selection
    # ------------------------------------------------------------------ #
    @staticmethod
    def _detect_backend() -> str:
        return detect_backend()

    def _build_backend(self, backend: str) -> BaseBackend:
        if backend == "auto":
            backend = detect_backend()
        return build_backend(backend)

    # ------------------------------------------------------------------ #
    # Sync API
    # ------------------------------------------------------------------ #
    def request(
        self,
        method: str,
        url: str,
        rate_limit_cost: float = 1.0,
        **kwargs,
    ) -> HttpResponse:
        if self.backend.scheme == "async":
            raise AsyncRequiredError(
                f"Backend '{self.backend.__class__.__name__}' only supports async; "
                "use request_async / request_many_async."
            )
        if self.limiter is not None:
            self.limiter.acquire(cost=rate_limit_cost)
        return self.backend.request(method, url, **kwargs)

    def get(self, url: str, **kwargs) -> HttpResponse:
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs) -> HttpResponse:
        return self.request("POST", url, **kwargs)

    def put(self, url: str, **kwargs) -> HttpResponse:
        return self.request("PUT", url, **kwargs)

    def delete(self, url: str, **kwargs) -> HttpResponse:
        return self.request("DELETE", url, **kwargs)

    def head(self, url: str, **kwargs) -> HttpResponse:
        return self.request("HEAD", url, **kwargs)

    def patch(self, url: str, **kwargs) -> HttpResponse:
        return self.request("PATCH", url, **kwargs)

    def request_many(
        self,
        items: Sequence[Union[Dict[str, Any], tuple]],
        max_workers: Optional[int] = None,
        return_exceptions: bool = True,
    ) -> List[Any]:
        items = [self._normalize(item) for item in items]
        workers = max_workers or self.max_workers
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(self.request, **item) for item in items]
            results: List[Any] = []
            for future in futures:
                try:
                    results.append(future.result())
                except Exception as exc:  # noqa: BLE001
                    if return_exceptions:
                        results.append(exc)
                    else:
                        raise
            return results

    def get_many(
        self,
        urls: Iterable[str],
        method: str = "GET",
        max_workers: Optional[int] = None,
        return_exceptions: bool = True,
        **kwargs,
    ) -> List[Any]:
        items = [{"method": method, "url": url, **kwargs} for url in urls]
        return self.request_many(
            items, max_workers=max_workers, return_exceptions=return_exceptions
        )

    # ------------------------------------------------------------------ #
    # Async API
    # ------------------------------------------------------------------ #
    async def request_async(
        self,
        method: str,
        url: str,
        rate_limit_cost: float = 1.0,
        **kwargs,
    ) -> HttpResponse:
        if self.backend.scheme == "sync":
            raise AsyncRequiredError(
                f"Backend '{self.backend.__class__.__name__}' only supports sync; "
                "use request / request_many."
            )
        if self.limiter is not None:
            await self.limiter.acquire_async(cost=rate_limit_cost)
        return await self.backend.request_async(method, url, **kwargs)

    async def get_async(self, url: str, **kwargs) -> HttpResponse:
        return await self.request_async("GET", url, **kwargs)

    async def post_async(self, url: str, **kwargs) -> HttpResponse:
        return await self.request_async("POST", url, **kwargs)

    async def put_async(self, url: str, **kwargs) -> HttpResponse:
        return await self.request_async("PUT", url, **kwargs)

    async def delete_async(self, url: str, **kwargs) -> HttpResponse:
        return await self.request_async("DELETE", url, **kwargs)

    async def head_async(self, url: str, **kwargs) -> HttpResponse:
        return await self.request_async("HEAD", url, **kwargs)

    async def patch_async(self, url: str, **kwargs) -> HttpResponse:
        return await self.request_async("PATCH", url, **kwargs)

    async def request_many_async(
        self,
        items: Sequence[Union[Dict[str, Any], tuple]],
        max_concurrency: Optional[int] = None,
        return_exceptions: bool = True,
    ) -> List[Any]:
        items = [self._normalize(item) for item in items]
        sem = asyncio.Semaphore(max_concurrency or self.max_concurrency)

        async def _run(item: Dict[str, Any]):
            async with sem:
                try:
                    return await self.request_async(**item)
                except Exception as exc:  # noqa: BLE001
                    if return_exceptions:
                        return exc
                    raise

        return await asyncio.gather(*(_run(item) for item in items))

    async def get_many_async(
        self,
        urls: Iterable[str],
        method: str = "GET",
        max_concurrency: Optional[int] = None,
        return_exceptions: bool = True,
        **kwargs,
    ) -> List[Any]:
        items = [{"method": method, "url": url, **kwargs} for url in urls]
        return await self.request_many_async(
            items,
            max_concurrency=max_concurrency,
            return_exceptions=return_exceptions,
        )

    # ------------------------------------------------------------------ #
    # Helpers / lifecycle
    # ------------------------------------------------------------------ #
    @staticmethod
    def _normalize(item: Union[Dict[str, Any], tuple]) -> Dict[str, Any]:
        if isinstance(item, dict):
            return item
        if isinstance(item, (list, tuple)):
            method, url = item[0], item[1]
            extra = item[2] if len(item) > 2 and isinstance(item[2], dict) else {}
            out: Dict[str, Any] = {"method": method, "url": url}
            out.update(extra)
            return out
        raise TypeError(f"Unsupported request item type: {type(item)!r}")

    def open(self) -> None:
        """Open / establish the underlying connection resources."""
        self.backend.open()

    async def open_async(self) -> None:
        """Async counterpart of :meth:`open`."""
        await self.backend.open_async()

    def close(self) -> None:
        self.backend.close()

    async def close_async(self) -> None:
        await self.backend.close_async()

    def __enter__(self) -> "HttpClient":
        self.open()
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    async def __aenter__(self) -> "HttpClient":
        await self.open_async()
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close_async()
