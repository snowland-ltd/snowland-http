"""Backend registry.

Concrete backends are imported lazily (inside :func:`build_backend`) so that
the package can be imported even when only a subset of the optional transport
libraries (requests / httpx / aiohttp) is installed.
"""

from snowland_http.backends.base import BaseBackend
from snowland_http.exceptions import BackendNotSupportedError


def detect_backend() -> str:
    for name, module in (("httpx", "httpx"), ("aiohttp", "aiohttp"), ("requests", "requests")):
        try:
            __import__(module)
            return name
        except ImportError:
            continue
    raise BackendNotSupportedError(
        "No HTTP backend available. Install one of: httpx, aiohttp, requests"
    )


def build_backend(name: str) -> BaseBackend:
    if name == "requests":
        from snowland_http.backends.requests_backend import RequestsBackend

        return RequestsBackend()
    if name == "httpx":
        from snowland_http.backends.httpx_backend import HttpxBackend

        return HttpxBackend()
    if name == "aiohttp":
        from snowland_http.backends.aiohttp_backend import AiohttpBackend

        return AiohttpBackend()
    raise BackendNotSupportedError(f"Unknown backend: {name}")


__all__ = ["BaseBackend", "detect_backend", "build_backend"]
