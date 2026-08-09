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
    # No third-party transport is installed: fall back to the dependency-free
    # standard-library backend (urllib) so HttpClient(backend="auto") still works
    # in a bare-Python environment. Note it is synchronous-only.
    return "stdlib"


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
    if name in ("stdlib", "urllib"):
        from snowland_http.backends.stdlib_backend import StdlibBackend

        return StdlibBackend()
    raise BackendNotSupportedError(f"Unknown backend: {name}")


__all__ = ["BaseBackend", "detect_backend", "build_backend"]
