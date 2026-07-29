"""Backend abstraction layer.

A *backend* knows how to actually perform an HTTP request using one of the
supported underlying libraries. The :class:`HttpClient` talks only to this
interface and never imports requests / httpx / aiohttp directly, so the
transport can be swapped at runtime.
"""

from snowland_http.response import HttpResponse


class BaseBackend:
    # One of: "sync" (blocking only), "async" (coroutine only), "both".
    scheme = "sync"

    def request(self, method: str, url: str, **kwargs) -> HttpResponse:
        raise NotImplementedError

    async def request_async(self, method: str, url: str, **kwargs) -> HttpResponse:
        raise NotImplementedError

    def open(self) -> None:
        """Open / (re)establish the underlying connection resources.

        Default no-op; backends that need an explicit connect step override it.
        """

    async def open_async(self) -> None:
        """Async counterpart of :meth:`open`."""

    def close(self) -> None:
        pass

    async def close_async(self) -> None:
        pass
