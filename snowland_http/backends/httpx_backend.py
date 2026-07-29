"""Backend built on the ``httpx`` library (supports both sync and async)."""

from typing import Optional

import httpx

from snowland_http.backends.base import BaseBackend
from snowland_http.response import HttpResponse


class HttpxBackend(BaseBackend):
    scheme = "both"

    def __init__(
        self,
        client: Optional[httpx.Client] = None,
        async_client: Optional[httpx.AsyncClient] = None,
    ):
        self.client = client or httpx.Client()
        self.async_client = async_client

    def request(self, method: str, url: str, **kwargs) -> HttpResponse:
        resp = self.client.request(method, url, **kwargs)
        return HttpResponse.from_httpx(resp)

    async def request_async(self, method: str, url: str, **kwargs) -> HttpResponse:
        if self.async_client is None:
            self.async_client = httpx.AsyncClient()
        resp = await self.async_client.request(method, url, **kwargs)
        return HttpResponse.from_httpx(resp)

    def close(self) -> None:
        self.client.close()

    async def close_async(self) -> None:
        if self.async_client is not None:
            await self.async_client.aclose()
