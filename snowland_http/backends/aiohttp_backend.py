"""Asynchronous backend built on the ``aiohttp`` library."""

from typing import Optional

import aiohttp

from snowland_http.backends.base import BaseBackend
from snowland_http.response import HttpResponse


class AiohttpBackend(BaseBackend):
    scheme = "async"

    def __init__(self, session: Optional[aiohttp.ClientSession] = None):
        self.session = session

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    async def open_async(self) -> None:
        # Create the session up front, bound to the current running loop.
        await self._get_session()

    async def request_async(self, method: str, url: str, **kwargs) -> HttpResponse:
        session = await self._get_session()
        async with session.request(method, url, **kwargs) as resp:
            content = await resp.read()
            return HttpResponse.from_aiohttp(resp, content)

    async def close_async(self) -> None:
        if self.session is not None and not self.session.closed:
            await self.session.close()
