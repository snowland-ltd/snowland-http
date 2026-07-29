"""Backend-agnostic HTTP response wrapper.

Regardless of which underlying library (requests / httpx / aiohttp) performed
the request, :class:`HttpResponse` exposes a single, stable interface so that
callers never have to branch on the active backend.
"""

import json
from typing import Any, Dict


class HttpResponse:
    def __init__(self, status_code: int, headers: Dict[str, str], content: bytes, url: str):
        self.status_code = status_code
        self.headers = headers
        self.content = content
        self.url = url

    @property
    def text(self) -> str:
        return self.content.decode("utf-8", errors="replace")

    @property
    def ok(self) -> bool:
        return 200 <= self.status_code < 300

    def json(self) -> Any:
        return json.loads(self.text)

    def raise_for_status(self) -> "HttpResponse":
        if not self.ok:
            raise RuntimeError(f"HTTP {self.status_code} for {self.url}")
        return self

    @classmethod
    def from_requests(cls, resp) -> "HttpResponse":
        return cls(
            status_code=resp.status_code,
            headers=dict(resp.headers),
            content=resp.content,
            url=str(resp.url),
        )

    @classmethod
    def from_httpx(cls, resp) -> "HttpResponse":
        return cls(
            status_code=resp.status_code,
            headers=dict(resp.headers),
            content=resp.content,
            url=str(resp.url),
        )

    @classmethod
    def from_aiohttp(cls, resp, content: bytes) -> "HttpResponse":
        return cls(
            status_code=resp.status,
            headers=dict(resp.headers),
            content=content,
            url=str(resp.url),
        )

    def __repr__(self) -> str:
        return f"<HttpResponse {self.status_code} {self.url}>"
