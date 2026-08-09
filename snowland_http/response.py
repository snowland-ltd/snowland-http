"""Backend-agnostic HTTP response wrapper.

Regardless of which underlying library (requests / httpx / aiohttp / urllib)
performed the request, :class:`HttpResponse` exposes a single, stable interface
so that callers never have to branch on the active backend.
"""

import json
from typing import Any, Dict, Optional

from snowland_http._encoding import parse_charset


class HttpResponse:
    def __init__(
        self,
        status_code: int,
        headers: Dict[str, str],
        content: bytes,
        url: str,
        encoding: Optional[str] = None,
    ):
        self.status_code = status_code
        self.headers = headers
        self.content = content
        self.url = url
        # Resolve the text encoding from the explicit argument, else from the
        # Content-Type charset, else fall back to ISO-8859-1 (the HTTP default
        # per RFC 7231). We never assume UTF-8.
        self.encoding = encoding or parse_charset(headers) or "iso-8859-1"

    @property
    def text(self) -> str:
        # Decode using the response's declared encoding. We intentionally do NOT
        # use errors="replace": a wrong encoding must surface as UnicodeDecodeError
        # so the caller learns the body is not valid text in that charset, rather
        # than silently getting mojibake.
        return self.content.decode(self.encoding)

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
            encoding=resp.encoding,
        )

    @classmethod
    def from_httpx(cls, resp) -> "HttpResponse":
        return cls(
            status_code=resp.status_code,
            headers=dict(resp.headers),
            content=resp.content,
            url=str(resp.url),
            encoding=resp.encoding,
        )

    @classmethod
    def from_aiohttp(cls, resp, content: bytes) -> "HttpResponse":
        return cls(
            status_code=resp.status,
            headers=dict(resp.headers),
            content=content,
            url=str(resp.url),
            encoding=resp.charset,
        )

    @classmethod
    def from_urllib(cls, resp, content: bytes) -> "HttpResponse":
        # ``resp`` is an ``http.client.HTTPResponse`` (also the case for
        # ``urllib.error.HTTPError``). ``resp.url`` is available on both.
        # http.client already exposes the Content-Type charset via .headers; we
        # let parse_charset derive it, so we don't pass an explicit encoding.
        return cls(
            status_code=resp.status,
            headers=dict(resp.headers),
            content=content,
            url=resp.geturl(),
        )

    def __repr__(self) -> str:
        return f"<HttpResponse {self.status_code} {self.url}>"
