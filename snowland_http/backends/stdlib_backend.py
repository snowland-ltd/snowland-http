"""Synchronous backend built on the Python standard library (urllib).

This backend has NO third-party dependencies, so it works even when requests /
httpx / aiohttp are not installed. It supports the subset of request options the
standard library can express: method, url, headers, query params, a request
body (raw bytes / str / dict form-encoded / JSON), and a timeout.

Async requests are NOT supported by urllib; use httpx/aiohttp for that.
"""

import json
import urllib.parse
import urllib.request
from typing import Any, Dict, Optional

from snowland_http.backends.base import BaseBackend
from snowland_http.response import HttpResponse


class StdlibBackend(BaseBackend):
    """Transport using only :mod:`urllib.request` (Python standard library)."""

    scheme = "sync"

    def __init__(self, opener: Optional[urllib.request.OpenerDirector] = None):
        # A default opener is sufficient for most use cases; a custom opener can
        # be supplied to add handlers (proxy, cookie, custom redirect, etc.).
        self.opener = opener or urllib.request.build_opener()

    # ------------------------------------------------------------------ #
    # Argument mapping
    # ------------------------------------------------------------------ #
    @staticmethod
    def _build_data(kwargs: Dict[str, Any]):
        """Normalise the various body representations into (bytes, headers)."""
        headers = dict(kwargs.get("headers") or {})
        data = kwargs.get("data")
        json_body = kwargs.get("json")

        if json_body is not None:
            if data is not None:
                raise ValueError("Cannot pass both `data` and `json`.")
            payload = json.dumps(json_body).encode("utf-8")
            # Only set Content-Type if the caller did not already provide one.
            if "Content-Type" not in headers and "content-type" not in headers:
                headers["Content-Type"] = "application/json"
            return payload, headers

        if data is None:
            return None, headers

        if isinstance(data, (bytes, bytearray)):
            return bytes(data), headers
        if isinstance(data, str):
            return data.encode("utf-8"), headers
        if isinstance(data, dict):
            # Form-encoded by default.
            if "Content-Type" not in headers and "content-type" not in headers:
                headers["Content-Type"] = "application/x-www-form-urlencoded"
            return urllib.parse.urlencode(data).encode("utf-8"), headers
        raise TypeError(f"Unsupported body type: {type(data)!r}")

    @staticmethod
    def _merge_query(url: str, params: Optional[Dict[str, Any]]) -> str:
        if not params:
            return url
        parsed = urllib.parse.urlsplit(url)
        existing = dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))
        existing.update({str(k): str(v) for k, v in params.items()})
        new_query = urllib.parse.urlencode(existing)
        return urllib.parse.urlunsplit(parsed._replace(query=new_query))

    def request(self, method: str, url: str, **kwargs) -> HttpResponse:
        url = self._merge_query(url, kwargs.get("params"))
        data, headers = self._build_data(kwargs)
        timeout = kwargs.get("timeout")

        req = urllib.request.Request(url, data=data, method=method.upper())
        for key, value in headers.items():
            req.add_header(key, value)

        try:
            raw = self.opener.open(req, timeout=timeout)
        except urllib.error.HTTPError as exc:  # HTTP-level error (4xx/5xx)
            return HttpResponse.from_urllib(exc, exc.read())
        except urllib.error.URLError as exc:
            raise ConnectionError(f"Request to {url} failed: {exc.reason}") from exc

        body = raw.read()
        return HttpResponse.from_urllib(raw, body)
