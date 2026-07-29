"""Synchronous backend built on the ``requests`` library."""

from typing import Optional

import requests

from snowland_http.backends.base import BaseBackend
from snowland_http.response import HttpResponse


class RequestsBackend(BaseBackend):
    scheme = "sync"

    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or requests.Session()

    def request(self, method: str, url: str, **kwargs) -> HttpResponse:
        resp = self.session.request(method, url, **kwargs)
        return HttpResponse.from_requests(resp)

    def close(self) -> None:
        self.session.close()
