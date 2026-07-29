"""snowland_http: a rate-limited, parallel HTTP client.

Transports (requests / httpx / aiohttp) are optional dependencies; install at
least one to use the client.
"""

from snowland_http.client import HttpClient
from snowland_http.exceptions import (
    AsyncRequiredError,
    BackendNotSupportedError,
    HttpClientError,
)
from snowland_http.ratelimit import RateLimitConfig, RateLimiter
from snowland_http.response import HttpResponse

__all__ = [
    "HttpClient",
    "HttpResponse",
    "RateLimitConfig",
    "RateLimiter",
    "HttpClientError",
    "BackendNotSupportedError",
    "AsyncRequiredError",
]
