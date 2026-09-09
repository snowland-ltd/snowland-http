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
from snowland_http.orchestrator import (
    SCHEDULING_IDLE,
    SCHEDULING_ROUND_ROBIN,
    Orchestrator,
    Task,
    execute_async_requests,
    execute_parallel_requests,
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
    "Orchestrator",
    "Task",
    "execute_parallel_requests",
    "execute_async_requests",
    "SCHEDULING_ROUND_ROBIN",
    "SCHEDULING_IDLE",
]


version = (0, 3, 1, "final", 0)
__version__ = "0.3.1"
