"""Exceptions raised by the snowland_http client."""


class HttpClientError(Exception):
    """Base class for all snowland_http errors."""


class BackendNotSupportedError(HttpClientError):
    """Raised when the requested backend is unavailable or unknown."""


class AsyncRequiredError(HttpClientError):
    """Raised when an async-only backend is used via the sync API (or vice versa)."""
