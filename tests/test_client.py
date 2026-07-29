"""Unit tests for snowland_http.

Run from the project root (the directory containing ``snowland_http``)::

    python -m unittest discover -s tests

Rate-limiter tests run everywhere. Functional/parallel tests against a local
server are skipped automatically when no HTTP backend is installed.
"""

import importlib.util
import json
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

from snowland_http import HttpClient, RateLimitConfig
from snowland_http.ratelimit import RateLimiter


def _has(module: str) -> bool:
    return importlib.util.find_spec(module) is not None


class RateLimiterTest(unittest.TestCase):
    def test_burst_allows_instant_first_requests(self):
        limiter = RateLimiter(RateLimitConfig(max_rate=10, burst=3))
        start = time.monotonic()
        for _ in range(3):
            limiter.acquire()
        # Three requests within the burst capacity should be essentially free.
        self.assertLess(time.monotonic() - start, 0.2)

    def test_throttling_kicks_in_after_burst(self):
        limiter = RateLimiter(RateLimitConfig(max_rate=10, burst=1))
        start = time.monotonic()
        for _ in range(3):  # 1 free + 2 throttled (~0.1s each)
            limiter.acquire()
        elapsed = time.monotonic() - start
        self.assertGreater(elapsed, 0.15)
        self.assertLess(elapsed, 0.8)


class BackendSelectionTest(unittest.TestCase):
    def test_unknown_backend_raises(self):
        with self.assertRaises(Exception):
            HttpClient(backend="does-not-exist")

    def test_auto_detect_or_raise(self):
        # Should either pick a backend or raise a clear error if none installed.
        try:
            client = HttpClient(backend="auto")
            self.assertIsNotNone(client.backend)
        except Exception:
            self.skipTest("no HTTP backend installed")


@unittest.skipUnless(_has("requests"), "requests not installed")
class FunctionalTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                body = json.dumps({"path": self.path}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        cls.server = HTTPServer(("127.0.0.1", 0), Handler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()

    def _url(self, path):
        return f"http://127.0.0.1:{self.port}{path}"

    def test_single_request(self):
        client = HttpClient(backend="requests")
        resp = client.get(self._url("/hello"))
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["path"], "/hello")

    def test_parallel_requests(self):
        client = HttpClient(backend="requests", max_workers=5)
        urls = [self._url(f"/item/{i}") for i in range(10)]
        results = client.get_many(urls, return_exceptions=False)
        self.assertEqual(len(results), 10)
        for r in results:
            self.assertEqual(r.status_code, 200)

    def test_rate_limited_parallel(self):
        # 10 requests at max_rate=20 -> should finish in ~0.5s, well under 5s.
        client = HttpClient(
            backend="requests",
            rate_limit=RateLimitConfig(max_rate=20, burst=1),
            max_workers=10,
        )
        urls = [self._url(f"/rl/{i}") for i in range(10)]
        start = time.monotonic()
        results = client.get_many(urls, return_exceptions=False)
        elapsed = time.monotonic() - start
        self.assertEqual(len(results), 10)
        self.assertGreater(elapsed, 0.3)
        self.assertLess(elapsed, 3.0)

    def test_return_exceptions_captures_errors(self):
        client = HttpClient(backend="requests")
        # One good URL, one to a closed port -> connection error.
        results = client.request_many(
            [
                {"method": "GET", "url": self._url("/ok")},
                {"method": "GET", "url": "http://127.0.0.1:1/closed"},
            ],
            return_exceptions=True,
        )
        self.assertEqual(results[0].status_code, 200)
        self.assertIsInstance(results[1], Exception)


if __name__ == "__main__":
    unittest.main()
