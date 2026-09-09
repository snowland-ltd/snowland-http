"""Shared demo HTTP server used by the examples in this folder.

The examples must run offline and without any third-party transport
(requests / httpx / aiohttp), so they talk to this local server through the
zero-dependency ``stdlib`` backend (urllib).

Routes
------
GET  /hello                  -> {"message": "hello"}
GET  /items/<id>             -> {"id": <id>, "name": "item-<id>", "stock": n}
GET  /page?page=&size=       -> {"page": n, "size": k, "items": [...], "has_more": bool}
GET  /delay/<ms>             -> sleeps <ms> milliseconds, then {"slept_ms": <ms>}
GET  /status/<code>          -> replies with the given HTTP status code
GET  /flaky?fail_rate=<0..1> -> randomly fails with HTTP 500
ANY  /echo                   -> echoes method, path, query, headers and body
GET  /stats                  -> {"in_flight": n, "max_in_flight": n, "requests": n}
GET  /stats/reset            -> zeroes the counters above

The server binds to 127.0.0.1 on a random free port, so no configuration and
no network access are required.
"""

import json
import random
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

TOTAL_PAGES = 3

# Server side counters; let the examples observe how many requests really run
# at the same time (that is how the scheduling strategies differ).
_STATS = {"in_flight": 0, "max_in_flight": 0, "requests": 0}
_STATS_LOCK = threading.Lock()


def _enter_request():
    with _STATS_LOCK:
        _STATS["in_flight"] += 1
        _STATS["requests"] += 1
        _STATS["max_in_flight"] = max(_STATS["max_in_flight"], _STATS["in_flight"])


def _leave_request():
    with _STATS_LOCK:
        _STATS["in_flight"] -= 1


def reset_stats():
    """Zero the counters (calls to /stats itself are not counted)."""
    with _STATS_LOCK:
        _STATS.update({"in_flight": 0, "max_in_flight": 0, "requests": 0})


def snapshot_stats():
    """Return a copy of the current counters."""
    with _STATS_LOCK:
        return dict(_STATS)


class _DemoHandler(BaseHTTPRequestHandler):
    # Keep-alive; ThreadingHTTPServer serves each connection on its own thread.
    protocol_version = "HTTP/1.1"
    server_version = "snowland-demo/1.0"

    def log_message(self, fmt, *args):
        """Silence the default stderr access log (examples print their own)."""

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    def _read_body(self) -> bytes:
        length = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(length) if length else b""

    def _respond(self, code, payload, content_type="application/json", body=True):
        raw = payload if isinstance(payload, bytes) else json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        if body:
            self.wfile.write(raw)

    def _query(self):
        return parse_qs(urlparse(self.path).query)

    # ------------------------------------------------------------------ #
    # Routing
    # ------------------------------------------------------------------ #
    def do_GET(self):
        self._handle("GET")

    def do_POST(self):
        self._handle("POST")

    def do_PUT(self):
        self._handle("PUT")

    def do_PATCH(self):
        self._handle("PATCH")

    def do_DELETE(self):
        self._handle("DELETE")

    def do_HEAD(self):
        self._handle("HEAD")

    def _handle(self, method):
        counted = urlparse(self.path).path not in ("/stats", "/stats/reset")
        if counted:
            _enter_request()
        try:
            self._dispatch(method)
        finally:
            if counted:
                _leave_request()

    def _dispatch(self, method):
        parsed = urlparse(self.path)
        path = parsed.path
        query = self._query()
        request_body = self._read_body()

        if path == "/stats":
            self._respond(200, snapshot_stats(), body=(method != "HEAD"))
            return

        if path == "/stats/reset":
            reset_stats()
            self._respond(200, {"reset": True}, body=(method != "HEAD"))
            return

        if path == "/echo":
            self._respond(
                200,
                {
                    "method": method,
                    "path": path,
                    "query": {k: v[0] for k, v in query.items()},
                    "headers": dict(self.headers),
                    "body": request_body.decode("utf-8", "replace"),
                },
                body=(method != "HEAD"),
            )
            return

        if path == "/hello":
            self._respond(200, {"message": "hello"}, body=(method != "HEAD"))
            return

        if path.startswith("/delay/"):
            ms = int(float(path.rsplit("/", 1)[1]))
            time.sleep(ms / 1000.0)
            self._respond(200, {"slept_ms": ms}, body=(method != "HEAD"))
            return

        if path.startswith("/items/"):
            item_id = path.rsplit("/", 1)[1]
            self._respond(
                200,
                {"id": item_id, "name": "item-%s" % item_id, "stock": len(item_id) * 7 % 50},
                body=(method != "HEAD"),
            )
            return

        if path == "/page":
            page = int(query.get("page", ["1"])[0])
            size = int(query.get("size", ["3"])[0])
            items = [
                {"id": str((page - 1) * size + i + 1), "name": "item-%d" % ((page - 1) * size + i + 1)}
                for i in range(size)
            ]
            self._respond(
                200,
                {"page": page, "size": size, "items": items, "has_more": page < TOTAL_PAGES},
                body=(method != "HEAD"),
            )
            return

        if path.startswith("/status/"):
            code = int(path.rsplit("/", 1)[1])
            self._respond(code, {"status": code, "error": "simulated failure"}, body=(method != "HEAD"))
            return

        if path == "/flaky":
            fail_rate = float(query.get("fail_rate", ["0.5"])[0])
            if random.random() < fail_rate:
                self._respond(500, {"error": "simulated flaky failure"}, body=(method != "HEAD"))
            else:
                self._respond(200, {"ok": True}, body=(method != "HEAD"))
            return

        self._respond(404, {"error": "no route for %s" % path}, body=(method != "HEAD"))


class _DemoHTTPServer(ThreadingHTTPServer):
    """ThreadingHTTPServer that does not shout when a client walks away.

    Some examples abort a request on purpose (timeout demo), which makes the
    socket layer raise; that is expected noise, not a server failure.
    """

    daemon_threads = True

    def handle_error(self, request, client_address):
        exc = sys.exc_info()[1]
        if isinstance(exc, (ConnectionResetError, ConnectionAbortedError, BrokenPipeError)):
            return
        super().handle_error(request, client_address)


class DemoServer:
    """Context manager serving the demo routes on a random local port.

    Usage:
        with DemoServer() as server:
            url = server.base_url + "/hello"
    """

    def __init__(self, host="127.0.0.1", port=0):
        self._server = _DemoHTTPServer((host, port), _DemoHandler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def base_url(self) -> str:
        host, port = self._server.server_address[:2]
        return "http://%s:%d" % (host, port)

    def __enter__(self) -> "DemoServer":
        self._thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)


if __name__ == "__main__":
    with DemoServer() as demo:
        print("demo server on", demo.base_url)
        print("try: curl", demo.base_url + "/hello")
        time.sleep(30)
