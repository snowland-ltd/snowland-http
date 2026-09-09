"""HttpClient basics: single requests, responses and simple parallelism.

Everything here works with zero third-party dependencies (the ``stdlib``
backend), so it runs in a plain Python environment.

Run:
    python examples/basic_client.py
"""

import importlib.util
import os
import sys

_EXAMPLES_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _EXAMPLES_DIR)  # for `from _demo_server import DemoServer`
sys.path.insert(0, os.path.dirname(_EXAMPLES_DIR))  # for `import snowland_http` from a checkout

from _demo_server import DemoServer

from snowland_http import HttpClient


def single_get(base):
    """One GET request with query params and headers."""
    print("=== single GET ===")
    client = HttpClient(backend="stdlib")
    resp = client.get(
        base + "/items/1",
        params={"trace": "demo"},
        headers={"X-Demo": "basic_client"},
    )
    print("status :", resp.status_code)
    print("ok     :", resp.ok)
    print("url    :", resp.url)
    print("text   :", resp.text)
    print("json   :", resp.json())


def inspect_response(base):
    """The HttpResponse object exposes the same API for every backend."""
    print("\n=== HttpResponse fields ===")
    client = HttpClient(backend="stdlib")
    resp = client.request("GET", base + "/hello")
    print("status_code :", resp.status_code)
    print("headers     :", {k: v for k, v in resp.headers.items() if k.lower() in ("content-type", "server")})
    print("content     :", resp.content)
    print("encoding    :", resp.encoding)
    print("text        :", resp.text)
    print("repr        :", repr(resp))


def post_json(base):
    """POST a JSON body and read the echo back."""
    print("\n=== POST json ===")
    client = HttpClient(backend="stdlib")
    resp = client.post(
        base + "/echo",
        json={"name": "snowland", "qty": 2},
        headers={"X-Request-Id": "abc-123"},
    )
    payload = resp.json()
    print("method sent :", payload["method"])
    print("body echoed :", payload["body"])
    print("request id  :", payload["headers"].get("X-Request-Id"))
    print("content type:", payload["headers"].get("Content-Type"))


def status_codes(base):
    """4xx/5xx are NOT exceptions: check `ok` or call raise_for_status()."""
    print("\n=== error status codes ===")
    client = HttpClient(backend="stdlib")
    resp = client.get(base + "/status/500")
    print("status :", resp.status_code, "ok:", resp.ok)
    try:
        resp.raise_for_status()
    except RuntimeError as exc:
        print("raise_for_status raised:", exc)


def parallel_get_many(base):
    """ThreadPoolExecutor based parallelism, results keep the input order."""
    print("\n=== get_many (thread pool) ===")
    client = HttpClient(backend="stdlib", max_workers=4)
    urls = [base + "/items/%d" % i for i in range(1, 6)]
    results = client.get_many(urls, max_workers=4)
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            print(i, "failed:", result)
        else:
            print(i, result.status_code, result.json()["name"])


def mixed_items(base):
    """request_many accepts dicts or (method, url, kwargs) tuples."""
    print("\n=== request_many with mixed item shapes ===")
    client = HttpClient(backend="stdlib")
    items = [
        {"method": "GET", "url": base + "/items/7"},
        ("GET", base + "/items/8", {"headers": {"X-Demo": "tuple-item"}}),
        {"method": "POST", "url": base + "/echo", "json": {"from": "dict-item"}},
    ]
    for result in client.request_many(items, max_workers=3):
        print(result.status_code, result.text[:80])


def lifecycle(base):
    """Explicit open()/close() versus the context manager."""
    print("\n=== lifecycle ===")
    client = HttpClient(backend="stdlib")
    client.open()
    try:
        print("manual open  ->", client.get(base + "/hello").status_code)
    finally:
        client.close()

    with HttpClient(backend="stdlib") as managed:
        print("context mgr  ->", managed.get(base + "/hello").status_code)


def backend_notes():
    """Which transports are installed in the current environment."""
    print("\n=== available backends ===")
    print("stdlib (urllib) : always available, sync only")
    for name in ("requests", "httpx", "aiohttp"):
        installed = importlib.util.find_spec(name) is not None
        print("%-16s: %s" % (name, "installed" if installed else "not installed (pip install %s)" % name))
    print('backend="auto" picks httpx > aiohttp > requests, else falls back to stdlib.')


def main():
    with DemoServer() as server:
        base = server.base_url
        print("demo server:", base)
        single_get(base)
        inspect_response(base)
        post_json(base)
        status_codes(base)
        parallel_get_many(base)
        mixed_items(base)
        lifecycle(base)
    backend_notes()


if __name__ == "__main__":
    main()
