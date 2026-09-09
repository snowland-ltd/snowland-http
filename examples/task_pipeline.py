"""Task pipelines: pre-processing, post-processing and a small ETL flow.

A Task bundles one request with two optional hooks:

    pre_process(params)    mutates the request params before sending
                           (auth headers, signing, trace ids, ...)
    post_process(response) turns the response into whatever you want
                           (parsed JSON, a dict, a model, ...)

If post_process is omitted you get the raw HttpResponse back.

Run:
    python examples/task_pipeline.py
"""

import importlib.util
import os
import sys
import time

_EXAMPLES_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _EXAMPLES_DIR)  # for `from _demo_server import DemoServer`
sys.path.insert(0, os.path.dirname(_EXAMPLES_DIR))  # for `import snowland_http` from a checkout

from _demo_server import DemoServer

from snowland_http import SCHEDULING_IDLE, HttpClient, Orchestrator, Task

TOKEN = "demo-token"


def add_auth(params):
    """pre_process: inject auth + trace headers into the request params."""
    headers = dict(params.get("headers") or {})
    headers["Authorization"] = "Bearer " + TOKEN
    headers["X-Trace-Id"] = "trace-%d" % (time.time_ns() % 10000)
    params["headers"] = headers


def as_json(response):
    """post_process: fail loudly on non-2xx, otherwise return parsed JSON."""
    response.raise_for_status()
    return response.json()


def single_task(base):
    """A Task can be executed directly against any HttpClient."""
    print("=== one task, executed by hand ===")
    task = Task(
        request_params={"method": "GET", "url": base + "/items/42"},
        pre_process=add_auth,
        post_process=as_json,
        name="item-42",
    )
    print("task repr :", repr(task))
    with HttpClient(backend="stdlib") as client:
        print("result    :", task.execute(client))
        print("params now:", task.request_params)


def batch_of_tasks(base):
    """Same hooks, many tasks, run across the connection pool."""
    print("\n=== batch of tasks (thread mode) ===")
    tasks = [
        Task(
            request_params={"method": "GET", "url": base + "/items/%d" % i},
            pre_process=add_auth,
            post_process=as_json,
            name="item-%d" % i,
        )
        for i in range(1, 9)
    ]
    with Orchestrator(num_connections=4, backend="stdlib", scheduling=SCHEDULING_IDLE) as orch:
        results = orch.execute_tasks_parallel(tasks, mode="thread", return_exceptions=True)

    ok = [r for r in results if not isinstance(r, Exception)]
    print("fetched %d/%d items" % (len(ok), len(tasks)))
    print("names   :", [r["name"] for r in ok])


def paged_pipeline(base):
    """Fetch several pages concurrently, then aggregate the results.

    post_process does the reduction, so the orchestrator returns plain data
    instead of HttpResponse objects.
    """
    print("\n=== paged fetch + aggregation ===")
    page_size = 4

    def make_task(page):
        def to_items(response):
            payload = as_json(response)
            return payload["items"]

        return Task(
            request_params={
                "method": "GET",
                "url": base + "/page",
                "params": {"page": page, "size": page_size},
            },
            pre_process=add_auth,
            post_process=to_items,
            name="page-%d" % page,
        )

    tasks = [make_task(page) for page in (1, 2, 3)]
    with Orchestrator(num_connections=3, backend="stdlib") as orch:
        pages = orch.execute_tasks_parallel(tasks, mode="thread", return_exceptions=True)

    failed = [p for p in pages if isinstance(p, Exception)]
    if failed:
        print("failed pages:", failed)
        return

    flat = [item for page in pages for item in page]
    print("pages      :", [len(p) for p in pages])
    print("total items:", len(flat))
    print("first item :", flat[0])
    print("ids        :", [item["id"] for item in flat])


def dependent_steps(base):
    """When a step depends on the previous one, mix sequential and parallel."""
    print("\n=== dependent steps: discover, then fan out ===")
    client = HttpClient(backend="stdlib")
    first = Task(
        request_params={"method": "GET", "url": base + "/page", "params": {"page": 1, "size": 2}},
        post_process=as_json,
    ).execute(client)
    print("page 1 ->", first["items"], "has_more:", first["has_more"])

    if first["has_more"]:
        follow_ups = [
            Task(
                request_params={
                    "method": "GET",
                    "url": base + "/items/%s" % item["id"],
                },
                pre_process=add_auth,
                post_process=as_json,
                name="detail-%s" % item["id"],
            )
            for item in first["items"]
        ]
        with Orchestrator(num_connections=2, backend="stdlib") as orch:
            details = orch.execute_tasks_parallel(follow_ups, mode="thread", return_exceptions=True)
        print("details ->", details)


async def async_tasks(base, backend):
    """The very same Task objects run on the async paths."""
    print("\n=== async tasks (backend=%s) ===" % backend)
    tasks = [
        Task(
            request_params={"method": "GET", "url": base + "/items/%d" % i},
            pre_process=add_auth,
            post_process=as_json,
            name="async-%d" % i,
        )
        for i in range(1, 5)
    ]
    async with Orchestrator(num_connections=2, backend=backend, scheduling=SCHEDULING_IDLE) as orch:
        results = await orch.execute_tasks_async(tasks, mode="coroutine", return_exceptions=True)
    print("results:", [r["name"] for r in results if not isinstance(r, Exception)])


def main():
    with DemoServer() as server:
        base = server.base_url
        print("demo server:", base)
        single_task(base)
        batch_of_tasks(base)
        paged_pipeline(base)
        dependent_steps(base)

        backend = next(
            (n for n in ("httpx", "aiohttp") if importlib.util.find_spec(n) is not None), None
        )
        if backend is None:
            print("\n=== async tasks skipped ===")
            print("no async transport installed; run: pip install httpx")
            return

        import asyncio

        asyncio.run(async_tasks(base, backend))


if __name__ == "__main__":
    main()
