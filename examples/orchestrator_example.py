"""Example usage of the Orchestrator module.

This example demonstrates how to use the Orchestrator class to manage
multiple HTTP connections and execute requests in parallel or concurrently.

Like every other example in this folder it talks to the local demo server
(`_demo_server.py`) through the zero-dependency `stdlib` backend, so it needs
neither internet access nor a third-party transport.

Run:
    python examples/orchestrator_example.py
"""

import asyncio
import importlib.util
import os
import sys

_EXAMPLES_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _EXAMPLES_DIR)  # for `from _demo_server import DemoServer`
sys.path.insert(0, os.path.dirname(_EXAMPLES_DIR))  # for `import snowland_http` from a checkout

from _demo_server import DemoServer

from snowland_http import (
    Orchestrator,
    RateLimitConfig,
    Task,
    execute_async_requests,
    execute_parallel_requests,
)

TOKEN = "demo-token"


def _report(results):
    """Print one line per result: status code, or the exception type."""
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            print("  %d failed: %s: %s" % (i, type(result).__name__, result))
        elif hasattr(result, "status_code"):
            print("  %d succeeded: %s" % (i, result.status_code))
        else:
            print("  %d succeeded: %r" % (i, result))


def _async_backend():
    """Return an installed async backend name, or None."""
    for name in ("httpx", "aiohttp"):
        if importlib.util.find_spec(name) is not None:
            return name
    return None


def example_parallel_thread_mode(base):
    """Example: Parallel execution using thread mode."""
    print("=== Parallel Thread Mode Example ===")

    # Create orchestrator with 4 connections
    with Orchestrator(
        num_connections=4,
        backend="stdlib",
        rate_limit=RateLimitConfig(max_rate=10, burst=5),
    ) as orch:
        # Define URLs to request
        urls = [base + "/items/%d" % i for i in range(1, 5)]

        # Execute in parallel using thread mode
        results = orch.get_many_parallel(urls, mode="thread", return_exceptions=True)

        # Process results (same order as the input URLs)
        _report(results)


def example_parallel_process_mode(base):
    """Example: Parallel execution using process mode."""
    print("\n=== Parallel Process Mode Example ===")

    # Define request items
    items = [{"method": "GET", "url": base + "/items/%d" % i} for i in (1, 2, 3)]

    # Execute in parallel using process mode
    results = execute_parallel_requests(
        items,
        num_connections=2,
        backend="stdlib",
        mode="process",
        return_exceptions=True,
    )

    # Process results
    _report(results)


async def example_async_coroutine_mode(base, backend):
    """Example: Async execution using coroutine mode."""
    print("\n=== Async Coroutine Mode Example ===")

    # Create orchestrator with async backend
    async with Orchestrator(
        num_connections=4,
        backend=backend,
        rate_limit=RateLimitConfig(max_rate=20, burst=10),
    ) as orch:
        # Define URLs to request
        urls = [base + "/items/%d" % i for i in range(1, 5)]

        # Execute concurrently using coroutine mode
        results = await orch.get_many_async(urls, mode="coroutine", return_exceptions=True)

        # Process results
        _report(results)


async def example_async_semaphore_mode(base, backend):
    """Example: Async execution using semaphore mode."""
    print("\n=== Async Semaphore Mode Example ===")

    # Create orchestrator with async backend
    async with Orchestrator(
        num_connections=2,
        backend=backend,
    ) as orch:
        # Define URLs to request
        urls = [base + "/items/%d" % i for i in range(1, 6)]

        # Execute concurrently with controlled concurrency
        results = await orch.get_many_async(
            urls,
            mode="semaphore",
            max_concurrency=3,
            return_exceptions=True,
        )

        # Process results
        _report(results)


async def example_standalone_async(base, backend):
    """Example: Standalone async function."""
    print("\n=== Standalone Async Function Example ===")

    # Define request items
    items = [{"method": "GET", "url": base + "/items/%d" % i} for i in (1, 2, 3)]

    # Execute using standalone function
    results = await execute_async_requests(
        items,
        num_connections=2,
        backend=backend,
        mode="coroutine",
        return_exceptions=True,
    )

    # Process results
    _report(results)


def example_custom_requests(base):
    """Example: Custom request items with different methods and parameters."""
    print("\n=== Custom Requests Example ===")

    with Orchestrator(num_connections=2, backend="stdlib") as orch:
        # Define custom request items (dicts and tuples are both accepted)
        items = [
            {"method": "GET", "url": base + "/items/1", "params": {"key": "value"}},
            {"method": "POST", "url": base + "/echo", "json": {"data": "test"}},
            ("GET", base + "/items/2", {"headers": {"X-Demo": "tuple-item"}}),
        ]

        # Execute in parallel
        results = orch.execute_parallel(items, mode="thread", return_exceptions=True)

        # Process results
        _report(results)


def example_connection_management(base):
    """Example: Manual connection management."""
    print("\n=== Connection Management Example ===")

    # Create orchestrator
    orch = Orchestrator(num_connections=3, backend="stdlib")

    # Open all connections
    orch.open_all()
    print("Opened %d connections" % orch.num_connections)

    # Get a specific connection
    conn = orch.get_connection(0)
    print("Got connection 0: %s" % type(conn).__name__)

    # Use the connection directly
    try:
        resp = conn.get(base + "/items/1")
        print("Direct request succeeded: %s" % resp.status_code)
    except Exception as e:
        print("Direct request failed: %s" % e)

    # Close all connections
    orch.close_all()
    print("Closed all connections")


def example_task_with_pre_process(base):
    """Example: Task with pre-processing."""
    print("\n=== Task with Pre-processing Example ===")

    def add_auth(params):
        """Add authentication header to request."""
        params["headers"] = {"Authorization": "Bearer " + TOKEN}

    with Orchestrator(num_connections=2, backend="stdlib") as orch:
        # Create tasks with pre-processing
        tasks = [
            Task(
                request_params={"method": "GET", "url": base + "/items/1"},
                pre_process=add_auth,
                name="auth_task_%d" % i,
            )
            for i in range(3)
        ]

        # Execute tasks in parallel
        results = orch.execute_tasks_parallel(tasks, mode="thread", return_exceptions=True)

        # Process results
        _report(results)


def example_task_with_post_process(base):
    """Example: Task with post-processing."""
    print("\n=== Task with Post-processing Example ===")

    def extract_json(response):
        """Extract JSON data from response."""
        return response.json()

    with Orchestrator(num_connections=2, backend="stdlib") as orch:
        # Create tasks with post-processing
        tasks = [
            Task(
                request_params={"method": "GET", "url": base + "/items/%d" % (i + 1)},
                post_process=extract_json,
                name="extract_task_%d" % i,
            )
            for i in range(3)
        ]

        # Execute tasks in parallel
        results = orch.execute_tasks_parallel(tasks, mode="thread", return_exceptions=True)

        # Process results
        _report(results)


def example_task_with_both(base):
    """Example: Task with both pre and post processing."""
    print("\n=== Task with Both Pre and Post-processing Example ===")

    def add_auth(params):
        """Add authentication header to request."""
        params["headers"] = {"Authorization": "Bearer " + TOKEN}

    def extract_json(response):
        """Extract JSON data from response."""
        return response.json()

    with Orchestrator(num_connections=2, backend="stdlib") as orch:
        # Create tasks with both pre and post processing
        tasks = [
            Task(
                request_params={"method": "GET", "url": base + "/items/%d" % (i + 1)},
                pre_process=add_auth,
                post_process=extract_json,
                name="full_task_%d" % i,
            )
            for i in range(3)
        ]

        # Execute tasks in parallel
        results = orch.execute_tasks_parallel(tasks, mode="thread", return_exceptions=True)

        # Process results
        _report(results)


async def example_async_task_execution(base, backend):
    """Example: Async task execution."""
    print("\n=== Async Task Execution Example ===")

    def extract_json(response):
        """Extract JSON data from response."""
        return response.json()

    async with Orchestrator(num_connections=2, backend=backend) as orch:
        # Create tasks with post-processing
        tasks = [
            Task(
                request_params={"method": "GET", "url": base + "/items/%d" % (i + 1)},
                post_process=extract_json,
                name="async_task_%d" % i,
            )
            for i in range(3)
        ]

        # Execute tasks concurrently
        results = await orch.execute_tasks_async(tasks, mode="coroutine", return_exceptions=True)

        # Process results
        _report(results)


def example_task_with_custom_logic(base):
    """Example: Task with custom pre/post processing logic."""
    print("\n=== Task with Custom Logic Example ===")

    def prepare_request(params):
        """Custom request preparation."""
        # Add custom headers
        params["headers"] = {
            "User-Agent": "MyApp/1.0",
            "Accept": "application/json",
        }
        # Add query parameters
        params["params"] = {"custom": "value"}

    def process_response(response):
        """Custom response processing."""
        data = response.json()
        # Extract specific fields
        return {
            "method": data.get("method"),
            "path": data.get("path"),
            "query": data.get("query", {}),
            "user_agent": data.get("headers", {}).get("User-Agent"),
        }

    with Orchestrator(num_connections=2, backend="stdlib") as orch:
        # Create tasks with custom logic
        tasks = [
            Task(
                request_params={"method": "GET", "url": base + "/echo"},
                pre_process=prepare_request,
                post_process=process_response,
                name="custom_task_%d" % i,
            )
            for i in range(2)
        ]

        # Execute tasks in parallel
        results = orch.execute_tasks_parallel(tasks, mode="thread", return_exceptions=True)

        # Process results
        _report(results)


def main():
    with DemoServer() as server:
        base = server.base_url
        print("demo server: %s" % base)

        # Run sync examples
        example_parallel_thread_mode(base)
        example_parallel_process_mode(base)
        example_custom_requests(base)
        example_connection_management(base)

        # Run task examples
        example_task_with_pre_process(base)
        example_task_with_post_process(base)
        example_task_with_both(base)
        example_task_with_custom_logic(base)

        # Run async examples
        backend = _async_backend()
        if backend is None:
            print("\n=== async examples skipped ===")
            print("no async transport installed; run: pip install httpx")
            return

        asyncio.run(example_async_coroutine_mode(base, backend))
        asyncio.run(example_async_semaphore_mode(base, backend))
        asyncio.run(example_standalone_async(base, backend))
        asyncio.run(example_async_task_execution(base, backend))


if __name__ == "__main__":
    main()
