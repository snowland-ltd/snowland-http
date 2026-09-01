"""Example usage of the Orchestrator module.

This example demonstrates how to use the Orchestrator class to manage
multiple HTTP connections and execute requests in parallel or concurrently.
"""

import asyncio
from snowland_http import Orchestrator, RateLimitConfig, Task, execute_parallel_requests, execute_async_requests


def example_parallel_thread_mode():
    """Example: Parallel execution using thread mode."""
    print("=== Parallel Thread Mode Example ===")

    # Create orchestrator with 4 connections
    with Orchestrator(
        num_connections=4,
        backend="stdlib",
        rate_limit=RateLimitConfig(max_rate=10, burst=5),
    ) as orch:
        # Define URLs to request
        urls = [
            "https://httpbin.org/get/1",
            "https://httpbin.org/get/2",
            "https://httpbin.org/get/3",
            "https://httpbin.org/get/4",
        ]

        # Execute in parallel using thread mode
        results = orch.get_many_parallel(urls, mode="thread", return_exceptions=True)

        # Process results
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                print(f"Request {i} failed: {result}")
            else:
                print(f"Request {i} succeeded: {result.status_code}")


def example_parallel_process_mode():
    """Example: Parallel execution using process mode."""
    print("\n=== Parallel Process Mode Example ===")

    # Define request items
    items = [
        {"method": "GET", "url": "https://httpbin.org/get/1"},
        {"method": "GET", "url": "https://httpbin.org/get/2"},
        {"method": "GET", "url": "https://httpbin.org/get/3"},
    ]

    # Execute in parallel using process mode
    results = execute_parallel_requests(
        items,
        num_connections=2,
        backend="stdlib",
        mode="process",
        return_exceptions=True,
    )

    # Process results
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            print(f"Request {i} failed: {result}")
        else:
            print(f"Request {i} succeeded: {result.status_code}")


async def example_async_coroutine_mode():
    """Example: Async execution using coroutine mode."""
    print("\n=== Async Coroutine Mode Example ===")

    try:
        # Create orchestrator with async backend
        async with Orchestrator(
            num_connections=4,
            backend="httpx",
            rate_limit=RateLimitConfig(max_rate=20, burst=10),
        ) as orch:
            # Define URLs to request
            urls = [
                "https://httpbin.org/get/1",
                "https://httpbin.org/get/2",
                "https://httpbin.org/get/3",
                "https://httpbin.org/get/4",
            ]

            # Execute concurrently using coroutine mode
            results = await orch.get_many_async(urls, mode="coroutine", return_exceptions=True)

            # Process results
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    print(f"Request {i} failed: {result}")
                else:
                    print(f"Request {i} succeeded: {result.status_code}")
    except Exception as e:
        print(f"Async example skipped (httpx not available): {e}")


async def example_async_semaphore_mode():
    """Example: Async execution using semaphore mode."""
    print("\n=== Async Semaphore Mode Example ===")

    try:
        # Create orchestrator with async backend
        async with Orchestrator(
            num_connections=2,
            backend="httpx",
        ) as orch:
            # Define URLs to request
            urls = [
                "https://httpbin.org/get/1",
                "https://httpbin.org/get/2",
                "https://httpbin.org/get/3",
                "https://httpbin.org/get/4",
                "https://httpbin.org/get/5",
            ]

            # Execute concurrently with controlled concurrency
            results = await orch.get_many_async(
                urls,
                mode="semaphore",
                max_concurrency=3,
                return_exceptions=True,
            )

            # Process results
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    print(f"Request {i} failed: {result}")
                else:
                    print(f"Request {i} succeeded: {result.status_code}")
    except Exception as e:
        print(f"Async example skipped (httpx not available): {e}")


async def example_standalone_async():
    """Example: Standalone async function."""
    print("\n=== Standalone Async Function Example ===")

    try:
        # Define request items
        items = [
            {"method": "GET", "url": "https://httpbin.org/get/1"},
            {"method": "GET", "url": "https://httpbin.org/get/2"},
            {"method": "GET", "url": "https://httpbin.org/get/3"},
        ]

        # Execute using standalone function
        results = await execute_async_requests(
            items,
            num_connections=2,
            backend="httpx",
            mode="coroutine",
            return_exceptions=True,
        )

        # Process results
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                print(f"Request {i} failed: {result}")
            else:
                print(f"Request {i} succeeded: {result.status_code}")
    except Exception as e:
        print(f"Async example skipped (httpx not available): {e}")


def example_custom_requests():
    """Example: Custom request items with different methods and parameters."""
    print("\n=== Custom Requests Example ===")

    with Orchestrator(num_connections=2, backend="stdlib") as orch:
        # Define custom request items
        items = [
            {"method": "GET", "url": "https://httpbin.org/get", "params": {"key": "value"}},
            {"method": "POST", "url": "https://httpbin.org/post", "json": {"data": "test"}},
            ("GET", "https://httpbin.org/get", {"params": {"custom": "param"}}),
        ]

        # Execute in parallel
        results = orch.execute_parallel(items, mode="thread", return_exceptions=True)

        # Process results
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                print(f"Request {i} failed: {result}")
            else:
                print(f"Request {i} succeeded: {result.status_code}")


def example_connection_management():
    """Example: Manual connection management."""
    print("\n=== Connection Management Example ===")

    # Create orchestrator
    orch = Orchestrator(num_connections=3, backend="stdlib")

    # Open all connections
    orch.open_all()
    print(f"Opened {orch.num_connections} connections")

    # Get a specific connection
    conn = orch.get_connection(0)
    print(f"Got connection 0: {type(conn).__name__}")

    # Use the connection directly
    try:
        resp = conn.get("https://httpbin.org/get")
        print(f"Direct request succeeded: {resp.status_code}")
    except Exception as e:
        print(f"Direct request failed: {e}")

    # Close all connections
    orch.close_all()
    print("Closed all connections")


def example_task_with_pre_process():
    """Example: Task with pre-processing."""
    print("\n=== Task with Pre-processing Example ===")

    def add_auth(params):
        """Add authentication header to request."""
        params["headers"] = {"Authorization": "Bearer my_token"}

    with Orchestrator(num_connections=2, backend="stdlib") as orch:
        # Create tasks with pre-processing
        tasks = [
            Task(
                request_params={"method": "GET", "url": "https://httpbin.org/get"},
                pre_process=add_auth,
                name=f"auth_task_{i}",
            )
            for i in range(3)
        ]

        # Execute tasks in parallel
        results = orch.execute_tasks_parallel(tasks, mode="thread", return_exceptions=True)

        # Process results
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                print(f"Task {i} failed: {result}")
            else:
                print(f"Task {i} succeeded: {result.status_code}")


def example_task_with_post_process():
    """Example: Task with post-processing."""
    print("\n=== Task with Post-processing Example ===")

    def extract_json(response):
        """Extract JSON data from response."""
        return response.json()

    with Orchestrator(num_connections=2, backend="stdlib") as orch:
        # Create tasks with post-processing
        tasks = [
            Task(
                request_params={"method": "GET", "url": "https://httpbin.org/get"},
                post_process=extract_json,
                name=f"extract_task_{i}",
            )
            for i in range(3)
        ]

        # Execute tasks in parallel
        results = orch.execute_tasks_parallel(tasks, mode="thread", return_exceptions=True)

        # Process results
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                print(f"Task {i} failed: {result}")
            else:
                print(f"Task {i} succeeded: {type(result).__name__}")


def example_task_with_both():
    """Example: Task with both pre and post processing."""
    print("\n=== Task with Both Pre and Post-processing Example ===")

    def add_auth(params):
        """Add authentication header to request."""
        params["headers"] = {"Authorization": "Bearer my_token"}

    def extract_json(response):
        """Extract JSON data from response."""
        return response.json()

    with Orchestrator(num_connections=2, backend="stdlib") as orch:
        # Create tasks with both pre and post processing
        tasks = [
            Task(
                request_params={"method": "GET", "url": "https://httpbin.org/get"},
                pre_process=add_auth,
                post_process=extract_json,
                name=f"full_task_{i}",
            )
            for i in range(3)
        ]

        # Execute tasks in parallel
        results = orch.execute_tasks_parallel(tasks, mode="thread", return_exceptions=True)

        # Process results
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                print(f"Task {i} failed: {result}")
            else:
                print(f"Task {i} succeeded: {type(result).__name__}")


async def example_async_task_execution():
    """Example: Async task execution."""
    print("\n=== Async Task Execution Example ===")

    try:
        def extract_json(response):
            """Extract JSON data from response."""
            return response.json()

        async with Orchestrator(num_connections=2, backend="httpx") as orch:
            # Create tasks with post-processing
            tasks = [
                Task(
                    request_params={"method": "GET", "url": "https://httpbin.org/get"},
                    post_process=extract_json,
                    name=f"async_task_{i}",
                )
                for i in range(3)
            ]

            # Execute tasks concurrently
            results = await orch.execute_tasks_async(tasks, mode="coroutine", return_exceptions=True)

            # Process results
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    print(f"Task {i} failed: {result}")
                else:
                    print(f"Task {i} succeeded: {type(result).__name__}")
    except Exception as e:
        print(f"Async example skipped (httpx not available): {e}")


def example_task_with_custom_logic():
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
            "url": data.get("url"),
            "headers": data.get("headers", {}),
            "args": data.get("args", {}),
        }

    with Orchestrator(num_connections=2, backend="stdlib") as orch:
        # Create tasks with custom logic
        tasks = [
            Task(
                request_params={"method": "GET", "url": "https://httpbin.org/get"},
                pre_process=prepare_request,
                post_process=process_response,
                name=f"custom_task_{i}",
            )
            for i in range(2)
        ]

        # Execute tasks in parallel
        results = orch.execute_tasks_parallel(tasks, mode="thread", return_exceptions=True)

        # Process results
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                print(f"Task {i} failed: {result}")
            else:
                print(f"Task {i} succeeded: {result}")


if __name__ == "__main__":
    # Run sync examples
    example_parallel_thread_mode()
    example_parallel_process_mode()
    example_custom_requests()
    example_connection_management()

    # Run task examples
    example_task_with_pre_process()
    example_task_with_post_process()
    example_task_with_both()
    example_task_with_custom_logic()

    # Run async examples
    asyncio.run(example_async_coroutine_mode())
    asyncio.run(example_async_semaphore_mode())
    asyncio.run(example_standalone_async())
    asyncio.run(example_async_task_execution())