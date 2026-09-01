"""Orchestrator for managing multiple HTTP connections with parallel/concurrent execution.

This module provides high-level orchestration for running HTTP requests across
multiple connections using either multiprocessing or asyncio coroutines.

Features
--------
* Multi-process parallel execution for CPU-bound or I/O-bound workloads
* Asyncio-based concurrent execution for high-throughput I/O operations
* Connection pooling across multiple HttpClient instances
* Load balancing across connections
* Unified interface for both sync and async workflows
* Task-based execution with pre-processing and post-processing hooks

Quick start
-----------
    from snowland_http import HttpClient, Orchestrator, Task

    # Create orchestrator with 4 connections
    orchestrator = Orchestrator(
        num_connections=4,
        backend="httpx",
        rate_limit=RateLimitConfig(max_rate=10, burst=5)
    )

    # Execute requests in parallel (multi-process)
    urls = ["https://api.example.com/data/1", "https://api.example.com/data/2"]
    results = orchestrator.execute_parallel(urls, mode="process")

    # Execute requests concurrently (asyncio)
    results = await orchestrator.execute_async(urls, mode="coroutine")

    # Execute tasks with pre/post processing
    def pre_process(params):
        params["headers"] = {"Authorization": "Bearer token"}

    def post_process(response):
        return response.json()

    task = Task(
        request_params={"method": "GET", "url": "https://api.example.com/data"},
        pre_process=pre_process,
        post_process=post_process,
    )
    results = orchestrator.execute_tasks_parallel([task], mode="thread")
"""

import asyncio
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Union

from snowland_http.client import HttpClient
from snowland_http.ratelimit import RateLimitConfig
from snowland_http.response import HttpResponse


class Task:
    """Represents an HTTP task with optional pre-processing and post-processing.

    A task encapsulates an HTTP request along with optional hooks for
    pre-processing (before the request) and post-processing (after the response).

    Args:
        request_params: HTTP request parameters (method, url, headers, etc.).
        pre_process: Optional function to call before the request.
            Receives request_params dict and can modify it in-place.
        post_process: Optional function to call after the response.
            Receives HttpResponse and returns the processed result.
        name: Optional task name for identification and logging.

    Example:
        >>> def add_auth(params):
        ...     params["headers"] = {"Authorization": "Bearer token"}
        >>>
        >>> def extract_data(response):
        ...     return response.json()
        >>>
        >>> task = Task(
        ...     request_params={"method": "GET", "url": "https://api.example.com/data"},
        ...     pre_process=add_auth,
        ...     post_process=extract_data,
        ...     name="fetch_data",
        ... )
    """

    def __init__(
        self,
        request_params: Dict[str, Any],
        pre_process: Optional[Callable[[Dict[str, Any]], None]] = None,
        post_process: Optional[Callable[[HttpResponse], Any]] = None,
        name: Optional[str] = None,
    ):
        self.request_params = request_params.copy() if request_params else {}
        self.pre_process = pre_process
        self.post_process = post_process
        self.name = name or f"task-{id(self)}"

    def execute(self, client: HttpClient) -> Any:
        """Execute the task with the given HTTP client.

        Args:
            client: HttpClient instance to use for the request.

        Returns:
            The result after post-processing, or the raw HttpResponse if no post_process.
        """
        # Pre-processing
        if self.pre_process:
            self.pre_process(self.request_params)

        # Execute request
        response = client.request(**self.request_params)

        # Post-processing
        if self.post_process:
            return self.post_process(response)

        return response

    async def execute_async(self, client: HttpClient) -> Any:
        """Async version of execute.

        Args:
            client: HttpClient instance to use for the request.

        Returns:
            The result after post-processing, or the raw HttpResponse if no post_process.
        """
        # Pre-processing
        if self.pre_process:
            self.pre_process(self.request_params)

        # Execute request
        response = await client.request_async(**self.request_params)

        # Post-processing
        if self.post_process:
            return self.post_process(response)

        return response

    def __repr__(self) -> str:
        return f"Task(name={self.name!r}, method={self.request_params.get('method')!r}, url={self.request_params.get('url')!r})"


class Orchestrator:
    """Manages multiple HTTP connections for parallel/concurrent execution.

    The orchestrator creates and manages a pool of HttpClient instances,
    distributing requests across them for improved throughput and resource
    utilization.

    Args:
        num_connections: Number of HttpClient instances (connections) to create.
        backend: HTTP backend to use for all connections.
        rate_limit: Rate limiting configuration shared across all connections.
        max_workers: Max workers for thread pool (sync mode).
        max_concurrency: Max concurrency for async mode.
    """

    def __init__(
        self,
        num_connections: int = 4,
        backend: str = "auto",
        rate_limit: Optional[RateLimitConfig] = None,
        max_workers: int = 10,
        max_concurrency: int = 10,
    ):
        if num_connections <= 0:
            raise ValueError("num_connections must be positive")

        self.num_connections = num_connections
        self.backend = backend
        self.rate_limit = rate_limit
        self.max_workers = max_workers
        self.max_concurrency = max_concurrency

        # Create connection pool
        self._connections: List[HttpClient] = []
        self._init_connections()

    def _init_connections(self) -> None:
        """Initialize the pool of HttpClient instances."""
        self._connections = [
            HttpClient(
                backend=self.backend,
                rate_limit=self.rate_limit,
                max_workers=self.max_workers,
                max_concurrency=self.max_concurrency,
            )
            for _ in range(self.num_connections)
        ]

    # ------------------------------------------------------------------ #
    # Connection management
    # ------------------------------------------------------------------ #
    def get_connection(self, index: int) -> HttpClient:
        """Get a specific connection by index.

        Args:
            index: Connection index (0 to num_connections-1).

        Returns:
            HttpClient instance at the specified index.

        Raises:
            IndexError: If index is out of range.
        """
        if index < 0 or index >= self.num_connections:
            raise IndexError(f"Connection index {index} out of range [0, {self.num_connections})")
        return self._connections[index]

    def open_all(self) -> None:
        """Open all connections in the pool."""
        for conn in self._connections:
            conn.open()

    async def open_all_async(self) -> None:
        """Async version of open_all."""
        await asyncio.gather(*(conn.open_async() for conn in self._connections))

    def close_all(self) -> None:
        """Close all connections in the pool."""
        for conn in self._connections:
            conn.close()

    async def close_all_async(self) -> None:
        """Async version of close_all."""
        await asyncio.gather(*(conn.close_async() for conn in self._connections))

    def __enter__(self) -> "Orchestrator":
        self.open_all()
        return self

    def __exit__(self, *exc) -> None:
        self.close_all()

    async def __aenter__(self) -> "Orchestrator":
        await self.open_all_async()
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close_all_async()

    # ------------------------------------------------------------------ #
    # Task execution (with pre/post processing)
    # ------------------------------------------------------------------ #
    def execute_tasks_parallel(
        self,
        tasks: Sequence["Task"],
        mode: str = "thread",
        max_workers: Optional[int] = None,
        return_exceptions: bool = True,
    ) -> List[Any]:
        """Execute tasks in parallel using multiple processes or threads.

        Args:
            tasks: Sequence of Task objects to execute.
            mode: Execution mode - "process" for multiprocessing, "thread" for threading.
            max_workers: Max number of workers. Defaults to num_connections.
            return_exceptions: If True, return exceptions instead of raising.

        Returns:
            List of results in the same order as input tasks.

        Raises:
            ValueError: If mode is not "process" or "thread".
        """
        if mode not in ("process", "thread"):
            raise ValueError(f"Invalid mode: {mode}. Must be 'process' or 'thread'")

        if mode == "process":
            return self._execute_tasks_multiprocess(tasks, max_workers, return_exceptions)
        else:
            return self._execute_tasks_multithread(tasks, max_workers, return_exceptions)

    def _execute_tasks_multiprocess(
        self,
        tasks: Sequence["Task"],
        max_workers: Optional[int],
        return_exceptions: bool,
    ) -> List[Any]:
        """Execute tasks using multiprocessing."""
        workers = max_workers or self.num_connections

        # Prepare data for multiprocessing (must be picklable)
        backend = self.backend
        rate_config_dict = (
            {
                "max_rate": self.rate_limit.max_rate,
                "burst": self.rate_limit.burst,
            }
            if self.rate_limit
            else None
        )

        def _worker(task: "Task") -> Any:
            """Worker function for multiprocessing."""
            client = HttpClient(
                backend=backend,
                rate_limit=RateLimitConfig(**rate_config_dict) if rate_config_dict else None,
            )
            try:
                return task.execute(client)
            except Exception as exc:  # noqa: BLE001
                if return_exceptions:
                    return exc
                raise

        # Execute in parallel
        results: List[Any] = [None] * len(tasks)
        with ProcessPoolExecutor(max_workers=workers) as executor:
            future_to_index = {
                executor.submit(_worker, task): idx
                for idx, task in enumerate(tasks)
            }
            for future in as_completed(future_to_index):
                idx = future_to_index[future]
                try:
                    results[idx] = future.result()
                except Exception as exc:  # noqa: BLE001
                    if return_exceptions:
                        results[idx] = exc
                    else:
                        raise

        return results

    def _execute_tasks_multithread(
        self,
        tasks: Sequence["Task"],
        max_workers: Optional[int],
        return_exceptions: bool,
    ) -> List[Any]:
        """Execute tasks using threading (load balanced across connections)."""
        workers = max_workers or self.num_connections

        # Distribute tasks across connections
        results: List[Any] = [None] * len(tasks)

        def _worker(conn_idx: int, task: "Task") -> Any:
            """Worker function for threading."""
            conn = self._connections[conn_idx % self.num_connections]
            try:
                return task.execute(conn)
            except Exception as exc:  # noqa: BLE001
                if return_exceptions:
                    return exc
                raise

        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_index = {
                executor.submit(_worker, idx, task): idx
                for idx, task in enumerate(tasks)
            }
            for future in as_completed(future_to_index):
                idx = future_to_index[future]
                try:
                    results[idx] = future.result()
                except Exception as exc:  # noqa: BLE001
                    if return_exceptions:
                        results[idx] = exc
                    else:
                        raise

        return results

    async def execute_tasks_async(
        self,
        tasks: Sequence["Task"],
        mode: str = "coroutine",
        max_concurrency: Optional[int] = None,
        return_exceptions: bool = True,
    ) -> List[Any]:
        """Execute tasks concurrently using asyncio.

        Args:
            tasks: Sequence of Task objects to execute.
            mode: Execution mode - "coroutine" for asyncio.gather, "semaphore" for controlled concurrency.
            max_concurrency: Max concurrent requests. Defaults to num_connections * max_concurrency.
            return_exceptions: If True, return exceptions instead of raising.

        Returns:
            List of results in the same order as input tasks.

        Raises:
            ValueError: If mode is not "coroutine" or "semaphore".
        """
        if mode not in ("coroutine", "semaphore"):
            raise ValueError(f"Invalid mode: {mode}. Must be 'coroutine' or 'semaphore'")

        if mode == "coroutine":
            return await self._execute_tasks_coroutine(tasks, return_exceptions)
        else:
            concurrency = max_concurrency or (self.num_connections * self.max_concurrency)
            return await self._execute_tasks_semaphore(tasks, concurrency, return_exceptions)

    async def _execute_tasks_coroutine(
        self,
        tasks: Sequence["Task"],
        return_exceptions: bool,
    ) -> List[Any]:
        """Execute tasks using asyncio.gather with load balancing."""
        async def _run(conn_idx: int, task: "Task") -> Any:
            conn = self._connections[conn_idx % self.num_connections]
            try:
                return await task.execute_async(conn)
            except Exception as exc:  # noqa: BLE001
                if return_exceptions:
                    return exc
                raise

        tasks_list = [_run(idx, task) for idx, task in enumerate(tasks)]
        return await asyncio.gather(*tasks_list, return_exceptions=return_exceptions)

    async def _execute_tasks_semaphore(
        self,
        tasks: Sequence["Task"],
        max_concurrency: int,
        return_exceptions: bool,
    ) -> List[Any]:
        """Execute tasks with controlled concurrency using semaphore."""
        sem = asyncio.Semaphore(max_concurrency)
        results: List[Any] = [None] * len(tasks)

        async def _run(idx: int, task: "Task") -> Any:
            async with sem:
                conn = self._connections[idx % self.num_connections]
                try:
                    return await task.execute_async(conn)
                except Exception as exc:  # noqa: BLE001
                    if return_exceptions:
                        return exc
                    raise

        tasks_list = [_run(idx, task) for idx, task in enumerate(tasks)]
        completed = await asyncio.gather(*tasks_list, return_exceptions=return_exceptions)

        for idx, result in enumerate(completed):
            results[idx] = result

        return results

    # ------------------------------------------------------------------ #
    # Parallel execution (multi-process)
    # ------------------------------------------------------------------ #
    def execute_parallel(
        self,
        items: Sequence[Union[Dict[str, Any], tuple]],
        mode: str = "process",
        max_workers: Optional[int] = None,
        return_exceptions: bool = True,
    ) -> List[Any]:
        """Execute requests in parallel using multiple processes or threads.

        Args:
            items: Sequence of request items (dict or tuple).
            mode: Execution mode - "process" for multiprocessing, "thread" for threading.
            max_workers: Max number of workers. Defaults to num_connections.
            return_exceptions: If True, return exceptions instead of raising.

        Returns:
            List of results in the same order as input items.

        Raises:
            ValueError: If mode is not "process" or "thread".
        """
        if mode not in ("process", "thread"):
            raise ValueError(f"Invalid mode: {mode}. Must be 'process' or 'thread'")

        if mode == "process":
            return self._execute_multiprocess(items, max_workers, return_exceptions)
        else:
            return self._execute_multithread(items, max_workers, return_exceptions)

    def _execute_multiprocess(
        self,
        items: Sequence[Union[Dict[str, Any], tuple]],
        max_workers: Optional[int],
        return_exceptions: bool,
    ) -> List[Any]:
        """Execute requests using multiprocessing."""
        workers = max_workers or self.num_connections

        # Prepare data for multiprocessing (must be picklable)
        backend = self.backend
        rate_config_dict = (
            {
                "max_rate": self.rate_limit.max_rate,
                "burst": self.rate_limit.burst,
            }
            if self.rate_limit
            else None
        )

        def _worker(item: Dict[str, Any]) -> Any:
            """Worker function for multiprocessing."""
            client = HttpClient(
                backend=backend,
                rate_limit=RateLimitConfig(**rate_config_dict) if rate_config_dict else None,
            )
            try:
                return client.request(**item)
            except Exception as exc:  # noqa: BLE001
                if return_exceptions:
                    return exc
                raise

        # Normalize items
        normalized_items = [self._normalize_item(item) for item in items]

        # Execute in parallel
        results: List[Any] = [None] * len(normalized_items)
        with ProcessPoolExecutor(max_workers=workers) as executor:
            future_to_index = {
                executor.submit(_worker, item): idx
                for idx, item in enumerate(normalized_items)
            }
            for future in as_completed(future_to_index):
                idx = future_to_index[future]
                try:
                    results[idx] = future.result()
                except Exception as exc:  # noqa: BLE001
                    if return_exceptions:
                        results[idx] = exc
                    else:
                        raise

        return results

    def _execute_multithread(
        self,
        items: Sequence[Union[Dict[str, Any], tuple]],
        max_workers: Optional[int],
        return_exceptions: bool,
    ) -> List[Any]:
        """Execute requests using threading (load balanced across connections)."""
        workers = max_workers or self.num_connections
        normalized_items = [self._normalize_item(item) for item in items]

        # Distribute items across connections
        results: List[Any] = [None] * len(normalized_items)

        def _worker(conn_idx: int, item: Dict[str, Any]) -> Any:
            """Worker function for threading."""
            conn = self._connections[conn_idx % self.num_connections]
            try:
                return conn.request(**item)
            except Exception as exc:  # noqa: BLE001
                if return_exceptions:
                    return exc
                raise

        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_index = {
                executor.submit(_worker, idx, item): idx
                for idx, item in enumerate(normalized_items)
            }
            for future in as_completed(future_to_index):
                idx = future_to_index[future]
                try:
                    results[idx] = future.result()
                except Exception as exc:  # noqa: BLE001
                    if return_exceptions:
                        results[idx] = exc
                    else:
                        raise

        return results

    # ------------------------------------------------------------------ #
    # Concurrent execution (asyncio)
    # ------------------------------------------------------------------ #
    async def execute_async(
        self,
        items: Sequence[Union[Dict[str, Any], tuple]],
        mode: str = "coroutine",
        max_concurrency: Optional[int] = None,
        return_exceptions: bool = True,
    ) -> List[Any]:
        """Execute requests concurrently using asyncio.

        Args:
            items: Sequence of request items (dict or tuple).
            mode: Execution mode - "coroutine" for asyncio.gather, "semaphore" for controlled concurrency.
            max_concurrency: Max concurrent requests. Defaults to num_connections * max_concurrency.
            return_exceptions: If True, return exceptions instead of raising.

        Returns:
            List of results in the same order as input items.

        Raises:
            ValueError: If mode is not "coroutine" or "semaphore".
        """
        if mode not in ("coroutine", "semaphore"):
            raise ValueError(f"Invalid mode: {mode}. Must be 'coroutine' or 'semaphore'")

        normalized_items = [self._normalize_item(item) for item in items]

        if mode == "coroutine":
            return await self._execute_coroutine(normalized_items, return_exceptions)
        else:
            concurrency = max_concurrency or (self.num_connections * self.max_concurrency)
            return await self._execute_semaphore(normalized_items, concurrency, return_exceptions)

    async def _execute_coroutine(
        self,
        items: List[Dict[str, Any]],
        return_exceptions: bool,
    ) -> List[Any]:
        """Execute using asyncio.gather with load balancing."""
        # Distribute items across connections
        async def _run(conn_idx: int, item: Dict[str, Any]) -> Any:
            conn = self._connections[conn_idx % self.num_connections]
            try:
                return await conn.request_async(**item)
            except Exception as exc:  # noqa: BLE001
                if return_exceptions:
                    return exc
                raise

        tasks = [_run(idx, item) for idx, item in enumerate(items)]
        return await asyncio.gather(*tasks, return_exceptions=return_exceptions)

    async def _execute_semaphore(
        self,
        items: List[Dict[str, Any]],
        max_concurrency: int,
        return_exceptions: bool,
    ) -> List[Any]:
        """Execute with controlled concurrency using semaphore."""
        sem = asyncio.Semaphore(max_concurrency)
        results: List[Any] = [None] * len(items)

        async def _run(idx: int, item: Dict[str, Any]) -> Any:
            async with sem:
                conn = self._connections[idx % self.num_connections]
                try:
                    return await conn.request_async(**item)
                except Exception as exc:  # noqa: BLE001
                    if return_exceptions:
                        return exc
                    raise

        tasks = [_run(idx, item) for idx, item in enumerate(items)]
        completed = await asyncio.gather(*tasks, return_exceptions=return_exceptions)

        for idx, result in enumerate(completed):
            results[idx] = result

        return results

    # ------------------------------------------------------------------ #
    # Convenience methods
    # ------------------------------------------------------------------ #
    def get_many_parallel(
        self,
        urls: Iterable[str],
        method: str = "GET",
        mode: str = "process",
        max_workers: Optional[int] = None,
        return_exceptions: bool = True,
        **kwargs,
    ) -> List[Any]:
        """Convenience method for parallel GET requests.

        Args:
            urls: Iterable of URLs to request.
            method: HTTP method (default: GET).
            mode: Execution mode - "process" or "thread".
            max_workers: Max number of workers.
            return_exceptions: If True, return exceptions instead of raising.
            **kwargs: Additional request parameters.

        Returns:
            List of responses or exceptions.
        """
        items = [{"method": method, "url": url, **kwargs} for url in urls]
        return self.execute_parallel(items, mode=mode, max_workers=max_workers, return_exceptions=return_exceptions)

    async def get_many_async(
        self,
        urls: Iterable[str],
        method: str = "GET",
        mode: str = "coroutine",
        max_concurrency: Optional[int] = None,
        return_exceptions: bool = True,
        **kwargs,
    ) -> List[Any]:
        """Convenience method for async concurrent GET requests.

        Args:
            urls: Iterable of URLs to request.
            method: HTTP method (default: GET).
            mode: Execution mode - "coroutine" or "semaphore".
            max_concurrency: Max concurrent requests.
            return_exceptions: If True, return exceptions instead of raising.
            **kwargs: Additional request parameters.

        Returns:
            List of responses or exceptions.
        """
        items = [{"method": method, "url": url, **kwargs} for url in urls]
        return await self.execute_async(items, mode=mode, max_concurrency=max_concurrency, return_exceptions=return_exceptions)

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    @staticmethod
    def _normalize_item(item: Union[Dict[str, Any], tuple]) -> Dict[str, Any]:
        """Normalize request item to dict format."""
        if isinstance(item, dict):
            return item
        if isinstance(item, (list, tuple)):
            method, url = item[0], item[1]
            extra = item[2] if len(item) > 2 and isinstance(item[2], dict) else {}
            out: Dict[str, Any] = {"method": method, "url": url}
            out.update(extra)
            return out
        raise TypeError(f"Unsupported request item type: {type(item)!r}")


# ------------------------------------------------------------------ #
# Helper functions for standalone usage
# ------------------------------------------------------------------ #
def execute_parallel_requests(
    items: Sequence[Union[Dict[str, Any], tuple]],
    num_connections: int = 4,
    backend: str = "auto",
    rate_limit: Optional[RateLimitConfig] = None,
    mode: str = "process",
    max_workers: Optional[int] = None,
    return_exceptions: bool = True,
) -> List[Any]:
    """Standalone function for parallel request execution.

    Args:
        items: Sequence of request items.
        num_connections: Number of connections to create.
        backend: HTTP backend to use.
        rate_limit: Rate limiting configuration.
        mode: Execution mode - "process" or "thread".
        max_workers: Max number of workers.
        return_exceptions: If True, return exceptions instead of raising.

    Returns:
        List of results.

    Example:
        >>> items = [
        ...     {"method": "GET", "url": "https://api.example.com/1"},
        ...     {"method": "GET", "url": "https://api.example.com/2"},
        ... ]
        >>> results = execute_parallel_requests(items, num_connections=2)
    """
    with Orchestrator(
        num_connections=num_connections,
        backend=backend,
        rate_limit=rate_limit,
    ) as orch:
        return orch.execute_parallel(items, mode=mode, max_workers=max_workers, return_exceptions=return_exceptions)


async def execute_async_requests(
    items: Sequence[Union[Dict[str, Any], tuple]],
    num_connections: int = 4,
    backend: str = "auto",
    rate_limit: Optional[RateLimitConfig] = None,
    mode: str = "coroutine",
    max_concurrency: Optional[int] = None,
    return_exceptions: bool = True,
) -> List[Any]:
    """Standalone async function for concurrent request execution.

    Args:
        items: Sequence of request items.
        num_connections: Number of connections to create.
        backend: HTTP backend to use.
        rate_limit: Rate limiting configuration.
        mode: Execution mode - "coroutine" or "semaphore".
        max_concurrency: Max concurrent requests.
        return_exceptions: If True, return exceptions instead of raising.

    Returns:
        List of results.

    Example:
        >>> import asyncio
        >>> items = [
        ...     {"method": "GET", "url": "https://api.example.com/1"},
        ...     {"method": "GET", "url": "https://api.example.com/2"},
        ... ]
        >>> results = asyncio.run(execute_async_requests(items, num_connections=2))
    """
    async with Orchestrator(
        num_connections=num_connections,
        backend=backend,
        rate_limit=rate_limit,
    ) as orch:
        return await orch.execute_async(items, mode=mode, max_concurrency=max_concurrency, return_exceptions=return_exceptions)
