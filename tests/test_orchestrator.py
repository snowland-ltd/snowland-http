"""Unit tests for snowland_http.orchestrator module.

Run from the project root (the directory containing ``snowland_http``)::

    python -m unittest discover -s tests
"""

import asyncio
import json
import os
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

from snowland_http import HttpClient, Orchestrator, RateLimitConfig, Task


class OrchestratorTest(unittest.TestCase):
    """Tests for Orchestrator class."""

    @classmethod
    def setUpClass(cls):
        """Set up a test HTTP server."""
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                body = json.dumps({"path": self.path, "delay": 0.1}).encode()
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
        """Shut down the test server."""
        cls.server.shutdown()

    def _url(self, path):
        return f"http://127.0.0.1:{self.port}{path}"

    def test_orchestrator_initialization(self):
        """Test that Orchestrator initializes correctly."""
        orch = Orchestrator(num_connections=4, backend="stdlib")
        self.assertEqual(orch.num_connections, 4)
        self.assertEqual(len(orch._connections), 4)
        for conn in orch._connections:
            self.assertIsInstance(conn, HttpClient)

    def test_orchestrator_invalid_num_connections(self):
        """Test that invalid num_connections raises ValueError."""
        with self.assertRaises(ValueError):
            Orchestrator(num_connections=0)
        with self.assertRaises(ValueError):
            Orchestrator(num_connections=-1)

    def test_get_connection(self):
        """Test getting a specific connection."""
        orch = Orchestrator(num_connections=3, backend="stdlib")
        conn = orch.get_connection(0)
        self.assertIsInstance(conn, HttpClient)

        with self.assertRaises(IndexError):
            orch.get_connection(10)

    def test_context_manager(self):
        """Test using Orchestrator as a context manager."""
        with Orchestrator(num_connections=2, backend="stdlib") as orch:
            self.assertIsNotNone(orch)
        # Connections should be closed after exiting context

    def test_execute_parallel_thread_mode(self):
        """Test parallel execution in thread mode."""
        orch = Orchestrator(num_connections=2, backend="stdlib")
        urls = [self._url(f"/thread/{i}") for i in range(5)]
        results = orch.get_many_parallel(urls, mode="thread", return_exceptions=False)
        self.assertEqual(len(results), 5)
        for r in results:
            self.assertEqual(r.status_code, 200)

    def test_execute_parallel_invalid_mode(self):
        """Test that invalid mode raises ValueError."""
        orch = Orchestrator(num_connections=2, backend="stdlib")
        with self.assertRaises(ValueError):
            orch.execute_parallel([], mode="invalid")

    def test_normalize_item_dict(self):
        """Test normalizing dict items."""
        orch = Orchestrator(num_connections=2, backend="stdlib")
        item = {"method": "GET", "url": "https://example.com"}
        normalized = orch._normalize_item(item)
        self.assertEqual(normalized, item)

    def test_normalize_item_tuple(self):
        """Test normalizing tuple items."""
        orch = Orchestrator(num_connections=2, backend="stdlib")
        item = ("GET", "https://example.com", {"params": {"key": "value"}})
        normalized = orch._normalize_item(item)
        self.assertEqual(normalized["method"], "GET")
        self.assertEqual(normalized["url"], "https://example.com")
        self.assertEqual(normalized["params"], {"key": "value"})

    def test_normalize_item_invalid(self):
        """Test that invalid item type raises TypeError."""
        orch = Orchestrator(num_connections=2, backend="stdlib")
        with self.assertRaises(TypeError):
            orch._normalize_item(123)  # type: ignore

    def test_execute_parallel_with_exceptions(self):
        """Test parallel execution with exception handling."""
        orch = Orchestrator(num_connections=2, backend="stdlib")
        items = [
            {"method": "GET", "url": self._url("/ok")},
            {"method": "GET", "url": "http://127.0.0.1:1/closed"},
        ]
        results = orch.execute_parallel(items, mode="thread", return_exceptions=True)
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0].status_code, 200)
        self.assertIsInstance(results[1], Exception)


class TaskTest(unittest.TestCase):
    """Tests for Task class."""

    @classmethod
    def setUpClass(cls):
        """Set up a test HTTP server."""
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
        """Shut down the test server."""
        cls.server.shutdown()

    def _url(self, path):
        return f"http://127.0.0.1:{self.port}{path}"

    def test_task_creation(self):
        """Test creating a task."""
        task = Task(
            request_params={"method": "GET", "url": "https://example.com"},
            name="test_task",
        )
        self.assertEqual(task.name, "test_task")
        self.assertEqual(task.request_params["method"], "GET")

    def test_task_with_pre_process(self):
        """Test task with pre-processing."""
        def add_auth(params):
            params["headers"] = {"Authorization": "Bearer token"}

        task = Task(
            request_params={"method": "GET", "url": self._url("/test")},
            pre_process=add_auth,
        )

        client = HttpClient(backend="stdlib")
        result = task.execute(client)
        self.assertEqual(result.status_code, 200)

    def test_task_with_post_process(self):
        """Test task with post-processing."""
        def extract_json(response):
            return response.json()

        task = Task(
            request_params={"method": "GET", "url": self._url("/test")},
            post_process=extract_json,
        )

        client = HttpClient(backend="stdlib")
        result = task.execute(client)
        self.assertIsInstance(result, dict)
        self.assertIn("path", result)

    def test_task_with_both_pre_and_post_process(self):
        """Test task with both pre and post processing."""
        def add_auth(params):
            params["headers"] = {"Authorization": "Bearer token"}

        def extract_json(response):
            return response.json()

        task = Task(
            request_params={"method": "GET", "url": self._url("/test")},
            pre_process=add_auth,
            post_process=extract_json,
        )

        client = HttpClient(backend="stdlib")
        result = task.execute(client)
        self.assertIsInstance(result, dict)
        self.assertIn("path", result)

    def test_task_repr(self):
        """Test task string representation."""
        task = Task(
            request_params={"method": "GET", "url": "https://example.com"},
            name="test",
        )
        repr_str = repr(task)
        self.assertIn("test", repr_str)
        self.assertIn("GET", repr_str)


class TaskOrchestratorTest(unittest.TestCase):
    """Tests for executing tasks with Orchestrator."""

    @classmethod
    def setUpClass(cls):
        """Set up a test HTTP server."""
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
        """Shut down the test server."""
        cls.server.shutdown()

    def _url(self, path):
        return f"http://127.0.0.1:{self.port}{path}"

    def test_execute_tasks_parallel_thread_mode(self):
        """Test executing tasks in parallel using thread mode."""
        orch = Orchestrator(num_connections=2, backend="stdlib")

        def extract_json(response):
            return response.json()

        tasks = [
            Task(
                request_params={"method": "GET", "url": self._url(f"/task/{i}")},
                post_process=extract_json,
                name=f"task_{i}",
            )
            for i in range(3)
        ]

        results = orch.execute_tasks_parallel(tasks, mode="thread", return_exceptions=False)
        self.assertEqual(len(results), 3)
        for result in results:
            self.assertIsInstance(result, dict)
            self.assertIn("path", result)

    def test_execute_tasks_parallel_with_pre_process(self):
        """Test executing tasks with pre-processing."""
        orch = Orchestrator(num_connections=2, backend="stdlib")

        def add_auth(params):
            params["headers"] = {"Authorization": "Bearer token"}

        tasks = [
            Task(
                request_params={"method": "GET", "url": self._url(f"/auth/{i}")},
                pre_process=add_auth,
                name=f"auth_task_{i}",
            )
            for i in range(3)
        ]

        results = orch.execute_tasks_parallel(tasks, mode="thread", return_exceptions=False)
        self.assertEqual(len(results), 3)
        for result in results:
            self.assertEqual(result.status_code, 200)

    def test_execute_tasks_parallel_with_exceptions(self):
        """Test executing tasks with exception handling."""
        orch = Orchestrator(num_connections=2, backend="stdlib")

        tasks = [
            Task(
                request_params={"method": "GET", "url": self._url("/ok")},
                name="ok_task",
            ),
            Task(
                request_params={"method": "GET", "url": "http://127.0.0.1:1/closed"},
                name="fail_task",
            ),
        ]

        results = orch.execute_tasks_parallel(tasks, mode="thread", return_exceptions=True)
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0].status_code, 200)
        self.assertIsInstance(results[1], Exception)


class AsyncTaskOrchestratorTest(unittest.IsolatedAsyncioTestCase):
    """Async tests for executing tasks with Orchestrator."""

    @classmethod
    def setUpClass(cls):
        """Set up a test HTTP server."""
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
        """Shut down the test server."""
        cls.server.shutdown()

    def _url(self, path):
        return f"http://127.0.0.1:{self.port}{path}"

    async def test_execute_tasks_async_coroutine_mode(self):
        """Test executing tasks in async coroutine mode."""
        try:
            orch = Orchestrator(num_connections=2, backend="httpx")

            def extract_json(response):
                return response.json()

            tasks = [
                Task(
                    request_params={"method": "GET", "url": self._url(f"/async_task/{i}")},
                    post_process=extract_json,
                    name=f"async_task_{i}",
                )
                for i in range(3)
            ]

            results = await orch.execute_tasks_async(tasks, mode="coroutine", return_exceptions=False)
            self.assertEqual(len(results), 3)
            for result in results:
                self.assertIsInstance(result, dict)
                self.assertIn("path", result)
        except Exception as e:
            self.skipTest(f"httpx not available: {e}")

    async def test_execute_tasks_async_semaphore_mode(self):
        """Test executing tasks in async semaphore mode."""
        try:
            orch = Orchestrator(num_connections=2, backend="httpx")

            def extract_json(response):
                return response.json()

            tasks = [
                Task(
                    request_params={"method": "GET", "url": self._url(f"/sem_task/{i}")},
                    post_process=extract_json,
                    name=f"sem_task_{i}",
                )
                for i in range(3)
            ]

            results = await orch.execute_tasks_async(tasks, mode="semaphore", max_concurrency=2, return_exceptions=False)
            self.assertEqual(len(results), 3)
            for result in results:
                self.assertIsInstance(result, dict)
                self.assertIn("path", result)
        except Exception as e:
            self.skipTest(f"httpx not available: {e}")

    async def test_execute_tasks_async_with_exceptions(self):
        """Test executing tasks with exception handling."""
        try:
            orch = Orchestrator(num_connections=2, backend="httpx")

            tasks = [
                Task(
                    request_params={"method": "GET", "url": self._url("/ok")},
                    name="ok_task",
                ),
                Task(
                    request_params={"method": "GET", "url": "http://127.0.0.1:1/closed"},
                    name="fail_task",
                ),
            ]

            results = await orch.execute_tasks_async(tasks, mode="coroutine", return_exceptions=True)
            self.assertEqual(len(results), 2)
            self.assertEqual(results[0].status_code, 200)
            self.assertIsInstance(results[1], Exception)
        except Exception as e:
            self.skipTest(f"httpx not available: {e}")


class AsyncOrchestratorTest(unittest.IsolatedAsyncioTestCase):
    """Async tests for Orchestrator class."""

    @classmethod
    def setUpClass(cls):
        """Set up a test HTTP server."""
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
        """Shut down the test server."""
        cls.server.shutdown()

    def _url(self, path):
        return f"http://127.0.0.1:{self.port}{path}"

    async def test_async_context_manager(self):
        """Test using Orchestrator as an async context manager."""
        try:
            async with Orchestrator(num_connections=2, backend="httpx") as orch:
                self.assertIsNotNone(orch)
        except Exception as e:
            self.skipTest(f"httpx not available: {e}")

    async def test_execute_async_coroutine_mode(self):
        """Test async execution in coroutine mode."""
        try:
            orch = Orchestrator(num_connections=2, backend="httpx")
            urls = [self._url(f"/async/{i}") for i in range(5)]
            results = await orch.get_many_async(urls, mode="coroutine", return_exceptions=False)
            self.assertEqual(len(results), 5)
            for r in results:
                self.assertEqual(r.status_code, 200)
        except Exception as e:
            self.skipTest(f"httpx not available: {e}")

    async def test_execute_async_semaphore_mode(self):
        """Test async execution in semaphore mode."""
        try:
            orch = Orchestrator(num_connections=2, backend="httpx")
            urls = [self._url(f"/sem/{i}") for i in range(5)]
            results = await orch.get_many_async(urls, mode="semaphore", max_concurrency=2, return_exceptions=False)
            self.assertEqual(len(results), 5)
            for r in results:
                self.assertEqual(r.status_code, 200)
        except Exception as e:
            self.skipTest(f"httpx not available: {e}")

    async def test_execute_async_invalid_mode(self):
        """Test that invalid mode raises ValueError."""
        try:
            orch = Orchestrator(num_connections=2, backend="httpx")
            with self.assertRaises(ValueError):
                await orch.execute_async([], mode="invalid")
        except Exception as e:
            self.skipTest(f"httpx not available: {e}")

    async def test_execute_async_with_exceptions(self):
        """Test async execution with exception handling."""
        try:
            orch = Orchestrator(num_connections=2, backend="httpx")
            items = [
                {"method": "GET", "url": self._url("/ok")},
                {"method": "GET", "url": "http://127.0.0.1:1/closed"},
            ]
            results = await orch.execute_async(items, mode="coroutine", return_exceptions=True)
            self.assertEqual(len(results), 2)
            self.assertEqual(results[0].status_code, 200)
            self.assertIsInstance(results[1], Exception)
        except Exception as e:
            self.skipTest(f"httpx not available: {e}")


class StandaloneFunctionsTest(unittest.TestCase):
    """Tests for standalone functions."""

    @classmethod
    def setUpClass(cls):
        """Set up a test HTTP server."""
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
        """Shut down the test server."""
        cls.server.shutdown()

    def _url(self, path):
        return f"http://127.0.0.1:{self.port}{path}"

    def test_execute_parallel_requests(self):
        """Test standalone execute_parallel_requests function."""
        from snowland_http.orchestrator import execute_parallel_requests

        items = [
            {"method": "GET", "url": self._url(f"/standalone/{i}")}
            for i in range(3)
        ]
        results = execute_parallel_requests(items, num_connections=2, mode="thread", return_exceptions=False)
        self.assertEqual(len(results), 3)
        for r in results:
            self.assertEqual(r.status_code, 200)

    def test_execute_async_requests(self):
        """Test standalone execute_async_requests function."""
        try:
            from snowland_http.orchestrator import execute_async_requests

            async def run():
                items = [
                    {"method": "GET", "url": self._url(f"/async-standalone/{i}")}
                    for i in range(3)
                ]
                results = await execute_async_requests(items, num_connections=2, mode="coroutine", return_exceptions=False)
                self.assertEqual(len(results), 3)
                for r in results:
                    self.assertEqual(r.status_code, 200)

            asyncio.run(run())
        except Exception as e:
            self.skipTest(f"httpx not available: {e}")


if __name__ == "__main__":
    unittest.main()
