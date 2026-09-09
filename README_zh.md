# snowland-http

[![PyPI 版本](https://img.shields.io/pypi/v/snowland-http.svg)](https://pypi.org/project/snowland-http/)
[![PyPI 下载量](https://img.shields.io/pypi/dm/snowland-http.svg?cacheSeconds=86400)](https://pypi.org/project/snowland-http/)
[![许可证](https://img.shields.io/badge/license-BSD%203--Clause-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/downloads/)
[![CI](https://github.com/snowland-ltd/snowland-http/actions/workflows/test.yml/badge.svg)](https://github.com/snowland-ltd/snowland-http/actions/workflows/test.yml)

一个支持**限流**与**并行**的 HTTP 客户端，底层可自由选择 `requests` / `httpx` / `aiohttp`，或零依赖的 `stdlib`（标准库 `urllib`）作为传输后端。

## 特性

- **可插拔传输层**：`requests`（仅同步）、`httpx`（同步+异步）、`aiohttp`（仅异步），以及 `stdlib`（仅同步，基于 Python 标准库 `urllib`，**无需安装任何第三方库**）。通过 `backend=` 指定，或 `backend="auto"` 自动探测（优先级 httpx > aiohttp > requests > **stdlib**）。
- **全局限流**：基于令牌桶算法，所有并行 worker 共享同一个限流器，保证聚合请求速率不超过上限；同时提供同步 `acquire()` 与异步 `acquire_async()`。
- **并行请求**：同步使用线程池（`ThreadPoolExecutor`），异步使用 `asyncio.gather` + `Semaphore` 控制并发度。
- **编排器（Orchestrator）**：管理多个 HTTP 连接，支持多进程或协程的并行/并发执行，提升吞吐量。
- **连接生命周期**：显式的 `open()` / `close()`（及其异步版本），并支持上下文管理器——进入时建立连接资源，退出时释放。

## 安装

三个第三方传输库（`requests` / `httpx` / `aiohttp`）**均为可选依赖**，彼此独立，不强制安装任何一个；包本身导入时不依赖它们（后端为惰性导入）。当三者均未安装时，客户端会自动回退到内置的 `stdlib` 后端（纯 `urllib`），因此**在纯净 Python 环境下无需安装任何东西即可使用**。安装至少其一即可使用对应后端：

```bash
# 方式一：直接装某个传输库
pip install requests          # 或 httpx / aiohttp，至少安装其一

# 方式二：通过 extras 安装（推荐）
pip install ".[requests]"     # 仅同步后端
pip install ".[httpx]"        # 同步 + 异步后端（推荐）
pip install ".[aiohttp]"      # 仅异步后端
pip install ".[all]"          # 全部
# ".[stdlib]" 是一个空 extra，仅用于说明始终可用的 urllib 后端。
```

`backend="auto"` 时按 `httpx > aiohttp > requests` 顺序自动探测已安装的库，最后回退到 `stdlib`（无需安装）。

## 快速开始

### 同步 + 限流 + 并行

```python
from snowland_http import HttpClient, RateLimitConfig

client = HttpClient(
    backend="requests",
    rate_limit=RateLimitConfig(max_rate=5, burst=2),  # 每秒最多 5 次，突发上限 2
)

resp = client.get("https://example.com")
print(resp.status_code, resp.json())

# 并行 GET
results = client.get_many(["https://example.com/1", "https://example.com/2"])
for r in results:
    print(r if isinstance(r, Exception) else r.status_code)
```

### 异步 + 限流 + 并行

```python
import asyncio
from snowland_http import HttpClient, RateLimitConfig

async def main():
    client = HttpClient(
        backend="httpx",
        rate_limit=RateLimitConfig(max_rate=10, burst=5),
    )
    async with client:  # 进入即 open_async（建立连接），退出即 close_async
        results = await client.get_many_async(["https://example.com/1", "https://example.com/2"])
        for r in results:
            print(r.status_code)

asyncio.run(main())
```

### 零依赖（stdlib / urllib）

无需安装任何第三方包——在标准 Python 下即可使用：

```bash
pip install snowland-http   # 无需再装其他东西
```

```python
from snowland_http import HttpClient

# backend="auto" 在 requests/httpx/aiohttp 缺失时自动回退到 stdlib，
# 也可显式指定：
client = HttpClient(backend="stdlib")
resp = client.get("https://example.com")
print(resp.status_code, resp.text)
```

> 上下文管理器会在进入时调用 `open()` / `open_async()`（建立连接资源），退出时调用 `close()` / `close_async()`。也可手动调用：`client.open()` / `await client.open_async()`。对 `aiohttp` 后端，`open_async()` 会在当前事件循环内创建 `ClientSession`。

## API

`HttpClient(backend="auto", rate_limit=None, max_workers=10, max_concurrency=10)`

`backend` 参数接受 `"requests"`、`"httpx"`、`"aiohttp"`、`"stdlib"`（或别名 `"urllib"`）、`"auto"`。使用 `"auto"` 时优先 `httpx > aiohttp > requests`，最后回退到零依赖的 `stdlib` 后端。

| 方法 | 说明 |
| --- | --- |
| `request(method, url, **kwargs)` | 单次同步请求 |
| `get/post/put/delete/head/patch(url, **kwargs)` | 同步便捷方法 |
| `request_many(items, max_workers, return_exceptions)` | 同步并行（线程池） |
| `get_many(urls, method="GET", ...)` | 同步并行 GET |
| `request_async(method, url, **kwargs)` | 单次异步请求 |
| `get_async/...` | 异步便捷方法 |
| `request_many_async(items, max_concurrency, return_exceptions)` | 异步并行 |
| `get_many_async(urls, ...)` | 异步并行 GET |
| `open()` / `open_async()` | 打开 / 建立连接资源 |
| `close()` / `close_async()` | 关闭连接资源 |

- `items` 元素可为 `dict`（`{"method": ..., "url": ..., ...}`）或 `(method, url, kwargs_dict)` 元组。
- 并行方法默认 `return_exceptions=True`，单个失败会以异常对象形式出现在结果列表中，不影响其余请求；设为 `False` 则整体抛出。

## 限流配置

`RateLimitConfig(max_rate, burst)`

- `max_rate`：每秒允许的最大请求数。`max_rate <= 0` 表示**完全禁用**限流（限流器变为不阻塞的空操作，不会抛异常）。
- `burst`：允许连续突发发送的请求数，超过后进入平滑限速。

## 响应编码

`HttpResponse.text` 按照 HTTP 规则解码，**而非**硬编码 UTF-8：

- `Content-Type` 头中声明的 `charset` 优先（例如 `text/html; charset=gbk`）；
- 未声明 `charset` 时，默认为 **ISO-8859-1**（latin-1，依据 RFC 7231）；
- 解码是严格的（不会静默使用 `errors="replace"`）：非法字节体会抛出 `UnicodeDecodeError`，避免乱码被悄悄掩盖。

如需覆盖自动判定，可在构造响应时传入 `encoding=`（后端内部使用此参数）。

## 后端选择约束

- `requests` 仅支持同步 API（调用 `request_async` 会抛 `AsyncRequiredError`）。
- `aiohttp` 仅支持异步 API（调用 `request` 会抛 `AsyncRequiredError`）。
- `httpx` 同时支持同步与异步。
- `stdlib`（urllib）仅支持同步 API（调用 `request_async` 会抛 `AsyncRequiredError`）。

## 编排器（Orchestrator）多连接并行/并发

`Orchestrator` 类管理一个 `HttpClient` 实例池，提供高层 API 在多个连接上执行请求：

```python
from snowland_http import Orchestrator, RateLimitConfig

# 创建包含 4 个连接的编排器
orch = Orchestrator(
    num_connections=4,
    backend="httpx",
    rate_limit=RateLimitConfig(max_rate=10, burst=5),
)

# 并行执行（多进程或多线程）
urls = ["https://api.example.com/data/1", "https://api.example.com/data/2"]
results = orch.execute_parallel(urls, mode="process")  # 或 mode="thread"

# 并发执行（asyncio）
results = await orch.execute_async(urls, mode="coroutine")  # 或 mode="semaphore"
```

**执行模式：**

- **并行（同步）**：
  - `mode="process"`：使用 `ProcessPoolExecutor` 的多进程执行
  - `mode="thread"`：多线程执行，在多个连接间负载均衡
- **并发（异步）**：
  - `mode="coroutine"`：使用 `asyncio.gather` 并在连接间负载均衡
  - `mode="semaphore"`：使用 `asyncio.Semaphore` 控制并发度

**调度策略（scheduling）：**

`scheduling` 决定任务如何分配到连接池中的各个连接，既可以在构造函数上设置（实例默认值），也可以在每次调用时覆盖。

| 取值 | 行为 |
| --- | --- |
| `"round_robin"`（默认） | 第 `i` 个任务固定使用第 `i % num_connections` 个连接。确定性强，但当任务耗时长短不一时，慢任务可能全堆在同一个连接上，其他连接空转。 |
| `"idle"` | 每个任务分配到**当前空闲**的连接，一个连接同一时刻只服务一个任务（并发上限为 `num_connections`）。适合请求耗时差异大的场景。 |

```python
from snowland_http import Orchestrator, SCHEDULING_IDLE

orch = Orchestrator(num_connections=4, backend="httpx", scheduling=SCHEDULING_IDLE)

# 也可以只在单次调用时指定
results = orch.execute_parallel(items, mode="thread", scheduling="idle")
```

注意：

- `mode="process"` 下该参数被忽略（会打印一条 warning 日志）：工作进程本身就从共享任务队列里取活，天然是空闲优先。
- 使用 `scheduling="idle"` 时，线程模式建议 `max_workers >= num_connections`，否则部分连接永远轮不到。

**便捷方法：**

```python
# 并行 GET 请求
results = orch.get_many_parallel(urls, mode="thread")

# 异步并发 GET 请求
results = await orch.get_many_async(urls, mode="coroutine")
```

**独立函数：**

```python
from snowland_http import execute_parallel_requests, execute_async_requests

# 并行执行
results = execute_parallel_requests(items, num_connections=4, mode="process")

# 异步执行
results = await execute_async_requests(items, num_connections=4, mode="coroutine")
```

**上下文管理器支持：**

```python
# 同步上下文管理器
with Orchestrator(num_connections=4) as orch:
    results = orch.get_many_parallel(urls)

# 异步上下文管理器
async with Orchestrator(num_connections=4) as orch:
    results = await orch.get_many_async(urls)
```

## 任务（Task）预处理和后处理

`Task` 类允许你定义带有可选预处理和后处理钩子的 HTTP 请求：

```python
from snowland_http import Task

# 定义预处理函数
def add_auth(params):
    params["headers"] = {"Authorization": "Bearer token"}

# 定义后处理函数
def extract_json(response):
    return response.json()

# 创建任务
task = Task(
    request_params={"method": "GET", "url": "https://api.example.com/data"},
    pre_process=add_auth,
    post_process=extract_json,
    name="fetch_data",
)

# 执行任务
result = task.execute(client)
```

**使用 Orchestrator 执行任务：**

```python
# 创建多个任务
tasks = [
    Task(
        request_params={"method": "GET", "url": f"https://api.example.com/data/{i}"},
        pre_process=add_auth,
        post_process=extract_json,
    )
    for i in range(10)
]

# 并行执行任务
results = orch.execute_tasks_parallel(tasks, mode="thread")

# 并发执行任务
results = await orch.execute_tasks_async(tasks, mode="coroutine")
```

**任务执行流程：**

1. **预处理**（可选）：在发送请求前修改请求参数
2. **HTTP 请求**：执行 HTTP 请求
3. **后处理**（可选）：在返回前处理响应

这允许你：
- 动态添加认证头
- 转换请求参数
- 提取和转换响应数据
- 实现自定义错误处理
- 添加日志或监控

## 开发与 CI

- 测试在 `master` 与 `dev` 分支上运行（见 `.github/workflows/test.yml`），覆盖 Python 3.8–3.12，安装 `.[all]` 以确保功能/并行测试可执行。
- 发布到 PyPI 通过 GitHub Release（`release: published`）触发 `.github/workflows/release.yml`，使用 `PYPI_API_TOKEN` 仓库密钥进行认证。

本地运行测试：

```bash
pip install -e ".[all]"
python -m unittest discover -s tests -v
```

## License

BSD 3-Clause。详见 [LICENSE](LICENSE)。
