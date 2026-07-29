# snowland-http

一个支持**限流**与**并行**的 HTTP 客户端，底层可自由选择 `requests` / `httpx` / `aiohttp` 作为传输后端。

## 特性

- **可插拔传输层**：`requests`（仅同步）、`httpx`（同步+异步）、`aiohttp`（仅异步），通过 `backend=` 指定，或 `backend="auto"` 自动探测（优先级 httpx > aiohttp > requests）。
- **全局限流**：基于令牌桶算法，所有并行 worker 共享同一个限流器，保证聚合请求速率不超过上限；同时提供同步 `acquire()` 与异步 `acquire_async()`。
- **并行请求**：同步使用线程池（`ThreadPoolExecutor`），异步使用 `asyncio.gather` + `Semaphore` 控制并发度。
- **连接生命周期**：显式的 `open()` / `close()`（及其异步版本），并支持上下文管理器——进入时建立连接资源，退出时释放。

## 安装

三个传输库（`requests` / `httpx` / `aiohttp`）**均为可选依赖**，彼此独立，不强制安装任何一个；包本身导入时不依赖它们（后端为惰性导入）。安装至少其一即可使用对应后端：

```bash
# 方式一：直接装某个传输库
pip install requests          # 或 httpx / aiohttp，至少安装其一

# 方式二：通过 extras 安装（推荐）
pip install ".[requests]"     # 仅同步后端
pip install ".[httpx]"        # 同步 + 异步后端（推荐）
pip install ".[aiohttp]"      # 仅异步后端
pip install ".[all]"          # 全部
```

`backend="auto"` 时按 `httpx > aiohttp > requests` 顺序自动探测已安装的库。

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

> 上下文管理器会在进入时调用 `open()` / `open_async()`（建立连接资源），退出时调用 `close()` / `close_async()`。也可手动调用：`client.open()` / `await client.open_async()`。对 `aiohttp` 后端，`open_async()` 会在当前事件循环内创建 `ClientSession`。

## API

`HttpClient(backend="auto", rate_limit=None, max_workers=10, max_concurrency=10)`

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

- `max_rate`：每秒允许的最大请求数（`<=0` 表示不限流）。
- `burst`：允许连续突发发送的请求数，超过后进入平滑限速。

## 后端选择约束

- `requests` 仅支持同步 API（调用 `request_async` 会抛 `AsyncRequiredError`）。
- `aiohttp` 仅支持异步 API（调用 `request` 会抛 `AsyncRequiredError`）。
- `httpx` 同时支持同步与异步。

## 开发与 CI

- 测试在 `master` 与 `dev` 分支上运行（见 `.github/workflows/test.yml`），覆盖 Python 3.8–3.12，安装 `.[all]` 以确保功能/并行测试可执行。
- 发布到 PyPI 通过 GitHub Release（`release: published`）触发 `.github/workflows/release.yml`，默认使用 PyPI Trusted Publishing（OIDC）。

本地运行测试：

```bash
pip install -e ".[all]"
python -m unittest discover -s tests -v
```

## License

BSD 3-Clause。详见 [LICENSE](LICENSE)。
