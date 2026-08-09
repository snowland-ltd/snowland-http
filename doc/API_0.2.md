# snowland-http API 文档（v0.2）

> 版本：0.2.0
> 许可证：BSD 3-Clause
> 一个支持**限流**与**并行**的 HTTP 客户端，底层可自由切换 `requests` / `httpx` / `aiohttp`，或零依赖的 `stdlib`（标准库 `urllib`）。

---

## 1. 模块与公开导出

包名：`snowland_http`

```python
from snowland_http import (
    HttpClient,         # 主客户端
    HttpResponse,       # 统一响应对象
    RateLimitConfig,    # 限流配置
    RateLimiter,        # 限流器（令牌桶）
    HttpClientError,    # 异常基类
    BackendNotSupportedError,
    AsyncRequiredError,
)
```

后端相关类（按需使用）：

```python
from snowland_http.backends import (
    BaseBackend,        # 后端抽象基类
    RequestsBackend,    # requests 实现（仅同步）
    HttpxBackend,       # httpx 实现（同步 + 异步）
    AiohttpBackend,     # aiohttp 实现（仅异步）
    StdlibBackend,      # 标准库 urllib 实现（仅同步，零依赖）
)
```

---

## 2. `HttpClient`

高层的 HTTP 客户端。所有请求共享同一个限流器，因此并行时聚合速率受 `RateLimitConfig` 约束。

### 构造

```python
HttpClient(
    backend: str = "auto",
    rate_limit: Optional[RateLimitConfig] = None,
    max_workers: int = 10,
    max_concurrency: int = 10,
)
```

| 参数 | 说明 |
| --- | --- |
| `backend` | 传输后端：`"requests"` / `"httpx"` / `"aiohttp"` / `"stdlib"`（或别名 `"urllib"`） / `"auto"`（自动探测，优先级 httpx > aiohttp > requests > **stdlib**）。未知值抛 `BackendNotSupportedError`。 |
| `rate_limit` | `RateLimitConfig` 实例；`max_rate <= 0` 表示不限流（限流器变为空操作，不抛异常）。 |
| `max_workers` | 同步并行（`request_many` / `get_many`）使用的线程池大小。 |
| `max_concurrency` | 异步并行（`request_many_async` / `get_many_async`）使用的信号量并发上限。 |

### 同步 API

| 方法 | 签名 | 说明 |
| --- | --- | --- |
| `request` | `request(method: str, url: str, **kwargs) -> HttpResponse` | 单次同步请求。`kwargs` 透传给底层库（如 `params`、`headers`、`json`、`data`、`timeout`）。 |
| `get` | `get(url, **kwargs) -> HttpResponse` | `request("GET", ...)` |
| `post` | `post(url, **kwargs) -> HttpResponse` | `request("POST", ...)` |
| `put` | `put(url, **kwargs) -> HttpResponse` | `request("PUT", ...)` |
| `delete` | `delete(url, **kwargs) -> HttpResponse` | `request("DELETE", ...)` |
| `head` | `head(url, **kwargs) -> HttpResponse` | `request("HEAD", ...)` |
| `patch` | `patch(url, **kwargs) -> HttpResponse` | `request("PATCH", ...)` |
| `request_many` | `request_many(items, max_workers=None, return_exceptions=True) -> List[Any]` | 线程池并行请求。 |
| `get_many` | `get_many(urls, method="GET", max_workers=None, return_exceptions=True, **kwargs) -> List[Any]` | 对一批 URL 并行请求。 |
| `open` | `open() -> None` | 打开/建立连接资源（进入同步上下文时自动调用）。 |

### 异步 API

| 方法 | 签名 | 说明 |
| --- | --- | --- |
| `request_async` | `request_async(method: str, url: str, **kwargs) -> HttpResponse` | 单次异步请求。 |
| `get_async` … `patch_async` | `get_async(url, **kwargs) -> HttpResponse` 等 | 异步便捷方法，对应同步版本。 |
| `request_many_async` | `request_many_async(items, max_concurrency=None, return_exceptions=True) -> List[Any]` | `asyncio.gather` + `Semaphore` 并行请求。 |
| `get_many_async` | `get_many_async(urls, method="GET", max_concurrency=None, return_exceptions=True, **kwargs) -> List[Any]` | 对一批 URL 异步并行请求。 |
| `open_async` | `open_async() -> None` | 打开/建立异步连接资源（进入异步上下文时自动调用）。 |

### 并行 `items` 的元素格式

`request_many` / `request_many_async` 的 `items` 中每个元素支持两种形式：

- **dict**：`{"method": "GET", "url": "https://...", **kwargs}`
- **tuple/list**：`(method, url)` 或 `(method, url, kwargs_dict)`

示例：

```python
client.request_many([
    {"method": "GET", "url": "https://a.com"},
    ("POST", "https://b.com", {"json": {"x": 1}}),
])
```

### 并行异常处理

- `return_exceptions=True`（默认）：单个请求失败不会中断其余请求，失败项在结果列表中以**异常对象**形式返回。
- `return_exceptions=False`：任一请求失败即整体抛出。

### 连接开关 / 上下文管理

除了退出时关闭，客户端还支持显式“打开”（建立连接资源）。进入上下文管理器时会先调用 `open` / `open_async`，退出时调用 `close` / `close_async`，从而形成完整的连接生命周期管理。

```python
client.open()                       # 打开同步后端资源（建立连接）
client.close()                      # 关闭同步后端资源

await client.open_async()           # 打开异步后端资源
await client.close_async()          # 关闭异步后端资源

# 上下文管理器：进入即 open，退出即 close
with HttpClient(...) as c:
    ...

# 异步上下文管理器：进入即 open_async，退出即 close_async
async with HttpClient(...) as c:
    ...
```

> 对 `aiohttp` 后端，`open_async()` 会在当前运行的事件循环内创建 `ClientSession`，避免“session 未在循环内创建”的问题；`requests` / `httpx` 后端的 `open()` 为幂等空操作（session/client 已在构造时建立）。`stdlib` 后端无持久连接资源，`open()` / `close()` 为空操作。

### 后端约束

- `requests` 仅支持同步 API；调用 `request_async` 等会抛 `AsyncRequiredError`。
- `aiohttp` 仅支持异步 API；调用 `request` 等会抛 `AsyncRequiredError`。
- `httpx` 同时支持同步与异步。
- `stdlib`（urllib）仅支持同步 API；调用 `request_async` 会抛 `AsyncRequiredError`。

---

## 3. `RateLimitConfig`

令牌桶限流配置（dataclass）。

```python
@dataclass
class RateLimitConfig:
    max_rate: float = 0.0   # 每秒允许的最大请求数；<= 0 表示不限流（限流器变为空操作）
    burst: int = 1          # 允许连续突发的请求数（突发上限）

    def enabled(self) -> bool:
        """max_rate > 0 时返回 True。"""
```

> **v0.2 变更**：`max_rate <= 0` 现在表示**完全禁用限流**（限流器不阻塞、不抛异常）。在 0.1 中该取值会抛 `ValueError`，现已修正为安全的“无限制”语义。

---

## 4. `RateLimiter`

令牌桶限流器，同步与异步共用同一令牌计数。

```python
class RateLimiter:
    def __init__(self, config: RateLimitConfig): ...
    def acquire(self) -> None: ...            # 阻塞获取：不足时 time.sleep 等待；禁用时立即返回
    async def acquire_async(self) -> None: ... # 异步获取：不足时 asyncio.sleep 等待；禁用时立即返回
```

> 内部实现保证“等待时间不会被重复计入补充”，因此连续受限请求会按 `1 / max_rate` 的间隔平滑节流。`enabled == False` 时 `acquire` / `acquire_async` 均为立即返回的空操作。

---

## 5. `HttpResponse`

跨后端统一的响应封装，屏蔽 `requests` / `httpx` / `aiohttp` / `urllib` 的差异。

```python
class HttpResponse:
    def __init__(self, status_code: int, headers: Dict[str, str], content: bytes, url: str, encoding: Optional[str] = None): ...

    @property
    def text(self) -> str: ...        # 按 Content-Type charset（缺省 ISO-8859-1）解码的响应体
    @property
    def ok(self) -> bool: ...         # 200 <= status_code < 300
    def json(self) -> Any: ...        # 解析 JSON
    def raise_for_status(self) -> "HttpResponse": ...  # 非 2xx 抛 RuntimeError

    # 工厂方法（由后端在内部调用，一般不直接使用）
    @classmethod
    def from_requests(cls, resp) -> "HttpResponse": ...
    @classmethod
    def from_httpx(cls, resp) -> "HttpResponse": ...
    @classmethod
    def from_aiohttp(cls, resp, content: bytes) -> "HttpResponse": ...
    @classmethod
    def from_urllib(cls, resp, content: bytes) -> "HttpResponse": ...  # v0.2 新增
```

字段：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `status_code` | `int` | HTTP 状态码 |
| `headers` | `Dict[str, str]` | 响应头 |
| `content` | `bytes` | 原始响应体（bytes） |
| `url` | `str` | 最终请求 URL |
| `encoding` | `str` | 文本解码所用编码（由显式参数 / `Content-Type` charset / 缺省 `iso-8859-1` 决定） |

> **v0.2 变更（编码）**：`text` 不再硬编码 UTF-8 容错解码，而是严格遵循 HTTP 编码规则——优先使用 `Content-Type` 的 `charset`，缺失时回落到 **ISO-8859-1**（RFC 7231 默认值）。解码不使用 `errors="replace"`，因此非法字节会抛出 `UnicodeDecodeError`，而非静默产生乱码。

---

## 6. 异常

| 异常 | 基类中 | 触发场景 |
| --- | --- | --- |
| `HttpClientError` | — | 所有本库异常的基类 |
| `BackendNotSupportedError` | `HttpClientError` | `backend` 未知，或 `auto` 时无任何传输库可用 |
| `AsyncRequiredError` | `HttpClientError` | 在仅同步/仅异步后端上误用了对端 API |

---

## 7. 后端（`snowland_http.backends`）

### `BaseBackend`

```python
class BaseBackend:
    scheme: str = "sync"   # "sync" | "async" | "both"

    def request(self, method, url, **kwargs) -> HttpResponse: ...
    async def request_async(self, method, url, **kwargs) -> HttpResponse: ...
    def close(self) -> None: ...
    async def close_async(self) -> None: ...
```

### `RequestsBackend`（仅同步）

```python
RequestsBackend(session: Optional[requests.Session] = None)
```

### `HttpxBackend`（同步 + 异步）

```python
HttpxBackend(
    client: Optional[httpx.Client] = None,
    async_client: Optional[httpx.AsyncClient] = None,
)
```

### `AiohttpBackend`（仅异步）

```python
AiohttpBackend(session: Optional[aiohttp.ClientSession] = None)
```

### `StdlibBackend`（仅同步，零依赖）  *v0.2 新增*

```python
StdlibBackend()
```

基于标准库 `urllib.request` 实现，无需安装任何第三方库。`scheme = "sync"`，支持 `method` / `url` / `headers` / `params` / `data`（bytes、str 或 dict 表单）/ `json` / `timeout` 等参数。在 `backend="auto"` 且未安装任何第三方传输库时自动被选用。

> 后端为惰性加载：未安装对应库时，包仍可正常 import，仅该后端不可用；`stdlib` 后端始终可用。

---

## 8. 版本说明（v0.2）

- 新增零依赖的 `stdlib`（urllib）后端，`auto` 模式下可在无第三方库时自动回退。
- 响应体文本解码遵循 HTTP 编码规则（charset 优先，缺省 ISO-8859-1），不再假设 UTF-8，且不再静默容错。
- 限流器在 `max_rate <= 0` 时安全禁用（不阻塞、不抛异常）。
- 支持同步（`requests` / `httpx` / `stdlib`）与异步（`httpx` / `aiohttp`）两种模式。
- 全局令牌桶限流，限流对并行请求生效。
- 同步并行基于线程池，异步并行基于 `asyncio.gather` + `Semaphore`。
- 第三方传输库均为可选依赖，可单独或组合安装；`stdlib` 始终可用。
