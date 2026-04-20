# IDMP MCP 支持 SSE FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-04-17 | 2026-04-17 | 0.1 | 谭雪峰 | 新增 IDMP MCP 对旧版 SSE 兼容入口的支持方案，补充 TDasset 代理行为、配置、测试与部署说明 |

## 2. 背景

TDasset 当前通过 `/api/v1/mcp/stream` 反代本地 `mcp-tdengine-idmp` 进程，为 IDE、CLI 和 SDK 提供 MCP 能力。原有实现存在两个问题：

1. TDasset 侧的 `/mcp/stream` 历史上基于阻塞式转发实现，长连接会占用 worker 线程，不适合同时承载 streamable HTTP 和 SSE。
2. `mcp-tdengine-idmp` 远程模式原先只暴露 streamable HTTP 入口，不具备旧版 SSE 兼容入口，导致官方 `SSEClientTransport` 无法直接接入。

本次需求的目标是：

1. 在 `mcp-tdengine-idmp` 侧补齐远程旧版 SSE 兼容入口。
2. 在 TDasset 侧同时支持 `/api/v1/mcp/stream` 与 `/api/v1/mcp/sse`，并保持非阻塞代理。
3. 对外只暴露稳定的代理路径，不要求调用方直接感知本地 6037 进程的内部路径。

## 3. 定义

### 3.1 Streamable HTTP

MCP 新版 HTTP 传输方式。客户端通过 `/mcp` 建立请求，服务端通过 `Mcp-Session-Id` 标识会话；可选存在一条附加的 SSE 监听流，用于服务端向客户端推送消息。

### 3.2 Legacy SSE

MCP 旧版 SSE 兼容传输方式。客户端先通过 `/sse` 建立 SSE 连接，服务端在首个 `event: endpoint` 中返回消息回传地址，客户端随后对该地址执行 POST。

### 3.3 Message Endpoint

Legacy SSE 模式下，SSE 首包 `event: endpoint` / `data:` 字段中声明的后续 POST 目标地址。本方案中 upstream 返回 `/mcp?sessionId=...`，TDasset 代理对外改写为 `/api/v1/mcp/stream?sessionId=...`。

### 3.4 Upstream

本方案中的 upstream 指本地独立进程 `mcp-tdengine-idmp`，默认监听 `127.0.0.1:6037`。

### 3.5 Proxy

本方案中的 proxy 指 TDasset 中 `tda-server` 提供的代理入口：

1. `/api/v1/mcp/stream`
2. `/api/v1/mcp/sse`

## 4. 行为说明

### 4.1 `mcp-tdengine-idmp` 远程模式新增 SSE 兼容入口

`/mnt/e/github/mcp-tdengine-idmp` 本次修改新增 `sse_endpoint_path` 配置项，并在远程模式下同时暴露：

1. `endpoint_path`，默认 `/mcp`
2. `sse_endpoint_path`，默认 `/sse`

新增配置项如下：

| 参数 | 环境变量 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `--endpoint_path` | `IDMP_ENDPOINT_PATH` | `/mcp` | Streamable HTTP 入口与 legacy SSE 的消息回传入口 |
| `--sse_endpoint_path` | `IDMP_SSE_ENDPOINT_PATH` | `/sse` | Legacy SSE 建链入口 |

行为约束如下：

1. 远程模式下必须提供 `endpoint_path`。
2. `sse_endpoint_path` 为空时自动回退为 `/sse`。
3. `sse_endpoint_path` 必须与 `endpoint_path` 不同，否则进程启动校验失败。

### 4.2 `mcp-tdengine-idmp` 的路由分发逻辑

`mcp-tdengine-idmp` 远程模式不再只启动一个 streamable HTTP server，而是改为统一的 `http.ServeMux`，同时挂载：

1. `GET {sse_endpoint_path}` -> legacy SSE `SSEHandler`
2. `{endpoint_path}` -> 统一入口

统一入口中对 POST 请求增加以下分流规则：

1. 如果请求方法是 `POST`
2. 且 query 中存在 `sessionId`
3. 且请求头中不存在 `Mcp-Session-Id`

则按 legacy SSE 消息回传请求处理；否则按 streamable HTTP 请求处理。

这使两种 transport 可以在同一进程、同一监听端口下并存。

### 4.3 TDasset 代理入口调整

TDasset 本次保留现有代理能力，同时新增独立 SSE 入口：

| 对外路径 | 方法 | 对应 upstream | 说明 |
| --- | --- | --- | --- |
| `/api/v1/mcp/stream` | `GET/POST/DELETE` | `/mcp` | Streamable HTTP 代理入口，同时承接 legacy SSE 的 message POST |
| `/api/v1/mcp/sse` | `GET` | `/sse` | Legacy SSE 建链代理入口 |

`McpResource` 不再承载真实代理逻辑，仅保留 OpenAPI 占位；真实流量由 `McpProxyRouter` 在 Vert.x route 层提前接管。

### 4.4 TDasset 代理的协议适配

TDasset 对 `/api/v1/mcp/sse` 做了一处最小协议适配：

1. 正常反代 upstream `/sse` 响应头与响应体。
2. 仅在首个 `event: endpoint` 帧中，将 upstream 返回的 `/mcp?sessionId=...` 改写为代理对外路径 `/api/v1/mcp/stream?sessionId=...`。
3. 首帧改写完成后，后续 SSE 数据块不再做任何正文改写，继续透明透传。

这样可以满足两个目标：

1. upstream 不需要知道 TDasset 的公开访问前缀。
2. 官方 `SSEClientTransport` 拿到 `endpoint` 后，后续 POST 可以自然落到代理入口，而不是绕过代理直接访问本地 6037。

### 4.5 请求头与上下文透传

#### 4.5.1 TDasset -> `mcp-tdengine-idmp`

TDasset 代理会透传或补齐以下上下文：

1. `Authorization`
2. `Accept-Language`
3. `Mcp-Session-Id`
4. `Last-Event-ID`
5. `X-Forwarded-For`

`X-Forwarded-For` 的处理规则如下：

1. 如果调用方已传 `X-Forwarded-For`，则原样透传。
2. 否则若存在 `X-Real-IP`，则用 `X-Real-IP` 补齐。
3. 再否则退回到当前连接的 remote address。

#### 4.5.2 `mcp-tdengine-idmp` -> IDMP

`mcp-tdengine-idmp` 在 `remoteHTTPContext` 中继续提取：

1. Bearer Token
2. `Accept-Language`
3. `X-Forwarded-For` / `X-Real-IP` / remote address

并用于上游 IDMP 访问与日志追踪。

### 4.6 非阻塞代理行为

TDasset 不再使用旧版 `@Blocking + OkHttp + StreamingOutput` 方案，而是统一改为 Vert.x `HttpProxy`：

1. `/api/v1/mcp/stream` 直接走 `HttpProxy`
2. `/api/v1/mcp/sse` 也走 `HttpProxy`
3. 只有 `/api/v1/mcp/sse` 的首个 endpoint 事件会经过一个极小的逐响应流包装器，用于首帧路径改写

除该首帧改写外，代理层不维护用户级会话状态，也不缓存消息内容。

### 4.7 接口示例

#### 4.7.1 Streamable HTTP 示例

```bash
curl -X POST \
  'http://127.0.0.1:6042/api/v1/mcp/stream' \
  -H 'Authorization: Bearer <idmp-token>' \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -d '{
    "jsonrpc":"2.0",
    "id":1,
    "method":"initialize",
    "params":{
      "protocolVersion":"2025-03-26",
      "capabilities":{},
      "clientInfo":{"name":"example-client","version":"1.0.0"}
    }
  }'
```

#### 4.7.2 Legacy SSE 示例

```bash
curl -N \
  'http://127.0.0.1:6042/api/v1/mcp/sse' \
  -H 'Authorization: Bearer <idmp-token>' \
  -H 'Accept: text/event-stream'
```

期望首包示例：

```text
event: endpoint
data: /api/v1/mcp/stream?sessionId=<session-id>
```

#### 4.7.3 Node.js SDK 示例

```js
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { SSEClientTransport } from '@modelcontextprotocol/sdk/client/sse.js';

const client = new Client({ name: 'example', version: '1.0.0' });
const transport = new SSEClientTransport(
  new URL('http://127.0.0.1:6042/api/v1/mcp/sse'),
  {
    requestInit: {
      headers: {
        Authorization: 'Bearer <idmp-token>',
        Accept: 'application/json, text/event-stream'
      }
    }
  }
);

await client.connect(transport);
const tools = await client.listTools();
console.log(tools.tools.map(item => item.name));
```

### 4.8 错误处理

| 场景 | 返回/现象 | 说明 |
| --- | --- | --- |
| `mcp-tdengine-idmp` 仍是旧版本，不支持 `/sse` | `/api/v1/mcp/sse` 返回 upstream `404` | 需要升级 `mcp-tdengine-idmp` 二进制 |
| `sse_endpoint_path` 与 `endpoint_path` 相同 | 远程模式启动失败 | 配置校验拒绝，避免 SSE 与 streamable HTTP 路由冲突 |
| TDasset `tda.mcp-proxy.upstream-url` 非法 | 代理返回 `500` JSON 错误 | 属于代理配置错误，不是业务错误 |
| 客户端错误地向 `/api/v1/mcp/sse` 发送 POST | upstream 或代理返回 `405` | Legacy SSE 的 POST 应回传到 `/api/v1/mcp/stream?sessionId=...` |
| 上游连接中断 | 当前请求失败或流结束 | 仅影响当前请求/当前连接，不影响其他连接 |

## 5. 性能

1. TDasset 侧使用 Vert.x `HttpProxy` 承接流式代理，避免旧版阻塞式实现长期占用 worker 线程。
2. `/api/v1/mcp/sse` 的额外开销仅限于首个 SSE frame 的局部缓存与改写，缓存上限为 8KB；超过后直接放行，不会持续缓存整条流。
3. `/api/v1/mcp/stream` 不引入正文改写，保持透明转发。
4. `mcp-tdengine-idmp` 侧通过统一的 HTTP mux 同时承载 streamable HTTP 与 legacy SSE，不需要额外监听第二个端口。

## 6. 安全

1. Bearer Token 仍由调用方按请求提供，TDasset 与 `mcp-tdengine-idmp` 不新增 token 持久化。
2. `X-Forwarded-For` 在代理链中保留，有利于审计与来源追踪。
3. SSE 首帧改写仅修改当前响应里的 message endpoint 路径，不会注入其他用户的 `sessionId`。
4. TDasset 代理实现不保存用户级消息缓存或会话映射，避免代理层因共享状态导致 A/B 串消息。

## 7. 兼容性

1. `/api/v1/mcp/stream` 作为既有 streamable HTTP 入口继续保留，现有调用方式不变。
2. `/api/v1/mcp/sse` 为新增能力，对已有 streamable HTTP 客户端无破坏性影响。
3. `mcp-tdengine-idmp` 增加新配置项 `sse_endpoint_path`，默认值为 `/sse`，旧配置文件未显式配置时可自动兼容。
4. `mcp-tdengine-idmp` 升级了 `github.com/mark3labs/mcp-go` 到 `v0.48.0`，用于支持新的 legacy SSE server 能力。

## 8. 运维

无

## 9. 使用场景

1. 支持新版 `StreamableHTTPClientTransport` 的 IDE、CLI、Agent 继续走 `/api/v1/mcp/stream`。
2. 只能使用官方旧版 `SSEClientTransport` 的客户端可改走 `/api/v1/mcp/sse`。
3. 反代场景下，外部客户端无需感知本地 `6037` 的真实路径布局。
4. 同一环境中可以同时存在 streamable HTTP 客户端与 legacy SSE 客户端。

## 10. 约束和限制

1. `/api/v1/mcp/sse` 只负责 SSE 建链，legacy SSE 的后续消息 POST 仍落到 `/api/v1/mcp/stream`。
2. TDasset 当前只改写首个 `event: endpoint` frame；如果 upstream 将来改变 legacy SSE 首包格式，需要同步调整改写逻辑。
3. `tda.mcp-proxy.upstream-url` 仍以 upstream `/mcp` 为基准，`/sse` 由 sibling path 推导得到。
4. 代理层不做按用户二次绑定的 session 管控；sessionId 仍由 upstream MCP 会话语义定义。

## 11. 常见错误和排查

| 错误现象 | 排查方向 | 处理方式 |
| --- | --- | --- |
| `/api/v1/mcp/sse` 返回 `404 page not found` | `mcp-tdengine-idmp` 仍运行旧二进制 | 替换 live binary 为支持 `sse_endpoint_path` 的版本 |
| 客户端建立 SSE 成功，但随后 POST 返回 `405` | 首包 `endpoint` 未指向 `/api/v1/mcp/stream` | 检查 TDasset 是否已部署包含 endpoint 改写的版本 |
| `/api/v1/mcp/stream` 返回 `500` JSON 错误 | `tda.mcp-proxy.upstream-url` 配置非法 | 修正 `application.yml` 中的 upstream URL |
| 验证脚本打印 `AbortError` | SDK 在主动 `close()` 时中断内部 GET SSE 监听流 | 若 `toolCount` 已返回且退出码为 0，可视为验证成功，不是服务崩溃 |

## 12. 可观测性

1. TDasset 仍可通过现有 `LoggingFilter` 观察 `/api/v1/mcp/stream`、`/api/v1/mcp/sse` 请求日志。
2. `mcp-tdengine-idmp` 继续在远程模式记录启动参数、来源 IP、QID 等上下文。
3. 对外无 UI 界面变化；本次改动主要影响 API 与代理行为。

## 13. 安装和卸载

无。

## 14. 文档

需要修改文档

## 15. 参考文档

## 16. 附录

### 16.1 交互关系

```text
Legacy SSE client
  ├─ GET /api/v1/mcp/sse
  │    └─ TDasset proxy -> GET /sse
  │         └─ upstream 返回: event:endpoint data:/mcp?sessionId=...
  │              └─ TDasset 改写为: /api/v1/mcp/stream?sessionId=...
  └─ POST /api/v1/mcp/stream?sessionId=...
       └─ TDasset proxy -> POST /mcp?sessionId=...
            └─ upstream 按 legacy SSE message request 处理

Streamable HTTP client
  └─ GET/POST/DELETE /api/v1/mcp/stream
       └─ TDasset proxy -> /mcp
            └─ upstream 按 streamable HTTP 处理
```
