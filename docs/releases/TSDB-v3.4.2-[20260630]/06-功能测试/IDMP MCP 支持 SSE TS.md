# IDMP MCP 支持 SSE TS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- |-----| -- |
| 2026-04-20 | 2026-04-20 | 0.1 | 谭雪峰 | 初稿 |

## 2. 测试目标

本需求的测试目标如下：

- 验证 `mcp-tdengine-idmp` 远程模式新增 `sse_endpoint_path` 后，默认值、环境变量/命令行覆盖、路径归一化和冲突校验符合 FS。
- 验证 `mcp-tdengine-idmp` 在同一监听端口下同时承载 streamable HTTP 与 legacy SSE 时，路由分发、message endpoint 回传和上下文注入行为正确。
- 验证 TDasset 对外新增 `/api/v1/mcp/sse` 后，路由接管、协议透传、首个 `event: endpoint` 改写和错误路径处理符合设计。
- 验证前端头像设置页生成的 MCP 配置示例已区分 Streamable HTTP 与 SSE，且 `type`、URL 和 Header 示例保持一致。

## 3. 参考文档

1. 功能说明：[IDMP MCP 支持 SSE FS](../05-设计文档/IDMP%20MCP%20支持%20SSE%20FS.md)
2. 关联 PR：
   - [taosdata/TDasset#3340](https://github.com/taosdata/TDasset/pull/3340)
   - [taosdata/mcp-tdengine-idmp#6](https://github.com/taosdata/mcp-tdengine-idmp/pull/6)
3. TDasset 相关测试文件：
   - `frontend/tests/unit/views/6-others/setting/mcp.spec.ts`
   - `tda-server/src/test/java/com/taosdata/asset/common/filter/McpProxyRouterTest.java`
   - `tda-server/src/test/java/com/taosdata/asset/resource/McpResourceTest.java`
   - `tda-server/src/test/java/com/taosdata/asset/resource/McpResourceUnitTest.java`
   - `tda-server/src/test/java/com/taosdata/asset/service/ai/McpProxyServiceExceptionPathTest.java`
   - `tda-server/src/test/java/com/taosdata/asset/service/ai/McpProxyServiceTest.java`
4. `mcp-tdengine-idmp` 相关测试文件：
   - `config/config_test.go`
   - `config/config_extra_test.go`
   - `system/server_test.go`

## 4. 测试结论

已为 IDMP MCP SSE 能力补齐覆盖两侧仓库关键路径的自动化测试。`mcp-tdengine-idmp` 的远程配置、legacy SSE 建链/消息回传分流、TDasset 的 `/api/v1/mcp/stream` 与 `/api/v1/mcp/sse` 代理行为、SSE endpoint 改写、路由异常处理，以及前端配置示例生成均已纳入自动化回归范围，相关测试全部通过。

## 5. 测试环境

- OS: Linux、Windows（跨平台配置、路径和兼容分支通过单元测试覆盖）
- TDasset Backend: Java 21、Quarkus Test、JUnit 5、Mockito、Vert.x、MockWebServer
- Frontend: Vitest
- `mcp-tdengine-idmp`: Go test、`net/http/httptest`
- Upstream 模拟：
  - TDasset 侧使用 `MockWebServer` 模拟 upstream MCP 服务
  - `mcp-tdengine-idmp` 侧使用 `httptest.NewServer` 和 SDK client 验证 streamable HTTP / legacy SSE 行为

## 6. 功能测试

### 6.1 `mcp-tdengine-idmp` 远程配置与启动参数

#### 6.1.1 测试要点

- `sse_endpoint_path` 默认值是否为 `/sse`。
- 命令行参数和环境变量是否支持覆盖默认值，并保持路径归一化。
- 远程模式配置校验是否拒绝 `sse_endpoint_path == endpoint_path`。
- 远程模式启动时，监听地址、streamable HTTP 路径和 SSE 路径是否同时传入运行时。

#### 6.1.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 默认配置补齐 SSE 路径 | `config/config_test.go:TestInitConfigFromEnv` 与 `config/config_extra_test.go:TestInitConfigDefaultsLocalAndRemoteSettings`：校验未显式配置时 `SSEEndpointPath` 自动采用默认值 `/sse`。 | 通过（自动化） |
| 2 | SSE 路径支持 flag 覆盖 env | `config/config_test.go:TestInitConfigSSEEndpointPathFlagOverridesEnv`：校验 `--sse_endpoint_path` 的优先级高于 `IDMP_SSE_ENDPOINT_PATH`。 | 通过（自动化） |
| 3 | 远程模式环境变量归一化 | `config/config_extra_test.go:TestInitConfigRemoteModeFromEnv`：校验 `remote-mcp`、`legacy-sse` 这类无前导斜杠配置会被归一化为 `/remote-mcp`、`/legacy-sse`。 | 通过（自动化） |
| 4 | 远程模式自动补默认 SSE 路径 | `config/config_extra_test.go:TestConfigValidateLocalAndRemote/remote valid`：校验远程模式只配置 `endpoint_path` 时，`Validate()` 会自动补齐 `/sse`。 | 通过（自动化） |
| 5 | 拒绝 SSE 路径与主路径冲突 | `config/config_extra_test.go:TestConfigValidateLocalAndRemote/remote rejects matching sse endpoint path`：校验 `sse_endpoint_path` 与 `endpoint_path` 相同时启动校验失败。 | 通过（自动化） |
| 6 | 远程启动透传两类路径 | `system/server_test.go:TestStartRemoteSuccess`：校验远程启动时 `listen_addr`、`endpoint_path` 和 `sse_endpoint_path` 会同时传入 `serveRemoteHTTP`。 | 通过（自动化） |

### 6.2 `mcp-tdengine-idmp` 远程 HTTP mux 与 legacy SSE 分流

#### 6.2.1 测试要点

- 同一 HTTP Server 下是否同时支持 streamable HTTP 与 legacy SSE。
- 自定义路径和默认路径场景下，SSE 首包返回的 message endpoint 是否指向 `endpoint_path`。
- legacy SSE 消息回传识别规则是否为：`POST + sessionId query + 无 Mcp-Session-Id`。
- 远程上下文是否继续提取 Bearer Token、`Accept-Language` 与来源 IP。

#### 6.2.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 同时支持 streamable HTTP 与 legacy SSE | `system/server_test.go:TestRemoteHTTPHandlerSupportsStreamableHTTPAndLegacySSE`：校验默认路径 `/mcp` `/sse` 与自定义路径 `/mcp-remote` `/legacy-sse` 均可被官方 client 正常初始化和 `Ping`。 | 通过（自动化） |
| 2 | SSE 首包回传主消息路径 | `system/server_test.go:TestRemoteHTTPHandlerSupportsStreamableHTTPAndLegacySSE`：校验 legacy SSE client 发现的 endpoint path 始终回指 `endpoint_path`，且携带 `sessionId`。 | 通过（自动化） |
| 3 | legacy SSE message POST 分流 | `system/server_test.go:TestIsLegacySSEMessageRequest`：校验只有满足 `POST + sessionId query + 无 session header` 的请求才按 legacy SSE message request 处理。 | 通过（自动化） |
| 4 | 远程上下文注入 Bearer Token 和语言 | `system/server_test.go:TestRemoteHTTPContextInjectsBearerTokenAndQID`：校验 `Authorization`、`Accept-Language`、QID 与 remote address 被写入 context。 | 通过（自动化） |
| 5 | 转发来源优先取 X-Forwarded-For | `system/server_test.go:TestRemoteHTTPContextPrefersForwardedClientIP`：校验存在 `X-Forwarded-For` 时优先使用该地址，而不是 `X-Real-IP`。 | 通过（自动化） |

### 6.3 TDasset 路由接管与占位 Resource 行为

#### 6.3.1 测试要点

- `McpProxyRouter` 是否为 `/api/v1/mcp/stream` 注册 GET/POST/DELETE，为 `/api/v1/mcp/sse` 注册 GET。
- Stream 路由与 SSE 路由是否分别委托给 `handleStream` 和 `handleSse`。
- 路由异常时，`ComException` 和 generic exception 是否按 JSON 错误响应输出。
- `McpResource` 是否只保留 OpenAPI 占位，运行时请求应快速失败并提示由 Router 接管。

#### 6.3.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 注册所有 HTTP 方法与 SSE 路由 | `McpProxyRouterTest.installRouteShouldRegisterAllHttpMethods`：校验 stream 路由覆盖 GET/POST/DELETE，SSE 路由覆盖 GET。 | 通过（自动化） |
| 2 | Stream 请求委托给代理服务 | `McpProxyRouterTest.installedHandlerShouldDelegateToProxyService`：校验 stream 请求由 Router 直接下沉到 `handleStream`。 | 通过（自动化） |
| 3 | SSE 请求委托给代理服务 | `McpProxyRouterTest.installedSseHandlerShouldDelegateToProxyService`：校验 SSE GET 请求由 Router 下沉到 `handleSse`。 | 通过（自动化） |
| 4 | ComException 返回 JSON 错误体 | `McpProxyRouterTest.installedHandlerShouldWriteComExceptionAsJson`：校验 `502` 和 detail/message 被正确序列化为 JSON。 | 通过（自动化） |
| 5 | generic exception 输出 root cause | `McpProxyRouterTest.installedHandlerShouldWriteRootCauseForGenericException`：校验非业务异常转成 `500`，detail 中保留根因。 | 通过（自动化） |
| 6 | 响应已关闭时不重复写错误 | `McpProxyRouterTest.installedHandlerShouldSkipFailureWriteWhenResponseAlreadyClosed`：校验关闭态响应不会重复写状态码和 body。 | 通过（自动化） |
| 7 | Resource 仅保留占位行为 | `McpResourceUnitTest` 中 4 个 `proxy*ShouldFailFastBecauseRouterOwnsRuntimeTraffic` 用例：校验 `/stream` 的 GET/POST/DELETE 和 `/sse` 的 GET 占位方法均快速失败，避免与 Router 真实流量路径冲突。 | 通过（自动化） |

### 6.4 TDasset 代理透传、SSE 改写与异常路径

#### 6.4.1 测试要点

- `/api/v1/mcp/stream` 的 GET/POST/DELETE 是否继续透传 query、body、关键 headers 与响应。
- `/api/v1/mcp/sse` 是否映射到 upstream sibling `/sse` 路径。
- 首个 `event: endpoint` 的 `/mcp?sessionId=...` 是否改写为 `/api/v1/mcp/stream?sessionId=...`。
- `X-Forwarded-For`、`Authorization` 与 remote address 的回退/透传规则是否正确。
- 上游 URL 非法、上游流异常、root path sibling 推导等异常路径是否有覆盖。

#### 6.4.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | POST 代理透传 headers/body/response | `McpResourceTest.proxyPostShouldForwardHeadersBodyAndResponseHeaders`：校验 POST 请求体、`Accept-Language`、`Mcp-Session-Id`、`Accept`、`Authorization` 和响应头透传。 | 通过（自动化） |
| 2 | Streamable HTTP GET 流式透传 | `McpResourceTest.proxyGetShouldForwardSseHeadersAndBody`：校验 GET 请求的 SSE 相关 header、body 和 query string 透传。 | 通过（自动化） |
| 3 | DELETE 状态透传 | `McpResourceTest.proxyDeleteShouldForwardSessionHeaderAndStatus`：校验 DELETE 请求对上游状态码的原样回传。 | 通过（自动化） |
| 4 | Authorization 原样透传 | `McpResourceTest.proxyPostShouldAllowAnonymousAuthorizationPassthrough`：校验调用方显式传入的 Bearer Token 不被代理层改写。 | 通过（自动化） |
| 5 | X-Forwarded-For 继承与回退 | `McpResourceTest.proxyPostShouldForwardProvidedForwardedForHeader` 与 `proxyPostShouldFallbackForwardedForToRemoteAddress`：校验已有 `X-Forwarded-For` 透传，缺失时可回退到 loopback/remote address。 | 通过（自动化） |
| 6 | `/mcp/sse` 映射到 sibling `/sse` | `McpResourceTest.proxySseGetShouldForwardToSiblingSsePath` 与 `McpProxyServiceTest.prepareProxyRequestShouldRewriteSsePathAndFallbackToRemoteAddress`：校验 SSE 请求会改写到 upstream sibling `/sse` 路径，并补齐来源地址。 | 通过（自动化） |
| 7 | 首帧 endpoint 改写为代理 stream 路径 | `McpResourceTest.proxySseGetShouldRewriteEndpointToProxyStreamPath`、`McpProxyServiceTest.rewriteSseEndpointFrameShouldMapToProxyStreamPath` 与 `rewriteSseEndpointBodyShouldHandleChunkedEndpointFrame`：校验无论整帧还是 chunked 帧，`/mcp?sessionId=...` 都会改写为 `/api/v1/mcp/stream?sessionId=...`。 | 通过（自动化） |
| 8 | 避免误改写更长前缀 | `McpProxyServiceTest.rewriteSseEndpointFrameShouldIgnoreLongerPathPrefixes`：校验 `/mcpExtra` 等非目标路径不会被错误替换。 | 通过（自动化） |
| 9 | sibling path 推导与 root fallback | `McpProxyServiceTest.buildSseTargetPathShouldResolveSiblingPath` 与 `McpProxyServiceExceptionPathTest.buildSseTargetPathShouldFallbackToDefaultWhenUpstreamPathIsRoot`：校验 upstream 为 `/nested/mcp` 时推导 `/nested/sse`，upstream 为 root 时回退到默认 `/sse`。 | 通过（自动化） |
| 10 | upstream 配置错误与流异常处理 | `McpProxyServiceTest.buildOriginRequestOptionsShouldRejectInvalidUpstreamUrl`、`McpProxyServiceExceptionPathTest.buildOriginRequestOptionsShouldRejectBlankUpstreamUrl`、`rewriteSseEndpointBodyShouldPropagateUpstreamException` 与 `resolveForwardedForShouldReturnNullWhenRemoteAddressMissing`：校验非法 upstream、上游断流和缺失 remote address 的异常路径。 | 通过（自动化） |

### 6.5 前端 MCP 配置展示与示例生成

#### 6.5.1 测试要点

- 前端配置示例是否区分 `type: "http"` 与 `type: "sse"`。
- Streamable HTTP 与 SSE 的 URL 是否分别落到 `/api/v1/mcp/stream` 与 `/api/v1/mcp/sse`。
- `origin` 末尾斜杠是否会被正确裁剪，避免出现双斜杠。
- 未提供 `origin` 时是否回退到占位地址。

#### 6.5.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 生成 Streamable HTTP 示例 | `frontend/tests/unit/views/6-others/setting/mcp.spec.ts` 第 1 个用例：校验生成结果包含 `type: "http"`、`/api/v1/mcp/stream` 和 `Authorization` Header。 | 通过（自动化） |
| 2 | 生成 SSE 示例 | `frontend/tests/unit/views/6-others/setting/mcp.spec.ts` 第 2 个用例：校验生成结果包含 `type: "sse"` 和 `/api/v1/mcp/sse`。 | 通过（自动化） |
| 3 | origin 末尾斜杠不生成双斜杠 | `frontend/tests/unit/views/6-others/setting/mcp.spec.ts` 第 3 个用例：校验 `https://idmp.example.com/` 会归一化为单斜杠 URL。 | 通过（自动化） |
| 4 | 缺失 origin 时回退占位地址 | `frontend/tests/unit/views/6-others/setting/mcp.spec.ts` 第 4 个用例：校验未传 origin 时回退到 `https://path/to/api/v1/mcp/sse`。 | 通过（自动化） |

## 7. 易用性测试（可选）

无

## 8. 长期稳定性测试（可选）

无

## 9. 性能测试

无

## 10. 安全测试

本次自动化覆盖的安全相关点包括：

1. `Authorization` 透传与 Bearer Token 提取逻辑，确保远程模式不在代理层持久化凭据。
2. `X-Forwarded-For` / `X-Real-IP` / remote address 的优先级和回退路径，保证来源可追踪。
3. SSE 首帧只改写目标 message endpoint，不会把 `/mcpExtra` 等非目标路径误改写到代理路径。
4. Resource 占位接口快速失败，避免路由层和 JAX-RS 层同时持有运行时流量入口。

## 11. 兼容性测试

当前自动化已覆盖以下兼容性点：

1. `mcp-tdengine-idmp` 默认路径 `/mcp` `/sse` 与自定义路径 `/mcp-remote` `/legacy-sse` 的兼容行为。
2. streamable HTTP 与 legacy SSE 在同一监听地址下共存。
3. TDasset 同时对外暴露 `/api/v1/mcp/stream` 与 `/api/v1/mcp/sse`，并把 legacy SSE 后续 POST 继续落到 `/api/v1/mcp/stream`。
4. 前端 MCP 示例同时支持 `type: "http"` 与 `type: "sse"`。

## 12. 已知问题和限制（可选）

无
