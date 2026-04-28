# IDMP MCP 代理 TS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-04-09 | 2026-04-09 | 0.1 | 谭雪峰 | 初稿 |

## 2. 测试目标

本需求的测试目标如下：

- 验证 `/api/v1/mcp/stream` 的 GET/POST/DELETE 代理行为、query/body/headers 透传及上游响应透传。
- 验证流式 HTTP 响应、资源释放和异常清理逻辑。
- 验证托管 MCP 子进程的启动、超时、中断清理，以及 `--log_path` 日志目录传递行为。
- 验证 `mcp-tdengine-idmp` 二进制在不同平台和安装目录下的解析规则。
- 验证 `base-url` 自动推导、`quarkus.http.insecure-requests` 分支、固定 `remote` 模式命令生成以及不支持场景的防御逻辑。

## 3. 参考文档

1. 功能说明：[IDMP MCP 代理功能设计 FS](../05-设计文档/IDMP%20MCP%20%E4%BB%A3%E7%90%86%20FS.md)
2. 相关测试类：
   - `tda-server/src/test/java/com/taosdata/asset/resource/McpResourceTest.java`
   - `tda-server/src/test/java/com/taosdata/asset/service/ai/McpProxyServiceTest.java`
   - `tda-server/src/test/java/com/taosdata/asset/service/ai/McpProxyServiceExceptionPathTest.java`
   - `tda-server/src/test/java/com/taosdata/asset/service/ai/McpProxyProcessServiceTest.java`
   - `tda-server/src/test/java/com/taosdata/asset/service/ai/McpProxyProcessServiceCleanupTest.java`
   - `tda-server/src/test/java/com/taosdata/asset/service/ai/McpProxyProcessServiceLogPathTest.java`
   - `tda-server/src/test/java/com/taosdata/asset/service/ai/McpProxyProcessSupportTest.java`
   - `tda-server/src/test/java/com/taosdata/asset/service/ai/McpProxyProcessSupportCommandTest.java`
   - `tda-server/src/test/java/com/taosdata/asset/service/ai/McpProxyProcessSupportLogPathTest.java`
   - `tda-server/src/test/java/com/taosdata/asset/common/util/McpProxyBinaryUtilTest.java`
   - `tda-server/src/test/java/com/taosdata/asset/common/util/McpProxyBinaryUtilReleaseNameTest.java`
3. 测试辅助类：
   - `tda-server/src/test/java/com/taosdata/asset/service/ai/FakeMcpProxyMain.java`

## 4. 测试结论

已为 MCP 代理能力补齐一组覆盖关键路径的自动化测试。按现有用例设计，代理入口、流式回传、响应清理、托管进程生命周期、日志目录下发、`base-url` 分支解析和跨平台二进制命名均已纳入自动化回归范围，具备作为发布前回归基线的条件。

本 TS 文档基于仓库中已实现的测试用例整理，不额外引入手工执行截图或性能压测数据。

## 5. 测试环境

- OS: Linux、Windows（平台相关命令和二进制命名通过单元测试覆盖）
- Browser: Chrome（如需通过 Swagger/UI 做联调）
- Backend: Quarkus Test、JUnit 5、Mockito、MockWebServer
- DB: H2 TCP Server，`localhost:6039`
- Upstream 模拟：`MockWebServer`，通过 `tda.mcp-proxy.upstream-url` 指向模拟 MCP 上游

## 6. 功能测试

### 6.1 代理入口与协议透传

#### 6.1.1 测试要点

- `/api/v1/mcp/stream` 是否支持 GET/POST/DELETE。
- query string、请求体以及关键请求头是否正确透传到上游。
- 上游状态码、响应头和响应体是否按预期回传。
- `X-Original-URI` 和 `X-Forwarded-For` 是否正确补充。

#### 6.1.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | POST 代理转发基础链路 | `McpResourceTest.proxyPostShouldForwardHeadersBodyAndResponseHeaders`：校验 POST 请求体、`Accept-Language`、`Mcp-Session-Id`、`Accept`、`Authorization` 和 `X-Original-URI` 透传，以及响应头回传。 | 已实现自动化 |
| 2 | GET 流式 HTTP 响应透传 | `McpResourceTest` 中的 GET 流式响应用例：校验 `Last-Event-ID`、`text/event-stream` 相关响应和 query 透传，确认流式 HTTP 响应能够被代理。 | 已实现自动化 |
| 3 | DELETE 状态透传 | `McpResourceTest.proxyDeleteShouldForwardSessionHeaderAndStatus`：校验 `DELETE /mcp/stream` 对上游 405 的原样回传。 | 已实现自动化 |
| 4 | Authorization 原样透传 | `McpResourceTest.proxyPostShouldAllowAnonymousAuthorizationPassthrough`：校验调用方显式传入的 Bearer Token 被原样送往上游。 | 已实现自动化 |
| 5 | X-Forwarded-For 继承 | `McpResourceTest.proxyPostShouldForwardProvidedForwardedForHeader`：校验调用方自带 `X-Forwarded-For` 时不被覆盖。 | 已实现自动化 |
| 6 | X-Forwarded-For 回退 | `McpResourceTest.proxyPostShouldFallbackForwardedForToRemoteAddress`：校验未显式传入时可从远端地址回推出 loopback 来源。 | 已实现自动化 |

### 6.2 响应流与资源清理

#### 6.2.1 测试要点

- 上游流式响应是否能被完整写回。
- 请求提前结束或异常时，上游响应体是否被正确关闭，避免泄漏。
- `RoutingContext` 清理回调失败时是否仍能确保关闭资源。

#### 6.2.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 路由提前结束时关闭上游响应 | `McpProxyServiceTest.buildProxyResponseShouldCloseUpstreamResponseWhenRoutingEndsBeforeStreaming`：校验 `addEndHandler` 回调可关闭上游响应体。 | 已实现自动化 |
| 2 | 流式输出完成后关闭响应 | `McpProxyServiceTest.buildProxyResponseShouldCloseUpstreamResponseAfterStreaming`：校验 `StreamingOutput` 写出完成后关闭上游响应体。 | 已实现自动化 |
| 3 | 清理注册失败时强制关闭 | `McpProxyServiceExceptionPathTest.buildProxyResponseShouldCloseUpstreamResponseWhenCleanupRegistrationFails`：校验异常路径下仍会关闭上游响应体。 | 已实现自动化 |

### 6.3 托管进程生命周期与配置

#### 6.3.1 测试要点

- 启动阶段是否在缺少二进制时不中断 IDMP 主启动。
- 就绪等待被中断时，子进程是否被销毁并清理内部状态。
- `tda.log-dir` 是否被传递给子进程，并由子进程在该目录下产生日志文件。
- 安装目录中的二进制是否能被识别。
- `base-url` 是否会根据 HTTPS 与 `quarkus.http.insecure-requests` 组合按规则推导。

#### 6.3.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 缺失二进制不阻塞启动 | `McpProxyProcessServiceTest.startManagedProcessSafelyShouldNotBlockStartupWhenBinaryMissing`：校验安全启动路径只记录失败，不阻塞应用启动。 | 已实现自动化 |
| 2 | 从安装目录解析二进制 | `McpProxyProcessServiceTest.resolveBinaryPathShouldUseDetectedBinaryWhenConfigUnset`：校验 `${tda.install-dir}/tools` 下的二进制可被识别。 | 已实现自动化 |
| 3 | 就绪等待中断时清理子进程 | `McpProxyProcessServiceCleanupTest.startManagedProcessShouldDestroyChildWhenInterruptedDuringReadinessWait`：校验线程中断后子进程被销毁、内部 `process` 字段清空。 | 已实现自动化 |
| 4 | 日志目录传递给托管进程 | `McpProxyProcessServiceLogPathTest.startManagedProcessShouldWriteLogsIntoConfiguredLogDir`：校验托管进程收到 `--log_path` 后，会在配置的日志目录中生成日志文件；同时不再依赖固定文件 `mcp-tdengine-idmp.log`。 | 已实现自动化 |
| 5 | 显式 base-url 优先生效 | `McpProxyProcessSupportTest.resolveBaseUrlShouldPreferExplicitOverride`：校验显式配置优先于自动推导。 | 已实现自动化 |
| 6 | 同时开启 HTTPS/HTTP 时优先 HTTP | `McpProxyProcessServiceTest.resolveManagedBaseUrlShouldPreferHttpWhenInsecureRequestsEnabled`：校验启用 HTTPS 且 `quarkus.http.insecure-requests=enabled` 时回填 `http://127.0.0.1:{http-port}`。 | 已实现自动化 |
| 7 | 仅 HTTPS 对外时回填 HTTPS | `McpProxyProcessServiceTest.resolveManagedBaseUrlShouldUseHttpsWhenInsecureRequestsAreNotEnabled`：校验启用 HTTPS 且 insecure requests 不为 `enabled` 时回填 `https://127.0.0.1:{ssl-port}`。 | 已实现自动化 |
| 8 | 托管模式拒绝 HTTPS upstream | `McpProxyProcessSupportTest.parseManagedUpstreamShouldRejectHttpsSchemeForManagedProcess`：校验托管模式对不受支持配置及时失败。 | 已实现自动化 |

### 6.4 二进制解析与平台兼容

#### 6.4.1 测试要点

- 发布产物命名是否按平台优先级选择。
- 启动命令是否在不同平台上追加 `--log_path`。
- Windows 脚本包装命令是否自动加上 `cmd.exe /c`。
- Linux/macOS/Windows 的 fallback 名称是否符合预期。
- path 归一化与监听地址转换是否正确。

#### 6.4.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 固定 remote 模式命令 | `McpProxyProcessSupportTest.buildCommandShouldUseFixedRemoteMode`：校验托管子进程启动命令固定带 `--mode remote`。 | 已实现自动化 |
| 2 | 二进制命令追加 log_path | `McpProxyProcessSupportLogPathTest.buildCommandShouldAppendLogPathForBinaryExecutables`：校验原生可执行文件启动命令会追加 `--log_path`。 | 已实现自动化 |
| 3 | Windows 脚本自动包裹命令壳并追加 log_path | `McpProxyProcessSupportLogPathTest.buildCommandShouldAppendLogPathForWindowsCommandScripts`：校验 `.cmd/.bat` 脚本走 `cmd.exe /c`，并带上 `--log_path`。 | 已实现自动化 |
| 4 | Windows 脚本自动包裹命令壳 | `McpProxyProcessSupportCommandTest.buildCommandShouldWrapWindowsCommandScripts`：校验现有命令包装逻辑仍保持兼容。 | 已实现自动化 |
| 5 | endpoint path 归一化 | `McpProxyProcessSupportTest.parseManagedUpstreamShouldNormalizeEndpointPath`：校验 `/mcp/` 归一化为 `/mcp`，并把 `0.0.0.0` 的 readiness host 转成 `127.0.0.1`。 | 已实现自动化 |
| 6 | 显式路径优先 | `McpProxyBinaryUtilTest.resolveManagedBinaryShouldPreferExplicitOverride`：校验显式 `binary-path` 覆盖自动查找。 | 已实现自动化 |
| 7 | 安装目录下优先找发布产物 | `McpProxyBinaryUtilReleaseNameTest.resolveManagedBinaryShouldPreferPublishedLinuxReleaseNameUnderInstallDir`：校验 Linux 发布名优先级高于旧名称。 | 已实现自动化 |
| 8 | 多平台命名矩阵 | `McpProxyBinaryUtilTest.candidateBinaryNamesShouldIncludePlatformSpecificReleases` 与 `McpProxyBinaryUtilReleaseNameTest.candidateBinaryNamesShouldPreferPublishedReleaseNames`：校验 Linux/macOS/Windows 的候选命名。 | 已实现自动化 |
| 9 | Windows arm64 不伪造不存在产物 | `McpProxyBinaryUtilReleaseNameTest.candidateBinaryNamesShouldNotInventUnsupportedWindowsArm64ReleaseName`：校验不生成不存在的发布名。 | 已实现自动化 |

## 7. 易用性测试（可选）

本 PR 不涉及前端 UI、视觉排版或交互布局调整，因此未单独设计易用性测试用例。若后续在前端增加 MCP 面板或按钮，可补充浏览器端联调和多语言展示用例。

## 8. 长期稳定性测试（可选）

当前 PR 未包含针对长时间流式 HTTP 长连接、频繁会话创建/删除或进程反复拉起的 soak test。若后续 MCP 调用量显著上升，建议补充 24 小时长连接稳定性和异常重连测试。

## 9. 性能测试

当前 PR 未包含专门的性能压测。建议后续补充：

1. 单实例下高并发 `/api/v1/mcp/stream` 请求压测。
2. 长时间流式 HTTP 连接数量与资源占用观测。
3. 托管模式与纯代理模式的额外延迟对比。

## 10. 安全测试

本次自动化覆盖的安全相关点包括：

1. `Authorization` 头原样透传，确保已登录调用方可复用现有凭据。
2. `X-Forwarded-For` 的继承与回退逻辑，保证来源信息可追踪。
3. 托管模式拒绝 HTTPS upstream，避免不受支持的本地监听配置混入运行时。

未覆盖的安全项包括渗透测试、恶意长连接耗尽、超大请求体和异常 header 组合攻击。

## 11. 兼容性测试

本次自动化已覆盖以下兼容性点：

1. Linux、macOS、Windows 的发布产物命名和 fallback 规则。
2. `0.0.0.0` 与 `127.0.0.1` 的 readiness host 转换。
3. 显式配置、自动推导、HTTPS/HTTP 组合分支和关闭托管模式的配置分支。

尚未在自动化中覆盖的兼容性项：

1. 安装包升级后携带旧版配置文件的迁移验证。
2. Docker 镜像运行时缺失预编译二进制的端到端回归。
3. 反向代理、TLS 终止和非默认端口组合场景的完整系统级联调。

## 12. 已知问题和限制（可选）

- 当前 TS 主要整理后端自动化测试，不包含浏览器 UI 或 API 文档页面的手工联调记录。
- 当前 TS 不包含性能和 soak test 的执行数据。
- 日志目录验证使用 `FakeMcpProxyMain` 模拟托管进程，未直接约束正式 `mcp-tdengine-idmp` 二进制的具体日志命名策略。
- 托管模式仅支持 HTTP upstream，若要验证 HTTPS upstream，需要走非托管纯代理路径并补充单独测试。
