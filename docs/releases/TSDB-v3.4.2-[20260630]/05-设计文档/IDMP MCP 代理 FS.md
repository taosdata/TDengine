# IDMP MCP 代理 FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-04-09 | 2026-04-09 | 0.1 | 谭雪峰 | 初稿 |

## 2. 背景

IDMP 需要通过统一的后端入口暴露 MCP（Model Context Protocol）能力，避免前端或外部调用方直接连接独立的 MCP 服务，同时复用现有 IDMP 的访问入口、鉴权上下文和部署方式。为 `tda-server` 新增了 `/api/v1/mcp/stream` 代理入口，以及随 IDMP 主进程一起启动的托管 MCP 子进程能力。

本特性的目标是：

1. 为 MCP 提供稳定的 IDMP 内部代理入口。
2. 让调用方在已接入 IDMP 的场景下复用现有 `Authorization` 信息，不再单独维护一套 MCP 账号口令。
3. 支持通过预编译的 `mcp-tdengine-idmp` 可执行文件在安装包或 Docker 镜像中直接启用 MCP。

## 3. 定义

| 术语 | 定义 |
| --- | --- |
| MCP | Model Context Protocol，本特性中指 `mcp-tdengine-idmp` 提供的远程代理能力。 |
| MCP 代理入口 | IDMP 暴露的 `/api/v1/mcp/stream` REST 入口，用于转发 GET/POST/DELETE 请求。 |
| 上游 MCP 地址 | `tda.mcp-proxy.upstream-url` 指向的目标地址，默认值为 `http://127.0.0.1:6037/mcp`。 |
| 托管 MCP 子进程 | IDMP 启动时可自动拉起的 `mcp-tdengine-idmp` 进程，固定使用 `remote` 模式。 |
| 流式 HTTP | Streamable HTTP。MCP 对外暴露的是流式 HTTP 交互能力，IDMP 负责代理并透传相应的流式响应。 |

## 4. 行为说明

### 4.1 新增代理入口

IDMP 新增如下代理入口：

- 路径：`/api/v1/mcp/stream`
- 完整访问路径：`http://{host}:{port}/api/v1/mcp/stream`
- 方法：`GET`、`POST`、`DELETE`

代理行为如下：

1. 目标地址固定取自 `tda.mcp-proxy.upstream-url`，请求原始 query string 原样拼接到上游地址。
2. `POST` 请求转发请求体；`GET`/`DELETE` 不附带请求体。
3. 上游返回的状态码、响应体和大多数响应头原样透传；`connection`、`transfer-encoding`、`content-length` 等 hop-by-hop 头会被过滤。
4. 当上游返回流式 HTTP 响应时，IDMP 以流式方式向客户端回传，支持长连接场景；相关内容类型和协议头按上游结果透传。

### 4.2 请求头和上下文透传

代理请求会尽量透传调用方上下文，并补充必要的代理信息。

| 请求头 | 行为 |
| --- | --- |
| `Authorization` | 原样透传给上游。调用方若已持有 IDMP 访问令牌，可直接复用该头。 |
| `Accept-Language` | 原样透传。 |
| `Mcp-Session-Id` | 原样透传，用于 MCP 会话连续性。 |
| `Last-Event-ID` | 原样透传，用于流式 HTTP 场景下的事件续传。 |
| `X-Forwarded-For` | 若调用方已提供则沿用；否则优先取 `X-Real-IP`，再回退为请求远端地址。 |
| `X-Original-URI` | 由 IDMP 自动补充，记录原始请求路径和 query string，例如 `/api/v1/mcp/stream?resume=true`。 |

以下请求头不会转发到上游：`connection`、`host`、`keep-alive`、`proxy-authenticate`、`proxy-authorization`、`te`、`trailer`、`transfer-encoding`、`upgrade`、`content-length`。

### 4.3 托管 MCP 子进程

当 `tda.mcp-proxy.process.enabled=true` 时，IDMP 会在启动阶段自动尝试拉起本地 MCP 子进程：

1. 二进制查找顺序：
   1. `tda.mcp-proxy.process.binary-path`
   2. `${tda.install-dir}/tools`
   3. `${tda.install-dir}/bin`
   4. `/app/bin`（非 Windows）
   5. `PATH`
2. 子进程固定追加参数：`--mode remote --log_path {tda.log-dir}`。其中 `tda.log-dir` 未显式配置时默认取 `./logs`，IDMP 会在启动前确保该目录存在。
3. IDMP 会向子进程注入以下环境变量：
   - `IDMP_LISTEN`：监听地址，来自 `tda.mcp-proxy.upstream-url` 的 host:port。
   - `IDMP_ENDPOINT_PATH`：监听路径，来自 `tda.mcp-proxy.upstream-url` 的 path，自动规整为无重复斜杠、无尾斜杠格式。
   - `IDMP_BASE_URL`：供子进程回填给客户端的外部访问地址。
4. 子进程日志文件由 `mcp-tdengine-idmp` 自己在 `--log_path` 指定目录下创建和写入，IDMP 不再把子进程 stdout/stderr 直接重定向到固定日志文件。
5. 若二进制不存在、不可执行、启动超时或进程提前退出，IDMP 主进程继续启动，但 MCP 远程入口不可用，调用时通常会表现为 502。

`IDMP_BASE_URL` 的求值逻辑如下：

1. 若配置了 `tda.mcp-proxy.process.base-url`，直接使用该值。
2. 否则当 IDMP 启用了 HTTPS，且 `quarkus.http.insecure-requests` 不为 `enabled` 时，自动回填为 `https://127.0.0.1:{ssl-port}`。
3. 否则回填为 `http://127.0.0.1:{http-port}`。这也包括“同时开启 HTTPS，但仍允许 HTTP 入口”的场景。

### 4.4 配置项

```yaml
tda:
  mcp-proxy:
    upstream-url: http://127.0.0.1:6037/mcp
    process:
      enabled: true
      binary-path:
      base-url:
      start-timeout-seconds: 30
```

| 配置键 | 环境变量 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `tda.mcp-proxy.upstream-url` | `TDA_MCP_PROXY_UPSTREAM_URL` | `http://127.0.0.1:6037/mcp` | 代理上游地址。托管模式下同时用于确定子进程监听地址和路径。 |
| `tda.mcp-proxy.process.enabled` | `TDA_MCP_PROXY_PROCESS_ENABLED` | `true` | 是否启用托管 MCP 子进程。关闭后仅保留纯代理行为。 |
| `tda.mcp-proxy.process.binary-path` | `TDA_MCP_PROXY_PROCESS_BINARY_PATH` | 空 | 显式指定二进制路径，优先级最高。 |
| `tda.mcp-proxy.process.base-url` | `TDA_MCP_PROXY_PROCESS_BASE_URL` | 空 | 覆盖自动推导的外部访问地址。适合 HTTPS、反向代理或非默认端口场景。 |
| `tda.mcp-proxy.process.start-timeout-seconds` | `TDA_MCP_PROXY_PROCESS_START_TIMEOUT_SECONDS` | `30` | 子进程就绪等待时间，单位秒。实现上最小按 1 秒处理。 |

### 4.5 安装、Docker 与发版行为

本特性引入了新的运行时要求：

1. Docker 打包流程不再在镜像构建阶段从源码编译 `mcp-tdengine-idmp`。
2. 若希望容器或安装包启动后即可使用 MCP，需要在运行时提供预编译可执行文件。
3. 推荐放置位置：
   - `/app/bin`
   - `${tda.install-dir}/tools`
   - `${tda.install-dir}/bin`
   - `PATH`

如需显式覆盖对外访问地址，可通过 `TDA_MCP_PROXY_PROCESS_BASE_URL` 配置；如不需要托管模式，可将 `TDA_MCP_PROXY_PROCESS_ENABLED` 设为 `false`。

### 4.6 出错处理

| 场景 | 对调用方的表现 | 说明 |
| --- | --- | --- |
| 上游正常返回业务状态 | 原样透传状态码和响应体，例如 200、405 | 代理不改写上游业务语义。 |
| 上游不可达、网络异常、托管进程未成功启动 | `502 Bad Gateway`，错误消息前缀为 `MCP proxy request failed:` | 调用方应检查 `upstream-url`、子进程状态、端口占用和网络连通性。 |
| `tda.mcp-proxy.upstream-url` 非法 | 服务端返回统一异常，错误消息包含 `Invalid MCP proxy upstream url` | 需要修正配置并重启服务。 |
| 二进制不存在或不可执行 | 启动日志报错，MCP 入口不可用 | 托管模式仅在日志中报错，不阻塞 IDMP 主进程启动。 |
| 子进程启动超时或提前退出 | 启动日志报错，并提示查看 `${tda.log-dir}` 目录下的 MCP 日志文件 | 可适当增大启动超时，或检查 `base-url`、监听地址和子进程输出日志。 |

## 5. 性能

本特性本身不引入额外的业务计算，仅增加一跳本地或远端 HTTP 代理和一层流式转发。对性能的影响主要体现在：

1. 每个 MCP 请求新增一次 OkHttp 代理转发。
2. 流式 HTTP 长连接会在 IDMP 侧保持一个长寿命转发连接。
3. 托管模式下额外常驻一个本地 MCP 子进程。

整体性能开销预计远低于实际 MCP 工具执行时间；若后续出现高并发流式连接场景，应单独评估连接数、线程占用和上游处理能力。

## 6. 安全

1. 调用方的 `Authorization` 头会原样透传，上游可复用现有 IDMP 鉴权上下文。
2. hop-by-hop 头不会被转发，避免把连接级元数据错误带入上游。
3. `X-Forwarded-For` 和 `X-Original-URI` 会被标准化补充，便于上游做审计和访问来源判断。
4. 托管模式要求本地监听地址使用 HTTP，而不是 HTTPS，避免把本地进程绑定逻辑和外部 TLS 终止混用。
5. 本特性不会自动生成新的 MCP 账号或密码，减少额外凭据管理成本。

## 7. 兼容性

1. 对未启用 MCP 的现有部署，属于增量能力，不影响原有 REST API。
2. 对使用外部 MCP 上游的部署，可通过关闭托管模式，仅使用 `/api/v1/mcp/stream` 作为纯代理。
3. 对依赖“Docker 构建阶段自动编译 MCP 可执行文件”的旧流程，这是一次发版行为变化：现在需要在运行时提供预编译二进制文件。

## 8. 运维

1. 托管 MCP 子进程日志目录默认是 `${tda.log-dir:-./logs}`；实际日志文件由 `mcp-tdengine-idmp` 在该目录下自行创建。
2. 当启用 HTTPS、反向代理或非默认端口时，建议显式配置 `TDA_MCP_PROXY_PROCESS_BASE_URL`。
3. 若 MCP 非核心能力或部署中已有独立上游，可将 `TDA_MCP_PROXY_PROCESS_ENABLED=false` 作为安全默认值。
4. 发生启动失败时，先检查：
   - 二进制是否存在且可执行
   - `${tda.log-dir}` 目录下是否生成了子进程日志文件
   - `upstream-url` 是否可绑定
   - 6037 等监听端口是否被占用
   - `base-url` 是否与实际访问入口一致

## 9. 使用场景

1. **IDMP 内嵌 MCP**：安装包或 Docker 镜像内已提供 `mcp-tdengine-idmp`，IDMP 启动后直接暴露 `/api/v1/mcp/stream`。
2. **独立 MCP 服务前置代理**：关闭托管模式，仅让 IDMP 作为统一入口转发到外部或本机已有 MCP 服务。
3. **流式会话恢复**：客户端通过 `Last-Event-ID` 与 `Mcp-Session-Id` 对接上游的流式 HTTP 会话恢复能力。
4. **多层网络转发**：部署在反向代理或 HTTPS 网关后，通过 `base-url` 显式指定对外回调地址。

## 10. 约束和限制

约束：

1. 托管模式要求 `tda.mcp-proxy.upstream-url` 使用 HTTP。
2. 托管二进制必须存在且在当前平台上可执行。
3. 调用方应自行携带需要透传的 `Authorization`、`Mcp-Session-Id` 等请求头。

限制：

1. 代理入口当前仅支持 `GET`、`POST`、`DELETE`。
2. 上游 path 固定取自 `tda.mcp-proxy.upstream-url`，不会根据请求自动做 path 重写。
3. 当前变更未包含浏览器 UI 或 taos Explorer 侧的专用交互界面。
4. Docker 镜像不再自动构建 MCP 可执行文件，必须提前准备预编译产物。

## 11. 常见错误和排查

| 错误现象 | 可能原因 | 排查方式 |
| --- | --- | --- |
| 调用 `/api/v1/mcp/stream` 返回 502 | 上游未启动、托管子进程启动失败、端口不通 | 检查 `tda.mcp-proxy.upstream-url`、`${tda.log-dir}` 下的 MCP 子进程日志、端口监听情况。 |
| 启动日志提示 `Managed MCP proxy binary is not executable` | 二进制权限不足或路径错误 | 修复文件权限，或通过 `TDA_MCP_PROXY_PROCESS_BINARY_PATH` 指定正确路径。 |
| 启动日志提示 `Timed out waiting for managed MCP proxy process readiness` | 子进程未在超时时间内监听端口 | 检查端口占用、`${tda.log-dir}` 下的子进程日志，必要时提高 `start-timeout-seconds`。 |
| 客户端拿到的回调地址不正确 | 自动推导的 `base-url` 与实际外部地址不一致 | 显式配置 `TDA_MCP_PROXY_PROCESS_BASE_URL`，或核对 HTTPS / `quarkus.http.insecure-requests` 组合。 |
| 在 HTTPS 场景下回调地址仍显示为 HTTP | `quarkus.http.insecure-requests=enabled`，系统仍保留 HTTP 入口 | 属于预期行为；如需强制回调为 HTTPS，请显式配置 `TDA_MCP_PROXY_PROCESS_BASE_URL`，或关闭 insecure requests。 |
| 启动阶段报 `Invalid MCP proxy upstream url` | `upstream-url` 格式非法 | 修正为合法 URL 后重启。 |

## 12. 可观测性

1. OpenAPI/Swagger 中会新增 `McpResource`，可见 `GET`、`POST`、`DELETE /mcp/stream`。
2. IDMP 主进程日志中会记录托管子进程的启动、停止、退出码以及启动失败原因；子进程自己的文件日志位于 `${tda.log-dir}`。
3. 上游若返回 `Mcp-Session-Id` 等响应头，IDMP 会透传给客户端，便于链路观察。
4. 本特性不直接修改 taos shell、taos Explorer 或 TDinsight 的功能界面。

## 13. 安装和卸载

安装要求：

1. 在运行时提供预编译的 `mcp-tdengine-idmp` 可执行文件，或配置可访问的外部上游地址。
2. 按需设置 `TDA_MCP_PROXY_UPSTREAM_URL`、`TDA_MCP_PROXY_PROCESS_BINARY_PATH`、`TDA_MCP_PROXY_PROCESS_BASE_URL`。
3. 启动 IDMP 后，通过 `/api/v1/mcp/stream` 验证代理链路。

卸载或停用要求：

1. 若仅停用托管子进程，设置 `TDA_MCP_PROXY_PROCESS_ENABLED=false` 并重启。
2. 若完全停用 MCP，移除相关环境变量和预编译二进制文件。
3. 若镜像或安装包不再携带 MCP，可保持该特性处于关闭状态，不影响其他 IDMP 功能。

## 14. 文档

1. 仓库内运行文档需要更新 `README.md` 与 `cicd/docker-compose/README.md`，说明 MCP 二进制的提供方式与 `base-url` 覆盖方式。
2. 企业版文档建议补充：
   - `/api/v1/mcp/stream` 的接入说明
   - 托管模式与纯代理模式的差异
   - 常见错误与日志排查
3. 官网文档若计划对外发布 MCP 代理能力，需在发版前补充部署与认证复用说明。

## 15. 参考文档



## 16. 附录

### 16.1 关键实现组件

| 组件 | 作用 |
| --- | --- |
| `McpResource` | 暴露 `/mcp/stream` 的 GET/POST/DELETE 入口。 |
| `McpProxyService` | 负责请求头透传、query 拼接、流式响应转发和异常转换。 |
| `McpProxyProcessService` | 负责托管 MCP 子进程的启动、健康等待、停止，以及日志目录准备与就绪失败清理。 |
| `McpProxyProcessSupport` | 负责 `base-url` 推导、监听地址解析，以及带 `--log_path` 的命令生成。 |
| `McpProxyBinaryUtil` | 负责按平台和安装目录查找二进制文件。 |

### 16.2 平台二进制命名约定

| 平台 | 优先发布名 |
| --- | --- |
| Linux x64 | `mcp-tdengine-idmp-linux-x64` |
| Linux arm64 | `mcp-tdengine-idmp-linux-arm64` |
| macOS x64 | `mcp-tdengine-idmp-macos-x64` |
| macOS arm64 | `mcp-tdengine-idmp-macos-arm64` |
| Windows x64 | `mcp-tdengine-idmp-windows-x64.exe` |
