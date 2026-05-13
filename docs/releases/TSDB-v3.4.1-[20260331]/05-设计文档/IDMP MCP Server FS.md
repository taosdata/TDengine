# IDMP MCP Server FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-02-24 | 2026-02-24 | 0.1 | 谭雪峰 | 编写文档 |
| 2026-02-25 | 2026-02-25 | 0.2 | 谭雪峰 | 更新tool输入输出样例 |

## 2. 背景

目标是让 MCP 客户端通过 stdio 调用 IDMP 能力。
- 需求背景：需要一个可被 MCP 客户端直接调用的 IDMP 接入层，减少手工对接 HTTP API 的工作量。
- 目标：
  - 提供统一 MCP 工具：`chat`、`system_config`
  - 支持命令行参数与环境变量配置
  - 在鉴权失效时自动重登录并重试一次

## 3. 定义

- `chat` 工具：MCP Tool，调用 `/api/v1/ai/chat/stream` 返回文本结果
- `system_config` 工具：MCP Tool，调用 `/api/v1/system/config` 返回系统配置 JSON

## 4. 行为说明

### 4.1 启动与配置行为

| 参数 | 环境变量 | 含义 |
| --- | --- | --- |
| `--base_url` | `IDMP_BASE_URL` | IDMP 服务地址 |
| `--user` | `IDMP_USER` | IDMP 登录用户名 |
| `--pass` | `IDMP_PASS` | IDMP 登录密码 |

- 配置优先级：命令行参数优先于环境变量；空白环境变量会被忽略。
- 启动时会检查是否启用图形验证码，如果启用则退出
- 启动后会立即尝试登录，登录失败则退出

### 4.2 API 行为

内部调用 IDMP HTTP API：
- `GET /api/v1/users/login-config`
- `POST /api/v1/users/login`
- `POST /api/v1/ai/chat/stream`
- `GET /api/v1/system/config`
MCP 对外工具：
- `chat(prompt: string)`：IDMP AI 对话功能
示例：
```sql {wrap}
2026-02-26T09:33:39.370+08:00 [info] [mcp.config.usrlocalmcp.tdengine-idmp] MCPServerManager#callTool (chat): {"prompt":"朝阳区有多少电表？"}
2026-02-26T09:33:45.755+08:00 [info] [mcp.config.usrlocalmcp.tdengine-idmp] MCPServerManager#callTool (chat) result: {"content":[{"type":"text","text":"[\"\\u671d\\u9633\\u533a\\u5171\\u670910\\u4e2a\\u7535\\u8868\\u3002\"]\n"}]}
```

- `system_config()`：获取 IDMP 系统信息
示例：
```sql {wrap}
2026-02-26T09:34:46.121+08:00 [info] [mcp.config.usrlocalmcp.tdengine-idmp] MCPServerManager#callTool (system_config): {}
2026-02-26T09:34:46.141+08:00 [info] [mcp.config.usrlocalmcp.tdengine-idmp] MCPServerManager#callTool (system_config) result: {"content":[{"type":"text","text":"{\"productTitle\":\"TDengine IDMP\",\"dataVersion\":\"1.0.12\",\"enableVersionControl\":false,\"version\":\"1.0.12.2\",\"gitCommitId\":\"\",\"collectEnabled\":false,\"language\":\"zh-CN\",\"crashEnabled\":true,\"elementsAutoRefresh\":false,\"elementsAutoRefreshInterval\":5}"}]}
```

## 5. 性能

无。主要是 MCP 工具封装、鉴权与错误处理逻辑

## 6. 安全

- 机密性：账号密码来自启动参数/环境变量；Token 仅保存在进程内存中，不落盘。
- 完整性：调用链路依赖 IDMP 服务端鉴权与返回内容；本项目不改写服务端数据。
- 可用性：在 4xx 情况下自动重登录并重试一次，降低短时会话失效影响。
- 风险与建议：
  - 建议生产使用 HTTPS `base_url`
  - 避免在命令历史中明文传递 `--pass`，优先使用环境变量注入

## 7. 兼容性

无。

## 8. 运维

- 部署方式：单二进制进程，通过 stdio 与 MCP 客户端通信。
- 运行依赖：可访问 IDMP 服务地址；账号需可登录且验证码关闭。

## 9. 使用场景

- Use Case：AI 问答查询
  - 前置条件：已配置并启动服务，登录成功
  - 操作步骤：调用 `chat` 并传入 `prompt`
  - 期望结果：返回 AI 文本

- Use Case：读取平台配置
  - 前置条件：已配置并启动服务，登录成功
  - 操作步骤：调用 `system_config`
  - 期望结果：返回系统配置 JSON

## 10. 约束和限制

- 必须提供可访问的 IDMP `base_url`。
- 账号必须可登录，且目标环境需关闭验证码。
- MCP 客户端需支持 stdio 模式。

## 11. 常见错误和排查

- 错误提示：LoginConfig captcha enable
  - 可能原因：IDMP 启用了验证码
  - 排查方式：在目标 IDMP 关闭验证码登录策略

- 错误提示：request for ... failed with status code
  - 可能原因：鉴权失效或权限问题
  - 排查方式：检查账号权限、token 时效

- 错误提示：failed after re-login
  - 可能原因：重登录后仍未恢复会话
  - 排查方式：检查账号状态、登录接口可用性

- 错误提示：required argument "prompt" not found
  - 可能原因：调用 chat 未传 prompt
  - 排查方式：按工具定义补齐参数

## 12. 可观测性

可观测性现状：主要是进程标准日志

## 13. 安装和卸载

- 安装方式：下载 release 二进制或 `go build` 生成二进制。
- 要求：将可执行文件放入 `$PATH` 或 MCP 配置中使用绝对路径。
- MCP 配置需传入 `--base_url`、`--user`、`--pass` 或对应环境变量。

## 14. 文档

需要修改文档

## 15. 参考文档

## 16. 附录
