# IDMP MCP 工具重构 TS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- |-----|--------|
| 2026-04-08 | 2026-04-08 | 1.0 | 谭雪峰 | 初稿     |

## 2. 测试目标

- 验证 MCP 对外能力面的行为正确性，覆盖显式读 Tool、Resource、Prompt 和写 Tool。
- 验证关键行为变化，包括 typed search、`get_panel`、`list_events` 枚举归一化、`ask_idmp` 会话与语言参数、Resource 缓存和 Prompt 参数约束。
- 验证结构化写与 AI 辅助写的参数校验、重试、清理与回滚行为。
- 验证本地模式、远程模式、Remote MCP 协议层暴露面以及远程 Bearer Token 透传。
- 验证运行时配置默认值与覆盖逻辑，重点包括 `listen_addr=:6037`、按平台区分的 `log_path` 默认值以及空字符串关闭文件日志的行为。

## 3. 参考文档

- 功能规格：[IDMP MCP 工具重构 FS](../05-设计文档/IDMP%20MCP%20工具重构%20FS.md)
- 配置与启动测试：`config/config_test.go`、`config/config_extra_test.go`、`system/server_test.go`
- 工具单元测试：`tool/new_tools_test.go`、`tool/panel_detail_test.go`、`tool/events_page_test.go`、`tool/tool_test.go`
- Resource / Prompt / 恢复工具测试：`tool/resource_test.go`、`tool/prompt_test.go`、`tool/restored_tools_test.go`
- In-process / 真实 IDMP 集成测试：`tool/core_tools_integration_test.go`、`tool/restored_tools_integration_test.go`、`tool/events_write_integration_test.go`、`tool/panels_write_integration_test.go`、`tool/retry_integration_test.go`
- Remote MCP 集成测试：`tool/remote_mcp_integration_test.go`
- API 集成测试：`api/integration_test.go`、`api/analysis_write_integration_test.go`、`api/attributes_write_integration_test.go`、`api/events_write_integration_test.go`、`api/panels_write_integration_test.go`
- AI 与日志相关测试：`api/ai_test.go`、`logging/logger_test.go`

## 4. 测试结论

**结论：当前仓库已经具备覆盖本次 MCP 工具重构核心行为的自动化测试。**

1. `go test ./...` 已通过。
2. 单元测试已覆盖参数校验、默认值、请求映射、Prompt 文案、Resource 缓存、结构化写约束、AI 写工具清理/重试/回滚和日志脱敏。
3. In-process / 真实 IDMP 集成测试已覆盖核心读工具、恢复工具、事件/面板安全写链路以及 token / 用户密码两类认证路径。
4. Remote MCP 集成测试已覆盖 `tools/list`、`resources/list`、`prompts/list`、远程 Tool 调用和多步场景链路。

## 5. 测试环境

- 本地单元 / 进程内 MCP：
  - OS：Linux
  - 运行方式：`go test`
  - 夹具方式：`httptest`、in-process MCP server/client、mock HTTP Transport
- 真实 IDMP 集成环境：
  - 门控：`IDMP_IT_ENABLE=true`
  - 认证：`IDMP_IT_TOKEN` 或 `IDMP_IT_USER` / `IDMP_IT_PASS`
  - 基础地址：`IDMP_IT_BASE_URL`
- Remote MCP 集成环境：
  - 门控：`IDMP_REMOTE_MCP_ENABLE=true`
  - 认证：`IDMP_REMOTE_MCP_TOKEN`
  - 传输：Streamable HTTP `/mcp`
  - 地址：`IDMP_REMOTE_MCP_URL`，未配置时使用测试文件中的默认地址
- 运行时默认配置验证项：
  - 远程监听地址默认值：`:6037`
  - 日志目录默认值：Linux/macOS 为 `/var/log/taos`，Windows 为 `C:\TDengine\log`
  - `log_path=""` 时回退为仅 stderr 输出

## 6. 功能测试

### 6.1 服务能力面与运行配置

#### 6.1.1 测试要点

- 验证服务会同时注册读 Tool、写 Tool、Resource 和 Prompt。
- 验证本地模式与远程模式启动分支、远程请求上下文和日志配置默认值。
- 验证配置默认值、环境变量覆盖、flag 覆盖以及平台相关默认日志目录。

#### 6.1.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | MCP 能力面注册 | 验证 `CreateServer` / `CreateServerWithConfig` 会注册服务的读 Tool、写 Tool、Resource 和 Prompt，并显式暴露写工具 | 已覆盖（`system/server_test.go`） |
| 2 | 本地/远程启动分支 | 验证 `Start()` 在 local / remote 两种模式下的登录、启动和 fatal 分支行为 | 已覆盖（`system/server_test.go`） |
| 3 | 远程请求上下文 | 验证远程模式下 Bearer Token、QID、客户端 IP 透传与 edge case 行为 | 已覆盖（`system/server_test.go`） |
| 4 | 配置默认值与覆盖 | 验证 `listen_addr=:6037`、`endpoint_path=/mcp`、平台日志目录默认值、`log_path=""` 覆盖关闭文件日志、flag/env 优先级 | 已覆盖（`config/config_test.go`、`config/config_extra_test.go`） |

### 6.2 显式读工具与关键行为

#### 6.2.1 测试要点

- 验证 typed search 的分域行为。
- 验证 `list_events` 枚举归一化、参数校验和结果压缩行为。
- 验证 `get_panel` 从显式 `element_id + panel_id` 读取 detail 和 `query_data` 的行为。
- 验证恢复工具与路径类工具支撑显式领域查询模式。
- 验证 `ask_idmp` 的新参数模型与重试路径。

#### 6.2.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | typed search 六类工具 | 验证 `search_elements`、`search_events`、`search_panels`、`search_dashboards`、`search_attributes`、`search_analyses` 的域隔离、基础查询与 `search_elements` 的 `attribute_ids` 增补 | 已覆盖（单元：`tool/new_tools_test.go`；集成：`tool/core_tools_integration_test.go`） |
| 2 | `list_events` 枚举归一化 | 验证 `UNACK/WARN` 等别名归一化、非法枚举过滤、负值参数校验、空结果与紧凑输出 | 已覆盖（单元：`tool/events_page_test.go`；集成：`tool/core_tools_integration_test.go`） |
| 3 | `get_panel` 查询语义 | 验证 `get_panel` 通过 `element_id + panel_id` 先取 detail 再取 `query_data`，并支持 `start_time/end_time` 覆盖 | 已覆盖（单元：`tool/panel_detail_test.go`；集成：`tool/core_tools_integration_test.go`） |
| 4 | `get_element_context` 与周边读链路 | 验证元素上下文、事件详情、分析列表、面板列表、注释和通知规则的读取链路 | 已覆盖（`tool/new_tools_test.go`、`tool/core_tools_integration_test.go`） |
| 5 | 恢复工具与路径/层级行为 | 验证 `get_system_config`、`list_categories`、`get_element_by_path`、`get_attributes_by_path`、`get_element_fullpath`、`list_element_children`、`list_elements`、`count_branch_elements` 等工具在当前工具集合中的行为 | 已覆盖（单元：`tool/restored_tools_test.go`；集成：`tool/restored_tools_integration_test.go`） |
| 6 | `ask_idmp` 新参数模型 | 验证 `question`、`session_id`、`language`、流式调用、重登录重试与 token 模式调用 | 已覆盖（单元：`tool/tool_test.go`、`api/ai_test.go`；集成：`tool/core_tools_integration_test.go`） |

### 6.3 Resource 与 Prompt

#### 6.3.1 测试要点

- 验证 4 个 Resource 的注册、返回结构、缓存和 fallback 行为。
- 验证 7 个 Prompt 的参数要求、默认值和引导文案。
- 验证 Resource / Prompt 在 Remote MCP 下可直接读取。

#### 6.3.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | `idmp://hierarchy` 层级读取 | 验证递归获取全量元素、补齐缺失 `parentId/templateName`、空结果与模板名回填 | 已覆盖（单元：`tool/resource_test.go`；集成：`tool/restored_tools_integration_test.go`） |
| 2 | `idmp://element-templates` / `idmp://event-templates` | 验证元素模板与事件模板 Resource 的读取行为 | 已覆盖（`tool/resource_test.go`、`tool/restored_tools_integration_test.go`、`tool/remote_mcp_integration_test.go`） |
| 3 | `idmp://analysis-algorithms` | 验证算法 Resource 的 trigger types 读取与模板 fallback 行为 | 已覆盖（单元：`tool/resource_test.go`；集成：`tool/restored_tools_integration_test.go`） |
| 4 | 7 个 Prompt 参数与文案 | 验证 `shift_handover`、`equipment_health_check`、`root_cause_analysis`、`batch_review`、`fleet_comparison`、`maintenance_due`、`alarm_triage` 的必填参数、默认值和引导文本 | 已覆盖（单元：`tool/prompt_test.go`；远程：`tool/remote_mcp_integration_test.go`） |
| 5 | Resource / Prompt 远程可见性 | 验证 `resources/list`、`prompts/list`、资源重复读取一致性以及远程拉取 prompt 内容 | 已覆盖（`tool/remote_mcp_integration_test.go`） |

### 6.4 写工具与 AI 辅助写

#### 6.4.1 测试要点

- 验证结构化写工具的字段约束、复用策略、时间参数处理和成功路径。
- 验证 AI 辅助写的清理、重试、回滚与通知规则更新约束。
- 验证事件/面板安全写工具和底层 API 写链路在真实 IDMP 环境中的行为。

#### 6.4.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | `create_attribute` / `create_analysis` / `create_alarm_rule` / `create_panel` | 验证结构化写的创建成功路径、同名复用、事件分析空输出、层级告警展开、Meta2d advanced panel、时间参数合并与文本 panel 必填项 | 已覆盖（`tool/new_tools_test.go`） |
| 2 | `add_analysis` 清理与重试 | 验证持久化失败时清理临时输出属性，TDengine metric/tag 校验错误时收紧 prompt 后重试 | 已覆盖（`tool/new_tools_test.go`） |
| 3 | `add_panel` / `manage_analysis` / `update_contact_point` / `recommend_*` | 验证 AI 生成 panel、分析管理、通知规则更新、推荐类工具 target 约束和 async 默认行为 | 已覆盖（`tool/new_tools_test.go`） |
| 4 | 事件/面板安全写工具 | 验证 `acknowledge_event`、`add_event_annotation`、`delete_panel`、面板安全写链路等 Tool 在真实环境中的行为 | 已覆盖（`tool/events_write_integration_test.go`、`tool/panels_write_integration_test.go`） |
| 5 | API 写链路 | 验证属性、分析、事件、面板等底层 API 写接口在真实环境中的行为 | 已覆盖（`api/analysis_write_integration_test.go`、`api/attributes_write_integration_test.go`、`api/events_write_integration_test.go`、`api/panels_write_integration_test.go`） |

### 6.5 远程 MCP 协议层与场景链路

#### 6.5.1 测试要点

- 验证 Remote MCP 的协议层可见性。
- 验证远程 Tool、Resource 和 Prompt 的调用路径。
- 验证多步场景链路能够组合使用显式 Tool、Resource 与写工具。

#### 6.5.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 远程 `smoke` 与 metadata | 验证远程服务可 `Ping`、可列出 tools/resources/prompts | 已覆盖（`tool/remote_mcp_integration_test.go`） |
| 2 | 远程 Tool 批量调用 | 覆盖 `ask_idmp`、显式读工具、恢复工具、写工具及参数校验分支的远程调用 | 已覆盖（`tool/remote_mcp_integration_test.go`） |
| 3 | 远程场景链路 | 验证 `scenario_children_then_context`、`scenario_events_then_detail_then_annotations`、`scenario_add_panel_then_get_then_delete` | 已覆盖（`tool/remote_mcp_integration_test.go`） |
| 4 | 认证与重试模式 | 验证 token 鉴权路径、用户密码 retry / relogin 路径和 Remote MCP Bearer Token 使用方式 | 已覆盖（`tool/retry_integration_test.go`、`tool/remote_mcp_integration_test.go`、`api/integration_test.go`） |

## 7. 易用性测试（可选）

无。

## 8. 长期稳定性测试（可选）

无。

## 9. 性能测试

无。

## 10. 安全测试

### 10.1 测试要点

- 验证 token 模式下 AI 流式调用与重登录处理行为。
- 验证远程模式下 Bearer Token、QID 与客户端来源字段注入。
- 验证工具日志中的 prompt / token 类敏感字段脱敏，以及文件日志配置行为。

### 10.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | token 模式 AI 流式调用 | 验证 token 模式下流式调用、401/403/432 处理与重登录路径 | 已覆盖（`api/ai_test.go`、`tool/retry_integration_test.go`） |
| 2 | 远程请求上下文注入 | 验证 Bearer Token、QID、客户端 IP 等字段进入远程请求上下文 | 已覆盖（`system/server_test.go`） |
| 3 | 日志脱敏与文件日志 | 验证 prompt / token 类字段脱敏、日志格式、文件日志写入与滚动行为 | 已覆盖（`tool/tool_test.go`、`logging/logger_test.go`） |

## 11. 兼容性测试

### 11.1 测试要点

- 验证 token 与用户密码两类认证路径均可工作。
- 验证远程监听地址、日志目录默认值、环境变量覆盖和 flag 覆盖行为。
- 验证恢复工具承接系统配置、分类、模板、路径和层级读取能力。

### 11.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 认证路径兼容 | 验证 token 鉴权与用户密码鉴权两类接入方式 | 已覆盖（`api/integration_test.go`、`tool/retry_integration_test.go`） |
| 2 | 配置默认值与覆盖 | 验证 `listen_addr=:6037`、按平台区分的 `log_path` 默认值以及 env / flag 覆盖逻辑 | 已覆盖（`config/config_extra_test.go`） |
| 3 | 恢复工具能力 | 验证系统配置、分类、模板、路径、层级、分页等历史读取能力在当前工具集合中可访问 | 已覆盖（`tool/restored_tools_test.go`、`tool/restored_tools_integration_test.go`） |

## 12. 已知问题和限制（可选）

无。
