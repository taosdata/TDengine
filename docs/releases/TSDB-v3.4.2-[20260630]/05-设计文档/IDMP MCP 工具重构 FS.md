# IDMP MCP 工具重构 FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- |-----|--------|
| 2026-04-08 | 2026-04-08 | 1.0 | 谭雪峰 | 初稿。    |

## 2. 背景

1. 把原来偏“宽而泛”的查询工具拆成显式的领域工具。
2. 在读工具之外新增 MCP Resource、MCP Prompt 和结构化写工具。
3. 让远程模式上下文、资源缓存和日志落盘成为正式运行时行为。

## 3. 定义

- **显式领域工具**：一个 Tool 只负责一个明确的数据域或动作，例如 `get_event`、`get_attribute_history`、`list_analyses`。
- **结构化写工具**：直接以受约束字段落库的 MCP Tool，例如 `create_analysis`、`create_panel`、`create_attribute`。
- **AI 生成写工具**：先调用 IDMP AI 生成草稿，再立即持久化的 Tool，例如 `add_analysis`、`add_panel`。
- **Resource**：通过 `resources/read` 暴露的静态或准静态数据集，例如层级、模板、算法列表。
- **Prompt**：通过 `prompts/get` 暴露的操作模板，用于指导 Agent 编排多步 Tool 调用。
- **远程 Token 模式**：`mode=remote` 时，服务不自行登录，而是逐请求复用调用方 Bearer Token。

## 4. 行为说明

### 4.1 对外能力面

服务对外注册的能力如下：

| 类型 | 数量 |
| --- | ---: |
| 读 Tool | 38 |
| 写 Tool | 12 |
| Resource | 4 |
| Prompt | 7 |

能力特点：

- 服务通过 `CreateServerWithConfig()` 统一注册读 Tool、写 Tool、Resource 和 Prompt。
- 只要实例启动成功，写工具就会出现在工具列表中，是否真正执行成功再由上游账号或 Token 权限决定。
- Tool 面按搜索、详情、历史、写入和流程模板等职责拆分，便于客户端按步骤编排调用。

### 4.2 读工具重排

#### 4.2.1 显式领域查询

服务将读能力拆分为以下显式工具组合：

- 元素整体态势：`get_element_context`
- 多元素详情：`get_elements_by_ids`
- 单事件详情：`get_event`
- 事件注释：`get_event_annotations`
- 未确认告警总数：`get_event_count_unacknowledged`
- 通知历史：`get_notification_history`
- 当前属性值：`get_attribute_value`
- 属性历史：`get_attribute_history`
- 分析任务列表与详情：`list_analyses`、`get_analysis`
- 面板列表与详情：`list_panels`、`get_panel`
- 元素注释与通知规则：`list_element_annotations`、`list_contact_points`

- 客户端按对象域发起调用，不依赖单一聚合入口。
- 每个 Tool 的参数和返回语义更稳定，但需要客户端自行编排多步调用。
- `tool/prompt.go` 中的 Prompt 围绕这一组显式 Tool 组织工作流。

#### 4.2.2 搜索与层级行为

服务提供 6 个 typed search Tool：

- `search_elements`
- `search_events`
- `search_panels`
- `search_dashboards`
- `search_attributes`
- `search_analyses`

使用特点如下：

1. 每个搜索工具只负责一个对象域，避免混合元素和事件结果。
2. `search_elements` 允许按 `template_id`、`parent_id` 限定范围，并可按 `attribute_ids` 增补当前属性值。
3. 推荐将搜索、详情、历史读取拆分为多步调用。

层级读取采用列表型语义：

- `list_element_children` 只接受**单个** `parent_id`，语义是“列出某父节点的直接子元素分页”。
- 如果需要从根节点做全局导航，推荐使用：
  - `idmp://hierarchy` Resource 获取全量层级平铺视图；
  - `list_elements` 做分页列举；
  - `search_elements` 做关键词查找。

#### 4.2.3 模板、路径和分页语义

系统配置、分类、属性、变更、分页与分支统计能力由以下工具提供：

| 主题 | Tool |
| --- | --- |
| 系统配置 | `get_system_config` |
| 分类 | `list_categories` |
| 元素属性定义 | `list_element_attributes` |
| 元素变更 | `list_element_changes` |
| 元素分页 | `list_elements` |
| 事件分页 | `list_events` |
| 分支计数 | `count_branch_elements` |

其中有几项工具具备明确的参数和语义约束：

1. `list_elements`
   - 新增 `keyword` 过滤。
   - 不支持 `top_element_id` 和 `limit_size` 参数。
   - 更像“常规分页列举工具”，不再承担搜索补偿行为。

2. `list_events`
   - 新增 `severity` 过滤。
   - `status` 和 `severity` 会在 Tool 层做枚举归一化，再映射到上游。
   - 当前对外定义的状态值是 `Unack` / `Ack`，严重级别是 `Default`、`Information`、`Warning`、`Minor`、`Major`、`Critical`。

3. `get_attributes_by_path`
   - 入参是单个字符串 `paths`，内容是逗号分隔的 `element_path|attribute_name`。
   - 行为是“按属性全路径拿当前属性值”。
   - 该接口不用于按元素聚合返回属性定义列表。

4. 模板读取
   - `list_element_templates` 作为 Tool 暴露元素模板能力。
   - 事件模板改为通过 Resource `idmp://event-templates` 提供。
   - 元素模板同时可通过 `list_element_templates` Tool 和 `idmp://element-templates` Resource 获取。

#### 4.2.4 面板读取行为

`get_panel` 具备以下语义：

- 只接受显式的 `element_id` + `panel_id`。
- 工具不只返回 panel 定义，而是：
   1. 先读取 panel detail；
   2. 再调用 `/api/v1/elements/{id}/panels/query` 执行查询；
   3. 返回 `panel_detail` 和 `query_data`。
- `start_time` / `end_time` 会覆盖 panel `params.fromText` / `params.toText`。
- 适合“拿一块面板的配置和当前数据”。
- 不支持仅凭 `panel_name` 或 `element_template_id` 自动定位。

### 4.3 新增 Resource 与 Prompt

#### 4.3.1 Resource

服务提供 4 个 MCP Resource：

| URI | 行为 |
| --- | --- |
| `idmp://hierarchy` | 返回全量元素层级平铺列表，并补齐模板名，便于 Agent 做名称定位。 |
| `idmp://element-templates` | 返回元素模板和标准属性定义。 |
| `idmp://event-templates` | 返回事件模板元数据。 |
| `idmp://analysis-algorithms` | 返回分析触发类型/算法候选，供分析创建时参考。 |

行为特点：

- Resource 读取结果带 TTL 内存缓存。
- 默认 TTL 为 300 秒，可通过 `cache_ttl_seconds` / `IDMP_CACHE_TTL_SECONDS` 调整。
- `analysis-algorithms` 会优先尝试从有模板信息的元素读取 trigger types，必要时回退到模板级 trigger types；没有可用目标时返回空数组。

#### 4.3.2 Prompt

服务提供 7 个 MCP Prompt：

- `shift_handover`
- `equipment_health_check`
- `root_cause_analysis`
- `batch_review`
- `fleet_comparison`
- `maintenance_due`
- `alarm_triage`

这些 Prompt 不直接修改上游数据，它们的作用是把显式 Tool 编排成可复用的工作流说明。

### 4.4 AI 交互行为

#### 4.4.1 `ask_idmp`

`ask_idmp` 是服务提供的 AI 交互 Tool：

- 必填入参：`question`
- 可选入参：`session_id`、`language`、`element_id`

行为特点：

1. 使用 `question` 作为提问文本。
2. 支持 `session_id`，允许客户端续接已有 AI 会话。
3. 支持 `language`，会透传为 `Accept-Language`。
4. `element_id` 当前只是保留字段，不会被转发到上游 `/ai/chat/stream`。
5. 适用于分析、诊断和推理场景。

#### 4.4.2 新增 AI 辅助能力

服务还提供两类 AI 辅助能力：

1. **推荐类读工具**
   - `recommend_panels`
   - `recommend_analyses`

2. **生成并落库类写工具**
   - `add_panel`
   - `add_analysis`

其中 `add_analysis` 的行为比简单“生成后创建”更强：

- 先调用 AI 生成分析草稿；
- 持久化失败时，如果错误体现为“缺少 TDengine metric/tag 属性”，会自动收紧 Prompt 再试一次；
- 失败过程中生成的临时输出属性会在重试或失败退出前清理，避免把半成品属性留在元素上。

### 4.5 写工具行为

当前 MCP Server 对外实际暴露 12 个写 Tool：

| 类别 | Tool | 主要行为 |
| --- | --- | --- |
| 事件 | `acknowledge_event` | 确认单个告警事件。 |
| 事件 | `add_event_annotation` | 给事件追加注释。 |
| 面板 | `delete_panel` | 删除元素下的 panel，要求 `confirm=true`。 |
| 属性 | `create_attribute` | 结构化创建属性。 |
| 分析 | `create_analysis` | 结构化创建分析任务。 |
| 分析 | `create_alarm_rule` | 结构化创建事件触发型告警分析。 |
| 面板 | `create_panel` | 结构化创建 panel。 |
| 分析 | `add_analysis` | 自然语言生成并持久化分析任务。 |
| 分析 | `manage_analysis` | `PAUSE` / `RESUME` / `DELETE` 分析任务。 |
| 面板 | `add_panel` | 自然语言生成并持久化 panel。 |
| 元素注释 | `create_element_annotation` | 给元素增加注释。 |
| 通知规则 | `update_contact_point` | 更新元素级事件通知规则。 |

#### 4.5.1 结构化写的约束

1. `create_attribute`
   - 默认 `reuse_if_exists=true`。
   - 如果同名属性已存在且 `value_type` 一致，则直接复用并返回 `reused=true`。
   - 如果同名但类型不一致，则返回 `RESOURCE_CONFLICT`。

2. `create_analysis`
   - 走结构化 `AnalysisReqDTO` 风格负载。
   - `trigger_type` 必填，并做枚举归一化。
   - 非 `Event` 分析要求 `output.attributes` 至少一项。
   - 可选 `event`、`event_template_id`、`severity` 等事件分析字段。

3. `create_alarm_rule`
   - 是 `Event` 分析的受限封装。
   - 当提供 `child_template_id` 时，不尝试持久化一个“层级聚合 event 分析”，而是展开成“每个命中后代元素一条自元素告警规则”。
   - 扩展模式下显式拒绝 `child_filter`、`child_level`。
   - 中途失败会对已经创建的分析做回滚；返回结果里会区分 `created` 和 `skipped`。

4. `create_panel`
   - `panel_type=text` 时必须提供 `text_content`。
   - `enable_advanced=true` 时必须提供 `advanced_queries`。
   - 对非 text、且非 `advanced` 的 panel，至少要有 `xa_attributes` 或 `ya_attributes`。
   - `params` 默认带 `fromText=now-12h`、`toText=now`，顶层 `from_text` / `to_text` 会覆盖默认值，且不允许传空字符串。

5. `update_contact_point`
   - 只支持更新**已有**元素级 notify rule。
   - 如果目标元素没有 notify rule，直接返回 `UNSUPPORTED_WORKFLOW`；当前不提供“隐式创建”路径。
   - 至少要提供一个待更新字段。

#### 4.5.2 写工具暴露边界

本次代码差异里，`api/` 层已经新增了分类、元素、仪表盘等多种写接口封装，但当前 `system/server.go` 和 `tool/write.go` **没有**把这些能力注册为 MCP Tool。

因此，当前对外行为仍应以本节列出的 12 个写 Tool 为准，不能把 `api/categories_write.go`、`api/elements_write.go`、`api/dashboards_write.go` 等内部 client 能力视为已经对 Agent 暴露。

#### 4.5.3 写错误返回

当前写工具统一使用 JSON error payload 作为 Tool error 文本，已显式出现的结构化错误码包括：

- `INVALID_ARGUMENT`
- `RESOURCE_CONFLICT`
- `UNSUPPORTED_WORKFLOW`

对上游 HTTP 失败，当前实现多数场景仍直接透传原始错误文本，不做统一业务码映射。

### 4.6 远程模式、认证和运维配置变化

#### 4.6.1 认证与上下文透传

服务支持两种启动模式：

- `mode=local`：服务自己登录 IDMP
- `mode=remote`：逐请求复用调用方 Bearer Token

远程模式和认证处理具备以下行为：

1. 远程 HTTP 请求头中的 `Accept-Language` 会写入上下文，并继续透传到上游 API。
2. `ask_idmp.language` 也会走同样的透传路径。
3. `api/client.go` 现在把 HTTP `432` 也视作认证失败状态，和 `401`、`403` 一起处理。
4. `tool/retry.go` 明确禁止在 Token 模式下走重新登录重试；如果远程模式仍收到“需要重连”的信号，会返回 `unexpected reconnect request in token auth mode`。

- 可以携带语言偏好；
- 仍然不负责自动续签或重登录；
- 写工具与读工具共用同一套 Token 权限边界。

#### 4.6.2 新增运行时配置

服务支持以下正式配置项：

| 参数 | 环境变量 | 默认值 | 行为 |
| --- | --- | --- | --- |
| `cache_ttl_seconds` | `IDMP_CACHE_TTL_SECONDS` | `300` | 控制 Resource 缓存 TTL。 |
| `log_path` | `IDMP_LOG_PATH` | Linux/macOS：`/var/log/taos`；Windows：`C:\TDengine\log` | 按平台默认写入对应目录；设为空字符串时关闭文件日志。 |
| `log_rotation_count` | `IDMP_LOG_ROTATION_COUNT` | `3` | 保留的历史日志文件数。 |
| `log_rotation_size` | `IDMP_LOG_ROTATION_SIZE` | `1GB` | 单日志文件滚动阈值。 |
| `log_keep_days` | `IDMP_LOG_KEEP_DAYS` | `3` | 日志保留天数。 |
| `log_compress` | `IDMP_LOG_COMPRESS` | `false` | 是否压缩滚动后的日志。 |
| `log_reserved_disk_size` | `IDMP_LOG_RESERVED_DISK_SIZE` | `1GB` | 日志目录最低保留磁盘空间。 |

对应行为：

- 启动时会调用 `logging.ConfigureFileOutput(...)`。
- 文件日志和 stderr 会同时输出，不是替代关系。
- 启动日志新增 `cache_ttl_seconds` 和整组文件日志配置字段，便于运维确认实例行为。

## 5. 性能

本次改动不是数据库性能优化类需求，但会带来以下行为层面的性能影响：

1. Resource 引入内存 TTL 缓存后，重复读取层级、模板和算法列表时，上游请求数下降。
2. `get_element_context`、`get_panel`、若干 Prompt 都会触发多次上游 API 调用，其单次调用成本高于简单单接口读取。
3. AI 生成类写工具可能经历“生成草稿 + 持久化 + 重试/清理”的多步流程，时延高于纯结构化写。
4. 服务启动时需要注册更多 Tool/Resource/Prompt，但这部分成本只发生在进程启动阶段。

因此，本次行为变化的主要目标是能力重构和调用编排清晰化，而不是吞吐或时延优化。

## 6. 安全

本次重构最重要的安全结论是：

- 一旦实例启动，写工具会出现在工具列表中。
- 真正的写权限边界取决于上游 IDMP 账号权限或远程 Bearer Token 权限。

其他安全行为如下：

- 删除类能力当前对外只暴露 `delete_panel`，且要求 `confirm=true`。
- 大多数写操作都要求显式资源 ID，不支持模糊删除或模糊修改。
- `update_contact_point` 不提供“无规则时自动创建”的捷径。
- 远程模式不缓存额外高权限凭据，也不做 Token 模式下的自动重登录。
- 启动日志与运行日志继续对用户名、密码、Token 等敏感字段做脱敏处理。

## 7. 客户端接入要求

客户端需要按当前 Tool 名称、参数模型和返回语义接入服务。

### 7.1 关键 Tool 接入要求

| Tool / 能力 | 接入要求 |
| --- | --- |
| `ask_idmp` | 使用 `question` 作为必填入参；可按需传 `session_id`、`language`、`element_id`。 |
| `get_system_config` | 用于读取系统配置。 |
| `list_element_children` | 需要单个 `parent_id`，返回直接子元素分页。 |
| `list_elements` | 支持 `keyword` 过滤；不支持 `top_element_id` 和 `limit_size`。 |
| `list_events` | 支持 `status`、`severity` 枚举归一化。 |
| 模板能力 | 通过 `list_element_templates`、`idmp://element-templates`、`idmp://event-templates` 组合使用。 |
| `get_panel` | 需要显式 `element_id` + `panel_id`，并返回 `panel_detail` 与 `query_data`。 |
| `count_branch_elements` | 用于分支计数。 |

### 7.2 参数要求

`get_attributes_by_path` 的入参为 `paths` 字符串，格式为 `element_path|attribute_name`，多个目标以逗号分隔；返回结果为当前属性值。

## 8. 运维

建议按以下方式管理服务实例：

1. 如果实例对外开放远程 MCP 接口，优先依赖上游最小权限 Token 管理访问边界。
2. 如果需要区分不同 Agent 的能力范围，应通过账号权限、网关策略或单独构建分支来实现。
3. 对 `log_path`、磁盘保留空间和日志滚动参数做显式配置，避免长时间运行实例把日志写满磁盘。
4. 对 Resource 缓存 TTL 做环境区分：静态环境可适当增大，结构频繁变化环境应缩短。

## 9. 使用场景

服务更适合以下使用场景：

1. Agent 先读取 `idmp://hierarchy` 和模板 Resource，再做元素/事件定位。
2. Agent 使用 typed search Tool 找对象，再用显式详情 Tool 深挖上下文。
3. Agent 通过 Prompt 生成交接、健康评估、批次复盘和告警分诊报告。
4. Agent 基于自然语言生成 panel 或 analysis，并立即落库。
5. Agent 在已知资源 ID 的前提下执行受约束的写操作，例如确认告警、补注释、创建结构化分析。

不适合的场景：

1. 需要根层导航和名称变体匹配的场景。
2. 需要 panel 名称解析或 template panel 自动匹配的场景。
3. 期望仅通过服务参数收窄当前实例的对外能力面。
4. 把 `api/` 包里已新增但未注册的写接口，误当作当前 MCP 已对外承诺的行为。

## 10. 约束和限制

约束：

- 当前 Tool 面必须显式区分读、写、Resource 和 Prompt。
- 大多数写操作必须有明确资源 ID。
- 远程模式依赖调用方 Token 权限，不负责重新登录。

限制：

- 当前 MCP 只暴露 12 个写 Tool，未暴露分类、元素、仪表盘等更多内部 API 写能力。
- `list_element_children` 只提供基于 `parent_id` 的直接子元素分页，不提供根层导航和模糊筛选。
- `get_panel` 不支持仅凭 `panel_name` 或 `element_template_id` 自动定位。
- `update_contact_point` 只能更新已有 notify rule，不能隐式创建。

## 11. 常见错误和排查

| 问题 | 可能原因 | 排查方式 |
| --- | --- | --- |
| 调用 Tool 失败且提示不存在或未注册 | 使用了未暴露的 Tool 名称 | 以附录中的 Tool、Resource 和 Prompt 清单为准核对调用名称 |
| `get_attributes_by_path` 返回参数错误或空结果 | `paths` 格式不符合 `路径|属性,路径|属性` | 改为 `paths=\"路径|属性,路径|属性\"` |
| `list_element_children` 不能从根节点开始浏览 | 当前工具强制要求 `parent_id` | 改用 `idmp://hierarchy`、`list_elements` 或 `search_elements` |
| `get_panel` 报找不到 panel | 只提供了 `panel_id` 或不知道 owner element | 先用 `list_panels` 确认 panel 所属 `element_id` |
| `create_panel` 返回 `INVALID_ARGUMENT` | `text_content` / `advanced_queries` / 轴属性 / 时间参数不满足约束 | 按 panel 类型补齐必填字段 |
| `update_contact_point` 返回 `UNSUPPORTED_WORKFLOW` | 目标元素没有现成 notify rule | 先在上游产品侧创建规则，再回到 MCP 更新 |
| 远程模式持续认证失败 | Bearer Token 失效或权限不足；`401/403/432` 都会被视为认证失败 | 更换有效 Token，并确认上游权限范围 |

## 12. 可观测性

服务具备以下可观测性能力：

- 启动日志会输出 `cache_ttl_seconds` 与整组文件日志参数。
- 每次 Tool 调用都会记录 `tool`、`elapsed_ms`、`item_count`。
- 写 Tool 还会额外记录 `operation`、`resource_type`、`resource_id`。
- 如配置 `log_path`，同一条日志会同时进入 stderr 和滚动文件。

## 13. 安装和卸载

本次改动不要求新增独立服务组件，仍是同一个二进制进程。

安装/启用要点：

- 可按 local 或 remote 模式启动。
- 如果需要 Resource 缓存调优，增加 `cache_ttl_seconds`。
- 默认文件日志目录为 Linux/macOS 的 `log_path=/var/log/taos` 或 Windows 的 `log_path=C:\TDengine\log`；如需覆盖或关闭，再显式配置 `log_path` 及相关滚动参数。

回退要点：

- 清空 `log_path` 并重启，可回退到仅 stderr 输出。
- 调整 `cache_ttl_seconds` 并重启，可改变 Resource 缓存行为。
- 如需调整对外 Tool 面，需要通过代码版本或构建产物管理，而不是只改配置。

## 14. 文档

需要修改文档

## 15. 参考文档


## 16. 附录

### 16.1 当前对外注册面清单

#### 16.1.1 读 Tool（38）

- `ask_idmp`
- `get_element_context`
- `list_events`
- `get_panel`
- `search_elements`
- `search_events`
- `search_panels`
- `search_dashboards`
- `search_attributes`
- `search_analyses`
- `get_event`
- `get_event_count_unacknowledged`
- `get_event_annotations`
- `get_notification_history`
- `get_attribute_value`
- `get_attribute_history`
- `list_analyses`
- `get_analysis`
- `list_panels`
- `list_element_annotations`
- `list_contact_points`
- `recommend_panels`
- `recommend_analyses`
- `get_system_config`
- `list_categories`
- `list_element_templates`
- `get_element_by_path`
- `get_attributes_by_path`
- `list_element_attributes`
- `get_element_fullpath`
- `list_element_changes`
- `list_element_children`
- `list_elements`
- `get_batch_attribute_data`
- `list_element_templates_in_scope`
- `get_panel_dashboard_counts`
- `count_branch_elements`
- `get_elements_by_ids`

#### 16.1.2 写 Tool（12）

- `acknowledge_event`
- `add_event_annotation`
- `delete_panel`
- `create_attribute`
- `create_analysis`
- `create_alarm_rule`
- `create_panel`
- `add_analysis`
- `manage_analysis`
- `add_panel`
- `create_element_annotation`
- `update_contact_point`

#### 16.1.3 Resource（4）

- `idmp://hierarchy`
- `idmp://element-templates`
- `idmp://event-templates`
- `idmp://analysis-algorithms`

#### 16.1.4 Prompt（7）

- `shift_handover`
- `equipment_health_check`
- `root_cause_analysis`
- `batch_review`
- `fleet_comparison`
- `maintenance_due`
- `alarm_triage`
