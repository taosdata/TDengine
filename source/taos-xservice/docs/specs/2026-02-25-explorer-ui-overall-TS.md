# Explorer UI Overall - 功能测试报告（Test Spec）

# 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-02-25 | 2026-02-25 | 1.0 | Oz | 新增 Explorer UI overall TS：基于 `docs/dev/EXPLORER_FS.md`、`explorer/src` 实现与 `explorer/tests` 可执行 Playwright 用例，整理自动化覆盖与手工测试点 |

# 测试目标

本测试报告覆盖 taosX 仓库中 `taos-explorer`（Explorer UI）的整体功能测试目标：

- 验证 Explorer UI 在 **docker/integrated 真环境** 下可用（真实后端、真实任务创建与启动）。
- 覆盖核心业务路径：
  - 登录与鉴权跳转
  - 左侧菜单路由/可见性
  - SQL Explorer 执行查询与导出确认
  - DataIn（数据写入）真实创建 TMQ 任务、连通性检查、列表状态与启动
  - Management（管理）入口与关键弹窗
- 明确自动化与手工测试边界：对 FS 中涉及多数据源（Kafka/MQTT/OPC/PI/CSV/关系库等）的大量组合配置，当前仅对 **TMQ（TDengine 3.x 数据订阅）** 形成可执行自动化覆盖，其余以手工测试点/待补充自动化形式记录。

# 参考文档

- 功能规格（FS）：`docs/dev/EXPLORER_FS.md`
- 路由与权限控制：
  - `explorer/src/router/index.ts`
  - `explorer/src/permission.ts`
  - 侧边栏菜单显示逻辑：`explorer/src/layout/components/Sider/index.vue`
- DataIn 表单配置（TMQ）：`explorer/taos-ui/components/dataIn/config/en/01-tmq.ts`
- 可执行自动化测试（Playwright）：
  - 配置：`explorer/playwright.config.ts`
  - 全局登录与 storageState：`explorer/tests/global.setup.ts`
  - 测试夹具（强制英文）：`explorer/tests/_utils/test.ts`
  - 用例：
    - `explorer/tests/login.spec.ts`
    - `explorer/tests/explorer.spec.ts`
    - `explorer/tests/task-creation.spec.ts`
    - `explorer/tests/tmq-task.spec.ts`
    - `explorer/tests/management-menu.spec.ts`

# 测试结论

- 已实现并可执行的自动化覆盖（Playwright）聚焦于：Login / Explorer SQL / DataIn TMQ / Management。
- 自动化用例在 **英文界面**（`localStorage.local_language = 'en'`）条件下，依赖的关键表单 selector（尤其是 TMQ 的 `groups_after` 下 group UUID）与 UI 文案更稳定。
- 其余模块（Topic/Stream/Tools/Programming/Profile/IDMP 等）已整理手工测试点与后续自动化建议，当前未纳入自动化 PASS 结论。

# 测试环境

- OS: Linux（开发机/CI 环境均可；本文以 Linux 为主）
- Browser: Chromium（Playwright `Desktop Chrome`）
- Explorer UI BaseURL: 默认 `http://localhost:6060`（可通过 `PLAYWRIGHT_BASE_URL` 覆盖，见下文）
- 后端环境：docker/integrated 真环境（需保证 Explorer UI 与 taosX/TDengine 后端服务已启动并可访问）
- 默认测试账号：`root / taosdata`

## 自动化执行方式（Playwright）

- 配置入口：`explorer/playwright.config.ts`
  - `use.baseURL = process.env.PLAYWRIGHT_BASE_URL || 'http://localhost:6060'`
  - `globalSetup = './tests/global.setup.ts'` 生成 `tests/.auth/root.json`
  - `workers: 1`，避免真实任务创建/启动时并发引入不稳定

推荐命令：

```bash
pnpm -C explorer exec playwright test
```

常用环境变量：

```bash
# 指定 Explorer UI 地址
PLAYWRIGHT_BASE_URL=http://localhost:6060 \
  pnpm -C explorer exec playwright test

# 已生成 tests/.auth/root.json 时，可跳过 globalSetup
PLAYWRIGHT_SKIP_GLOBAL_SETUP=true \
  pnpm -C explorer exec playwright test
```

## 关键测试约束（必须）

1. **强制英文（强依赖）**
   - 全局 setup 与 test fixture 均注入：`localStorage.local_language = 'en'`：
     - `explorer/tests/global.setup.ts`
     - `explorer/tests/_utils/test.ts`
   - 原因：DataIn 的 TMQ 表单 config 中，英文与中文的 `groups_after` group UUID 不同：
     - EN: `explorer/taos-ui/components/dataIn/config/en/01-tmq.ts` 中 `field: 'd5209d3d-4964-437b-8762-f76a279adbc6'`
     - ZH: `explorer/taos-ui/components/dataIn/config/zh/01-tmq.ts` 中 `field: '1257130d-bd33-4400-b2b6-3f4f69b700dc'`
   - 若不强制英文，自动化用例中针对 `client.id` 的 selector 将失效。

2. **真实创建并启动任务（强依赖）**
   - E2E 用例会通过 Explorer SQL 创建数据库与 topic，再进入 DataIn 创建 TMQ task 并启动。

# 功能测试

## 1. 登录与鉴权（Login / Permission Guard）

### 测试要点

- 未登录访问时，路由 guard 会跳转登录页（`explorer/src/permission.ts`）。
- 登录页 UI 元素与基础校验。
- 使用 root 账号登录后，应跳转到 `/explorer` 并加载数据库树（`.dbs-tree-header`）。

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1.1 | 登录页渲染 | 打开 `/login`，检查 `.login-content`、`.demo-dynamic`、`button.signin`、`.language` 可见 | PASS（自动化：`explorer/tests/login.spec.ts`） |
| 1.2 | 必填校验 | 点击 Sign in，不输入用户名/密码，出现 `.el-form-item__error` | PASS（自动化：`explorer/tests/login.spec.ts`） |
| 1.3 | root 登录成功 | 输入 `root/taosdata`，点击 Sign in，跳转到 `/explorer`，`.dbs-tree-header` 可见 | PASS（自动化：`explorer/tests/login.spec.ts`；公共 helper：`explorer/tests/_utils/auth.ts`） |
| 1.4 | 未登录访问保护路由 | 未登录直接访问 `/explorer`、`/dataIn/Task` 等，应重定向 `/login` | MANUAL（建议后续补充自动化；参考：`explorer/src/permission.ts`） |

## 2. 整体布局与菜单（Layout / Sider）

### 测试要点

- 菜单项是否按产品形态/License/用户身份正确显示（`explorer/src/layout/components/Sider/index.vue`）。
- Management 菜单只在 `localStorage.username == 'root'` 时显示。
- 路由重定向行为：
  - `/dataIn` => `/dataIn/Task`
  - `/management` => `/management/user`

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 2.1 | /management 重定向 | 打开 `/management` 自动跳转 `/management/user` | PASS（自动化：`explorer/tests/management-menu.spec.ts`；路由：`explorer/src/router/index.ts`） |
| 2.2 | Management 菜单可见性（root） | root 登录后，侧边栏出现 Management 菜单 | MANUAL（可补自动化：断言菜单文本/图标；逻辑：`Sider/index.vue`） |
| 2.3 | /dataIn 重定向 | 打开 `/dataIn` 自动跳转 `/dataIn/Task` | MANUAL（建议补充；路由：`explorer/src/router/index.ts`） |

## 3. 数据浏览器（Explorer / SQL）

### 测试要点

- SQL 编辑器可输入并执行 SQL（CodeMirror 6）。
- 执行结果表格渲染。
- Export 按钮弹出确认框并可确认关闭。

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 3.1 | 执行 SQL 并返回结果 | 在 `/explorer` 执行 `select server_version();`，结果表头可见（`.gird .el-table__header-wrapper th`） | PASS（自动化：`explorer/tests/explorer.spec.ts`；helper：`explorer/tests/_utils/explorerSql.ts`） |
| 3.2 | Export 确认框 | 点击 Export，`.el-message-box` 出现；点击确认后隐藏 | PASS（自动化：`explorer/tests/explorer.spec.ts`） |
| 3.3 | SQL 错误提示 | 执行非法 SQL，页面应出现错误提示/通知 | MANUAL（建议补充自动化：断言 message/notification） |

## 4. 数据写入（DataIn）

### 测试要点

基于 `docs/dev/EXPLORER_FS.md`：

- DataIn 任务列表信息展示：任务名/类型/目标 DB/创建时间/代理/指标入口/状态。
- 任务状态流转：`created/queued/running/ticked/interrupted/waiting/resumed/stopping/suspending/...`。
- 任务操作：启动/停止/刷新/查看修改/删除/复制。
- 新建任务页面：基本信息（name/type/targetDB/agent）与数据源特定配置。
- TMQ（TDengine 3.x）数据源：Topic DSN、订阅组/客户端 ID、起始位置、timeout 等。

### 自动化：TMQ 真实 E2E（创建 + 启动）

该部分对“真实创建并启动任务”提供可执行验证：

- 先在 Explorer SQL 中创建源/目标 DB 与 topic（`CREATE TOPIC ... AS DATABASE ...`）。
- 进入 `/dataIn/Task` -> Add Source（或直接 `/dataIn/add`）。
- 填写并提交 TMQ task。
- 在任务列表中找到该 task，若存在 Start 则点击并确认；若已自动启动则识别 Stop。
- 断言状态包含 `Queued|Started|Running`。

关键 selector（来自 DataIn 实现）：

- 基本信息：`#name`、`#type`、`#targetDB`（`explorer/taos-ui/components/dataIn/views/sourceConfig.vue`）
- TMQ Topic DSN：`#data\.connection_options\.endpoint`（由 `formItem.vue` 生成 `:id="parent + field"`）
- TMQ Client ID（英文配置）：`#data\.groups_after\.d5209d3d-4964-437b-8762-f76a279adbc6\.client\.id`（EN TMQ config）

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 4.1 | TMQ 连通性检查 | 在 TMQ 配置页点击 Check Connectivity，结果包含 reachable | PASS（自动化：`explorer/tests/tmq-task.spec.ts`） |
| 4.2 | TMQ 创建任务并启动 | 通过 UI 提交 TMQ task，并在列表中启动（或识别已启动），状态进入 Queued/Started/Running | PASS（自动化：`explorer/tests/task-creation.spec.ts`；helpers：`explorer/tests/_utils/datain.ts`） |
| 4.3 | DataIn 列表展示字段 | `/dataIn/Task` 列表显示关键字段（任务名/类型/DB/状态/操作） | MANUAL（建议补充：断言 `.tasks-table` 列头与某行内容） |
| 4.4 | 任务停止（Stop） | 对运行中任务执行 Stop，确认框与状态变化 | MANUAL（可补自动化；操作入口逻辑与 Start 类似） |
| 4.5 | 查看/修改配置 | 从任务列表进入 View/Edit（readonly/edit 模式切换）并校验按钮组文案 | MANUAL（自动化 helper 已预留 `viewTaskReadonlyFromRow`，但当前未在 spec 中默认执行） |
| 4.6 | 删除/复制 | Stop 状态可删除；复制生成新任务草稿 | MANUAL（需避免污染环境，建议使用唯一名称并做清理） |

### 手工测试建议：覆盖 FS 中其他数据源类型

以下数据源在 FS 中有详细字段/组合（MySQL/PostgreSQL/Kafka/MQTT/OPC-UA/OPC-DA/PI/CSV/AVEVA Historian/TDengine 2.x 等），建议按“表单渲染/必填校验/连通性检查/任务运行/失败提示/资源清理”统一方法补充测试：

- 基础：进入 `/dataIn/add`，切换 `#type`，检查对应配置块渲染与必填校验。
- 连通性：若支持 check connectivity，检查成功/失败文案与超时处理。
- 运行：创建并启动任务，观察状态流转与 metrics 入口。

## 5. Management（管理）

### 测试要点

- `/management` 重定向到 `/management/user`。
- User 页面可打开 Add User 对话框。
- 其他 tab（backup/replication/cluster/license/audit/slowSql）是否按 License/构建产物可见。

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 5.1 | /management 跳转 | 打开 `/management` 跳转 `/management/user`，并能看到表格 | PASS（自动化：`explorer/tests/management-menu.spec.ts`） |
| 5.2 | Add User 弹窗 | 在 `/management/user` 点击 Add，`.el-dialog` 出现 | PASS（自动化：`explorer/tests/management-menu.spec.ts`） |
| 5.3 | Backup tab 路由 | 若存在 Backup tab，点击后 URL 变为 `/management/backup` | PASS/COND（自动化：`explorer/tests/management-menu.spec.ts`，tab 不存在则 skip） |

## 6. Topic（数据订阅 / Topic 管理）

### 测试要点

基于 `explorer/src/router/index.ts` 与实现：

- Topic 列表显示 topic_name/db/sql/dsn/create_time。
- Create Topic：Wizard/SQL 两种模式。
- 行操作：Sample code（/topic/example）、Share Topic（/topic/share）、Delete。
- Sample code 页面：Go/Rust/Python/Java tab 切换与文档展示。

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 6.1 | Topic 列表渲染 | 打开 `/topic`，列表可见，Refresh/Create Topic 按钮可用 | MANUAL（参考实现：`explorer/src/views/6_topic/views/topic.vue`） |
| 6.2 | Create Topic（Wizard） | 通过 Wizard 创建 topic，成功后列表出现 | MANUAL（参考：`explorer/src/views/6_topic/components/addTopic.vue`） |
| 6.3 | Copy DSN | 点击 Copy 按钮，将 DSN 复制到剪贴板 | MANUAL |
| 6.4 | Sample code / Share Topic 路由 | 行操作进入 `/topic/example`、`/topic/share`，topic selector 可用 | MANUAL（参考：`explorer/src/views/6_topic/views/example.vue`、`shareTopic.vue`） |
| 6.5 | Delete Topic | 删除 topic 前确认框；删除后刷新列表 | MANUAL |

## 7. Stream

### 测试要点

- `/stream` 页面可进入并展示 stream 列表。
- Create Stream（SQL editor），校验 SQL 合法性，创建成功提示。

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 7.1 | Create Stream | 打开 Create Stream 弹窗，输入合法 `CREATE STREAM ...` 并创建成功 | MANUAL（参考：`explorer/src/views/5_stream/components/addStream.vue`） |

## 8. Programming / Tools 文档页

### 测试要点

- `/programming` 展示 connector 文档列表，点击条目进入 `/docs/connector/:lang`。
- `/tools` 展示 tool 文档列表，点击条目进入 `/docs/tool/:lang`。

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 8.1 | Programming 列表 | 进入 `/programming`，文档列表渲染 | MANUAL（参考：`explorer/src/views/4_programming/views/main.vue`） |
| 8.2 | Tools 列表 | 进入 `/tools`，文档列表渲染 | MANUAL（参考：`explorer/src/views/7_tools/views/main.vue`） |

# 易用性测试（可选）

建议检查项（与自动化强制英文策略对齐）：

- 语言切换后，菜单与关键按钮文案是否正确刷新。
- DataIn 表单切换 type 时，描述信息（右侧文档）与字段校验是否一致。
- 弹窗（Create Topic / Add User / Export confirm）是否存在遮挡、焦点不正确、关闭不响应等问题。

# 长期稳定性测试（可选）

- 长时间运行的 DataIn 任务（例如 TMQ 持续订阅）在 UI 中状态刷新是否正常。
- Explorer SQL 连续执行多条查询，UI 是否存在内存增长、卡顿。

# 性能测试

- Explorer SQL 执行大结果集查询时，渲染与导出提示是否可接受。
- DataIn 任务列表在大量任务（例如 1000+）时的加载/分页性能。

# 安全测试

- 权限：非 root 用户是否无法看到 Management 菜单（`localStorage.username != 'root'`）。
- 会话：无 session/过期 session 访问受保护页面是否正确跳转 `/login`。
- 敏感信息：页面是否避免明文显示密码/DSN 中敏感字段（如有脱敏需求需另行定义）。

# 兼容性测试

- 升级安装后，旧版本创建的 DataIn 任务是否能继续展示与运行（特别关注任务配置 schema 变化）。
- 可选：回退/降级场景（仅在支持的发布策略下执行）。

# 已知问题和限制（可选）

- 当前自动化对 DataIn TMQ 的关键 selector 依赖英文配置的 group UUID（见“关键测试约束”）；若该 UUID 在配置文件中变更，需要同步更新测试 selector 或引入更稳健的定位策略。
- 其他数据源（Kafka/MQTT/OPC/PI/关系库等）由于依赖外部服务与复杂参数组合，当前仅记录手工测试点，未纳入自动化回归。