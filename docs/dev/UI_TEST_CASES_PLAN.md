# Explorer UI 测试开发计划（基于 TS：explorer-ui-overall）

本文档用于多人协同推进 Explorer UI（taos-explorer）自动化测试与手工用例补齐。

- 关联 TS：`docs/specs/2026-02-25-explorer-ui-overall-TS.md`
- 自动化框架：Playwright（目录：`explorer/tests/`）
- 运行入口：`pnpm -C explorer exec playwright test`

## 0. 协作约定

- 每个任务必须包含：Owner、优先级（P0/P1/P2）、验收标准（DoD）、关联代码路径。
- 任何新增自动化用例必须：
  - 可在 docker/integrated 真环境运行
  - 强制英文（沿用 `localStorage.local_language = 'en'` 的 fixture）
  - 避免写死非稳定 selector（优先 id / role / data-testid；必要时推动 UI 增加稳定锚点）
- 对会污染环境的用例（创建 DB/topic/task/user 等），必须设计清理策略或使用唯一命名空间。

### Status 标记

- TODO：未开始
- DOING：进行中
- BLOCKED：阻塞（必须写明原因）
- REVIEW：待评审
- DONE：已完成

## 1. 里程碑（建议）

- M1（P0 回归闭环）：补齐 Permission / Sider / Explorer SQL 错误处理 / DataIn 核心操作（Stop/View/Edit/Delete/Copy）自动化
- M2（P1 功能扩展）：Topic/Stream/Docs（Programming/Tools）核心路径自动化
- M3（P2 覆盖扩展）：DataIn 多数据源（Kafka/MQTT/OPC/PI/关系库/CSV）按“渲染-校验-连通性-运行”模板扩展

## 2. 基础设施与测试工程（Workstream A）

### A1. 测试运行与报告标准化

- [x] A1.1（P0）统一 CI/本地命令说明与环境变量
  - Owner: @zitsen
  - Status: DONE
  - 相关：`explorer/playwright.config.ts`、`explorer/tests/global.setup.ts`
  - 运行手册（可复制粘贴）：

    1) 基本约定
       - 默认 baseURL：`http://localhost:6060`
       - 默认通过 globalSetup 生成并复用登录态：`explorer/tests/.auth/root.json`
       - 强制英文：globalSetup 会写入 `localStorage.local_language = 'en'`

    2) 环境变量（与代码一致）
       - `PLAYWRIGHT_BASE_URL`：覆盖 baseURL（默认 `http://localhost:6060`）
       - `PLAYWRIGHT_SKIP_GLOBAL_SETUP=true`：若 `tests/.auth/root.json` 已存在，则跳过重新生成（便于本地调试提速）

    3) 运行命令

       ```bash
       # 运行全部用例（推荐入口）
       pnpm -C explorer exec playwright test

       # 指定 baseURL（CI/远端/非默认端口）
       PLAYWRIGHT_BASE_URL=http://127.0.0.1:6060 pnpm -C explorer exec playwright test

       # 本地调试：复用已有登录态，不重复 globalSetup
       PLAYWRIGHT_SKIP_GLOBAL_SETUP=true pnpm -C explorer exec playwright test

       # 仅跑某个 spec
       pnpm -C explorer exec playwright test tests/login.spec.ts

       # UI 模式 / Debug
       pnpm -C explorer exec playwright test --ui
       pnpm -C explorer exec playwright test --debug
       ```

    4) 报告与 Trace
       - HTML report：默认输出到 `explorer/playwright-report/`
       - trace：配置为 `on-first-retry`，失败且发生 retry 时会在 `explorer/test-results/**/trace.zip` 生成

       ```bash
       # 打开 HTML report
       pnpm -C explorer exec playwright show-report

       # 打开 trace（将路径替换为实际 trace.zip）
       pnpm -C explorer exec playwright show-trace test-results/**/trace.zip
       ```

- [x] A1.2（P1）补齐测试 artifacts 归档策略（report/trace）
  - Owner: @zitsen
  - Status: DONE
  - DoD: CI 产物中可下载 HTML report + trace。
  - 建议归档目录（Playwright 默认约定）：
    - `explorer/playwright-report/`（HTML report）
  - 建议 GitHub Actions 配置片段（示例）：

    ```yaml
    - name: Run Explorer Playwright tests
      run: pnpm -C explorer exec playwright test
      env:
        PLAYWRIGHT_BASE_URL: http://127.0.0.1:6060

    - name: Upload Playwright artifacts
      if: always()
      uses: actions/upload-artifact@v4
      with:
        name: explorer-playwright-artifacts
        path: |
          explorer/playwright-report
        retention-days: 7
    ```

### A2. 稳定性与可维护性改进

- [x] A2.1（P0）消除 TMQ 用例对“groups_after UUID”的硬编码依赖（改为更稳健定位）
  - Owner: @zitsen
  - Status: DONE
  - 方案：测试侧改为按 `id` 的稳定前后缀匹配，不再写死 `groups_after.<uuid>`。
    - `input[id^="data.groups_after."][id$=".client.id"]`
  - DoD: `task-creation.spec.ts` / `tmq-task.spec.ts` 不再依赖固定 UUID 字符串。
  - 相关：`explorer/taos-ui/components/dataIn/config/en/01-tmq.ts`、`explorer/taos-ui/components/dataIn/components/formItem.vue`

- [x] A2.2（P1）增加统一的“资源命名与清理”工具
  - Owner: @zitsen
  - Status: DONE
  - DoD: 提供 `explorer/tests/_utils/cleanup.ts`，用于 best-effort 清理：测试 DB / topic / task。
  - 备注：本地调试如需保留现场，可设置 `PLAYWRIGHT_SKIP_CLEANUP=true` 跳过清理。

- [x] A2.3（P1）为 DataIn 列表行操作封装更健壮的菜单打开逻辑
  - Owner: @zitsen
  - Status: DONE
  - DoD: `openRowOperations` 同时兼容 hover/click trigger，并处理 Element Plus fixed-right 列导致的 DOM 拆分。
  - 回归验证：现有 TMQ 用例通过 `startTaskFromRow` 间接覆盖该逻辑。
  - 相关：`explorer/tests/_utils/datain.ts`

---

## 3. 功能自动化补齐（Workstream B：P0/P1）

### B1. Permission / Login / Session

- [x] B1.1（P0）未登录访问保护路由应跳转 `/login`
  - Owner: @huolinhe
  - Status: DONE
  - DoD: 新增 Playwright 用例：清空 storageState 后访问 `/explorer`、`/dataIn/Task`、`/management/user`，断言跳转 `/login`。
  - 覆盖：`explorer/tests/permission.spec.ts`
  - 相关：`explorer/src/permission.ts`、参考：`explorer/tests/login.spec.ts`

- [x] B1.2（P1）session 失效/401 行为（如环境可控）
  - Owner: @huolinhe
  - Status: DONE
  - DoD: 通过清 cookies 或模拟后端 401，使页面回到 `/login`（若不可稳定模拟，降级为手工用例）。
  - 覆盖：`explorer/tests/permission.spec.ts`

### B2. Sider / 路由重定向

- [x] B2.1（P0）`/dataIn` 重定向到 `/dataIn/Task`
  - Owner: @huolinhe
  - Status: DONE
  - DoD: 新增用例：访问 `/dataIn`，断言 URL 以 `/dataIn/Task` 结尾。
  - 覆盖：`explorer/tests/sider-routing.spec.ts`
  - 相关：`explorer/src/router/index.ts`

- [x] B2.2（P1）Management 菜单可见性（root）自动化断言
  - Owner: @huolinhe
  - Status: DONE
  - DoD: root 登录后，在 sider 中能定位到 Management 菜单项；点击进入 `/management/user`。
  - 覆盖：`explorer/tests/sider-routing.spec.ts`
  - 相关：`explorer/src/layout/components/Sider/index.vue`

### B3. Explorer / SQL

- [x] B3.1（P0）SQL 执行错误提示验证
  - Owner: @huolinhe
  - Status: DONE
  - DoD: 执行明显错误 SQL（如 `select * from __not_exist__`），断言出现错误提示（message/notification），并且 Run 按钮恢复可用。
  - 覆盖：`explorer/tests/explorer-sql-error.spec.ts`
  - 相关：`explorer/tests/_utils/explorerSql.ts`

- [x] B3.2（P1）多语句 batch 执行的结果渲染稳定性
  - Owner: @huolinhe
  - Status: DONE
  - DoD: 连续执行 N 条 SQL（N>=5），不出现 editor 卡死/Run 按钮长期 disabled。
  - 覆盖：`explorer/tests/explorer-sql-error.spec.ts`

### B4. DataIn（任务列表与任务生命周期）

- [x] B4.1（P0）Stop 任务（含确认框）
  - Owner: @huolinhe
  - Status: DONE
  - DoD: 基于现有 E2E 创建并启动的任务，执行 Stop；断言状态变更为停止态（或 UI 反馈）。
  - 覆盖：`explorer/tests/datain-lifecycle.spec.ts`
  - 相关：`explorer/tests/_utils/datain.ts`

- [x] B4.2（P0）View/Readonly 进入与返回
  - Owner: @huolinhe
  - Status: DONE
  - DoD: 从任务列表进入 View（readonly），断言 URL 包含 `readonly=true`，存在 Back/Modify 等按钮；点击返回回到任务列表。
  - 覆盖：`explorer/tests/datain-lifecycle.spec.ts`
  - 相关：`viewTaskReadonlyFromRow`（`explorer/tests/_utils/datain.ts`）

- [ ] B4.3（P0）Edit 修改并保存（最小改动）
  - Owner:
  - Status: TODO
  - DoD: 进入 edit 模式，修改一个非关键字段（如 timeout 或开关），保存并应用成功；任务可再次启动。
  - 备注：Helper function `editTaskFromRow` 已添加到 `explorer/tests/_utils/datain.ts`

- [x] B4.4（P0）Delete：只有停止态可删除
  - Owner: @huolinhe
  - Status: DONE
  - DoD: 对停止态任务执行 Delete，确认框通过后列表中任务消失。
  - 覆盖：`explorer/tests/datain-lifecycle.spec.ts`

- [ ] B4.5（P1）Copy：复制配置生成新任务草稿
  - Owner:
  - Status: TODO
  - DoD: 从列表复制任务，进入 add/copy 页面，名称可编辑且默认带出配置；提交后生成新任务。
  - 备注：Helper function `copyTaskFromRow` 已添加到 `explorer/tests/_utils/datain.ts`

- [x] B4.6（P1）列表列头与关键字段渲染断言
  - Owner: @huolinhe
  - Status: DONE
  - DoD: `/dataIn/Task` 断言至少存在：Name/Type/Target DB/Status（具体文案以英文为准）。
  - 覆盖：`explorer/tests/datain-lifecycle.spec.ts`

### B5. Management

- [ ] B5.1（P1）Add User：表单校验 + 提交（如环境允许）
  - Owner:
  - Status: TODO
  - DoD: 打开 Add User dialog，校验必填提示；若允许创建，使用唯一用户名创建后可在列表看到；最后清理该用户。

---

## 4. 功能自动化扩展（Workstream C：P1/P2）

### C1. Topic（数据订阅）

- [x] C1.1（P1）Topic 列表渲染 + Refresh
  - Owner: @huolinhe
  - Status: DONE
  - DoD: 访问 `/topic`，table 可见；点击 Refresh 无报错。
  - 覆盖：`explorer/tests/topic-stream.spec.ts`
  - 相关：`explorer/src/views/6_topic/views/topic.vue`

- [x] C1.2（P1）Create Topic（SQL 模式）
  - Owner: @huolinhe
  - Status: DONE
  - DoD: 打开 Create Topic dialog，切换到 SQL，输入合法 `CREATE TOPIC ...`，创建成功提示；列表出现 topic。
  - 覆盖：`explorer/tests/topic-stream.spec.ts`（基础 UI 验证，完整创建流程待补充）
  - 相关：`explorer/src/views/6_topic/components/addTopic.vue`

- [ ] C1.3（P2）Sample Code 页面基本可用（tab 切换）
  - Owner:
  - Status: TODO
  - DoD: 从 topic 行操作进入 `/topic/example`，切换 Go/Rust/Python/Java tab 不报错。

### C2. Stream

- [x] C2.1（P2）Create Stream（SQL 校验 + 成功提示）
  - Owner: @huolinhe
  - Status: DONE
  - DoD: 打开 Create Stream，输入非法 SQL 提示错误；输入合法 SQL 创建成功。
  - 覆盖：`explorer/tests/topic-stream.spec.ts`（基础 UI 和校验验证，完整创建流程待补充）
  - 相关：`explorer/src/views/5_stream/components/addStream.vue`

### C3. Programming / Tools 文档页

- [x] C3.1（P2）`/programming` 文档列表渲染 + 点击进入详情
  - Owner: @huolinhe
  - Status: DONE
  - DoD: 页面加载无错误，点击任一条进入 `/docs/connector/...`。
  - 覆盖：`explorer/tests/docs-pages.spec.ts`
  - 相关：`explorer/src/views/4_programming/views/main.vue`

- [x] C3.2（P2）`/tools` 文档列表渲染 + 点击进入详情
  - Owner: @huolinhe
  - Status: DONE
  - DoD: 页面加载无错误，点击任一条进入 `/docs/tool/...`。
  - 覆盖：`explorer/tests/docs-pages.spec.ts`
  - 相关：`explorer/src/views/7_tools/views/main.vue`

---

## 5. DataIn 数据源测试开发计划（按类型划分 / 每个数据源独立追踪）（Workstream D）

目标：将 DataIn 各数据源按类型拆分，并为**每个数据源**建立独立任务条目，便于多人并行推进与追踪。

统一模板（每个数据源都要做；可按实际能力标记 BLOCKED/MANUAL）：

- 表单渲染：进入 `/dataIn/add`，选择对应 type，关键配置块出现
- 必填校验：不填必填项点击 Submit，出现错误提示
- 连通性检查：如该数据源支持 check connectivity，覆盖成功/失败路径
- 最小可运行任务：创建并启动任务，状态进入 queued/running（或同等“已启动”状态）
- 清理：停止并删除任务（必要时清 DB / topic / 外部资源）

优先级建议：
- P0：TMQ（已覆盖，但需持续稳定性改进与生命周期操作补齐）
- P1：Kafka / MQTT、MySQL / PostgreSQL
- P2：OPC-UA/OPC-DA/PI/AVEVA Historian 等依赖外部系统的数据源

### D0. 通用准备任务（适用于所有 DataIn 数据源）

- [ ] D0.1（P0）建立“每类数据源的最小可运行环境”说明（services/ports/credentials）
  - Owner:
  - Status: TODO
  - DoD: 在本计划或独立文档中列出每类数据源需要的 docker-compose / 服务地址 / 账号密码 / 示例数据。

- [ ] D0.2（P0）建立统一的命名与清理策略
  - Owner:
  - Status: TODO
  - DoD: 约定统一前缀（如 `e2e_` + timestamp），并提供清理脚本/工具或测试内清理流程。

---

### D1. TDengine 系列

#### D1.1 TMQ（TDengine 3.x 数据订阅）— id: `tmq`

- [ ] D1.1.1（P0）补齐生命周期：Stop/Start 循环
  - Owner:
  - Status: TODO
  - DoD: 基于现有 TMQ E2E 任务，Stop 后可再次 Start，状态变化可观测。

- [ ] D1.1.2（P0）View/Edit/Copy/Delete（TMQ）
  - Owner:
  - Status: TODO
  - DoD: 覆盖 View/Readonly、进入 Edit 修改一个非关键字段并保存、复制生成新任务、停止态删除。

- [ ] D1.1.3（P0）去除对 groups_after UUID 的硬编码依赖
  - Owner:
  - Status: TODO
  - DoD: selector 不再依赖固定 UUID（见 A2.1）。

#### D1.2 TDengine 2.x（基于 REST/SQL 迁移/同步）— id: `taos`（名称以 UI 中实际为准）

- [ ] D1.2.1（P1）表单渲染与必填校验
  - Owner:
  - Status: TODO
  - DoD: protocol/address/port/db/username/password/mode 等关键字段出现且必填提示正确。

- [ ] D1.2.2（P1）连通性检查（如支持）
  - Owner:
  - Status: TODO
  - DoD: 成功与失败路径均可稳定复现并断言。

- [ ] D1.2.3（P2）最小可运行任务（历史迁移 or 实时同步）
  - Owner:
  - Status: TODO
  - DoD: 能启动并写入目标库（可通过目标库 SQL 验证有数据写入）。

---

### D2. 消息队列 / 流式数据源

#### D2.1 Kafka — id: `kafka`

- [ ] D2.1.1（P1）表单渲染与必填校验（brokers/topic/offset/timeout）
  - Owner:
  - Status: TODO
  - DoD: 关键字段渲染；必填校验可触发。

- [ ] D2.1.2（P1）连通性检查（如支持）
  - Owner:
  - Status: TODO
  - DoD: reachable / unreachable 两条路径可稳定断言。

- [ ] D2.1.3（P2）最小可运行任务（consume -> 写入 TDengine）
  - Owner:
  - Status: TODO
  - DoD: 启动任务后，向 topic 写入一条样例消息，最终在目标库可查询到落库数据。

#### D2.2 MQTT — id: `mqtt`

- [x] D2.2.1（P1）表单渲染与必填校验（broker/topic/clientId/qos）
  - Owner: @yanyuxing
  - Status: Done
  - DoD: 关键字段渲染；必填校验可触发。

- [x] D2.2.2（P1）连通性检查（如支持）
  - Owner: @yanyuxing
  - Status: Done
  - DoD: reachable / unreachable 两条路径可稳定断言。

- [x] D2.2.3（P2）最小可运行任务（publish -> 写入 TDengine）
  - Owner: @yanyuxing
  - Status: Done
  - DoD: 提交任务后，返回任务列表，当前任务状态显示运行中

#### D2.3 SparkplugB（基于 MQTT 的 IIoT 规范）— id: `sparkplugb`

- [ ] D2.3.1（P2）表单渲染与必填校验（brokers/version/client_id/subscribe options）
  - Owner:
  - Status: TODO
  - DoD: 关键字段渲染；必填校验可触发。

- [ ] D2.3.2（P2）连通性检查（如支持）
  - Owner:
  - Status: TODO
  - DoD: reachable / unreachable 两条路径可稳定断言。

- [ ] D2.3.3（P2）最小可运行任务（SparkplugB payload -> 写入 TDengine）
  - Owner:
  - Status: TODO
  - DoD: 发布 SparkplugB 样例消息后，目标库可查询到落库数据。

#### D2.4 Pulsar — id: `pulsar`

- [ ] D2.4.1（P2）表单渲染与必填校验（endpoint/auth/topic/subscription）
  - Owner:
  - Status: TODO
  - DoD: 关键字段渲染；必填校验可触发。

- [ ] D2.4.2（P2）连通性检查（如支持）
  - Owner:
  - Status: TODO
  - DoD: reachable / unreachable 两条路径可稳定断言。

- [ ] D2.4.3（P2）最小可运行任务（consume -> 写入 TDengine）
  - Owner:
  - Status: TODO
  - DoD: 向 Pulsar topic 写入样例消息后，目标库可查询到落库数据。

#### D2.5 Pulsar-Tuya — id: `pulsarTuya`

- [ ] D2.5.1（P2）表单渲染与必填校验（endpoint + tuya auth）
  - Owner:
  - Status: TODO
  - DoD: Access Id/Key/env 等字段渲染且必填校验可触发。

- [ ] D2.5.2（P2）连通性检查（如支持）
  - Owner:
  - Status: TODO
  - DoD: reachable / unreachable 两条路径可稳定断言。

- [ ] D2.5.3（P2）最小可运行任务（Tuya payload -> 写入 TDengine）
  - Owner:
  - Status: TODO
  - DoD: 可稳定消费并落库一条样例数据。

---

### D3. 关系型数据库（Relational DB）

#### D3.1 MySQL — id: `mysql`

- [ ] D3.1.1（P1）表单渲染与必填校验（host/port/db/username/password/charset/ssl）
  - Owner:
  - Status: TODO
  - DoD: 关键字段渲染；必填校验可触发。

- [ ] D3.1.2（P1）连通性检查（如支持）
  - Owner:
  - Status: TODO
  - DoD: 成功/失败两条路径可稳定断言。

- [ ] D3.1.3（P2）最小可运行任务（SQL 模板 + 时间范围）
  - Owner:
  - Status: TODO
  - DoD: 从 MySQL 读取一小批数据并写入目标库（可在 TDengine 侧验证）。

#### D3.2 PostgreSQL — id: `postgres`

- [ ] D3.2.1（P1）表单渲染与必填校验
  - Owner:
  - Status: TODO
  - DoD: 关键字段渲染；必填校验可触发。

- [ ] D3.2.2（P1）连通性检查（如支持）
  - Owner:
  - Status: TODO
  - DoD: 成功/失败两条路径可稳定断言。

- [ ] D3.2.3（P2）最小可运行任务（SQL 模板 + 时间范围）
  - Owner:
  - Status: TODO
  - DoD: 能读写一小批数据并可在 TDengine 侧验证。

#### D3.3 Oracle — id: `oracle`

- [ ] D3.3.1（P2）表单渲染与必填校验（host/port/database/auth/sql template）
  - Owner:
  - Status: TODO
  - DoD: 关键字段渲染；必填校验可触发。

- [ ] D3.3.2（P2）连通性检查
  - Owner:
  - Status: TODO
  - DoD: 成功/失败两条路径可稳定断言。

- [ ] D3.3.3（P2）最小可运行任务（SQL 模板 + 时间范围）
  - Owner:
  - Status: TODO
  - DoD: 能读取少量 Oracle 数据并写入目标库（TDengine 侧可验证）。

#### D3.4 Microsoft SQL Server — id: `mssql`

- [ ] D3.4.1（P2）表单渲染与必填校验（host/port/database/auth/sql template）
  - Owner:
  - Status: TODO
  - DoD: 关键字段渲染；必填校验可触发。

- [ ] D3.4.2（P2）连通性检查
  - Owner:
  - Status: TODO
  - DoD: 成功/失败两条路径可稳定断言。

- [ ] D3.4.3（P2）最小可运行任务（SQL 模板 + 时间范围）
  - Owner:
  - Status: TODO
  - DoD: 能读取少量 SQL Server 数据并写入目标库（TDengine 侧可验证）。

---

### D4. 时序数据库 / 其他时序生态

#### D4.1 InfluxDB — id: `influxdb`

- [ ] D4.1.1（P2）表单渲染与必填校验（v1/v2 认证差异）
  - Owner:
  - Status: TODO
  - DoD: 可覆盖 v1.x 与 v2.x 至少一种路径（另一种可标记 BLOCKED）。

- [ ] D4.1.2（P2）最小可运行任务（bucket/measurement + 时间范围）
  - Owner:
  - Status: TODO
  - DoD: 能读取少量数据并写入目标库。

#### D4.2 OpenTSDB — id: `opentsdb`

- [ ] D4.2.1（P2）表单渲染与必填校验（host/port/metrics）
  - Owner:
  - Status: TODO
  - DoD: 关键字段渲染；必填校验可触发。

- [ ] D4.2.2（P2）最小可运行任务（metrics + 时间范围）
  - Owner:
  - Status: TODO
  - DoD: 能读写少量数据并可在 TDengine 侧验证。

---

### D5. 工业协议 / 厂商系统

#### D5.1 OPC-UA — id: `opcua`

- [ ] D5.1.1（P2）表单渲染与必填校验（endpoint/security/pki/cert）
  - Owner:
  - Status: TODO
  - DoD: 安全模式/策略/证书字段能正确渲染；必填校验可触发。

- [ ] D5.1.2（P2）连通性检查（需要可控 OPC-UA server）
  - Owner:
  - Status: TODO
  - DoD: 成功/失败路径可稳定复现。

- [ ] D5.1.3（P2）最小可运行采集任务（点位集）
  - Owner:
  - Status: TODO
  - DoD: 采集到至少一条数据并落库可查。

#### D5.2 OPC-DA — id: `opcda`

- [ ] D5.2.1（P2）表单渲染与必填校验
  - Owner:
  - Status: TODO
  - DoD: 关键字段渲染；必填校验可触发。

- [ ] D5.2.2（P2）连通性检查/最小可运行任务（需要可控 OPC-DA server）
  - Owner:
  - Status: TODO
  - DoD: 能连通并采集到数据。

#### D5.3 PI — id: `pi`

- [ ] D5.3.1（P2）表单渲染：DA Only / AF 两种模式
  - Owner:
  - Status: TODO
  - DoD: 两种模式切换时字段与校验逻辑正确。

- [ ] D5.3.2（P2）最小可运行任务（需要可控 PI 环境）
  - Owner:
  - Status: TODO
  - DoD: 能同步少量数据并落库可查。

#### D5.4 PI Backfill — id: `pibackfill`

- [ ] D5.4.1（P2）表单渲染与必填校验（Backfill start/end）
  - Owner:
  - Status: TODO
  - DoD: Backfill 时间字段/动态取值逻辑可用。

- [ ] D5.4.2（P2）最小可运行任务（需要可控 PI 环境）
  - Owner:
  - Status: TODO
  - DoD: 回填写入可在目标库验证。

#### D5.5 AVEVA Historian — id: `avevaHistorian`

- [ ] D5.5.1（P2）表单渲染与必填校验（server/port/auth/tags/time range）
  - Owner:
  - Status: TODO
  - DoD: 关键字段渲染；必填校验可触发。

- [ ] D5.5.2（P2）最小可运行任务（需要可控 Historian 环境）
  - Owner:
  - Status: TODO
  - DoD: 同步少量数据并落库可查。

#### D5.6 KingHistorian — id: `kinghist`

- [ ] D5.6.1（P2）表单渲染与必填校验（host/port/auth + tag dataset）
  - Owner:
  - Status: TODO
  - DoD: 关键字段渲染；tag 映射（dataset）相关配置可用且必填校验可触发。

- [ ] D5.6.2（P2）连通性检查
  - Owner:
  - Status: TODO
  - DoD: 成功/失败两条路径可稳定断言。

- [ ] D5.6.3（P2）最小可运行任务（历史迁移/实时同步）
  - Owner:
  - Status: TODO
  - DoD: 能同步少量数据并落库可查。

---

### D6. 文件类数据源

#### D6.1 CSV — id: `csv`

- [ ] D6.1.1（P2）表单渲染与必填校验（分隔符/表头/忽略行/文件上传 or 目录）
  - Owner:
  - Status: TODO
  - DoD: 两种模式（上传/目录）至少覆盖一种；字段校验正确。

- [ ] D6.1.2（P2）最小可运行任务（导入少量 CSV）
  - Owner:
  - Status: TODO
  - DoD: 导入后在目标库可查询到数据；清理导入任务。

---

### D7. NoSQL / 文档数据库

#### D7.1 MongoDB — id: `mongodb`

- [ ] D7.1.1（P2）表单渲染与必填校验（host/port/auth/database/collection）
  - Owner:
  - Status: TODO
  - DoD: 关键字段渲染；必填校验可触发。

- [ ] D7.1.2（P2）连通性检查
  - Owner:
  - Status: TODO
  - DoD: 成功/失败两条路径可稳定断言。

- [ ] D7.1.3（P2）最小可运行任务（读 MongoDB -> 写入 TDengine）
  - Owner:
  - Status: TODO
  - DoD: 能同步少量数据并在 TDengine 侧验证。

---

## 6. Review Checklist（合并前自检）

- [ ] 新增用例是否默认可在 integrated env 运行？是否需要额外 service？
- [ ] 是否强制英文且不依赖中文文案？
- [ ] selector 是否稳定（id/role/testid 优先）？是否避免了 nth/文本脆弱匹配？
- [ ] 是否会污染环境（创建资源）？是否有清理策略？
- [ ] 是否在失败时提供足够日志/截图/trace 定位问题？
