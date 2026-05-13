# TDengine Grafana Plugin v4.0.0 - 威胁建模报告

### 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-03-26 | 2026-03-26 | 1.0 | 霍琳贺 | 初始版本 |

---

### 2. 基本信息

| 项目信息 | 内容 |
| --- | --- |
| 报告编号 | `TM-TSDB-GRAFPLUGIN-001` |
| 需求名称 | TDengine Grafana Plugin v4.0.0 |
| 设计文档链接 | `docs/releases/TSDB-v3.4.1-[20260331]/05-设计文档/grafanaplugin-v4.0.0-FS.md` |
| 版本编号 | 3.4.1 |
| 业务负责人 | 佘彦杰 |
| 发起人 | 佘彦杰 |
| 安全负责人 | 霍琳贺 |
| 参会人员 | 霍琳贺、关胜亮、肖波、张心治、佘彦杰 |
| 报告日期 | 2026-03-26 |
| 总体评价 | 附带条件通过 |

---

### 3. 分析报告

本报告针对 TDengine Grafana Plugin v4.0.0 进行威胁建模分析。该版本是一个重大变更版本（Breaking Changes Release），核心变更为将数据源架构从 `DataSourceApi`（前端直连）迁移至 `DataSourceWithBackend`（后端代理），同时修复了已知 CVE 漏洞并新增 SQL 宏扩展能力。

- **核心发现：** 共识别出 `7` 个潜在威胁，其中 `高危` `1` 个（已在 v4.0.0 中修复），`中危` `4` 个，`低危` `2` 个。
- **主要风险场景：**
  - **SQL 模板变量注入：** Grafana 仪表板变量值直接拼接进 SQL（如 `AND location = '$location'`），若变量值未经校验，可能被利用执行非预期 SQL 操作。
  - **过度查询耗尽资源：** 用户构造超大时间范围或无过滤条件的 SQL，导致 TDengine 返回海量数据，耗尽 Grafana 后端内存或处理资源。
  - **错误信息泄露：** TDengine 返回的原始数据库错误（含表名、列名等结构信息）被直接透传至前端展示。
- **结论：** 该版本架构迁移整体提升了安全基线，CVE 高危漏洞已主动修复。SQL 模板变量注入是主要残余风险，通过配置最小权限账号和使用 Grafana 变量白名单可有效缓解，建议在文档和部署规范中明确说明。

---

### 4. 威胁识别与分析（STRIDE）

本次评审针对 TDengine Grafana Plugin v4.0.0 的核心数据流：用户在 Grafana 界面输入 SQL → Grafana 前端 Plugin → Grafana 后端 Plugin（Go）→ TDengine REST API → 数据返回至前端展示。

#### 4.1 关键实体与数据

| 实体/数据 | 描述 | 敏感性 |
| --- | --- | --- |
| TDengine 连接凭证 | 用于连接 TDengine 的用户名、密码或 Token，存储于 Grafana secureJsonData | 高 |
| SQL 查询语句 | 用户在 QueryEditor 编写、包含 SQL 宏和 Grafana 变量的查询文本 | 中 |
| Grafana 模板变量值 | 仪表板变量（如 `$location`、`${table_name}`），由用户或仪表板配置决定 | 中 |
| TDengine 时序数据 | 从 TDengine 查询返回的时序数据（传感器读数、监控指标等） | 高（取决于业务） |
| SQL 宏替换结果 | `$__timeFilter` 等宏在后端展开后的时间范围 SQL 片段 | 低 |
| TDengine 错误信息 | TDengine REST API 返回的错误描述，可能含表名、SQL 片段等内部信息 | 中 |
| Grafana 插件 gRPC 通道 | 前端与 Go 后端插件之间的内部 gRPC 通信通道 | 中 |

#### 4.2 威胁评估

| 威胁 ID | 威胁描述/攻击场景 | STRIDE | 相关组件/数据流 | 风险等级 |
| --- | --- | --- | --- | --- |
| T-GRAFPLUGIN-01 | **SQL 模板变量注入：** 攻击者（具有仪表板编辑权限或利用变量 URL 参数）将恶意 SQL 片段注入 Grafana 模板变量值（如将 `$location` 设为 `'; SELECT * FROM sensitive_table; --`），通过 SQL 拼接（如 `AND location = '$location'`）在 TDengine 中执行非预期查询，可能导致未授权数据读取或（若账号有写权限）数据篡改。 | T（篡改）、I（信息泄露） | Grafana 变量 → 后端 SQL 构造 → TDengine REST API | 中 |
| T-GRAFPLUGIN-02 | **TDengine 凭证不当存储泄露：** 若运维人员在配置数据源时将 TDengine 凭证直接嵌入连接 URL（如 `http://user:pass@tdengine:6041/`）而非使用 Grafana secureJsonData 机制，凭证将以明文形式出现在 Grafana 配置文件、日志或 API 响应中，造成凭证泄露。 | I（信息泄露） | 数据源配置 → Grafana 配置存储 / 后端日志 | 中 |
| T-GRAFPLUGIN-03 | **恶意大查询拒绝服务：** 具有数据源访问权限的用户构造不含时间过滤的 SQL（绕过 `$__timeFilter` 宏、使用 `$__timeFrom` 和 `$__timeTo` 设置超大范围，或直接全表扫描），导致 TDengine 返回数百万条记录，耗尽 Grafana 后端 Go 进程的内存资源，影响其他用户正常使用。 | D（拒绝服务） | SQL 查询 → TDengine REST API → 后端数据处理（convertRow、LongToWide） | 中 |
| T-GRAFPLUGIN-04 | **仪表板共享越权数据访问：** Grafana 管理员将包含高权限 TDengine 数据源的仪表板设置为公开共享（anonymous access），未经认证的用户或低权限用户可通过公开仪表板的查询接口访问不应公开的时序数据（如工厂设备状态、能源消耗数据等）。 | E（权限提升） | 共享仪表板 → TDengine 数据源 → TDengine REST API | 中 |
| T-GRAFPLUGIN-05 | **依赖组件供应链漏洞（已在 v4.0.0 修复）：** `google.golang.org/grpc`（CVE-2026-33186）存在 gRPC 通信安全漏洞，`go.opentelemetry.io/otel/sdk`（CVE-2026-24051）存在 SDK 安全漏洞，攻击者可利用这些漏洞攻击 Grafana 后端插件进程。v4.0.0 已通过升级依赖版本修复。 | S（仿冒）、T（篡改）、I（信息泄露） | Grafana 后端 gRPC 通道、OpenTelemetry 数据采集 | 高（已修复） |
| T-GRAFPLUGIN-06 | **TDengine 错误信息泄露内部结构：** 当 SQL 执行出错时（如表名错误、语法错误），TDengine REST API 返回的原始错误信息（含数据库名、表名、列名、SQL 片段）被后端直接透传至前端展示（如章节 7 的错误排查指引中描述的场景），使攻击者可以通过构造错误查询来探测数据库内部结构。 | I（信息泄露） | TDengine REST API 错误响应 → 后端错误处理 → Grafana 前端展示 | 低 |
| T-GRAFPLUGIN-07 | **时间宏参数伪造绕过时间过滤：** 攻击者通过构造特殊的 Grafana 时间范围参数（如将时间范围设置为数年或数十年的历史跨度），使 `$__timeFilter` 宏展开为极大的时间范围 SQL 片段，绕过正常的数据访问时间边界约束，批量获取大量历史时序数据。 | I（信息泄露） | Grafana 时间范围参数 → SQL 宏替换 → TDengine 查询 | 低 |

---

### 5. 安全需求与设计约束

| 威胁 ID | 转化后的安全需求/设计约束 | 类型 | 优先级 |
| --- | --- | --- | --- |
| T-GRAFPLUGIN-01 | **SEC-GRAFPLUGIN-001（最小权限账号）：** 在插件部署文档/README 中明确要求：应为 Grafana TDengine 数据源配置专用只读账号（仅具备 SELECT 权限），从根本上将 SQL 注入的危害范围限制为"只能读取该账号有权访问的数据"，防止通过变量注入执行 INSERT/UPDATE/DELETE/DROP 等破坏性操作。同时建议提示用户使用 Grafana Custom Variable（预定义白名单值）限制变量可选范围，而非开放自由输入。 | 安全设计约束（文档层面） | 高 |
| T-GRAFPLUGIN-02 | **SEC-GRAFPLUGIN-002（凭证安全存储）：** 在部署文档中明确禁止将 TDengine 凭证嵌入连接 URL，必须通过 Grafana 数据源配置界面的 User/Password/Token 字段填写，由 Grafana secureJsonData 机制加密存储。（v4.0.0 后端代理架构已在技术层面强制执行凭证隔离，此为文档合规要求。） | 安全设计约束（文档层面） | 中 |
| T-GRAFPLUGIN-03 | **SEC-GRAFPLUGIN-003（查询资源防护）：** 建议在后端 `QueryDataHandler` 中增加查询响应大小上限配置（如最大返回行数）和查询超时配置，防止超大查询耗尽后端资源。可参考 Grafana 插件 SDK 的 `QueryDataRequest.TimeRange` 字段对时间范围合法性进行前置校验。 | 非功能安全需求 | 中 |
| T-GRAFPLUGIN-04 | **SEC-GRAFPLUGIN-004（数据源权限配置规范）：** 在部署文档中说明 Grafana 数据源的访问权限配置，明确公开仪表板不应绑定高权限 TDengine 数据源，建议为公开展示场景创建权限受限的专用数据源配置。 | 安全设计约束（文档层面） | 中 |
| T-GRAFPLUGIN-06 | **SEC-GRAFPLUGIN-005（错误信息脱敏）：** 后端在将 TDengine REST API 返回的错误信息透传至前端前，应对可能包含内部结构信息的原始错误信息进行适当过滤或摘要化处理，避免直接暴露数据库内部表名、列名等结构详情。 | 非功能安全需求 | 低 |

---

### 6. 后续行动与验证

| 行动项 | 描述 | 责任方 | 完成标准 |
| --- | --- | --- | --- |
| 1. 补充部署安全文档 | 在 README 或部署指南中补充 SQL 模板变量安全使用规范（使用只读账号、变量白名单建议），以及凭证安全存储规范，对应 SEC-GRAFPLUGIN-001、SEC-GRAFPLUGIN-002。 | 佘彦杰 | 安全说明内容合入 README，经安全代表确认。 |
| 2. 评估查询资源限制实现可行性 | 评估在后端 `QueryDataHandler` 中增加最大返回行数和查询超时配置的实现方案，对应 SEC-GRAFPLUGIN-003。 | 佘彦杰 | 形成实现方案或确认通过 Grafana 配置覆盖（可推迟至下一版本）。 |
| 3. 验证 CVE 修复有效性 | 确认 v4.0.0 发布包中 `google.golang.org/grpc` 已为 v1.79.3，`go.opentelemetry.io/otel/sdk` 已为 v1.40.0。 | 佘彦杰 | `go.sum` 或 `go.mod` 中版本确认，CI 扫描通过。 |

---

### 7. 审批意见

| 角色 | 意见 | 签字 | 日期 |
| --- | --- | --- | --- |
| 产品负责人 | 确认理解安全需求的重要性，并将其纳入开发优先级。 | | |
| 技术负责人 | 确认上述安全约束在技术上是可行且必要的，将在架构设计中落实。 | | |
| 安全负责人 | 确认威胁分析全面，安全需求是保障该功能顺利上线的关键。 | | |
