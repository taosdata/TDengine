# taosAdapter 支持 HTTP JSON 写入 - 威胁建模报告

### 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-10-27 | 2025-10-27 | 1.0 | 霍琳贺 | 初始版本 |

### 2. 基本信息

| 项目信息 | 内容 |
| --- | --- |
| 报告编号 | `TM-ADAPTER-HTTPJSON-001` |
| 需求名称 | taosAdapter 支持 HTTP JSON 写入（input_json） |
| 设计文档链接 | [taosAdapter 支持 HTTP JSON 写入 FS](https://taosdata.feishu.cn/wiki/Eb5CwW9QwiqUXjkmDMQcTDzInHh) |
| 版本编号 | 3.4.0.0 |
| 业务负责人 | 霍琳贺 |
| 发起人 | 谭雪峰 |
| 安全负责人 | 霍琳贺 |
| 参会人员 | 霍琳贺、谭雪峰、佘彦杰 |
| 报告日期 | 2025-10-27 |
| 总体评价 | 附带条件通过 |

### 3. 分析报告

本报告针对 taosAdapter HTTP JSON 写入（input_json）功能进行了威胁建模分析。该功能旨在为 IoT/HTTP 直连写入场景提供“HTTP + JSON + 配置化映射”的写入入口：接收 JSON payload，经 JSONata 转换为一维数组后生成写入 SQL 并写入 TDengine，并提供 dry_run 用于调试。
- 核心发现：共识别出 `15` 个潜在威胁，其中 `高危` `6` 个，`中危` `7` 个，`低危` `2` 个。
- 主要风险场景：
   - 未授权写入 / 凭证泄露：认证配置不当或未强制 TLS，导致凭证被窃取、接口被滥用。
   - SQL 注入与标识符注入：SQL 拼接中对 value/identifier 的处理不严谨，可能被构造 payload 绕过。
   - DoS（大包 / 复杂 JSONata / 解析耗时）：攻击者构造大 payload 或高复杂度转换与解析，导致 CPU/内存/连接池耗尽。
   - 调试接口泄露：dry_run 返回转换后的 JSON 与生成 SQL，可能暴露敏感数据或内部表结构。
   - 配置篡改：规则配置被篡改会改变写入目标与字段映射，造成数据完整性破坏。
- 结论：该功能在提升接入效率方面价值显著，但配置驱动的转换与 SQL 生成引入了新的攻击面。必须在功能详细设计与编码实现前，将本报告提出的安全需求作为核心设计约束予以落实，重点关注强认证与 TLS、输入与标识符校验、请求大小/复杂度限制、dry_run 风险控制、配置与日志安全。

### 4. 威胁识别与分析（STRIDE）

功能描述：
1. 用户通过 HTTP POST 调用 `POST /input_json/v1/{endpoint}`，提交 JSON payload。
2. taosAdapter 根据配置文件中的 `input_json.rules` 选择匹配 `{endpoint}` 的 Rule。
3. 使用 JSONata（仅支持 1.5.4）将 payload 转换为打平的一维数组。
4. 从转换结果中提取（或使用默认值）db/supertable/subtable/time/fields。
5. 解析时间（支持多种 `timeFormat`），并生成自动建表写入 SQL（列名反引号包裹，字符串按规则转义）。
6. 通过与其他接口共用的连接池写入 TDengine。
7. `dry_run=true` 时返回转换后的 JSON 与 SQL，不实际写入。
信任边界与数据流：
- 外部客户端 -> taosAdapter（HTTP 请求边界）
- taosAdapter -> TDengine（数据库连接边界）
- 配置文件/磁盘 -> taosAdapter 进程（配置完整性边界）
- taosAdapter -> 日志/监控系统（可观测性边界）

#### 4.1 关键实体与数据

| 实体/数据 | 描述 | 敏感性 |
| --- | --- | --- |
| HTTP 请求（JSON payload） | 外部输入，可能包含业务敏感数据与恶意构造内容 | 高 |
| 认证凭证 | Basic Auth 或其他方式的用户名/密码/token | 高 |
| input_json 配置文件 | `input_json.enable`、`rules`、`transformation` 等 | 高（可改变写入目标与逻辑） |
| JSONata 表达式 | 转换逻辑，复杂度可被滥用导致资源消耗 | 中 |
| 转换后的 JSON（一维数组） | 写入前的中间数据结构 | 中 |
| 生成的 SQL | 自动建表写入 SQL（含表名/字段名/数据） | 高 |
| dry_run 输出 | 返回转换 JSON 与 SQL（可能包含敏感数据） | 高 |
| 连接池 | 与其他 RESTful/schemaless 接口共享 | 中 |
| 错误日志与审计日志 | 记录失败原因、可能包含 payload/SQL 片段 | 中~高 |
| 指标（adapter_input_json） | total/success/fail/inflight/affected + tag | 低~中（关注泄露与基数膨胀） |

#### 4.2 威胁评估

| 威胁 ID | 威胁描述 / 攻击场景 | STRIDE | 相关组件 / 数据流 | 风险等级 |
| --- | --- | --- | --- | --- |
| T-HTTPJSON-01 | **未授权访问/认证绕过**：接口暴露在不可信网络中，认证被关闭、弱口令或未正确校验，导致任意用户可写入数据库。 | S/E | TB1 -> HTTP Handler -> 写入逻辑 | 高 |
| T-HTTPJSON-02 | **凭证嗅探/中间人攻击**：未强制 TLS 时，Basic Auth 凭证可能被窃取，攻击者复用凭证写入/篡改数据。 | S/I | TB1（网络传输） | 高 |
| T-HTTPJSON-03 | **暴力破解/撞库**：攻击者对 endpoint 进行探测并对认证进行暴力尝试，导致账号被猜中或服务性能下降。 | S/D | HTTP 入口 / 认证模块 | 中 |
| T-HTTPJSON-04 | **重放攻击**：无 nonce/时间戳/签名机制时，攻击者重放捕获到的写入请求，造成重复写入、数据污染。 | R/T | TB1（请求重放） | 中 |
| T-HTTPJSON-05 | **大请求体 DoS**：发送超大 JSON payload 或深层嵌套 JSON，导致解析、转换、内存占用暴涨，引发 OOM 或长时间 GC。 | D | JSON 解析 / 转换 / SQL 构建 | 高 |
| T-HTTPJSON-06 | **JSONata 复杂度 DoS**：构造 payload 触发复杂 transformation（或 transformation 本身过于复杂），导致 CPU 饱和、请求超时、影响其他接口（共享资源）。 | D | JSONata 计算 / TB1 | 高 |
| T-HTTPJSON-07 | **时间解析 DoS/异常崩溃**：构造极端时间字符串或超大数值（unix_ns 等），触发解析慢路径、溢出或 panic，导致服务不可用。 | D | timeKey/timeFormat/timezone 解析 | 中 |
| T-HTTPJSON-08 | **值注入导致 SQL 注入**：字符串转义规则实现存在缺陷时，攻击者通过 payload 中的值注入拼接 SQL，造成越权写入/执行非预期语句。 | T/E | SQL 拼接（values） -> TB2 | 高 |
| T-HTTPJSON-09 | **标识符注入（db/table/column）**：dbKey/superTableKey/subTableKey/fields.key 未严格校验时，攻击者可注入反引号/特殊字符，破坏 SQL 结构或写入到非预期对象。 | T/E | SQL 拼接（identifiers） | 高 |
| T-HTTPJSON-10 | **跨库/跨表越权写入**：当 dbKey / superTableKey / subTableKey 从外部 JSON 提取时，攻击者可指定任意库表名称，造成多租户环境数据越权。 | E/T | Rule 解析 -> SQL 生成 -> TB2 | 中 |
| T-HTTPJSON-11 | **dry_run 信息泄露**：`dry_run=true` 返回转换后的 JSON 与 SQL，可能泄露业务数据、库表结构、字段名与时间格式；也可能被用于探测注入点。 | I | dry_run 响应 | 中 |
| T-HTTPJSON-12 | **日志泄露敏感信息**：错误日志记录完整 payload/SQL/凭证（或包含敏感字段），导致通过日志系统泄露。 | I | TB4（日志/监控） | 中 |
| T-HTTPJSON-13 | **日志注入/伪造审计**：攻击者在输入中注入换行/控制字符，使日志被分行伪造、污染检索与审计结论。 | R/T | TB4（日志） | 低 |
| T-HTTPJSON-14 | **配置文件篡改**：攻击者篡改 input_json 规则（endpoint/db/table/fields/transformation），导致数据被写入错误库表、字段被替换或被植入恶意写入逻辑。 | T/E | TB3（配置） | 高 |
| T-HTTPJSON-15 | **指标高基数/资源耗尽**：若将用户可控内容写入 metric tag（如 endpoint/db/table），可能导致 tag 基数爆炸，引发监控系统或本地缓存资源耗尽。 | D | 指标采集 | 低 |


### 5. 安全需求与设计约束

| 威胁 ID | 转化后的安全需求 / 设计约束 | 类型 | 优先级 |
| --- | --- | --- | --- |
| T-HTTPJSON-01 | **SEC-HTTPJSON-001（强认证）**：接口必须启用认证；禁止默认弱口令；支持最小权限账号（仅允许写入目标库表范围）。 | 安全功能需求 | 高 |
| T-HTTPJSON-02 | **SEC-HTTPJSON-002（强制 TLS）**：生产环境必须强制 HTTPS（或在可信反向代理后强制内网 mTLS），禁止明文传输凭证。 | 安全设计约束 | 高 |
| T-HTTPJSON-03 | **SEC-HTTPJSON-003（防爆破）**：为认证失败增加限流/延迟/封禁策略；对 endpoint 探测增加速率限制。 | 安全功能需求 | 中 |
| T-HTTPJSON-04 | **SEC-HTTPJSON-004（防重放）**：对关键场景提供幂等/去重策略（例如引入 request-id + 时窗去重），或在网关层提供签名/时间戳校验。 | 安全设计约束 | 中 |
| T-HTTPJSON-05 | **SEC-HTTPJSON-005（请求大小与深度限制）**：限制最大请求体大小、JSON 最大深度/数组长度；超限直接拒绝。 | 安全设计约束 | 高 |
| T-HTTPJSON-06 | **SEC-HTTPJSON-006（转换复杂度与超时）**：对 JSONata 执行设置超时与资源上限；必要时限制 transformation 功能子集；对单请求 CPU 时间做隔离（例如 worker 池 + 超时）。 | 安全设计约束 | 高 |
| T-HTTPJSON-07 | **SEC-HTTPJSON-007（时间解析健壮性）**：对时间字段做范围校验与失败兜底；禁止 panic；超长字符串与异常输入需快速失败。 | 健壮性需求 | 中 |
| T-HTTPJSON-08 | **SEC-HTTPJSON-008（SQL value 安全）**：严格实现并单测字符串转义规则；对所有字符串写入做一致转义；避免多语句执行；必要时引入更安全的写入 API（如参数化/协议层写入）。 | 安全功能需求 | 高 |
| T-HTTPJSON-09 | **SEC-HTTPJSON-009（标识符白名单校验）**：对 db/table/column 标识符做严格校验（建议正则白名单：`^[A-Za-z_][A-Za-z0-9_]{0,63}$` 或符合 TDengine 规则的子集）；拒绝包含反引号、点号、空白与控制字符的输入；禁止从外部直接透传任意标识符（或仅允许在白名单映射中选择）。 | 安全设计约束 | 高 |
| T-HTTPJSON-10 | **SEC-HTTPJSON-010（多租户隔离）**：当启用 dbKey/superTableKey/subTableKey 时，必须提供“允许列表映射”或按认证主体绑定可写库表范围；禁止任意库表写入。 | 安全设计约束 | 中 |
| T-HTTPJSON-11 | **SEC-HTTPJSON-011（dry_run 风险控制）**：提供配置开关控制 dry_run 是否可用；生产默认关闭或仅对管理员开放；响应中避免返回完整原始数据（可脱敏/截断）。 | 安全功能需求 | 中 |
| T-HTTPJSON-12 | **SEC-HTTPJSON-012（日志脱敏与最小化）**：禁止记录凭证；对 payload/SQL 做脱敏与截断；错误信息对外最小化，对内保留审计但受权限保护。 | 安全设计约束 | 中 |
| T-HTTPJSON-13 | **SEC-HTTPJSON-013（日志安全编码）**：对写入日志的外部输入进行控制字符过滤/转义，避免换行注入与伪造。 | 安全设计约束 | 低 |
| T-HTTPJSON-14 | **SEC-HTTPJSON-014（配置保护）**：配置文件需最小权限（如 600/640）、变更审计；可选：HMAC/签名校验；运行期配置加载失败时默认安全（fail-closed）。 | 安全设计约束 | 高 |
| T-HTTPJSON-15 | **SEC-HTTPJSON-015（指标基数控制）**：指标 tag 必须来自受控集合（如 endpoint 来自配置），不得使用用户输入；必要时对 tag 做截断与规范化。 | 可观测性约束 | 低 |

### 6. 后续行动与验证

| 行动项 | 描述 | 责任方 | 完成标准 |
| --- | --- | --- | --- |
| 1. 更新功能规格 | 将“安全需求与设计约束”纳入该功能的详细设计文档（FS/RS）。 | 架构师/开发 | 安全需求成为设计的组成部分。 |
| 2. 认证与权限设计 | 明确认证方式与最小权限账号策略（库表范围、禁用默认弱口令）。 | 开发/架构 | 设计评审通过，并形成配置/文档。 |
| 3. TLS 部署策略 | 生产强制 HTTPS/mTLS（含反向代理部署说明）。 | 运维/安全 | 部署验收通过，禁止明文入口。 |
| 4. 请求限制与限流 | 限制 body 大小、并发、QPS；对失败认证限流。 | 开发/运维 | 压测与攻击模拟下稳定。 |
| 5. JSONata 超时与资源隔离 | 为 transformation 执行引入超时/隔离机制。 | 开发 | 复杂 payload 下不拖垮服务。 |
| 6. SQL 安全单测 | 针对转义、标识符校验、注入 payload 建立单测/模糊测试。 | 开发/QA | 安全用例通过，无注入成功路径。 |
| 7. dry_run 控制 | 增加开关与权限控制；输出脱敏/截断策略。 | 开发/安全 | 生产默认关闭或仅管理员可用。 |
| 8. 日志与审计 | 日志脱敏、控制字符过滤；关键操作审计（endpoint、账号、来源 IP、request id）。 | 开发/运维 | 安全评审通过；日志可追溯。 |
| 9. 配置文件加固 | 权限、变更审计、可选完整性校验。 | 运维/安全 | 基线脚本/文档到位。 |
| 10. 指标基数治理 | 确认 tag 全部来自配置或受控集合。 | 开发 | 无高基数风险。 |

### 7. 审批意见

| 角色 | 意见 | 签字 | 日期 |
| --- | --- | --- | --- |
| 产品负责人 | 确认理解安全需求的重要性，并将其纳入开发优先级。 | 谭雪峰 | 2025-10-27 |
| 技术负责人 | 确认上述安全约束在技术上是可行且必要的，将在架构设计中落实。 | 谭雪峰 | 2025-10-27 |
| 安全负责人 | 确认威胁分析全面，安全需求是保障该功能顺利上线的关键。 | 霍琳贺 | 2025-10-27 |
