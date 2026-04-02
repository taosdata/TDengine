# Explain analyze 优化 - 威胁建模报告

### 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-02-12 | 2026-02-12 | 1.0 | 霍琳贺 | 初版发布 |

### 2. 基本信息

| 项目信息 | 内容 |
| --- | --- |
| 报告编号 | `TM-TSDB-EXPANA-001` |
| 需求名称 | Explain analyze 优化 |
| 设计文档链接 | docs/releases/TSDB-v3.4.1-[20260331]/05-设计文档/Explain analyze 优化 FS.md |
| 版本编号 | 3.4.1 |
| 业务负责人 | 张天毅 |
| 发起人 | 张天毅 |
| 安全负责人 | 霍琳贺 |
| 参会人员 | 霍琳贺、关胜亮、肖波、张心治、张天毅 |
| 报告日期 | 2026-02-12 |
| 总体评价 | 通过 |

### 3. 分析报告

本报告针对 Explain analyze 优化功能进行了威胁建模分析。该功能旨在增强 TDengine 的 `EXPLAIN ANALYZE` 语句，通过新增算子执行时间分解（计算时间、等待时间）、I/O 代价细分指标、vgroup 性能偏离分析、Exchange 网络指标等诊断信息，帮助用户和交付人员定位查询性能瓶颈。

- 核心发现：共识别出 `5` 个潜在威胁，其中 `高危` `0` 个，`中危` `2` 个，`低危` `3` 个。
- 主要风险场景：
  - 执行计划信息泄露：verbose 模式下输出的 vgroup 拓扑、I/O 分布等信息可能被用于推断集群架构。
  - 资源消耗：`EXPLAIN ANALYZE` 实际执行查询，可被用于探测系统负载能力。
- 结论：该功能为只读诊断能力增强，攻击面极小。设计中已包含多项正向安全决策（删除 endpoint 输出、默认非 verbose 模式、拒绝 DML 支持），整体安全风险低。

### 4. 威胁识别与分析（STRIDE）

`EXPLAIN ANALYZE` 优化功能在现有查询诊断能力基础上，新增了详细的执行时间分解、I/O 代价指标和 vgroup 性能统计。该功能不涉及数据写入，不引入新的通信协议，执行依赖现有 SQL 解析和查询执行链路。

#### 4.1 关键实体与数据

| 实体/数据 | 描述 | 敏感性 |
| --- | --- | --- |
| 算子执行指标 | exec_elapsed、input_wait_elapsed、output_wait_elapsed 等时间分解数据 | 低（系统运行状态） |
| I/O 代价指标 | file_load_blocks、stt_load_blocks、mem_load_blocks 等存储层指标 | 低（存储层统计） |
| vgroup 拓扑信息 | slowest_vgroup_id、slow_deviation_rate、cost_ratio 等 | 中（可推断集群拓扑） |
| Network 指标 | fetch_times、fetch_rows、fetch_cost 等 RPC 通信指标 | 低（通信性能统计） |
| explain_rsp 消息 | 服务端返回的执行计划响应，新增 vgroup_id 字段 | 低（内部协议扩展） |

#### 4.2 威胁评估

| 威胁ID | **威胁描述/攻击场景** | **STRIDE** | **相关组件/数据流** | **风险等级** |
| --- | --- | --- | --- | --- |
| T-EXPANA-01 | 执行计划信息泄露：攻击者通过 `EXPLAIN ANALYZE VERBOSE TRUE` 获取 vgroup 拓扑（slowest_vgroup_id）、I/O 分布模式（各类 block 读取比例）和 vgroup 间性能偏离率，推断集群节点部署架构和数据分布规律。 | I (信息泄露) | verbose 输出 → 客户端 | 中 |
| T-EXPANA-02 | 诊断语句资源消耗：`EXPLAIN ANALYZE` 实际执行目标查询，恶意用户可构造高开销查询并反复执行 `EXPLAIN ANALYZE`，既获取诊断信息又消耗服务端计算和 I/O 资源，等效于查询级别的资源滥用。 | D (拒绝服务) | 客户端 → 查询引擎 → vnode | 中 |
| T-EXPANA-03 | verbose 模式下的数据分布推断：通过分析 `data_deviation_rate`、`total_rows`、`check_rows` 等指标在不同查询条件下的变化，攻击者可间接推断各 vgroup 的数据量分布，用于判断数据倾斜情况或估算业务数据规模。 | I (信息泄露) | verbose 输出指标 | 低 |
| T-EXPANA-04 | 统计指标精度溢出：新增指标涉及除法运算（如 `slow_deviation_rate = (max_time - median_time) / median_time * 100%`、`cost_ratio = max_time / min_time`），当分母为零或极小值时可能产生异常浮点数（Inf/NaN），若未正确处理可能导致客户端解析异常或输出格式紊乱。 | T (篡改) | 算子统计 → 格式化输出 | 低 |
| T-EXPANA-05 | Ratio 参数残留行为：FS 第 4.2 节指出当前 Ratio 参数"不设置时会输出一个错误值"，新设计标注"暂不支持"。若解析层未完全屏蔽该参数，用户传入非预期的 Ratio 值可能触发未定义行为或不一致的采样结果。 | T (篡改) | SQL 解析 → 查询执行 | 低 |

### 5. 安全需求与设计约束

| 威胁ID | **转化后的安全需求/设计约束** | **类型** | **优先级** |
| --- | --- | --- | --- |
| T-EXPANA-01 | SEC-EXPANA-001（权限复用）：`EXPLAIN ANALYZE` 语句的权限校验必须与其包含的目标查询保持一致，确保用户仅能诊断自身有权限执行的查询。当前设计已通过复用查询执行链路实现此约束。 | 安全设计约束 | 中 |
| T-EXPANA-02 | SEC-EXPANA-002（资源管控复用）：`EXPLAIN ANALYZE` 的实际查询执行应受现有查询资源管控机制（如查询超时、并发限制）的约束，不应绕过任何资源限制策略。 | 非功能安全需求 | 中 |
| T-EXPANA-04 | SEC-EXPANA-003（除零保护）：`slow_deviation_rate`、`cost_ratio`、`data_deviation_rate`、`filter_efficiency` 等涉及除法的指标计算，必须对分母为零的边界情况进行保护，输出 `0%` 或 `N/A` 而非异常值。 | 健壮性需求 | 中 |
| T-EXPANA-05 | SEC-EXPANA-004（Ratio 参数处理）：在 Ratio 功能正式实现前，SQL 解析层应正确处理 Ratio 参数——要么拒绝并返回明确错误提示，要么静默忽略并不输出相关信息。禁止输出错误的默认值。 | 健壮性需求 | 低 |

### 6. 后续行动与验证

| 行动项 | 描述 | 责任方 | 完成标准 |
| --- | --- | --- | --- |
| 1. 除零边界保护 | 实现 `slow_deviation_rate`、`cost_ratio`、`data_deviation_rate`、`filter_efficiency` 的除零保护逻辑 | 张天毅 | 单 vgroup、空结果集等边界场景下指标输出正常，无 Inf/NaN |
| 2. Ratio 参数清理 | 确保 Ratio 参数在暂不支持期间被正确处理，不输出错误默认值 | 张天毅 | `EXPLAIN ANALYZE` 不再输出 `Ratio: 0.001000` 等无效信息 |

### 7. 审批意见

| 角色 | 意见 | 签字 | 日期 |
| --- | --- | --- | --- |
| 产品负责人 | 确认该功能安全风险低，诊断信息输出在可接受范围内。 |  |  |
| 技术负责人 | 确认除零保护和 Ratio 参数清理在技术上可行，将在编码阶段落实。 |  |  |
| 安全负责人 | 确认威胁分析覆盖了该只读诊断功能的主要攻击面，安全需求合理。 |  |  |
