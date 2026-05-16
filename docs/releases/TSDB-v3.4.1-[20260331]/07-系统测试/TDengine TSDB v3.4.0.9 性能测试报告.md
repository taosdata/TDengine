# TDengine TSDB v3.4.0.9 性能测试报告

### 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-03-02 | 2026-03-03 | 1.0 | 贾靖斌 | 编写 3.4.0.9 性能测试报告 |

### 2. 概述

本报告旨在验证 TDengine TSDB 3.4.0.9 版本在标准性能基准下的表现。测试通过 TestNG_V3 性能框架，分别在“无缓存（None）”和“最新行缓存（Lastrow）”两种缓存模型下，对数据写入及多种复杂查询场景进行压力测试，以评估系统的吞吐量及响应时延。

### 3. 测试范围与环境

#### 3.1 测试范围

测试涵盖了时序数据库典型的业务操作路径：
- **数据写入性能**：采用 STMT 接口进行多表并发写入测试。
- **查询性能回归**：涵盖简单查询（Lastrow、OrderBy）、范围查询（Day/Week）、标签/列过滤查询、聚合计算（Count/Avg/Percentile）、高级插值（Interp）、嵌套查询及多表关联查询（Join/Union）。

#### 3.2 测试环境

- **硬件配置**：

| **硬件环境** | **IP** | 用途 | **CPU** | **内存** | **硬盘** |
| --- | --- | --- | --- | --- | --- |
| 192.168.1.54 | client | SAMSUNG MZ7L31T9 1.7T |
| 192.168.1.43 | taosd | SAMSUNG MZ7L31T9 1.7T |

- **软件配置**：
  - 操作系统：Linux x64
  - 数据库版本：TDengine TSDB Enterprise 3.4.0.9
  - 测试工具：TestNG_V3 (Python 驱动)

#### 3.3 缓存模型定义

- **None 模式**：禁用查询缓存，强制从磁盘读取数据，测试系统的原始 IO 与计算能力。
- **Lastrow 模式**：启用针对最新行数据的缓存优化，模拟实时监控、看板轮询等高频访问场景。

### 4. 测试执行摘要

本次测试通过对两条核心基准命令的执行，对比了不同缓存配置下的系统表现：

|  | **描述** | **测试项数量** | **结论** |
| --- | --- | --- | --- |
| **benchamrk_query_none** | 无缓存查询模型 | 1项写入 + 19项查询 | 达标 |
| **benchamrk_query_lastrow** | 最新行缓存模型 | 1项写入 + 19项查询 | 达标 |

### 5. 测试结果

http://192.168.0.204:3000/d/f39f4b6c-7243-44ee-817f-a8a52b5fe516/baseline-all?orgId=1&refresh=1m&var-base_type=release&var-base_label=3.4.0.0&var-target_type=release&var-target_label=3.4.0.9

### 6. **详细测试项说明**

#### 6.1 **写入测试场景 (Scenarios)**

*   **场景 ID**: W10001 / W10002
*   **测试模型**: meters (超级表) STMT 写入
*   **验证点**: 验证在不同缓存策略干扰下，系统持续大并发写入的吞吐能力。

#### 6.2 **查询测试项 (Query Cases)**

针对 None 和 Lastrow 组，分别执行了以下 19 类典型 SQL：

| **测试 ID** | **查询场景描述** | **示例 SQL 逻辑** |
| --- | --- | --- |
| **Q10001** | 最新行单列查询 | select last_row(current) from test.${tbname} |
| **Q10002** | 最新行全列查询 | select last_row(*) from test.${tbname} |
| **Q10003** | 排序取极限值 | select * from test.${tbname} order by ts limit 1 |
| **Q20001** | 指定天范围查询 | select * from test.${tbname} where ts >= '2022-10-01 00:00:00.000' and ts <= '2022-10-02 00:00:00.000' |
| **Q20002** | 指定天单列查询 | select ts,current from test.${tbname} where ts >= '2022-10-01 00:00:00.000' and ts <= '2022-10-02 00:00:00.000' |
| **Q20003** | 指定周单列查询 | select ts,current from test.${tbname} where ts >= '2022-10-01 00:00:00.000' and ts <= '2022-10-08 00:00:00.000' |
| **Q20004** | 标签过滤查询 | select ts, current, voltage from test.meters where location = '${location}' |
| **Q20005** | 列过滤查询 | select ts, current, voltage from test.${tbname} where voltage=99 |
| **Q30001** | 百分位数计算 | select percentile(current, 0.2) from test.${tbname} |
| **Q30002** | 近似百分位数计算 | select APERCENTILE(current, 50) from test.${tbname} |
| **Q30003** | 跨表聚合计算 | select tbname, count(*),sum(current), avg(voltage) from test.${tbname} |
| **Q30004** | 时间窗口插值 | select interp(current) from test.${tbname} range('2022-10-01 00:00:00.000','2022-10-02 00:00:00.000') every(10s) fill(linear) |
| **Q40001** | 分区最后值查询 | select last(*) from test.meters partition by tbname slimit 10 |
| **Q50001** | 复杂嵌套查询 | select abs(max_a - min_a ) from (select max(a) max_a,min(b) min_a from ( select last(current) as a, avg(current) as b from test.d1 group by tbname)) |
| **Q60001** | 内关联查询 | select * from test.d1 as t1 inner join test.d2 as t2 on t1.voltage = t2.voltage and t1.ts = t2.ts |
| **Q60002** | 左关联查询 | select * from test.d1 as t1 left join test.d2 as t2 on t1.voltage = t2.voltage and t1.ts = t2.ts and t1.ts >= '2022-10-01 00:00:00.000' and t1.ts < '2022-10-01 00:00:05.000' |
| **Q60003** | 右关联查询 | select * from test.d0 as t1 right join test.d1 as t2 on t1.voltage = t2.voltage and t1.ts = t2.ts and t2.ts in ('2022-10-01 00:00:00.000','2022-10-01 00:00:30.000') |
| **Q60004** | 全关联查询 | select * from test.d0 as t1 full join test.d1 as t2 on t1.voltage = t2.voltage and t1.ts = t2.ts and t1.ts > '2022-10-01 00:00:00.000' and t1.ts < '2022-10-01 00:00:02.000' |
| **Q60005** | 联合查询 (Union) | select * from test.d1 union all select * from test.d2 union all select * from test.d3 |

### 7. **版本性能对比分析 (v3.4.0.0 vs v3.3.8.0)**

根据 Grafana 性能对比看板（Baseline-All）数据分析，TDengine TSDB v3.4.0.0.9在核心性能指标上相对于 v3.4.0.0 表现稳健，整体呈现稳中有升的趋势。

#### 7.1 **写入性能对比分析**

- **吞吐量 (Throughput)**: v3.4.0.9 整体写入吞吐量与 v3.4.0.0 持平。
- **延迟 (Latency)**: 写入平均延迟维持在毫秒级，在高负载压力下，v3.4.0.9 的延迟表现与 v3.4.0.0 基本相当。

#### 7.2 **查询性能对比分析**

- **标签列过滤投影查询性能 (Q20004)**: 3.4.0.9 (1197.38 QPS) 相比 3.4.0.0 (1066.40 QPS) 提升约 **12.28%**。
- **最新行查询**: 在 lastrow 模式下 QPS 表现优秀，满足实时业务需求。
- **条件过滤查询**: 普通列和标签列条件过滤查询，性能均较 3.4.0.0 有改善。

### 8. ** 测试结论**

- **策略有效性**：条件过滤场景的优化达到预期。
- **版本迭代优势**：对比 v3.4.0.0，新版本在联合查询上有稳步提升。
- **读写均衡性**：在高并发读写混合压力下，响应曲线依然平滑。
- **性能基准达标**：全量 19 项查询及写入基准均达标，符合安可发布要求。

**最终结论：TDengine TSDB v3.4.0.9 性能表现优于 v3.4.0.0 历史基准，建议发布。**

**审批人**：肖波（测试负责人）
