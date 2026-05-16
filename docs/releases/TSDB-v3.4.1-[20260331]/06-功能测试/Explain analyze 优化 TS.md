# Explain analyze 优化 TS

## 1. 修订记录

| **编写日期** | **发布日期** | **版本** | **修订人** | **主要修改内容** |
| --- | --- | --- | --- | --- |
| 2026-03-16 | 2026-03-16 | 1.0 | @张天毅 | 基于新增的 explain 测试用例生成测试文档 |

## 2. 测试目标

本文档用于梳理 `test/cases/09-DataQuerying/15-Explain/test_query_explain.py` 中新增的 `test_explain_analyze` 测试用例及相关校验逻辑。
- 覆盖 `explain analyze verbose true` 在多类算子与 SQL 组合场景下的计划输出
- 校验 Explain Plan 中 `cost`、`rows`、`Exec cost`、`Network`、`Filter efficiency` 等关键字段合法性
- 验证窗口、排序分页、聚合、Join、子查询、系统表扫描、插值、动态查询等典型路径
- 回归校验 `partition by tag`、`union + filter` 相关 `Data Exchange` 行为
- 记录本轮提交中移除 `ratio` 场景

## 3. 参考文档

- [Explain analyze 优化 FS](https://taosdata.feishu.cn/wiki/E70aw8Ze2iKcPNkyrqmceAUVnQb)

## 4. 测试结论

本次测试全部通过，增加用例涵盖：
- 从基础 `explain`/`verbose`/`analyze` 回归，扩展到针对 Explain Plan 内容合法性的结构化校验
- 新增 `test_explain_analyze` 入口，合计覆盖 52 个 `explain analyze verbose true` 场景，覆盖窗口、排序、聚合、Join、子查询、系统表、插值、动态查询与分区交换回归路径

## 5. 测试环境

- OS: Linux

## 6. 功能测试

### 6.1 Explain Plan 合法性校验

#### 6.1.1 测试要点

- 统一通过 `__check_explain_plan_rules()` 对计划输出执行规则检查
- 校验 `Filter` 行中的 `efficiency` 百分比存在且位于 `0~100`
- 校验 `cost=A..C` 或 `cost=A(B)..C(D)` 格式合法、非负、非异常大值，且区间单调
- 校验 `rows` 与父子算子层级关系合理，避免父节点 `rows` 大于直接子节点总和（部分能够自主生成数据的算子允许，如 `fill`）
- 校验 `Exec cost` 中 `compute`、`times`、`input_wait`、`output_wait` 字段格式与数值关系
- 校验 `Network` 行带宽/耗时等指标非负且聚合值关系合理

#### 6.1.2 用例列表

`__check_explain_plan_rules()`方法检查内容：

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 过滤效率校验 | 检查 Filter 行包含 `efficiency` 且百分比合法 | 通过 |
| 2 | 成本与行数校验 | 检查 `cost`/`rows` 字段格式、范围和单调关系 | 通过 |
| 3 | 行数层级校验 | 检查父子算子 `rows` 层级关系，兼容 `Data Exchange` 放大系数 | 通过 |
| 4 | 执行成本校验 | 检查 `Exec cost` 字段完整性与均值/最大值关系 | 通过 |
| 5 | 网络成本校验 | 检查 `Network` 相关字段非负且聚合关系合理 | 通过 |

### 6.2 窗口类

#### 6.2.1 测试要点

- 覆盖 interval、state、session、event、count window
- 组合验证 `fill`、`partition by`、`order by`、`limit`、`slimit`、`soffset`
- 针对窗口计划输出执行统一规则校验

#### 6.2.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | interval 基础 | `select _wstart, _wend, count(*) from stb interval(10s)` | 通过 |
| 2 | interval + fill(linear) | 带时间范围与线性填充 | 通过 |
| 3 | interval + sliding | 验证滑动窗口计划 | 通过 |
| 4 | state window | `state_window(c1)` 计划输出 | 通过 |
| 5 | session window | `session(ts, 10s)` 计划输出 | 通过 |
| 6 | event window | `start with` / `end with` 条件计划 | 通过 |
| 7 | count window | `count_window(3)` 基础场景 | 通过 |
| 8 | count window + sliding | `count_window(5, 2)` 组合场景 | 通过 |
| 9 | interval + partition by | 校验分区窗口计划 | 通过 |
| 10 | interval + order/limit | 校验窗口结果排序分页 | 通过 |
| 11 | session + partition by | 校验分区 session window | 通过 |
| 12 | state window + where | 带过滤条件的 state window | 通过 |
| 13 | interval + fill + slimit/soffset | 多特性组合场景 | 通过 |
| 14 | event window + 多聚合 | `count/sum/avg` 组合计划 | 通过 |
| 15 | interval + fill + order + limit | 窗口场景复杂组合回归 | 通过 |

### 6.3 排序与分页

#### 6.3.1 测试要点

- 覆盖超表排序、过滤后排序、`limit/offset`
- 覆盖 `partition by` 后排序限制

#### 6.3.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 超级表 order by ts + limit | `select * from stb order by ts limit 3` | 通过 |
| 2 | where + 多字段排序 + offset | `order by c1 desc, ts asc limit 2 offset 1` | 通过 |
| 3 | partition by + order by + limit | 分区后排序取 TopN | 通过 |

### 6.4 Group By / Having Explain Analyze

#### 6.4.1 测试要点

- 覆盖普通聚合、tag 聚合、where/having/order by 组合
- 验证多聚合函数同时存在时的计划合法性

#### 6.4.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | group by 基础聚合 | `select c1, count(*) from stb group by c1` | 通过 |
| 2 | group by + having | `having count(*) > 1` | 通过 |
| 3 | tag 列聚合 | `select gid, sum(c1) from stb group by gid` | 通过 |
| 4 | where + group by | 过滤后分组聚合 | 通过 |
| 5 | having + order by | 聚合后排序回归 | 通过 |
| 6 | 多聚合函数组合 | `count/sum/avg/min/max` 同时存在 | 通过 |
| 7 | having(sum) | 基于聚合结果过滤 | 通过 |

### 6.5 Join Explain Analyze

#### 6.5.1 测试要点

- 覆盖内连接、左连接、多表连接
- 覆盖 Join + 过滤、Join + 聚合路径

#### 6.5.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 基础 inner join | `ctb1 a join ctb2 b on a.ts = b.ts` | 通过 |
| 2 | join + where | 超表自连接后带过滤 | 通过 |
| 3 | left join | 左连接执行计划 | 通过 |
| 4 | join + group by | Join 后按 `gid` 聚合 | 通过 |
| 5 | 多表 join | 三表串联 Join 回归 | 通过 |

### 6.6 子查询 Explain Analyze

#### 6.6.1 测试要点

- 覆盖 `FROM` 子查询、嵌套子查询
- 覆盖子查询与聚合、排序分页、窗口结合

#### 6.6.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | FROM 子查询 | `select * from (select ts, c1 from stb where c1 > 2)` | 通过 |
| 2 | 子查询 + 聚合 | 子查询产出聚合结果后外层过滤 | 通过 |
| 3 | 嵌套子查询 | 两层 `subquery` 嵌套 | 通过 |
| 4 | 子查询 + 排序分页 | 内层 `order by ... limit` | 通过 |
| 5 | 子查询 + 窗口 | 内层 interval window，外层过滤 | 通过 |

### 6.7 系统表扫描 Explain Analyze

#### 6.7.1 测试要点

- 覆盖 `information_schema` 与 `performance_schema`
- 验证系统表过滤和元数据扫描计划输出

#### 6.7.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | `ins_databases` 扫描 | 查询数据库元信息 | 通过 |
| 2 | `ins_tables` + filter | 按 `db_name` 过滤系统表 | 通过 |
| 3 | `ins_stables` 扫描 | 查询稳定表元信息 | 通过 |
| 4 | `perf_connections` 扫描 | 查询性能连接信息 | 通过 |
| 5 | `ins_dnodes` 扫描 | 查询 dnode 元信息 | 通过 |

### 6.8 插值与 Fill Explain Analyze

#### 6.8.1 测试要点

- 覆盖 `interp()` 与多种 `fill` 策略
- 覆盖 `partition by`、`where`、`range/every` 组合

#### 6.8.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | interp + fill(linear) | 线性插值计划 | 通过 |
| 2 | interp + fill(value, 0) | 常量填充值计划 | 通过 |
| 3 | interp + partition by | 分区插值计划 | 通过 |
| 4 | interp + fill(prev) | 前值填充计划 | 通过 |
| 5 | interp + where + fill(next) | 带过滤的 next 填充 | 通过 |

### 6.9 动态查询与虚拟表 Explain Analyze

#### 6.9.1 测试要点

- 覆盖虚拟稳定表、虚拟子表、动态查询控制路径
- 覆盖聚合、分区、排序分页组合

#### 6.9.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 虚拟稳定表基础查询 | `select * from vstb` | 通过 |
| 2 | 虚拟稳定表聚合 | `select gid, count(*) from vstb group by gid` | 通过 |
| 3 | 虚拟稳定表分区 | `select * from vstb partition by gid` | 通过 |
| 4 | 虚拟子表直接查询 | `select * from vctb1` | 通过 |
| 5 | 虚拟稳定表排序分页 | `select * from vstb order by ts limit 5` | 通过 |

### 6.10 Partition By Tag 回归

#### 6.10.1 测试要点

- 回归验证 `partition by gid` 场景下的 `Data Exchange` 节点数量
- 回归验证 `union + filter` 场景下交换节点分支行为

#### 6.10.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | partition by gid | 断言存在 2 个 `Data Exchange` 节点 | 通过 |
| 2 | union + filter | 断言交换节点数量符合预期 | 通过 |

## 7. 已知问题和限制

- `ratio` 相关 Explain 场景已在本轮提交中移除，原因是代码侧尚未实现
- `test_explain_basic` 中仍保留部分注释掉的未通过场景，本轮文档未将其作为新增覆盖目标
