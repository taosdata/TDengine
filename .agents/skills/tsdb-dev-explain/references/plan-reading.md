# TDengine EXPLAIN 执行计划解读

当用户提供了 SQL、`QUERY_PLAN`，或者明确是在分析慢查询时，读取此参考文档。

## 固定阅读顺序

建议按以下顺序阅读执行计划：

1. 扫描
2. 过滤与裁剪
3. 结果形态变换算子
4. 聚合或窗口
5. 数据交换
6. 运行时指标

## 常见算子

### 扫描

- `Table Scan on ...`
- `Tag Scan on ...`
- `Virtual Table Scan on ...`
- `Table Merge Scan on ...`
- `Last Row Scan on ...`

需要回答的问题：

- 扫描范围是否比预期更大？
- 查询是否没有走更有选择性的路径？
- 是否出现了意料之外的多表归并成本？

### 过滤与裁剪

重点关注：

- `Primary Filter`
- `Tag Index Filter`
- `Time Range`

解读规则：

- 如果 SQL 里存在很强的过滤条件，但在底层节点看不到，可能没有成功下推。
- 如果出现 `Tag Index Filter`，通常说明 tag 条件参与了裁剪。
- 如果 `Time Range` 比预期宽很多，通常意味着扫描被放大了。

### 结果形态变换

重点关注：

- `Projection`
- `Sort`
- `Group Sort`
- `Merge`

解读规则：

- 显式 `Sort` 往往意味着额外的 CPU 和内存开销。
- `Group Sort` 在分组或分区输出路径里尤其关键。
- `Merge` 常见于多个输入流或局部结果的合并阶段。

### 聚合与窗口

重点关注：

- `Aggregate`
- `GroupAggregate`
- `Interval`
- `Session`
- `StateWindow`
- `Event`
- `External`

解读规则：

- 先区分普通聚合和带分组键的聚合。
- 解读窗口算子时，要结合对应的时间或状态语义。
- 如果用户反馈行数不对或时延异常，要确认成本究竟在窗口算子本身，还是在其上游扫描和交换阶段。

### 数据交换

重点关注：

- `Data Exchange`
- `Network`

解读规则：

- 即使各个扫描节点看起来不重，分布式交换仍可能主导整体时延。
- 如果 `fetch cost` 或 `network mode` 明显不理想，要明确指出跨节点数据移动可能是问题来源。

## 运行时指标

当出现 `ANALYZE` 时，优先解释这些字段：

- `rows=`
- `cost=first..last`
- `Exec cost:`
- `I/O cost`

快速判断：

- `rows=` 很高且裁剪效果差，通常意味着扫描放大。
- 如果 `Sort` 或 `Exchange` 上的 `cost` / `Exec cost` 很高，瓶颈通常不只是底层扫描。
- 对扫描型负载来说，`I/O cost` 往往非常关键。

## 输出风格

解读执行计划时，建议按以下顺序输出：

1. 一句话总结瓶颈
2. 从根到叶列出关键算子
3. 指出最强的证据字段
4. 给出一到两个具体下一步动作
