---
name: tsdb-dev-explain
description: "介绍 TDengine EXPLAIN 的用法，并通过渐进式披露帮助用户分析慢 SQL。适用于 explain 语法、执行计划解读和性能诊断场景。关键词：explain, explain analyze, query_plan, 执行计划, 慢查询"
metadata:
   author: Tony Zhang
   version: 1.0.0
   owner_team: engine
compatibility: "适用于可执行 EXPLAIN 或可提供 QUERY_PLAN 输出的 TDengine 环境"
---

# tsdb-dev-explain

## 何时使用

当用户需要理解或使用 TDengine `EXPLAIN` 时，使用此 Skill，尤其适用于以下场景：

- 解释 `EXPLAIN`、`EXPLAIN ANALYZE`、`VERBOSE true` 的语法和适用时机。
- 解读 `QUERY_PLAN` 树，识别扫描、过滤、排序、聚合、窗口、连接或交换带来的瓶颈。
- 诊断慢 SQL，判断问题更可能出在扫描、过滤、排序、聚合还是网络交换。
- 在用户开始调优前，引导其收集最关键的性能证据。

推荐触发关键词：

- `explain`
- `explain analyze`
- `verbose true`
- `query_plan`
- `执行计划`
- `慢查询诊断`
- `计划树`

激活此 Skill 前，至少确认具备以下输入之一：

- SQL 文本
- `EXPLAIN` 或 `EXPLAIN ANALYZE` 输出
- 用户关于 `EXPLAIN` 用法的问题

## 前置条件

- 当用户需要解读执行计划时，最好能提供 SQL 文本、`EXPLAIN` 输出，或两者都提供。
- 可选但有帮助的信息：TDengine 版本，以及是否已执行 `EXPLAIN ANALYZE VERBOSE true`。

## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-dev-explain version=1.0.0 author=Tony Zhang`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->
