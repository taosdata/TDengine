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

> 调用 `skill-telemetry`，传入 `name=tsdb-dev-explain version=0.1.1 author=Tony Zhang`。
> 如果 skill-telemetry 不可用，跳过并继续正常执行，无需通知用户。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

## 渐进式披露

不要一开始就加载全部参考资料；只有当前回答确实需要时，才补充读取额外 reference。

只有当用户问题同时涉及多个处理路径（如既需要语法介绍又需要性能诊断）时，才同时加载多个 reference；不要在不同场景下重复加载无关文件。

## 输入

| 参数 | 必需 | 默认值 | 说明 |
|------|:----:|--------|------|
| `goal` | ✅ | — | 用户目标：语法讲解、计划解读、性能诊断 |
| `sql` | ❌ | — | 目标 SQL；做计划解释或性能分析时强烈建议提供 |
| `query_plan` | ❌ | — | 已有 `EXPLAIN` / `EXPLAIN ANALYZE` 输出 |
| `version_hint` | ❌ | 最新稳定版（3.4.x），如无法确定则明确告知用户版本可能影响结果 | 用户关心版本差异、语法兼容性时提供 |

如果缺少关键信息，优先追问最小必要输入：

- 纯语法问题：通常无需追问。
- 执行计划解读：优先索要 SQL 或粘贴的 `QUERY_PLAN`。
- 性能诊断：如条件允许，优先索要 `EXPLAIN ANALYZE VERBOSE true <sql>` 输出；如果 taos 客户端折叠了长结果，可提示用户在 SQL 末尾、分号前追加 `\G` 获取完整输出。

## 执行步骤

1. **识别问题类型**

   选择一个主要处理路径：

   - 语法介绍
   - 执行计划解读
   - 性能诊断

2. **先读取官方手册，再按需加载参考文档**

   - 语法介绍：优先查看 `docs/zh/14-reference/03-taos-sql/28-explain.md`；如果用户没有源码、无法访问该路径，可在用户同意且网络可用时尝试查看官网文档 `https://docs.taosdata.com/reference/taos-sql/explain/`；如果网络不可用或用户不同意，再读取 `references/quickstart.md`
   - 执行计划解读或性能诊断：优先查看 `docs/zh/14-reference/03-taos-sql/28-explain.md`；如果用户没有源码、无法访问该路径，可在用户同意且网络可用时尝试查看官网文档 `https://docs.taosdata.com/reference/taos-sql/explain/`；如果网络不可用或用户不同意，再读取 `references/plan-reading.md`
   - 如果用户需要分步骤的诊断流程：在上述基础上补充读取 `references/performance-workflow.md`

3. **提炼关键语义点后再开始回答**

   先确认语法、输出字段和 `ANALYZE` / `VERBOSE` 的含义，再结合 reference 文件组织回答结构和诊断顺序。

4. **返回最小但完整的答案**

   - 语法类问题：说明语法、差异和少量示例
   - 执行计划类问题：指出主要算子、瓶颈和一两个下一步动作
   - 性能类问题：给出可能瓶颈、关键证据字段和一两个调优方向

## 输出

根据用户问题，产出以下其中一种结果：

- **语法指南**：简明说明 `EXPLAIN`、`ANALYZE`、`VERBOSE` 的区别、适用场景和示例 SQL。
- **执行计划解读报告**：按算子解释计划，指出可能瓶颈，并给出一到两个优化建议。
- **性能诊断说明**：给出最可能的瓶颈、最强证据，以及下一条最值得尝试的 SQL 或调优动作。

质量要求：

- 不要把 `EXPLAIN` 仅泛化为数据库通用能力，必须结合 TDengine 的输出字段来解释。
- 优先依据官方文档和可见的 `QUERY_PLAN` 字段，而不是主观猜测。
- 明确区分静态计划解读与运行时分析。
- 默认只回答与当前问题直接相关的信息，并优先保留结论、关键证据和下一步动作；仅在用户明确要求更多细节时再展开。

## Examples

**用户说：** "介绍一下 TDengine explain 的用法"

**Agent 行为：**
- 解释 `EXPLAIN`、`EXPLAIN ANALYZE`、`VERBOSE true` 的区别
- 给出 2 到 3 条符合 TDengine 风格的示例

**用户说：** "帮我看这条 SQL 为什么慢，这是 explain analyze verbose true 的输出"

**Agent 行为：**
- 优先识别扫描、过滤、排序、交换和运行时成本信号
- 如果输出被 taos 客户端折叠，提示用户在 SQL 末尾、分号前加 `\G`
- 给出最可能的瓶颈和最小下一步动作

**用户说：** "我只有 SQL，还没跑 explain，应该怎么分析性能问题"

**Agent 行为：**
- 优先索要成本最低、最有用的缺失证据
- 引导用户执行最小必要的 `EXPLAIN` 命令，并说明重点关注什么；如结果过长被折叠，可在 SQL 末尾、分号前加 `\G`

## 目录提示

- `references/quickstart.md` 保存语法和快速入门示例。
- `references/plan-reading.md` 保存算子和指标解读规则。
- `references/performance-workflow.md` 保存面向用户的慢查询诊断流程。
- `docs/zh/14-reference/03-taos-sql/28-explain.md` 是解释语法和输出字段时的最终依据。
- `https://docs.taosdata.com/reference/taos-sql/explain/` 可作为用户无源码时的官方文档回退来源。
- 当前版本不需要 `scripts/` 和 `assets/`。

## 安全约束

- 禁止读取 `.env`、私钥、令牌或无关的敏感文件。
- 禁止执行破坏性 SQL 或 shell 命令，禁止执行DDL、DML 或其他可能修改数据的操作，只能执行只读查询。
- 在没有引用可见 `QUERY_PLAN` 字段的情况下，不要武断判断索引命中、谓词下推或网络瓶颈。
- 需要访问官网文档时，若用户不同意或当前环境无法联网，则不要继续尝试网络访问，应回退到本 Skill 自带的 reference 文件。
- Scope 限制：仅分析 TDengine 官方文档，以及用户提供的与 `EXPLAIN` 相关的 SQL 或 `QUERY_PLAN` 内容。
