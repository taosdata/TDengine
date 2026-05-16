# OPC 超级表 Schema 兼容性设计文档

## 概述

当 taosx 升级导致 OPC 超级表的 tag 定义发生变化时（例如新版本将 Property 子节点的值作为父节点的 tag），旧数据库中已有的超级表 schema 可能与新版本期望的 schema 不一致。本文档描述 taosx 如何在不修改已有超级表 schema 的前提下，正确创建新子表并写入数据。

## 问题背景

### 新旧版本 tag 差异

以 OPC UA 的 `opc_float` 超级表为例：

| 版本 | tag 数量 | tag 列 |
|------|----------|--------|
| 旧版 | 5 | `point_id`, `id`, `path`, `device`, `point_name` |
| 新版 (PR #3871+) | 11 | 上述 5 个 + `EURange`, `EngineeringUnits`, `EnumValues`, `Measurement_Unit_ID`, `SecondLanguageDescription`, `ValuePrecision` |

新版将 Property 子节点的值合并为父 DynamicVariable 的 tag，而旧版将它们作为独立子表。

### 不兼容的三阶段故障链

当新版 taosx 运行旧任务时，会触发以下连锁故障：

```
阶段 1: handle_point_message_init
  CREATE TABLE IF NOT EXISTS 使用 11 tag 建表
  → 超级表已存在（5 tag）→ IF NOT EXISTS 跳过
  → 306 个新 OPC 节点的子表未创建（11 tag 与 5 tag 不匹配）
  → warn 日志，继续

阶段 2: consume_point_record
  批量 INSERT INTO 语句包含已有表和新表
  → 新表不存在 → 0x2603 (table not exist) → 整批 INSERT 失败
  → 同批次中的已有表（如 bb1）也无法写入（"连坐"）

阶段 3: 错误处理
  → CREATE STABLE (11 tag) → 0x0360 (stable already exists) → ignore
  → CREATE TABLE ... USING stable (11 tag) TAGS (11 values)
  → 0x2600 (tag 数量不匹配) → retry 5 次 → 放弃
```

**影响**：新子表永远无法创建，同批次中的已有表持续无法写入。

## 解决方案：自适应 Tag Schema

### 设计原则

**不修改已有超级表的 schema**（不执行 `ALTER STABLE ADD TAG`），而是让子表建表语句**适配**超级表的实际 tag 列表。

### 集合分析

设 **C** = taosx config 中的 tag 集合（新版本生成的），**S** = 超级表实际的 tag 集合（`DESCRIBE` 返回的）。

| 集合关系 | 场景举例 | 处理策略 |
|----------|----------|----------|
| **C ⊃ S**（C 包含 S） | 当前问题：C={11 tag}，S={5 tag}，S ⊂ C | S 中每个 tag 都能从 C 找到值 → 全填真实值 |
| **C ⊂ S**（S 包含 C） | stable 被手动 ALTER ADD TAG | C 有的填值，S 多出来的填 NULL |
| **C ∩ S ≠ ∅，互不包含** | 版本间 tag 有增有删 | 交集部分填值，S 有但 C 没有的填 NULL |
| **C = S** | 正常场景 | 一一对应，全填值 |
| **C ∩ S = ∅** | 极端情况 | S 的 tag 全填 NULL |

**统一策略**：以 **S（stable 实际 tag）为准**构建 `CREATE TABLE ... USING` 语句。遍历 S 中的每个 tag：

- 该 tag 在 C 中存在 → 使用 C 提供的值
- 该 tag 在 C 中不存在 → 填 NULL

C 中有但 S 中没有的 tag → 丢弃。

### 修改位置

仅修改 `consume_point_record` 的**错误处理阶段**（`taosx-core/src/plugins/sink/mod.rs`，`0x2603` 错误处理分支）。

**不修改** `handle_point_message_init` 阶段。原因：init 阶段的 `CREATE TABLE IF NOT EXISTS` 失败只打 `warn`，不影响后续流程。当首次 INSERT 触发 `0x2603` 时，错误处理阶段会正确建表并 retry。新子表只需经历一次"INSERT 失败 → 建表 → retry 成功"，之后不会再失败。

### 修复后流程

```
INSERT batch → 0x2603 (table not exist)
  → CREATE STABLE (n tags from config)
    → 0x0360 (stable already exists) → ignore
  → DESCRIBE stable → 获取实际 tag 列表 S (m 个 tag)
  → 对每个子表的建表 SQL：
      解析原始 (tag_names) TAGS (tag_values) → Vec<(name, value)>
      以 S 为准重建：遍历 S 中的 tag，按名称从原始值中匹配（无匹配则 NULL）
      生成: CREATE TABLE `子表` USING `stable` (`tag_s1`, ..., `tag_sm`) TAGS (v1, ..., vm)
  → 执行 CREATE TABLE → 成功
  → retry INSERT → 成功
```

### 关键约束

- 按**名称**匹配 tag，不按位置（顺序可能不同）
- 缺失的 tag 值填 NULL，确保 SQL 语法正确
- DESCRIBE 失败时回退到使用 config 原始 tag（走现有错误处理路径）
- Fast path：当 config tag 和 stable tag 完全一致时，跳过适配

## 实现

### 辅助函数

```rust
// 解析 "(`tag1`, `tag2`) TAGS (val1, val2)" → Vec<(name, value)>
fn parse_child_table_create_sql(sql: &str) -> Vec<(String, String)>

// 以 actual_tag_names 为准，从 config_tags 中按名称匹配取值，重建建表 SQL
fn rebuild_child_table_create_sql(
    actual_tag_names: &[String],
    config_tags: &[(String, String)],
) -> String
```

### 核心逻辑（`0x0360` 处理后）

```rust
// 当 CREATE STABLE 返回 0x0360 (stable already exists):
// 1. DESCRIBE stable 获取实际 tag 列表
let actual_tag_names: Option<Vec<String>> = match taos.describe(&stable_name).await {
    Ok(desc) => {
        let tags: Vec<String> = desc.iter()
            .filter(|c| c.is_tag())
            .map(|c| c.field().to_string())
            .collect();
        if tags.is_empty() { None } else { Some(tags) }
    }
    Err(_) => None, // 回退到 config 原始 tag
};

// 2. 对每个子表，适配建表 SQL
for (child_table_name, child_table_create_sql) in child_table_create_sql_map {
    let adapted_sql = if let Some(ref actual_tags) = actual_tag_names {
        let config_tags = parse_child_table_create_sql(child_table_create_sql);
        rebuild_child_table_create_sql(actual_tags, &config_tags)
    } else {
        child_table_create_sql.clone()
    };
    // 使用 adapted_sql 建表
}
```

## 源码位置

| 文件 | 函数/位置 | 说明 |
|------|-----------|------|
| `taosx-core/src/plugins/sink/mod.rs` | `consume_point_record` 0x2603 分支 | 自适应建表逻辑 |
| `taosx-core/src/plugins/sink/mod.rs` | `parse_child_table_create_sql` | 解析建表 SQL 中的 tag name-value 对 |
| `taosx-core/src/plugins/sink/mod.rs` | `rebuild_child_table_create_sql` | 以实际 stable tag 为准重建建表 SQL |
| `taosx-core/src/plugins/sink/mod.rs` | `parse_backtick_names` | 解析反引号括起的名称列表 |
| `taosx-core/src/plugins/sink/mod.rs` | `parse_sql_values` | 解析可能含逗号的 SQL 值列表 |
