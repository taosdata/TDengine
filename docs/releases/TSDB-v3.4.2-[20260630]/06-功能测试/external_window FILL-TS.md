# 功能测试报告：external_window FILL 功能

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-04-03 | 2026-04-03 | 0.1 | 任新胜 | 新建 external_window FILL 功能测试文档 |

## 2. 测试目标

本测试文档覆盖 external_window FILL 功能的全面验证，确保各 FILL 模式在不同数据分布、分组、HAVING/ORDER BY 和多 vgroup 场景下的行为正确。

- 验证 `NONE/NULL/NULL_F/VALUE/VALUE_F/PREV/NEXT` 七种 FILL 模式的基本功能。
- 验证 `HAVING`、`ORDER BY` 作用在填充后的窗口结果上。
- 验证 `PARTITION BY` 下各 partition 独立填充、不串组。
- 验证 forced/non-forced 差异（全空区间 vs 部分空窗口）。
- 验证伪列（`_wstart/_wend/_wduration`）和窗口属性列（`w.mark`）在填充行中的正确性。
- 验证多 vgroup（merge aligned）路径的填充正确性。
- 验证 `LINEAR/NEAR/SURROUND` 不支持模式的负例报错。
- 验证 HAVING/ORDER BY 引用 SELECT 外聚合函数时 fill 值映射的正确性：当 `HAVING` 或 `ORDER BY` 引用了 SELECT 中不存在的聚合函数（如 `avg(v)`）时，填充值不能出现偏移（找错 slot）。

## 3. 参考文档

- 设计文档：`../05-设计文档/external_window FILL 功能支持 FS.md`
- interval FILL 测试文档：`interval-fill-support-matrix-TS.md`
- 测试脚本：`TDinternal/community/test/cases/13-TimeSeriesExt/08-ExternalWindow/test_external_fill.py`
- PR：`https://github.com/taosdata/TDengine/pull/35021`

## 4. 测试结论

external_window FILL 功能相关测试均已执行通过。测试过程如下：

1. 基础功能验证通过：NONE/NULL/NULL_F/VALUE/VALUE_F/PREV/NEXT 七种模式在单表、分组、多列、空窗口等场景下均行为正确。
2. 边界场景验证通过：首窗空（PREV=NULL）、末窗空（NEXT=NULL）、全部窗口空（forced/non-forced 差异）、数据仅在最后一个窗口的分组键补丁路径。
3. HAVING/ORDER BY 交互通过：填充后的窗口正确参与 HAVING 过滤和 ORDER BY 排序。
4. 多 vgroup 路径通过：merge aligned + fill(value/null/prev) + partition by 均正确。
5. **测试过程中发现并修正了 fill-value-to-column 错位问题**（该问题仅影响 external_window FILL 新实现，interval 不受影响，详见 6.6 节）。

综合结论：external_window FILL 功能基本达到设计预期，可以进入回归。

## 5. 测试环境

- OS: Linux
- Python: 3.10.12
- Test Framework: pytest 8.3.5
- Target Repo: `TDinternal`
- Test Entry: `TDinternal/community/test/cases/13-TimeSeriesExt/08-ExternalWindow/test_external_fill.py`
- 验证命令：`cd TDinternal/community/test && /usr/bin/python3 -m pytest cases/13-TimeSeriesExt/08-ExternalWindow/test_external_fill.py -k "external_fill or fill_value_mismatch" --skip_stop`
- 验证结果：全部通过。

## 6. 功能测试

### 6.1 基础功能验证

#### 6.1.1 测试要点

- 使用 4 个 10 分钟窗口，2 个子表（ext_fill_src_1 有部分数据、ext_fill_src_empty 无数据），验证所有 FILL 模式的基础行为。
- 同时验证 PARTITION BY + PREV 不跨分组。
- 验证窗口属性列 `w.mark` 在填充行中可正常引用。
- 验证 `LINEAR/NEAR/SURROUND` 三种不支持模式的负例报错。

#### 6.1.2 用例列表

| # | 测试用例（helper） | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | `_check_fill_none_basic` | `fill(none)`: 空窗口不输出行 | 通过 |
| 2 | `_check_fill_null_basic` | `fill(null)`: 空窗口输出 NULL | 通过 |
| 3 | `_check_fill_null_force_all_empty` | `fill(null_f)`: 全空表强制输出 4 个窗口 | 通过 |
| 4 | `_check_fill_value_basic` | `fill(value, 999)`: 空窗口填充用户指定值 | 通过 |
| 5 | `_check_fill_value_force_all_empty` | `fill(value_f, 999)`: 全空表强制输出 | 通过 |
| 6 | `_check_fill_prev_basic` | `fill(prev)`: 空窗口使用前一窗口聚合值 | 通过 |
| 7 | `_check_fill_next_basic` | `fill(next)`: 空窗口使用后一窗口聚合值 | 通过 |
| 8 | `_check_fill_prev_next_all_empty` | `fill(prev/next)` 全空表: 不输出结果行 | 通过 |
| 9 | `_check_partition_fill_prev_basic` | `PARTITION BY` + `fill(prev)`: 不跨分组 | 通过 |
| 10 | `_check_fill_mark_reference_basic` | `w.mark` 在填充行中正确引用 | 通过 |
| 11 | `_check_basic_negative_cases` | `LINEAR/NEAR/SURROUND`、fill value 个数不匹配、非聚合 + FILL 报错 | 通过 |

#### 6.1.3 空窗口行为矩阵

4 个窗口（10 分钟），ext_fill_src_1 数据分布：窗口 0 有 2 行、窗口 1 空、窗口 2 有 1 行、窗口 3 有 1 行。

| FILL 模式 | 空窗口 1 行为 | 非空窗口 `sum(v)` |
| --- | --- | --- |
| `NONE` | 不输出 | 正常聚合值 |
| `NULL` | 输出一行, `sum=NULL` | 正常聚合值 |
| `NULL_F` | 全空时也输出 | 正常聚合值 |
| `VALUE(999)` | 输出一行, `sum=999` | 正常聚合值 |
| `VALUE_F(999)` | 全空时也输出 | 正常聚合值 |
| `PREV` | 复制前一非空窗口的 `sum(v)`；若无前值则为 `NULL` | 正常聚合值 |
| `NEXT` | 复制后一非空窗口的 `sum(v)`；若无后值则为 `NULL` | 正常聚合值 |

### 6.2 HAVING 与 ORDER BY 交互

#### 6.2.1 测试要点

- 验证 HAVING 在 FILL 之后执行：填充后的窗口可以满足或不满足 HAVING 条件。
- 验证 `fill(prev)` + `HAVING(sum(v) >= N)`: 空窗口被 PREV 填充后的值参与 HAVING 过滤。
- 验证 `fill(value)` + `HAVING(sum(v) = N)`: VALUE 填充的空窗口可以被 HAVING 精确匹配。
- 验证 `fill(null)` + `HAVING(sum(v) IS NOT NULL)`: NULL 填充的空窗口被过滤掉。
- 验证 multi-column `HAVING` 和 `ORDER BY` 组合。
- 验证 `PARTITION BY` + `HAVING` 在每个 partition 的填充行上独立过滤。

#### 6.2.2 用例列表

| # | 测试用例（helper） | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | `_check_external_fill_having_order` | 6 个 HAVING 场景（prev/value/null + 不同条件） | 通过 |
| 2 | `_check_external_fill_no_partition_column` | 无 PARTITION BY 列时 HAVING 表现 | 通过 |
| 3 | `_check_external_fill_all_empty_force` | 全空 + forced 模式 + HAVING | 通过 |
| 4 | `_check_external_fill_partition_value_no_having` | PARTITION BY + fill(value) 无 HAVING 基线 | 通过 |
| 5 | `_check_fill_mark_reference_having` | `w.mark` + HAVING 组合 | 通过 |

### 6.3 扩展覆盖

#### 6.3.1 测试要点

- 多聚合列 + fill(value) 的多列正确性。
- 连续多个空窗口的 PREV/NEXT 传播。
- PARTITION BY + fill(next) 不跨分组。
- _wstart 在填充行中的正确性。
- fill(none) + PARTITION BY 只输出有数据的窗口。

#### 6.3.2 用例列表

| # | 测试用例（helper） | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | `_check_fill_value_multi_col` | fill(value) 多聚合列 | 通过 |
| 2 | `_check_fill_prev_consecutive_empty` | PREV 连续空窗口传播 | 通过 |
| 3 | `_check_fill_next_consecutive_empty` | NEXT 连续空窗口传播 | 通过 |
| 4 | `_check_fill_partition_next` | PARTITION BY + fill(next) 不跨分组 | 通过 |
| 5 | `_check_fill_wstart_correctness` | _wstart 在填充行中正确 | 通过 |
| 6 | `_check_fill_none_partition` | fill(none) + PARTITION BY | 通过 |

### 6.4 边界场景

#### 6.4.1 测试要点

- 数据仅在最后一个窗口的分组：group key 补丁路径 (`pAnyRow` 搜索) 在窗口末尾才找到数据行。
- PARTITION BY + fill(null) 分组键投影：填充行 `t1` 不为 NULL。
- _wend 在填充行中的正确性（空窗口 vs 有数据窗口）。
- fill(next) 末尾窗口无后续数据 → NULL。
- fill(prev) 首部窗口无前序数据 → NULL。

#### 6.4.2 用例列表

| # | 测试用例（helper） | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | `_check_group_key_only_in_last_window` | 数据仅在末窗口，PARTITION BY 分组键正确 | 通过 |
| 2 | `_check_partition_fill_null_t1_projection` | PARTITION BY + fill(null) t1 不为 NULL | 通过 |
| 3 | `_check_wend_correctness` | _wend 在填充行和数据行均正确 | 通过 |
| 4 | `_check_fill_next_last_window_empty` | fill(next) 末尾空窗口 → NULL | 通过 |
| 5 | `_check_fill_prev_first_window_empty` | fill(prev) 首部空窗口 → NULL | 通过 |

### 6.5 多 vgroup（Merge Aligned）路径

#### 6.5.1 测试要点

- 4 vgroup 数据库，3 个子表分布在不同 vgroup，测试 merge aligned external window + FILL。
- fill(value) + PARTITION BY: 各分组窗口的填充值正确、`w.mark` 正确。
- fill(null) + PARTITION BY: 无数据分组(t1=3)不物化。
- fill(prev) 单表路径：在多 vgroup 数据库中仍正确传播。

#### 6.5.2 用例列表

| # | 测试用例（helper） | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | `_check_multi_vgroup_fill` (value) | fill(value, 888) + PARTITION BY 跨 4 vgroup | 通过 |
| 2 | `_check_multi_vgroup_fill` (null) | fill(null) + PARTITION BY 跨 4 vgroup | 通过 |
| 3 | `_check_multi_vgroup_fill` (prev) | fill(prev) 单表查询跨 4 vgroup | 通过 |

### 6.6 开发过程中发现的实现问题

#### 6.6.1 external_window + FILL + HAVING/ORDER BY 回归

在本次 external_window FILL 功能开发过程中，发现 external_window + FILL + HAVING/ORDER BY 组合在新实现联调阶段存在一个 fill-value-to-column 映射缺陷：

- **正确语义**：对于 `external_window(...) FILL(...)` 查询，HAVING / ORDER BY 中额外引入但未出现在 SELECT 列表中的聚合函数，不应打乱 fill value 到目标聚合列的映射顺序。

已增加回归 case 并修复。

**现象**：

```sql
SELECT sum(v) as s1, avg(v) as s2
FROM src
EXTERNAL_WINDOW(... fill(value, 888, 999))
HAVING(avg(v) IS NOT NULL OR avg(v) IS NULL)
```

修正前空窗口结果会出现 fill value 映射偏移，修正后可按投影顺序正确落到目标聚合列。

**修正方案**：

1. **planLogicCreater.c**：在 `rewriteExprsForSelect` 之前从 `pSelect->pProjectionList` 构建 `pFillExprs`，保持与 Parser fill-value 映射相同的顺序。
2. **planPhysiCreater.c**：通过 `nodesEqualNode` 将逻辑 `pFillExprs` 条目与 `pFuncs` 匹配，获取对应的已分配 slot 的物理节点。

**修正状态**：已修正并验证。

**回归测试**：新增 `test_fill_value_mismatch_regression`，覆盖以下场景：

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | `_check_fill_value_having_extra_agg` | fill(value) + HAVING avg(v)（不在 SELECT 中）| 通过 |
| 2 | `_check_fill_value_order_by_extra_agg` | fill(value) + ORDER BY avg(v)（不在 SELECT 中）| 通过 |
| 3 | `_check_fill_value_f_having_extra_agg` | fill(value_f) + HAVING avg(v)（不在 SELECT 中）| 通过 |

## 7. 易用性测试

不涉及。

## 8. 长期稳定性测试

无。

## 9. 性能测试

无。本次聚焦功能正确性验证。external_window FILL 的实现复用 ExternalWindowOperator 内部逻辑，未引入额外 FillOperator 节点。

## 10. 安全性测试

无。

## 11. 兼容性测试

- external_window 不带 FILL 时行为不变（`FILL(NONE)` 为默认）。
- 历史 external_window 用例在默认配置下通过。
- 该功能只影响查询结果，不落盘，无数据格式升级风险。

## 12. 已知问题和限制

- `LINEAR`、`NEAR`、`SURROUND` 不支持，语义层报错。
- `PARTITION BY` 下完全缺席的分组不物化，force 模式也不补出该分组（与 interval 行为一致）。
- fill-value-to-column 错位问题已修正（见 6.6.1）。该问题仅影响 external_window FILL 新实现，interval FILL 使用独立的 `collectFillExprs` 路径不受影响。
- `_wend` 在空窗口上的值为 `endtime + 1`（空窗口的 `tw.ekey` 初始化为 `wend + 1`），数据窗口为 `endtime`。这是当前实现的正常行为，已在边界用例 `_check_wend_correctness` 中覆盖。
- `fill(prev)` / `fill(next)` 在“整表无数据”的 external_window 基础用例中不输出结果行；仅在存在可借用前值/后值的窗口集合内，空窗口才会得到传播值。

## 13. 测试用例总览

| # | test method | 覆盖维度 | helper 数 | 状态 |
| --- | --- | --- | --- | --- |
| 1 | `test_external_fill_basic` | 7 种 FILL 模式基础 + 负例 | 14 | 通过 |
| 2 | `test_external_fill_having_order` | HAVING + PARTITION BY + ORDER BY | 5 | 通过 |
| 3 | `test_external_fill_extended` | 多列、连续空窗、_wstart、partition | 6 | 通过 |
| 4 | `test_external_fill_edge_cases` | group key 补丁、_wend、PREV/NEXT 边界 | 5 | 通过 |
| 5 | `test_external_fill_multi_vgroup` | 多 vgroup merge aligned 路径 | 1（含 3 子场景）| 通过 |
| 6 | `test_fill_value_mismatch_regression` | fill-value 错位问题回归 | 3 | 通过 |
