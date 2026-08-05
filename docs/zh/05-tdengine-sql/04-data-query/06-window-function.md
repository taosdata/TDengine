---
sidebar_label: 窗口函数
title: 窗口函数
description: OVER 子句与 SQL 标准窗口函数说明
---

自 `v3.4.2.0` 起，TDengine 支持 SQL 标准的 `OVER` 子句和窗口函数。窗口函数为结果集中的**每一行**计算一个值，计算时既能看到当前行，也能看到同一窗口内的其他行，但**不会把多行合并成一行**。这一点与 [特色查询](./04-distinguished.md) 中的时间窗口（`INTERVAL`、`STATE_WINDOW`、`SESSION` 等）不同：时间窗口把窗口内的多行聚合成一行输出，而窗口函数保留原始的每一行，只是额外附加一列计算结果。

窗口函数适合移动平均、运行总计、分区排名、前后值对比等分析场景，常见于报表和 BI 工具自动生成的 SQL。

## 概念

一次窗口函数调用由两部分组成：

```sql
function_name ( [ arguments ] ) OVER ( window_spec | window_name )
```

- **window_spec**：内联窗口规范，直接写在 `OVER (...)` 括号内。
- **window_name**：引用由 `WINDOW` 子句定义的 [命名窗口](#命名窗口)。

窗口规范描述了如何为当前行确定参与计算的行集合，由三个可选部分组成：

```sql
window_spec:
    [ PARTITION BY expr [, ...] ]
    [ ORDER BY expr [ ASC | DESC ] [ NULLS FIRST | NULLS LAST ] [, ...] ]
    [ frame_clause ]
```

- **PARTITION BY**：把输入结果集按一个或多个表达式切分成互不影响的**分区**，每个分区独立计算。省略时整个结果集视为一个分区。
- **ORDER BY**：在分区内按一个或多个表达式排序，支持 `ASC` / `DESC`（默认 `ASC`）和 `NULLS FIRST` / `NULLS LAST`。排序决定了序号、前后值、累计区间等与顺序相关的语义。
- **frame_clause**：窗口帧，进一步在排序后的分区内界定当前行参与计算的行范围，详见 [窗口帧](#窗口帧)。

窗口函数只能出现在查询块的 `SELECT` 列表和 `ORDER BY` 中，不能出现在 `WHERE`、`GROUP BY`、`HAVING`、`PARTITION BY` 或窗口帧边界表达式里。如需基于窗口结果再做过滤或聚合，请把窗口查询写成子查询，在外层引用其输出列。

## 窗口帧

窗口帧用于在排序后的分区内，为当前行界定一个更小的行范围（例如「最近 3 行」「前后各 1 行」「当前行之前的全部行」）。帧由帧单位和上下边界组成：

```sql
frame_clause:
    { ROWS | RANGE } frame_extent

frame_extent:
    frame_bound
  | BETWEEN frame_bound AND frame_bound

frame_bound:
    UNBOUNDED PRECEDING
  | expr PRECEDING
  | CURRENT ROW
  | expr FOLLOWING
  | UNBOUNDED FOLLOWING
```

- **帧单位**
  - `ROWS`：按**物理行数**界定，`expr` 是非负整数行数。
  - `RANGE`：按 `ORDER BY` 值的**距离**界定，`CURRENT ROW` 包含所有与当前行排序值相等的行（peer rows）。
- **省略 `BETWEEN`** 的简写形式只指定起点，终点默认为 `CURRENT ROW`。例如 `ROWS 10 PRECEDING` 等价于 `ROWS BETWEEN 10 PRECEDING AND CURRENT ROW`。
- 五种边界：`UNBOUNDED PRECEDING`（分区起点）、`expr PRECEDING`（当前行之前）、`CURRENT ROW`（当前行）、`expr FOLLOWING`（当前行之后）、`UNBOUNDED FOLLOWING`（分区终点）。
- 当窗口边界超出分区范围时，只使用分区内实际存在的行。

### 默认窗口帧

未显式指定帧时，按如下规则确定默认帧：

| 场景 | 默认帧 |
| --- | --- |
| 没有 `ORDER BY` | 整个分区（`ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`） |
| 有 `ORDER BY`，聚合类窗口函数 | `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`（从分区起点到当前行及其 peer rows） |
| 有 `ORDER BY`，序号 / 分布 / 取值类窗口函数 | 这些函数不依赖帧，按整个分区的排序结果计算 |

### RANGE 帧的约束

- 带数值或时间**偏移**（`expr PRECEDING` / `expr FOLLOWING`）的 `RANGE` 帧，只允许**一个** `ORDER BY` 表达式；多个排序表达式时报错。
- 偏移类型必须与排序列类型匹配：
  - 排序列为时间戳类型时，偏移必须是 TDengine 时间长度写法，如 `10s PRECEDING`、`1m PRECEDING`。
  - 排序列为数值类型（整数或浮点，不含 `UNSIGNED BIGINT`）时，偏移必须是非负整数。
- 不带偏移的 `RANGE`（仅 `CURRENT ROW`、`UNBOUNDED PRECEDING` / `FOLLOWING`）按 peer 语义计算，允许多个 `ORDER BY` 表达式，按全部排序键判定 peer rows。
- 排序列为字符串、布尔等无法解释范围距离的类型时，带偏移的 `RANGE` 报错。

## 命名窗口

当一条查询中多个窗口函数共用同一套窗口规范时，可以用 `WINDOW` 子句给规范命名，再通过 `OVER window_name` 引用，避免重复书写：

```sql
SELECT
    AVG(voltage) OVER win AS ma,
    MAX(voltage) OVER win AS mx
FROM meters
WINDOW win AS (PARTITION BY tbname ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
ORDER BY ts;
```

`WINDOW` 子句位于查询语句尾部，可定义多个命名窗口，用逗号分隔。命名窗口遵循以下规则：

- 命名窗口只在定义它的查询块内有效，不会泄漏到外层或内层查询块。
- `OVER window_name` 必须**完整引用**命名窗口的定义，不能在引用处追加或覆盖 `PARTITION BY`、`ORDER BY`、帧等子句。
- 不支持通过一个命名窗口继承另一个命名窗口。
- 引用未定义的窗口名称、在同一查询块内重复定义同名窗口，都会返回明确错误。

## 窗口函数列表

窗口函数分为两类：一类是既有的聚合 / 选择函数，加上 `OVER` 子句即可作为窗口函数使用；另一类是本特性新增的专用窗口函数。

### 聚合与选择类窗口函数

下列既有函数在带 `OVER` 子句时可作为窗口聚合使用，对当前行所在的窗口帧求值。空值处理、数值精度和类型推导沿用其普通聚合语义。

| 函数 | 说明 |
| --------------------- | --- |
| `COUNT(expr)`         | 窗口帧内的非空行数 |
| `SUM(expr)`           | 窗口帧内的求和 |
| `MIN(expr)`           | 窗口帧内的最小值 |
| `MAX(expr)`           | 窗口帧内的最大值 |
| `AVG(expr)`           | 窗口帧内的平均值 |
| `PERCENTILE(expr, p)` | 窗口帧内的百分位数 |
| `FIRST(expr)`         | 窗口帧内的第一个非空值 |
| `LAST(expr)`          | 窗口帧内的最后一个非空值 |
| `LAST_ROW(expr)`      | 窗口帧内最后一行的值，不忽略空值 |

这些函数都不要求指定 `ORDER BY`：未指定时窗口覆盖整个分区；指定 `ORDER BY` 但未指定帧时，默认覆盖从分区起点到当前行（含 peer rows）。

### 序号类窗口函数

序号类函数依赖排序结果，**必须**指定 `ORDER BY`，且忽略窗口帧。

| 函数 | 返回类型 | 说明 |
| -------------- | -------- | --- |
| `ROW_NUMBER()` | `BIGINT` | 当前行在分区内的行号，从 1 开始，同值行也严格递增 |
| `RANK()`       | `BIGINT` | 当前行的名次，同值行（peer rows）名次相同，之后名次跳过并列的数量 |
| `DENSE_RANK()` | `BIGINT` | 当前行的名次，同值行名次相同，之后名次不跳过 |

### 分布类窗口函数

分布类函数依赖排序结果，**必须**指定 `ORDER BY`，且忽略窗口帧。

| 函数 | 返回类型 | 说明 |
| ---------------- | -------- | --- |
| `PERCENT_RANK()` | `DOUBLE` | 相对名次，`(rank - 1) / (分区行数 - 1)`；分区只有一行时返回 0 |
| `CUME_DIST()`    | `DOUBLE` | 累计分布，小于等于当前行排序值的行数 / 分区行数 |

### 取值类窗口函数

取值类函数依赖排序结果，**必须**指定 `ORDER BY`。

| 函数 | 返回类型 | 说明 |
| ----------------------------------- | --- | --- |
| `LAG(expr [, offset [, default]])`  | 同 `expr` | 当前行**之前**第 `offset` 行的 `expr` 值 |
| `LEAD(expr [, offset [, default]])` | 同 `expr` | 当前行**之后**第 `offset` 行的 `expr` 值 |
| `FIRST_VALUE(expr)`                 | 同 `expr` | 当前窗口帧内第一行的 `expr` 值 |
| `LAST_VALUE(expr)`                  | 同 `expr` | 当前窗口帧内最后一行的 `expr` 值 |
| `NTH_VALUE(expr, n)`                | 同 `expr` | 当前窗口帧内第 `n` 行的 `expr` 值，`n` 从 1 开始 |

`LAG` / `LEAD` 参数说明：

- `offset`：偏移行数，省略时默认为 `1`；作为窗口函数使用时必须 ≥ `0`（`offset` 为 `0` 表示当前行）。
- `default`：目标行不存在时返回的值，需与 `expr` 类型兼容；未指定时返回 `NULL`。
- `NTH_VALUE` 的 `n` 必须 ≥ `1`；当第 `n` 行不存在时返回 `NULL`。

:::note
`LAG` / `LEAD` 也可以**不带** `OVER` 子句使用，此时按输入结果集的行序计算，详见 [LAG](./03-function.md#lag)。两种用法的参数规则略有差异：不带 `OVER` 时 `offset` 必须为大于 `0` 的整数，带 `OVER` 时 `offset` 可以为 `0`。
:::

## 使用限制

- 窗口函数只能出现在查询块的 `SELECT` 列表和 `ORDER BY` 中。出现在 `WHERE`、`GROUP BY`、`HAVING`、`PARTITION BY`、窗口帧边界表达式，或作为标量函数参数、另一个窗口函数的窗口规范时，均返回错误。
- 不允许窗口函数嵌套，即一个窗口函数的参数或窗口规范中不能再出现窗口函数。
- 顺序敏感的窗口函数（序号类、分布类、取值类）未指定 `ORDER BY` 时返回错误。
- 当前仅支持批查询，不支持在流式计算中使用窗口函数。
- 排序中 `NULL` 默认按最低值处理；同值行的输出顺序由 `ORDER BY` 决定，需要稳定逐行顺序时应显式补充排序键。

### 配合 OFFSET 跳过预热期

使用固定长度窗口计算移动指标时，前若干行往往没有足够的历史数据。可以在窗口计算完成后用 `OFFSET N` 跳过前 N 行结果。`OFFSET` 在窗口值计算完成后生效，不会改变已算出的窗口值。

自 `v3.4.2.0` 起，`OFFSET N` 可以脱离 `LIMIT` 独立使用：

```sql
SELECT v, AVG(v) OVER (ORDER BY ts ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS ma
FROM meters
ORDER BY ts
OFFSET 9;
```

## 示例

以下示例基于文档统一使用的智能电表数据模型（超级表 `meters`，包含 `ts`、`current`、`voltage`、`phase` 列和 `location`、`groupid` 标签）。

**移动平均**：计算每个电表最近 10 次采样的平均电压。

```sql
SELECT tbname, ts, voltage,
       AVG(voltage) OVER (PARTITION BY tbname ORDER BY ts ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS ma
FROM meters
ORDER BY tbname, ts;
```

**运行总计**：计算每个电表电流的累计值。

```sql
SELECT tbname, ts, current,
       SUM(current) OVER (PARTITION BY tbname ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
FROM meters
ORDER BY tbname, ts;
```

**分区排名**：在每个分组内按电压降序排名。

```sql
SELECT groupid, tbname, voltage,
       ROW_NUMBER() OVER (PARTITION BY groupid ORDER BY voltage DESC) AS rn,
       RANK()       OVER (PARTITION BY groupid ORDER BY voltage DESC) AS rk,
       DENSE_RANK() OVER (PARTITION BY groupid ORDER BY voltage DESC) AS drk
FROM meters;
```

**前后值对比**：计算每个电表相邻采样的电压差。

```sql
SELECT tbname, ts, voltage,
       LAG(voltage)           OVER (PARTITION BY tbname ORDER BY ts) AS prev_v,
       voltage - LAG(voltage) OVER (PARTITION BY tbname ORDER BY ts) AS delta
FROM meters
ORDER BY tbname, ts;
```

**时间范围窗口**：统计每行前 10 秒内（按时间值）的电压总和。

```sql
SELECT tbname, ts, voltage,
       SUM(voltage) OVER (PARTITION BY tbname ORDER BY ts RANGE BETWEEN 10s PRECEDING AND CURRENT ROW) AS sum_10s
FROM meters
ORDER BY tbname, ts;
```

**命名窗口 + 子查询过滤**：先在子查询中计算移动平均，再在外层过滤出高于移动平均的行。

```sql
SELECT tbname, ts, voltage, ma
FROM (
    SELECT tbname, ts, voltage,
           AVG(voltage) OVER win AS ma
    FROM meters
    WINDOW win AS (PARTITION BY tbname ORDER BY ts ROWS BETWEEN 9 PRECEDING AND CURRENT ROW)
) t
WHERE voltage > ma
ORDER BY tbname, ts;
```
