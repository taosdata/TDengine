---
title: Window Functions
description: The OVER clause and SQL standard window functions
---

Starting from v3.4.1.0, TDengine supports the SQL standard `OVER` clause and window functions. A window function computes a value for **each row** in the result set, and during the computation it can see both the current row and other rows in the same window, but it does **not** collapse multiple rows into one. This differs from the time windows (`INTERVAL`, `STATE_WINDOW`, `SESSION`, etc.) described in [Time-Series Extensions](./24-distinguished.md): a time window aggregates multiple rows in the window into a single output row, while a window function keeps every original row and only appends a computed column.

Window functions are well suited for analytical scenarios such as moving averages, running totals, partitioned rankings, and comparisons with neighboring rows. They are commonly seen in SQL generated automatically by reporting and BI tools.

## Concepts

A window function call consists of two parts:

```sql
function_name ( [ arguments ] ) OVER ( window_spec | window_name )
```

- **window_spec**: an inline window specification written directly inside the `OVER (...)` parentheses.
- **window_name**: a reference to a [named window](#named-windows) defined by the `WINDOW` clause.

The window specification describes how the set of rows participating in the computation is determined for the current row. It has three optional parts:

```sql
window_spec:
    [ PARTITION BY expr [, ...] ]
    [ ORDER BY expr [ ASC | DESC ] [ NULLS FIRST | NULLS LAST ] [, ...] ]
    [ frame_clause ]
```

- **PARTITION BY**: splits the input result set into independent **partitions** by one or more expressions; each partition is computed independently. When omitted, the entire result set is treated as a single partition.
- **ORDER BY**: orders rows within a partition by one or more expressions, supporting `ASC`/`DESC` (default `ASC`) and `NULLS FIRST`/`NULLS LAST`. The ordering determines order-sensitive semantics such as row numbers, neighboring values, and cumulative ranges.
- **frame_clause**: the window frame, which further bounds the range of rows participating in the computation for the current row within the ordered partition. See [Window Frame](#window-frame).

Window functions may only appear in the `SELECT` list and `ORDER BY` of a query block. They are not allowed in `WHERE`, `GROUP BY`, `HAVING`, `PARTITION BY`, or frame boundary expressions. To filter or aggregate on window results, write the window query as a subquery and reference its output columns in the outer query.

## Window Frame

A window frame bounds a smaller set of rows for the current row within the ordered partition (for example, "the last 3 rows", "one row before and after", or "all rows before the current row"). A frame consists of a frame unit and its lower and upper bounds:

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

- **Frame unit**:
  - `ROWS`: bounds by **physical row count**; `expr` is a non-negative integer row count.
  - `RANGE`: bounds by the **distance** of `ORDER BY` values; `CURRENT ROW` includes all rows whose ordering value equals that of the current row (peer rows).
- The shorthand form that **omits `BETWEEN`** specifies only the start bound; the end bound defaults to `CURRENT ROW`. For example, `ROWS 10 PRECEDING` is equivalent to `ROWS BETWEEN 10 PRECEDING AND CURRENT ROW`.
- Five bounds: `UNBOUNDED PRECEDING` (partition start), `expr PRECEDING` (before the current row), `CURRENT ROW` (the current row), `expr FOLLOWING` (after the current row), `UNBOUNDED FOLLOWING` (partition end).
- When a frame bound exceeds the partition range, only the rows actually present in the partition are used.

### Default Window Frame

When no frame is specified explicitly, the default frame is determined by the following rules:

| Scenario | Default frame |
|----------|---------------|
| No `ORDER BY` | The entire partition (`ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`) |
| With `ORDER BY`, aggregate-class window functions | `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` (from the partition start to the current row and its peer rows) |
| With `ORDER BY`, ranking/distribution/value-class window functions | These functions do not depend on a frame; they are computed over the ordered result of the entire partition |

### RANGE Frame Constraints

- A `RANGE` frame with a numeric or time **offset** (`expr PRECEDING`/`expr FOLLOWING`) allows only **one** `ORDER BY` expression; multiple ordering expressions cause an error.
- The offset type must match the ordering column type:
  - When the ordering column is a timestamp type, the offset must use TDengine time-duration notation, such as `10s PRECEDING` or `1m PRECEDING`.
  - When the ordering column is a numeric type (integer or float, excluding `UNSIGNED BIGINT`), the offset must be a non-negative integer.
- A `RANGE` frame without an offset (only `CURRENT ROW`, `UNBOUNDED PRECEDING`/`FOLLOWING`) is computed by peer semantics and allows multiple `ORDER BY` expressions, determining peer rows by all ordering keys.
- When the ordering column is a string, boolean, or other type for which a range distance cannot be interpreted, a `RANGE` frame with an offset causes an error.

## Named Windows

When multiple window functions in a query share the same window specification, you can name the specification with the `WINDOW` clause and reference it via `OVER window_name` to avoid repetition:

```sql
SELECT
    avg(voltage) OVER win AS ma,
    max(voltage) OVER win AS mx
FROM meters
WINDOW win AS (PARTITION BY tbname ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
ORDER BY ts;
```

The `WINDOW` clause is placed at the end of the query statement and can define multiple named windows separated by commas. Named windows follow these rules:

- A named window is valid only within the query block that defines it; it does not leak into outer or inner query blocks.
- `OVER window_name` must reference the named window's definition **as a whole**; you cannot append or override `PARTITION BY`, `ORDER BY`, or the frame at the reference site.
- Inheriting one named window from another is not supported.
- Referencing an undefined window name, or defining the same window name more than once in the same query block, returns an explicit error.

## Window Function List

Window functions fall into two categories: existing aggregate/selection functions that can be used as window functions by adding an `OVER` clause, and dedicated window functions newly added by this feature.

### Aggregate and Selection Window Functions

The following existing functions can be used as window aggregates when given an `OVER` clause, evaluating over the window frame of the current row. Null handling, numeric precision, and type inference follow their regular aggregate semantics.

| Function | Description |
|----------|-------------|
| `count(expr)` | Number of non-null rows in the window frame |
| `sum(expr)` | Sum over the window frame |
| `min(expr)` | Minimum value in the window frame |
| `max(expr)` | Maximum value in the window frame |
| `avg(expr)` | Average over the window frame |
| `percentile(expr, p)` | Percentile over the window frame |
| `first(expr)` | First non-null value in the window frame |
| `last(expr)` | Last non-null value in the window frame |
| `last_row(expr)` | Value of the last row in the window frame, not ignoring nulls |

None of these functions require `ORDER BY`: when omitted, the window covers the entire partition; when `ORDER BY` is given but no frame is specified, the window defaults to covering from the partition start to the current row (including peer rows).

### Ranking Window Functions

Ranking functions depend on the ordering result, **require** `ORDER BY`, and ignore the window frame.

| Function | Return Type | Description |
|----------|-------------|-------------|
| `row_number()` | BIGINT | The row number of the current row within the partition, starting from 1; strictly increasing even for peer rows |
| `rank()` | BIGINT | The rank of the current row; peer rows share the same rank, and subsequent ranks skip by the number of ties |
| `dense_rank()` | BIGINT | The rank of the current row; peer rows share the same rank, and subsequent ranks do not skip |

### Distribution Window Functions

Distribution functions depend on the ordering result, **require** `ORDER BY`, and ignore the window frame.

| Function | Return Type | Description |
|----------|-------------|-------------|
| `percent_rank()` | DOUBLE | Relative rank, `(rank - 1) / (partition rows - 1)`; returns 0 when the partition has only one row |
| `cume_dist()` | DOUBLE | Cumulative distribution, `number of rows whose ordering value <= the current row / partition rows` |

### Value Window Functions

Value functions depend on the ordering result and **require** `ORDER BY`.

| Function | Return Type | Description |
|----------|-------------|-------------|
| `lag(expr [, offset [, default]])` | Same as `expr` | The `expr` value of the row `offset` rows **before** the current row |
| `lead(expr [, offset [, default]])` | Same as `expr` | The `expr` value of the row `offset` rows **after** the current row |
| `first_value(expr)` | Same as `expr` | The `expr` value of the first row in the current window frame |
| `last_value(expr)` | Same as `expr` | The `expr` value of the last row in the current window frame |
| `nth_value(expr, n)` | Same as `expr` | The `expr` value of the `n`-th row in the current window frame, `n` starting from 1 |

Parameter notes for `lag`/`lead`:

- `offset`: the row offset, defaulting to 1 when omitted. When used as a window function, it must be `>= 0` (`offset` 0 means the current row).
- `default`: the value returned when the target row does not exist; it must be type-compatible with `expr`. If omitted, `NULL` is returned.
- For `nth_value`, `n` must be `>= 1`; when the `n`-th row does not exist, `NULL` is returned.

:::note
`lag`/`lead` can also be used **without** an `OVER` clause, in which case they are evaluated on the row order of the input result set. See [Sequential Analysis Functions](./22-function.md#lag). The parameter rules differ slightly: without `OVER`, `offset` must be an integer greater than 0; with `OVER`, `offset` may be 0.
:::

## Usage Restrictions

- Window functions may only appear in the `SELECT` list and `ORDER BY` of a query block. They return an error when they appear in `WHERE`, `GROUP BY`, `HAVING`, `PARTITION BY`, frame boundary expressions, or as an argument to a scalar function or another window function's specification.
- Window functions cannot be nested; a window function's arguments or window specification cannot contain another window function.
- Order-sensitive window functions (ranking, distribution, and value classes) return an error when `ORDER BY` is not specified.
- The current version supports batch queries only; window functions are not supported in stream processing.
- In ordering, `NULL` is treated as the lowest value by default. The output order of peer rows is determined by `ORDER BY`; add explicit ordering keys when a stable row-by-row order is required.

### Skipping the Warm-up Period with OFFSET

When computing moving metrics with a fixed-length window, the first few rows often lack enough history. You can use `OFFSET N` to skip the first N result rows after the window computation is complete. `OFFSET` takes effect after the window values are computed and does not change the already-computed window values.

Starting from this version, `OFFSET N` can be used independently without `LIMIT`:

```sql
SELECT v, avg(v) OVER (ORDER BY ts ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS ma
FROM meters
ORDER BY ts
OFFSET 9;
```

## Examples

The following examples are based on the smart meter data model used throughout the TDengine documentation (supertable `meters`, with columns `ts`, `current`, `voltage`, `phase` and tags `location`, `groupid`).

**Moving average**: compute the average voltage of the last 10 samples for each meter.

```sql
SELECT tbname, ts, voltage,
       avg(voltage) OVER (PARTITION BY tbname ORDER BY ts ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS ma
FROM meters
ORDER BY tbname, ts;
```

**Running total**: compute the cumulative current for each meter.

```sql
SELECT tbname, ts, current,
       sum(current) OVER (PARTITION BY tbname ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
FROM meters
ORDER BY tbname, ts;
```

**Partitioned ranking**: rank by voltage in descending order within each group.

```sql
SELECT groupid, tbname, voltage,
       row_number() OVER (PARTITION BY groupid ORDER BY voltage DESC) AS rn,
       rank()       OVER (PARTITION BY groupid ORDER BY voltage DESC) AS rk,
       dense_rank() OVER (PARTITION BY groupid ORDER BY voltage DESC) AS drk
FROM meters;
```

**Neighboring value comparison**: compute the voltage difference between adjacent samples for each meter.

```sql
SELECT tbname, ts, voltage,
       lag(voltage) OVER (PARTITION BY tbname ORDER BY ts) AS prev_v,
       voltage - lag(voltage) OVER (PARTITION BY tbname ORDER BY ts) AS delta
FROM meters
ORDER BY tbname, ts;
```

**Time range window**: sum the voltage within the preceding 10 seconds (by time value) of each row.

```sql
SELECT tbname, ts, voltage,
       sum(voltage) OVER (PARTITION BY tbname ORDER BY ts RANGE BETWEEN 10s PRECEDING AND CURRENT ROW) AS sum_10s
FROM meters
ORDER BY tbname, ts;
```

**Named window + subquery filtering**: compute the moving average in a subquery, then filter rows above the moving average in the outer query.

```sql
SELECT tbname, ts, voltage, ma
FROM (
    SELECT tbname, ts, voltage,
           avg(voltage) OVER win AS ma
    FROM meters
    WINDOW win AS (PARTITION BY tbname ORDER BY ts ROWS BETWEEN 9 PRECEDING AND CURRENT ROW)
) t
WHERE voltage > ma
ORDER BY tbname, ts;
```
