---
title: Join Queries
---

## Join Concepts

### Driving Table

The table that drives the join query. In the Left Join series, the left table is the driving table, and in the Right Join series, the right table is the driving table.

### Join Conditions

Join conditions specify which rows from the two inputs can match. Joins that match rows by a condition must specify that condition explicitly. An inner join can also be written as `FROM a, b WHERE condition`; for ordinary row-level conditions, this returns the same matching row pairs as `FROM a INNER JOIN b ON condition`. A Window Join uses `WINDOW_OFFSET` to specify its time window.

Use explicit `CROSS JOIN` or comma syntax for an unconditional join; both return the Cartesian product of the two inputs. `ASOF JOIN` is the exception that allows `ON` to be omitted: without it, the default time-matching condition is used. Write `ON` to specify another supported time-matching condition or grouping conditions.

JOIN conditions must be evaluable as Boolean expressions. Whole `JSON` values, `BLOB`, and `MEDIUMBLOB` cannot be used as equality match keys. Aggregate and window functions cannot be written directly in `ON`, but columns computed by a subquery can participate in a join.

ASOF JOIN's main condition is based on primary timestamps and supports the specified comparison operators or `TIMETRUNCATE` expressions. Tags and ordinary columns can only be used as equality grouping conditions. See [Left/Right ASOF Join](#leftright-asof-join) for details. WINDOW JOIN specifies its time range through `WINDOW_OFFSET`; see its section for other condition restrictions.

### Grouping Conditions

The characteristic ASOF/Window Join of time-series databases supports grouping the input data of join queries and then performing join operations within each group. Grouping restricts which rows can match; it is not GROUP BY aggregation. Equality conditions that appear after `ON` in ASOF/Window Join (except for ASOF's main join condition) will be considered as grouping conditions.

### Primary Key Timeline

A timeline identifies the time column and ordering used for time-related operations. A table's primary timestamp column provides its timeline. Whether a subquery or JOIN result has a usable timeline depends on its output columns and ordering.

Starting with `v3.4.3.0`, ordinary JOINs do not require a primary timestamp column in the join condition. The optimizer selects an applicable algorithm based on the condition and input order. A subquery that outputs no time column can still join on business columns.

Timelines still apply to subsequent timeline functions and window queries. Hash and Theta do not guarantee time-ordered output, so a successful JOIN does not imply that its result meets those requirements. Retain the necessary time column and follow the [timeline rules in Data Querying](./01-query.md#timeline) for subsequent time-related operations. Use ORDER BY for ordinary result sorting.

## Conditions Supported by Ordinary Joins

Starting with `v3.4.3.0`, ordinary joins no longer require primary timestamp equality in the join condition. You can join by business columns, expressions, ranges, or combined Boolean conditions. ASOF JOIN and WINDOW JOIN retain their respective time-matching rules.

| Condition | Example | Behavior |
| --- | --- | --- |
| Business-column equality | `a.groupId = b.groupId` | No additional `a.ts = b.ts` is required |
| Separate expressions | `a.voltage + 1 = b.voltage` | Each expression references its respective input; result types must be compatible |
| Equality referencing both inputs | `a.voltage + b.groupId = b.voltage` | The complete condition determines matches |
| Range | `a.voltage < b.voltage` | An equality condition is not required |
| Combined Boolean conditions | `a.voltage = b.voltage OR a.current > b.current` | The complete condition determines matches, preserving SQL three-valued logic |
| One-sided or constant conditions | `a.voltage > 220`, `TRUE`, `FALSE` | Only TRUE matches; outer joins still preserve unmatched rows |

The examples use smart-meter data: the `meters` supertable has columns `ts TIMESTAMP`, `current FLOAT`, `voltage INT`, and `phase FLOAT`, and tags `groupId INT` and `location BINARY(64)`. Tables `d1001` and `d1002` are its subtables. Create and populate these tables before running the queries. Result order is not guaranteed without `ORDER BY`.

### Timestamp Equality with Additional Conditions

The existing timestamp-based join syntax remains valid. This query compares voltages of rows with matching timestamps:

```sql
SELECT a.ts, a.voltage, b.voltage
FROM d1001 a JOIN d1002 b
ON a.ts = b.ts AND a.voltage > b.voltage;
```

Additional conditions can contain `OR`. This left join preserves every row from `d1001`; right-side columns are populated only when both the timestamp and date conditions match:

```sql
SELECT a.ts, b.ts, b.voltage
FROM d1001 a LEFT JOIN d1002 b
ON a.ts = b.ts
AND (a.ts < '2025-01-01 00:00:00.000'
     OR a.ts >= '2025-01-02 00:00:00.000');
```

### Joining at a Time Granularity with TIMETRUNCATE

Truncate timestamps to seconds and return all row pairs within the same second where the left current exceeds the right current:

```sql
SELECT a.ts, b.ts, a.current, b.current
FROM d1001 a JOIN d1002 b
ON TIMETRUNCATE(a.ts, 1s) = TIMETRUNCATE(b.ts, 1s)
AND a.current > b.current;
```

### Equality on Business Columns and Expressions

Starting with `v3.4.3.0`, joins can match business columns without an additional primary timestamp equality condition. This query matches records with the same group and location, then compares voltages. Matching rows can have different timestamps:

```sql
SELECT a.ts, b.ts, a.voltage, b.voltage
FROM meters a JOIN meters b
ON a.groupId = b.groupId AND a.location = b.location
AND a.voltage > b.voltage;
```

Expressions can reference their respective inputs separately, or the complete condition can reference both inputs:

```sql
-- The left voltage plus 1 equals the right voltage
SELECT a.ts, b.ts, a.voltage, b.voltage
FROM d1001 a LEFT JOIN d1002 b
ON a.voltage + 1 = b.voltage;

-- Compare after explicit casts
SELECT a.ts, b.ts
FROM d1001 a JOIN d1002 b
ON CAST(a.voltage AS BIGINT) = CAST(b.voltage AS BIGINT);

-- An equality referencing both inputs; the system selects an applicable algorithm
SELECT a.ts, b.ts
FROM meters a JOIN meters b
ON a.voltage + b.groupId = b.voltage;
```

### Range and Combined Boolean Conditions

These queries contain no equality condition. They match using a voltage range and a combined Boolean condition, respectively:

```sql
SELECT a.ts, b.ts, a.voltage, b.voltage
FROM d1001 a JOIN d1002 b
ON a.voltage BETWEEN b.voltage - 5 AND b.voltage + 5;

SELECT a.ts, b.ts
FROM d1001 a JOIN d1002 b
ON a.voltage < b.voltage OR a.current > b.current;
```

### ON and WHERE in INNER JOIN

For these ordinary row-level conditions, both forms return the same row pairs:

```sql
SELECT a.ts, b.ts
FROM d1001 a INNER JOIN d1002 b
ON a.voltage = b.voltage AND a.current > 10;

SELECT a.ts, b.ts
FROM d1001 a, d1002 b
WHERE a.voltage = b.voltage AND a.current > 10;
```

This equivalence does not apply to outer joins. The first query below preserves all left-side rows and fills right-side columns with NULL when no row matches. The second query filters out those unmatched rows:

```sql
SELECT a.ts, b.ts, b.voltage
FROM d1001 a LEFT JOIN d1002 b
ON a.ts = b.ts AND b.voltage > 220;

SELECT a.ts, b.ts, b.voltage
FROM d1001 a LEFT JOIN d1002 b
ON a.ts = b.ts
WHERE b.voltage > 220;
```

### One-Sided Conditions, Constants, and Unconditional Joins

One-sided ON conditions are also evaluated for row pairs. In the first query, left-side rows with voltage greater than 220 match every right-side row; unmatched left-side rows receive NULL values on the right. The second query has no matches: all left-side rows are preserved, with NULL in every right-side column:

```sql
SELECT a.ts, b.ts
FROM d1001 a LEFT JOIN d1002 b ON a.voltage > 220;

SELECT a.ts, b.ts
FROM d1001 a LEFT JOIN d1002 b ON FALSE;
```

Use `CROSS JOIN` or comma syntax for an unconditional join. Both queries return the complete Cartesian product:

```sql
SELECT a.ts, b.ts FROM d1001 a CROSS JOIN d1002 b;
SELECT a.ts, b.ts FROM d1001 a, d1002 b;
```

Ordinary `JOIN` still requires `ON`. `CROSS JOIN` does not accept `ON`; use `WHERE` to filter its results.

### Joining Subqueries Without Time Columns

Neither aggregate subquery below outputs a time column, but they can still join on `groupId`. A successful ordinary JOIN does not imply that its result can be used directly by subsequent timeline functions or window queries:

```sql
SELECT a.groupId, a.avg_voltage, b.avg_current
FROM (SELECT groupId, AVG(voltage) AS avg_voltage
      FROM meters GROUP BY groupId) a
JOIN (SELECT groupId, AVG(current) AS avg_current
      FROM meters GROUP BY groupId) b
ON a.groupId = b.groupId;
```

Broader condition support does not remove existing type or input-source
restrictions. See the following ASOF/WINDOW sections for their examples and
specific rules.

## Join Algorithms and Hints

The following ordinary JOIN algorithm selection and JOIN_ALGO Hint are supported starting with `v3.4.3.0`. Theta supports the newly available range and general Boolean join conditions.

The optimizer normally selects the algorithm without a Hint. Algorithm selection preserves the join type, complete condition, and duplicate-row semantics. It does not guarantee output order without ORDER BY.

| Algorithm | Applicable conditions | Main costs |
| --- | --- | --- |
| Merge | Supported timestamp equality expressions with appropriately ordered inputs | Additional sorting when needed; subquery ORDER BY/LIMIT semantics remain unchanged |
| Hash | Compatible equality keys, including supported expressions | Build-side memory, key computation, and duplicate-key candidate matching |
| Theta | Range or general Boolean conditions without a usable equality key, and unconditional joins | Checks input row pairs; runtime and memory costs can be high for large inputs |

Filtering out unnecessary data first can reduce join input and matching costs. For tuning, inspect the algorithm with EXPLAIN and compare execution on representative data.

Place `/*+ JOIN_ALGO(algorithm) */` after SELECT. The algorithm parameter is HASH, MERGE, or THETA:

```sql
EXPLAIN SELECT /*+ JOIN_ALGO(HASH) */ a.ts, b.ts
FROM meters a JOIN meters b ON a.groupId = b.groupId;

EXPLAIN SELECT /*+ JOIN_ALGO(MERGE) */ a.ts, b.ts
FROM meters a JOIN meters b ON a.ts = b.ts;

EXPLAIN SELECT /*+ JOIN_ALGO(THETA) */ a.ts, b.ts
FROM meters a JOIN meters b ON a.voltage < b.voltage;
```

- A Hint expresses an algorithm preference. An inapplicable Merge preference can fall back to Hash; without usable equality keys, execution can fall back to Theta. Check EXPLAIN for the actual algorithm rather than inferring it from the Hint.
- The existing parameterless `HASH_JOIN` form corresponds to `JOIN_ALGO(HASH)`. The parameterless `MERGE_JOIN` shorthand, introduced in `v3.4.3.0`, corresponds to `JOIN_ALGO(MERGE)`. Use the unified `JOIN_ALGO` form for new SQL.
- ASOF/WINDOW retain their time-matching rules regardless of the algorithm Hint.

For general syntax and other options, see [Hints in Data Querying](./01-query.md#hints).

## Syntax Explanation

The following sections describe Left and Right Join together. In expressions such as "left/right", the part before `/` applies to Left Join and the part after `/` applies to Right Join.

For example:

- "left/right table" means the left table for Left Join and the right table for Right Join.
- "right/left table" means the right table for Left Join and the left table for Right Join.

## Join Features

### Inner Join

#### Definition

Inner Join returns all left/right row pairs that satisfy the join condition. A row can match multiple rows from the other input, and each matching pair is returned.

#### Syntax

```sql
SELECT ... FROM table_name1 [INNER] JOIN table_name2 ON ... [WHERE ...] [...]
or
SELECT ... FROM table_name1 CROSS JOIN table_name2 [WHERE ...] [...]
or
SELECT ... FROM table_name1, table_name2 [WHERE ...] [...]
```

#### Result Set

All left/right row pairs that satisfy the join condition.

#### Applicable Scope

Supports Inner Join between supertables, basic tables, subtables, virtual tables, and subqueries.

#### Explanation

- For the first syntax, the `INNER` keyword is optional and `ON` is required to specify the join condition. `WHERE` filters rows after the Join.
- `CROSS JOIN` and comma syntax represent unconditional joins. Without `WHERE`, they return the complete Cartesian product; with `WHERE`, they filter that product.
- Tag equality conditions can be combined with other equality conditions. Specify ORDER BY when ordered results are required.

#### Example

Moments when both table d1001 and table d1002 have voltages greater than 220V and their respective voltage values:

```sql
SELECT a.ts, a.voltage, b.voltage FROM d1001 a JOIN d1002 b ON a.ts = b.ts AND a.voltage > 220 AND b.voltage > 220;
```

### Left/Right Outer Join

#### Definition

Left/Right (Outer) Join returns all matching row pairs and preserves unmatched rows from the left/right table.

#### Syntax

```sql
SELECT ... FROM table_name1 LEFT|RIGHT [OUTER] JOIN table_name2 ON ... [WHERE ...] [...]
```

#### Result Set

The Inner Join result, plus unmatched rows from the left/right table with `NULL` values for columns from the right/left table.

#### Applicable Scope

Supports Left/Right Join between supertables, basic tables, subtables, virtual tables, and subqueries.

#### Explanation

- OUTER keyword is optional.

#### Example

Return every timestamp and voltage from `d1001`. Include the voltage from `d1002` only when the timestamps match and both voltages exceed 220V:

```sql
SELECT a.ts, a.voltage, b.voltage FROM d1001 a LEFT JOIN d1002 b ON a.ts = b.ts AND a.voltage > 220 AND b.voltage > 220;
```

### Left/Right Semi Join

#### Definition

Left/Right Semi Join - Typically expresses the meaning of `IN`/`EXISTS`, i.e., for any row in the left/right table, it is returned only if there is any row in the right/left table that meets the join conditions.

#### Syntax

```sql
SELECT ... FROM table_name1 LEFT|RIGHT SEMI JOIN table_name2 ON ... [WHERE ...] [...]
```

#### Result Set

The row data set composed of rows from the left/right table that meet the join conditions and any row from the right/left table that meets the join conditions.

#### Applicable Scope

Supports Left/Right Semi Join between supertables, basic tables, subtables, virtual tables, and subqueries.

#### Example

Times in table d1001 when the voltage is greater than 220V and there are other meters at the same moment with voltage also greater than 220V:

```sql
SELECT a.ts FROM d1001 a LEFT SEMI JOIN meters b ON a.ts = b.ts AND a.voltage > 220 AND b.voltage > 220 AND b.tbname != 'd1001';
```

### Left/Right Anti-Semi Join

#### Definition

Left/Right Anti Join - Opposite to the logic of Left/Right Semi Join, usually expresses the meaning of `NOT EXISTS`. That is, for any row in the left/right table, it returns the row data from the left/right table only if there is no corresponding data in the right/left table that meets the join condition.

#### Syntax

```sql
SELECT ... FROM table_name1 LEFT|RIGHT ANTI JOIN table_name2 ON ... [WHERE ...] [...]
```

#### Result Set

Unmatched rows from the left/right table, with `NULL` values for columns from the right/left table.

#### Applicable Scenarios

Supports Left/Right Anti-Semi Join between supertables, basic tables, subtables, virtual tables, and subqueries.

#### Example

Times when the voltage in table d1001 is greater than 220V and there is no other electric meter at the same time with a voltage also greater than 220V:

```sql
SELECT a.ts FROM d1001 a LEFT ANTI JOIN meters b ON a.ts = b.ts AND b.voltage > 220 AND b.tbname != 'd1001' WHERE a.voltage > 220;
```

### Left/Right ASOF Join

#### Definition

Left/Right ASOF Join - Unlike other traditional joins that require exact matches, ASOF Join allows for approximate matching based on the closest primary key timestamp.

#### Syntax

```sql
SELECT ... FROM table_name1 LEFT|RIGHT ASOF JOIN table_name2 [ON ...] [JLIMIT jlimit_num] [WHERE ...] [...]
```

#### Result Set

The Cartesian product of each row from the left/right table with up to `jlimit_num` rows from the right/left table that meet the join condition and are closest in timestamp, sorted by the primary key column, or null data (`NULL`).

#### Applicable Scenarios

Supports Left/Right ASOF Join between supertables, basic tables, and subtables.

Starting with `v3.4.3.0`, either input can also be a virtual table, a FROM
subquery, or a JOIN result. For rules on using JOIN results as inputs, see
[Mixed Nested Joins](#mixed-nested-joins).

#### Notes

- The ON clause supports specifying a single match rule based on the primary key
  column or the timetruncate function of the primary key column (does not
  support other scalar operations and functions). The following table assumes
  `a.ts operator b.ts`, where `a` is the left table and `b` is the right table:

  |    **Operator**   |       **Meaning for Left ASOF**       |
  | :-------------: | ------------------------ |
  | >    | Matches rows in the right table where the primary key timestamp is less than and closest to the left table's primary key timestamp      |
  | >=    | Matches rows in the right table where the primary key timestamp is less than or equal to and closest to the left table's primary key timestamp  |
  | =    | Matches rows in the right table where the primary key timestamp equals the left table's primary key timestamp  |
  | \<    | Matches rows in the right table where the primary key timestamp is greater than and closest to the left table's primary key timestamp  |
  | \<=    | Matches rows in the right table where the primary key timestamp is greater than or equal to and closest to the left table's primary key timestamp  |

  For Right ASOF, each right-side row selects the closest records from the left table that satisfy the comparison. For example, `ON a.ts <= b.ts` matches left-side records at or before the right-side timestamp.

- If the ON clause is absent or does not specify a primary key match rule, the
  default matches records at or before the driving row's timestamp: Left ASOF is
  equivalent to `ON a.ts >= b.ts`, and Right ASOF to `ON a.ts <= b.ts`, where
  `a` is the left table and `b` is the right table. Multiple primary join
  conditions are not supported.
- The ON clause can also specify equality conditions between tags or normal
  columns (scalar functions and operations not supported) for grouped
  calculations, other types of conditions are not supported.
- All conditions in the ON clause only support the `AND` operation.
- `JLIMIT` optionally specifies the maximum number of matches per driving row.
  The default is `1`, so each left/right row obtains at most one match from the
  right/left table. The range is `[0, 1024]`; `JLIMIT 0` returns an empty
  result. The matching rows need not share a timestamp. Fewer than `jlimit_num`
  rows may be returned if fewer matches exist. When more matches exist, the
  choice among rows tied at the cutoff timestamp is unspecified.

#### Example

For each row in `d1001`, first match the row in `d1002` at the same timestamp or the closest earlier timestamp. Then retain only results where both voltages exceed 220V. If the closest record fails the voltage filter, the query does not search for an earlier record:

```sql
SELECT a.ts, a.voltage, b.ts, b.voltage FROM d1001 a LEFT ASOF JOIN d1002 b ON a.ts >= b.ts WHERE a.voltage > 220 AND b.voltage > 220;
```

### Left/Right Window Join

#### Definition

Left/Right Window Join - Constructs windows based on the time reference values
of each row in the left/right input and the window boundaries, and performs
window joins accordingly, supporting projection, scalar, and aggregation
operations within the window.

#### Syntax

```sql
SELECT ... FROM table_reference1
LEFT|RIGHT WINDOW JOIN table_reference2 [ON ...]
[WINDOW_TIME(left_time_column, right_time_column)]
WINDOW_OFFSET(start_offset, end_offset)
[JLIMIT jlimit_num] [WHERE ...] [...]
```

#### Result Set

One of the following:

- Each left/right row paired with up to `jlimit_num` right/left rows within the
  window defined by the driving row's primary timestamp and `WINDOW_OFFSET`, or
  with `NULL` values if no match exists.
- Each left/right row combined with aggregate results over up to `jlimit_num`
  rows in that window. With no matches, the result depends on the aggregate
  function: for example, `COUNT(b.voltage)` is `0`, whereas `AVG(b.voltage)` is
  `NULL`, where `b` is the table being matched.

#### Applicable Scenarios

Supports Left/Right Window Join between supertables, basic tables, and subtables.

Starting with `v3.4.3.0`, either input can also be a virtual table, a FROM
subquery, or a JOIN result. The `WINDOW_TIME` clause can specify the time
reference column for each WINDOW JOIN input. For rules on using JOIN results
as inputs, see [Mixed Nested Joins](#mixed-nested-joins).

#### Notes

- The `ON` clause is optional, only supports specifying equality conditions between tags and normal columns (excluding scalar functions and operations) other than the primary key column for grouped calculations, and only supports `AND` operations between all conditions;
- `WINDOW_OFFSET` is used to specify the left and right boundaries of the window
  relative to the primary key timestamp of the left/right table, supporting
  formats with built-in time units, for example: `WINDOW_OFFSET(-1a, 1a)` for a
  Left Window Join, represents each window as
  [left table primary key timestamp - 1 millisecond,
  left table primary key timestamp + 1 millisecond], with both boundaries
  included. The time units following the numbers can be
  `b` (nanoseconds), `u` (microseconds), `a` (milliseconds), `s` (seconds), `m`
  (minutes), `h` (hours), `d` (days), `w` (weeks), does not support natural
  months (`n`), natural years (`y`), and the smallest supported time unit is the
  database precision, the precision of the databases of the left and right
  tables must be consistent.
- `JLIMIT` optionally specifies the maximum number of matching rows in each
  window. By default, all matching rows are used. The range is `[0, 1024]`;
  `JLIMIT 0` returns an empty result. If the input being matched has fewer than
  `jlimit_num` matches, fewer rows may be returned. If it has more, the
  `jlimit_num` rows with the earliest primary timestamps in the window are used.
- SQL statements cannot contain other `GROUP BY`/`PARTITION BY`/window queries;
- Supports scalar filtering in the `WHERE` clause, supports aggregation function filtering for each window in the `HAVING` clause (does not support scalar filtering), does not support `SLIMIT`, does not support various window pseudocolumns;
- The first `WINDOW_TIME` argument is resolved only in the left input, and the
  second only in the right input. Arguments must be `TIMESTAMP` columns and can
  be qualified by input aliases. Constants, functions, and other expressions
  are not supported.
- Actual tables, virtual tables, and FROM subqueries can specify a non-primary
  key `TIMESTAMP` column.
- Without `WINDOW_TIME`, actual and virtual tables use their primary timestamp
  column. A FROM subquery prefers the first `TIMESTAMP` column in its output
  list that preserves primary timestamp semantics. If no such column exists,
  it uses the first `TIMESTAMP` column, including a constant timestamp.
  Selection is independent of input ordering. The rules for preserving primary
  timestamp semantics are the same as for ASOF JOIN.
- Rows with a `NULL` time reference do not participate in the join and are not
  returned as unmatched rows.

#### Examples

For each row in `d1001` whose voltage exceeds 220V, return voltages from `d1002` within one second before or after its timestamp:

```sql
SELECT a.ts, a.voltage, b.voltage FROM d1001 a LEFT WINDOW JOIN d1002 b WINDOW_OFFSET(-1s, 1s) WHERE a.voltage > 220;
```

For each row in `d1001` whose voltage exceeds 220V, calculate the average voltage of `d1002` within one second before or after its timestamp. Return the timestamp, voltage, and average only when that average also exceeds 220V:

```sql
SELECT a.ts, a.voltage, AVG(b.voltage) FROM d1001 a LEFT WINDOW JOIN d1002 b WINDOW_OFFSET(-1s, 1s) WHERE a.voltage > 220 HAVING (AVG(b.voltage) > 220);
```

### Full Outer Join

#### Definition

Full (Outer) Join returns all matching row pairs and preserves unmatched rows from both tables.

#### Syntax

```sql
SELECT ... FROM table_name1 FULL [OUTER] JOIN table_name2 ON ... [WHERE ...] [...]
```

#### Result Set

The Inner Join result, plus:

- Unmatched left-side rows with `NULL` values for right-side columns.
- Unmatched right-side rows with `NULL` values for left-side columns.

#### Applicable Scenarios

Supports Full Outer Join between supertables, basic tables, subtables, virtual tables, and subqueries.

#### Notes

- The OUTER keyword is optional.

#### Examples

Records of all moments and voltage values in tables d1001 and d1002:

```sql
SELECT a.ts, a.voltage, b.ts, b.voltage FROM d1001 a FULL JOIN d1002 b ON a.ts = b.ts;
```

## Constraints and Limitations

### Input Timeline Limitations

- Ordinary JOINs do not require a primary timestamp column in the join condition; subqueries without a time column can still join on other columns. See [Primary Key Timeline](#primary-key-timeline).
- A JOIN algorithm Hint does not change the ordering or row-selection semantics of a subquery.
- A subquery's ORDER BY and LIMIT still determine which rows it selects before the join; JOIN does not change that behavior.
- ASOF/WINDOW inputs must still meet their respective time requirements.
  For JOIN results as inputs, see [Mixed Nested Joins](#mixed-nested-joins).

### Join Condition Limitations

- Joins that match rows by a condition require an explicit join condition. An inner join can also use comma syntax with `WHERE`; unconditional joins use `CROSS JOIN` or comma syntax without a matching condition. `ASOF JOIN` allows `ON` to be omitted, and `WINDOW JOIN` specifies its time range through `WINDOW_OFFSET`. See their respective sections for details;
- Except for ASOF JOIN and WINDOW JOIN, JOIN conditions do not require primary timestamp equality or another equality condition; business-column comparisons, ranges, and combined Boolean conditions are supported;
- `ON` expressions must be evaluable as Boolean expressions, and compared values must have compatible types. Compute aggregate and window functions in a subquery before using their output in a join.

### Grouping Condition Limitations

These restrictions apply to grouping for ASOF/WINDOW matching:

- Only supports equality conditions for Tags and ordinary columns other than the primary key column;
- Scalar operations are not supported;
- Supports multiple grouping conditions, only `AND` operations are supported between conditions;

### Query Result Order Limitations

- Ordinary JOINs do not guarantee result order, including joins between regular tables, subtables, and subqueries. Use explicit `ORDER BY` when a defined order is required.

## Mixed Nested Joins

Starting with `v3.4.3.0`, a JOIN result can be the left or right input of
another JOIN. `INNER`, `CROSS`, `LEFT`, `RIGHT`, `FULL`, `SEMI`, `ANTI`,
`ASOF`, and `WINDOW JOIN` can be mixed and nested across multiple tables.
Each JOIN follows its own rules for matching and retaining rows.

Use parentheses to specify the grouping of joins. Columns referenced in each
`ON` clause must belong to that JOIN's left or right input. When a JOIN is
inside a FROM subquery, the outer query can access only the subquery's output
columns through its alias. Grouping affects the results of outer, SEMI/ANTI,
and temporal joins, so their order cannot be changed arbitrarily.

### Examples

These examples also use `d1003`, a subtable of `meters`. Create it and insert
data before running the queries. The first example joins two meters by time,
then matches each row from the left meter with the third meter's nearest
reading at or before that time:

```sql
SELECT a.ts, a.voltage, b.voltage, c.voltage
FROM (d1001 a LEFT JOIN d1002 b ON a.ts = b.ts)
LEFT ASOF JOIN d1003 c ON a.ts >= c.ts;
```

The right input can also be a JOIN result. This example uses `b.ts` to match
the joined rows from two tables within one second before or after `a.ts`:

```sql
SELECT a.ts, b.voltage, c.voltage
FROM d1001 a
LEFT WINDOW JOIN (d1002 b JOIN d1003 c ON b.ts = c.ts)
WINDOW_TIME(a.ts, b.ts) WINDOW_OFFSET(-1s, 1s);
```

### JOIN Results as ASOF/WINDOW Inputs

A JOIN result used as an ASOF/WINDOW input must provide a usable `TIMESTAMP`
time reference column. Specify it in the ASOF time matching condition or the
WINDOW `WINDOW_TIME` clause. If it is not specified explicitly:

- For ordinary JOIN results, INNER/CROSS/FULL select from both sides, LEFT
  variants select from the left side, and RIGHT variants select from the
  right side. The first primary timestamp column in column order takes
  precedence; if none exists, the first `TIMESTAMP` column is used.
- For ASOF/WINDOW results, the driving side's time reference column is reused.
- For FROM subqueries, follow the subquery time column rules in the respective
  sections. The required time column must be included in the subquery output.

Rows whose selected time column is `NULL` do not participate in the outer
ASOF/WINDOW JOIN, even on the driving side.
