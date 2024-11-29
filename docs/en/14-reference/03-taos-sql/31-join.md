---
title: Join Queries
description: Detailed description of join queries
slug: /tdengine-reference/sql-manual/join-queries
---

## Concept of Join

### Driver Table

The table that drives the join query. In Left Join series, the left table is the driver table; in Right Join series, the right table is the driver table.

### Join Condition

The join condition refers to the specified conditions for table associations. All join queries supported by TDengine must specify join conditions, which usually (except for Inner Join and Window Join) appear only after `ON`. According to semantics, conditions appearing after `WHERE` in an Inner Join can also be regarded as join conditions, while Window Join specifies join conditions through `WINDOW_OFFSET`.

Except for ASOF Join, all types of Join supported by TDengine must explicitly specify join conditions. ASOF Join can omit the explicit specification of join conditions because it has implicit join conditions defined by default, provided that the default conditions meet the requirements.

In addition to the primary join condition, other join conditions can be included in the join condition, and the primary join condition must have an `AND` relationship with the other join conditions, which do not have this restriction. Other join conditions can include primary key columns, tags, ordinary columns, constants, and any logical combination of scalar functions or operations.

For example, in a smart meter context, the following SQL statements contain valid join conditions:

```sql
SELECT a.* FROM meters a LEFT JOIN meters b ON a.ts = b.ts AND a.ts > '2023-10-18 10:00:00.000';
SELECT a.* FROM meters a LEFT JOIN meters b ON a.ts = b.ts AND (a.ts > '2023-10-18 10:00:00.000' OR a.ts < '2023-10-17 10:00:00.000');
SELECT a.* FROM meters a LEFT JOIN meters b ON timetruncate(a.ts, 1s) = timetruncate(b.ts, 1s) AND (a.ts + 1s > '2023-10-18 10:00:00.000' OR a.groupId > 0);
SELECT a.* FROM meters a LEFT ASOF JOIN meters b ON timetruncate(a.ts, 1s) < timetruncate(b.ts, 1s) AND a.groupId = b.groupId;
```

### Primary Join Condition

As a time-series database, all join queries in TDengine revolve around the primary key timestamp column, thus requiring all join queries (except ASOF/Window Join) to include an equality join condition for the primary key column. The first appearance of the primary key column equality join condition in the join condition will be treated as the primary join condition. The primary join condition for ASOF Join can include non-equality join conditions, while Window Join specifies its primary join condition through `WINDOW_OFFSET`.

Except for Window Join, TDengine supports the use of the `timetruncate` function in the primary join condition, for example, `ON timetruncate(a.ts, 1s) = timetruncate(b.ts, 1s)`; however, other functions and scalar operations are not supported.

### Grouping Conditions

The ASOF/Window Join, characteristic of time-series databases, supports grouping the input data for join queries and then performing join operations within each group. Grouping only applies to the input of join queries, and the output will not contain grouping information. The equality conditions appearing after `ON` in ASOF/Window Join (except for the primary join conditions of ASOF) will be treated as grouping conditions.

### Primary Key Timeline

As a time-series database, TDengine requires every table (subtable) to have a primary key timestamp column, which will serve as the primary key timeline for many time-related operations. The results of subqueries or Join operations must also clearly specify which column will be regarded as the primary key timeline for subsequent time-related operations. In subqueries, the first ordered primary key column (or its operation) or pseudo-columns equivalent to the primary key column (`_wstart`/`_wend`) that appears in the query results will be regarded as the primary key timeline for that output table. The selection of the primary key timeline in Join output results follows these rules:

- In Left/Right Join series, the primary key column of the driver table (subquery) will be used as the primary key timeline for subsequent queries; additionally, within the Window Join, both tables are ordered, so any primary key column can serve as the primary key timeline, prioritizing the driver table's primary key column.
- In Inner Join, any primary key column can serve as the primary key timeline. When similar grouping conditions (equality conditions on tag columns related to the primary join condition with `AND`) exist, it will not be possible to produce a primary key timeline.
- Full Join cannot produce any valid primary key timeline, meaning there is no primary key timeline in Full Join, which also implies that time-related operations cannot be performed within Full Join.

## Syntax Explanation

In the following sections, the Left/Right Join series will be introduced using a common approach, thus the descriptions for Outer, Semi, Anti-Semi, ASOF, and Window series will adopt similar "left/right" phrasing for simultaneous explanation of Left/Right Join. Here is a brief introduction to this phrasing's meaning: the part before "/" applies to Left Join, while the part after "/" applies to Right Join.

For example:

"Left/Right Table" means "left table" for Left Join and "right table" for Right Join; similarly, "Right/Left Table" means "right table" for Left Join and "left table" for Right Join.

## Join Functions

### Inner Join

#### Definition

Inner Join - Only the data from both tables that meet the join condition will be returned, which can be viewed as the intersection of the data that meets the join condition from both tables.

#### Syntax

```sql
SELECT ... FROM table_name1 [INNER] JOIN table_name2 [ON ...] [WHERE ...] [...];
or
SELECT ... FROM table_name1, table_name2 WHERE ... [...];
```

#### Result Set

The Cartesian product set of the row data from both tables that meet the join conditions.

#### Applicable Scope

Supports Inner Join between supertables, basic tables, subtables, and subqueries.

#### Notes

- For the first syntax, the `INNER` keyword is optional, and the `ON` and/or `WHERE` clauses can specify primary join conditions and other join conditions; filtering conditions can also be specified in `WHERE`, with at least one specified in `ON`/`WHERE`.
- For the second syntax, primary join conditions, other join conditions, and filtering conditions can be specified in `WHERE`.
- When performing Inner Join on supertables, equality conditions on tag columns that relate to the primary join condition with `AND` will be used as similar grouping conditions, thus the output results may not remain ordered.

#### Example

The timestamps when the voltage exceeds 220V appear in both table d1001 and table d1002, along with their respective voltage values:

```sql
SELECT a.ts, a.voltage, b.voltage FROM d1001 a JOIN d1002 b ON a.ts = b.ts AND a.voltage > 220 AND b.voltage > 220;
```

### Left/Right Outer Join

#### Definition

Left/Right (Outer) Join - Contains both the data sets that meet the join conditions from both tables as well as the data sets from the left/right tables that do not meet the join conditions.

#### Syntax

```sql
SELECT ... FROM table_name1 LEFT|RIGHT [OUTER] JOIN table_name2 ON ... [WHERE ...] [...];
```

#### Result Set

The result set of Inner Join + rows from the left/right table that do not meet the join conditions, combined with NULL data from the right/left table.

#### Applicable Scope

Supports Left/Right Join between supertables, basic tables, subtables, and subqueries.

#### Notes

- The OUTER keyword is optional.

#### Example

All timestamps from table d1001 with their voltage values along with those from table d1002 that have voltages exceeding 220V at the same time:

```sql
SELECT a.ts, a.voltage, b.voltage FROM d1001 a LEFT JOIN d1002 b ON a.ts = b.ts AND a.voltage > 220 AND b.voltage > 220;
```

### Left/Right Semi Join

#### Definition

Left/Right Semi Join - Typically represents the meaning of `IN`/`EXISTS`, meaning that for any data from the left/right table, the left/right table row data will only be returned if there exists any data in the right/left table that meets the join conditions.

#### Syntax

```sql
SELECT ... FROM table_name1 LEFT|RIGHT SEMI JOIN table_name2 ON ... [WHERE ...] [...];
```

#### Result Set

The row data set composed of rows from the left/right table that meet the join conditions along with any rows from the right/left table that meet the join conditions.

#### Applicable Scope

Supports Left/Right Semi Join between supertables, basic tables, subtables, and subqueries.

#### Example

The timestamps when the voltage exceeds 220V in table d1001 and where other meters also have voltages exceeding 220V at the same time:

```sql
SELECT a.ts FROM d1001 a LEFT SEMI JOIN meters b ON a.ts = b.ts AND a.voltage > 220 AND b.voltage > 220 AND b.tbname != 'd1001';
```

### Left/Right Anti-Semi Join

#### Definition

Left/Right Anti Join - The logic is exactly the opposite of Left/Right Semi Join, typically representing the meaning of `NOT IN`/`NOT EXISTS`, meaning that for any data from the left/right table, the left/right table row data will only be returned if no data in the right/left table meets the join conditions.

#### Syntax

```sql
SELECT ... FROM table_name1 LEFT|RIGHT ANTI JOIN table_name2 ON ... [WHERE ...] [...];
```

#### Result Set

The row data set composed of rows from the left/right table that do not meet the join conditions along with NULL data from the right/left table.

#### Applicable Scope

Supports Left/Right Anti-Semi Join between supertables, basic tables, subtables, and subqueries.

#### Example

The timestamps when the voltage exceeds 220V in table d1001 and where no other meters have voltages exceeding 220V at the same time:

```sql
SELECT a.ts FROM d1001 a LEFT ANTI JOIN meters b ON a.ts = b.ts AND b.voltage > 220 AND b.tbname != 'd1001' WHERE a.voltage > 220;
```

### Left/Right ASOF Join

#### Definition

Left/Right ASOF Join - Unlike other traditional Join's complete matching mode, ASOF Join allows for specified matching patterns for partial matches, matching based on the closest primary key timestamp.

#### Syntax

```sql
SELECT ... FROM table_name1 LEFT|RIGHT ASOF JOIN table_name2 [ON ...] [JLIMIT jlimit_num] [WHERE ...] [...];
```

##### Result Set

For each row in the left/right table, the Cartesian product set of that row with at most `jlimit_num` rows of data from the right/left table that meet the join conditions, sorted by the primary key column based on timestamp.

##### Applicable Scope

Supports Left/Right ASOF Join between supertables, basic tables, and subtables.

#### Notes

- Only supports ASOF Join between tables, not between subqueries.
- The ON clause supports specifying a single matching rule for the primary key column or its `timetruncate` function operation (other scalar operations and functions are not supported).
  
  |    **Operator**   |       **Meaning in Left ASOF**       |
  | :-------------: | ------------------------ |
  | &gt;    | Match rows in the right table where the primary key timestamp is less than that of the left table, closest in timestamp.      |
  | &gt;=    | Match rows in the right table where the primary key timestamp is less than or equal to that of the left table, closest in timestamp.  |
  | =    | Match rows in the right table where the primary key timestamp equals that of the left table.  |
  | &lt;    | Match rows in the right table where the primary key timestamp is greater than that of the left table, closest in timestamp.  |
  | &lt;=    | Match rows in the right table where the primary key timestamp is greater than or equal to that of the left table, closest in timestamp.  |

  For Right ASOF, the meanings of the above operators are exactly the opposite.

- If there is no `ON` clause or the `ON` clause does not specify the matching rule for the primary key column, the default matching rule for the primary key is “>=”, meaning (for Left ASOF Join) rows where the primary key timestamp in the right table is less than or equal to the primary key timestamp in the left table. Multiple primary join conditions are not supported.
- The ON clause can also specify equality conditions between tags and ordinary columns (not supporting scalar functions and operations) to be used for grouping calculations, but no other types of conditions are supported.
- All ON conditions support only `AND` operations.
- `JLIMIT` is used to specify the maximum number of matching rows for a single row; optional, and defaults to 1 if not specified, meaning that each row from the left/right table can obtain at most one matching result from the right/left table. The value of `JLIMIT` can be in the range of [0, 1024]. The returned number of matching rows that meet the conditions may be less than `jlimit_num` when there are not enough rows in the right/left table; when there are more than `jlimit_num` rows that meet the conditions, if their timestamps are the same, `jlimit_num` rows will be returned randomly.

#### Example

The timestamps when the voltage exceeds 220V in table d1001 and the last timestamps in table d1002 where the voltage exceeded 220V at the same time or slightly earlier:

```sql
SELECT a.ts, a.voltage, b.ts, b.voltage FROM d1001 a LEFT ASOF JOIN d1002 b ON a.ts >= b.ts WHERE a.voltage > 220 AND b.voltage > 220;
```

### Left/Right Window Join

#### Definition

Left/Right Window Join - Constructs a window based on the primary key timestamps of each row in the left/right table and the window boundaries, and performs window joins accordingly, supporting projection, scalar, and aggregation operations within the window.

#### Syntax

```sql
SELECT ... FROM table_name1 LEFT|RIGHT WINDOW JOIN table_name2 [ON ...] WINDOW_OFFSET(start_offset, end_offset) [JLIMIT jlimit_num] [WHERE ...] [...];
```

#### Result Set

The Cartesian product set of each row in the left/right table with at most `jlimit_num` rows from the right/left table based on the primary key timestamp column of the left/right table and the window defined by `WINDOW_OFFSET`, or the aggregate results from the left/right table with at most `jlimit_num` rows from the right/left table based on the same criteria.

#### Applicable Scope

Supports Left/Right Window Join between supertables, basic tables, and subtables.

#### Notes

- Only supports Window Join between tables, not between subqueries.
- The ON clause is optional and only supports specifying equality conditions between tags and ordinary columns (not supporting scalar functions and operations) for grouping calculations, all conditions support only `AND` operations.
- `WINDOW_OFFSET` specifies the left and right boundaries of the window relative to the primary key timestamps of the left/right table, supporting formats with time units, such as: `WINDOW_OFFSET(-1a, 1a)`. For Left Window Join, this means each window is defined as [left table primary key timestamp - 1 millisecond, left table primary key timestamp + 1 millisecond], with both boundaries being closed intervals. The time units can be `b` (nanoseconds), `u` (microseconds), `a` (milliseconds), `s` (seconds), `m` (minutes), `h` (hours), `d` (days), `w` (weeks), but not natural month (`n`) or natural year (`y`); the minimum time unit supported is the database precision, and the precisions of the left/right tables must remain consistent.
- `JLIMIT` specifies the maximum number of matching rows in a single window; optional, and if not specified, all matching rows in each window are returned by default. The value of `JLIMIT` can be in the range of [0, 1024]. The returned number of matching rows may be less than `jlimit_num` when there are not enough rows in the right table; when there are more than `jlimit_num` rows that meet the conditions, the rows with the smallest primary key timestamps in the window will be prioritized.
- SQL statements cannot include other `GROUP BY`/`PARTITION BY`/window queries.
- Scalar filtering can be performed in the `WHERE` clause, and aggregation function filtering (not scalar filtering) can be done in the `HAVING` clause for each window; `SLIMIT` is not supported, and various window pseudo-columns are not supported.

#### Example

The voltage values from table d1002 within one second before and after when the voltage in table d1001 exceeds 220V:

```sql
SELECT a.ts, a.voltage, b.voltage FROM d1001 a LEFT WINDOW JOIN d1002 b WINDOW_OFFSET(-1s, 1s) WHERE a.voltage > 220;
```

The timestamps from table d1001 when the voltage exceeds 220V and where the average voltage in table d1002 within one second before and after is also greater than 220V:

```sql
SELECT a.ts, a.voltage, AVG(b.voltage) FROM d1001 a LEFT WINDOW JOIN d1002 b WINDOW_OFFSET(-1s, 1s) WHERE a.voltage > 220 HAVING (AVG(b.voltage) > 220);
```

### Full Outer Join

#### Definition

Full (Outer) Join - Contains both the data sets that meet the join conditions from both tables as well as the data sets from both tables that do not meet the join conditions.

#### Syntax

```sql
SELECT ... FROM table_name1 FULL [OUTER] JOIN table_name2 ON ... [WHERE ...] [...];
```

#### Result Set

The result set of Inner Join + rows from the left table that do not meet the join conditions, combined with NULL data from the right table, plus rows from the right table that do not meet the join conditions, combined with NULL data from the left table.

#### Applicable Scope

Supports Full Outer Join between supertables, basic tables, subtables, and subqueries.

#### Notes

- The OUTER keyword is optional.

#### Example

All timestamps and voltage values recorded in both table d1001 and table d1002:

```sql
SELECT a.ts, a.voltage, b.ts, b.voltage FROM d1001 a FULL JOIN d1002 b ON a.ts = b.ts;
```

## Constraints and Limitations

### Input Timeline Limitations

- Currently, all Joins require the input data to contain valid primary key timelines. All table queries can satisfy this condition, while subqueries need to ensure that their output data contains valid primary key timelines.

### Join Condition Limitations

- Except for ASOF and Window Joins, other Join conditions must include a primary join condition for the primary key column
- Only `AND` operations are supported between the primary join condition and other join conditions
- The primary join condition's primary key column only supports `timetruncate` function operations (other functions and scalar operations are not supported), while other join conditions are unrestricted.

### Grouping Condition Limitations

- Only equality conditions between tag and ordinary columns (excluding the primary key column) are supported;
- Scalar operations are not supported;
- Multiple grouping conditions are supported, but only `AND` operations are allowed between them.

### Query Result Order Limitations

- In scenarios with basic tables, subtables, or subqueries without grouping conditions and no sorting, the query results will be output in the order of the primary key columns of the driver table;
- In queries on supertables, Full Joins, or those with grouping conditions but no sorting, the query results do not have a fixed output order; thus, sorting operations need to be performed in scenarios where ordering is required. Some functions that depend on the timeline may not execute because of the lack of valid timelines in the output.

### Nested Joins and Multi-Table Join Limitations

- Currently, except for Inner Joins, other types of Joins do not support nested and multi-table joins.
