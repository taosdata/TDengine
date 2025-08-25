---
title: Join Queries
slug: /tdengine-reference/sql-manual/join-queries
---

## Join Concepts

### Driving Table

The table that drives the join query. In the Left Join series, the left table is the driving table, and in the Right Join series, the right table is the driving table.

### Join Conditions

Join conditions refer to the conditions specified for table joins. All join queries supported by TDengine require specifying join conditions, which typically (except for Inner Join and Window Join) appear after `ON`. Semantically, conditions that appear after `WHERE` in an Inner Join can also be considered join conditions, while Window Join specifies join conditions through `WINDOW_OFFSET`.

  Except for ASOF Join, all Join types supported by TDengine must explicitly specify join conditions. ASOF Join has implicit default join conditions, so it is not necessary to specify them explicitly if the default conditions are sufficient.

Apart from ASOF/Window Join, join conditions can include any number of additional conditions besides the main join condition, which must be related by `AND` with the main join condition, while there is no such restriction between other conditions. These additional conditions can include any logical combination of primary key columns, Tags, ordinary columns, constants, and their scalar functions or operations.

For example, with smart meters, the following SQL statements all contain valid join conditions:

```sql
SELECT a.* FROM meters a LEFT JOIN meters b ON a.ts = b.ts AND a.ts > '2023-10-18 10:00:00.000';
SELECT a.* FROM meters a LEFT JOIN meters b ON a.ts = b.ts AND (a.ts > '2023-10-18 10:00:00.000' OR a.ts < '2023-10-17 10:00:00.000');
SELECT a.* FROM meters a LEFT JOIN meters b ON timetruncate(a.ts, 1s) = timetruncate(b.ts, 1s) AND (a.ts + 1s > '2023-10-18 10:00:00.000' OR a.groupId > 0);
SELECT a.* FROM meters a LEFT ASOF JOIN meters b ON timetruncate(a.ts, 1s) < timetruncate(b.ts, 1s) AND a.groupId = b.groupId;
```

### Main Join Condition

As a time-series database, all join queries in TDengine revolve around the primary key timestamp column. Therefore, all join queries (except ASOF/Window Join) must include an equality condition on the primary key column, and the first primary key column equality condition that appears in the join conditions will be considered the main join condition. ASOF Join's main join condition can include non-equality conditions, while Window Join's main join condition is specified through `WINDOW_OFFSET`.
Starting from version 3.3.6.0, TDengine supports constant timestamps in subqueries (including constant functions with return timestamps such as today (), now (), etc., constant timestamps and their addition and subtraction operations) as equivalent primary key columns that can appear in the main join condition. For example:

```sql
SELECT * from d1001 a JOIN (SELECT today() as ts1, * from d1002 WHERE ts = '2025-03-19 10:00:00.000') b ON timetruncate(a.ts, 1d) = b.ts1;
```

The above example SQL will perform join operation between all records in table d1001 today and a certain time record in table d1002. It should be noticed that the constant time string appears in SQL will not be treated as a timestamp by default. For example, "2025-03-19 10:00:00.000" will only be treated as a string instead of a timestamp. Therefore, when it needs to be treated as a constant timestamp, you can specify the constant string as a timestamp type by using the type prefix timestamp. For example:

```sql
SELECT * from d1001 a JOIN (SELECT timestamp '2025-03-19 10:00:00.000' as ts1, * from d1002 WHERE ts = '2025-03-19 10:00:00.000') b ON timetruncate(a.ts, 1d) = b.ts1;
```

Apart from Window Join, TDengine supports the `timetruncate` function operation in the main join condition, such as `ON timetruncate(a.ts, 1s) = timetruncate(b.ts, 1s)`, but does not support other functions and scalar operations.

### Grouping Conditions

The characteristic ASOF/Window Join of time-series databases supports grouping the input data of join queries and then performing join operations within each group. Grouping only affects the input of the join query, and the output results will not include group information. Equality conditions that appear after `ON` in ASOF/Window Join (except for ASOF's main join condition) will be considered as grouping conditions.

### Primary Key Timeline

As a time-series database, TDengine requires each table (subtable) to have a primary key timestamp column, which will serve as the primary key timeline for many time-related operations. The result of a subquery or the result of a Join operation also needs to clearly identify which column will be considered the primary key timeline for subsequent time-related operations. In subqueries, the first appearing ordered primary key column (or its operation) or a pseudocolumn equivalent to the primary key column (`_wstart`/`_wend`) will be considered the primary key timeline of the output table. In addition, starting with version 3.3.6.0, TDengine also supports constant timestamp columns in subquery results as the primary key timeline for the output table. The selection of the primary key timeline in Join output results follows these rules:

- In the Left/Right Join series, the primary key column of the driving table (subquery) will be used as the primary key timeline for subsequent queries; additionally, within the Window Join window, since both tables are ordered, any table's primary key column can be used as the primary key timeline, with a preference for the primary key column of the same table.
- Inner Join can use the primary key column of any table as the primary key timeline, but when there are grouping conditions similar to tag column equality conditions related by `AND` with the main join condition, it will not produce a primary key timeline.
- Full Join cannot produce any valid primary key sequence, thus it has no primary key timeline, which means that time-related operations cannot be performed in Full Join.

## Syntax Explanation

In the following sections, the Left/Right Join series will be introduced in a shared manner. Therefore, subsequent introductions including Outer, Semi, Anti-Semi, ASOF, and Window series all adopt a similar "left/right" notation to introduce Left/Right Join simultaneously. Here is a brief explanation of this notation: the part written before "/" applies to Left Join, while the part written after "/" applies to Right Join.

For example:

"left/right table" means for Left Join, it refers to "left table", and for Right Join, it refers to "right table";

Similarly,

"right/left table" means for Left Join, it refers to "right table", and for Right Join, it refers to "left table";

## Join Features

### Inner Join

#### Definition

Inner Join - Only the data that meets the join conditions in both the left and right tables will be returned, which can be seen as the intersection of data that meets the join conditions from both tables.

#### Syntax

```sql
SELECT ... FROM table_name1 [INNER] JOIN table_name2 [ON ...] [WHERE ...] [...]
or
SELECT ... FROM table_name1, table_name2 WHERE ... [...]
```

#### Result Set

The Cartesian product set of rows from the left and right tables that meet the join conditions.

#### Applicable Scope

Supports Inner Join between supertables, basic tables, subtables, and subqueries.

#### Explanation

- For the first syntax, the `INNER` keyword is optional, `ON` and/or `WHERE` can specify the main join conditions and other join conditions, and `WHERE` can also specify filter conditions. At least one of `ON`/`WHERE` must be specified.
- For the second syntax, you can specify the main join conditions, other join conditions, and filter conditions in `WHERE`.
- When performing an Inner Join on supertables, the tag column equality conditions related to the main join condition `AND` will be used as grouping conditions, so the output results cannot be guaranteed to be ordered.

#### Example

Moments when both table d1001 and table d1002 have voltages greater than 220V and their respective voltage values:

```sql
SELECT a.ts, a.voltage, b.voltage FROM d1001 a JOIN d1002 b ON a.ts = b.ts and a.voltage > 220 and b.voltage > 220
```

### Left/Right Outer Join

#### Definition

Left/Right (Outer) Join - Includes both the data set that meets the join conditions from both tables and the data set from the left/right table that does not meet the join conditions.

#### Syntax

```sql
SELECT ... FROM table_name1 LEFT|RIGHT [OUTER] JOIN table_name2 ON ... [WHERE ...] [...]
```

#### Result Set

The result set of Inner Join + rows from the left/right table that do not meet the join conditions and rows composed of null data (`NULL`) from the right/left table.

#### Applicable Scope

Supports Left/Right Join between supertables, basic tables, subtables, and subqueries.

#### Explanation

- OUTER keyword is optional.

#### Example

All moments of voltage values from table d1001 and moments when both tables have voltages greater than 220V and their respective voltage values:

```sql
SELECT a.ts, a.voltage, b.voltage FROM d1001 a LEFT JOIN d1002 b ON a.ts = b.ts and a.voltage > 220 and b.voltage > 220
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

Supports Left/Right Semi Join between supertables, basic tables, subtables, and subqueries.

#### Example

Times in table d1001 when the voltage is greater than 220V and there are other meters at the same moment with voltage also greater than 220V:

```sql
SELECT a.ts FROM d1001 a LEFT SEMI JOIN meters b ON a.ts = b.ts and a.voltage > 220 and b.voltage > 220 and b.tbname != 'd1001'
```

### Left/Right Anti-Semi Join

#### Definition

Left/Right Anti Join - Opposite to the logic of Left/Right Semi Join, usually expresses the meaning of `NOT IN`/`NOT EXISTS`. That is, for any row in the left/right table, it returns the row data from the left/right table only if there is no corresponding data in the right/left table that meets the join condition.

#### Syntax

```sql
SELECT ... FROM table_name1 LEFT|RIGHT ANTI JOIN table_name2 ON ... [WHERE ...] [...]
```

#### Result Set

The result set consists of rows from the left/right table that do not meet the join condition and rows from the right/left table filled with null data (`NULL`).

#### Applicable Scenarios

Supports Left/Right Anti-Semi Join between supertables, basic tables, subtables, and subqueries.

#### Example

Times when the voltage in table d1001 is greater than 220V and there is no other electric meter at the same time with a voltage also greater than 220V:

```sql
SELECT a.ts FROM d1001 a LEFT ANTI JOIN meters b ON a.ts = b.ts and b.voltage > 220 and b.tbname != 'd1001' WHERE a.voltage > 220
```

### Left/Right ASOF Join

#### Definition

Left/Right ASOF Join - Unlike other traditional joins that require exact matches, ASOF Join allows for approximate matching based on the closest primary key timestamp.

#### Syntax

```sql
SELECT ... FROM table_name1 LEFT|RIGHT ASOF JOIN table_name2 [ON ...] [JLIMIT jlimit_num] [WHERE ...] [...]
```

##### Result Set

The Cartesian product of each row from the left/right table with up to `jlimit_num` rows from the right/left table that meet the join condition and are closest in timestamp, sorted by the primary key column, or null data (`NULL`).

##### Applicable Scenarios

Supports Left/Right ASOF Join between supertables, basic tables, and subtables.

#### Notes

- Only supports ASOF Join between tables, not between subqueries.
- The ON clause supports specifying a single match rule based on the primary key column or the timetruncate function of the primary key column (does not support other scalar operations and functions). The supported operators and their meanings are as follows:

  |    **Operator**   |       **Meaning for Left ASOF**       |
  | :-------------: | ------------------------ |
  | >    | Matches rows in the right table where the primary key timestamp is less than and closest to the left table's primary key timestamp      |
  | >=    | Matches rows in the right table where the primary key timestamp is less than or equal to and closest to the left table's primary key timestamp  |
  | =    | Matches rows in the right table where the primary key timestamp equals the left table's primary key timestamp  |
  | \<    | Matches rows in the right table where the primary key timestamp is greater than and closest to the left table's primary key timestamp  |
  | \<=    | Matches rows in the right table where the primary key timestamp is greater than or equal to and closest to the left table's primary key timestamp  |

  For Right ASOF, the meanings of the operators are the opposite.

- If the ON clause is absent or does not specify a primary key column match rule, the default primary key match rule operator is `>=`, i.e., rows in the right table where the primary key timestamp is less than or equal to the left table's primary key timestamp. Multiple primary join conditions are not supported.
- The ON clause can also specify equality conditions between tags or normal columns (scalar functions and operations not supported) for grouped calculations, other types of conditions are not supported.
- All conditions in the ON clause only support the `AND` operation.
- `JLIMIT` specifies the maximum number of rows for a single row match, optional, default value is 1, i.e., each row from the left/right table can obtain up to one matching row from the right/left table. `JLIMIT` range is [0, 1024]. The `jlimit_num` matching rows do not need to have the same timestamp, and if there are fewer than `jlimit_num` rows in the right/left table that meet the conditions, the number of result rows may be less than `jlimit_num`; if there are more than `jlimit_num` rows that meet the conditions, if the timestamps are the same, `jlimit_num` rows will be returned randomly.

#### Example

Times and respective voltage values when the voltage in table d1001 is greater than 220V and table d1002 has a voltage greater than 220V at the same or slightly earlier time:

```sql
SELECT a.ts, a.voltage, b.ts, b.voltage FROM d1001 a LEFT ASOF JOIN d1002 b ON a.ts >= b.ts where a.voltage > 220 and b.voltage > 220 
```

### Left/Right Window Join

#### Definition

Left/Right Window Join - Constructs windows based on the primary key timestamps of each row in the left/right table and the window boundaries, and performs window joins accordingly, supporting projection, scalar, and aggregation operations within the window.

#### Syntax

```sql
SELECT ... FROM table_name1 LEFT|RIGHT WINDOW JOIN table_name2 [ON ...] WINDOW_OFFSET(start_offset, end_offset) [JLIMIT jlimit_num] [WHERE ...] [...]
```

#### Result Set

The Cartesian product set of each row of data in the left/right table with up to `jlimit_num` rows of data or no data (`NULL`) from the right/left table within the window defined by the primary timestamp column of the left/right table and `WINDOW_OFFSET`, or
The row data set consisting of the aggregation results of each row of data in the left/right table with up to `jlimit_num` rows of data or no data (`NULL`) from the right/left table within the window defined by the primary timestamp column of the left/right table and `WINDOW_OFFSET`.

#### Applicable Scenarios

Supports Left/Right Window Join between supertables, basic tables, and subtables.

#### Notes

- Only supports Window Join between tables, not between subqueries;
- The `ON` clause is optional, only supports specifying equality conditions between tags and normal columns (excluding scalar functions and operations) other than the primary key column for grouped calculations, and only supports `AND` operations between all conditions;
- `WINDOW_OFFSET` is used to specify the left and right boundaries of the window relative to the primary key timestamp of the left/right table, supporting formats with built-in time units, for example: `WINDOW_OFFSET(-1a, 1a)` for a Left Window Join, represents each window as [left table primary key timestamp - 1 millisecond, left table primary key timestamp + 1 millisecond], both boundaries are closed intervals. The time units following the numbers can be `b` (nanoseconds), `u` (microseconds), `a` (milliseconds), `s` (seconds), `m` (minutes), `h` (hours), `d` (days), `w` (weeks), does not support natural months (`n`), natural years (`y`), and the smallest supported time unit is the database precision, the precision of the databases of the left and right tables must be consistent.
- `JLIMIT` is used to specify the maximum number of matching rows within a single window, optional, by default retrieves all matching rows within each window. The range of `JLIMIT` is [0, 1024], when there are no `jlimit_num` rows of data in the right table that meet the conditions, the number of result rows may be less than `jlimit_num`; when there are more than `jlimit_num` rows of data in the right table that meet the conditions, it prioritizes returning the `jlimit_num` rows of data with the smallest primary key timestamps within the window.
- SQL statements cannot contain other `GROUP BY`/`PARTITION BY`/window queries;
- Supports scalar filtering in the `WHERE` clause, supports aggregation function filtering for each window in the `HAVING` clause (does not support scalar filtering), does not support `SLIMIT`, does not support various window pseudocolumns;

#### Examples

Voltage values from table d1001 greater than 220V within a 1-second interval before and after from table d1002:

```sql
SELECT a.ts, a.voltage, b.voltage FROM d1001 a LEFT WINDOW JOIN d1002 b WINDOW_OFFSET (-1s, 1s) where a.voltage > 220
```

Timestamps and voltage values from table d1001 greater than 220V and the average voltage value from table d1002 also greater than 220V within a 1-second interval before and after:

```sql
SELECT a.ts, a.voltage, avg(b.voltage) FROM d1001 a LEFT WINDOW JOIN d1002 b WINDOW_OFFSET (-1s, 1s) where a.voltage > 220 HAVING(avg(b.voltage) > 220)
```

### Full Outer Join

#### Definition

Full (Outer) Join - Includes both the data set where both left and right tables meet the join conditions and the data set where either table does not meet the join conditions.

#### Syntax

SELECT ... FROM table_name1 FULL [OUTER] JOIN table_name2 ON ... [WHERE ...] [...]

#### Result Set

The result set of Inner Join + rows from the left table that do not meet the join conditions combined with empty data from the right table + rows from the right table that do not meet the join conditions combined with empty data (`NULL`) from the left table.

#### Applicable Scenarios

Supports Full Outer Join between supertables, basic tables, subtables, and subqueries.

#### Notes

- The OUTER keyword is optional.

#### Examples

Records of all moments and voltage values in tables d1001 and d1002:

```sql
SELECT a.ts, a.voltage, b.ts, b.voltage FROM d1001 a FULL JOIN d1002 b on a.ts = b.ts
```

## Constraints and Limitations

### Input Timeline Limitations

- Currently, all Joins require input data containing a valid primary key timeline; all table queries can meet this requirement, but subqueries need to ensure that the output data contains a valid primary key timeline.

### Join Condition Limitations

- Except for ASOF and Window Join, other Joins must include the primary key column in the main join conditions; and
- Only `AND` operations are supported between the main join condition and other conditions;
- The primary key column used as the main join condition only supports the `timetruncate` function (other functions and scalar operations are not supported); there are no restrictions when used as other join conditions;

### Grouping Condition Limitations

- Only supports equality conditions for Tags and ordinary columns other than the primary key column;
- Scalar operations are not supported;
- Supports multiple grouping conditions, only `AND` operations are supported between conditions;

### Query Result Order Limitations

- In scenarios with basic tables, subtables, subqueries without grouping conditions and without sorting, the query results will be output in the order of the primary key column of the driving table;
- In scenarios with supertable queries, Full Join, or with grouping conditions but without sorting, there is no fixed order of output;
Therefore, in scenarios where sorting is needed and the output order is not fixed, sorting operations are needed. Some functions dependent on the timeline may not execute due to the lack of a valid timeline.

### Nested Join and Multi-table Join Limitations

- Currently, except for Inner Join which supports nested and multi-table Joins, other types of Joins do not support nested and multi-table Joins.
