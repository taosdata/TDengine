---
sidebar_label: JOIN
title: JOIN
description: JOIN Description
---


## Join Concept

### Driving Table

The table used for driving Join queries is the left table in the Left Join series and the right table in the Right Join series.

### Join Conditions

Join conditions refer to the conditions specified for join operation. All join queries supported by TDengine require specifying join conditions. Join conditions usually only appear in `ON` (except for Inner Join and Window Join). For Inner Join, conditions that appear in `WHERE` can also be regarded as join conditions. For Window Join join conditions are specified in `WINDOW_OFFSET` clause.

 Except for ASOF Join, all join types supported by TDengine must explicitly specify join conditions. Since ASOF Join has implicit join conditions defined by default, it is not necessary to explicitly specify the join conditions (if the default conditions meet the requirements).

Except for ASOF/Window Join, the join condition can include not only the primary join condition(refer below), but also any number of other join conditions. The primary join condition must have an `AND` relationship with the other join conditions, while there is no such restriction between the other join conditions. The other join conditions can include any logical operation combination of primary key columns, Tag columns, normal columns, constants, and their scalar functions or operations.


Taking smart meters as an example, the following SQL statements all contain valid join conditions:

```sql
SELECT a.* FROM meters a LEFT JOIN meters b ON a.ts = b.ts AND a.ts > '2023-10-18 10:00:00.000';
SELECT a.* FROM meters a LEFT JOIN meters b ON a.ts = b.ts AND (a.ts > '2023-10-18 10:00:00.000' OR a.ts < '2023-10-17 10:00:00.000');
SELECT a.* FROM meters a LEFT JOIN meters b ON timetruncate(a.ts, 1s) = timetruncate(b.ts, 1s) AND (a.ts + 1s > '2023-10-18 10:00:00.000' OR a.groupId > 0);
SELECT a.* FROM meters a LEFT ASOF JOIN meters b ON timetruncate(a.ts, 1s) < timetruncate(b.ts, 1s) AND a.groupId = b.groupId;
```

### Primary Join Condition

As a time series database, all join queries of TDengine revolve around the primary timestamp column, so all join queries except ASOF/Window Join are required to contain equivalent join condition of the primary key column. The equivalent join condition of the primary key column that first appear in the join conditions in order will be used as the primary join condition. The primary join condition of ASOF Join can contain non-equivalent join condition, for Window Join the primary join condition is specified by `WINDOW_OFFSET` clause.

Except for Window Join, TDengine supports performing `timetruncate` function operation in the primary join condition, e.g. `ON timetruncate(a.ts, 1s) = timetruncate(b.ts, 1s)`. Other functions and scalar operations to primary key column are not currently supported in the primary join condition.

### Grouping Conditions

ASOF/Window Join supports grouping the input data of join queries, and then performing join operations within each group. Grouping only applies to the input of join queries, and the output result will not include grouping information. Equivalent conditions that appear in `ON` in ASOF/Window Join (excluding the primary join condition of ASOF) will be used as grouping conditions.


### Primary Key Timeline

TDengine, as a time series database, requires that each table must have a primary key timestamp column, which will perform many time-related operations as the primary key timeline of the table. It is also necessary to clarify which column will be regarded as the primary key timeline for subsequent time-related operations in the results of subqueries or join operations. In subqueries, the ordered first occurrence of the primary key column (or its operation) or the pseudo-column equivalent to the primary key column (`_wstart`/`_wend`) in the query results will be regarded as the primary key timeline of the output table. The selection of the primary key timeline in the join output results follows the following rules:

- The primary key column of the driving table (subquery) in the Left/Right Join series will be used as the primary key timeline for subsequent queries. In addition, in each Window Join window, because the left and right tables are ordered at the same time, the primary key column of any table can be used as the primary key timeline in the window, and the primary key column of current table is preferentially selected as the primary key timeline.

- The primary key column of any table in Inner Join can be treated as the primary key timeline. When there are similar grouping conditions (equivalent conditions of TAG columns and `AND` relationship with the primary join condition), there will be no available primary key timeline.

- Full Join will not result in any primary key timeline because it cannot generate any valid primary key time series, so no timeline-related operations can be performed in or after a Full Join.


## Syntax Conventions
Because we will introduce the Left/Right Join series simultaneously through sharing below, the introductions of Left/Right Outer, Semi, Anti-Semi, ASOF, and Window series Joins will all use a similar "left/right" approach to introduce Left/Right Join simultaneously. Here is a brief introduction to the meaning of this writing method. The words written before "/" are the words applied to Left Join, and the words written after "/" are the words applied to Right Join. 

For example:

The phrase "left/right table" means "left table" for Left Join and "right table" for Right Join.

Similarly,

The phrase "right/left table" means "right table" for Left Join and "left table" for Right Join.

## Join Function

### Inner Join

#### Definition
Only data from both left and right tables that meet the join conditions will be returned, which can be regarded as the intersection of data from two tables that meet the join conditions.

#### Grammar
```sql
SELECT ... FROM table_name1 [INNER] JOIN table_name2 [ON ...] [WHERE ...] [...]
Or
SELECT ... FROM table_name1, table_name2 WHERE ... [...]
```
#### Result set
Cartesian product set of left and right table row data that meets the join conditions.

#### Scope
Inner Join are supported between super tables, normal tables, child tables, and subqueries.

#### Notes
- For the first type syntax, the `INNER` keyword is optional. The primary join condition and other join conditions can be specified in `ON` and/or `WHERE`, and filters can also be specified in `WHERE`. At least one of `ON`/`WHERE` must be specified.
- For the second type syntax, all primary join condition, other join conditions, and filters can be specified in `WHERE`.
- When performing Inner Join on the super table, the Tag column equivalent conditions with the `AND` relationship of the primary join condition will be used as a similar grouping condition, so the output result cannot remain time serious ordered.

#### Examples

The timestamp when the voltage is greater than 220V occurs simultaneously in table d1001 and table d1002 and their respective voltage values:
```sql
SELECT a.ts, a.voltage, b.voltage FROM d1001 a JOIN d1002 b ON a.ts = b.ts and a.voltage > 220 and b.voltage > 220
```


### Left/Right Outer Join


#### Definition
It returns data sets that meet the join conditions for both left and right tables, as well as data sets that do not meet the join conditions in the left/right tables.

#### Grammar
```sql
SELECT ... FROM table_name1 LEFT|RIGHT [OUTER] JOIN table_name2 ON ... [WHERE ...] [...]
```

#### Result set
The result set of Inner Join are rows in the left/right table that do not meet the join conditions combining with null data (`NULL`) in the right/left table.

#### Scope
Left/Right Outer Join are supported between super tables, normal tables, child tables, and subqueries.

#### Notes
- the `OUTER` keyword is optional.

#### Examples

Timestamp and voltage values at all times in table d1001 and the timestamp when the voltage is greater than 220V occurs simultaneously in table d1001 and table d1002 and their respective voltage values:
```sql
SELECT a.ts, a.voltage, b.voltage FROM d1001 a LEFT JOIN d1002 b ON a.ts = b.ts and a.voltage > 220 and b.voltage > 220
```

### Left/Right Semi Join

#### Definition
It usually expresses the meaning of `IN`/`EXISTS`, which means that for any data in the left/right table, only when there is any row data in the right/left table that meets the join conditions, will the left/right table row data be returned.

#### Grammar
```sql
SELECT ... FROM table_name1 LEFT|RIGHT SEMI JOIN table_name2 ON ... [WHERE ...] [...]
```

#### Result set
The row data set composed of rows that meet the join conditions in the left/right table and any one row that meets the join conditions in the right/left table.

#### Scope
Left/Right Semi Join are supported between super tables, normal tables, child tables, and subqueries.

#### Examples

The timestamp when the voltage in table d1001 is greater than 220V and there are other meters with voltages greater than 220V at the same time:
```sql
SELECT a.ts FROM d1001 a LEFT SEMI JOIN meters b ON a.ts = b.ts and a.voltage > 220 and b.voltage > 220 and b.tbname != 'd1001'
```

### Left/Right Anti-Semi Join

#### Definition
Opposite meaning to the Left/Right Semi Join. It usually expresses the meaning of `NOT IN`/`NOT EXISTS`, that is, for any row data in the left/right table, only will be returned when there is no row data that meets the join conditions in the right/left table.

#### Grammar
```sql
SELECT ... FROM table_name1 LEFT|RIGHT ANTI JOIN table_name2 ON ... [WHERE ...] [...]
```

#### Result set
A collection of rows in the left/right table that do not meet the join conditions and null data (`NULL`) in the right/left table.

#### Scope
Left/Right Anti-Semi Join are supported between super tables, normal tables, child tables, and subqueries.

#### Examples

The timestamp when the voltage in table d1001 is greater than 220V and there is not any other meters with voltages greater than 220V at the same time:
```sql
SELECT a.ts FROM d1001 a LEFT ANTI JOIN meters b ON a.ts = b.ts and b.voltage > 220 and b.tbname != 'd1001' WHERE a.voltage > 220
```

### left/Right ASOF Join

#### Definition
Different from other traditional join's exact matching patterns, ASOF Join allows for incomplete matching in a specified matching pattern, that is, matching in the manner closest to the primary key timestamp.

#### Grammar
```sql
SELECT ... FROM table_name1 LEFT|RIGHT ASOF JOIN table_name2 [ON ...] [JLIMIT jlimit_num] [WHERE ...] [...]
```

##### Result set
The Cartesian product set of up to `jlimit_num` rows data or null data (`NULL`) closest to the timestamp of each row in the left/right table, ordered by primary key, that meets the join conditions in the right/left table.


##### Scope
Left/Right ASOF Join are supported between super tables, normal tables, child tables.

#### Notes
- Only supports ASOF Join between tables, not between subqueries.
- The `ON` clause supports a single matching rule (primary join condition) with the primary key column or the timetruncate function operation of the primary key column (other scalar operations and functions are not supported). The supported operators and their meanings are as follows:


  |    **Operator**   |       **Meaning for Left ASOF Join**       |
  | :-------------: | ------------------------ |
  | &gt;    | Match rows in the right table whose primary key timestamp is less than and the most closed to the left table's primary key timestamp      |
  | &gt;=    | Match rows in the right table whose primary key timestamp is less than or equal to and the most closed to the left table's primary key timestamp  |
  | =    | Match rows in the right table whose primary key timestamp is equal to the left table's primary key timestamp  |
  | &lt;    | Match rows in the right table whose the primary key timestamp is greater than and the most closed to the left table's primary key timestamp  |
  | &lt;=    | Match rows in the right table whose primary key timestamp is greater than or equal to and the most closed to the left table's primary key timestamp  |

  For Right ASOF Join, the above operators have the opposite meaning.

- If there is no `ON` clause or no primary join condition is specified in the `ON` clause, the default primary join condition operator will be “>=”， that is, (for Left ASOF Join) matching rows in the right table whose primary key timestamp is less than or equal to the left table's primary key timestamp. Multiple primary join conditions are not supported.
- In the `ON` clause, except for the primary key column, equivalent conditions between Tag columns and ordinary columns (which do not support scalar functions and operations) can be specified for grouping calculations. Other types of conditions are not supported.
- Only `AND` operation is supported between all `ON` conditions.
- `JLIMIT` is used to specify the maximum number of rows for a single row match result. It's optional. The default value is 1 when not specified, which means that each row of data in the left/right table can obtain at most one row of matching results from the right/left table. The value range of `JLIMIT` is [0,1024]. All the `jlimit_num` rows data that meet the join conditions do not require the same timestamp. When there are not enough `jlimit_num` rows data that meet the conditions in the right/left table, the number of returned result rows may be less than `jlimit_num`. When there are more than `jlimit_num` rows data that meet the conditions in the right/left table and all their timestamps are the same, random `jlimit_num` rows data will be returned.

#### Examples

The moment that voltage in table d1001 is greater than 220V and at the same time or at the last moment the voltage in table d1002 is also greater than 220V and their respective voltage values:
```sql
SELECT a.ts, a.voltage, a.ts, b.voltage FROM d1001 a LEFT ASOF JOIN d1002 b ON a.ts >= b.ts where a.voltage > 220 and b.voltage > 220 
```

### Left/Right Window Join

#### Definition
Construct windows based on the primary key timestamp of each row in the left/right table and the window boundary, and then perform window join accordingly, supporting projection, scalar, and aggregation operations within the window.

#### Grammar
```sql
SELECT ... FROM table_name1 LEFT|RIGHT WINDOW JOIN table_name2 [ON ...] WINDOW_OFFSET(start_offset, end_offset) [JLIMIT jlimit_num] [WHERE ...] [...]
```

#### Result set
The Cartesian product of each row of data in the left/right table and null data (`NULL`) or up to `jlimit_num` rows of data in the constructed window(based on the left/right table primary key timestamp and `WINDOW_OFFSET`) in the right/left table.
Or 
The Cartesian product of each row of data in the left/right table and null data (`NULL`) or the aggregation result of up to `jlimit_num` rows of data in the constructed window(based on the left/right table primary key timestamp and `WINDOW_OFFSET`) in the right/left table.

#### Scope
Left/Right Window Join are supported between super tables, normal tables, child tables.

#### Notes
- Only supports Window Join between tables, not between subqueries.
- The `ON` clause is optional. Except for the primary key column, equivalent conditions between Tag columns and ordinary columns (which do not support scalar functions and operations) can be specified in `ON` clause for grouping calculations. Other types of conditions are not supported.
- Only `AND` operation is supported between all `ON` conditions.
- `WINDOW_OFFSET` is used to specify the offset of the left and right boundaries of the window relative to the timestamp of the left/right table's primary key. It supports the form of built-in time units. For example: `WINDOW_OFFSET (-1a, 1a)`, for Left Window Join, it means that each window boundary is [left table primary key timestamp - 1 millisecond, left table primary key timestamp + 1 millisecond], and both the left and right boundaries are closed intervals. The time unit after the number can be `b` (nanosecond), `u` (microsecond), `a` (millisecond), `s` (second), `m` (minute), `h` (hour), `d` (day), `w` (week). Natural months (`n`) and natural years (`y`) are not supported. The minimum time unit supported is database precision. The precision of the databases where the left and right tables are located should be the same.
- `JLIMIT` is used to specify the maximum number of matching rows in a single window. Optional. If not specified, all matching rows in each window are obtained by default. The value range of `JLIMIT` is [0,1024]. Less than `jlimit_num` rows of data will be returned when there are not enough `jlimit_num` rows of data in the right table that meet the condition. When there are more than `jlimit_num` rows of data in the right table that meet the condition, `jlimit_num` rows of data with the smallest primary key timestamp in the window will be returned.
- No `GROUP BY`/`PARTITION BY`/Window queries could be used together with Window Join in one single SQL statement.
- Supports scalar filtering in the `WHERE` clause, aggregation function filtering for each window in the `HAVING` clause (does not support scalar filtering), does not support `SLIMIT`, and does not support various window pseudo-columns.

#### Examples

The voltage value of table d1002 within 1 second before and after the moment that voltage value of table d1001 is greater than 220V:
```sql
SELECT a.ts, a.voltage, b.voltage FROM d1001 a LEFT WINDOW JOIN d1002 b WINDOW_OFFSET（-1s, 1s) where a.voltage > 220
```

The moment that the voltage value of table d1001 is greater than 220V and the average voltage value of table d1002 is also greater than 220V in the interval of 1 second before and after that:
```sql
SELECT a.ts, a.voltage, avg(b.voltage) FROM d1001 a LEFT WINDOW JOIN d1002 b WINDOW_OFFSET（-1s, 1s) where a.voltage > 220 HAVING(avg(b.voltage) > 220)
```

### Full Outer Join

#### Definition
It includes data sets that meet the join conditions for both left and right tables, as well as data sets that do not meet the join conditions in the left and right tables.

#### Grammar
SELECT ... FROM table_name1 FULL [OUTER] JOIN table_name2 ON ... [WHERE ...] [...]

#### Result set
The result set of Inner Join + rows data set composed of rows in the left table that do not meet the join conditions and null data(`NULL`) in the right table + rows data set composed of rows in the right table that do not meet the join conditions and null data(`NULL`) in the left table.

#### Scope
Full Outer Join is supported between super tables, normal tables, child tables, and subqueries.

#### Notes
- the `OUTER` keyword is optional.

#### Examples

All timestamps and voltage values recorded in both tables d1001 and d1002:
```sql
SELECT a.ts, a.voltage, b.ts, b.voltage FROM d1001 a FULL JOIN d1002 b on a.ts = b.ts
```

## Limitations

### Input timeline limits
- Currently, all types of join require input data to contain a valid primary key timeline, which can be satisfied by all table queries. Subqueries need to pay attention to whether the output data contains a valid primary key timeline.

### Join conditions limits
- Except for ASOF and Window Join, the join conditions of other types of join must include the primary join condition; 
- Only `AND` operation is supported between the primary join condition and other join conditions.
- The primary key column used in the primary join condition only supports `timetruncate` function operations (not other functions and scalar operations), and there are no restrictions when used as other join conditions.

### Grouping conditions limits
- Only support equivalent conditions for Tag and ordinary columns except for primary key columns.
- Does not support scalar operations.
- Supports multiple grouping conditions, and only supports `AND` operation between conditions.

### Query result order limits
- In scenarios where there are normal tables, subtables, and subqueries without grouping conditions or sorting, the query results will be output in the order of the primary key columns of the driving table.
- In scenarios such as super table queries, Full Join, or with grouping conditions and without sorting, there is no fixed output order for query results.
Therefore, in scenarios where sorting is required and the output is not in a fixed order, sorting operations need to be performed. Some functions that rely on timelines may not be able to execute without soring due to the lack of valid timeline output.

### Nested join and multi-table join limits
- Currently, except for Inner Join which supports nesting and multi-table Join, other types of join do not support nesting and multi-table join.
