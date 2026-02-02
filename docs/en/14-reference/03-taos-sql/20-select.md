---
title: Data Querying
slug: /tdengine-reference/sql-manual/query-data
---

## Query Syntax

```sql
SELECT {DATABASE() | CLIENT_VERSION() | SERVER_VERSION() | SERVER_STATUS() | NOW() | TODAY() | TIMEZONE() | CURRENT_USER() | USER() }

SELECT [hints] [DISTINCT] [TAGS] select_list
    from_clause
    [WHERE condition]
    [partition_by_clause]
    [interp_clause]
    [window_clause]
    [group_by_clause]
    [order_by_clasue]
    [SLIMIT limit_val [SOFFSET offset_val]]
    [LIMIT limit_val [OFFSET offset_val]]
    [>> export_file]

hints: /*+ [hint([hint_param_list])] [hint([hint_param_list])] */

hint:
    BATCH_SCAN | NO_BATCH_SCAN | SORT_FOR_GROUP | PARTITION_FIRST | PARA_TABLES_SORT | SMALLDATA_TS_SORT

select_list:
    select_expr [, select_expr] ...

select_expr: {
    *
  | query_name.*
  | [schema_name.] {table_name | view_name} .*
  | t_alias.*
  | expr [[AS] c_alias]
}

from_clause: {
    table_reference [, table_reference] ...
  | table_reference join_clause [, join_clause] ...
}

table_reference:
    table_expr t_alias

table_expr: {
    table_name
  | view_name
  | ( subquery )
}

join_clause:
    [INNER|LEFT|RIGHT|FULL] [OUTER|SEMI|ANTI|ASOF|WINDOW] JOIN table_reference [ON condition] [WINDOW_OFFSET(start_offset, end_offset)] [JLIMIT jlimit_num]

window_clause: {
    SESSION(ts_col, tol_val)
  | STATE_WINDOW(col [, extend[, zeroth_state]]) [TRUE_FOR(true_for_expr)]
  | INTERVAL(interval_val [, interval_offset]) [SLIDING (sliding_val)] [WATERMARK(watermark_val)] [fill_clause]
  | EVENT_WINDOW START WITH start_trigger_condition END WITH end_trigger_condition [TRUE_FOR(true_for_expr)]
  | COUNT_WINDOW(count_val[, sliding_val][, col_name ...])
}

interp_clause:
    RANGE(ts_val [, ts_val]) EVERY(every_val) fill_clause

fill_clause:
    FILL(fill_mode_and_val) [SURROUND(surrounding_time_val [, fill_vals])]

fill_mode_and_val:
    NONE
  | NULL|NULL_F
  | VALUE|VALUE_F [, fill_vals]
  | PREV|NEXT|NEAR
  | LINEAR
 
group_by_clause:
    GROUP BY group_by_expr [, group_by_expr] ... HAVING condition
                                                    
group_by_expr:
    {expr | position | c_alias}

partition_by_clause:
    PARTITION BY partition_by_expr [, partition_by_expr] ...

partition_by_expr:
    {expr | position | c_alias}

order_by_clasue:
    ORDER BY order_expr [, order_expr] ...

order_expr:
    {expr | position | c_alias} [DESC | ASC] [NULLS FIRST | NULLS LAST]

true_for_expr: {
    duration_time
  | COUNT count_val
  | duration_time AND COUNT count_val
  | duration_time OR COUNT count_val
}
```

### Partial Field Description

- select_expr: Select list expressions that can be constants, columns, operations, functions, and their mixed operations, and not support nested aggregate functions.
- from_clause: Specify the data source for the query, which can be a single table (super table, sub table, regular table, virtual table), a view, support multiple table association queries.
- table_reference: Specify the name of a single table (including views), and optionally specify an alias for the table.
- table_expr: Specify the query data source, which can be table name, view name, or subquery.
- join_clause: Join query, supports sub tables, regular tables, super tables, and sub queries. In window join, WINDOW_OFFSET uses start_offset and end_offset to specify the offset of the left and right boundaries of the window relative to the primary keys of the left and right tables. There is no size correlation between the two, this is a required field. Precision can be selected from 1n (nanoseconds), 1u (microseconds), 1a (milliseconds), 1s (seconds), 1m (minutes), 1h (hours), 1d (days), and 1w (weeks), such as window_offset (-1a, 1a). JLIMIT limits the maximum number of rows for single line matching, with a default value of 1 and a value range of [0,1024]. For detailed information, please refer to the join query chapter [TDengine Join Queries](../join-queries/).
- window_clause: Specifies data to be split and aggregated according to the window, it is a distinctive query of time-series databases. For detailed information, please refer to the distinctive query chapter [TDengine Distinctive Queries](../03-taos-sql/24-distinguished.md).
  - SESSION: Session window, ts_col specifies the timestamp primary key column, tol_val specifies the time interval, positive value, and time precision can be selected from 1n, 1u, 1a, 1s, 1m, 1h, 1d, 1w, such as SESSION (ts, 12s).
  - STATE_WINDOW: State window, col specifies the state column. Extend specifies the extension strategy for the start and end of a window. The optional values are 0 (default), 1, and 2, representing no extension, backward extension, and forward extension respectively. The zeroth state refers to the "zero state". Windows with this state in the state column will not be calculated or output, and the input must be an integer, boolean, or string constant. TRUE_FOR specifies the filtering condition for windows. Supports the following four modes:
    - `TRUE_FOR(duration_time)`: Filters based on duration only. The window duration must be greater than or equal to `duration_time`.
    - `TRUE_FOR(COUNT n)`: Filters based on row count only. The window row count must be greater than or equal to `n`.
    - `TRUE_FOR(duration_time AND COUNT n)`: Both duration and row count conditions must be satisfied.
    - `TRUE_FOR(duration_time OR COUNT n)`: Either duration or row count condition must be satisfied.

    Where `duration_time` is a positive time value with supported units: 1n (nanoseconds), 1u (microseconds), 1a (milliseconds), 1s (seconds), 1m (minutes), 1h (hours), 1d (days), 1w (weeks). Examples: `TRUE_FOR(1a)`, `TRUE_FOR(COUNT 100)`, `TRUE_FOR(10m AND COUNT 50)`, `TRUE_FOR(5m OR COUNT 20)`.
  - INTERVAL: Time window, interval_val specifies the window size, sliding_val specifies the window sliding time, sliding_val time is limited to the interval_val range, interval_val and sliding_val time ranges are positive values, and precision can be selected from 1n, 1u, 1a, 1s, 1m, 1h, 1d, and 1w, such as interval_val (2d) and SLIDING (1d).
  - EVENT_WINDOW: The event window uses start_trigger_condition and end_trigger_condition to specify start and end conditions, supports any expression, and can specify different columns. TRUE_FOR specifies the filtering condition for windows. Supports the following four modes:
    - `TRUE_FOR(duration_time)`: Filters based on duration only. The window duration must be greater than or equal to `duration_time`.
    - `TRUE_FOR(COUNT n)`: Filters based on row count only. The window row count must be greater than or equal to `n`.
    - `TRUE_FOR(duration_time AND COUNT n)`: Both duration and row count conditions must be satisfied.
    - `TRUE_FOR(duration_time OR COUNT n)`: Either duration or row count condition must be satisfied.

    Where `duration_time` is a positive time value with supported units: 1n (nanoseconds), 1u (microseconds), 1a (milliseconds), 1s (seconds), 1m (minutes), 1h (hours), 1d (days), 1w (weeks). Examples: `TRUE_FOR(10m)`, `TRUE_FOR(COUNT 100)`, `TRUE_FOR(10m AND COUNT 50)`, `TRUE_FOR(5m OR COUNT 20)`.
  - COUNT_WINDOW: Count window, specifying the division of the window by the number of rows, count_val window contains the maximum number of rows, with a range of [2,2147483647]. The sliding quantity of the window is [1, count_val].The col_name parameter starts to be supported after version 3.3.7.0. col_name specifies one or more columns. When counting in the count_window, for each row of data in the window, at least one of the specified columns must be non-null; otherwise, that row of data is not included in the counting window. If col_name is not specified, it means there is no non-null restriction.
- interp_clause: Interp clause, used in conjunction with the interp function, specifying the recorded value or interpolation of the time section, can specify the time range of interpolation, output time interval, and interpolation type.
  - RANGE: Specify a single or start end time value, the end time must be greater than the start time. ts_val is a standard timestamp type. Such as ```RANGE('2023-10-01T00:00:00.000')``` or ```RANGE('2023-10-01T00:00:00.000', '2023-10-01T23:59:59.999')```.
  - EVERY: Time interval range, with every_val being a positive value and precision options of 1n, 1u, 1a, 1s, 1m, 1h, 1d, and 1w, such as EVERY (1s).
- fill_clause: Fill clause, can be used with interp function or interval window, to specify the data filling method when data is missing.
- group_by_expr: Specify data grouping and aggregation rules. Supports expressions, functions, positions, columns, and aliases. When using positional syntax, it must appear in the selection column, such as `select ts, current from meters order by ts desc, 2`, where 2 corresponds to the current column.
- partition_by_expr: Specify the data slicing conditions, and calculate the data independently within the slice. Supports expressions, functions, positions, columns, and aliases. When using positional syntax, it must appear in the selection column, such as `select current from meters partition by 1`, where 1 corresponds to the current column.
- order_expr: Specify the sorting rule for the output data, which is not sorted by default. Supports expressions, functions, positions, columns, and aliases. Different sorting rules can be used for each column in a single or multiple columns, and null values can be specified to be sorted first or last.
- SLIMIT: Specify the number of output shards, limit_val specifies the number of outputs, offset_val specifies the start position of the offset, offset_val is optional, limit_val and offset_val are both positive values, used in the PARTITION BY and GROUP BY clauses. Only output one shard when using the ORDER BY clause.
- LIMIT: Specify the number of output data, limit_val specifies the number of outputs, offset_val specifies the start position of the offset, offset_val is optional, both limit_val and offset_val are positive values. When using the PARTITION BY clause, the number of shards per shard is controlled.

## Hints

Hints are a means for users to control the optimization of individual statement queries. When a Hint is not applicable to the current query statement, it will be automatically ignored. The specific instructions are as follows:

- Hints syntax starts with `/*+` and ends with `*/`, spaces may exist before and after.
- Hints syntax can only follow the SELECT keyword.
- Each Hints can contain multiple Hints, separated by spaces. If multiple Hints conflict or are the same, the first one prevails.
- If an error occurs in one of the Hints, the valid Hints before the error remain effective, and the current and subsequent Hints are ignored.
- hint_param_list is the parameter list for each Hint, which varies depending on the Hint.

The currently supported Hints list is as follows:

|      **Hint**       | **Parameter** | **Description**                                                                                                                                                                       | **Applicable Scope**                            |
|:-------------------:|---------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------|
|     BATCH_SCAN      | None          | Use batch table reading                                                                                                                                                               | Supertable JOIN statements                      |
|    NO_BATCH_SCAN    | None          | Use sequential table reading                                                                                                                                                          | Supertable JOIN statements                      |
|   SORT_FOR_GROUP    | None          | Use sort method for grouping, conflicts with PARTITION_FIRST                                                                                                                          | When partition by list includes regular columns |
|   PARTITION_FIRST   | None          | Use PARTITION to calculate groups before aggregation, conflicts with SORT_FOR_GROUP                                                                                                   | When partition by list includes regular columns |
|  PARA_TABLES_SORT   | None          | When sorting supertable data by timestamp, use memory instead of temporary disk space. When there are many subtables and rows are large, it will use a lot of memory and may cause OOM | When sorting supertable data by timestamp       |
|  SMALLDATA_TS_SORT  | None          | When sorting supertable data by timestamp, if the query column length is greater than or equal to 256 but the number of rows is not large, using this hint can improve performance    | When sorting supertable data by timestamp       |
|      SKIP_TSMA      | None          | Explicitly disable TSMA query optimization                                                                                                                                            | Queries with Agg functions                      |

Examples:

```sql
SELECT /*+ BATCH_SCAN() */ a.ts FROM stable1 a, stable2 b where a.tag0 = b.tag0 and a.ts = b.ts;
SELECT /*+ SORT_FOR_GROUP() */ count(*), c1 FROM stable1 PARTITION BY c1;
SELECT /*+ PARTITION_FIRST() */ count(*), c1 FROM stable1 PARTITION BY c1;
SELECT /*+ PARA_TABLES_SORT() */ * from stable1 order by ts;
SELECT /*+ SMALLDATA_TS_SORT() */ * from stable1 order by ts;
```

## List

Query statements can specify some or all columns as the return results. Both data columns and tag columns can appear in the list.

### Wildcard

The wildcard * can be used to refer to all columns. For basic tables and subtables, only regular columns are included in the results. For supertables, tag columns are also included.

```sql
SELECT * FROM d1001;
```

The wildcard supports table name prefixes, the following two SQL statements both return all columns:

```sql
SELECT * FROM d1001;
SELECT d1001.* FROM d1001;
```

In JOIN queries, there is a difference between a prefixed *and an unprefixed*; * returns all column data from all tables (excluding tags), while a prefixed wildcard returns only the column data from that table.

```sql
SELECT * FROM d1001, d1003 WHERE d1001.ts=d1003.ts;
SELECT d1001.* FROM d1001,d1003 WHERE d1001.ts = d1003.ts;
```

In the above query statements, the former returns all columns from both d1001 and d1003, while the latter only returns all columns from d1001.

In the process of using SQL functions for queries, some SQL functions support wildcard operations. The difference is:
The `count(*)` function only returns one column. The `first`, `last`, `last_row` functions return all columns.

### Tag Columns

In queries involving supertables and subtables, *tag columns* can be specified, and the values of the tag columns are returned along with the data of the regular columns.

```sql
SELECT location, groupid, current FROM d1001 LIMIT 2;
```

### Aliases

The naming rules for aliases are the same as for columns, supporting direct specification of Chinese aliases in UTF-8 encoding format.

### Deduplication of Results

The `DISTINCT` keyword can be used to deduplicate one or more columns in the result set, and the columns can be either tag columns or data columns.

Deduplication of tag columns:

```sql
SELECT DISTINCT tag_name [, tag_name ...] FROM stb_name;
```

Deduplication of data columns:

```sql
SELECT DISTINCT col_name [, col_name ...] FROM tb_name;
```

:::info

1. The configuration parameter maxNumOfDistinctRes in the cfg file limits the number of data rows that DISTINCT can output. The minimum value is 100000, the maximum value is 100000000, and the default value is 10000000. If the actual calculation result exceeds this limit, only a portion within this range will be output.
2. Due to the inherent precision mechanism of floating-point numbers, using DISTINCT on FLOAT and DOUBLE columns may not guarantee the complete uniqueness of the output values under specific conditions.

:::

### Tag Query

When only tag columns are queried, the `TAGS` keyword can specify the return of tag columns for all subtables. Each subtable returns one row of tag columns.

Return the tag columns of all subtables:

```sql
SELECT TAGS tag_name [, tag_name ...] FROM stb_name
```

### Result Set Column Names

In the `SELECT` clause, if the column names of the result set are not specified, the default column names of the result set use the expression names in the `SELECT` clause. Additionally, users can use `AS` to rename the columns in the result set. For example:

```sql
taos> SELECT ts, ts AS primary_key_ts FROM d1001;
```

However, renaming individual columns is not supported for `first(*)`, `last(*)`, `last_row(*)`.

### Pseudo Columns

**Pseudo Columns**: The behavior of pseudocolumns is similar to regular data columns, but they are not actually stored in the table. Pseudocolumns can be queried, but cannot be inserted, updated, or deleted. Pseudo columns are somewhat like functions without parameters. Below are the available pseudo columns:

**TBNAME**
`TBNAME` can be considered a special tag in a supertable, representing the table name of a subtable.

Retrieve all subtable names and related tag information from a supertable:

```sql
SELECT TAGS TBNAME, location FROM meters;
```

It is recommended that users query the subtable tag information of supertables using the INS_TAGS system table under INFORMATION_SCHEMA, for example, to get all subtable names and tag values of the supertable meters:

```sql
SELECT table_name, tag_name, tag_type, tag_value FROM information_schema.ins_tags WHERE stable_name='meters';
```

Count the number of subtables under a supertable:

```sql
SELECT COUNT(*) FROM (SELECT DISTINCT TBNAME FROM meters);
```

Both queries only support adding filtering conditions for tags (TAGS) in the WHERE clause.

**\_QSTART/\_QEND**

\_qstart and \_qend represent the query time range input by the user, i.e., the time range limited by the primary key timestamp condition in the WHERE clause. If there is no valid primary key timestamp condition in the WHERE clause, the time range is [-2^63, 2^63-1].

\_qstart and \_qend cannot be used in the WHERE clause.

**\_WSTART/\_WEND/\_WDURATION**
\_wstart pseudocolumn, \_wend pseudo column, and \_wduration pseudo column
\_wstart represents the window start timestamp, \_wend represents the window end timestamp, \_wduration represents the window duration.

These three pseudocolumns can only be used in window slicing queries within time windows, and must appear after the window slicing clause.

**\_c0/\_ROWTS**

In TDengine, the first column of all tables must be of timestamp type and serve as the primary key. The pseudocolumns `_rowts` and `_c0` both represent the value of this column. Compared to the actual primary key timestamp column, using pseudo-columns is more flexible and semantically standard. For example, they can be used with functions like max and min.

```sql
select _rowts, max(current) from meters;
```

**\_IROWTS**

The `_irowts` pseudocolumn can only be used with the interp function to return the timestamp column corresponding to the interpolation result of the interp function.

```sql
select _irowts, interp(current) from meters range('2020-01-01 10:00:00', '2020-01-01 10:30:00') every(1s) fill(linear);
```

**\_IROWTS\_ORIGIN**
The `_irowts_origin` pseudocolumn can only be used with the interp function, and is only applicable for FILL types PREV/NEXT/NEAR. It returns the timestamp column of the original data used by the interp function. If there are no values within the range, it returns NULL.

```sql
select _iorwts_origin, interp(current) from meters range('2020-01-01 10:00:00', '2020-01-01 10:30:00') every(1s) fill(NEXT);
```

## Query Objects

The FROM keyword can be followed by a list of tables (supertables) or the result of a subquery.
If the user's current database is not specified, the database name can be used before the table name to specify the database to which the table belongs. For example, using `power.d1001` to cross-database use tables.

TDengine supports INNER JOIN based on the timestamp primary key, with the following rules:

1. Supports both FROM table list and explicit JOIN clause syntax.
2. For basic tables and subtables, the ON condition must have and only have an equality condition on the timestamp primary key.
3. For supertables, in addition to the equality condition on the timestamp primary key, the ON condition also requires an equality condition on the label columns that can be corresponded one-to-one, and does not support OR conditions.
4. Tables involved in JOIN calculations must be of the same type, i.e., all must be supertables, subtables, or basic tables.
5. Both sides of JOIN support subqueries.
6. Does not support mixing with the FILL clause.

## INTERP

The INTERP clause is a dedicated syntax for the [INTERP function](./22-function.md#interp). When an SQL statement contains an INTERP clause, it can only query the INTERP function and cannot be used with other functions. Additionally, the INTERP clause cannot be used simultaneously with window clauses (window_clause) or group by clauses (group_by_clause). The INTERP function must be used with the RANGE, EVERY, and FILL clauses.

- The output time range for INTERP is specified by the RANGE(timestamp1, timestamp2) field, which must satisfy timestamp1 \<= timestamp2. Here, timestamp1 is the start value of the output time range, i.e., if the conditions for interpolation are met at timestamp1, then timestamp1 is the first record output, and timestamp2 is the end value of the output time range, i.e., the timestamp of the last record output cannot be greater than timestamp2.
- INTERP determines the number of results within the output time range based on the EVERY(time_unit) field, starting from timestamp1 and interpolating at fixed intervals of time (time_unit value), where time_unit can be time units: 1a (milliseconds), 1s (seconds), 1m (minutes), 1h (hours), 1d (days), 1w (weeks). For example, EVERY(500a) will interpolate the specified data every 500 milliseconds.
- INTERP determines how to interpolate at each time point that meets the output conditions based on the FILL field. For how to use the FILL clause, refer to [FILL Clause](./20-select.md#fill-clause). Note: The sampled data used for interpolation is not limited to the constraints of the RANGE field, but rather to all data that meets the conditions of the WHERE clause; if no WHERE clause is specified, the entire table data is used. When the parameter of the FILL clause is PREV/NEXT/NEAR, adjacent valid data will be used for interpolation. Whether NULL data is considered valid data depends on the ignore_null_values parameter of the INTERP function. To limit the scope of sampled data, you can use the SURROUND clause.
- INTERP can interpolate at a single time point specified in the RANGE field, in which case the EVERY field can be omitted. For example: `SELECT INTERP(col) FROM tb RANGE('2023-01-01 00:00:00') FILL(linear)`.
- INTERP query supports NEAR FILL mode, i.e., when FILL is needed, it uses the valid data closest to the current time point for interpolation. When the timestamps before and after are equally close to the current time slice, FILL the previous row's value. This mode is not supported in window queries. For example: `SELECT INTERP(col) FROM tb RANGE('2023-01-01 00:00:00', '2023-01-01 00:10:00') FILL(NEAR)` (Supported from version 3.3.4.9).

## FILL Clause

The FILL statement specifies the filling mode when data is missing in a window interval. The filling modes include:

1. No filling: NONE (default filling mode).
2. VALUE filling: Fixed value filling, where the fill value must be specified. For example `FILL(VALUE, 1.23)`. Note that the final fill value is determined by the type of the corresponding column, such as `FILL(VALUE, 1.23)`, if the corresponding column is of INT type, then the fill value is 1. If multiple columns in the query list need FILL, then each FILL column must specify a VALUE, such as `SELECT _wstart, min(c1), max(c1) FROM ... FILL(VALUE, 0, 0)`. Note, only ordinary columns in the SELECT expression need to specify FILL VALUE, such as `_wstart`, `_wstart+1a`, `now`, `1+1` and the `partition key` (like tbname) used with `partition by` do not need to specify VALUE, like `timediff(last(ts), _wstart)` needs to specify VALUE.
3. NULL filling: Fill data with NULL. For example `FILL(NULL)`.
4. PREV filling: Fill data with the previous valid value. For example `FILL(PREV)`.
5. NEXT filling: Fill data with the next valid value. For example `FILL(NEXT)`.
6. NEAR filling: Fill data with the nearest valid value. For example `FILL(NEAR)`. Not supported in window queries.
7. LINEAR filling: Perform linear interpolation filling based on the nearest valid values before and after. For example `FILL(LINEAR)`.

Among all filling modes above, except for the NONE mode which does not fill by default, other modes will not produce fill values if there is no data in the entire query time range, and the query result will be empty. For PREV, NEXT, LINEAR modes, this is reasonable because without valid data, filling cannot be performed.

The definition of "valid data" differs between the INTERVAL clause and the INTERP clause: in the INTERVAL clause, all scanned data are valid data, for example FILL(PREV) uses the data from the adjacent previous window for filling; in the INTERP clause, whether NULL values are valid depends on the ignore_null_values parameter of the INTERP function, for example FILL(PREV) with NULL values invalid, will skip all NULL values and keep searching forward for non-NULL data, if all data are NULL, no filling is performed. In the INTERP clause, under PREV, NEXT, and NEAR modes, it will continue to search forward/backward/both directions for valid data within the WHERE condition range, if all data are NULL, no filling is performed.

For other modes (NULL, VALUE), theoretically fill values can be generated, whether to output fill values depends on the application's needs. To meet the needs of applications that require forced filling of data or NULL, while not breaking the compatibility of existing filling modes, starting from version 3.0.3.0, two new filling modes have been added:

1. NULL_F: Force fill with NULL values
1. VALUE_F: Force fill with VALUE values

The differences between NULL, NULL_F, VALUE, VALUE_F filling modes for different scenarios are as follows:

- INTERVAL clause: NULL_F, VALUE_F are forced filling modes; NULL, VALUE are non-forced modes. In this mode, their semantics match their names.
- Stream computing's INTERVAL clause: NULL_F behaves the same as NULL, both are non-forced modes; VALUE_F behaves the same as VALUE, both are non-forced modes. Thus, there are no forced modes in the INTERVAL of stream computing.
- INTERP clause: NULL and NULL_F behave the same, both are forced modes; VALUE and VALUE_F behave the same, both are forced modes. Thus, there are no non-forced modes in INTERP.

:::info

1. When using the FILL statement, a large amount of fill output may be generated, so be sure to specify the query time range. For each query, the system can return up to 10 million results with interpolation.
2. In time dimension aggregation, the returned results have a strictly monotonically increasing time-series.
3. If the query object is a supertable, the aggregate functions will apply to all tables under the supertable that meet the value filtering conditions. If the query does not use a PARTITION BY statement, the returned results will have a strictly monotonically increasing time-series; if the query uses a PARTITION BY statement for grouping, the results within each PARTITION will have a strictly monotonically increasing time series.
4. FILL has continuity, if only the first value in a column is not NULL, then fill(prev) will fill all subsequent rows with that value.

:::

### SURROUND Clause

Used to limit the filling range of the FILL clause. Can only be used in PREV, NEXT, NEAR modes.

The surrounding_time_val parameter is used to specify the time range for valid data. If the time difference between a row with valid data and the current row exceeds surrounding_time_val, its data will not be used, and instead the values specified by the fill_vals parameter will be used for filling. The value must be positive, and the unit can be any time unit except month (n) and year (y). In interval window queries, since data has the same time interval, the surrounding_time_val parameter must exceed the time length of the interval window.

The fill_vals parameter is used to specify the filling values. The number and format are the same as the VALUE filling mode of the FILL clause. It can be constants or constant expressions, and subqueries are not supported.

Example:

```sql
select * from fill_example;
           ts            |   c1        |
========================================
 2026-01-01 00:00:00.000 | 2026        |
 2026-01-01 00:00:01.000 | NULL        |
 2026-01-01 00:00:02.000 | NULL        |
 2026-01-01 00:00:03.000 | NULL        |
 2026-01-01 00:00:04.000 | NULL        |
 2026-01-01 00:00:05.000 | NULL        |
 2026-01-01 00:00:06.000 | 6202        |

select _irowts as ts, interp(c1) from fill_example range('2026-01-01 00:00:01', '2026-01-01 00:00:05') every(1s) fill(near);
           ts            |   c1        |
========================================
 2026-01-01 00:00:01.000 | 2026        |
 2026-01-01 00:00:02.000 | 2026        |
 2026-01-01 00:00:03.000 | 2026        |
 2026-01-01 00:00:04.000 | 6202        |
 2026-01-01 00:00:05.000 | 6202        |

select _irowts as ts, interp(c1) from fill_example range('2026-01-01 00:00:01', '2026-01-01 00:00:05') every(1s) fill(near) surround(2s, 0);
           ts            |   c1        |
========================================
 2026-01-01 00:00:01.000 | 2026        |
 2026-01-01 00:00:02.000 | 2026        |
 2026-01-01 00:00:03.000 | 0           |
 2026-01-01 00:00:04.000 | 6202        |
 2026-01-01 00:00:05.000 | 6202        |
```

## GROUP BY

If a GROUP BY clause is specified in the statement, the SELECT list can only contain the following expressions:

1. Constants
2. Aggregate functions
3. Expressions identical to those after GROUP BY.
4. Expressions containing the above expressions

The GROUP BY clause groups each row of data according to the value of the expression after GROUP BY and returns a summary row for each group.

The GROUP BY clause can group by any column in the table or view by specifying the column name, which does not need to appear in the SELECT list.

The GROUP BY clause can use positional syntax, where the position is a positive integer starting from 1, indicating the grouping by the nth expression in the SELECT list.

The GROUP BY clause can use the result set column name, indicating grouping by the specified expression in the SELECT list.

When using positional syntax and result set column names for grouping in the GROUP BY clause, the corresponding expressions in the SELECT list cannot be aggregate functions.

This clause groups rows but does not guarantee the order of the result set. To sort the groups, use the ORDER BY clause.

## PARTITION BY

The PARTITION BY clause is a distinctive syntax introduced in TDengine 3.0, used to partition data based on part_list, allowing various calculations within each partition slice.

PARTITION BY is similar in basic meaning to GROUP BY, both involving grouping data by a specified list and then performing calculations. The difference is that PARTITION BY does not have the various restrictions of the GROUP BY clause's SELECT list, allowing any operation within the group (constants, aggregates, scalars, expressions, etc.), thus PARTITION BY is fully compatible with GROUP BY, and all places using the GROUP BY clause can be replaced with PARTITION BY. Note that without aggregate queries, the results of the two may differ.

Since PARTITION BY does not require returning a single row of aggregated data, it also supports various window operations after group slicing, and all window operations requiring grouping can only use the PARTITION BY clause.

See [TDengine Distinctive Queries](../time-series-extensions/)

## ORDER BY

The ORDER BY clause sorts the result set. If ORDER BY is not specified, the order of the result set returned by the same query multiple times cannot be guaranteed.

ORDER BY can use positional syntax, where the position is indicated by a positive integer starting from 1, representing the expression in the SELECT list used for sorting.

ASC indicates ascending order, and DESC indicates descending order.

The NULLS syntax is used to specify the position of NULL values in the output of the sorting. NULLS LAST is the default for ascending order, and NULLS FIRST is the default for descending order.

## LIMIT

LIMIT controls the number of output rows, and OFFSET specifies starting from which row to begin output. The execution order of LIMIT/OFFSET is after ORDER BY. LIMIT 5 OFFSET 2 can be abbreviated as LIMIT 2, 5, both outputting data from row 3 to row 7.

When there is a PARTITION BY/GROUP BY clause, LIMIT controls the output within each partition slice, not the total result set output.

## SLIMIT

SLIMIT is used with the PARTITION BY/GROUP BY clause to control the number of output slices. SLIMIT 5 SOFFSET 2 can be abbreviated as SLIMIT 2, 5, both indicating output from the 3rd to the 7th slice.

Note that if there is an ORDER BY clause, only one slice is output.

## Special Features

Some special query functions can be executed without using the FROM clause.

### Get Current Database

The following command retrieves the current database with database(). If no default database was specified at login, and the `USE` command was not used to switch databases, it returns NULL.

```sql
SELECT DATABASE();
```

### Get Server and Client Version Numbers

```sql
SELECT CLIENT_VERSION();
SELECT SERVER_VERSION();
```

### Get Server Status

Server status check statement. If the server is normal, it returns a number (e.g., 1). If the server is abnormal, it returns an error code. This SQL syntax is compatible with connection pools checking the status of TDengine and third-party tools checking the status of database servers. It can also prevent connection pool disconnections caused by incorrect heartbeat check SQL statements.

```sql
SELECT SERVER_STATUS();
```

### Get Current Time

```sql
SELECT NOW();
```

### Get Current Date

```sql
SELECT TODAY();
```

### Get Current Time Zone

```sql
SELECT TIMEZONE();
```

### Get Current User

```sql
SELECT CURRENT_USER();
```

## Regular Expression Filtering

### Syntax

```text
WHERE (column|tbname) match/MATCH/nmatch/NMATCH _regex_
```

### Regular Expression Standards

Ensure that the regular expressions used comply with the POSIX standards, specific standards can be found at [Regular Expressions](https://pubs.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap09.html)

### Usage Restrictions

Regular expression filtering can only be applied to table names (i.e., tbname filtering), binary/nchar type values.

The length of the regular match string cannot exceed 128 bytes. You can set and adjust the maximum allowed regular match string length through the parameter *maxRegexStringLen*, which is a client configuration parameter and requires a restart to take effect.

## CASE Expression

### Syntax

```text
CASE value WHEN compare_value THEN result [WHEN compare_value THEN result ...] [ELSE result] END
CASE WHEN condition THEN result [WHEN condition THEN result ...] [ELSE result] END
```

### Description

TDengine allows users to use IF ... THEN ... ELSE logic in SQL statements through CASE expressions.

The first CASE syntax returns the result where the first value equals compare_value, if no compare_value matches, it returns the result after ELSE, if there is no ELSE part, it returns NULL.

The second syntax returns the result where the first condition is true. If no condition matches, it returns the result after ELSE, if there is no ELSE part, it returns NULL.

The return type of the CASE expression is the result type of the first WHEN THEN part, and the result types of the other WHEN THEN parts and the ELSE part must be convertible to it, otherwise TDengine will report an error.

### Example

A device has three status codes, displaying its status, the statement is as follows:

```sql
SELECT CASE dev_status WHEN 1 THEN 'Running' WHEN 2 THEN 'Warning' WHEN 3 THEN 'Downtime' ELSE 'Unknown' END FROM dev_table;
```

Calculate the average voltage of smart meters, and if the voltage is less than 200 or greater than 250, it is considered a statistical error, and the value is corrected to 220, the statement is as follows:

```sql
SELECT AVG(CASE WHEN voltage < 200 or voltage > 250 THEN 220 ELSE voltage END) FROM meters;
```

## JOIN Clause

Before version 3.3.0.0, TDengine only supported inner joins. From version 3.3.0.0, TDengine supports a wider range of JOIN types, including traditional database joins like LEFT JOIN, RIGHT JOIN, FULL JOIN, SEMI JOIN, ANTI-SEMI JOIN, as well as time-series specific joins like ASOF JOIN, WINDOW JOIN. JOIN operations are supported between subtables, regular tables, supertables, and subqueries.

### Example

JOIN operation between regular tables:

```sql
SELECT *
FROM temp_tb_1 t1, pressure_tb_1 t2
WHERE t1.ts = t2.ts
```

LEFT JOIN operation between supertables:

```sql
SELECT *
FROM temp_stable t1 LEFT JOIN temp_stable t2
ON t1.ts = t2.ts AND t1.deviceid = t2.deviceid AND t1.status=0;
```

LEFT ASOF JOIN operation between a subtable and a supertable:

```sql
SELECT *
FROM temp_ctable t1 LEFT ASOF JOIN temp_stable t2
ON t1.ts = t2.ts AND t1.deviceid = t2.deviceid;
```

For more information on JOIN operations, see the page [TDengine Join Queries](../join-queries/)

## Nested Queries

"Nested queries," also known as "subqueries," mean that in a single SQL statement, the result of the "inner query" can be used as the computation object for the "outer query."

Starting from version 2.2.0.0, TDengine's query engine began to support non-correlated subqueries in the FROM clause (meaning the subquery does not use parameters from the parent query). That is, in the tb_name_list position of a regular SELECT statement, an independent SELECT statement is used instead (enclosed in English parentheses), thus a complete nested query SQL statement looks like:

```sql
SELECT ... FROM (SELECT ... FROM ...) ...;
```

:::info

- The result of the inner query will serve as a "virtual table" for the outer query, and it is recommended to alias this virtual table for easy reference in the outer query.
- The outer query supports direct referencing of columns or pseudocolumns from the inner query by column name or `column name`.
- Both inner and outer queries support regular table-to-table/supertable joins. The result of the inner query can also participate in JOIN operations with data subtables.
- The functional features supported by the inner query are consistent with those of non-nested queries.
  - The ORDER BY clause in the inner query generally has no meaning and is recommended to be avoided to prevent unnecessary resource consumption.
- Compared to non-nested queries, the outer query has the following limitations in supported functional features:
  - Part of calculation functions:
    - If the result data of the inner query does not provide timestamps, then functions implicitly dependent on timestamps will not work properly in the outer query. Examples include: INTERP, DERIVATIVE, IRATE, LAST_ROW, FIRST, LAST, TWA, STATEDURATION, TAIL, UNIQUE.
    - If the result data of the inner query is not ordered by timestamp, then functions dependent on data being ordered by time will not work properly in the outer query. Examples include: LEASTSQUARES, ELAPSED, INTERP, DERIVATIVE, IRATE, TWA, DIFF, STATECOUNT, STATEDURATION, CSUM, MAVG, TAIL, UNIQUE.
    - Functions that require two passes of scanning will not work properly in the outer query. Examples of such functions include: PERCENTILE.

:::

## Non-Correlated Scalar Subqueries

A non-correlated scalar subquery is a type of independent executable subquery in SQL, with its core characteristic being that it returns only a single value (one row, one column), and the execution process is completely independent of any fields from the outer query. Any query statement that conforms to this characteristic can be used as a non-correlated scalar subquery. Non-correlated scalar subqueries can be used in any clause, function, or expression within a query statement, as long as it is syntactically defined as an expression. Non-correlated scalar subqueries can also be nested.

Non-correlated scalar subqueries can independently compute the result first, and then substitute that result into the outer query as a filter condition or reference value. They are commonly used in scenarios involving filtering based on aggregate values (such as average, maximum) or combining results from multiple table queries. Non-correlated scalar subqueries have higher execution efficiency than correlated subqueries.

Since version 3.4.0.0, TDengine TSDB has begun to support non-correlated scalar subqueries in query statements. Other statements (such as stream computations, subscriptions, DDL, DML, etc.) are not yet supported.

Examples of non-correlated scalar subqueries appearing in SELECT and WHERE clauses are as follows:

```sql
SELECT col1, (SELECT sum(col1) FROM tb1) FROM tb2;
SELECT col1 FROM tb2 WHERE col1 >= (SELECT avg(col1) FROM tb1);
```

## UNION Clause

```text title="Syntax"
SELECT ...
UNION [ALL] SELECT ...
[UNION [ALL] SELECT ...]
```

In TDengine, the UNION [ALL] operator is used to combine the results of multiple SELECT clauses. When using this operator, the multiple SELECT clauses must satisfy the following two conditions:

1. Each SELECT clause must return results with the same number of columns;
2. Columns in corresponding positions must be in the same order and have the same or compatible data types.

After combination, the column names of the result set are determined by those defined in the first SELECT clause.

## SQL Examples

For the following example, the table tb1 is created with the statement:

```sql
CREATE TABLE tb1 (ts TIMESTAMP, col1 INT, col2 FLOAT, col3 BINARY(50));
```

Query all records from tb1 for the past hour:

```sql
SELECT * FROM tb1 WHERE ts >= NOW - 1h;
```

Query the table tb1 for the time range from 2018-06-01 08:00:00.000 to 2018-06-02 08:00:00.000, and records where the string of col3 ends with 'nny', results ordered by timestamp in descending order:

```sql
SELECT * FROM tb1 WHERE ts > '2018-06-01 08:00:00.000' AND ts <= '2018-06-02 08:00:00.000' AND col3 LIKE '%nny' ORDER BY ts DESC;
```

Query the sum of col1 and col2, named as complex, where the time is greater than 2018-06-01 08:00:00.000, col2 is greater than 1.2, and only output the first 10 records starting from the 5th:

```sql
SELECT (col1 + col2) AS 'complex' FROM tb1 WHERE ts > '2018-06-01 08:00:00.000' AND col2 > 1.2 LIMIT 10 OFFSET 5;
```

Query records from the past 10 minutes where col2 is greater than 3.14, and output the results to the file `/home/testoutput.csv`:

```sql
SELECT COUNT(*) FROM tb1 WHERE ts >= NOW - 10m AND col2 > 3.14 >> /home/testoutput.csv;
```
