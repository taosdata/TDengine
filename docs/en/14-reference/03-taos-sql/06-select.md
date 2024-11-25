---
title: Query Data
description: Detailed syntax for querying data
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
    [order_by_clause]
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
  | STATE_WINDOW(col)
  | INTERVAL(interval_val [, interval_offset]) [SLIDING (sliding_val)] [WATERMARK(watermark_val)] [FILL(fill_mod_and_val)]
  | EVENT_WINDOW START WITH start_trigger_condition END WITH end_trigger_condition
  | COUNT_WINDOW(count_val[, sliding_val])

interp_clause:
    RANGE(ts_val [, ts_val]) EVERY(every_val) FILL(fill_mod_and_val)

partition_by_clause:
    PARTITION BY partition_by_expr [, partition_by_expr] ...

partition_by_expr:
    {expr | position | c_alias}

group_by_clause:
    GROUP BY group_by_expr [, group_by_expr] ... HAVING condition
                                                    

group_by_expr:
    {expr | position | c_alias}

order_by_clause:
    ORDER BY order_expr [, order_expr] ...

order_expr:
    {expr | position | c_alias} [DESC | ASC] [NULLS FIRST | NULLS LAST]
```

## Hints

Hints are a means for users to control query optimization for individual statements. Hints that do not apply to the current query will be automatically ignored. The specifics are as follows:

- Hints syntax starts with `/*+` and ends with `*/`, with optional spaces around.
- Hints syntax can only follow the SELECT keyword.
- Each hint can contain multiple hints, separated by spaces. When multiple hints conflict or are the same, the first one takes precedence.
- If an error occurs in any hint, all valid hints before the error remain effective, while the current and subsequent hints are ignored.
- hint_param_list consists of parameters for each hint, varying by hint type.

The currently supported list of hints is as follows:

|    **Hint**   |    **Parameters**    |         **Description**           |       **Applicable Scope**         |
| :-----------: | -------------- | -------------------------- | -----------------------------|
| BATCH_SCAN    | None             | Use batch reading from the table         | Supertable JOIN statements             |
| NO_BATCH_SCAN | None             | Use sequential reading from the table         | Supertable JOIN statements             |
| SORT_FOR_GROUP| None             | Use sorting for grouping, conflicts with PARTITION_FIRST  | When partition by list has ordinary columns  |
| PARTITION_FIRST| None             | Use partition calculations for grouping before aggregation, conflicts with SORT_FOR_GROUP | When partition by list has ordinary columns  |
| PARA_TABLES_SORT| None             | When sorting supertable data by timestamp, do not use temporary disk space, only memory. This can consume large amounts of memory and may cause OOM when the number of subtables is high or row length is large. | When sorting supertable data by timestamp  |
| SMALLDATA_TS_SORT| None             | When sorting supertable data by timestamp, if the length of the queried columns is greater than or equal to 256 but the number of rows is not large, this hint can improve performance | When sorting supertable data by timestamp  |
| SKIP_TSMA | None | Used to disable TSMA query optimization | Queries with Agg functions |

Examples:

```sql
SELECT /*+ BATCH_SCAN() */ a.ts FROM stable1 a, stable2 b WHERE a.tag0 = b.tag0 AND a.ts = b.ts;
SELECT /*+ SORT_FOR_GROUP() */ count(*), c1 FROM stable1 PARTITION BY c1;
SELECT /*+ PARTITION_FIRST() */ count(*), c1 FROM stable1 PARTITION BY c1;
SELECT /*+ PARA_TABLES_SORT() */ * FROM stable1 ORDER BY ts;
SELECT /*+ SMALLDATA_TS_SORT() */ * FROM stable1 ORDER BY ts;
```

## Lists

Query statements can specify some or all columns as return results. Both data columns and tag columns can appear in the list.

### Wildcards

The wildcard `*` can be used to refer to all columns. For basic tables and subtables, the result only includes normal columns. For supertables, it also includes TAG columns.

```sql
SELECT * FROM d1001;
```

The wildcard supports prefixes on the table name; the following two SQL statements return all columns:

```sql
SELECT * FROM d1001;
SELECT d1001.* FROM d1001;
```

In JOIN queries, using `*` with or without a table prefix returns different results. The `*` returns all columns from all tables (excluding tags), while the prefixed wildcard returns only the column data from that table.

```sql
SELECT * FROM d1001, d1003 WHERE d1001.ts = d1003.ts;
SELECT d1001.* FROM d1001, d1003 WHERE d1001.ts = d1003.ts;
```

In the above query statements, the first one returns all columns from both d1001 and d1003, while the second one only returns all columns from d1001.

When using SQL functions for queries, some SQL functions support wildcard operations. The difference is that the `count(*)` function returns only one column, while `first`, `last`, and `last_row` functions return all columns.

### Tag Columns

In queries for supertables and subtables, you can specify _tag columns_, and the values of tag columns will be returned along with the data in normal columns.

```sql
SELECT location, groupid, current FROM d1001 LIMIT 2;
```

### Aliases

The naming rules for aliases are the same as for columns, and UTF-8 encoded Chinese aliases are supported.

### Result Deduplication

The `DISTINCT` keyword can deduplicate one or more columns in the result set, removing duplicates from both tag columns and data columns.

Deduplicating tag columns:

```sql
SELECT DISTINCT tag_name [, tag_name ...] FROM stb_name;
```

Deduplicating data columns:

```sql
SELECT DISTINCT col_name [, col_name ...] FROM tb_name;
```

:::info

1. The configuration parameter `maxNumOfDistinctRes` in the cfg file limits the number of data rows that DISTINCT can output. Its minimum value is 100,000, the maximum is 100,000,000, and the default is 10,000,000. If the actual calculation result exceeds this limit, only a portion within this range will be output.
2. Due to the inherent precision mechanisms of floating-point numbers, using DISTINCT on FLOAT and DOUBLE columns does not guarantee completely unique output values in certain cases.

:::

### Tag Queries

When the queried columns consist only of tag columns, the `TAGS` keyword can be used to specify the return of all subtables' tag columns. Each subtable returns only one row of tag columns.

Return all subtables' tag columns:

```sql
SELECT TAGS tag_name [, tag_name ...] FROM stb_name
```

### Result Set Column Names

In the `SELECT` clause, if the column names for the return result set are not specified, the result set's column names will default to the expression names in the `SELECT` clause. Additionally, users can use `AS` to rename the column names in the return result set. For example:

```sql
taos> SELECT ts, ts AS primary_key_ts FROM d1001;
```

However, the functions `first(*)`, `last(*)`, and `last_row(*)` do not support renaming single columns.

### Pseudo Columns

**Pseudo Columns**: Pseudo columns behave similarly to regular data columns but are not actually stored in the table. You can query pseudo columns, but you cannot insert, update, or delete them. Pseudo columns are akin to functions without parameters. Below are the available pseudo columns:

**TBNAME**
`TBNAME` can be regarded as a special tag in the supertable, representing the name of the subtable.

Retrieve all subtable names and related tag information from a supertable:

```mysql
SELECT TAGS TBNAME, location FROM meters;
```

It is recommended to use the INS_TAGS system table under INFORMATION_SCHEMA to query the tag information of subtables under supertables, for example, to retrieve all subtable names and tag values from the supertable `meters`:

```mysql
SELECT table_name, tag_name, tag_type, tag_value FROM information_schema.ins_tags WHERE stable_name='meters';
```

Count the number of subtables under the supertable:

```mysql
SELECT COUNT(*) FROM (SELECT DISTINCT TBNAME FROM meters);
```

The above two queries only support adding filters for tags (TAGS) in the WHERE condition clause.

**\_QSTART/\_QEND**

\_qstart and \_qend represent the query time range specified by the timestamp condition in the WHERE clause. If there is no valid timestamp condition in the WHERE clause, the time range is [-2^63, 2^63-1].

\_qstart and \_qend cannot be used in the WHERE clause.

**\_WSTART/\_WEND/\_WDURATION**
\_wstart, \_wend, and \_wduration pseudo columns
\_wstart indicates the starting timestamp of the window, \_wend indicates the ending timestamp of the window, and \_wduration indicates the duration of the window.

These three pseudo columns can only be used in time-windowed split queries and must appear after the window-splitting clause.

**\_c0/\_ROWTS**

In TDengine, every table must have a timestamp type as its primary key, and the pseudo columns \_rowts and \_c0 both represent the value of this column. Compared to the actual primary key timestamp column, using pseudo columns is more flexible and has clearer semantics. For example, they can be used with functions like max and min.

```sql
SELECT _rowts, MAX(current) FROM meters;
```

**\_IROWTS**

The \_irowts pseudo column can only be used with the interp function to return the timestamps corresponding to the results of the interp function.

```sql
SELECT _irowts, interp(current) FROM meters RANGE('2020-01-01 10:00:00', '2020-01-01 10:30:00') EVERY(1s) FILL(linear);
```

## Query Objects

After the FROM keyword, there can be several table (supertable) lists, or the results of subqueries.
If you do not specify the user's current database, you can specify the database to which the table belongs by using the database name before the table name. For example: `power.d1001` can be used to access tables across databases.

TDengine supports INNER JOIN based on the timestamp primary key, with the following rules:

1. It supports both table lists after the FROM keyword and explicit JOIN clauses.
2. For basic tables and subtables, the ON condition must only include equal conditions for the timestamp primary key.
3. For supertables, the ON condition must include equal conditions for the timestamp primary key as well as tag columns that correspond one-to-one. OR conditions are not supported.
4. The tables involved in the JOIN computation must be of the same type; they can only be supertables, subtables, or basic tables.
5. Both sides of the JOIN support subqueries.
6. JOIN cannot be mixed with the FILL clause.

## GROUP BY

If both GROUP BY and SELECT clauses are specified in the statement, then the SELECT list can only include the following expressions:

1. Constants
2. Aggregate functions
3. Expressions that match the GROUP BY expression.
4. Expressions that include the above expressions.

The GROUP BY clause groups the data based on the values of the expressions following GROUP BY, returning one row of summary information for each group.

The GROUP BY clause can group by any columns in a table or view by specifying the column names, and these columns do not need to appear in the SELECT list.

The GROUP BY clause can use positional syntax, where the position is a positive integer starting from 1, representing the expression's position in the SELECT list to group by.

The GROUP BY clause can use result set column names to indicate the specified expressions in the SELECT list for grouping.

When using positional syntax and result set column names in the GROUP BY clause, the corresponding expressions in the SELECT list cannot be aggregate functions.

This clause groups rows, but does not guarantee the order of the result set. To sort the groups, use the ORDER BY clause.

## PARTITION BY

The PARTITION BY clause is a special syntax introduced in TDengine version 3.0, used to partition data based on part_list, allowing various calculations within each partition.

PARTITION BY is similar in meaning to GROUP BY; both group data based on specified lists and then calculate. The difference is that PARTITION BY has no restrictions on the SELECT list in GROUP BY, allowing any operations (constants, aggregates, scalars, expressions, etc.) within the groups. Therefore, PARTITION BY is fully compatible with GROUP BY, but note that without aggregation queries, their results may differ.

Because PARTITION BY does not require returning one row of aggregated data, it supports various window operations after grouping slices, and all window operations that require grouping can only use the PARTITION BY clause.

See also [TDengine Distinctive Queries](../time-series-extensions/).

## ORDER BY

The ORDER BY clause sorts the result set. If no ORDER BY is specified, the order of the result set returned in multiple queries of the same statement is not guaranteed to be consistent.

After ORDER BY, positional syntax can be used, where the position is a positive integer starting from 1, indicating which expression in the SELECT list to use for sorting.

ASC indicates ascending order, and DESC indicates descending order.

The NULLS syntax specifies the position of NULL values in sorting. NULLS LAST is the default for ascending order, and NULLS FIRST is the default for descending order.

## LIMIT

LIMIT controls the number of output rows, and OFFSET specifies which row to start outputting from. LIMIT/OFFSET is applied after the ORDER BY result set. LIMIT 5 OFFSET 2 can be abbreviated as LIMIT 2, 5, which outputs data from the 3rd to the 7th row.

When there are PARTITION BY/GROUP BY clauses, LIMIT controls the output within each partition slice rather than the total output of the result set.

## SLIMIT

SLIMIT is used with PARTITION BY/GROUP BY clauses to control the number of output slices. SLIMIT 5 SOFFSET 2 can be abbreviated as SLIMIT 2, 5, which outputs the 3rd to the 7th slices.

Note that if there is an ORDER BY clause, only one slice is output.

## Special Features

Some special query features can be executed without a FROM clause.

### Get Current Database

The following command retrieves the current database, `DATABASE()`. If a default database was not specified when logging in, and the `USE` command has not been used to switch databases, it returns NULL.

```sql
SELECT DATABASE();
```

### Get Server and Client Version Numbers

```sql
SELECT CLIENT_VERSION();
SELECT SERVER_VERSION();
```

### Get Server Status

This server status detection statement returns a number (e.g., 1) if the server is normal. If the server is abnormal, it returns an error code. This SQL syntax is compatible with connection pools for TDengine status checks and third-party tools for database server status checks, avoiding issues caused by using incorrect heartbeat detection SQL statements that lead to connection loss in connection pools.

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

```txt
WHERE (column|tbname) match/MATCH/nmatch/NMATCH _regex_
```

### Regular Expression Specifications

Ensure that the regular expressions used comply with POSIX standards; refer to [Regular Expressions](https://pubs.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap09.html) for specifics.

### Usage Limitations

Regular expression filtering can only target table names (i.e., tbname filtering) and binary/nchar type values.

The length of the matched regular expression string cannot exceed 128 bytes. You can set and adjust the maximum allowable length of the regex matching string using the parameter `_maxRegexStringLen`, which is a client configuration parameter requiring a restart to take effect.

## CASE Expression

### Syntax

```txt
CASE value WHEN compare_value THEN result [WHEN compare_value THEN result ...] [ELSE result] END
CASE WHEN condition THEN result [WHEN condition THEN result ...] [ELSE result] END
```

### Description

TDengine allows users to use IF ... THEN ... ELSE logic within SQL statements through the CASE expression.

The first CASE syntax returns the first result whose value equals compare_value. If no compare_value matches, it returns the result after ELSE. If there is no ELSE part, it returns NULL.

The second syntax returns the first true result for a condition. If no condition matches, it returns the result after ELSE. If there is no ELSE part, it returns NULL.

The return type of the CASE expression is the type of the first result after WHEN THEN, while the results of the remaining WHEN THEN parts and the ELSE part must be convertible to this type; otherwise, TDengine will raise an error.

### Example

A device has three status codes, displaying its status as follows:

```sql
SELECT CASE dev_status WHEN 1 THEN 'Running' WHEN 2 THEN 'Warning' WHEN 3 THEN 'Downtime' ELSE 'Unknown' END FROM dev_table;
```

Calculating the average voltage of smart meters, when the voltage is less than 200 or greater than 250, it is considered an erroneous statistic, correcting its value to 220, as follows:

```sql
SELECT AVG(CASE WHEN voltage < 200 OR voltage > 250 THEN 220 ELSE voltage END) FROM meters;
```

## JOIN Clause

Prior to version 3.3.0.0, TDengine only supported INNER JOIN; starting from version 3.3.0.0, TDengine supports a wider range of JOIN types, including traditional database types such as LEFT JOIN, RIGHT JOIN, FULL JOIN, SEMI JOIN, ANTI-SEMI JOIN, as well as time-series features like ASOF JOIN and WINDOW JOIN. JOIN operations can occur between subtables, basic tables, supertables, and subqueries.

### Example

JOIN operation between basic tables:

```sql
SELECT *
FROM temp_tb_1 t1, pressure_tb_1 t2
WHERE t1.ts = t2.ts
```

LEFT JOIN operation between supertables:

```sql
SELECT *
FROM temp_stable t1 LEFT JOIN temp_stable t2
ON t1.ts = t2.ts AND t1.deviceid = t2.deviceid AND t1.status = 0;
```

LEFT ASOF JOIN operation between subtables and supertables:

```sql
SELECT *
FROM temp_ctable t1 LEFT ASOF JOIN temp_stable t2
ON t1.ts = t2.ts AND t1.deviceid = t2.deviceid;
```

For more information on JOIN operations, see [TDengine Join Queries](../join-queries/).

## Nested Queries

"Nesting Queries," also known as "Subqueries," means that in a single SQL statement, the results of the "inner query" can be used as computation objects in the "outer query."

Starting from version 2.2.0.0, TDengine's query engine supports using non-correlated subqueries in the FROM clause (where "non-correlated" means the subquery does not use parameters from the parent query). This means you can replace the tb_name_list in the ordinary SELECT statement with an independent SELECT statement (enclosed in parentheses), resulting in the complete nested query SQL statement:

```sql
SELECT ... FROM (SELECT ... FROM ...) ...;
```

:::info

- The result of the inner query will be used as a "virtual table" for the outer query. It is recommended to alias this virtual table for easy reference in the outer query.
- The outer query can refer to the inner query's columns or pseudo columns directly by name or as `column_name`.
- Both inner and outer queries support basic table and supertable JOINs. The results of the inner query can also participate in JOIN operations with data subtables.
- The functional capabilities of the inner query are consistent with those of non-nested query statements.
  - The ORDER BY clause in the inner query generally has no significance; it is recommended to avoid such usage to prevent unnecessary resource consumption.
- Compared to non-nested query statements, there are some functional limitations for the outer query:
  - Calculation function parts:
    - If the inner query's result data does not provide timestamps, calculation functions that implicitly depend on timestamps will not work correctly in the outer query. For example: INTERP, DERIVATIVE, IRATE, LAST_ROW, FIRST, LAST, TWA, STATEDURATION, TAIL, UNIQUE.
    - If the result data of the inner query is not ordered by timestamps, calculation functions that depend on ordered data will not work correctly in the outer query. For example: LEASTSQUARES, ELAPSED, INTERP, DERIVATIVE, IRATE, TWA, DIFF, STATECOUNT, STATEDURATION, CSUM, MAVG, TAIL, UNIQUE.
    - Functions requiring two scans of the calculation process will not work properly in the outer query. For example: PERCENTILE.

:::

## UNION ALL Clause

```txt title=Syntax
SELECT ...
UNION ALL SELECT ...
[UNION ALL SELECT ...]
```

TDengine supports the UNION ALL operator, meaning that if multiple SELECT statements return result sets with identical structures (column names, column types, number of columns, order), they can be combined into one result set using UNION ALL. Currently, only UNION ALL mode is supported, meaning no deduplication occurs during the result set merging. Up to 100 UNION ALL statements are supported within a single SQL statement.

## SQL Example

For the example below, the table `tb1` is created with the following statement:

```sql
CREATE TABLE tb1 (ts TIMESTAMP, col1 INT, col2 FLOAT, col3 BINARY(50));
```

Query all records from `tb1` from the past hour:

```sql
SELECT * FROM tb1 WHERE ts >= NOW - 1h;
```

Query records from `tb1` from `2018-06-01 08:00:00.000` to `2018-06-02 08:00:00.000`, and where `col3` ends with 'nny', sorting results in descending order of timestamp:

```sql
SELECT * FROM tb1 WHERE ts > '2018-06-01 08:00:00.000' AND ts <= '2018-06-02 08:00:00.000' AND col3 LIKE '%nny' ORDER BY ts DESC;
```

Query the sum of `col1` and `col2`, naming it `complex`, for timestamps greater than `2018-06-01 08:00:00.000` and where `col2` is greater than `1.2`, outputting only 10 records starting from the 5th record:

```sql
SELECT (col1 + col2) AS 'complex' FROM tb1 WHERE ts > '2018-06-01 08:00:00.000' AND col2 > 1.2 LIMIT 10 OFFSET 5;
```

Query records from the past 10 minutes, where `col2` is greater than `3.14`, and output results to the file `/home/testoutput.csv`:

```sql
SELECT COUNT(*) FROM tb1 WHERE ts >= NOW - 10m AND col2 > 3.14 >> /home/testoutput.csv;
```
