---
title: Select
sidebar_label: Select
description: This document describes how to query data in TDengine.
---

## Syntax

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
    BATCH_SCAN | NO_BATCH_SCAN | SORT_FOR_GROUP | PARA_TABLES_SORT

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
  | join_clause [, join_clause] ...
}

table_reference:
    table_expr t_alias

table_expr: {
    table_name
  | view_name
  | ( subquery )
}

join_clause:
    table_reference [INNER] JOIN table_reference ON condition

window_clause: {
    SESSION(ts_col, tol_val)
  | STATE_WINDOW(col)
  | INTERVAL(interval_val [, interval_offset]) [SLIDING (sliding_val)] [WATERMARK(watermark_val)] [FILL(fill_mod_and_val)]

interp_clause:
    RANGE(ts_val [, ts_val]) EVERY(every_val) FILL(fill_mod_and_val)

partition_by_clause:
    PARTITION BY expr [, expr] ...

group_by_clause:
    GROUP BY expr [, expr] ... HAVING condition

order_by_clasue:
    ORDER BY order_expr [, order_expr] ...

order_expr:
    {expr | position | c_alias} [DESC | ASC] [NULLS FIRST | NULLS LAST]
```

## Hints

Hints are a means of user control over query optimization for individual statements. Hints will be ignore automatically if they are not applicable to the current query statement. The specific instructions are as follows:

- Hints syntax starts with `/*+` and ends with `*/`,  spaces are allowed before or after.
- Hints syntax can only follow the SELECT keyword.
- Each hints can contain multiple hint, separated by spaces. When multiple hints conflict or are identical, whichever comes first takes effect.
- When an error occurs with a hint in hints, the effective hint before the error is still valid, and the current and subsequent hints are ignored.
- hint_param_list are arguments to each hint, which varies according to each hint.

The list of currently supported Hints is as follows:

|    **Hint**   |    **Params**  |         **Comment**        |       **Scope**            |
| :-----------: | -------------- | -------------------------- | -----------------------------------|
| BATCH_SCAN    | None           | Batch table scan           | JOIN statment for stable           |
| NO_BATCH_SCAN | None           | Sequential table scan      | JOIN statment for stable           |
| SORT_FOR_GROUP| None           | Use sort for partition, conflict with PARTITION_FIRST     | With normal column in partition by list |
| PARTITION_FIRST| None          | Use Partition before aggregate, conflict with SORT_FOR_GROUP | With normal column in partition by list |
| PARA_TABLES_SORT| None         | When sorting the supertable rows by timestamp, No temporary disk space is used. When there are numerous tables, each with long rows, the corresponding algorithm associated with this prompt may consume a substantial amount of memory, potentially leading to an Out Of Memory (OOM) situation. | Sorting the supertable rows by timestamp  |

For example:

```sql
SELECT /*+ BATCH_SCAN() */ a.ts FROM stable1 a, stable2 b where a.tag0 = b.tag0 and a.ts = b.ts;
SELECT /*+ SORT_FOR_GROUP() */ count(*), c1 FROM stable1 PARTITION BY c1;
SELECT /*+ PARTITION_FIRST() */ count(*), c1 FROM stable1 PARTITION BY c1;
SELECT /*+ PARA_TABLES_SORT() */ * from stable1 order by ts;
```

## Lists

A query can be performed on some or all columns. Data and tag columns can all be included in the SELECT list.

### Wildcards

You can use an asterisk (\*) as a wildcard character to indicate all columns. For normal tables or sub-tables, the asterisk indicates only data columns. For supertables, tag columns are also included when using asterisk (\*).

```sql
SELECT * FROM d1001;
```

You can use a table name as a prefix before an asterisk. For example, the following SQL statements both return all columns from the d1001 table:

```sql
SELECT * FROM d1001;
SELECT d1001.* FROM d1001;
```

However, in a JOIN query, using a table name prefix with an asterisk returns different results. In this case, querying * returns all data in all columns in all tables (not including tags), whereas using a table name prefix returns all data in all columns in the specified table only.

```sql
SELECT * FROM d1001, d1003 WHERE d1001.ts=d1003.ts;
SELECT d1001.* FROM d1001,d1003 WHERE d1001.ts = d1003.ts;
```

The first of the preceding SQL statements returns all columns from the d1001 and d1003 tables, but the second of the preceding SQL statements returns all columns from the d1001 table only.

With regard to the other SQL functions that support wildcards, the differences are as follows:
`count(*)` only returns one column. `first`, `last`, and `last_row` return all columns.

### Tag Columns

You can query tag columns in supertables and subtables and receive results in the same way as querying data columns.

```sql
SELECT location, groupid, current FROM d1001 LIMIT 2;
```

### Alias Name

The naming rules for aliases are the same as those for columns, and it supports directly specifying Chinese aliases in UTF-8 encoding format.


### Distinct Values

The DISTINCT keyword returns only values that are different over one or more columns. You can use the DISTINCT keyword with tag columns and data columns.

The following SQL statement returns distinct values from a tag column:

```sql
SELECT DISTINCT tag_name [, tag_name ...] FROM stb_name;
```

The following SQL statement returns distinct values from a data column:

```sql
SELECT DISTINCT col_name [, col_name ...] FROM tb_name;
```

:::info

1. Configuration parameter `maxNumOfDistinctRes` in `taos.cfg` is used to control the number of rows to output. The minimum configurable value is 100,000, the maximum configurable value is 100,000,000, the default value is 1,000,000. If the actual number of rows exceeds the value of this parameter, only the number of rows specified by this parameter will be output.
2. It can't be guaranteed that the results selected by using `DISTINCT` on columns of `FLOAT` or `DOUBLE` are exactly unique because of the precision errors in floating point numbers.

:::

### Column Names

When using `SELECT`, the column names in the result set will be the same as that in the select clause if `AS` is not used. `AS` can be used to rename the column names in the result set. For example:

```sql
taos> SELECT ts, ts AS primary_key_ts FROM d1001;
```

`AS` can't be used together with `first(*)`, `last(*)`, or `last_row(*)`.

### Pseudocolumns

**Pseudocolumn:** A pseudo-column behaves like a table column but is not actually stored in the table. You can select from pseudo-columns, but you cannot insert, update, or delete their values. A pseudo-column is also similar to a function without arguments. This section describes these pseudo-columns:

**TBNAME**
The TBNAME pseudocolumn in a supertable contains the names of subtables within the supertable.

The following SQL statement returns all unique subtable names and locations within the meters supertable:

```mysql
SELECT TAGS TBNAME, location FROM meters;
```

Use the `INS_TAGS` system table in `INFORMATION_SCHEMA` to query the information for subtables in a supertable. For example, the following statement returns the name and tag values for each subtable in the `meters` supertable.

```mysql
SELECT table_name, tag_name, tag_type, tag_value FROM information_schema.ins_tags WHERE stable_name='meters';
```

The following SQL statement returns the number of subtables within the meters supertable.

```mysql
SELECT COUNT(*) FROM (SELECT DISTINCT TBNAME FROM meters);
```

In the preceding two statements, only tags can be used as filtering conditions in the WHERE clause. 

**\_QSTART and \_QEND**

The \_QSTART and \_QEND pseudocolumns contain the beginning and end of the time range of a query. If the WHERE clause in a statement does not contain valid timestamps, the time range is equal to [-2^63, 2^63 - 1].

The \_QSTART and \_QEND pseudocolumns cannot be used in a WHERE clause.

**\_WSTART, \_WEND, and \_WDURATION**

The \_WSTART, \_WEND, and \_WDURATION pseudocolumns indicate the beginning, end, and duration of a window.

These pseudocolumns can be used only in time window-based aggregations and must occur after the aggregation clause.

**\_c0 and \_ROWTS**

In TDengine, the first column of all tables must be a timestamp. This column is the primary key of the table. The \_c0 and \_ROWTS pseudocolumns both represent the values of this column. These pseudocolumns enable greater flexibility and standardization. For example, you can use functions such as MAX and MIN with these pseudocolumns.

```sql
select _rowts, max(current) from meters;
```

**\_IROWTS**

The \_IROWTS pseudocolumn can only be used with INTERP function. This pseudocolumn can be used to retrieve the corresponding timestamp column associated with the interpolation results.

```sql
select _irowts, interp(current) from meters range('2020-01-01 10:00:00', '2020-01-01 10:30:00') every(1s) fill(linear);
```

### TAGS Query

The TAGS keyword returns only tag columns from all child tables when only tag columns are specified. One row containing tag columns is returned for each child table.

```sql
SELECT TAGS tag_name [, tag_name ...] FROM stb_name
```

## Query Objects

`FROM` can be followed by a number of tables or super tables, or can be followed by a sub-query.
If no database is specified as current database in use, table names must be preceded with database name, for example, `power.d1001`.

You can perform INNER JOIN statements based on the primary key. The following conditions apply:

1. You can use FROM table list or an explicit JOIN clause.
2. For standard tables and subtables, you must specify an ON condition and the condition must be equivalent to the primary key.
3. For supertables, the ON condition must be equivalent to the primary key. In addition, the tag columns of the tables on which the INNER JOIN is performed must have a one-to-one relationship. You cannot specify an OR condition.
4. The tables that are included in a JOIN clause must be of the same type (supertable, standard table, or subtable).
5. You can include subqueries before and after the JOIN keyword.
6. You cannot include a FILL clause and a JOIN clause in the same statement.

## GROUP BY

If you use a GROUP BY clause, the SELECT list can only include the following items:

1. Constants
2. Aggregate functions
3. Expressions that are consistent with the expression following the GROUP BY clause
4. Expressions that include the preceding expression

The GROUP BY clause groups each row of data by the value of the expression following the clause and returns a combined result for each group.

The expressions in a GROUP BY clause can include any column in any table or view. It is not necessary that the expressions appear in the SELECT list.

The GROUP BY clause does not guarantee that the results are ordered. If you want to ensure that grouped data is ordered, use the ORDER BY clause.


## PARTITION BY

The PARTITION BY clause is a TDengine-specific extension to standard SQL introduced in TDengine 3.0. This clause partitions data based on the part_list and performs computations per partition.

PARTITION BY and GROUP BY have similar meanings. They both group data according to a specified list and then perform calculations. The difference is that PARTITION BY does not have various restrictions on the SELECT list of the GROUP BY clause. Any operation can be performed within the group (constants, aggregations, scalars, expressions, etc.). Therefore, PARTITION BY is fully compatible with GROUP BY in terms of usage. All places that use the GROUP BY clause can be replaced with PARTITION BY.

Because PARTITION BY does not require returning a row of aggregated data, it can also support various window operations after grouping slices. All window operations that need to be grouped can only use the PARTITION BY clause.

For more information, see TDengine Extensions.

## ORDER BY

The ORDER BY keyword orders query results. If you do not include an ORDER BY clause in a query, the order of the results can be inconsistent.

You can specify integers after ORDER BY to indicate the order in which you want the items in the SELECT list to be displayed. For example, 1 indicates the first item in the select list.

You can specify ASC for ascending order or DESC for descending order.

You can also use the NULLS keyword to specify the position of null values. Ascending order uses NULLS LAST by default. Descending order uses NULLS FIRST by default.

## LIMIT

The LIMIT keyword controls the number of results that are displayed. You can also use the OFFSET keyword to specify the result to display first. `LIMIT` and `OFFSET` are executed after `ORDER BY` in the query execution. You can include an offset in a LIMIT clause. For example, LIMIT 5 OFFSET 2 can also be written LIMIT 2, 5. Both of these clauses display the third through the seventh results.

In a statement that includes a PARTITION BY/GROUP BY clause, the LIMIT keyword is performed on each partition/group, not on the entire set of results.

## SLIMIT

The SLIMIT keyword is used with a PARTITION BY/GROUP BY clause to control the number of partitions/groups that are displayed. You can include an offset in a SLIMIT clause. For example, SLIMIT 5 OFFSET 2 can also be written LIMIT 2, 5. Both of these clauses display the third through the seventh partitions/groups.

Note: If you include an ORDER BY clause, only one partition/group can be displayed.

## Special Query

Some special query functions can be invoked without `FROM` sub-clause.

### Obtain Current Database

The following SQL statement returns the current database. If a database has not been specified on login or with the `USE` command, a null value is returned.

```sql
SELECT DATABASE();
```

### Obtain Current Version

```sql
SELECT CLIENT_VERSION();
SELECT SERVER_VERSION();
```

### Obtain Server Status

The following SQL statement returns the status of the TDengine server. An integer indicates that the server is running normally. An error code indicates that an error has occurred. This statement can also detect whether a connection pool or third-party tool is connected to TDengine properly. By using this statement, you can ensure that connections in a pool are not lost due to an incorrect heartbeat detection statement.

```sql
SELECT SERVER_STATUS();
```

### Obtain Current Time

```sql
SELECT NOW();
```

### Obtain Current Date

```sql
SELECT TODAY();
```

### Obtain Current Time Zone

```sql
SELECT TIMEZONE();
```

### Obtain Current User

```sql
SELECT CURRENT_USER();
```

## Regular Expression

### Syntax

```txt
WHERE (column|tbname) match/MATCH/nmatch/NMATCH _regex_
```

### Specification

TDengine supports POSIX regular expression syntax. For more information, see [Regular Expressions](https://pubs.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap09.html).

### Restrictions

Regular expression filtering is supported only on table names (TBNAME), BINARY tags, and NCHAR tags. Regular expression filtering cannot be performed on data columns.

A regular expression string cannot exceed 128 bytes. You can configure this value by modifying the maxRegexStringLen parameter on the TDengine Client. The modified value takes effect when the client is restarted.

## CASE Expressions

### Syntax

```txt
CASE value WHEN compare_value THEN result [WHEN compare_value THEN result ...] [ELSE result] END
CASE WHEN condition THEN result [WHEN condition THEN result ...] [ELSE result] END
```

### Description
CASE expressions let you use IF ... THEN ... ELSE logic in SQL statements without having to invoke procedures. 

The first CASE syntax returns the `result` for the first `value`=`compare_value` comparison that is true. 

The second syntax returns the `result` for the first `condition` that is true. 

If no comparison or condition is true, the result after ELSE is returned, or NULL if there is no ELSE part.

The return type of the CASE expression is the result type of the first WHEN WHEN part, and the result type of the other WHEN WHEN parts and ELSE parts can be converted to them, otherwise TDengine will report an error.

### Examples

A device has three status codes to display its status. The statements are as follows:

```sql
SELECT CASE dev_status WHEN 1 THEN 'Running' WHEN 2 THEN 'Warning' WHEN 3 THEN 'Downtime' ELSE 'Unknown' END FROM dev_table;
```

The average voltage value of the smart meter is counted. When the voltage is less than 200 or more than 250, it is considered that the statistics is wrong, and the value is corrected to 220. The statement is as follows:

```sql
SELECT AVG(CASE WHEN voltage < 200 or voltage > 250 THEN 220 ELSE voltage END) FROM meters;
```

## JOIN

TDengine supports the `INTER JOIN` based on the timestamp primary key, that is, the `JOIN` condition must contain the timestamp primary key. As long as the requirement of timestamp-based primary key is met, `INTER JOIN` can be made between normal tables, sub-tables, super tables and sub-queries at will, and there is no limit on the number of tables, primary key and other conditions must be combined with `AND` operator.

For standard tables:

```sql
SELECT *
FROM temp_tb_1 t1, pressure_tb_1 t2
WHERE t1.ts = t2.ts
```

For supertables:

```sql
SELECT *
FROM temp_stable t1, temp_stable t2
WHERE t1.ts = t2.ts AND t1.deviceid = t2.deviceid AND t1.status=0;
```

For sub-table and super table:

```sql
SELECT *
FROM temp_ctable t1, temp_stable t2
WHERE t1.ts = t2.ts AND t1.deviceid = t2.deviceid AND t1.status=0;
```

Similarly, join operations can be performed on the result sets of multiple subqueries.

## Nested Query

Nested query is also called sub query. This means that in a single SQL statement the result of inner query can be used as the data source of the outer query.

From 2.2.0.0, unassociated sub query can be used in the `FROM` clause. Unassociated means the sub query doesn't use the parameters in the parent query. More specifically, in the `tb_name_list` of `SELECT` statement, an independent SELECT statement can be used. So a complete nested query looks like:

```
SELECT ... FROM (SELECT ... FROM ...) ...;
```

:::info

- The result of a nested query is returned as a virtual table used by the outer query. It's recommended to give an alias to this table for the convenience of using it in the outer query.
- Outer queries support directly referencing columns or pseudo-columns of inner queries in the form of column names or `column names`.
- JOIN operation is allowed between tables/STables inside both inner and outer queries. Join operation can be performed on the result set of the inner query.
- The features that can be used in the inner query are the same as those that can be used in a non-nested query.
  - `ORDER BY` inside the inner query is unnecessary and will slow down the query performance significantly. It is best to avoid the use of `ORDER BY` inside the inner query.
- Compared to the non-nested query, the functionality that can be used in the outer query has the following restrictions:
  - Functions
    - If the result set returned by the inner query doesn't contain timestamp column, then functions relying on timestamp can't be used in the outer query, like INTERP,DERIVATIVE, IRATE, LAST_ROW, FIRST, LAST, TWA, STATEDURATION, TAIL, UNIQUE.
    - If the result set returned by the inner query are not sorted in order by timestamp, then functions relying on data ordered by timestamp can't be used in the outer query, like LEASTSQUARES, ELAPSED, INTERP, DERIVATIVE, IRATE, TWA, DIFF, STATECOUNT, STATEDURATION, CSUM, MAVG, TAIL, UNIQUE. 
    - Functions that need to scan the data twice can't be used in the outer query, like PERCENTILE.

:::

## UNION ALL

```txt title=Syntax
SELECT ...
UNION ALL SELECT ...
[UNION ALL SELECT ...]
```

TDengine supports the `UNION ALL` operation. `UNION ALL` operator can be used to combine the result set from multiple select statements as long as the result set of these select statements have exactly the same columns. `UNION ALL` doesn't remove redundant rows from multiple result sets. In a single SQL statement, at most 100 `UNION ALL` can be supported.

### Examples

table `tb1` is created using below SQL statement:

```
CREATE TABLE tb1 (ts TIMESTAMP, col1 INT, col2 FLOAT, col3 BINARY(50));
```

The rows in the past one hour in `tb1` can be selected using below SQL statement:

```
SELECT * FROM tb1 WHERE ts >= NOW - 1h;
```

The rows between 2018-06-01 08:00:00.000 and 2018-06-02 08:00:00.000 and col3 ends with 'nny' can be selected in the descending order of timestamp using below SQL statement:

```
SELECT * FROM tb1 WHERE ts > '2018-06-01 08:00:00.000' AND ts <= '2018-06-02 08:00:00.000' AND col3 LIKE '%nny' ORDER BY ts DESC;
```

The sum of col1 and col2 for rows later than 2018-06-01 08:00:00.000 and whose col2 is bigger than 1.2 can be selected and renamed as "complex", while only 10 rows are output from the 5th row, by below SQL statement:

```
SELECT (col1 + col2) AS 'complex' FROM tb1 WHERE ts > '2018-06-01 08:00:00.000' AND col2 > 1.2 LIMIT 10 OFFSET 5;
```

The rows in the past 10 minutes and whose col2 is bigger than 3.14 are selected and output to the result file `/home/testoutput.csv` with below SQL statement:

```
SELECT COUNT(*) FROM tb1 WHERE ts >= NOW - 10m AND col2 > 3.14 >> /home/testoutput.csv;
```
