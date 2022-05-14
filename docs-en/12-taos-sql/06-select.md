---
sidebar_label: Select
title: Select
---

## Syntax

```SQL
SELECT select_expr [, select_expr ...]
    FROM {tb_name_list}
    [WHERE where_condition]
    [SESSION(ts_col, tol_val)]
    [STATE_WINDOW(col)]
    [INTERVAL(interval_val [, interval_offset]) [SLIDING sliding_val]]
    [FILL(fill_mod_and_val)]
    [GROUP BY col_list]
    [ORDER BY col_list { DESC | ASC }]
    [SLIMIT limit_val [SOFFSET offset_val]]
    [LIMIT limit_val [OFFSET offset_val]]
    [>> export_file];
```

## Wildcard

Wilcard \* can be used to specify all columns. The result includes only data columns for normal tables.

```
taos> SELECT * FROM d1001;
           ts            |       current        |   voltage   |        phase         |
======================================================================================
 2018-10-03 14:38:05.000 |             10.30000 |         219 |              0.31000 |
 2018-10-03 14:38:15.000 |             12.60000 |         218 |              0.33000 |
 2018-10-03 14:38:16.800 |             12.30000 |         221 |              0.31000 |
Query OK, 3 row(s) in set (0.001165s)
```

The result includes both data columns and tag columns for super table.

```
taos> SELECT * FROM meters;
           ts            |       current        |   voltage   |        phase         |            location            |   groupid   |
=====================================================================================================================================
 2018-10-03 14:38:05.500 |             11.80000 |         221 |              0.28000 | Beijing.Haidian                |           2 |
 2018-10-03 14:38:16.600 |             13.40000 |         223 |              0.29000 | Beijing.Haidian                |           2 |
 2018-10-03 14:38:05.000 |             10.80000 |         223 |              0.29000 | Beijing.Haidian                |           3 |
 2018-10-03 14:38:06.500 |             11.50000 |         221 |              0.35000 | Beijing.Haidian                |           3 |
 2018-10-03 14:38:04.000 |             10.20000 |         220 |              0.23000 | Beijing.Chaoyang               |           3 |
 2018-10-03 14:38:16.650 |             10.30000 |         218 |              0.25000 | Beijing.Chaoyang               |           3 |
 2018-10-03 14:38:05.000 |             10.30000 |         219 |              0.31000 | Beijing.Chaoyang               |           2 |
 2018-10-03 14:38:15.000 |             12.60000 |         218 |              0.33000 | Beijing.Chaoyang               |           2 |
 2018-10-03 14:38:16.800 |             12.30000 |         221 |              0.31000 | Beijing.Chaoyang               |           2 |
Query OK, 9 row(s) in set (0.002022s)
```

Wildcard can be used with table name as prefix, both below SQL statements have same effects and return all columns.

```SQL
SELECT * FROM d1001;
SELECT d1001.* FROM d1001;
```

In JOIN query, however, with or without table name prefix will return different results. \* without table prefix will return all the columns of both tables, but \* with table name as prefix will return only the columns of that table.

```
taos> SELECT * FROM d1001, d1003 WHERE d1001.ts=d1003.ts;
           ts            | current |   voltage   |    phase     |           ts            | current |   voltage   |    phase     |
==================================================================================================================================
 2018-10-03 14:38:05.000 | 10.30000|         219 |      0.31000 | 2018-10-03 14:38:05.000 | 10.80000|         223 |      0.29000 |
Query OK, 1 row(s) in set (0.017385s)
```

```
taos> SELECT d1001.* FROM d1001,d1003 WHERE d1001.ts = d1003.ts;
           ts            |       current        |   voltage   |        phase         |
======================================================================================
 2018-10-03 14:38:05.000 |             10.30000 |         219 |              0.31000 |
Query OK, 1 row(s) in set (0.020443s)
```

Wilcard \* can be used with some functions, but the result may be different depending on the function being used. For example, `count(*)` returns only one column, i.e. the number of rows; `first`, `last` and `last_row` return all columns of the selected row.

```
taos> SELECT COUNT(*) FROM d1001;
       count(*)        |
========================
                     3 |
Query OK, 1 row(s) in set (0.001035s)
```

```
taos> SELECT FIRST(*) FROM d1001;
        first(ts)        |    first(current)    | first(voltage) |     first(phase)     |
=========================================================================================
 2018-10-03 14:38:05.000 |             10.30000 |            219 |              0.31000 |
Query OK, 1 row(s) in set (0.000849s)
```

## Tags

Starting from version 2.0.14, tag columns can be selected together with data columns when querying sub tables. Please be noted that, however, wildcard \* doesn't represent any tag column, that means tag columns must be specified explicitly like below example.

```
taos> SELECT location, groupid, current FROM d1001 LIMIT 2;
            location            |   groupid   |       current        |
======================================================================
 Beijing.Chaoyang               |           2 |             10.30000 |
 Beijing.Chaoyang               |           2 |             12.60000 |
Query OK, 2 row(s) in set (0.003112s)
```

## Get distinct values

`DISTINCT` keyword can be used to get all the unique values of tag columns from a super table, it can also be used to get all the unique values of data columns from a table or sub table.

```sql
SELECT DISTINCT tag_name [, tag_name ...] FROM stb_name;
SELECT DISTINCT col_name [, col_name ...] FROM tb_name;
```

:::info

1. Configuration parameter `maxNumOfDistinctRes` in `taos.cfg` is used to control the number of rows to output. The minimum configurable value is 100,000, the maximum configurable value is 100,000,000, the default value is 1000,000. If the actual number of rows exceeds the value of this parameter, only the number of rows specified by this parameter will be output.
2. It can't be guaranteed that the results selected by using `DISTINCT` on columns of `FLOAT` or `DOUBLE` are exactly unique because of the precision nature of floating numbers.
3. `DISTINCT` can't be used in the sub-query of a nested query statement, and can't be used together with aggregate functions, `GROUP BY` or `JOIN` in same SQL statement.

:::

## Columns Names of Result Set

When using `SELECT`, the column names in the result set will be same as that in the select clause if `AS` is not used. `AS` can be used to rename the column names in the result set. For example

```
taos> SELECT ts, ts AS primary_key_ts FROM d1001;
           ts            |     primary_key_ts      |
====================================================
 2018-10-03 14:38:05.000 | 2018-10-03 14:38:05.000 |
 2018-10-03 14:38:15.000 | 2018-10-03 14:38:15.000 |
 2018-10-03 14:38:16.800 | 2018-10-03 14:38:16.800 |
Query OK, 3 row(s) in set (0.001191s)
```

`AS` can't be used together with `first(*)`, `last(*)`, or `last_row(*)`.

## Implicit Columns

`Select_exprs` can be column names of a table, or function expression or arithmetic expression on columns. The maximum number of allowed column names and expressions is 256. Timestamp and the corresponding tag names will be returned in the result set if `interval` or `group by tags` are used, and timestamp will always be the first column in the result set.

## Table List

`FROM` can be followed by a number of tables or super tables, or can be followed by a sub-query. If no database is specified as current database in use, table names must be preceded with database name, like `power.d1001`.

```SQL
SELECT * FROM power.d1001;
```

has same effect as

```SQL
USE power;
SELECT * FROM d1001;
```

## Special Query

Some special query functionalities can be performed without `FORM` sub-clause. For example, below statement can be used to get the current database in use.

```
taos> SELECT DATABASE();
           database()           |
=================================
 power                          |
Query OK, 1 row(s) in set (0.000079s)
```

If no database is specified upon logging in and no database is specified with `USE` after login, NULL will be returned by `select database()`.

```
taos> SELECT DATABASE();
           database()           |
=================================
 NULL                           |
Query OK, 1 row(s) in set (0.000184s)
```

Below statement can be used to get the version of client or server.

```
taos> SELECT CLIENT_VERSION();
 client_version() |
===================
 2.0.0.0          |
Query OK, 1 row(s) in set (0.000070s)

taos> SELECT SERVER_VERSION();
 server_version() |
===================
 2.0.0.0          |
Query OK, 1 row(s) in set (0.000077s)
```

Below statement is used to check the server status. One integer, like `1`, is returned if the server status is OK, otherwise an error code is returned. This way is compatible with the status check for TDengine from connection pool or 3rd party tools, and can avoid the problem of losing connection from connection pool when using wrong heartbeat checking SQL statement.

```
taos> SELECT SERVER_STATUS();
 server_status() |
==================
               1 |
Query OK, 1 row(s) in set (0.000074s)

taos> SELECT SERVER_STATUS() AS status;
   status    |
==============
           1 |
Query OK, 1 row(s) in set (0.000081s)
```

## \_block_dist

**Description**: Get the data block distribution of a table or stable.

```SQL title="Syntax"
SELECT _block_dist() FROM { tb_name | stb_name }
```

**Restrictions**：No argument is allowed, where clause is not allowed

**Sub Query**：Sub query or nested query are not supported

**Return value**: A string which includes the data block distribution of the specified table or stable, i.e. the histogram of rows stored in the data blocks of the table or stable.

```text title="Result"
summary:
5th=[392], 10th=[392], 20th=[392], 30th=[392], 40th=[792], 50th=[792] 60th=[792], 70th=[792], 80th=[792], 90th=[792], 95th=[792], 99th=[792] Min=[392(Rows)] Max=[800(Rows)] Avg=[666(Rows)] Stddev=[2.17] Rows=[2000], Blocks=[3], Size=[5.440(Kb)] Comp=[0.23] RowsInMem=[0] SeekHeaderTime=[1(us)]
```

**More explanation about above example**:

- Histogram about the rows stored in the data blocks of the table or stable: the value of rows for 5%, 10%, 20%, 30%, 40%, 50%, 60%, 70%, 80%, 90%, 95%, and 99%
- Minimum number of rows stored in a data block, i.e. Min=[392(Rows)]
- Maximum number of rows stored in a data block, i.e. Max=[800(Rows)]
- Average number of rows stored in a data block, i.e. Avg=[666(Rows)]
- stddev of number of rows, i.e. Stddev=[2.17]
- Total number of rows, i.e. Rows[2000]
- Total number of data blocks, i.e. Blocks=[3]
- Total disk size consumed, i.e. Size=[5.440(Kb)]
- Compression ratio, which means the compressed size divided by original size, i.e. Comp=[0.23]
- Total number of rows in memory, i.e. RowsInMem=[0], which means no rows in memory
- The time spent on reading head file (to retrieve data block information), i.e. SeekHeaderTime=[1(us)], which means 1 microsecond.

## Special Keywords in TAOS SQL

- `TBNAME`： it is treated as a special tag when selecting on a super table, representing the name of sub-tables in that super table.
- `_c0`: represents the first column of a table or super table.

## Tips

To get all the sub tables and corresponding tag values from a super table:

```SQL
SELECT TBNAME, location FROM meters;
```

To get the number of sub tables in a super table：

```SQL
SELECT COUNT(TBNAME) FROM meters;
```

Only filter on `TAGS` are allowed in the `where` clause for above two query statements. For example:

```
taos> SELECT TBNAME, location FROM meters;
             tbname             |            location            |
==================================================================
 d1004                          | Beijing.Haidian                |
 d1003                          | Beijing.Haidian                |
 d1002                          | Beijing.Chaoyang               |
 d1001                          | Beijing.Chaoyang               |
Query OK, 4 row(s) in set (0.000881s)

taos> SELECT COUNT(tbname) FROM meters WHERE groupId > 2;
     count(tbname)     |
========================
                     2 |
Query OK, 1 row(s) in set (0.001091s)
```

- Wildcard \* can be used to get all columns, or specific column names can be specified. Arithmetic operation can be performed on columns of number types, columns can be renamed in the result set.
- Arithmetic operation on columns can't be used in where clause. For example, `where a*2>6;` is not allowed but `where a>6/2;` can be used instead for same purpose.
- Arithmetic operation on columns can't be used as the objectives of select statement. For example, `select min(2*a) from t;` is not allowed but `select 2*min(a) from t;` can be used instead.
- Logical operation can be used in `WHERE` clause to filter numeric values, wildcard can be used to filter string values.
- Result set are arranged in ascending order of the first column, i.e. timestamp, but it can be controlled to output as descending order of timestamp. If `order by` is used on other columns, the result may be not as expected. By the way, \_c0 is used to represent the first column, i.e. timestamp.
- `LIMIT` parameter is used to control the number of rows to output. `OFFSET` parameter is used to specify from which row to output. `LIMIT` and `OFFSET` are executed after `ORDER BY` in the query execution. A simple tip is that `LIMIT 5 OFFSET 2` can be abbreviated as `LIMIT 2, 5`.
- What is controlled by `LIMIT` is the number of rows in each group when `GROUP BY` is used.
- `SLIMIT` parameter is used to control the number of groups when `GROUP BY` is used. Similar to `LIMIT`, `SLIMIT 5 OFFSET 2` can be abbreviated as `SLIMIT 2, 5`.
- ">>" can be used to output the result set of `select` statement to the specified file.

## Where

Logical operations in below table can be used in `where` clause to filter the resulting rows.

| **Operation** | **Note**                 | **Applicable Data Types**                 |
| ------------- | ------------------------ | ----------------------------------------- |
| >             | larger than              | all types except bool                     |
| <             | smaller than             | all types except bool                     |
| >=            | larger than or equal to  | all types except bool                     |
| <=            | smaller than or equal to | all types except bool                     |
| =             | equal to                 | all types                                 |
| <\>           | not equal to             | all types                                 |
| is [not] null | is null or is not null   | all types                                 |
| between and   | within a certain range   | all types except bool                     |
| in            | match any value in a set | all types except first column `timestamp` |
| like          | match a wildcard string  | **`binary`** **`nchar`**                  |
| match/nmatch  | filter regex             | **`binary`** **`nchar`**                  |

**使用说明**:

- Operator `<\>` is equal to `!=`, please be noted that this operator can't be used on the first column of any table, i.e.timestamp column.
- Operator `like` is used together with wildcards to match strings
  - '%' matches 0 or any number of characters, '\_' matches any single ASCII character.
  - `\_` is used to match the \_ in the string.
  - The maximum length of wildcard string is 100 bytes from version 2.1.6.1 (before that the maximum length is 20 bytes). `maxWildCardsLength` in `taos.cfg` can be used to control this threshold. Too long wildcard string may slowdown the execution performance of `LIKE` operator.
- `AND` keyword can be used to filter multiple columns simultaneously. AND/OR operation can be performed on single or multiple columns from version 2.3.0.0. However, before 2.3.0.0 `OR` can't be used on multiple columns.
- For timestamp column, only one condition can be used; for other columns or tags, `OR` keyword can be used to combine multiple logical operators. For example, `((value > 20 AND value < 30) OR (value < 12))`.
  - From version 2.3.0.0, multiple conditions can be used on timestamp column, but the result set can only contain single time range.
- From version 2.0.17.0, operator `BETWEEN AND` can be used in where clause, for example `WHERE col2 BETWEEN 1.5 AND 3.25` means the filter condition is equal to "1.5 ≤ col2 ≤ 3.25".
- From version 2.1.4.0, operator `IN` can be used in where clause. For example, `WHERE city IN ('Beijing', 'Shanghai')`. For bool type, both `{true, false}` and `{0, 1}` are allowed, but integers other than 0 or 1 are not allowed. FLOAT and DOUBLE types are impacted by floating precision, only values that match the condition within the tolerance will be selected. Non-primary key column of timestamp type can be used with `IN`.
- From version 2.3.0.0, regular expression is supported in where clause with keyword `match` or `nmatch`, the regular expression is case insensitive.

## Regular Expression

### Syntax

```SQL
WHERE (column|tbname) **match/MATCH/nmatch/NMATCH** _regex_
```

### Specification

The regular expression being used must be compliant with POSIX specification, please refer to [Regular Expressions](https://pubs.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap09.html).

### Restrictions

Regular expression can be used against only table names, i.e. `tbname`, and tags of binary/nchar types, but can't be used against data columns.

The maximum length of regular expression string is 128 bytes. Configuration parameter `maxRegexStringLen` can be used to set the maximum allowed regular expression. It's a configuration parameter on client side, and will take in effect after restarting the client.

## JOIN

From version 2.2.0.0, inner join is fully supported in TDengine. More specifically, the inner join between table and table, that between stable and stable, and that between sub query and sub query are supported.

Only primary key, i.e. timestamp, can be used in the join operation between table and table. For example:

```sql
SELECT *
FROM temp_tb_1 t1, pressure_tb_1 t2
WHERE t1.ts = t2.ts
```

In the join operation between stable and stable, besides the primary key, i.e. timestamp, tags can also be used. For example:

```sql
SELECT *
FROM temp_stable t1, temp_stable t2
WHERE t1.ts = t2.ts AND t1.deviceid = t2.deviceid AND t1.status=0;
```

Similary, join operation can be performed on the result set of multiple sub queries.

:::note
Restrictions on join operation:

- The number of tables or stables in single join operation can't exceed 10.
- `FILL` is not allowed in the query statement that includes JOIN operation.
- Arithmetic operation is not allowed on the result set of join operation.
- `GROUP BY` is not allowed on a part of tables that participate in join operation.
- `OR` can't be used in the conditions for join operation
- join operation can't be performed on data columns, i.e. can only be performed on tags or primary key, i.e. timestamp

:::

## Nested Query

Nested query is also called sub query, that means in a single SQL statement the result of inner query can be used as the data source of the outer query.

From 2.2.0.0, unassociated sub query can be used in the `FROM` clause. unassociated means the sub query doesn't use the parameters in the parent query. More specifically, in the `tb_name_list` of `SELECT` statement, an independent SELECT statement can be used. So a complete nested query looks like:

```SQL
SELECT ... FROM (SELECT ... FROM ...) ...;
```

:::info

- Only one layer of nesting is allowed, that means no sub query is allowed in a sub query
- The result set returned by the inner query will be used as a "virtual table" by the outer query, the "virtual table" can be renamed using `AS` keyword for easy reference in the outer query.
- Sub query is not allowed in continuous query.
- JOIN operation is allowed between tables/stables inside both inner and outer queries. Join operation can be performed on the result set of the inner query.
- UNION operation is not allowed in either inner query or outer query.
- The functionalities that can be used in the inner query is same as non-nested query.
  - `ORDER BY` inside the inner query doesn't make any sense but will slow down the query performance significantly, so please avoid such usage.
- Compared to the non-nested query, the functionalities that can be used in the outer query have such restrictions as:
  - Functions
    - If the result set returned by the inner query doesn't contain timestamp column, then functions relying on timestamp can't be used in the outer query, like `TOP`, `BOTTOM`, `FIRST`, `LAST`, `DIFF`.
    - Functions that need to scan the data twice can't be used in the outer query, like `STDDEV`, `PERCENTILE`.
  - `IN` operator is not allowed in the outer query but can be used in the inner query.
  - `GROUP BY` is not supported in the outer query.

:::

## UNION ALL

```SQL title=Syntax
SELECT ...
UNION ALL SELECT ...
[UNION ALL SELECT ...]
```

`UNION ALL` operator can be used to combine the result set from multiple select statements as long as the result set of these select statements have exactly same columns. `UNION ALL` doesn't remove redundant rows from multiple result sets. In single SQL statement, at most 100 `UNION ALL` can be supported.

### Examples

table `tb1` is created using below SQL statement:

```SQL
CREATE TABLE tb1 (ts TIMESTAMP, col1 INT, col2 FLOAT, col3 BINARY(50));
```

The rows in the past one hour in `tb1` can be selected using below SQL statement:

```SQL
SELECT * FROM tb1 WHERE ts >= NOW - 1h;
```

The rows between 2018-06-01 08:00:00.000 and 2018-06-02 08:00:00.000 and col3 ends with 'nny' can be selected in the descending order of timestamp using below SQL statement:

```SQL
SELECT * FROM tb1 WHERE ts > '2018-06-01 08:00:00.000' AND ts <= '2018-06-02 08:00:00.000' AND col3 LIKE '%nny' ORDER BY ts DESC;
```

The sum of col1 and col2 for rows later than 2018-06-01 08:00:00.000 and whose col2 is bigger than 1.2 can be selected and renamed as "complex", while only 10 rows are output from the 5th row, by below SQL statement:

```SQL
SELECT (col1 + col2) AS 'complex' FROM tb1 WHERE ts > '2018-06-01 08:00:00.000' AND col2 > 1.2 LIMIT 10 OFFSET 5;
```

The rows in the past 10 minutes and whose col2 is bigger than 3.14 are selected and output to the result file `/home/testoutpu.csv` with below SQL statement:

```SQL
SELECT COUNT(*) FROM tb1 WHERE ts >= NOW - 10m AND col2 > 3.14 >> /home/testoutpu.csv;
```
