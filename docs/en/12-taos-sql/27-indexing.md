---
sidebar_label: Window Pre-Aggregation
title: Window Pre-Aggregation
description: Instructions for using Window Pre-Aggregation
---

To improve the performance of aggregate function queries on large datasets, you can create Time-Range Small Materialized Aggregates (TSMA) objects. These objects perform pre-computation on specified aggregate functions using fixed time windows and store the computed results. When querying, you can retrieve the pre-computed results to enhance query performance.

## Creating TSMA

```sql
-- Create TSMA based on a super table or regular table
CREATE TSMA tsma_name ON [dbname.]table_name FUNCTION (func_name(func_param) [, ...] ) INTERVAL(time_duration);
-- Create a large window TSMA based on a small window TSMA
CREATE RECURSIVE TSMA tsma_name ON [db_name.]tsma_name1 INTERVAL(time_duration);

time_duration:
    number unit
```

To create a TSMA, you need to specify the TSMA name, table name, function list, and window size. When creating a TSMA based on an existing TSMA, using the `RECURSIVE` keyword, you don't need to specify the `FUNCTION()`. It will create a TSMA with the same function list as the existing TSMA, and the INTERVAL must be a multiple of the window of the base TSMA.

The naming rule for TSMA is similar to the table name, with a maximum length of the table name length minus the length of the output table suffix. The table name length limit is 193, and the output table suffix is `_tsma_res_stb_`. The maximum length of the TSMA name is 178.

TSMA can only be created based on super tables and regular tables, not on subtables.

In the function list, you can only specify supported aggregate functions (see below), and the number of function parameters must be 1, even if the current function supports multiple parameters. The function parameters must be ordinary column names, not tag columns. Duplicate functions and columns in the function list will be deduplicated. When calculating TSMA, all `intermediate results of the functions` will be output to another super table, and the output super table also includes all tag columns of the original table. The maximum number of functions in the function list is the maximum number of columns in the output table (including tag columns) minus the four additional columns added for TSMA calculation, namely `_wstart`, `_wend`, `_wduration`, and a new tag column `tbname`, minus the number of tag columns in the original table. If the number of columns exceeds the limit, an error `Too many columns` will be reported.

Since the output of TSMA is a super table, the row length of the output table is subject to the maximum row length limit. The size of the `intermediate results of different functions` varies, but they are generally larger than the original data size. If the row length of the output table exceeds the maximum row length limit, an error `Row length exceeds max length` will be reported. In this case, you need to reduce the number of functions or split commonly used functions groups into multiple TSMA objects.

The window size is limited to [1m ~ 1h]. The unit of INTERVAL is the same as the INTERVAL clause in the query, such as a (milliseconds), b (nanoseconds), h (hours), m (minutes), s (seconds), u (microseconds).

TSMA is a database-level object, but it is globally unique. The number of TSMA that can be created in the cluster is limited by the parameter `maxTsmaNum`, with a default value of 3 and a range of [0-3]. Note that since TSMA background calculation uses stream computing, creating a TSMA will create a stream. Therefore, the number of TSMA that can be created is also limited by the number of existing streams and the maximum number of streams that can be created.

## Supported Functions
| function |  comments |
|---|---|
|min||
|max||
|sum||
|first||
|last||
|avg||
|count| If you want to use count(*), you should create the count(ts) function|
|spread||
|stddev||
|||

## Drop TSMA
```sql
DROP TSMA [db_name.]tsma_name;
```

If there are other TSMA created based on the TSMA being deleted, the delete operation will report an `Invalid drop base tsma, drop recursive tsma first` error. Therefore, all Recursive TSMA must be deleted first.

## TSMA Calculation
The calculation result of TSMA is a super table in the same database as the original table, but it is not visible to users. It cannot be deleted and will be automatically deleted when `DROP TSMA` is executed. The calculation of TSMA is done through stream computing, which is a background asynchronous process. The calculation result of TSMA is not guaranteed to be real-time, but it can guarantee eventual correctness.

If there is no data in the original subtable, the corresponding output subtable may not be created. Therefore, in count queries, even if `countAlwaysReturnValue` is configured, the result of this subtable will not be returned.

When there is a large amount of historical data, after creating TSMA, the stream computing will first calculate the historical data. During this period, newly created TSMA will not be used. The calculation will be automatically recalculated when data updates, deletions, or expired data arrive. During the recalculation period, the TSMA query results are not guaranteed to be real-time. If you want to query real-time data, you can use the hint `/*+ skip_tsma() */` in the SQL statement or disable the `querySmaOptimize` parameter to query from the original data.

## Using and Limitations of TSMA

Client configuration parameter: `querySmaOptimize`, used to control whether to use TSMA during queries. Set it to `True` to use TSMA, and `False` to query from the original data.

Client configuration parameter: `maxTsmaCalcDelay`, in seconds, is used to control the acceptable TSMA calculation delay for users. If the calculation progress of a TSMA is within this range from the latest time, the TSMA will be used. If it exceeds this range, it will not be used. The default value is 600 (10 minutes), with a minimum value of 600 (10 minutes) and a maximum value of 86400 (1 day).

Client configuration parameter: `tsmaDataDeleteMark`, in milliseconds, consistent with the stream computing parameter `deleteMark`, is used to control the retention time of intermediate results in stream computing. The default value is 1 day, with a minimum value of 1 hour. Therefore, historical data that is older than the configuration parameter will not have the intermediate results saved in stream computing. If you modify the data within these time windows, the TSMA calculation results will not include the updated results. This means that the TSMA results will be inconsistent with querying the original data.

### Using TSMA Duraing Query

The aggregate functions defined in TSMA can be directly used in most query scenarios. If multiple TSMA are available, the one with the larger window size is preferred. For unclosed windows, the calculation can be done using smaller window TSMA or the original data. However, there are certain scenarios where TSMA cannot be used (see below). In such cases, the entire query will be calculated using the original data.

The default behavior for queries without specified window sizes is to prioritize the use of the largest window TSMA that includes all the aggregate functions used in the query. For example, `SELECT COUNT(*) FROM stable GROUP BY tbname` will use the TSMA with the largest window that includes the `count(ts)` function. Therefore, when using aggregate queries frequently, it is recommended to create TSMA objects with larger window size.

When specifying the window size, which is the `INTERVAL` statement, use the largest TSMA window that is divisible by the window size of the query. In window queries, the window size of the `INTERVAL`, `OFFSET`, and `SLIDING` all affect the TSMA window size that can be used. Divisible window TSMA refers to a TSMA window size that is divisible by the `INTERVAL`, `OFFSET`, and `SLIDING` of the query statement. Therefore, when using window queries frequently, consider the window size, as well as the offset and sliding size when creating TSMA objects.

Example 1. If TSMA with window size of `5m` and `10m` is created, and the query is `INTERVAL(30m)`, the TSMA with window size of `10m` will be used. If the query is `INTERVAL(30m, 10m) SLIDING(5m)`, only the TSMA with window size of `5m` can be used for the query.

### Limitations of Query

When the parameter `querySmaOptimize` is enabled and there is no `skip_tsma()` hint, the following query scenarios cannot use TSMA:

- When the aggregate functions defined in a TSMA do not cover the function list of the current query.
- Non-`INTERVAL` windows or the query window size (including `INTERVAL, SLIDING, OFFSET`) is not multiples of the defined window size. For example, if the defined window is 2m and the query uses a 5-minute window, but if there is a 1m window available, it can be used.
- Query with filtering on any regular column (non-primary key time column) in the `WHERE` condition.
- When `PARTITION` or `GROUP BY` includes any regular column or its expression
- When other faster optimization logic can be used, such as last cache optimization, if it meets the conditions for last optimization, it will be prioritized. If last optimization is not possible, then it will be determined whether TSMA optimization can be used.
- When the current TSMA calculation progress delay is greater than the configuration parameter `maxTsmaCalcDelay`

Some examples:

```sql
SELECT agg_func_list [, pesudo_col_list] FROM stable WHERE exprs [GROUP/PARTITION BY [tbname] [, tag_list]] [HAVING ...] [INTERVAL(time_duration, offset) SLIDING(duration)]...;

-- create
CREATE TSMA tsma1 ON stable FUNCTION(COUNT(ts), SUM(c1), SUM(c3), MIN(c1), MIN(c3), AVG(c1)) INTERVAL(1m);
-- query
SELECT COUNT(*), SUM(c1) + SUM(c3) FROM stable; ---- use tsma1
SELECT COUNT(*), AVG(c1) FROM stable GROUP/PARTITION BY tbname, tag1, tag2;  --- use tsma1
SELECT COUNT(*), MIN(c1) FROM stable INTERVAL(1h);  ---use tsma1
SELECT COUNT(*), MIN(c1), SPREAD(c1) FROM stable INTERVAL(1h); ----- can't use, spread func not defined, although SPREAD can be calculated by MIN and MAX which are defined.
SELECT COUNT(*), MIN(c1) FROM stable INTERVAL(30s); ----- can't use tsma1, time_duration not fit. Normally, query_time_duration should be multple of create_duration.
SELECT COUNT(*), MIN(c1) FROM stable where c2 > 0; ---- can't use tsma1, can't do c2 filtering
SELECT COUNT(*) FROM stable GROUP BY c2; ---- can't use any tsma
SELECT MIN(c3), MIN(c2) FROM stable INTERVAL(1m); ---- can't use tsma1, c2 is not defined in tsma1.

-- Another tsma2 created with INTERVAL(1h) based on tsma1
CREATE RECURSIVE TSMA tsma2 on tsma1 INTERVAL(1h);
SELECT COUNT(*), SUM(c1) FROM stable; ---- use tsma2
SELECT COUNT(*), AVG(c1) FROM stable GROUP/PARTITION BY tbname, tag1, tag2;  --- use tsma2
SELECT COUNT(*), MIN(c1) FROM stable INTERVAL(2h);  ---use tsma2
SELECT COUNT(*), MIN(c1) FROM stable WHERE ts < '2023-01-01 10:10:10' INTERVAL(30m); --use tsma1
SELECT COUNT(*), MIN(c1) + MIN(c3) FROM stable INTERVAL(30m);  ---use tsma1
SELECT COUNT(*), MIN(c1) FROM stable INTERVAL(1h) SLIDING(30m);  ---use tsma1
SELECT COUNT(*), MIN(c1), SPREAD(c1) FROM stable INTERVAL(1h); ----- can't use tsma1 or tsma2, spread func not defined
SELECT COUNT(*), MIN(c1) FROM stable INTERVAL(30s); ----- can't use tsma1 or tsma2, time_duration not fit. Normally, query_time_duration should be multple of create_duration.
SELECT COUNT(*), MIN(c1) FROM stable where c2 > 0; ---- can't use tsma1 or tsam2, can't do c2 filtering
```

### Limitations of Usage

After creating a TSMA, there are certain restrictions on operations that can be performed on the original table:

- You must delete all TSMAs on the table before you can delete the table itself.
- All tag columns of the original table cannot be deleted, nor can the tag column names or sub-table tag values be modified. You must first delete the TSMA before you can delete the tag column.
- If some columns are being used by the TSMA, these columns cannot be deleted. You must first delete the TSMA. However, adding new columns to the table is not affected. However, new columns added are not included in any TSMA, so if you want to calculate the new columns, you need to create new TSMA for them.

## Show TSMA

```sql
SHOW [db_name.]TSMAS;
SELECT * FROM information_schema.ins_tsma;
```

If more functions are specified during creation, and the column names are longer, the function list may be truncated when displayed (currently supports a maximum output of 256KB)
