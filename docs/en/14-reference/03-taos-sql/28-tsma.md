---
sidebar_label: Manage TSMAs
title: Manage Time-Range Small Materialized Aggregates (TSMAs)
description: Instructions for using window pre-aggregation
slug: /tdengine-reference/sql-manual/manage-tsmas
---

To improve the performance of aggregate function queries with large data volumes, window pre-aggregation (TSMA Time-Range Small Materialized Aggregates) objects are created. Using fixed time windows, specified aggregate functions are pre-calculated, and the results are stored, allowing queries to utilize the pre-calculated results for improved performance.

## Creating TSMA

```sql
-- Create a TSMA based on a supertable or a basic table
CREATE TSMA tsma_name ON [dbname.]table_name FUNCTION (func_name(func_param) [, ...] ) INTERVAL(time_duration);
-- Create a large window TSMA based on a small window TSMA
CREATE RECURSIVE TSMA tsma_name ON [db_name.]tsma_name1 INTERVAL(time_duration);

time_duration:
    number unit
```

When creating a TSMA, you need to specify the TSMA name, table name, function list, and window size. When creating a new TSMA based on an already existing TSMA, the `RECURSIVE` keyword must be used, but the `FUNCTION()` cannot be specified. The newly created TSMA will have the same function list as the existing TSMA, and in this case, the specified INTERVAL must be an integer multiple of the window length of the base TSMA. Additionally, a daily TSMA cannot be based on 2h or 3h windows; it can only be based on 1h, and similarly, monthly TSMA can only be based on 1d and not on 2d or 3d.

The naming rules for TSMA are similar to those for table names, with the maximum length limited to the table name length minus the output table suffix length. The table name length limit is 193, and the output table suffix is `_tsma_res_stb_`, so the maximum length for a TSMA name is 178.

TSMAs can only be created based on supertables and basic tables, not on subtables.

In the function list, only supported aggregate functions can be specified (see below), and the function parameters must be one, even if the current function supports multiple parameters. The function parameters must be ordinary column names, not tag columns. Duplicate functions and columns in the function list will be removed, meaning that if two `avg(c1)` functions are created simultaneously, only one output will be calculated. During TSMA computation, all intermediate results of the functions will be output to another supertable, which also includes all tag columns from the original table. The number of functions in the function list must not exceed the maximum column number of the created table (including tag columns) minus the four additional columns for TSMA computation: `_wstart`, `_wend`, `_wduration`, and an additional tag column `tbname`, minus the number of tag columns from the original table. If the column count exceeds the limit, an error will be raised stating `Too many columns`.

Since TSMA outputs to a supertable, the row length of the output table is subject to the maximum row length limit. The size of the intermediate results for different functions varies, usually larger than the size of the original data. If the row length of the output table exceeds the maximum row length limit, an error will be raised stating `Row length exceeds max length`. In such cases, it is necessary to reduce the number of functions or split frequently used functions into multiple TSMAs.

The window size limit is [1m ~ 1y/12n]. The INTERVAL unit is the same as the INTERVAL clause in queries, such as a (milliseconds), b (nanoseconds), h (hours), m (minutes), s (seconds), u (microseconds), d (days), w (weeks), n (months), y (years).

TSMA is a database object, but the name is globally unique. The total number of TSMAs that can be created in a cluster is limited by the parameter `maxTsmaNum`, with a default value of 3, and a range of [0-3]. Note that since TSMA background calculations use stream calculations, creating a TSMA will create a stream, so the number of TSMAs that can be created is also limited by the number of streams already existing and the maximum number of streams that can be created.

## Supported Function List

| Function | Remarks |
|---|---|
|min| |
|max| |
|sum| |
|first| |
|last| |
|avg| |
|count| To use count(*), you should create the function count(ts) |
|spread| |
|stddev| |
| | |

## Deleting TSMA

```sql
DROP TSMA [db_name.]tsma_name;
```

If there are other TSMAs based on the currently deleted TSMA, the deletion operation will raise an error `Invalid drop base tsma, drop recursive tsma first`. Therefore, all recursive TSMAs must be deleted first.

## TSMA Calculation

The calculation result of TSMA is a supertable located in the same database as the original table, and this table is not visible to users. It cannot be deleted and will be automatically deleted upon `DROP TSMA`. The calculation of TSMA is completed via stream calculations, and this process is an asynchronous background process. The results of the TSMA calculations do not guarantee real-time accuracy but can guarantee final correctness.

If there is no data in the original subtable during TSMA calculation, the corresponding output subtable may not be created. Therefore, in count queries, even if `countAlwaysReturnValue` is configured, it will not return results for that table.

When there is a large amount of historical data, after creating a TSMA, the stream calculations will first compute the historical data. During this period, the newly created TSMA will not be used. When data is updated or deleted, or expired data arrives, the affected data will be automatically recalculated. The results of TSMA queries do not guarantee real-time accuracy during the recalculation period. If you wish to query real-time data, you can add the hint `/*+ skip_tsma() */` in SQL or disable the parameter `querySmaOptimize` to query from the original data.

## TSMA Usage and Limitations

Client configuration parameter: `querySmaOptimize`, which controls whether to use TSMA during queries, where `True` means to use it and `False` means to query from the original data.

Client configuration parameter: `maxTsmaCalcDelay`, in seconds, which controls the acceptable TSMA calculation delay for users. If the calculation progress of TSMA is within this range of the latest time, the TSMA will be used; if it exceeds this range, it will not be used. The default value is 600 (10 minutes), with a minimum of 600 (10 minutes) and a maximum of 86400 (1 day).

Client configuration parameter: `tsmaDataDeleteMark`, in milliseconds, consistent with the stream calculation parameter `deleteMark`, which controls the retention time of the intermediate results of stream calculations. The default value is 1d, with a minimum of 1h. Therefore, historical data older than the time of the last data will not retain the intermediate results of stream calculations. If data within these time windows is modified, the TSMA calculation results will not include the updated results, thus leading to inconsistency with the original data query results.

### Querying with TSMA

Aggregate functions defined in the TSMA can be used directly in most query scenarios. If multiple TSMAs are available, the larger window TSMA will be prioritized, and non-closed windows will calculate using smaller window TSMAs or the original data. However, there are certain scenarios where TSMA cannot be used (see below). If it is not available, the entire query will calculate using the original data.

If no window size is specified in the query statement, the largest TSMA containing all query aggregate functions will be used for data calculations by default. For example, `SELECT COUNT(*) FROM stable GROUP BY tbname` will use the TSMA that includes count(ts) and has the largest window. Therefore, if aggregate queries are frequent, larger window TSMAs should be created as much as possible.

When specifying a window size in the `INTERVAL` statement, the largest TSMA that can be evenly divided by the specified window will be used. In window queries, the `INTERVAL`, `OFFSET`, and `SLIDING` impact the available TSMA window size. An evenly divisible window TSMA is one where the TSMA window size can be divided by the query statement's `INTERVAL`, `OFFSET`, and `SLIDING`. Therefore, if window queries are frequent, it is necessary to consider the frequently queried window sizes, as well as the sizes of offsets and sliding to create TSMAs.

Example 1. If a TSMA is created with a window size of `5m` and another with `10m`, and the query is `INTERVAL(30m)`, then the `10m` TSMA will be used first. If the query is `INTERVAL(30m, 10m) SLIDING(5m)`, then only the `5m` TSMA can be queried.

### Query Limitations

When the parameter `querySmaOptimize` is enabled and there is no `skip_tsma()` hint, the following query scenarios cannot use TSMA:

- When the agg functions defined in a TSMA cannot cover the current query's function list.
- Non-`INTERVAL` window queries, or when the `INTERVAL` query window size (including `INTERVAL`, `SLIDING`, and `OFFSET`) is not an integer multiple of the defined window. For instance, if the defined window is 2m, the query using a 5-minute window can use it, but if there exists a 1m window, it can also be used.
- Queries with `WHERE` conditions that include any ordinary column (non-primary time column) filters.
- `PARTITION` or `GROUP BY` clauses that include any ordinary column or their expressions.
- When other faster optimization logics can be used, such as last cache optimization; if the last optimization conditions are met, it will first use last optimization. If it cannot proceed with last optimization, it will then determine whether TSMA optimization can be applied.
- When the current TSMA calculation progress delay exceeds the configuration parameter `maxTsmaCalcDelay`.

Here are some examples:

```sql
SELECT agg_func_list [, pesudo_col_list] FROM stable WHERE exprs [GROUP/PARTITION BY [tbname] [, tag_list]] [HAVING ...] [INTERVAL(time_duration, offset) SLIDING(duration)]...;

-- Create
CREATE TSMA tsma1 ON stable FUNCTION(COUNT(ts), SUM(c1), SUM(c3), MIN(c1), MIN(c3), AVG(c1)) INTERVAL(1m);
-- Query
SELECT COUNT(*), SUM(c1) + SUM(c3) FROM stable; ---- use tsma1
SELECT COUNT(*), AVG(c1) FROM stable GROUP/PARTITION BY tbname, tag1, tag2;  --- use tsma1
SELECT COUNT(*), MIN(c1) FROM stable INTERVAL(1h);  ---use tsma1
SELECT COUNT(*), MIN(c1) FROM stable INTERVAL(30s); ----- can't use tsma1, time_duration not fit. Normally, query_time_duration should be multple of create_duration.
SELECT COUNT(*), MIN(c1) FROM stable WHERE c2 > 0; ---- can't use tsma1, can't do c2 filtering
SELECT COUNT(*) FROM stable GROUP BY c2; ---- can't use any tsma
SELECT MIN(c3), MIN(c2) FROM stable INTERVAL(1m); ---- can't use tsma1 or tsma2, spread func not defined.

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
SELECT COUNT(*), MIN(c1) FROM stable WHERE c2 > 0; ---- can't use tsma1 or tsma2, can't do c2 filtering
```

### Usage Limitations

After creating a TSMA, the following restrictions apply to operations on the original supertable:

- The table cannot be deleted until all TSMAs on it are deleted.
- All tag columns in the original table cannot be deleted, and tag column names or subtable tag values cannot be modified; TSMA must be deleted before tag columns can be removed.
- If some columns are used by a TSMA, those columns cannot be deleted; TSMA must be deleted first. Adding columns does not affect, but newly added columns will not be in any TSMA, so if you want to calculate the new columns, you need to create other TSMAs.

## Viewing TSMA

```sql
SHOW [db_name.]TSMAS;
SELECT * FROM information_schema.ins_tsma;
```

If too many functions were specified at creation, and the column names are long, the displayed function list may be truncated (the current maximum supported output is 256KB).
