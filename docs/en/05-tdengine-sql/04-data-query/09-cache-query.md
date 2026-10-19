---
sidebar_label: Read Cache
title: Read Cache
description: Cache the latest subtable data with CACHEMODEL to accelerate LAST / LAST_ROW queries
toc_max_heading_level: 4
---

Read cache stores the most recent data of each subtable in memory to accelerate “current value” queries. On a cache hit, `LAST` / `LAST_ROW` do not need to read historical data from disk.

Typical use cases include dashboards that show the latest device readings and per-table latest status. For write cache, metadata cache, or WAL-related caching, see [Data Caching](../../15-internals/07-cache.md).

## LAST and LAST_ROW

Read cache is mainly used with the following two aggregate functions. Their semantics differ, so choose the matching `CACHEMODEL` setting:

| Function | Semantics (summary) | Full reference |
| --- | --- | --- |
| `LAST` | The last non-NULL value written for a column (can be taken per column) | [LAST](./03-function.md#last) |
| `LAST_ROW` | The last row of a table / supertable (column values on that row may be NULL) | [LAST_ROW](./03-function.md#last_row) |

## Configure Read Cache

Read cache is controlled by the database parameters `CACHEMODEL` and `CACHESIZE`. You can set them in `CREATE DATABASE` or change them with `ALTER DATABASE`.

### CACHEMODEL

| Value | Meaning | Primarily accelerates |
| --- | --- | --- |
| `none` | No caching (default) | — |
| `last_row` | Cache the most recent row of each subtable | `LAST_ROW` |
| `last_value` | Cache the most recent non-NULL value of each column | `LAST` when not affected by `WHERE`, `ORDER BY`, `GROUP BY`, `INTERVAL`, and similar |
| `both` | Cache both the most recent row and the most recent column values | `LAST_ROW` and `LAST` under the conditions above |

:::note

- Frequent `CACHEMODEL` switches may make `LAST` / `LAST_ROW` results briefly inaccurate. Use caution.
- `LAST` queries with filters, sorting, grouping, or windows often cannot fully use the `last_value` cache.
- Enabling read cache maintains the cache on the write path and can affect write performance. For high-throughput workloads, change `both` to `last_row` or `last_value`; see [Ingesting Data Efficiently](../../10-developer-guide/05-high-throughput.md).

:::

### CACHESIZE

`CACHESIZE` sets the memory size used by each vnode to cache the most recent subtable data. Default is `1`, range `[1, 65536]`, in MB. Size it according to machine memory and table scale. For how to decide whether capacity is sufficient, see [Modify CACHESIZE](../02-ddl/01-database.md#modify-cachesize).

For full parameter details and other database options, see [CACHEMODEL](../02-ddl/01-database.md#cachemodel) and [CACHESIZE](../02-ddl/01-database.md#cachesize).

### Create and Alter Examples

```sql
-- Enable at create time
CREATE DATABASE power CACHEMODEL 'both' CACHESIZE 16;

-- Enable or adjust on an existing database
ALTER DATABASE power CACHEMODEL 'both';
ALTER DATABASE power CACHESIZE 32;
```

After enabling, use `SHOW CREATE DATABASE` to confirm parameters, and use `SHOW VGROUPS` to check each vnode’s `cacheload` (current last-cache usage in bytes).

## Practice Example

The following example uses smart-meter data to compare `LAST` / `LAST_ROW` latency before and after enabling read cache. First generate test data with `taosBenchmark`:

```shell
taosBenchmark -d power -Q --start-timestamp=1600000000000 --tables=10000 --records=10000 --time-step=10000 -y
```

This creates database `power` and supertable `meters` with about 100 million rows: 10,000 subtables, 10,000 rows each, starting timestamp `1600000000000` (`2020-09-13T20:26:40+08:00`), 10-second interval. Default `CACHEMODEL` is `none`.

Query the latest current and timestamp without read cache:

```sql
taos> SELECT LAST(ts, current) FROM meters;
        last(ts)         |    last(current)     |
=================================================
 2020-09-15 00:13:10.000 |            1.1294620 |
Query OK, 1 row(s) in set (0.353815s)

taos> SELECT LAST_ROW(ts, current) FROM meters;
      last_row(ts)       |  last_row(current)   |
=================================================
 2020-09-15 00:13:10.000 |            1.1294620 |
Query OK, 1 row(s) in set (0.344070s)
```

Enable read cache and confirm it took effect:

```sql
taos> ALTER DATABASE power CACHEMODEL 'both';
Query OK, 0 row(s) affected (0.046092s)

taos> SHOW CREATE DATABASE power\G;
*************************** 1.row ***************************
       Database: power
Create Database: CREATE DATABASE `power` BUFFER 256 CACHESIZE 1 CACHEMODEL 'both' COMP 2 DURATION 14400m WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 100 STT_TRIGGER 2 KEEP 5256000m,5256000m,5256000m PAGES 256 PAGESIZE 4 PRECISION 'ms' REPLICA 1 WAL_LEVEL 1 VGROUPS 10 SINGLE_STABLE 0 TABLE_PREFIX 0 TABLE_SUFFIX 0 TSDB_PAGESIZE 4 WAL_RETENTION_PERIOD 3600 WAL_RETENTION_SIZE 0 KEEP_TIME_OFFSET 0
Query OK, 1 row(s) in set (0.000282s)
```

Query again. The first query fills the cache; later queries usually show much lower latency:

```sql
taos> SELECT LAST(ts, current) FROM meters;
        last(ts)         |    last(current)     |
=================================================
 2020-09-15 00:13:10.000 |            1.1294620 |
Query OK, 1 row(s) in set (0.044021s)

taos> SELECT LAST_ROW(ts, current) FROM meters;
      last_row(ts)       |  last_row(current)   |
=================================================
 2020-09-15 00:13:10.000 |            1.1294620 |
Query OK, 1 row(s) in set (0.046682s)
```

In this example, latency dropped from about 353 / 344 ms to about 44 ms. Actual results depend on data scale, hardware, and concurrent load.

## Related Documents

- [LAST](./03-function.md#last) / [LAST_ROW](./03-function.md#last_row): function semantics
- [CACHEMODEL](../02-ddl/01-database.md#cachemodel) / [CACHESIZE](../02-ddl/01-database.md#cachesize): parameter reference
- [Data Caching](../../15-internals/07-cache.md): write cache, metadata cache, WAL, and other cache types
- [Ingesting Data Efficiently](../../10-developer-guide/05-high-throughput.md): write-path impact of read cache and tuning
