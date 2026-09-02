---
sidebar_label: RSMA
title: RSMA
description: Create, alter, compute, and query rollup SMA (RSMA) for downsampled storage
---

Starting from `v3.3.8.0`, TDengine Enterprise provides downsampled storage. Rollup SMA (Small Materialized Aggregation), or RSMA, downsamples user data by time window and stores the results. It fits scenarios where raw data is retained for a short period and downsampled data for a longer period. Downsampled data is much smaller than raw data, which reduces disk usage; queries can scan the downsampled data directly for faster response.

## Basic Logic

- RSMA supports automatic triggers: when data migrates from a lower storage tier to a higher one, downsampling is performed automatically, controlled by the database [`KEEP`](../02-ddl/01-database.md) option (`keep0` / `keep1` / `keep2`). For multi-tier storage, see [Multi-Tier Storage](../../12-operations-and-tooling/02-operations/01-planning.md#multi-tier-storage).
- RSMA supports manual triggers: useful when downsampled data must be recalculated after updates or deletes, or when data has already migrated to the target tier but has not yet been downsampled.
- RSMA does not change query semantics.

## Details

- After RSMA downsampling completes, the corresponding raw data files are deleted; the time ranges of downsampled data and raw data do not overlap.
- Data at storage tiers 1 / 2 / 3 all support updates and deletes, but updates and deletes do not automatically trigger recalculation. Therefore, when out-of-order writes occur at tiers 2 / 3, an already aggregated window may contain multiple rows; manual recalculation aggregates both the prior aggregation result and the out-of-order data, which may be inaccurate for functions such as `AVG` / `FIRST` / `LAST`, while `MIN` / `MAX` / `SUM` are unaffected.
- Tables can still be altered, created, or dropped when RSMA exists; these DDL changes take effect with delay during calculation and recalculation.
- RSMA depends on S3 migration: the first S3 migration requires RSMA calculation to finish first.

## Create RSMA

```sql
CREATE RSMA [IF NOT EXISTS] rsma_name ON [dbname.]table_name FUNCTION([func_name(col_name)[, ...]]) INTERVAL(interval1[, interval2]);
```

- Specify the RSMA name, table name, function list, and window sizes. Naming rules are the same as for table names; the maximum length is `193`.
- RSMA can only be created on a supertable. If the supertable includes `BLOB` / `MEDIUMBLOB` columns, RSMA is not supported yet.
- Supported functions: `MIN`, `MAX`, `SUM`, `AVG`, `FIRST`, `LAST`. Each function must take exactly one argument, which must be a non-primary-key ordinary column (not a tag). Non-numeric columns cannot use numeric aggregates such as `SUM` / `AVG`. `FUNCTION` may be omitted or empty; columns without an explicit function default to `LAST`. Composite primary-key columns support only `FIRST` / `LAST`, and also default to `LAST` when not specified explicitly.
- `INTERVAL` must specify at least 1 and at most 2 values. The range is `[0, DURATION]` (converted at database precision), with at least one non-`0` value. `interval1 = 0` means level 2 is not downsampled; `interval2 = 0` means level 3 is not downsampled. Unless otherwise noted, intervals below are positive integers.
- A time unit is required. Allowed units: `a` (milliseconds), `b` (nanoseconds), `u` (microseconds), `s` (seconds), `m` (minutes), `h` (hours), `d` (days). `w` / `n` / `y` are not supported.
- When both intervals are positive, `interval1 < interval2` is required, and `interval2` must be an integer multiple of `interval1`; neither may exceed the database `DURATION`. `DURATION` must be divisible by each positive interval to reduce complexity, resource use, and fragmentation from cross-file-boundary calculation.
- Making `interval2` an integer multiple of `interval1` ensures correctness of `MIN` / `MAX` / `SUM` / `FIRST` / `LAST` relative to the raw data; `AVG` may still have error.

## Alter RSMA

```sql
ALTER RSMA [IF EXISTS] [db_name.]rsma_name FUNCTION ([func_name(col_name)[, ...]]);
```

Changes the aggregation function for columns, mainly for newly added columns. Only columns that were not previously assigned an explicit function can be altered. Columns without an explicit function default to `LAST`; changing them may alter aggregation semantics before and after the change—confirm business requirements first.

## Drop RSMA

```sql
DROP RSMA [IF EXISTS] [db_name.]rsma_name;
```

Dropping and recreating may cause inconsistent aggregation functions before and after—confirm business requirements first.

## Show Create Statement

```sql
SHOW CREATE RSMA [db_name.]rsma_name;
```

**Example**

```sql
taos> SHOW CREATE RSMA rsma7\G;
*************************** 1.row ***************************
       RSMA: `rsma7`
Create RSMA: CREATE RSMA `rsma7` ON `d0`.`stb1` FUNCTION(min(`c0`),max(`c1`),avg(`c2`),sum(`c3`),first(`c4`),last(`c5`),first(`c6`)) INTERVAL(60000a,300000a)
Query OK, 1 row(s) in set (0.005250s)
```

## Show All RSMAs

```sql
SHOW [db_name.]RSMAS;
SELECT * FROM information_schema.ins_rsmas [WHERE db_name = '{db_name}'];
```

**Example**

```sql
taos> SHOW RSMAS\G;
*************************** 1.row ***************************
  rsma_name: rsma7
    rsma_id: 4785417934375247480
    db_name: d0
 table_name: stb1
 table_type: SUPER_TABLE
create_time: 2025-10-03 23:03:57.577
   interval: 60000a,300000a
  func_list: min(c0),max(c1),avg(c2),sum(c3),first(c4),last(c5),first(c6)
Query OK, 1 row(s) in set (0.014238s)
```

`func_list` shows only functions explicitly specified via `FUNCTION` at creation time.

## Manual RSMA Calculation

```sql
ROLLUP DATABASE db_name [start_opt] [end_opt]
ROLLUP [db_name] VGROUPS IN (vgroup_ids) [start_opt] [end_opt]

start_opt ::= START WITH timestamp_literal   -- e.g. 'YYYY-MM-DD HH:MM:SS'
            | START WITH unix_timestamp      -- e.g. 1672531200
            | START WITH TIMESTAMP timestamp_literal

end_opt   ::= END WITH timestamp_literal
            | END WITH unix_timestamp
            | END WITH TIMESTAMP timestamp_literal
```

**Example**

```sql
taos> ROLLUP DATABASE d0 START WITH '2025-12-30 10:00:00.000' END WITH '2025-12-31 10:00:00.000';
  result  |      id     | reason  |
===================================
 accepted |   53584270  | success |
Query OK, 1 row(s) in set (0.009359s)

taos> ROLLUP d0 VGROUPS IN (2,3) START WITH '2025-12-30 10:00:00.000';
  result  |      id     | reason  |
===================================
 accepted |  1726039381 | success |
Query OK, 1 row(s) in set (0.010345s)
```

Notes:

- Manual recalculation is mainly for downsampling and storing level 2 / 3 file groups that do not yet meet multi-tier migration conditions.
- You can specify a time range and/or a database or vgroup:
  1. With no time range, all file groups whose `KEEP` falls in `[INT64_MIN, now]` are calculated.
  2. With a time range, file groups in that range are calculated.
  3. After `ROLLUP`, if no new data is written, calculation is not repeated.
  4. If file groups in the specified range are still at level 1 and do not meet migration conditions to a higher tier, they are not calculated.
  5. For level 2 / 3 file groups: calculation runs if new data was written or updated after the last `ROLLUP`, or if level 2 → level 3 migration conditions are met.
- If file groups to recalculate are already on S3, recalculation writes new file groups locally again and remote file groups become inactive; later S3 uploads may fail and remote file groups must be deleted manually. This behavior matches `COMPACT`.

### Show RSMA Tasks

```sql
SHOW RETENTIONS;
SHOW RETENTION {retention_id};
```

**Example**

```sql
taos> SHOW RETENTIONS;
 retention_id | db_name |       start_time        | trigger_mode | type   |
============================================================================
    857434526 | d0      | 2025-10-11 11:26:04.649 | manual       | rollup |
Query OK, 1 row(s) in set (0.004885s)

taos> SHOW RETENTION 857434526;
 retention_id | vgroup_id | dnode_id | number_fileset | finished |       start_time        | progress(%) | remain_time(s) |
===========================================================================================================================
    857434526 |         6 |        1 |              4 |        1 | 2025-10-11 11:26:04.649 |          24 |             31 |
    857434526 |         7 |        1 |              0 |        0 | 2025-10-11 11:26:04.649 |           0 |              0 |
Query OK, 2 row(s) in set (0.005828s)
```

### Kill RSMA Tasks

```sql
KILL RETENTION {retention_id};
```

### Querying with RSMA

RSMA does not change query semantics. If the query time range spans multiple storage tiers, results may include both raw and downsampled data.
