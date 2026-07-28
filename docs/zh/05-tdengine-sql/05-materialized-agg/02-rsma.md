---
sidebar_label: 降采样存储
title: 降采样存储
description: 降采样存储的创建、修改、计算与查询说明
---

自 `v3.3.8.0` 起，TDengine 企业版提供数据降采样存储功能。Rollup SMA（Small Materialized Aggregation），简称 RSMA，是按时间窗口对用户数据做降采样（downsampling）并存储的一种 SMA，适用于原始数据保存时长较短、降采样数据保存时长较长的场景。降采样数据体积远小于原始数据，可减少磁盘占用；查询时可直接扫描降采样数据，响应更快。

## 基本逻辑

- RSMA 支持自动触发：数据由低存储层级向高存储层级迁移时自动完成降采样存储，由数据库 [`KEEP`](../02-ddl/01-database.md#keep)（`keep0` / `keep1` / `keep2`）控制。多级存储说明参见 [多级存储](../../12-operations-and-tooling/02-operations/01-planning.md#多级存储)。
- RSMA 支持手动触发：适用于「降采样数据更新或删除后需要重算」，以及「已迁移至目标存储层级但尚未降采样」等场景。
- RSMA 不影响查询语义本身。

## 详细说明

- RSMA 降采样完成后会删除对应原始数据文件；降采样数据与原始数据的时间范围不重叠。
- 1 / 2 / 3 存储层级的数据均支持更新和删除，但更新与删除不会自动触发重算。因此，当 2 / 3 层存在乱序写入时，已聚合窗口内可能出现多条数据；手动重算会同时基于聚合结果与乱序数据，对 `AVG` / `FIRST` / `LAST` 等函数再次聚合的结果可能不准确，对 `MIN` / `MAX` / `SUM` 等无影响。
- 存在 RSMA 时仍可做表结构修改、建表删表；这些操作在计算与重算时延迟生效。
- RSMA 与 S3 迁移存在依赖：S3 首次迁移时需先完成 RSMA 计算。

## 创建 RSMA

```sql
CREATE RSMA [IF NOT EXISTS] rsma_name ON [dbname.]table_name FUNCTION([func_name(col_name)[, ...]]) INTERVAL(interval1[, interval2]);
```

- 创建时需指定 RSMA 名称、表名、函数列表以及窗口大小。命名规则与表名相同，最大长度为 `193`。
- 只能基于超级表创建。若超级表列包含 `BLOB` / `MEDIUMBLOB` 类型，暂不支持 RSMA。
- 函数列表支持 `MIN`、`MAX`、`SUM`、`AVG`、`FIRST`、`LAST`。函数参数必须为 1 个，且为非主键普通列，不能为标签列。非数值类型列不能指定 `SUM` / `AVG` 等数值聚合函数。`FUNCTION` 可省略或为空；未显式指定函数的列默认使用 `LAST`。复合主键列仅支持 `FIRST` / `LAST`，未显式指定时默认亦为 `LAST`。
- `INTERVAL` 至少指定 1 个、至多 2 个。取值范围为 `[0, DURATION]`（按数据库精度换算），且至少一个非 `0`。`interval1 = 0` 表示 level 2 不降采样；`interval2 = 0` 表示 level 3 不降采样。下文若无特别说明，所述 interval 均为正整数。
- 须指定时间单位，可用单位为：`a`（毫秒）、`b`（纳秒）、`u`（微秒）、`s`（秒）、`m`（分钟）、`h`（小时）、`d`（天）。不支持 `w` / `n` / `y`。
- 当两个 interval 均为正数时，须满足 `interval1 < interval2`，且 `interval2` 为 `interval1` 的整数倍；二者均不得超过数据库 `DURATION`。`DURATION` 必须能被各正数 interval 整除，以降低跨文件边界计算带来的复杂度、资源消耗和碎片化。
- `interval2` 为 `interval1` 整数倍可保证 `MIN` / `MAX` / `SUM` / `FIRST` / `LAST` 相对原始数据的正确性；`AVG` 仍可能存在误差。

## 修改 RSMA

```sql
ALTER RSMA [IF EXISTS] [db_name.]rsma_name FUNCTION ([func_name(col_name)[, ...]]);
```

用于修改列的聚合函数，主要用于新增列场景。只允许修改此前未显式指定函数的列。未指定函数的列默认聚合为 `LAST`，修改可能导致前后聚合语义不一致，操作前请确认业务需求。

## 删除 RSMA

```sql
DROP RSMA [IF EXISTS] [db_name.]rsma_name;
```

删除后再重建，有可能造成聚合函数前后不一致，操作前请确认业务需求。

## 显示 RSMA 创建语句

```sql
SHOW CREATE RSMA [db_name.]rsma_name;
```

**示例**

```sql
taos> SHOW CREATE RSMA rsma7\G;
*************************** 1.row ***************************
       RSMA: `rsma7`
Create RSMA: CREATE RSMA `rsma7` ON `d0`.`stb1` FUNCTION(min(`c0`),max(`c1`),avg(`c2`),sum(`c3`),first(`c4`),last(`c5`),first(`c6`)) INTERVAL(60000a,300000a)
Query OK, 1 row(s) in set (0.005250s)
```

## 显示所有 RSMA

```sql
SHOW [db_name.]RSMAS;
SELECT * FROM information_schema.ins_rsmas [WHERE db_name = '{db_name}'];
```

**示例**

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

其中，`func_list` 只显示创建时通过 `FUNCTION` 显式指定的函数。

## 手动计算 RSMA

```sql
ROLLUP DATABASE db_name [start_opt] [end_opt]
ROLLUP [db_name] VGROUPS IN (vgroup_ids) [start_opt] [end_opt]

start_opt ::= START WITH timestamp_literal   -- 如 'YYYY-MM-DD HH:MM:SS'
            | START WITH unix_timestamp      -- 如 1672531200
            | START WITH TIMESTAMP timestamp_literal

end_opt   ::= END WITH timestamp_literal
            | END WITH unix_timestamp
            | END WITH TIMESTAMP timestamp_literal
```

**示例**

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

说明：

- 手动重算主要用于对尚不满足多级存储迁移条件的 level 2 / 3 文件组做降采样计算与存储。
- 可指定时间范围，也可指定 database 或 vgroup：
  1. 未指定时间范围时，计算 `KEEP` 落在 `[INT64_MIN, now]` 之间的所有文件组。
  2. 指定时间范围时，计算该范围内的文件组。
  3. `ROLLUP` 后若未写入新数据，不会重复计算。
  4. 若指定时间范围内的文件组仍在 level 1，且不满足向更高层级迁移的条件，则不计算。
  5. 对 level 2 / 3 文件组：若上次 `ROLLUP` 后有新数据写入或更新，或满足 level 2 向 level 3 迁移条件，则进行计算。
- 若需重算的文件组已在 S3 上，重算生成的文件组会重新落到本地，远端文件组不再生效；后续再触发 S3 上传可能报错，需手工删除远端文件组。该逻辑与 `COMPACT` 相同。

### 显示 RSMA 任务

```sql
SHOW RETENTIONS;
SHOW RETENTION {retention_id};
```

**示例**

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

### 中止 RSMA 任务

```sql
KILL RETENTION {retention_id};
```

### 查询时使用 RSMA

RSMA 不改变查询语义。若查询时间范围跨越多个存储层级，结果中可能同时包含原始数据与降采样数据。
