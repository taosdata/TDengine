---
sidebar_label: 降采样存储
title: 降采样存储
description: 降采样存储使用说明
---

自 3.3.8.0 版本起，TDengine TSDB 企业版提供数据降采样存储功能。Rollup SMA（Small Materialized Aggregation），简称 RSMA，是一种按时间窗口对用户数据降采样（downsampling）存储的 SMA，适用于原始数据保存时长较短，降采样数据保存时长较长的场景。降采样数据体积远小于原始数据，可大幅减少磁盘空间占用；查询时直接扫描降采样数据，响应速度更快。

## 基本逻辑

- RSMA 支持自动触发。由低存储层级向高存储层级迁移时自动完成降采样存储，通过 DB keep (keep0/keep1/keep2) 参数控制，
- RSMA 支持手动触发。适用于 `降采样存储的数据更新或删除后需要重算` 以及 `已经迁移至目标存储层级但未进行降采样存储` 的场景。
- RSMA 不影响任何查询行为。

## 详细说明

- RSMA 降采样存储完成后删除原始数据文件，降采样数据与原始数据时间范围不存在重叠。
- 1/2/3 存储层级的数据均支持更新和删除，但是更新和删除不会触发重算。因此，2/3 存储层级存在乱序数据写入时，已经聚合过的窗口内可能存在多条数据；此时进行手动重算会基于聚合数据和乱序数据，对于 avg/first/last 等函数再次聚合的结果可能不准确，对于 min/max/sum 等函数无影响。
- 存在 RSMA 时，可以进行表结构修改、表的创建删除，这些操作在计算和重算时延迟生效。
- RSMA 和 S3 迁移之间存在依赖关系，在 S3 首次迁移时，需要进行 RSMA 的计算。

## 创建 RSMA

```sql
CREATE RSMA [IF NOT EXISTS] rsma_name ON [dbname.]table_name FUNCTION([func_name(col_name)[,...]]) INTERVAL(interval1[,interval2]);
```

- 创建 RSMA 时需要指定 RSMA 名字，表名字，函数列表以及窗口大小。其中 RSMA 命名规则与表名字相同，最大长度限制为 193。
- RSMA 只能基于超级表创建。如果超级表的列包含 blob 数据类型，暂不支持 RSMA。
- 函数列表支持 `min, max, sum, avg, first, last`，函数参数必须为 1 个，函数参数必须为非主键普通列，不能为标签列。非数值类型的列不能指定 sum/avg 等数值计数函数。FUNCTION 参数可为空，未显式指定 func_name 的列默认函数为 last。非 Primary Key 复合主键列，func_name 仅支持 first/last，未显式指定默认函数为 last。
- RSMA interval 至少指定 1 个，至多指定 2 个。取值范围为 [0, DB duration] 之间的整数，至少一个 非 0。Interval 1 取 0，表示 level 2 存储层级的数据不进行降采样存储；interval 2  取 0，表示 level 3 存储层级的数据不进行降采样存储。本文后续对 interval 的描述，如果没有特别说明，均为正整数。
- RSMA 表的 interval 满足 `interval1 < interval2 <= duration`，且必须指定单位，单位取值范围：a(毫秒)、b(纳秒)、h(小时)、m(分钟)、s(秒)、u(微秒)、d(天)。
- 跨文件边界的 interval 计算会增加系统的复杂性、资源消耗和文件碎片化，降低查询读取效率，并且带来的收益较低。因此，约定 DB duration 参数必须能够被 RSMA 表的 interval 整除。
- 为保证 interval 1/2 窗口聚合结果相对于原始数据的正确性，约定 interval 2 必须为 interval 1 的整数倍。这可以保证 min/max/sum/first/last 结果的正确性，但无法保证 avg 结果的正确性，仍可能有误差。

## 修改 RSMA

```sql
ALTER RSMA [IF EXISTS] [db_name.]rsma_name FUNCTION ([func_name(col_name)[,...]]);
```

- 修改列的聚合函数，主要用于新增列的场景。只允许修改未显式指定 func_name 的列。注意：未指定聚合函数的列，默认聚合函数为 last，修改可能会造成聚合函数前后不一致，在操作时，要仔细确认业务需求。

## 删除 RSMA

```sql
DROP RSMA [IF EXISTS] [db_name.]rsma_name;
```

- 删除 rsma。删除 rsma 后，再重建 rsma，有可能造成聚合函数前后不一致，在操作时，要仔细确认业务需求。

## 显示 RSMA 创建语句

```sql
SHOW CREATE RSMA [db_name.]rsma_name;

示例：
taos> show create rsma rsma7\G;
*************************** 1.row ***************************
       RSMA: `rsma7`
Create RSMA: CREATE RSMA `rsma7` ON `d0`.`stb1` FUNCTION(min(`c0`),max(`c1`),avg(`c2`),sum(`c3`),first(`c4`),last(`c5`),first(`c6`)) INTERVAL(60000a,300000a)
Query OK, 1 row(s) in set (0.005250s)
```

## 显示所有 RSMA

```sql
SHOW [db_name.]RSMAS;
SELECT * FROM information_schema.ins_rsmas [where db_name='{db_name}'];

示例如下：
taos> show rsmas\G;
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

其中，func_list 只显示在创建 RSMA 时通过 FUNCTION 显式指定的函数。
```

## 手动计算 RSMA

```sql
ROLLUP DATABASE db_name [start_opt] [end_opt]
ROLLUP [db_name] VGROUPS IN (vgroup_ids) [start_opt] [end_opt]
start_opt ::= START WITH timestamp_literal, e.g. 'YYYY-MM-DD HH:MM:SS'
start_opt ::= START WITH unix_timestamp, e.g. 1672531200
start_opt ::= START WITH TIMESTAMP timestamp_literal
end_opt ::= END WITH timestamp_literal
end_opt ::= END WITH unix_timestamp
end_opt ::= END WITH TIMESTAMP timestamp_literal

示例：
taos> rollup database d0 start with '2025-12-30 10:00:00.000' end with '2025-12-31 10:00:00.000';
              result              |     id      |             reason             |
==================================================================================
 accepted                         |    53584270 | success                        |
Query OK, 1 row(s) in set (0.009359s)

taos> rollup d0.vgroups in (2,3) start with '2025-12-30 10:00:00.000';
              result              |     id      |             reason             |
==================================================================================
 accepted                         |  1726039381 | success                        |
Query OK, 1 row(s) in set (0.010345s)
```

- 手动重算主要用于对不满足多级存储迁移条件的 level 2/3 存储层级的文件组进行降采样计算存储。
- 可指定时间范围，可指定 database 或 vgroups。1）未指定时间范围时，计算 keep 在 [INT64_MIN, now] 之间的所有文件组；2）指定时间范围时，则计算时间范围内的文件组。3）rollup 后未写入新的数据，不会重复计算。4）如果 rollup 指定时间范围的文件组在 level 1 存储层级但是不满足向高存储层级迁移的条件，则不进行计算。5）rollup 针对 level 2/3 存储层级的文件组，如果满足上次 rollup 后有新数据写入或更新，或满足 level 2 向 level 3 存储层级迁移条件时，则进行计算。
需要注意的是：如果需要重算的文件组已经在 s3 上，则重算生成的文件组会重新保存到本地，s3 远端的文件组不再生效。后续再触发 s3 上传时会报错，需要手工删除远端的文件组。该逻辑与 compact 操作是相同的。

### 显示 RSMA 任务

```sql
show retentions;
show retention {retention_id};

示例：
taos> show retentions;
 retention_id |            db_name             |       start_time        | trigger_mode |     type     |
========================================================================================================
    857434526 | d0                             | 2025-10-11 11:26:04.649 | manual       | rollup       |
Query OK, 1 row(s) in set (0.004885s)

taos> show retention 857434526;
 retention_id |  vgroup_id  |  dnode_id   | number_fileset |  finished   |       start_time        | progress(%) |    remain_time(s)     |
==========================================================================================================================================
    857434526 |           6 |           1 |              4 |           1 | 2025-10-11 11:26:04.649 |          24 |                    31 |
    857434526 |           7 |           1 |              0 |           0 | 2025-10-11 11:26:04.649 |           0 |                     0 |
Query OK, 2 row(s) in set (0.005828s)
```

### 中止 RSMA 任务

```sql
kill retention {retention_id};
```

### 查询时使用 RSMA

- RSMA 不影响任何查询行为。如果查询范围跨越了存储层级，有可能同时返回原始数据和降采样数据。
