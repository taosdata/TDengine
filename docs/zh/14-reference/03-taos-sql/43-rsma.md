---
sidebar_label: 降采样存储
title: 降采样存储
description: 降采样存储使用说明
---

Rollup SMA（Small Materialized Aggregation），简称 RSMA，是一种按时间窗口对用户数据降采样（downsampling）存储的 SMA，主要目标是减少磁盘空间占用。适用于原始数据保存时长较短，降采样数据保存时长较长的场景。降采样数据体积远小于原始数据，可大幅减少磁盘空间占用；查询降采样数据时无需扫描海量原始数据，响应速度更快。

## 基本逻辑

1. RSMA 支持自动触发。由低层级向高层级迁移时自动完成降采样存储，通过 DB keep (keep0/keep1/keep2) 参数控制，
2. RSMA 支持手动触发。适用于 `降采样存储的数据更新或删除后需要重算` 以及 `已经迁移至目标存储层级但未进行降采样存储` 的场景。
4. RSMA 不影响任何查询行为。如果查询范围跨越了存储层级，有可能同时返回原始数据和降采样数据。

## 详细说明及使用限制
1. RSMA 计算完成后删除原有数据文件，降采样数据与原始数据时间范围不存在重叠。
2. 1/2/3 存储层级的数据均支持更新和删除，但是更新和删除不会触发重算。因此，level 2/3 层级存在乱序数据写入时，原有聚合过的窗口内可能存在多条数据；此时进行手动重算会基于聚合数据和乱序数据，对于 avg/first/last 等函数再次聚合的结果可能不准确，对于 min/max/sum 等函数无影响。
3. 存在 RSMA 时，可以进行表结构修改、表的创建删除，这些操作在计算和重算时延迟生效。
4. RSMA 和 S3 迁移之间存在依赖关系，在 S3 首次迁移时，需要进行 RSMA 的计算。

## 创建 RSMA

```sql
CREATE RSMA [IF NOT EXISTS] rsma_name ON [dbname.]table_name FUNCTION([func_name(col_name)[,...]]) INTERVAL(interval1[,interval2]; 
```
创建 RSMA 时需要指定 RSMA 名字，表名字，函数列表以及窗口大小。

其中 RSMA 命名规则与表名字相同，最大长度限制为 193。

RSMA 只能基于超级表创建，暂不支持包含 blob 数据类型的超级表。

函数列表支持 min, max, sum, avg, first, last，函数参数必须为 1 个，函数参数必须为非主键普通列名，不能为标签列。非数值类型的列不能指定 sum/avg 等数值计数函数。FUNCTION 参数可为空，未显式指定 func_name 的列默认函数为 last。

RSMA interval 取值范围为 [0, DB duration] 之间整数，至少一个 非 0。Interval 1 取 0，表示 level 2 存储层级的数据不进行降采样存储；interval 2  取 0，表示 level 3 存储层级的数据不进行降采样存储。注：后续对 interval 的描述，如果没有特别说明，均为正整数。

RSMA 表的 interval 满足 interval1 < interval2 <= duration， 且必须指定单位，取值范围：a(毫秒)、b(纳秒)、h(小时)、m(分钟)、s(秒)、u(微秒)、d(天)。

跨文件边界的 interval 计算会增加系统的复杂性、资源消耗和文件碎片化，降低查询读取效率，并且带来的收益较低。因此，约定 DB duration 参数必须能够被 RSMA 表的 interval 整除。

为保证 interval 1/2 窗口聚合结果相对于原始数据的正确性，约定 interval 2 必须为 interval 1 的整数倍。可以保证 min/max/sum/first/last 结果的正确性，avg 结果正确性无法保证，还可能有误差。

## 修改 RSMA

```sql
ALTER RSMA [IF EXISTS] [db_name.]rsma_name FUNCTION ([func_name(col_name)[,...]]);
```
修改列的聚合函数，适用于新增列的场景。只允许修改未指定 func_name 的列。注意：因为未指定聚合函数时，默认聚合函数为 last，修改会造成聚合函数前后不一致，在操作时，要仔细确认业务的实际需求。

## 删除 RSMA

```sql
DROP RSMA [IF EXISTS] [db_name.]rsma_name;
```

## 显示 RSMA 创建语句

```sql
SHOW CREATE RSMA [db_name.]rsma_name;
```

## 显示所有 RSMA

```sql
SHOW [db_name.]RSMAS;
SELECT * FROM information_schema.ins_rsmas [where db_name='{db_name}'];
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
```

手动重算 RSMA 在某一时间范围的数据，可指定 database 或 vgroups。1）未指定时间范围时，计算 keep 在 [INT64_MIN, now] 之间的所有文件组；2）指定时间范围时，则计算时间范围内包含的整个文件组。3）rollup 后未写入新的数据，不会重复计算。4）如果 rollup 时间范围的文件组在 keep 1，则不进行计算，即 rollup 只对 keep 2/3 的文件组生效。
-- 手动重算适用于对不满足多级存储迁移条件的 2/3 级文件组进行计算。需要注意的是：如果需要重算的文件组已经在 s3 上，则重算生成的文件组会重新保存到本地，参照 7.1 节的说明。


## TSMA 的计算

TSMA 的计算结果为与原始表相同库下的一张超级表，此表用户不可见。不可删除，在 `DROP TSMA` 时自动删除。TSMA 的计算是通过流计算完成的，此过程为后台异步过程，TSMA 的计算结果不保证实时性，但可以保证最终正确性。

TSMA 计算时若原始子表内没有数据，则可能不会创建对应的输出子表，因此在 count 查询中，即使配置了 `countAlwaysReturnValue`，也不会返回该表的结果。

当存在大量历史数据时，创建 TSMA 之后，流计算将会首先计算历史数据，此期间新创建的 TSMA 不会被使用。数据更新删除或者过期数据到来时自动重新计算影响部分数据。在重新计算期间 TSMA 查询结果不保证实时性。若希望查询实时数据，可以通过在 SQL 中添加 hint `/*+ skip_tsma() */` 或者关闭参数 `querySmaOptimize` 从原始数据查询。

## TSMA 的使用与限制

- 客户端配置参数：`querySmaOptimize`，用于控制查询时是否使用 TSMA，`True`为使用，`False` 为不使用即从原始数据查询。
- 客户端配置参数：`maxTsmaCalcDelay`，单位为秒，用于控制用户可以接受的 TSMA 计算延迟，若 TSMA 的计算进度与最新时间差距在此范围内，则该 TSMA 将会被使用，若超出该范围，则不使用，默认值：600（10 分钟），最小值：600（10 分钟），最大值：86400（1 天）。
- 客户端配置参数：`tsmaDataDeleteMark`，单位毫秒，与流计算参数 `deleteMark` 一致，用于控制流计算中间结果的保存时间，默认值为 1d，最小值为 1h。因此那些距最后一条数据时间大于配置参数的历史数据将不保存流计算中间结果，因此若修改这些时间窗口内的数据，TSMA 的计算结果中将不包含更新的结果。即与查询原始数据结果将不一致。

### 查询时使用 TSMA

已在 TSMA 中定义的 agg 函数在大部分查询场景下都可直接使用，若存在多个可用的 TSMA，优先使用大窗口的 TSMA，未闭合窗口通过查询小窗口 TSMA 或者原始数据计算。同时也有某些场景不能使用 TSMA(见下文)。不可用时整个查询将使用原始数据进行计算。

未指定窗口大小的查询语句默认优先使用包含所有查询聚合函数的最大窗口 TSMA 进行数据的计算。如 `SELECT COUNT(*) FROM stable GROUP BY tbname` 将会使用包含 count(ts) 且窗口最大的 TSMA。因此若使用聚合查询频率高时，应当尽可能创建大窗口的 TSMA。

指定窗口大小时即 `INTERVAL` 语句，使用最大的可整除窗口 TSMA。窗口查询中，`INTERVAL` 的窗口大小、`OFFSET` 以及 `SLIDING` 都影响能使用的 TSMA 窗口大小。因此若使用窗口查询较多时，需要考虑经常查询的窗口大小，以及 offset、sliding 大小来创建 TSMA。

例如 创建 TSMA 窗口大小 `5m` 一条，`10m` 一条，查询时 `INTERVAL(30m)`，那么优先使用 `10m` 的 TSMA，若查询为 `INTERVAL(30m, 10m) SLIDING(5m)`，那么仅可使用 `5m` 的 TSMA 查询。

### 查询限制

在开启了参数 `querySmaOptimize` 并且无 `skip_tsma()` hint 时，以下查询场景无法使用 TSMA。

- 某个 TSMA 中定义的 agg 函数不能覆盖当前查询的函数列表时
- 非 `INTERVAL` 的其他窗口，或者 `INTERVAL` 查询窗口大小（包括 `INTERVAL，SLIDING，OFFSET`）不是定义窗口的整数倍，如定义窗口为 2m，查询使用 5 分钟窗口，但若存在 1m 的窗口，则可以使用。
- 查询 `WHERE` 条件中包含任意普通列 (非主键时间列) 的过滤。
- `PARTITION` 或者 `GROUY BY` 包含任意普通列或其表达式时
- 可以使用其他更快的优化逻辑时，如 last cache 优化，若符合 last 优化的条件，则先走 last 优化，无法走 last 时，再判断是否可以走 tsma 优化
- 当前 TSMA 计算进度延迟大于配置参数 `maxTsmaCalcDelay`时

下面是一些例子：

```sql
SELECT agg_func_list [, pseudo_col_list] FROM stable WHERE exprs [GROUP/PARTITION BY [tbname] [, tag_list]] [HAVING ...] [INTERVAL(time_duration, offset) SLIDING(duration)]...;

-- 创建
CREATE TSMA tsma1 ON stable FUNCTION(COUNT(ts), SUM(c1), SUM(c3), MIN(c1), MIN(c3), AVG(c1)) INTERVAL(1m);

-- 查询
SELECT COUNT(*), SUM(c1) + SUM(c3) FROM stable; ---- use tsma1
SELECT COUNT(*), AVG(c1) FROM stable GROUP/PARTITION BY tbname, tag1, tag2;  --- use tsma1
SELECT COUNT(*), MIN(c1) FROM stable INTERVAL(1h);  --- use tsma1
SELECT COUNT(*), MIN(c1), SPREAD(c1) FROM stable INTERVAL(1h); ----- can't use, spread func not defined, although SPREAD can be calculated by MIN and MAX which are defined.
SELECT COUNT(*), MIN(c1) FROM stable INTERVAL(30s); ----- can't use tsma1, time_duration not fit. Normally, query_time_duration should be multiple of create_duration.
SELECT COUNT(*), MIN(c1) FROM stable where c2 > 0; ---- can't use tsma1, can't do c2 filtering
SELECT COUNT(*) FROM stable GROUP BY c2; ---- can't use any tsma
SELECT MIN(c3), MIN(c2) FROM stable INTERVAL(1m); ---- can't use tsma1, c2 is not defined in tsma1.

-- Another tsma2 created with INTERVAL(1h) based on tsma1
CREATE RECURSIVE TSMA tsma2 on tsma1 INTERVAL(1h);
SELECT COUNT(*), SUM(c1) FROM stable; ---- use tsma2
SELECT COUNT(*), AVG(c1) FROM stable GROUP/PARTITION BY tbname, tag1, tag2;  --- use tsma2
SELECT COUNT(*), MIN(c1) FROM stable INTERVAL(2h);  --- use tsma2
SELECT COUNT(*), MIN(c1) FROM stable WHERE ts < '2023-01-01 10:10:10' INTERVAL(30m); --use tsma1
SELECT COUNT(*), MIN(c1) + MIN(c3) FROM stable INTERVAL(30m);  --- use tsma1
SELECT COUNT(*), MIN(c1) FROM stable INTERVAL(1h) SLIDING(30m);  --- use tsma1
SELECT COUNT(*), MIN(c1), SPREAD(c1) FROM stable INTERVAL(1h); ----- can't use tsma1 or tsma2, spread func not defined
SELECT COUNT(*), MIN(c1) FROM stable INTERVAL(30s); ----- can't use tsma1 or tsma2, time_duration not fit. Normally, query_time_duration should be multiple of create_duration.
SELECT COUNT(*), MIN(c1) FROM stable where c2 > 0; ---- can't use tsma1 or tsam2, can't do c2 filtering
```

### 使用限制

创建 TSMA 之后，对原始超级表的操作有以下限制：

- 必须删除该表上的所有 TSMA 才能删除该表。
- 原始表所有 tag 列不能删除，也不能修改 tag 列名或子表的 tag 值，必须先删除 TSMA，才能删除 tag 列。
- 若某些列被 TSMA 使用了，则这些列不能被删除，必须先删除 TSMA。添加列不受影响，但是新添加的列不在任何 TSMA 中，因此若要计算新增列，需要新创建其他的 TSMA。

## 查看 TSMA

```sql
SHOW [db_name.]TSMAS;
SELECT * FROM information_schema.ins_tsma;
```

若创建时指定的较多的函数，且列名较长，在显示函数列表时可能会被截断 (目前最大支持输出 256KB)。
