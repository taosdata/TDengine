---
sidebar_label: 读缓存
title: 读缓存
description: 通过 CACHEMODEL 缓存子表最近数据，加速 LAST / LAST_ROW 查询
toc_max_heading_level: 4
---

在物联网（IoT）和工业互联网（IIoT）场景中，业务往往更关心设备的当前状态，而不是全量历史数据。例如产线监控中的温度、压力，车联网中的实时位置，以及看板、智能仪表上的最新读数，都需要低延迟获取“当前值”。

## 传统缓存方案的局限

为支撑高频实时查询，不少系统会在数据库与应用之间引入 Redis 等外部缓存，但这通常会带来：

- 架构更复杂：需要额外部署和维护缓存集群。
- 成本更高：占用额外硬件，并增加运维开销。
- 一致性更难保证：缓存与数据库之间需要额外同步机制。

## TDengine 的读缓存

针对上述场景，TDengine 提供内置读缓存：可将每张子表的最近数据缓存在内存中，无需引入第三方缓存即可加速当前值查询。

系统按时间优先保留最新数据；查询命中缓存时无需访问磁盘。缓存达到容量上限后，较早数据会批量落盘，既提升查询效率，也减轻磁盘写入压力。

通过 `CACHEMODEL` 可选择缓存最近一行、每列最近非 `NULL` 值，或两者同时缓存。该机制降低了查询延迟，减少了对外部缓存的依赖，也减轻了高并发查询对存储的压力。

## 读缓存配置

创建数据库时可配置是否缓存子表最近数据，由参数 `CACHEMODEL` 控制，取值如下：

- `none`：不缓存（默认值）。
- `last_row`：缓存子表最近一行数据，可改善 `LAST_ROW` 的性能。
- `last_value`：缓存子表每一列最近的非 `NULL` 值，可改善无 `WHERE`、`ORDER BY`、`GROUP BY`、`INTERVAL` 等特殊影响时 `LAST` 的性能。
- `both`：同时缓存最近行与最近列值。

:::note
频繁切换 `CACHEMODEL` 可能导致 `LAST` / `LAST_ROW` 的查询结果短期内不准确，请谨慎操作。
:::

启用读缓存后，可用 `CACHESIZE` 配置每个 vnode 用于缓存子表最近数据的内存大小。默认值为 `1`，取值范围为 `[1, 65536]`，单位为 MB，需按机器内存合理设置。

数据库创建及相关参数说明，参见 [数据库](../02-ddl/01-database.md#cachemodel)。

## 实践示例

以下以智能电表为例，观察 `LAST` 缓存对实时查询性能的影响。先用 `taosBenchmark` 生成测试数据：

```shell
taosBenchmark -d power -Q --start-timestamp=1600000000000 --tables=10000 --records=10000 --time-step=10000 -y
```

该命令会创建测试数据库 `power`，生成约 1 亿条时序数据：时间戳从 `1600000000000`（`2020-09-13T20:26:40+08:00`）开始，超级表为 `meters`，包含 10000 个子表，每个子表 10000 条记录，采集间隔为 10 秒。

查询最新电流与时间戳：

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

启用读缓存并确认生效：

```sql
taos> ALTER DATABASE power CACHEMODEL 'both';
Query OK, 0 row(s) affected (0.046092s)

taos> SHOW CREATE DATABASE power\G;
*************************** 1.row ***************************
       Database: power
Create Database: CREATE DATABASE `power` BUFFER 256 CACHESIZE 1 CACHEMODEL 'both' COMP 2 DURATION 14400m WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 100 STT_TRIGGER 2 KEEP 5256000m,5256000m,5256000m PAGES 256 PAGESIZE 4 PRECISION 'ms' REPLICA 1 WAL_LEVEL 1 VGROUPS 10 SINGLE_STABLE 0 TABLE_PREFIX 0 TABLE_SUFFIX 0 TSDB_PAGESIZE 4 WAL_RETENTION_PERIOD 3600 WAL_RETENTION_SIZE 0 KEEP_TIME_OFFSET 0
Query OK, 1 row(s) in set (0.000282s)
```

再次查询最新数据。首次查询会填充缓存，后续查询时延会明显下降：

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

本例中，查询时延从约 353 / 344 ms 缩短到约 44 ms，提升约 8 倍。实际效果与数据规模、硬件和并发负载有关。
