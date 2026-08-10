---
sidebar_label: 读缓存
title: 读缓存
description: 通过 CACHEMODEL 缓存子表最近数据，加速 LAST / LAST_ROW 查询
toc_max_heading_level: 4
---

读缓存把子表的最近数据缓存在内存中，用于加速“当前值”类查询。命中缓存时，`LAST` / `LAST_ROW` 不必再从磁盘读取历史数据。

适用场景包括：监控看板取设备最新读数、按表取最新状态等。若还需了解写缓存、元数据缓存或 WAL 相关缓存，参见 [数据缓存](../../15-internals/07-cache.md)。

## LAST 与 LAST_ROW

读缓存主要配合以下两个聚合函数使用，语义不同，配置时需对应选择：

| 函数 | 语义（摘要） | 完整说明 |
| --- | --- | --- |
| `LAST` | 指定列最后写入的非 NULL 值（可按列分别取） | [LAST](./03-function.md#last) |
| `LAST_ROW` | 表 / 超级表的最后一条记录（该行上的列值可为 NULL） | [LAST_ROW](./03-function.md#last_row) |

## 配置读缓存

读缓存由数据库参数 `CACHEMODEL` 与 `CACHESIZE` 控制，可在 `CREATE DATABASE` 时指定，也可用 `ALTER DATABASE` 修改。

### CACHEMODEL

| 取值 | 含义 | 主要加速 |
| --- | --- | --- |
| `none` | 不缓存（默认） | — |
| `last_row` | 缓存子表最近一行 | `LAST_ROW` |
| `last_value` | 缓存子表每列最近的非 NULL 值 | 无 `WHERE`、`ORDER BY`、`GROUP BY`、`INTERVAL` 等特殊影响时的 `LAST` |
| `both` | 同时缓存最近行与最近列值 | `LAST_ROW` 与上述条件下的 `LAST` |

:::note

- 频繁切换 `CACHEMODEL` 可能导致短期内 `LAST` / `LAST_ROW` 结果不准确，请谨慎操作。
- 带过滤、排序、分组或窗口的 `LAST` 往往无法充分利用 `last_value` 缓存。
- 开启读缓存会在写入路径维护缓存，对写入性能有一定影响。高吞吐场景可将 `both` 调整为 `last_row` 或 `last_value`，参见 [高吞吐写入](../../10-developer-guide/05-high-throughput.md)。

:::

### CACHESIZE

`CACHESIZE` 指定每个 vnode 用于缓存子表最近数据的内存大小。默认 `1`，范围 `[1, 65536]`，单位 MB。请按机器内存与表规模合理设置；容量是否够用的判断步骤见 [修改 CACHESIZE](../02-ddl/01-database.md#修改-cachesize)。

参数取值与其它数据库选项的完整说明见 [CACHEMODEL](../02-ddl/01-database.md#cachemodel)、[CACHESIZE](../02-ddl/01-database.md#cachesize)。

### 创建与修改示例

```sql
-- 创建时启用
CREATE DATABASE power CACHEMODEL 'both' CACHESIZE 16;

-- 已有库上启用或调整
ALTER DATABASE power CACHEMODEL 'both';
ALTER DATABASE power CACHESIZE 32;
```

启用后可用 `SHOW CREATE DATABASE` 确认参数，并用 `SHOW VGROUPS` 查看各 vnode 的 `cacheload`（当前 last 缓存占用，单位为字节）。

## 实践示例

以下用智能电表数据对比开启读缓存前后的 `LAST` / `LAST_ROW` 时延。先用 `taosBenchmark` 生成测试数据：

```shell
taosBenchmark -d power -Q --start-timestamp=1600000000000 --tables=10000 --records=10000 --time-step=10000 -y
```

该命令创建数据库 `power`，超级表 `meters`，约 1 亿条记录：10000 个子表，每表 10000 条，起始时间戳 `1600000000000`（`2020-09-13T20:26:40+08:00`），采集间隔 10 秒。默认 `CACHEMODEL` 为 `none`。

未启用读缓存时查询最新电流与时间戳：

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

再次查询。首次查询会填充缓存，后续查询时延通常明显下降：

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

本例中时延从约 353 / 344 ms 降至约 44 ms。实际效果与数据规模、硬件和并发负载有关。

## 相关文档

- [LAST](./03-function.md#last) / [LAST_ROW](./03-function.md#last_row)：函数语义
- [CACHEMODEL](../02-ddl/01-database.md#cachemodel) / [CACHESIZE](../02-ddl/01-database.md#cachesize)：参数说明
- [数据缓存](../../15-internals/07-cache.md)：写缓存、元数据缓存与 WAL 等其它缓存类型
- [高吞吐写入](../../10-developer-guide/05-high-throughput.md)：读缓存对写入的影响与调优
