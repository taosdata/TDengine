---
sidebar_label: 读缓存
title: 读缓存
toc_max_heading_level: 4
---

在物联网（IoT）和工业互联网（IIoT）大数据应用场景中，实时数据的价值往往远超历史数据。企业不仅需要数据处理系统具备高效的实时写入能力，更需要能快速获取设备的最新状态，或者对最新数据进行实时计算和分析。无论是工业设备的状态监控、车联网中的车辆位置追踪，还是智能仪表的实时读数，当前值都是业务运行中不可或缺的核心数据。这些数据直接关系到生产安全、运营效率以及用户体验。

例如，在工业生产中，生产线设备的当前运行状态至关重要。操作员需要实时监控温度、压力、转速等关键指标，一旦设备出现异常，这些数据必须即时呈现，以便迅速调整工艺参数，避免停产或更大的损失。在车联网领域，以滴滴为例，车辆的实时位置数据是滴滴平台优化派单策略、提升运营效率的关键，确保每位乘客快速上车并享受更高质量的出行体验。

同时，看板系统和智能仪表作为现场操作和用户端的窗口，也需要实时数据支撑。无论是工厂管理者通过看板获取的实时生产指标，还是家庭用户随时查询智能水表、电表的用量，实时性不仅影响到运营和决策效率，更直接关系到用户对服务的满意程度。

## 传统缓存方案的局限性

为了满足这些高频实时查询需求，许多企业选择将 Redis 等缓存技术集成到大数据平台中，通过在数据库和应用之间添加一层缓存来提升查询性能。然而，这种方法也带来了不少问题：
- 系统复杂性增加：需要额外部署和维护缓存集群，对系统架构提出了更高的要求。
- 运营成本上升：需要额外的硬件资源来支撑缓存，增加了维护和管理的开销。
- 一致性问题：缓存和数据库之间的数据同步需要额外的机制来保障，否则可能出现数据不一致的情况。

## TDengine TSDB 的解决方案：内置读缓存

为了解决这些问题，TDengine TSDB 针对物联网和工业互联网的高频实时查询场景，设计并实现了读缓存机制。这一机制能够自动将每张表的最后一条记录缓存到内存中，从而在不引入第三方缓存技术的情况下，直接满足用户对当前值的实时查询需求。

TDengine TSDB 采用时间驱动的缓存管理策略，将最新数据优先存储在缓存中，查询时无需访问硬盘即可快速返回结果。当缓存容量达到设定上限时，系统会批量将最早的数据写入硬盘，既提升了查询效率，也有效减少了硬盘的写入负担，延长硬件使用寿命。

用户可通过设置 cachemodel 参数，自定义缓存模式，包括缓存最新一行数据、每列最近的非 NULL 值，或同时缓存行和列的数据。这种灵活设计在物联网场景中尤为重要，使设备状态的实时查询更加高效精准。

这种读缓存机制的内置化设计显著降低了查询延迟，避免了引入 Redis 等外部系统的复杂性和运维成本。同时，减少了频繁查询对存储系统的压力，大幅提升系统的整体吞吐能力，确保在高并发场景下依然稳定高效运行。通过读缓存，TDengine TSDB 为用户提供了一种更轻量化的实时数据处理方案，不仅优化了查询性能，还降低了整体运维成本，为物联网和工业互联网用户提供强有力的技术支持。

## TDengine TSDB 的读缓存配置

在创建数据库时，用户可以选择是否启用缓存机制以存储该数据库中每张子表的最新数据。这一缓存机制由数据库创建参数 cachemodel 进行控制。参数 cachemodel 具有如下 4 种情况：
- none：不缓存
- last_row：缓存子表最近一行数据，这将显著改善 last_row 函数的性能
- last_value：缓存子表每一列最近的非 NULL 值，这将显著改善无特殊影响（比如 WHERE、ORDER BY、GROUP BY、INTERVAL）时的 last 函数的性能
- both：同时缓存最近的行和列，即等同于上述 cachemodel 值为 last_row 和 last_value 的行为同时生效

当使用数据库读缓存时，可以使用参数 cachesize 来配置每个 vnode 的内存大小。
- cachesize：表示每个 vnode 中用于缓存子表最近数据的内存大小。默认为 1，范围是 [1，65536]，单位是 MB。需要根据机器内存合理配置。

关于数据库的具体创建，相关参数和操作说明请参考[创建数据库](../../reference/taos-sql/database/)

## 实时数据查询的缓存实践

本节以智能电表为例，来详细看看 LAST 缓存对实时数据查询的性能提升。首先使用 taosBenchmark 工具，生成本章内容需要的智能电表的时序数据。

```shell
# taosBenchmark -d power -Q --start-timestamp=1600000000000 --tables=10000 --records=10000 --time-step=10000 -y
```

上面的命令，taosBenchmark 工具在 TDengine TSDB 中生成了一个用于测试的 电表数据库 power，产生共 10 亿条时序数据。时序数据的时间戳从 `1600000000000（2020-09-13T20:26:40+08:00）` 开始，超级表为 `meters`，包含 10000  个设备（子表），每个设备有 10000 条数据，时序数据的采集频率是 10 秒/条。

查询任意一个电表的最新的电流和时间戳数据，执行如下 SQL

```sql
taos> select last(ts, current) from meters;
        last(ts)         |    last(current)     |
=================================================
 2020-09-15 00:13:10.000 |            1.1294620 |
Query OK, 1 row(s) in set (0.353815s)

taos> select last_row(ts, current) from meters;
      last_row(ts)       |  last_row(current)   |
=================================================
 2020-09-15 00:13:10.000 |            1.1294620 |
Query OK, 1 row(s) in set (0.344070s)
```

希望使用缓存来查询任意一个电表的最新时间戳数据，执行如下 SQL，并检查数据库的缓存生效。

```sql
taos> alter database power cachemodel 'both' ;
Query OK, 0 row(s) affected (0.046092s)

taos> show create database power\G;
*************************** 1.row ***************************
       Database: power
Create Database: CREATE DATABASE `power` BUFFER 256 CACHESIZE 1 CACHEMODEL 'both' COMP 2 DURATION 14400m WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 100 STT_TRIGGER 2 KEEP 5256000m,5256000m,5256000m PAGES 256 PAGESIZE 4 PRECISION 'ms' REPLICA 1 WAL_LEVEL 1 VGROUPS 10 SINGLE_STABLE 0 TABLE_PREFIX 0 TABLE_SUFFIX 0 TSDB_PAGESIZE 4 WAL_RETENTION_PERIOD 3600 WAL_RETENTION_SIZE 0 KEEP_TIME_OFFSET 0
Query OK, 1 row(s) in set (0.000282s)
```

再次查询电表的最新的实时数据，第一次查询会做缓存计算，后续的查询时延就大大缩减。

```sql
taos> select last(ts, current) from meters;
        last(ts)         |    last(current)     |
=================================================
 2020-09-15 00:13:10.000 |            1.1294620 |
Query OK, 1 row(s) in set (0.044021s)

taos> select last_row(ts, current) from meters;
      last_row(ts)       |  last_row(current)   |
=================================================
 2020-09-15 00:13:10.000 |            1.1294620 |
Query OK, 1 row(s) in set (0.046682s)
```

可以看到查询的时延从 353/344ms 缩短到了 44ms，提升约 8 倍。
