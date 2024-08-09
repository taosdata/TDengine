---
sidebar_label: 数据缓存
title: 数据缓存
toc_max_heading_level: 4
---

在工业互联网和物联网大数据应用场景中，时序数据库的性能表现尤为关键。这类应用程序不仅要求数据的实时写入能力，还需求能够迅速获取设备的最新状态或对最新数据进行实时计算。通常，大数据平台会通过部署 Redis 或类似的缓存技术来满足这些需求。然而，这种做法会增加系统的复杂性和运营成本。

为了解决这一问题，TDengine 采用了针对性的缓存优化策略。通过精心设计的缓存机制，TDengine 实现了数据的实时高效写入和快速查询，从而有效降低整个集群的复杂性和运营成本。这种优化不仅提升了性能，还为用户带来了更简洁、易用的解决方案，使他们能够更专注于核心业务的发展。

## 写缓存

TDengine 采用了一种创新的时间驱动缓存管理策略，亦称为写驱动的缓存管理机制。这一策略与传统的读驱动的缓存模式有所不同，其核心思想是将最新写入的数据优先保存在缓存中。当缓存容量达到预设的临界值时，系统会将最早存储的数据批量写入硬盘，从而实现缓存与硬盘之间的动态平衡。

在物联网数据应用中，用户往往最关注最近产生的数据，即设备的当前状态。TDengine 充分利用了这一业务特性，将最近到达的当前状态数据优先存储在缓存中，以便用户能够快速获取所需信息。

为了实现数据的分布式存储和高可用性，TDengine 引入了虚拟节点（vnode）的概念。每个 vnode 可以拥有多达 3 个副本，这些副本共同组成一个 vnode group，简称 vgroup。在创建数据库时，用户需要确定每个 vnode 的写入缓存大小，以确保数据的合理分配和高效存储。

创建数据库时的两个关键参数 `vgroups` 和 `buffer` 分别决定了数据库中的数据由多少个 vgroup 进行处理，以及为每个 vnode 分配多少写入缓存。通过合理配置这两个
参数，用户可以根据实际需求调整数据库的性能和存储容量，从而实现最佳的性能和成本效益。

例 如， 下面的 SQL 创建了包含 10 个 vgroup，每个 vnode 占 用 256MB 内存的数据库。
```sql
CREATE DATABASE POWER VGROUPS 10 BUFFER 256 CACHEMODEL 'NONE' PAGES 128 PAGESIZE 16;
```

缓存越大越好，但超过一定阈值后再增加缓存对写入性能提升并无帮助。

## 读缓存

在创建数据库时，用户可以选择是否启用缓存机制以存储该数据库中每张子表的最新数据。这一缓存机制由数据库创建参数 cachemodel 进行控制。参数 cachemodel 具有如
下 4 种情况：
- none: 不缓存
- last_row: 缓存子表最近一行数据，这将显著改善 last_row 函数的性能
- last_value: 缓存子表每一列最近的非 NULL 值，这将显著改善无特殊影响（比如 WHERE， ORDER BY， GROUP BY， INTERVAL）时的 last 函数的性能
- both: 同时缓存最近的行和列，即等同于上述 cachemodel 值为 last_row 和 last_value 的行为同时生效

当使用数据库读缓存时，可以使用参数 cachesize 来配置每个 vnode 的内存大小。
- cachesize：表示每个 vnode 中用于缓存子表最近数据的内存大小。默认为 1 ，范围是[1， 65536]，单位是 MB。需要根据机器内存合理配置。

## 元数据缓存

为了提升查询和写入操作的效率，每个 vnode 都配备了缓存机制，用于存储其曾经获取过的元数据。这一元数据缓存的大小由创建数据库时的两个参数 pages 和 pagesize 共同决定。其中，pagesize 参数的单位是 KB，用于指定每个缓存页的大小。如下 SQL 会为数据库 power 的每个 vnode 创建 128 个 page、每个 page 16KB 的元数据缓存

```sql
CREATE DATABASE POWER PAGES 128 PAGESIZE 16;
```

## 文件系统缓存

TDengine 采用 WAL 技术作为基本的数据可靠性保障手段。WAL 是一种先进的数据保护机制，旨在确保在发生故障时能够迅速恢复数据。其核心原理在于，在数据实际写入数据存储层之前，先将其变更记录到一个日志文件中。这样一来，即便集群遭遇崩溃或其他故障，也能确保数据安全无损。

TDengine 利用这些日志文件实现故障前的状态恢复。在写入 WAL 的过程中，数据是以顺序追加的方式写入硬盘文件的。因此，文件系统缓存在此过程中发挥着关键作用，对写入性能产生显著影响。为了确保数据真正落盘，系统会调用 fsync 函数，该函数负责将文件系统缓存中的数据强制写入硬盘。

数据库参数 wal_level 和 wal_fsync_period 共同决定了 WAL 的保存行为。。
- wal_level：此参数控制 WAL 的保存级别。级别 1 表示仅将数据写入 WAL，但不立即执行 fsync 函数；级别 2 则表示在写入 WAL 的同时执行 fsync 函数。默认情况下，wal_level 设为 1。虽然执行 fsync 函数可以提高数据的持久性，但相应地也会降低写入性能。
- wal_fsync_period：当 wal_level 设置为 2 时，这个参数控制执行 fsync 的频率。设置为 0 表示每次写入后立即执行 fsync，这可以确保数据的安全性，但可能会牺牲一些性能。当设置为大于 0 的数值时，表示 fsync 周期，默认为 3000，范围是[1， 180000]，单位毫秒。

```sql
CREATE DATABASE POWER WAL_LEVEL 1 WAL_FSYNC_PERIOD 3000;
```

在创建数据库时可以选择不同的参数类型，来选择性能优先或者可靠性优先。
- 1: 写 WAL 但不执行 fsync ，新写入 WAL 的数据保存在文件系统缓存中但并未写入磁盘，这种方式性能优先
- 2: 写 WAL 且执行 fsync，新写入 WAL 的数据被立即同步到磁盘上，可靠性更高

## 实时数据查询的缓存实践

本节以智能电表为例，来详细看看 LAST 缓存对实时数据查询的性能提升。首先使用 taosBenchmark 工具，生成本章内容需要的智能电表的时序数据。

```shell
# taosBenchmark -d power -Q --start-timestamp=1600000000000 --tables=10000 --records=10000 --time-step=10000 -y
```

上面的命令，taosBenchmark 工具在 TDengine 中生成了一个用于测试的 电表数据库 power，产生共 10 亿条时序数据。时序数据的时间戳从 `1600000000000（2020-09-13T20:26:40+08:00）`开始，超级表为 `meter`s，包含 10000  个设备（子表），每个设备有 10000 条数据，时序数据的采集频率是 10 秒/ 条。

查询任意一个电表的最新的电流和时间戳数据，执行如下 SQL

```sql
taos> select last(ts,current) from meters;
        last(ts)         |    last(current)     |
=================================================
 2020-09-15 00:13:10.000 |            1.1294620 |
Query OK, 1 row(s) in set (0.353815s)

taos> select last_row(ts,current) from meters;
      last_row(ts)       |  last_row(current)   |
=================================================
 2020-09-15 00:13:10.000 |            1.1294620 |
Query OK, 1 row(s) in set (0.344070s)
```

希望使用缓存来查询任意一个电表的最新时间戳数据，执行如下 SQL ，并检查数据库的缓存生效。

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
taos> select last(ts,current) from meters;
        last(ts)         |    last(current)     |
=================================================
 2020-09-15 00:13:10.000 |            1.1294620 |
Query OK, 1 row(s) in set (0.044021s)

taos> select last_row(ts,current) from meters;
      last_row(ts)       |  last_row(current)   |
=================================================
 2020-09-15 00:13:10.000 |            1.1294620 |
Query OK, 1 row(s) in set (0.046682s)
```

可以看到查询的时延从 353/344ms 缩短到了 44ms，提升约 8 倍。