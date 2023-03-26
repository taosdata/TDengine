---
sidebar_label: 数据库
title: 数据库
description: "创建、删除数据库，查看、修改数据库参数"
---

## 创建数据库

```sql
CREATE DATABASE [IF NOT EXISTS] db_name [database_options]
 
database_options:
    database_option ...
 
database_option: {
    BUFFER value
  | CACHEMODEL {'none' | 'last_row' | 'last_value' | 'both'}
  | CACHESIZE value
  | COMP {0 | 1 | 2}
  | DURATION value
  | WAL_FSYNC_PERIOD value
  | MAXROWS value
  | MINROWS value
  | KEEP value
  | PAGES value
  | PAGESIZE  value
  | PRECISION {'ms' | 'us' | 'ns'}
  | REPLICA value
  | RETENTIONS ingestion_duration:keep_duration ...
  | WAL_LEVEL {1 | 2}
  | VGROUPS value
  | SINGLE_STABLE {0 | 1}
  | STT_TRIGGER value
  | TABLE_PREFIX value
  | TABLE_SUFFIX value
  | TSDB_PAGESIZE value
  | WAL_RETENTION_PERIOD value
  | WAL_ROLL_PERIOD value
  | WAL_RETENTION_SIZE value
  | WAL_SEGMENT_SIZE value
}
```

### 参数说明

- BUFFER: 一个 VNODE 写入内存池大小，单位为 MB，默认为 96，最小为 3，最大为 16384。
- CACHEMODEL：表示是否在内存中缓存子表的最近数据。默认为 none。
  - none：表示不缓存。
  - last_row：表示缓存子表最近一行数据。这将显著改善 LAST_ROW 函数的性能表现。
  - last_value：表示缓存子表每一列的最近的非 NULL 值。这将显著改善无特殊影响（WHERE、ORDER BY、GROUP BY、INTERVAL）下的 LAST 函数的性能表现。
  - both：表示同时打开缓存最近行和列功能。
- CACHESIZE：表示每个 vnode 中用于缓存子表最近数据的内存大小。默认为 1 ，范围是[1, 65536]，单位是 MB。
- COMP：表示数据库文件压缩标志位，缺省值为 2，取值范围为 [0, 2]。
  - 0：表示不压缩。
  - 1：表示一阶段压缩。
  - 2：表示两阶段压缩。
- DURATION：数据文件存储数据的时间跨度。可以使用加单位的表示形式，如 DURATION 100h、DURATION 10d 等，支持 m（分钟）、h（小时）和 d（天）三个单位。不加时间单位时默认单位为天，如 DURATION 50 表示 50 天。
- WAL_FSYNC_PERIOD：当 WAL 参数设置为 2 时，落盘的周期。默认为 3000，单位毫秒。最小为 0，表示每次写入立即落盘；最大为 180000，即三分钟。
- MAXROWS：文件块中记录的最大条数，默认为 4096 条。
- MINROWS：文件块中记录的最小条数，默认为 100 条。
- KEEP：表示数据文件保存的天数，缺省值为 3650，取值范围 [1, 365000]，且必须大于或等于 DURATION 参数值。数据库会自动删除保存时间超过 KEEP 值的数据。KEEP 可以使用加单位的表示形式，如 KEEP 100h、KEEP 10d 等，支持 m（分钟）、h（小时）和 d（天）三个单位。也可以不写单位，如 KEEP 50，此时默认单位为天。企业版支持[多级存储](https://docs.taosdata.com/tdinternal/arch/#%E5%A4%9A%E7%BA%A7%E5%AD%98%E5%82%A8)功能, 因此, 可以设置多个保存时间（多个以英文逗号分隔，最多 3 个，满足 keep 0 <= keep 1 <= keep 2，如 KEEP 100h,100d,3650d）; 社区版不支持多级存储功能（即使配置了多个保存时间, 也不会生效, KEEP 会取最大的保存时间）。
- PAGES：一个 VNODE 中元数据存储引擎的缓存页个数，默认为 256，最小 64。一个 VNODE 元数据存储占用 PAGESIZE \* PAGES，默认情况下为 1MB 内存。
- PAGESIZE：一个 VNODE 中元数据存储引擎的页大小，单位为 KB，默认为 4 KB。范围为 1 到 16384，即 1 KB 到 16 MB。
- PRECISION：数据库的时间戳精度。ms 表示毫秒，us 表示微秒，ns 表示纳秒，默认 ms 毫秒。
- REPLICA：表示数据库副本数，取值为 1 或 3，默认为 1。在集群中使用，副本数必须小于或等于 DNODE 的数目。
- RETENTIONS：表示数据的聚合周期和保存时长，如 RETENTIONS 15s:7d,1m:21d,15m:50d 表示数据原始采集周期为 15 秒，原始数据保存 7 天；按 1 分钟聚合的数据保存 21 天；按 15 分钟聚合的数据保存 50 天。目前支持且只支持三级存储周期。
- WAL_LEVEL：WAL 级别，默认为 1。
  - 1：写 WAL，但不执行 fsync。
  - 2：写 WAL，而且执行 fsync。
- VGROUPS：数据库中初始 vgroup 的数目。
- SINGLE_STABLE：表示此数据库中是否只可以创建一个超级表，用于超级表列非常多的情况。
  - 0：表示可以创建多张超级表。
  - 1：表示只可以创建一张超级表。
- STT_TRIGGER：表示落盘文件触发文件合并的个数。默认为 1，范围 1 到 16。对于少表高频场景，此参数建议使用默认配置，或较小的值；而对于多表低频场景，此参数建议配置较大的值。
- TABLE_PREFIX：内部存储引擎根据表名分配存储该表数据的 VNODE 时要忽略的前缀的长度。
- TABLE_SUFFIX：内部存储引擎根据表名分配存储该表数据的 VNODE 时要忽略的后缀的长度。
- TSDB_PAGESIZE：一个 VNODE 中时序数据存储引擎的页大小，单位为 KB，默认为 4 KB。范围为 1 到 16384，即 1 KB到 16 MB。
- WAL_RETENTION_PERIOD：数据订阅已消费WAL日志，WAL文件的最大额外保留的时长策略。单位为 s。默认为 0，表示无需额外保留。-1, 表示额外保留，时间无上限。
- WAL_RETENTION_SIZE：数据订阅已消费WAL日志，WAL文件的最大额外保留的累计大小策略。单位为 KB。默认为 0，表示无需额外保留。-1, 表示额外保留，累计大小无上限。
- WAL_ROLL_PERIOD：wal 文件切换时长，单位为 s。当WAL文件创建并写入后，经过该时间，会自动创建一个新的WAL文件。默认为 0，即仅在TSDB落盘时创建新文件。
- WAL_SEGMENT_SIZE：wal 单个文件大小，单位为 KB。当前写入文件大小超过上限后会自动创建一个新的WAL文件。默认为 0，即仅在TSDB落盘时创建新文件。

### 创建数据库示例

```sql
create database if not exists db vgroups 10 buffer 10

```

以上示例创建了一个有 10 个 vgroup 名为 db 的数据库， 其中每个 vnode 分配也 10MB 的写入缓存

### 使用数据库

```
USE db_name;
```

使用/切换数据库（在 REST 连接方式下无效）。

## 删除数据库

```
DROP DATABASE [IF EXISTS] db_name
```

删除数据库。指定 Database 所包含的全部数据表将被删除，该数据库的所有 vgroups 也会被全部销毁，请谨慎使用！

## 修改数据库参数

```sql
ALTER DATABASE db_name [alter_database_options]

alter_database_options:
    alter_database_option ...

alter_database_option: {
    CACHEMODEL {'none' | 'last_row' | 'last_value' | 'both'}
  | CACHESIZE value
  | BUFFER value
  | PAGES value
  | REPLICA value
  | STT_TRIGGER value
  | WAL_LEVEL value
  | WAL_FSYNC_PERIOD value
  | KEEP value
}
```

###  修改 CACHESIZE

修改数据库参数的命令使用简单，难的是如何确定是否需要修改以及如何修改。本小节描述如何判断数据库的 cachesize 是否够用。

1. 如何查看 cachesize?

通过 select * from information_schema.ins_databases; 可以查看这些 cachesize 的具体值。

2. 如何查看 cacheload?

通过 show <db_name>.vgroups; 可以查看 cacheload

3. 判断 cachesize 是否够用

如果 cacheload 非常接近 cachesize，则 cachesize 可能过小。 如果 cacheload 明显小于 cachesize 则 cachesize 是够用的。可以根据这个原则判断是否需要修改 cachesize 。具体修改值可以根据系统可用内存情况来决定是加倍或者是提高几倍。

:::note
其它参数在 3.0.0.0 中暂不支持修改

:::

## 查看数据库

### 查看系统中的所有数据库

```
SHOW DATABASES;
```

### 显示一个数据库的创建语句

```
SHOW CREATE DATABASE db_name \G;
```

常用于数据库迁移。对一个已经存在的数据库，返回其创建语句；在另一个集群中执行该语句，就能得到一个设置完全相同的 Database。

### 查看数据库参数

```sql
SELECT * FROM INFORMATION_SCHEMA.INS_DATABASES WHERE NAME='db_name' \G;
```

会列出指定数据库的配置参数，并且每行只显示一个参数。

## 删除过期数据

```sql
TRIM DATABASE db_name;
```

删除过期数据，并根据多级存储的配置归整数据。

## 调整VGROUP中VNODE的分布

```sql
REDISTRIBUTE VGROUP vgroup_no DNODE dnode_id1 [DNODE dnode_id2] [DNODE dnode_id3]
```

按照给定的dnode列表，调整vgroup中的vnode分布。因为副本数目最大为3，所以最多输入3个dnode。

## 自动调整VGROUP中VNODE的分布

```sql
BALANCE VGROUP
```

自动调整集群所有vgroup中的vnode分布，相当于在vnode级别对集群进行数据的负载均衡操作。
