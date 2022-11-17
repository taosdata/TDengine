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
  | STRICT {'off' | 'on'}
  | WAL_LEVEL {1 | 2}
  | VGROUPS value
  | SINGLE_STABLE {0 | 1}
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
- KEEP：表示数据文件保存的天数，缺省值为 3650，取值范围 [1, 365000]，且必须大于或等于 DURATION 参数值。数据库会自动删除保存时间超过 KEEP 值的数据。KEEP 可以使用加单位的表示形式，如 KEEP 100h、KEEP 10d 等，支持 m（分钟）、h（小时）和 d（天）三个单位。也可以不写单位，如 KEEP 50，此时默认单位为天。
- PAGES：一个 VNODE 中元数据存储引擎的缓存页个数，默认为 256，最小 64。一个 VNODE 元数据存储占用 PAGESIZE \* PAGES，默认情况下为 1MB 内存。
- PAGESIZE：一个 VNODE 中元数据存储引擎的页大小，单位为 KB，默认为 4 KB。范围为 1 到 16384，即 1 KB 到 16 MB。
- PRECISION：数据库的时间戳精度。ms 表示毫秒，us 表示微秒，ns 表示纳秒，默认 ms 毫秒。
- REPLICA：表示数据库副本数，取值为 1 或 3，默认为 1。在集群中使用，副本数必须小于或等于 DNODE 的数目。
- RETENTIONS：表示数据的聚合周期和保存时长，如 RETENTIONS 15s:7d,1m:21d,15m:50d 表示数据原始采集周期为 15 秒，原始数据保存 7 天；按 1 分钟聚合的数据保存 21 天；按 15 分钟聚合的数据保存 50 天。目前支持且只支持三级存储周期。
- STRICT：表示数据同步的一致性要求，默认为 off。
  - on 表示强一致，即运行标准的 raft 协议，半数提交返回成功。
  - off 表示弱一致，本地提交即返回成功。
- WAL_LEVEL：WAL 级别，默认为 1。
  - 1：写 WAL，但不执行 fsync。
  - 2：写 WAL，而且执行 fsync。
- VGROUPS：数据库中初始 vgroup 的数目。
- SINGLE_STABLE：表示此数据库中是否只可以创建一个超级表，用于超级表列非常多的情况。
  - 0：表示可以创建多张超级表。
  - 1：表示只可以创建一张超级表。
- WAL_RETENTION_PERIOD：wal 文件的额外保留策略，用于数据订阅。wal 的保存时长，单位为 s。单副本默认为 0，即落盘后立即删除。-1 表示不删除。多副本默认为 4 天。
- WAL_RETENTION_SIZE：wal 文件的额外保留策略，用于数据订阅。wal 的保存的最大上限，单位为 KB。单副本默认为 0，即落盘后立即删除。多副本默认为-1，表示不删除。
- WAL_ROLL_PERIOD：wal 文件切换时长，单位为 s。当 wal 文件创建并写入后，经过该时间，会自动创建一个新的 wal 文件。单副本默认为 0，即仅在落盘时创建新文件。多副本默认为 1 天。
- WAL_SEGMENT_SIZE：wal 单个文件大小，单位为 KB。当前写入文件大小超过上限后会自动创建一个新的 wal 文件。默认为 0，即仅在落盘时创建新文件。

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
  | WAL_LEVEL value
  | WAL_FSYNC_PERIOD value
  | KEEP value
}
```

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
SHOW CREATE DATABASE db_name;
```

常用于数据库迁移。对一个已经存在的数据库，返回其创建语句；在另一个集群中执行该语句，就能得到一个设置完全相同的 Database。

### 查看数据库参数

```sql
SELECT * FROM INFORMATION_SCHEMA.INS_DATABASES WHERE NAME='DBNAME' \G;
```

会列出指定数据库的配置参数，并且每行只显示一个参数。

## 删除过期数据

```sql
TRIM DATABASE db_name;
```

删除过期数据，并根据多级存储的配置归整数据。
