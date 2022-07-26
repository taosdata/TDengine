---
sidebar_label: 数据库管理
title: 数据库管理
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
  | FSYNC value
  | MAXROWS value
  | MINROWS value
  | KEEP value
  | PAGES value
  | PAGESIZE  value
  | PRECISION {'ms' | 'us' | 'ns'}
  | REPLICA value
  | RETENTIONS ingestion_duration:keep_duration ...
  | STRICT {'off' | 'on'}
  | WAL {1 | 2}
  | VGROUPS value
  | SINGLE_STABLE {0 | 1}
  | WAL_RETENTION_PERIOD value
  | WAL_ROLL_PERIOD value
  | WAL_RETENTION_SIZE value
  | WAL_SEGMENT_SIZE value
}
```

### 参数说明
- buffer: 一个 VNODE 写入内存池大小，单位为MB，默认为96，最小为3，最大为16384。
- CACHEMODEL：表示是否在内存中缓存子表的最近数据。默认为none。
   - none：表示不缓存。
   - last_row：表示缓存子表最近一行数据。这将显著改善 LAST_ROW 函数的性能表现。
   - last_value：表示缓存子表每一列的最近的非 NULL 值。这将显著改善无特殊影响（WHERE、ORDER BY、GROUP BY、INTERVAL）下的 LAST 函数的性能表现。
   - both：表示同时打开缓存最近行和列功能。
- CACHESIZE：表示缓存子表最近数据的内存大小。默认为 1 ，范围是[1, 65536]，单位是 MB。
- COMP：表示数据库文件压缩标志位，缺省值为 2，取值范围为 [0, 2]。
   - 0：表示不压缩。
   - 1：表示一阶段压缩。
   - 2：表示两阶段压缩。
- DURATION：数据文件存储数据的时间跨度。可以使用加单位的表示形式，如 DURATION 100h、DURATION 10d等，支持 m（分钟）、h（小时）和 d（天）三个单位。不加时间单位时默认单位为天，如 DURATION 50 表示 50 天。
- FSYNC：当 WAL 参数设置为2时，落盘的周期。默认为3000，单位毫秒。最小为0，表示每次写入立即落盘；最大为180000，即三分钟。
- MAXROWS：文件块中记录的最大条数，默认为4096条。
- MINROWS：文件块中记录的最小条数，默认为100条。
- KEEP：表示数据文件保存的天数，缺省值为 3650，取值范围 [1, 365000]，且必须大于或等于 DURATION 参数值。数据库会自动删除保存时间超过KEEP值的数据。KEEP 可以使用加单位的表示形式，如 KEEP 100h、KEEP 10d 等，支持m（分钟）、h（小时）和 d（天）三个单位。也可以不写单位，如 KEEP 50，此时默认单位为天。
- PAGES：一个 VNODE 中元数据存储引擎的缓存页个数，默认为256，最小64。一个 VNODE 元数据存储占用 PAGESIZE * PAGES，默认情况下为1MB内存。
- PAGESIZE：一个 VNODE 中元数据存储引擎的页大小，单位为KB，默认为4 KB。范围为1到16384，即1 KB到16 MB。
- PRECISION：数据库的时间戳精度。ms表示毫秒，us表示微秒，ns表示纳秒，默认ms毫秒。
- REPLICA：表示数据库副本数，取值为1或3，默认为1。在集群中使用，副本数必须小于或等于 DNODE 的数目。
- RETENTIONS：表示数据的聚合周期和保存时长，如RETENTIONS 15s:7d,1m:21d,15m:50d表示数据原始采集周期为15秒，原始数据保存7天；按1分钟聚合的数据保存21天；按15分钟聚合的数据保存50天。目前支持且只支持三级存储周期。
- STRICT：表示数据同步的一致性要求，默认为off。
   - on 表示强一致，即运行标准的 raft 协议，半数提交返回成功。
   - off表示弱一致，本地提交即返回成功。
- WAL：WAL级别，默认为1。
   - 1：写WAL，但不执行fsync。
   - 2：写WAL，而且执行fsync。
- VGROUPS：数据库中初始vgroup的数目。
- SINGLE_STABLE：表示此数据库中是否只可以创建一个超级表，用于超级表列非常多的情况。
   - 0：表示可以创建多张超级表。
   - 1：表示只可以创建一张超级表。
- WAL_RETENTION_PERIOD：wal文件的额外保留策略，用于数据订阅。wal的保存时长，单位为s。默认为0，即落盘后立即删除。-1表示不删除。
- WAL_RETENTION_SIZE：wal文件的额外保留策略，用于数据订阅。wal的保存的最大上限，单位为KB。默认为0，即落盘后立即删除。-1表示不删除。
- WAL_ROLL_PERIOD：wal文件切换时长，单位为s。当wal文件创建并写入后，经过该时间，会自动创建一个新的wal文件。默认为0，即仅在落盘时创建新文件。
- WAL_SEGMENT_SIZE：wal单个文件大小，单位为KB。当前写入文件大小超过上限后会自动创建一个新的wal文件。默认为0，即仅在落盘时创建新文件。

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
  | FSYNC value
  | KEEP value
  | WAL value
}
```

:::note
其它参数在3.0.0.0中暂不支持修改

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
