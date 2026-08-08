---
sidebar_label: 数据库
title: 数据库
description: 创建、删除数据库，查看、修改数据库参数
---

## 创建数据库

```sql
CREATE DATABASE [IF NOT EXISTS] db_name [database_options];

database_options:
    database_option ...

database_option: {
    VGROUPS value
  | PRECISION {'ms' | 'us' | 'ns'}
  | REPLICA value
  | REPLICAS value
  | BUFFER value
  | PAGES value
  | PAGESIZE  value
  | CACHEMODEL {'none' | 'last_row' | 'last_value' | 'both'}
  | CACHESIZE value
  | CACHESHARDBITS value
  | COMP {0 | 1 | 2}
  | DURATION value
  | MAXROWS value
  | MINROWS value
  | KEEP value
  | KEEP_TIME_OFFSET value
  | RETENTIONS retention_list
  | SCHEMALESS {0 | 1}
  | STT_TRIGGER value
  | SINGLE_STABLE {0 | 1}
  | TABLE_PREFIX value
  | TABLE_SUFFIX value
  | DNODES value
  | TSDB_PAGESIZE value
  | WAL_LEVEL {1 | 2}
  | WAL_FSYNC_PERIOD value
  | WAL_RETENTION_PERIOD value
  | WAL_RETENTION_SIZE value
  | WAL_ROLL_PERIOD value
  | WAL_SEGMENT_SIZE value
  | COMPACT_INTERVAL value
  | COMPACT_TIME_RANGE value
  | COMPACT_TIME_OFFSET value
  | SS_KEEPLOCAL value
  | SS_CHUNKPAGES value
  | SS_COMPACT value
  | ENCRYPT_ALGORITHM value
  | IS_AUDIT {0 | 1}
  | ALLOW_DROP {0 | 1}
  | SECURE_DELETE {0 | 1}
  | SECURITY_LEVEL value
}
```

### 选项说明

#### VGROUPS

数据库中初始 `vgroup` 的数目。

#### PRECISION

数据库的时间戳精度。

- `ms`：毫秒，默认值。
- `us`：微秒。
- `ns`：纳秒。

#### REPLICA

表示数据库副本数，取值为 `1`、`2` 或 `3`，默认为 `1`。`2` 仅在企业版支持。在集群中使用时，副本数必须小于或等于 `DNODE` 的数目。`REPLICAS` 是 `REPLICA` 的等价写法。

- 单副本数据库可变更为双副本数据库，但不支持从双副本变更为其他副本数，也不支持从三副本变更为双副本。

#### BUFFER

一个 `vnode` 写入内存池大小，单位为 MB，默认为 `256`，最小为 `3`，最大为 `16384`。

#### PAGES

一个 `vnode` 中元数据存储引擎的缓存页个数，默认为 `256`，最小为 `64`。一个 `vnode` 元数据存储占用 `PAGESIZE * PAGES`，默认情况下为 1 MB 内存。

#### PAGESIZE

一个 `vnode` 中元数据存储引擎的页大小，单位为 KB，默认为 4 KB。取值范围为 `[1, 16384]`，即 1 KB 到 16 MB。

#### CACHEMODEL

表示是否在内存中缓存子表的最近数据。

- `none`：不缓存，默认值。
- `last_row`：缓存子表最近一行数据，可以改善 `LAST_ROW` 函数的性能表现。
- `last_value`：缓存子表每一列最近的非 `NULL` 值，可以改善无 `WHERE`、`ORDER BY`、`GROUP BY`、`INTERVAL` 影响时 `LAST` 函数的性能表现。
- `both`：同时缓存最近行和最近值。

:::note
频繁切换 `CACHEMODEL` 可能导致 `LAST`/`LAST_ROW` 的查询结果短期内不准确，请谨慎操作。
:::

#### CACHESIZE

表示每个 `vnode` 中用于缓存子表最近数据的内存大小。默认为 `1`，取值范围为 `[1, 65536]`，单位为 MB。

#### CACHESHARDBITS

表示 `last` 缓存（LRU cache）的分片位数，用于控制缓存内部的并发锁粒度。默认为 `-1`（自动推算），取值范围为 `[-1, 19]`。

- 实际分片数为 `2^CACHESHARDBITS`，例如 `CACHESHARDBITS=3` 表示 8 个分片。
- 取值为 `-1` 时，系统自动根据 `CACHESIZE` 推算合适的分片位数，规则如下：
  - 每个分片至少 512 KB，即理论最大分片数为 `CACHESIZE / 512 KB`。
  - 对该值取 `log2`（向下取整）得到分片位数，上限为 6，即最多 64 个分片。
  - 当 `CACHESIZE < 512 KB` 时，分片位数为 0，即只有 1 个分片。
  - 自动推算示例：

  | `CACHESIZE` | 理论最大分片数 | 分片位数 | 实际分片数 |
  | ----------- | --- | --- | --- |
  | 1 MB        | 2   | 1   | 2  |
  | 4 MB        | 8   | 3   | 8  |
  | 32 MB       | 64  | 6   | 64 |
  | 256 MB      | 512 | 6   | 64 |

- 分片数越多，并发写入缓存时的锁竞争越小，适合高并发场景；但分片数过多会增加内存管理开销。

:::warning
修改 `CACHESHARDBITS` 会导致该 `vnode` 的全部 `last` 缓存立即失效，已缓存的数据需重新从磁盘加载，短期内可能影响查询性能。
:::

#### COMP

表示数据库文件压缩标志位，默认值为 `2`，取值范围为 `[0, 2]`。

- `0`：不压缩。
- `1`：一阶段压缩。
- `2`：两阶段压缩。

#### DURATION

数据文件存储数据的时间跨度。默认值为 `10d`，取值范围为 `[60m, 3650d]`。

- 可以使用带单位的写法，如 `DURATION 100h`、`DURATION 10d`，支持 `m`（分钟）、`h`（小时）和 `d`（天）。
- 不加时间单位时默认单位为天，如 `DURATION 50` 表示 50 天。

#### MAXROWS

文件块中记录的最大条数，默认为 `4096` 条。

#### MINROWS

文件块中记录的最小条数，默认为 `100` 条。

#### KEEP

表示数据文件保存的时长，默认值为 `3650` 天，取值范围为 `[1, 365000]`，且必须大于或等于 `DURATION` 参数值的 3 倍。

- 数据库会自动删除保存时间超过 `KEEP` 值的数据，从而释放存储空间。
- `KEEP` 可以使用带单位的写法，如 `KEEP 100h`、`KEEP 10d`，支持 `m`（分钟）、`h`（小时）和 `d`（天）。
- 也可以不写单位，如 `KEEP 50`，此时默认单位为天。
- 企业版支持 [多级存储](../../12-operations-and-tooling/02-operations/01-planning.md#多级存储)，可以设置多个保存时间。多个值以英文逗号分隔，最多 3 个，并满足 `keep0 <= keep1 <= keep2`，如 `KEEP 100h,100d,3650d`。
- 社区版不支持多级存储功能。即使配置多个保存时间，也不会生效，`KEEP` 会取最大的保存时间。

更多说明参见 [关于主键时间戳](../03-data-write/01-insert.md)。

#### KEEP_TIME_OFFSET

删除或迁移保存时间超过 `KEEP` 值的数据时的延迟执行时间，默认值为 `0h`。

- 在数据文件保存时间超过 KEEP 后，删除或迁移操作不会立即执行，而会额外等待本参数指定的时间间隔，以实现与业务高峰期错开的目的。

#### STT_TRIGGER

表示落盘文件触发文件合并的个数。

- 对于少表高频写入场景，此参数建议使用默认配置；
- 而对于多表低频写入场景，此参数建议配置较大的值。

#### SINGLE_STABLE

表示此数据库中是否只可以创建一个超级表，用于超级表列非常多的场景。

- `0`：可以创建多张超级表。
- `1`：只可以创建一张超级表。

#### RETENTIONS

指定数据库中数据的聚合周期和保存时长，用于保留多级聚合结果。该选项与降采样存储相关，更多说明请参见 [降采样存储](../05-materialized-agg/02-rsma.md)。

#### SCHEMALESS

指定数据库是否允许无模式写入（schemaless）。

- `0`：不允许。
- `1`：允许。

#### TABLE_PREFIX

分配数据表到某个 `vgroup` 时，用于忽略或仅使用表名前缀的长度值。

- 当其为正值时，决定表分配到哪个 `vgroup` 时会忽略表名中指定长度的前缀。
- 当其为负值时，决定表分配到哪个 `vgroup` 时只使用表名中指定长度的前缀。
- 例如，假定表名为 `v30001`，当 `TABLE_PREFIX = 2` 时，使用 `0001` 来决定分配到哪个 `vgroup`；当 `TABLE_PREFIX = -2` 时，使用 `v3` 来决定分配到哪个 `vgroup`。

#### TABLE_SUFFIX

分配数据表到某个 `vgroup` 时，用于忽略或仅使用表名后缀的长度值。

- 当其为正值时，决定表分配到哪个 `vgroup` 时会忽略表名中指定长度的后缀。
- 当其为负值时，决定表分配到哪个 `vgroup` 时只使用表名中指定长度的后缀。
- 例如，假定表名为 `v30001`，当 `TABLE_SUFFIX = 2` 时，使用 `v300` 来决定分配到哪个 `vgroup`；当 `TABLE_SUFFIX = -2` 时，使用 `01` 来决定分配到哪个 `vgroup`。

#### TSDB_PAGESIZE

一个 `vnode` 中时序数据存储引擎的页大小，单位为 KB，默认为 4 KB。取值范围为 `[1, 16384]`，即 1 KB 到 16 MB。

#### DNODES

指定 `vnode` 所在的 `DNODE` 列表，如 `'1,2,3'`，以逗号分隔且字符间不能有空格（仅企业版支持）。

#### WAL_LEVEL

`WAL` 级别，默认为 `1`。

- `1`：写 `WAL`，但不执行 `fsync`。
- `2`：写 `WAL`，并执行 `fsync`。

#### WAL_FSYNC_PERIOD

当 `WAL_LEVEL` 参数设置为 `2` 时，用于设置落盘周期。默认为 `3000`，单位为毫秒。最小为 `0`，表示每次写入立即落盘；最大为 `180000`，即 3 分钟。

#### WAL_RETENTION_PERIOD

为了数据订阅消费，设置 `WAL` 日志文件额外保留的最大时长。`WAL` 日志清理不受订阅客户端消费状态影响。单位为秒。默认为 `3600`，表示保留最近 3600 秒的数据。

#### WAL_RETENTION_SIZE

为了数据订阅消费，设置 `WAL` 日志文件额外保留的最大累计大小。单位为 KB。默认为 `0`，表示累计大小无上限。

#### WAL_ROLL_PERIOD

`WAL` 文件滚动周期。通常保持默认值即可。

#### WAL_SEGMENT_SIZE

单个 `WAL` 文件段大小。通常保持默认值即可。

#### COMPACT_INTERVAL

自动 `compact` 触发周期（从 `1970-01-01T00:00:00Z` 开始切分的时间周期，仅企业版支持）。

- 取值范围：`0` 或 `[10m, keep2]`，单位为 `m`（分钟）、`h`（小时）、`d`（天）。
- 不加时间单位时默认单位为天，默认值为 `0`，即不触发自动 `compact` 功能。
- 如果数据库中有未完成的 `compact` 任务，不会重复下发 `compact` 任务。

#### COMPACT_TIME_RANGE

自动 `compact` 任务触发的 `compact` 时间范围（仅企业版支持）。

- 取值范围：`[-keep2, -duration]`，单位为 `m`（分钟）、`h`（小时）、`d`（天）。
- 不加时间单位时默认单位为天，默认值为 `[0, 0]`。
- 取默认值 `[0, 0]` 时，如果 `COMPACT_INTERVAL` 大于 0，会按照 `[-keep2, -duration]` 下发自动 `compact`。
- 要关闭自动 `compact` 功能，需要将 `COMPACT_INTERVAL` 设置为 `0`。
- 例如，`-300,-200` 表示每次触发自动 `compact` 时，整理距今 300 天到 200 天之间的数据。
- 这些值必须为负数，表示待 `compact` 的时间范围位于过去。如果数据库的 `DURATION` 仍为默认值 `10d`，则 `-300,-5` 会报错，因为第二个值（距今 5 天）比 `-DURATION`（距今 10 天）更靠近当前时间。

#### COMPACT_TIME_OFFSET

自动 `compact` 任务触发的 `compact` 时间相对本地时间的偏移量（仅企业版支持）。取值范围为 `[0, 23]`，单位为 `h`（小时），默认值为 `0`。以 UTC 0 时区为例：

- 如果 `COMPACT_INTERVAL` 为 `1d`，当 `COMPACT_TIME_OFFSET` 为 `0` 时，在每天 0 点下发自动 `compact`。
- 如果 `COMPACT_TIME_OFFSET` 为 `2`，在每天 2 点下发自动 `compact`。

#### SS_KEEPLOCAL

使用共享存储时，数据在本地保留的时长，即数据文件在本地磁盘保留多长时间后可以上传到共享存储（仅企业版支持）。取值范围为 1 天到 36500 天，且必须大于或等于 `DURATION` 参数的 3 倍。默认为 365 天。

#### SS_CHUNKPAGES

使用共享存储时，上传对象大小的阈值（仅企业版支持）。小于此选项的数据文件不会上传，单位是 TSDB 页，默认每页 4 KB。

- 取值范围：[131072, 1048576]，默认值为 131072。
- 只能在创建数据库时指定，创建后不可修改。

#### SS_COMPACT

首次上传共享存储前，是否对文件组进行 `compact`（仅企业版支持）。`0` 表示首次迁移前不进行 `compact`，`1` 表示首次迁移前进行 `compact`。默认值为 `1`。

#### ENCRYPT_ALGORITHM

指定数据库加密算法。该选项与数据安全能力相关，详见 [数据安全](../../11-security-guide/03-data-security.md)。

#### IS_AUDIT

指定是否创建审计库。审计库有额外的参数约束，详见 [审计与合规](../../11-security-guide/05-audit-and-compliance.md)。

#### ALLOW_DROP

指定是否允许删除数据库。该选项主要用于审计库等安全场景。

#### SECURE_DELETE

指定是否启用安全删除。取值 `0`（默认）或 `1`。

- `0`：删除操作写入删除标记；磁盘上的数据块不会被立即物理覆写。
- `1`：在写入删除标记之外，对落盘 DATA / STT 文件中对应区间的数据块做物理覆写（secure erase），降低通过文件系统直接读取已删数据的风险。

可通过 `CREATE DATABASE` / `ALTER DATABASE` 设置；单次 `DELETE` 也可在语句末尾加 `SECURE_DELETE` 关键字。行为、局限与示例见 [数据安全 · 安全删除](../../11-security-guide/03-data-security.md#安全删除)。

#### SECURITY_LEVEL

指定数据库安全级别（MAC）。语法与规则详见 [权限管理](../07-user-and-privilege/02-grant.md#强制访问控制mac)。

### 创建示例

```sql
CREATE DATABASE IF NOT EXISTS db VGROUPS 10 BUFFER 10;
```

以上示例创建了名为 `db` 的数据库，并设置初始 `vgroup` 数为 `10`，每个 `vnode` 分配 10 MB 写入缓存。

### 使用数据库

```sql
USE db_name;
```

使用或切换当前数据库（在 REST 连接方式下无效）。

## 删除数据库

```sql
DROP DATABASE [IF EXISTS] db_name;
```

删除数据库。指定数据库包含的全部数据表会被删除，该数据库的所有 `vgroup` 也会被销毁，请谨慎使用。

## 修改数据库

```sql
ALTER DATABASE db_name [alter_database_options]

alter_database_options:
    alter_database_option ...

alter_database_option: {
    CACHEMODEL {'none' | 'last_row' | 'last_value' | 'both'}
  | CACHESIZE value
  | CACHESHARDBITS value
  | BUFFER value
  | PAGES value
  | REPLICA value [PARALLEL value]
  | STT_TRIGGER value
  | WAL_LEVEL value
  | WAL_FSYNC_PERIOD value
  | KEEP value
  | WAL_RETENTION_PERIOD value
  | WAL_RETENTION_SIZE value
  | MINROWS value
  | COMPACT_INTERVAL value
  | COMPACT_TIME_RANGE value
  | COMPACT_TIME_OFFSET value
  | SS_KEEPLOCAL value
  | SS_COMPACT value
  | KEEP_TIME_OFFSET value
  | ENCRYPT_ALGORITHM value
  | ALLOW_DROP {0 | 1}
  | SECURE_DELETE {0 | 1}
}
```

### 修改副本数与并发控制

在修改数据库副本数时，可以通过 `PARALLEL` 参数控制副本变更操作的并发度：

```sql
ALTER DATABASE db_name REPLICA value [PARALLEL parallel_value];
```

- `value`：目标副本数，取值为 `1`、`2` 或 `3`。
- `parallel_value`：控制同时执行副本变更的 `vgroup` 数量，默认值为 `0`（无限制）。
  - `0`：无限制并发。所有 `vgroup` 同时执行副本变更。速度最快，但资源消耗较高。
  - `1`：串行执行。一次只有一个 `vgroup` 执行副本变更。速度最慢，但资源消耗最低。
  - `N`（`N > 1`）：有限并发。最多 `N` 个 `vgroup` 同时执行副本变更，在速度和资源消耗之间取得平衡。

:::note
`PARALLEL` 参数仅适用于修改 `REPLICA` 时使用，不能与其他 `ALTER DATABASE` 选项一起使用。
:::

示例：

```sql
-- 修改为 3 副本，无限制并发（默认）
ALTER DATABASE db_name REPLICA 3;

-- 修改为 3 副本，串行执行（一个 vgroup 一个 vgroup 地执行）
ALTER DATABASE db_name REPLICA 3 PARALLEL 1;

-- 修改为 3 副本，有限并发（最多 2 个 vgroup 同时执行）
ALTER DATABASE db_name REPLICA 3 PARALLEL 2;
```

### 修改 CACHESHARDBITS

```sql
ALTER DATABASE db_name CACHESHARDBITS value;
```

- `value` 取值范围为 `[-1, 19]`，`-1` 表示由系统根据 `CACHESIZE` 自动推算分片位数。
- 实际分片数为 `2^value`，如 `value=3` 对应 8 个分片，`value=6` 对应 64 个分片。
- 修改后立即生效，但会导致该数据库所有 `vnode` 的 `last` 缓存（LRU cache）全部失效，已缓存数据将在后续查询时重新从磁盘加载，期间查询延迟可能短暂升高。

### 修改 CACHESIZE

修改数据库参数的命令使用简单，难点在于判断是否需要修改以及如何修改。本小节描述如何判断数据库的 `CACHESIZE` 是否够用。

1. 查看 `CACHESIZE`。

   ```sql
   SELECT * FROM INFORMATION_SCHEMA.INS_DATABASES;
   ```

   可以查看 `CACHESIZE` 的具体值，单位为 MB。

2. 查看 `cacheload`。

   ```sql
   SHOW db_name.VGROUPS;
   ```

   可以查看 `cacheload`，单位为字节。

3. 判断 `CACHESIZE` 是否够用。

- 如果 `cacheload` 非常接近 `CACHESIZE`，则 `CACHESIZE` 可能过小。
- 如果 `cacheload` 明显小于 `CACHESIZE`，则 `CACHESIZE` 通常够用。
- 可以根据系统可用内存情况，决定将 `CACHESIZE` 加倍或提高几倍。

:::note
其他参数暂不支持修改。

:::

## 查看数据库

### 查看系统中的所有数据库

```sql
SHOW DATABASES;
```

### 显示数据库的创建语句

```sql
SHOW CREATE DATABASE db_name \G;
```

常用于数据库迁移。对一个已经存在的数据库，返回其创建语句；在另一个集群中执行该语句，即可得到一个设置完全相同的数据库。

### 查看数据库参数

```sql
SELECT * FROM INFORMATION_SCHEMA.INS_DATABASES WHERE NAME='db_name' \G;
```

会列出指定数据库的配置参数，并且每行只显示一个参数。

## 运维操作

### 删除过期数据

```sql
TRIM DATABASE db_name;
```

删除过期数据，并根据多级存储配置归整数据。

### 手动删除过期 WAL

```sql
TRIM DATABASE db_name WAL;
```

删除过期的 `WAL` 日志。使用 `TRIM DATABASE ... WAL` 删除过期 `WAL` 日志时，会忽略 `vgroup` 的 `keep_version` 限制。

### 落盘内存数据

```sql
FLUSH DATABASE db_name;
```

落盘内存中的数据。在关闭节点之前执行这条命令，可以避免重启后的预写数据日志回放，加速启动过程。

### 调整 VGROUP 中 VNODE 的分布

```sql
REDISTRIBUTE VGROUP vgroup_no DNODE dnode_id1 [DNODE dnode_id2] [DNODE dnode_id3];
```

按照给定的 `dnode` 列表，调整 `vgroup` 中的 `vnode` 分布。因为副本数目最大为 3，所以最多输入 3 个 `dnode`。

### 自动调整 VGROUP 中 VNODE 的分布

```sql
BALANCE VGROUP;
```

自动调整集群中所有 `vgroup` 的 `vnode` 分布，相当于在 `vnode` 层面对整个集群执行数据负载均衡。

### 自动调整 VGROUP 中 LEADER 的分布

```sql
BALANCE VGROUP LEADER;
```

触发集群所有 `vgroup` 中的 `leader` 重新选主，对集群各节点进行负载均衡操作（仅企业版支持）。

### 查看数据库工作状态

```sql
SHOW db_name.ALIVE;
```

查询数据库 `db_name` 的可用状态。返回值说明如下：

- `0`：不可用。
- `1`：完全可用。
- `2`：部分可用，即数据库包含的 `VNODE` 部分节点可用、部分节点不可用。

### 查看数据库的磁盘空间占用

```sql
SELECT * FROM INFORMATION_SCHEMA.INS_DISK_USAGE WHERE db_name = 'db_name';
```

查看数据库各个模块占用的磁盘空间。

```sql
SHOW db_name.DISK_INFO;
```

查看数据库 `db_name` 的数据压缩率和数据在磁盘上占用的大小。

该命令本质上等同于：

```sql
SELECT
    SUM(data1 + data2 + data3) / SUM(raw_data),
    SUM(data1 + data2 + data3)
FROM information_schema.ins_disk_usage
WHERE db_name = 'db_name';
```
