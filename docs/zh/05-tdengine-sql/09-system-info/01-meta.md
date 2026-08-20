---
sidebar_label: 元数据视图
title: 元数据视图
description: INFORMATION_SCHEMA 中系统元数据视图的表结构说明
---

TDengine 内置数据库 `INFORMATION_SCHEMA`，用于访问数据库元数据、系统信息与状态（例如数据库或表名称、当前执行的 SQL 等）。其中包含多张只读表；这些表实际为视图而非基表，没有关联数据文件，因此只能查询，不能执行 `INSERT` 等写入操作。

`INFORMATION_SCHEMA` 以更一致的方式提供与各类 [SHOW 命令](./03-show.md)（如 [`SHOW TABLES`](./03-show.md#show-tables)、[`SHOW DATABASES`](./03-show.md#show-databases)）等价的信息。相较 `SHOW`，使用 `SELECT ... FROM INFORMATION_SCHEMA.tablename` 有以下优点：

1. 可用 `USE` 将 `INFORMATION_SCHEMA` 设为默认数据库
2. 可沿用熟悉的 `SELECT` 语法，只需了解表名与列名
3. 可对结果筛选、排序，并使用 TDengine 支持的任意 `SELECT` 能力
4. 后续可为已有表增加列而不影响既有业务
5. 与其他数据库系统的数据字典查询方式更接近（例如熟悉 Oracle 数据字典的用户）

:::info

- `SHOW` 语句仍保留，便于沿用既有习惯。
- 系统表中部分列名为关键字，查询时需用反引号转义。例如查询数据库 `test` 的 `vgroup` 数量：

```sql
SELECT `vgroups` FROM information_schema.ins_databases WHERE name = 'test';
```

:::

下文说明 `INFORMATION_SCHEMA` 中各表及其列结构。

## INS_ANODES

提供分析节点（anode）的地址、状态与创建/更新时间等信息。也可以使用 [`SHOW ANODES`](./03-show.md#show-anodes) 查询。SYSINFO 为 0 的用户不能查看此表。

| #   | **列名** | **数据类型** | **说明** |
| --- | ------------- | ------------ | --- |
| 1   | `id`          | INT          | ID |
| 2   | `url`         | VARCHAR(128) | URL |
| 3   | `status`      | VARCHAR(10)  | 当前状态 |
| 4   | `create_time` | TIMESTAMP    | 创建时间 |
| 5   | `update_time` | TIMESTAMP    | 更新时间 |

## INS_ANODES_FULL

提供分析节点上已加载算法的类型、名称、状态与备注等明细。也可以使用 [`SHOW ANODES FULL`](./03-show.md#show-anodes) 查询。SYSINFO 为 0 的用户不能查看此表。

| #   | **列名** | **数据类型** | **说明** |
| --- | -------- | ------------ | --- |
| 1   | `id`     | INT          | ID |
| 2   | `type`   | VARCHAR(24)  | 类型 |
| 3   | `algo`   | VARCHAR(64)  | 算法 |
| 4   | `status` | VARCHAR(10)  | 当前状态 |
| 5   | `note`   | VARCHAR(256) | 备注 |

## INS_ARBGROUPS

提供仲裁组中副本 `dnode`、同步状态与指派令牌等信息。也可以使用 [`SHOW ARBGROUPS`](./03-show.md#show-arbgroups) 查询。SYSINFO 为 0 的用户不能查看此表。

| #   | **列名** | **数据类型** | **说明** |
| --- | ----------------- | ----------- | --- |
| 1   | `db_name`         | VARCHAR(64) | 数据库名 |
| 2   | `vgroup_id`       | INT         | `vgroup` ID |
| 3   | `v1_dnode`        | SMALLINT    | v1_dnode |
| 4   | `v2_dnode`        | SMALLINT    | v2_dnode |
| 5   | `is_sync`         | BOOL        | is_sync |
| 6   | `check_sync_code` | VARCHAR(98) | check_sync_code |
| 7   | `assigned_dnode`  | SMALLINT    | assigned_dnode |
| 8   | `assigned_token`  | VARCHAR(32) | assigned_token |
| 9   | `assigned_acked`  | SMALLINT    | assigned_acked |

## INS_BNODES

提供集群中各 `bnode`（桥接节点）的标识、地址、协议与创建时间等信息。也可以使用 [`SHOW BNODES`](./03-show.md#show-bnodes) 查询。SYSINFO 为 0 的用户不能查看此表。

| #   | **列名** | **数据类型** | **说明** |
| --- | ------------- | ------------ | --- |
| 1   | `id`          | INT          | ID |
| 2   | `endpoint`    | VARCHAR(134) | 地址 |
| 3   | `protocol`    | VARCHAR(14)  | 协议 |
| 4   | `create_time` | TIMESTAMP    | 创建时间 |

## INS_CLUSTER

提供当前集群的标识、名称、运行时长、版本与授权到期时间等信息。也可以使用 [`SHOW CLUSTER`](./03-show.md#show-cluster) 查询。SYSINFO 为 0 的用户不能查看此表。

| #   | **列名** | **数据类型** | **说明** |
| --- | ------------- | ----------- | --- |
| 1   | `id`          | BIGINT      | 集群 ID |
| 2   | `name`        | VARCHAR(40) | 集群名称 |
| 3   | `uptime`      | INT         | 运行时长（秒） |
| 4   | `create_time` | TIMESTAMP   | 创建时间 |
| 5   | `version`     | VARCHAR(10) | 版本号 |
| 6   | `expire_time` | TIMESTAMP   | 到期时间 |

## INS_COLUMNS

提供表列的名称、类型、长度、精度以及虚拟表列的数据来源等信息。

| #   | **列名** | **数据类型** | **说明** |
| --- | --------------- | ------------ | --- |
| 1   | `table_name`    | VARCHAR(192) | 表名 |
| 2   | `db_name`       | VARCHAR(64)  | 该表所在的数据库的名称 |
| 3   | `table_type`    | VARCHAR(21)  | 表类型 |
| 4   | `col_name`      | VARCHAR(64)  | 列 的名称 |
| 5   | `col_type`      | VARCHAR(32)  | 列 的类型 |
| 6   | `col_length`    | INT          | 列 的长度 |
| 7   | `col_precision` | INT          | 列 的精度 |
| 8   | `col_scale`     | INT          | 列 的比例 |
| 9   | `col_nullable`  | INT          | 列 是否可以为空 |
| 10  | `col_source`    | VARCHAR(258) | 列 的数据来源。只有虚拟表的列才会有该值，表示虚拟表的数据来源，为 db_name.table_name.col_name |
| 11  | `col_id`        | SMALLINT     | 列 ID |

## INS_COMPACTS

提供数据压缩（compact）任务的标识、目标库与开始时间等信息。也可以使用 [`SHOW COMPACTS`](./03-show.md#show-compacts) 查询。

| #   | **列名** | **数据类型** | **说明** |
| --- | ------------ | ----------- | --- |
| 1   | `compact_id` | INT         | 压缩任务 ID |
| 2   | `db_name`    | VARCHAR(64) | 数据库名 |
| 3   | `start_time` | TIMESTAMP   | 开始时间 |

## INS_COMPACT_DETAILS

提供压缩任务在各 `vgroup`/`dnode` 上的进度、完成情况与剩余时间等明细。也可以使用 [`SHOW COMPACT`](./03-show.md#show-compacts) 查询。

| #   | **列名** | **数据类型** | **说明** |
| --- | ---------------- | --------- | --- |
| 1   | `compact_id`     | INT       | 压缩任务 ID |
| 2   | `vgroup_id`      | INT       | `vgroup` ID |
| 3   | `dnode_id`       | INT       | `dnode` ID |
| 4   | `number_fileset` | INT       | number_fileset |
| 5   | `finished`       | INT       | finished |
| 6   | `start_time`     | TIMESTAMP | 开始时间 |
| 7   | `progress(%)`    | INT       | progress(%) |
| 8   | `remain_time(s)` | BIGINT    | remain_time(s) |

## INS_CONFIGS

提供当前生效的系统配置参数名称与取值。也可以使用 [`SHOW CLUSTER VARIABLES`](./03-show.md#show-cluster-variables) 查询。

| #   | **列名** | **数据类型** | **说明** |
| --- | ------- | ----------- | --- |
| 1   | `name`  | VARCHAR(32) | 配置项名称 |
| 2   | `value` | VARCHAR(64) | 该配置项的值。关键字列，查询时需用反引号转义（如 `` `value` ``）。 |

## INS_CPU_ALLOCATION

提供各 `dnode` 上管理/写入/读取线程的 CPU 核心分配情况。启用 `enableCpuAffinity` 时数据有效。也可以使用 [`SHOW CPU_ALLOCATION`](./03-show.md#show-cpu_allocation) 查询。SYSINFO 为 0 的用户不能查看此表。

| #   | **列名** | **数据类型** | **说明** |
| --- | ----------------- | ------------ | --- |
| 1   | `dnode_id`        | INT          | Dnode 标识符 |
| 2   | `thread_category` | VARCHAR(16)  | 线程类别：`management`（管理）、`write`（写入）或 `read`（读取） |
| 3   | `cores`           | INT          | 分配给该类别的 CPU 核心数量（禁用时为 0） |
| 4   | `core_ids`        | VARCHAR(256) | 已分配的核心 ID 列表（逗号分隔），禁用时为 `"-"` |
| 5   | `enabled`         | BOOL         | 该类别是否启用了 CPU 亲和性 |

## INS_DATABASES

提供用户数据库的配置与状态信息，例如副本数、保留策略、缓存与 WAL 相关参数。也可以使用 [`SHOW DATABASES`](./03-show.md#show-databases) 查询。

| #   | **列名** | **数据类型** | **说明** |
| --- | ---------------------- | ----------- | --- |
| 1   | `name`                 | VARCHAR(64) | 数据库名 |
| 2   | `create_time`          | TIMESTAMP   | 创建时间 |
| 3   | `vgroups`              | INT         | 数据库中有多少个 vgroup。关键字列，查询时需用反引号转义（如 `` `vgroups` ``）。 |
| 4   | `ntables`              | BIGINT      | 数据库中表的数量，包含子表和普通表但不包含超级表 |
| 5   | `replica`              | TINYINT     | 副本数。关键字列，查询时需用反引号转义（如 `` `replica` ``）。 |
| 6   | `strict`               | VARCHAR(4)  | 废弃参数 |
| 7   | `duration`             | VARCHAR(10) | 单文件存储数据的时间跨度。关键字列，查询时需用反引号转义（如 `` `duration` ``）。内部存储单位为分钟，查询时有可能转换为天或小时展示 |
| 8   | `keep`                 | VARCHAR(32) | 数据保留时长。关键字列，查询时需用反引号转义（如 `` `keep` ``）。内部存储单位为分钟，查询时有可能转换为天或小时展示 |
| 9   | `buffer`               | INT         | 每个 vnode 写缓存的内存块大小，单位 MB。关键字列，查询时需用反引号转义（如 `` `buffer` ``）。 |
| 10  | `pagesize`             | INT         | 每个 VNODE 中元数据存储引擎的页大小，单位为 KB。关键字列，查询时需用反引号转义（如 `` `pagesize` ``）。 |
| 11  | `pages`                | INT         | 每个 vnode 元数据存储引擎的缓存页个数。关键字列，查询时需用反引号转义（如 `` `pages` ``）。 |
| 12  | `minrows`              | INT         | 文件块中记录的最小条数。关键字列，查询时需用反引号转义（如 `` `minrows` ``）。 |
| 13  | `maxrows`              | INT         | 文件块中记录的最大条数。关键字列，查询时需用反引号转义（如 `` `maxrows` ``）。 |
| 14  | `comp`                 | TINYINT     | 数据压缩方式。关键字列，查询时需用反引号转义（如 `` `comp` ``）。 |
| 15  | `precision`            | VARCHAR(2)  | 时间分辨率。关键字列，查询时需用反引号转义（如 `` `precision` ``）。 |
| 16  | `status`               | VARCHAR(10) | 数据库状态 |
| 17  | `retentions`           | VARCHAR(60) | 数据的聚合周期和保存时长。关键字列，查询时需用反引号转义（如 `` `retentions` ``）。 |
| 18  | `single_stable`        | BOOL        | 表示此数据库中是否只可以创建一个超级表。关键字列，查询时需用反引号转义（如 `` `single_stable` ``）。 |
| 19  | `cachemodel`           | VARCHAR(11) | 表示是否在内存中缓存子表的最近数据。关键字列，查询时需用反引号转义（如 `` `cachemodel` ``）。 |
| 20  | `cachesize`            | INT         | 表示每个 vnode 中用于缓存子表最近数据的内存大小。关键字列，查询时需用反引号转义（如 `` `cachesize` ``）。 |
| 21  | `cacheshardbits`       | INT         | last 缓存（LRU cache）的分片位数。实际分片数为 `2^cacheshardbits`，-1 表示由系统根据 cachesize 自动推算。关键字列，查询时需用反引号转义（如 `` `cacheshardbits` ``）。 |
| 22  | `wal_level`            | TINYINT     | WAL 级别。关键字列，查询时需用反引号转义（如 `` `wal_level` ``）。 |
| 23  | `wal_fsync_period`     | INT         | 数据落盘周期。关键字列，查询时需用反引号转义（如 `` `wal_fsync_period` ``）。 |
| 24  | `wal_retention_period` | INT         | WAL 的保存时长，单位为秒。关键字列，查询时需用反引号转义（如 `` `wal_retention_period` ``）。 |
| 25  | `wal_retention_size`   | BIGINT      | WAL 的保存上限。关键字列，查询时需用反引号转义（如 `` `wal_retention_size` ``）。 |
| 26  | `stt_trigger`          | SMALLINT    | 触发文件合并的落盘文件的个数。关键字列，查询时需用反引号转义（如 `` `stt_trigger` ``）。 |
| 27  | `table_prefix`         | SMALLINT    | 内部存储引擎根据表名分配存储该表数据的 VNODE 时要忽略的前缀的长度。关键字列，查询时需用反引号转义（如 `` `table_prefix` ``）。 |
| 28  | `table_suffix`         | SMALLINT    | 内部存储引擎根据表名分配存储该表数据的 VNODE 时要忽略的后缀的长度。关键字列，查询时需用反引号转义（如 `` `table_suffix` ``）。 |
| 29  | `tsdb_pagesize`        | INT         | 时序数据存储引擎中的页大小。关键字列，查询时需用反引号转义（如 `` `tsdb_pagesize` ``）。 |
| 30  | `keep_time_offset`     | INT         | `KEEP` 时间偏移 |
| 31  | `ss_chunkpages`        | INT         | 共享存储 chunk 页数 |
| 32  | `ss_keeplocal`         | VARCHAR(10) | 共享存储本地保留策略 |
| 33  | `ss_compact`           | TINYINT     | 共享存储压缩相关配置 |
| 34  | `with_arbitrator`      | TINYINT     | 是否启用仲裁 |
| 35  | `encrypt_algorithm`    | VARCHAR(16) | 加密算法 |
| 36  | `compact_interval`     | VARCHAR(12) | 自动压缩间隔 |
| 37  | `compact_time_range`   | VARCHAR(24) | 自动压缩时间范围 |
| 38  | `compact_time_offset`  | VARCHAR(4)  | 自动压缩时间偏移 |
| 39  | `is_audit`             | BOOL        | 是否为审计库 |
| 40  | `owner`                | VARCHAR(24) | 所有者 |
| 41  | `allow_drop`           | BOOL        | 是否允许删除 |
| 42  | `sec_level`            | TINYINT     | 安全等级 |

## INS_DISK_USAGE

按数据库与 `vgroup` 汇总 WAL、多级存储、缓存与元数据等磁盘占用（单位 KB）。也可以使用 [`SHOW DISK_INFO`](./03-show.md#show-disk_info) 查询。

| #   | **列名** | **数据类型** | **说明** |
| --- | ------------ | ----------- | --- |
| 1   | `db_name`    | VARCHAR(32) | 数据库名称 |
| 2   | `vgroup_id`  | INT         | vgroup 的 ID |
| 3   | `wal_size`   | BIGINT      | wal 文件大小，单位为 KB |
| 4   | `data1`      | BIGINT      | 一级存储上数据文件的大小，单位为 KB |
| 5   | `data2`      | BIGINT      | 二级存储上数据文件的大小，单位为 KB |
| 6   | `data3`      | BIGINT      | 三级存储上数据文件的大小，单位为 KB |
| 7   | `cache_rdb`  | BIGINT      | last/last_row 文件的大小，单位为 KB |
| 8   | `table_meta` | BIGINT      | meta 文件的大小，单位为 KB |
| 9   | `ss`         | BIGINT      | 共享存储上占用的大小，单位为 KB |
| 10  | `raw_data`   | BIGINT      | 预估的原始数据的大小，单位为 KB |

## INS_DNODES

提供集群中各 `dnode`（数据节点）的标识、地址、`vnode` 容量与运行状态等信息。也可以使用 [`SHOW DNODES`](./03-show.md#show-dnodes) 查询。SYSINFO 为 0 的用户不能查看此表。

| #   | **列名** | **数据类型** | **说明** |
| --- | ---------------- | ------------ | --- |
| 1   | `id`             | INT          | `dnode` ID |
| 2   | `endpoint`       | VARCHAR(134) | `dnode` 地址 |
| 3   | `vnodes`         | SMALLINT     | 实际 `vnode` 个数。关键字列，查询时需用反引号转义（如 `` `vnodes` ``） |
| 4   | `support_vnodes` | SMALLINT     | 最多支持的 `vnode` 个数 |
| 5   | `status`         | VARCHAR(10)  | 当前状态 |
| 6   | `create_time`    | TIMESTAMP    | 创建时间 |
| 7   | `reboot_time`    | TIMESTAMP    | 最近重启时间 |
| 8   | `note`           | VARCHAR(256) | 离线原因等信息 |
| 9   | `machine_id`     | VARCHAR(24)  | 机器标识（企业版） |

## INS_DNODE_VARIABLES

提供各 `dnode` 上的配置参数及其作用域、类别与说明。也可以使用 [`SHOW DNODE VARIABLES`](./03-show.md#show-cluster-variables) 查询。SYSINFO 为 0 的用户不能查看此表。

| #   | **列名** | **数据类型** | **说明** |
| --- | ---------- | ------------- | --- |
| 1   | `dnode_id` | INT           | dnode 的 ID |
| 2   | `name`     | VARCHAR(32)   | 配置项名称 |
| 3   | `value`    | VARCHAR(4096) | 该配置项的值。关键字列，查询时需用反引号转义（如 `` `value` ``）。 |
| 4   | `scope`    | VARCHAR(8)    | 配置作用域 |
| 5   | `category` | VARCHAR(8)    | 配置类别 |
| 6   | `info`     | VARCHAR(64)   | 配置说明 |

## INS_ENCRYPTIONS

提供各 `dnode` 的加密密钥状态。也可以使用 [`SHOW ENCRYPTIONS`](./03-show.md#show-encryptions) 查询。SYSINFO 为 0 的用户不能查看此表。

| #   | **列名** | **数据类型** | **说明** |
| --- | ------------ | ----------- | --- |
| 1   | `dnode_id`   | INT         | `dnode` ID |
| 2   | `key_status` | VARCHAR(12) | key_status |

## INS_ENCRYPT_ALGORITHMS

提供系统可用的加密算法名称、类型、来源与描述等信息。也可以使用 [`SHOW ENCRYPT_ALGORITHMS`](./03-show.md#show-encrypt_algorithms) 查询。

| #   | **列名** | **数据类型** | **说明** |
| --- | ---------------- | ------------ | --- |
| 1   | `id`             | INT          | ID |
| 2   | `algorithm_id`   | VARCHAR(64)  | algorithm_id |
| 3   | `name`           | VARCHAR(64)  | 名称 |
| 4   | `desc`           | VARCHAR(128) | 描述 |
| 5   | `type`           | VARCHAR(198) | 类型 |
| 6   | `source`         | VARCHAR(198) | source |
| 7   | `ossl_algr_name` | VARCHAR(64)  | ossl_algr_name |

## INS_ENCRYPT_STATUS

提供当前加密范围、所用算法与加密状态等信息。也可以使用 [`SHOW ENCRYPT_STATUS`](./03-show.md#show-encrypt_status) 查询。

| #   | **列名** | **数据类型** | **说明** |
| --- | --------------- | ----------- | --- |
| 1   | `encrypt_scope` | VARCHAR(32) | encrypt_scope |
| 2   | `algorithm`     | VARCHAR(32) | algorithm |
| 3   | `status`        | VARCHAR(16) | 当前状态 |

## INS_EXT_SOURCES

提供联邦查询外部数据源的连接信息、库/schema 与选项等。也可以使用 [`SHOW EXTERNAL SOURCES`](./03-show.md#show-external-sources) 查询。

| #   | **列名** | **数据类型** | **说明** |
| --- | ------------- | ------------- | --- |
| 1   | `source_name` | VARCHAR(64)   | source_name |
| 2   | `type`        | VARCHAR(16)   | 类型 |
| 3   | `host`        | VARCHAR(256)  | 主机 |
| 4   | `port`        | INT           | 端口 |
| 5   | `user`        | VARCHAR(128)  | 用户名 |
| 6   | `password`    | VARCHAR(8)    | 密码 |
| 7   | `database`    | VARCHAR(64)       | 数据库 |
| 8   | `schema`      | VARCHAR(64)       | schema |
| 9   | `options`     | VARCHAR(8191) | 选项 |
| 10  | `create_time` | TIMESTAMP     | 创建时间 |

## INS_FILESETS

提供数据 fileset 的时间覆盖范围、大小、最近压缩时间及是否需再压缩等信息。

| #   | **列名** | **数据类型** | **说明** |
| --- | ---------------- | ------------ | --- |
| 1   | `db_name`        | VARCHAR(64)  | 数据库名 |
| 2   | `vgroup_id`      | INT          | vgroup id |
| 3   | `fileset_id`     | INT          | 文件组 id |
| 4   | `start_time`     | TIMESTAMP    | 文件组的覆盖数据的开始时间 |
| 5   | `end_time`       | TIMESTAMP    | 文件组的覆盖数据的结束时间 |
| 6   | `total_size`     | BIGINT       | 文件组的总大小 |
| 7   | `last_compact`   | TIMESTAMP    | 最后一次压缩的时间 |
| 8   | `should_compact` | BOOL         | 是否需要压缩，true：需要，false：不需要 |
| 9   | `details`        | VARCHAR(256) | 明细信息 |

## INS_FUNCTIONS

提供用户自定义函数（UDF）的名称、类型、语言、函数体与版本等信息。也可以使用 [`SHOW FUNCTIONS`](./03-show.md#show-functions) 查询。

| #   | **列名** | **数据类型** | **说明** |
| --- | --------------- | -------------- | --- |
| 1   | `name`          | VARCHAR(64)    | 函数名 |
| 2   | `comment`       | VARCHAR(4095)  | 补充说明。关键字列，查询时需用反引号转义（如 `` `comment` ``）。 |
| 3   | `aggregate`     | INT            | 是否为聚合函数。关键字列，查询时需用反引号转义（如 `` `aggregate` ``）。 |
| 4   | `output_type`   | VARCHAR(31)    | 输出类型 |
| 5   | `create_time`   | TIMESTAMP      | 创建时间 |
| 6   | `code_len`      | INT            | 代码长度 |
| 7   | `bufsize`       | INT            | buffer 大小 |
| 8   | `func_language` | VARCHAR(31)    | 自定义函数编程语言 |
| 9   | `func_body`     | VARCHAR(65517) | 函数体定义 |
| 10  | `func_version`  | INT            | 函数版本号。初始版本为 0，每次替换更新，版本号加 1。 |

## INS_GRANTS

提供企业版授权概况，例如授权状态、测点、`dnode`/`vnode` 配额与存储上限等。也可以使用 [`SHOW LICENCES`](./03-show.md#show-licences) 查询。SYSINFO 为 0 的用户不能查看此表。

| #   | **列名** | **数据类型** | **说明** |
| --- | -------------- | ----------- | --- |
| 1   | `version`      | VARCHAR(64) | 企业版授权版本说明 |
| 2   | `expire_time`  | VARCHAR(19) | 到期时间 |
| 3   | `service_time` | VARCHAR(19) | 服务时间 |
| 4   | `expired`      | VARCHAR(5)  | 是否到期 |
| 5   | `state`        | VARCHAR(9)  | 授权状态 |
| 6   | `timeseries`   | VARCHAR(43) | 授权测点数量 |
| 7   | `dnodes`       | VARCHAR(21) | 授权 `dnode` 数量。关键字列，查询时需用反引号转义（如 `` `dnodes` ``） |
| 8   | `cpu_cores`    | VARCHAR(21) | 授权 CPU 核心数量 |
| 9   | `vnodes`       | VARCHAR(21) | 授权 `vnode` 数量。关键字列，查询时需用反引号转义（如 `` `vnodes` ``） |
| 10  | `storage_size` | VARCHAR(43) | 授权存储空间大小 |

## INS_GRANTS_FULL

提供企业版各授权项的显示名、到期时间与限额等明细。也可以使用 [`SHOW GRANTS FULL`](./03-show.md#show-licences) 查询。SYSINFO 为 0 的用户不能查看此表。

| #   | **列名** | **数据类型** | **说明** |
| --- | -------------- | ------------ | --- |
| 1   | `grant_name`   | VARCHAR(32)  | 授权项名称 |
| 2   | `display_name` | VARCHAR(256) | 显示名称 |
| 3   | `expire`       | VARCHAR(32)  | 过期时间（秒） |
| 4   | `limits`       | VARCHAR(512) | limits |

## INS_GRANTS_LOGS

提供授权激活、吊销及机器绑定等相关日志信息。也可以使用 [`SHOW GRANTS LOGS`](./03-show.md#show-licences) 查询。SYSINFO 为 0 的用户不能查看此表。

| #   | **列名** | **数据类型** | **说明** |
| --- | ------------- | -------------- | --- |
| 1   | `state`       | VARCHAR(1536)  | state |
| 2   | `active`      | VARCHAR(512)   | active |
| 3   | `machine`     | VARCHAR(15600) | machine |
| 4   | `active_info` | VARCHAR(512)   | active_info |
| 5   | `revoke_info` | VARCHAR(30)    | revoke_info |

## INS_INDEXES

提供已创建索引的名称、所属库表、列名与索引类型等信息。也可以使用 [`SHOW INDEXES`](./03-show.md#show-indexes) 查询。

| #   | **列名** | **数据类型** | **说明** |
| --- | ------------- | ------------ | --- |
| 1   | `index_name`  | VARCHAR(192) | 索引名 |
| 2   | `db_name`     | VARCHAR(64)  | 索引所属表所在数据库 |
| 3   | `table_name`  | VARCHAR(192) | 索引所属表名 |
| 4   | `vgroup_id`   | INT          | `vgroup` ID |
| 5   | `create_time` | TIMESTAMP    | 创建时间 |
| 6   | `column_name` | VARCHAR(192) | 建索引的列名 |
| 7   | `index_type`  | VARCHAR(192) | 索引类型（如 SMA、tag） |

## INS_MACHINES

提供授权绑定的机器标识、`dnode` 数量与版本等信息。也可以使用 [`SHOW CLUSTER MACHINES`](./03-show.md#show-cluster-machines) 查询。SYSINFO 为 0 的用户不能查看此表。

| #   | **列名** | **数据类型** | **说明** |
| --- | ----------- | ------------- | --- |
| 1   | `id`        | VARCHAR(41)   | ID |
| 2   | `dnode_num` | INT           | dnode_num |
| 3   | `machine`   | VARCHAR(7552) | machine |
| 4   | `version`   | VARCHAR(32)   | version |

## INS_MNODES

提供集群中各 `mnode`（管理节点）的标识、地址、角色与状态等信息。也可以使用 [`SHOW MNODES`](./03-show.md#show-mnodes) 查询。SYSINFO 为 0 的用户不能查看此表。

| #   | **列名** | **数据类型** | **说明** |
| --- | ------------- | ------------ | --- |
| 1   | `id`          | INT          | mnode id |
| 2   | `endpoint`    | VARCHAR(134) | mnode 的地址 |
| 3   | `role`        | VARCHAR(12)  | 当前角色 |
| 4   | `status`      | VARCHAR(9)   | 当前状态 |
| 5   | `create_time` | TIMESTAMP    | 创建时间 |
| 6   | `role_time`   | TIMESTAMP    | 成为当前角色的时间 |

## INS_MOUNTS

提供数据库挂载（mount）的名称、所在 `dnode`、路径与创建时间等信息。也可以使用 [`SHOW MOUNTS`](./03-show.md#show-mounts) 查询。SYSINFO 为 0 的用户不能查看此表。

| #   | **列名** | **数据类型** | **说明** |
| --- | ------------- | ------------ | --- |
| 1   | `name`        | VARCHAR(77)  | 名称 |
| 2   | `dnode`       | INT          | dnode |
| 3   | `create_time` | TIMESTAMP    | 创建时间 |
| 4   | `path`        | VARCHAR(128) | 路径 |

## INS_QNODES

提供集群中各 `qnode`（查询节点）的标识、地址与创建时间等信息。也可以使用 [`SHOW QNODES`](./03-show.md#show-qnodes) 查询。SYSINFO 为 0 的用户不能查看此表。

| #   | **列名** | **数据类型** | **说明** |
| --- | ------------- | ------------ | --- |
| 1   | `id`          | INT          | qnode id |
| 2   | `endpoint`    | VARCHAR(134) | qnode 的地址 |
| 3   | `create_time` | TIMESTAMP    | 创建时间 |

## INS_RETENTIONS

提供数据保留（retention）任务的标识、目标库、触发模式与类型等信息。也可以使用 [`SHOW RETENTIONS`](./03-show.md#show-retentions) 查询。

| #   | **列名** | **数据类型** | **说明** |
| --- | -------------- | ----------- | --- |
| 1   | `retention_id` | INT         | 保留任务 ID |
| 2   | `db_name`      | VARCHAR(64) | 数据库名 |
| 3   | `start_time`   | TIMESTAMP   | 开始时间 |
| 4   | `trigger_mode` | VARCHAR(10) | trigger_mode |
| 5   | `type`         | VARCHAR(10) | 类型 |

## INS_RETENTION_DETAILS

提供保留任务在各 `vgroup`/`dnode` 上的进度与剩余时间等明细。也可以使用 [`SHOW RETENTION`](./03-show.md#show-retentions) 查询。

| #   | **列名** | **数据类型** | **说明** |
| --- | ---------------- | --------- | --- |
| 1   | `retention_id`   | INT       | 保留任务 ID |
| 2   | `vgroup_id`      | INT       | `vgroup` ID |
| 3   | `dnode_id`       | INT       | `dnode` ID |
| 4   | `number_fileset` | INT       | number_fileset |
| 5   | `finished`       | INT       | finished |
| 6   | `start_time`     | TIMESTAMP | 开始时间 |
| 7   | `progress(%)`    | INT       | progress(%) |
| 8   | `remain_time(s)` | BIGINT    | remain_time(s) |

## INS_ROLES

提供角色的名称、启用状态、类型与子角色等信息。也可以使用 [`SHOW ROLES`](./03-show.md#show-roles) 查询。SYSINFO 为 0 的用户不能查看此表。

| #   | **列名** | **数据类型** | **说明** |
| --- | ------------- | ------------- | --- |
| 1   | `name`        | VARCHAR(64)   | 名称 |
| 2   | `enable`      | TINYINT       | 是否启用 |
| 3   | `create_time` | TIMESTAMP     | 创建时间 |
| 4   | `update_time` | TIMESTAMP     | 更新时间 |
| 5   | `role_type`   | VARCHAR(7)    | role_type |
| 6   | `subroles`    | VARCHAR(2048) | subroles |

## INS_ROLE_COLUMN_PRIVILEGES

提供角色在列级上的权限明细。也可以使用 [`SHOW ROLE COLUMN PRIVILEGES`](./03-show.md#show-role-column-privileges) 查询。SYSINFO 为 0 的用户不能查看此表。

| #   | **列名** | **数据类型** | **说明** |
| --- | ------------- | ------------ | --- |
| 1   | `role_name`   | VARCHAR(24)  | role_name |
| 2   | `priv_type`   | VARCHAR(128) | priv_type |
| 3   | `priv_scope`  | VARCHAR(32)  | priv_scope |
| 4   | `db_name`     | VARCHAR(65)  | 数据库名 |
| 5   | `table_name`  | VARCHAR(193) | 表名 |
| 6   | `column_name` | VARCHAR(65)  | column_name |
| 7   | `condition`   | VARCHAR(48)  | condition |
| 8   | `update_time` | VARCHAR(40)  | 更新时间 |
| 9   | `notes`       | VARCHAR(64)  | notes |

## INS_ROLE_PRIVILEGES

提供角色权限明细，包括权限类型、作用域、库表范围与条件等。也可以使用 [`SHOW ROLE PRIVILEGES`](./03-show.md#show-role-privileges) 查询。SYSINFO 为 0 的用户不能查看此表。

| #   | **列名** | **数据类型** | **说明** |
| --- | ------------- | ------------ | --- |
| 1   | `role_name`   | VARCHAR(24)  | role_name |
| 2   | `priv_type`   | VARCHAR(128) | priv_type |
| 3   | `priv_scope`  | VARCHAR(32)  | priv_scope |
| 4   | `db_name`     | VARCHAR(65)  | 数据库名 |
| 5   | `table_name`  | VARCHAR(193) | 表名 |
| 6   | `condition`   | VARCHAR(48)  | condition |
| 7   | `notes`       | VARCHAR(64)  | notes |
| 8   | `columns`     | VARCHAR(12)  | columns |
| 9   | `update_time` | VARCHAR(40)  | 更新时间 |

## INS_RSMAS

提供 RSMA 定义的名称、源表、聚合周期与函数列表等信息。也可以使用 [`SHOW RSMAS`](./03-show.md#show-rsmas) 查询。

| #   | **列名** | **数据类型** | **说明** |
| --- | ------------- | ------------- | --- |
| 1   | `rsma_name`   | VARCHAR(192)  | rsma_name |
| 2   | `rsma_id`     | BIGINT        | rsma_id |
| 3   | `db_name`     | VARCHAR(64)   | 数据库名 |
| 4   | `table_name`  | VARCHAR(192)  | 表名 |
| 5   | `table_type`  | VARCHAR(21)   | table_type |
| 6   | `create_time` | TIMESTAMP     | 创建时间 |
| 7   | `interval`    | VARCHAR(64)   | interval |
| 8   | `func_list`   | VARCHAR(2048) | func_list |

## INS_SCANS

提供数据扫描任务的标识、目标库与开始时间等信息。也可以使用 [`SHOW SCANS`](./03-show.md#show-scans) 查询。

| #   | **列名** | **数据类型** | **说明** |
| --- | ------------ | ----------- | --- |
| 1   | `scan_id`    | INT         | 扫描任务 ID |
| 2   | `db_name`    | VARCHAR(64) | 数据库名 |
| 3   | `start_time` | TIMESTAMP   | 开始时间 |

## INS_SCAN_DETAILS

提供扫描任务在各 `vgroup`/`dnode` 上的进度与剩余时间等明细。也可以使用 [`SHOW SCAN`](./03-show.md#show-scans) 查询。

| #   | **列名** | **数据类型** | **说明** |
| --- | ---------------- | --------- | --- |
| 1   | `scan_id`        | INT       | 扫描任务 ID |
| 2   | `vgroup_id`      | INT       | `vgroup` ID |
| 3   | `dnode_id`       | INT       | `dnode` ID |
| 4   | `number_fileset` | INT       | number_fileset |
| 5   | `finished`       | INT       | finished |
| 6   | `start_time`     | TIMESTAMP | 开始时间 |
| 7   | `progress(%)`    | INT       | progress(%) |
| 8   | `remain_time(s)` | BIGINT    | remain_time(s) |

## INS_SECURITY_POLICIES

提供安全策略的名称、模式、操作者与最近更新时间等信息。也可以使用 [`SHOW SECURITY_POLICIES`](./03-show.md#show-security_policies) 查询。SYSINFO 为 0 的用户不能查看此表。

| #   | **列名** | **数据类型** | **说明** |
| --- | ------------- | ------------ | --- |
| 1   | `name`        | VARCHAR(3)   | 名称 |
| 2   | `mode`        | VARCHAR(30)  | 模式 |
| 3   | `operator`    | VARCHAR(24)  | 操作者 |
| 4   | `last_update` | TIMESTAMP    | 最近更新时间 |
| 5   | `desc`        | VARCHAR(128) | 描述 |

## INS_SNAP_SEND_FILESETS

提供快照复制场景下按 fileset 统计的读写量、进度与传输类型等明细。

| #   | **列名** | **数据类型** | **说明** |
| --- | --------------------- | ----------- | --- |
| 1   | `vgroup_id`           | INT         | `vgroup` ID |
| 2   | `fid`                 | INT         | fid |
| 3   | `file_count`          | INT         | file_count |
| 4   | `finished_file_count` | INT         | finished_file_count |
| 5   | `total_size`          | BIGINT      | total_size |
| 6   | `read_size`           | BIGINT      | read_size |
| 7   | `start_time`          | TIMESTAMP   | 开始时间 |
| 8   | `elapsed`             | VARCHAR(32) | 耗时 |
| 9   | `start_index`         | BIGINT      | start_index |
| 10  | `end_index`           | BIGINT      | end_index |
| 11  | `transfer_type`       | VARCHAR(4)  | transfer_type |

## INS_SNAP_SEND_VNODES

提供快照复制场景下按 `vnode` 统计的 fileset 传输进度与耗时。

| #   | **列名** | **数据类型** | **说明** |
| --- | -------------------- | ----------- | --- |
| 1   | `vgroup_id`          | INT         | `vgroup` ID |
| 2   | `dnode_id`           | INT         | `dnode` ID |
| 3   | `total_file_sets`    | INT         | total_file_sets |
| 4   | `finished_file_sets` | INT         | finished_file_sets |
| 5   | `start_time`         | TIMESTAMP   | 开始时间 |
| 6   | `elapsed`            | VARCHAR(32) | 耗时 |

## INS_SNODES

提供集群中各 `snode`（流计算节点）的标识、地址与副本关系等信息。也可以使用 [`SHOW SNODES`](./03-show.md#show-snodes) 查询。SYSINFO 为 0 的用户不能查看此表。

| #   | **列名** | **数据类型** | **说明** |
| --- | ------------- | ------------ | --- |
| 1   | `id`          | INT          | `snode` ID |
| 2   | `endpoint`    | VARCHAR(134) | `snode` 地址 |
| 3   | `create_time` | TIMESTAMP    | 创建时间 |
| 4   | `replicaId`   | INT          | 副本 ID |
| 5   | `asReplicaOf` | VARCHAR(64)  | 作为哪个 `snode` 的副本 |

## INS_SSMIGRATES

提供共享存储迁移任务的进度，包括已迁移 `vgroup`/fileset 等信息。也可以使用 [`SHOW SSMIGRATES`](./03-show.md#show-ssmigrates) 查询。

| #   | **列名** | **数据类型** | **说明** |
| --- | ------------------ | ----------- | --- |
| 1   | `ssmigrate_id`     | INT         | 共享存储迁移任务 ID |
| 2   | `db_name`          | VARCHAR(64) | 数据库名 |
| 3   | `start_time`       | TIMESTAMP   | 开始时间 |
| 4   | `number_vgroup`    | INT         | number_vgroup |
| 5   | `migrated_vgroup`  | INT         | migrated_vgroup |
| 6   | `vgroup_id`        | INT         | `vgroup` ID |
| 7   | `number_fileset`   | INT         | number_fileset |
| 8   | `migrated_fileset` | INT         | migrated_fileset |
| 9   | `fileset_id`       | INT         | fileset_id |

## INS_STABLES

提供超级表的结构与属性信息，例如列/标签数量、注释、所有者与安全等级。也可以使用 [`SHOW STABLES`](./03-show.md#show-stables) 查询。

| #   | **列名** | **数据类型** | **说明** |
| --- | --------------- | ------------- | --- |
| 1   | `stable_name`   | VARCHAR(192)  | 超级表表名 |
| 2   | `db_name`       | VARCHAR(64)   | 超级表所在的数据库的名称 |
| 3   | `create_time`   | TIMESTAMP     | 创建时间 |
| 4   | `columns`       | INT           | 列数目 |
| 5   | `tags`          | INT           | 标签数目。关键字列，查询时需用反引号转义（如 `` `tags` ``）。 |
| 6   | `last_update`   | TIMESTAMP     | 最后更新时间 |
| 7   | `table_comment` | VARCHAR(1024) | 表注释 |
| 8   | `watermark`     | VARCHAR(64)   | 窗口的关闭时间。关键字列，查询时需用反引号转义（如 `` `watermark` ``）。 |
| 9   | `max_delay`     | VARCHAR(64)   | 推送计算结果的最大延迟。关键字列，查询时需用反引号转义（如 `` `max_delay` ``）。 |
| 10  | `rollup`        | VARCHAR(128)  | rollup 聚合函数。关键字列，查询时需用反引号转义（如 `` `rollup` ``）。 |
| 11  | `uid`           | BIGINT        | 超级表 UID |
| 12  | `isvirtual`     | BOOL          | 是否为虚拟超级表 |
| 13  | `keep`          | BIGINT        | 数据保留时长 |
| 14  | `owner`         | VARCHAR(24)   | 所有者 |
| 15  | `sec_level`     | TINYINT       | 安全等级 |

## INS_STREAMS

提供流式计算任务的名称、所属库、状态、`snode` 分布与错误信息等。也可以使用 [`SHOW STREAMS`](./03-show.md#show-streams) 查询。

| #   | **列名** | **数据类型** | **说明** |
| --- | -------------- | -------------- | --- |
| 1   | `stream_name`  | VARCHAR(192)   | 流名称 |
| 2   | `db_name`      | VARCHAR(64)    | 流所属数据库 |
| 3   | `create_time`  | TIMESTAMP      | 创建时间 |
| 4   | `stream_id`    | VARCHAR(19)    | 流 ID |
| 5   | `sql`          | VARCHAR(49152) | 创建流时的 SQL |
| 6   | `status`       | VARCHAR(20)    | 当前状态 |
| 7   | `snodeLeader`  | INT            | Leader `snode` |
| 8   | `snodeReplica` | INT            | 副本 `snode` |
| 9   | `message`      | VARCHAR(256)   | 状态或错误信息 |
| 10  | `external_sources` | INT        | 流引用的外部数据源数量 |
| 11  | `realtime_lag_ms` | BIGINT       | 最慢有效入口 Reader 的实时处理延迟，单位为毫秒 |
| 12  | `input_rows_per_sec_1m` | DOUBLE | 最近一个完整 60 秒窗口内，流接纳的逻辑输入速率，单位为行/秒 |
| 13  | `output_rows_per_sec_1m` | DOUBLE | 最近一个完整 60 秒窗口内，成功交付的最终结果速率，单位为行/秒 |
| 14  | `runner_result_latency_avg_1m_ms` | DOUBLE | 最近一个完整 60 秒窗口内，Runner 形成结果的平均延迟，单位为毫秒 |
| 15  | `history_progress_pct` | INT      | 历史数据计算进度，取值为 0 到 100 |

一分钟指标在任务尚未形成首个完整 60 秒窗口时为 `NULL`。引用外部数据源的流没有 WAL 实时进度，因此 `realtime_lag_ms` 为 `NULL`。未启用历史数据计算时，`history_progress_pct` 为 `NULL`。详细解释和诊断方法见[可观测性与故障诊断](../../07-stream-processing/04-observability.md)。

## INS_STREAM_RECALCULATES

提供流式计算重算任务的时间范围、进度与标识等信息。

| #   | **列名** | **数据类型** | **说明** |
| --- | ------------- | ------------ | --- |
| 1   | `stream_name` | VARCHAR(192) | 流名称 |
| 2   | `stream_id`   | VARCHAR(19)  | 流 ID |
| 3   | `recalc_id`   | VARCHAR(19)  | 重算 ID |
| 4   | `start`       | TIMESTAMP    | 重算时间范围的开始时间 |
| 5   | `end`         | TIMESTAMP    | 重算时间范围的结束时间 |
| 6   | `progress`    | VARCHAR(20)  | 重算进度百分比，例如 `42%` |
| 7   | `status`      | VARCHAR(16)  | 重算状态：`Pending`、`Running`、`Finished` 或 `Failed` |

已结束的重算记录从 mnode 首次观察到终态开始保留 1 小时，每个流最多保留 100 条。`Pending` 和 `Running` 记录不受该数量上限影响。记录仅保存在内存中，不保证跨进程重启保留。

## INS_STREAM_TASKS

提供流式计算内部任务的部署位置、类型、状态与最近更新时间等信息。

| #   | **列名** | **数据类型** | **说明** |
| --- | ------------- | ------------ | --- |
| 1   | `stream_name` | VARCHAR(192) | 流名称 |
| 2   | `stream_id`   | VARCHAR(19)  | 流 ID |
| 3   | `task_id`     | VARCHAR(19)  | 任务 ID |
| 4   | `type`        | VARCHAR(20)  | 类型 |
| 5   | `serious_id`  | VARCHAR(19)  | serious_id |
| 6   | `deploy_id`   | INT          | deploy_id |
| 7   | `node_type`   | VARCHAR(10)  | node_type |
| 8   | `node_id`     | INT          | node_id |
| 9   | `task_idx`    | INT          | task_idx |
| 10  | `status`      | VARCHAR(20)  | 当前状态 |
| 11  | `start_time`  | TIMESTAMP    | 开始时间 |
| 12  | `last_update` | TIMESTAMP    | 最近更新时间 |
| 13  | `extra_info`  | VARCHAR(64)  | extra_info |
| 14  | `message`     | VARCHAR(256) | 信息 |
| 15  | `input_rows_per_sec_1m` | DOUBLE | 入口 Reader 最近一个完整 60 秒窗口内处理的物理输入速率，单位为行/秒 |
| 16  | `output_rows_per_sec_1m` | DOUBLE | 最终结果 Runner 最近一个完整 60 秒窗口内成功交付的结果速率，单位为行/秒 |
| 17  | `runner_result_latency_avg_1m_ms` | DOUBLE | 最终结果 Runner 最近一个完整 60 秒窗口内形成结果的平均延迟，单位为毫秒 |

指标不适用于当前任务类型、任务尚未形成首个完整 60 秒窗口或没有有效样本时，对应列为 `NULL`。可结合 `status` 和 `last_update` 判断任务状态及指标是否新鲜。

## INS_SUBSCRIPTIONS

提供主题订阅关系，包括消费者组、分配的 `vgroup`、消费进度与已消费行数等。也可以使用 [`SHOW SUBSCRIPTIONS`](./03-show.md#show-subscriptions) 查询。

| #   | **列名** | **数据类型** | **说明** |
| --- | ---------------- | ----------- | --- |
| 1   | `topic_name`     | BINARY(205) | 被订阅的 topic |
| 2   | `consumer_group` | BINARY(193) | 订阅者的消费者组 |
| 3   | `vgroup_id`      | INT         | 消费者被分配的 vgroup id |
| 4   | `consumer_id`    | BINARY(32)  | 消费者的唯一 id |
| 5   | `user`           | BINARY(24)  | 消费者的登录的用户名 |
| 6   | `fqdn`           | BINARY(128) | 消费者的所在机器的 fqdn |
| 7   | `offset`         | BINARY(64)  | 消费者的消费进度 |
| 8   | `rows`           | BIGINT      | 消费者的消费的数据条数 |

## INS_TABLES

提供普通表与子表的结构与属性信息，例如所属超级表、`vgroup`、TTL 与表类型。也可以使用 [`SHOW TABLES`](./03-show.md#show-tables) 查询。

| #   | **列名** | **数据类型** | **说明** |
| --- | --------------- | ------------- | --- |
| 1   | `table_name`    | VARCHAR(192)  | 表名 |
| 2   | `db_name`       | VARCHAR(64)   | 数据库名 |
| 3   | `create_time`   | TIMESTAMP     | 创建时间 |
| 4   | `columns`       | INT           | 列数目 |
| 5   | `stable_name`   | VARCHAR(192)  | 所属的超级表表名 |
| 6   | `uid`           | BIGINT        | 表 id |
| 7   | `vgroup_id`     | INT           | vgroup id |
| 8   | `ttl`           | INT           | 表的生命周期。关键字列，查询时需用反引号转义（如 `` `ttl` ``）。 |
| 9   | `table_comment` | VARCHAR(1024) | 表注释 |
| 10  | `type`          | VARCHAR(21)   | 表类型 |

## INS_TABLE_FIXED_DISTRIBUTED

提供表数据块分布与压缩等统计信息，可用于分析数据倾斜与存储特征。也可以使用 [`SHOW TABLE DISTRIBUTED`](./03-show.md#show-table-distributed) 查询。

| #   | **列名** | **数据类型** | **说明** |
| --- | ------------------- | ------------ | --- |
| 1   | `db_name`           | VARCHAR(64)  | 数据库名 |
| 2   | `table_name`        | VARCHAR(192) | 表名 |
| 3   | `vgroup_id`         | INT          | `vgroup` ID |
| 4   | `total_blocks`      | BIGINT       | total_blocks |
| 5   | `total_size`        | BIGINT       | total_size |
| 6   | `average_size`      | DOUBLE       | average_size |
| 7   | `compression_ratio` | DOUBLE       | compression_ratio |
| 8   | `block_rows`        | BIGINT       | block_rows |
| 9   | `min_rows`          | INT          | min_rows |
| 10  | `max_rows`          | INT          | max_rows |
| 11  | `avg_rows`          | DOUBLE       | avg_rows |
| 12  | `in_mem_rows`       | BIGINT       | in_mem_rows |
| 13  | `stt_rows`          | BIGINT       | stt_rows |
| 14  | `total_tables`      | BIGINT       | total_tables |
| 15  | `total_filesets`    | BIGINT       | total_filesets |
| 16  | `total_vgroups`     | BIGINT       | total_vgroups |
| 17  | `row_size`          | INT          | row_size |
| 18  | `block_dist_64`     | BIGINT       | block_dist_64 |
| 19  | `block_dist_128`    | BIGINT       | block_dist_128 |
| 20  | `block_dist_256`    | BIGINT       | block_dist_256 |
| 21  | `block_dist_512`    | BIGINT       | block_dist_512 |
| 22  | `block_dist_1024`   | BIGINT       | block_dist_1024 |
| 23  | `block_dist_2048`   | BIGINT       | block_dist_2048 |
| 24  | `block_dist_4096`   | BIGINT       | block_dist_4096 |
| 25  | `block_dist_other`  | BIGINT       | block_dist_other |

## INS_TAGS

提供表的标签名、类型与取值，便于按标签检索与核对元数据。除子表外，也包含带标签的普通表与虚拟普通表的标签，此时 `stable_name` 列为 `NULL`。也可以使用 [`SHOW TAGS`](./03-show.md#show-tags) 查询。

| #   | **列名** | **数据类型** | **说明** |
| --- | ------------- | -------------- | --- |
| 1   | `table_name`  | VARCHAR(192)   | 表名 |
| 2   | `db_name`     | VARCHAR(64)    | 该表所在的数据库的名称 |
| 3   | `stable_name` | VARCHAR(192)   | 所属的超级表表名 |
| 4   | `tag_name`    | VARCHAR(64)    | tag 的名称 |
| 5   | `tag_type`    | VARCHAR(32)    | tag 的类型 |
| 6   | `tag_value`   | VARCHAR(16384) | tag 的值 |

## INS_TOKENS

提供用户访问令牌的名称、所属用户、提供方、启用状态与过期时间等信息。也可以使用 [`SHOW TOKENS`](./03-show.md#show-tokens) 查询。

| #   | **列名** | **数据类型** | **说明** |
| --- | ------------- | ------------- | --- |
| 1   | `name`        | VARCHAR(32)       | 名称 |
| 2   | `user`        | VARCHAR(24)   | 用户名 |
| 3 | `provider` | VARCHAR(64) | 令牌提供方 |
| 4   | `enable`      | TINYINT       | 是否启用 |
| 5   | `create_time` | TIMESTAMP     | 创建时间 |
| 6 | `expire_time` | TIMESTAMP | 过期时间 |
| 7 | `extra_info` | VARCHAR(1024) | 附加信息 |

## INS_TOPICS

提供已创建数据订阅主题的名称、所属数据库、创建 SQL 与 schema 等信息。也可以使用 [`SHOW TOPICS`](./03-show.md#show-topics) 查询。

| #   | **列名** | **数据类型** | **说明** |
| --- | ------------- | ------------- | --- |
| 1   | `topic_name`  | BINARY(192)   | topic 名称 |
| 2   | `db_name`     | BINARY(64)    | topic 相关的 DB |
| 3   | `create_time` | TIMESTAMP     | topic 的 创建时间 |
| 4   | `sql`         | BINARY(2048)  | 创建该 topic 时所用的 SQL 语句 |
| 5   | `schema`      | BINARY(65517) | 主题 schema |

## INS_TRANSACTIONS

提供当前正在执行的元数据事务的阶段、操作对象、失败次数与最近执行信息等。也可以使用 [`SHOW TRANSACTIONS`](./03-show.md#show-transactions) 查询。

| #   | **列名** | **数据类型** | **说明** |
| --- | ------------------ | ------------ | --- |
| 1   | `id`               | BIGINT       | ID |
| 2   | `create_time`      | TIMESTAMP    | 创建时间 |
| 3   | `stage`            | VARCHAR(12)  | 当前阶段 |
| 4   | `oper`             | VARCHAR(22)  | 操作 |
| 5   | `db`               | VARCHAR(64)  | 相关数据库 |
| 6   | `stable`           | VARCHAR(192) | 相关超级表 |
| 7   | `killable`         | VARCHAR(10)  | 是否可终止 |
| 8   | `failed_times`     | INT          | 失败次数 |
| 9   | `last_exec_time`   | TIMESTAMP    | 上次执行时间 |
| 10  | `last_action_info` | VARCHAR(511) | 上次执行失败明细 |
| 11  | `type`             | VARCHAR(10)  | 类型 |

## INS_TRANSACTION_DETAILS

提供元数据事务中各动作的对象类型、目标、结果与明细。也可以使用 [`SHOW TRANSACTION`](./03-show.md#show-transactions) 查询。

| #   | **列名** | **数据类型** | **说明** |
| --- | ---------------- | ------------ | --- |
| 1   | `transaction_id` | INT          | transaction_id |
| 2   | `action`         | VARCHAR(30)  | action |
| 3   | `obj_type`       | VARCHAR(40)  | obj_type |
| 4   | `result`         | VARCHAR(100) | result |
| 5   | `target`         | VARCHAR(300) | target |
| 6   | `detail`         | VARCHAR(100) | detail |

## INS_TRANSACTION_LOGS

提供已完成元数据事务的历史记录，包括创建者、状态、类型与完成时间等。也可以使用 [`SHOW TRANSACTION LOGS`](./03-show.md#show-transaction-logs) 查询。

| #   | **列名** | **数据类型** | **说明** |
| --- | --------------- | ------------ | --- |
| 1   | `id`            | BIGINT       | ID |
| 2   | `create_user`   | VARCHAR(24)  | 创建用户 |
| 3   | `create_time`   | TIMESTAMP    | 创建时间 |
| 4   | `complete_time` | TIMESTAMP    | 完成时间 |
| 5   | `status`        | VARCHAR(12)  | 当前状态 |
| 6   | `comment`       | VARCHAR(128) | 注释 |
| 7   | `type`          | VARCHAR(10)  | 类型 |

## INS_TRANSACTION_ORPHANS

提供孤儿事务检测结果，包括关联 `vgroup`、首次/最近发现时间与上报次数等。也可以使用 [`SHOW TRANSACTION ORPHANS`](./03-show.md#show-transaction-orphans) 查询。

| #   | **列名** | **数据类型** | **说明** |
| --- | -------------- | --------- | --- |
| 1   | `id`           | BIGINT    | ID |
| 2   | `vgroup_id`    | INT       | `vgroup` ID |
| 3   | `first_seen`   | TIMESTAMP | 首次发现时间 |
| 4   | `last_seen`    | TIMESTAMP | 最近发现时间 |
| 5   | `report_count` | INT       | 上报次数 |

## INS_TSMAS

提供时序 SMA（TSMA）的名称、源表/目标表、聚合周期与创建 SQL 等信息。也可以使用 [`SHOW TSMAS`](./03-show.md#show-tsmas) 查询。

| #   | **列名** | **数据类型** | **说明** |
| --- | ------------- | ------------- | --- |
| 1   | `tsma_name`   | VARCHAR(192)  | tsma_name |
| 2   | `db_name`     | VARCHAR(64)   | 数据库名 |
| 3   | `table_name`  | VARCHAR(192)  | 表名 |
| 4   | `target_db`   | VARCHAR(64)   | target_db |
| 5   | `target_stb`  | VARCHAR(192)  | target_stb |
| 6   | `stream_name` | VARCHAR(64)   | 流名称 |
| 7   | `create_time` | TIMESTAMP     | 创建时间 |
| 8   | `interval`    | VARCHAR(64)   | interval |
| 9   | `create_sql`  | VARCHAR(2048) | create_sql |
| 10  | `func_list`   | VARCHAR(2048) | func_list |

## INS_USERS

提供系统用户的基本属性，例如是否超级用户、是否启用、是否可查看系统信息等。也可以使用 [`SHOW USERS`](./03-show.md#show-users) 查询。SYSINFO 为 0 的用户不能查看此表。

| #   | **列名** | **数据类型** | **说明** |
| --- | ------------------ | ------------- | --- |
| 1   | `name`             | VARCHAR(24)   | 用户名 |
| 2   | `super`            | TINYINT       | 是否超级用户：`1` 是，`0` 否 |
| 3   | `enable`           | TINYINT       | 是否启用：`1` 是，`0` 否 |
| 4   | `sysinfo`          | TINYINT       | 是否可查看系统信息：`1` 是，`0` 否 |
| 5   | `createdb`         | TINYINT       | 是否可创建数据库 |
| 6   | `create_time`      | TIMESTAMP     | 创建时间 |
| 7   | `totp`             | TINYINT       | 是否启用 TOTP |
| 8   | `allowed_host`     | VARCHAR(48)   | IP 白名单 |
| 9   | `allowed_datetime` | VARCHAR(48)   | 允许登录的时间窗口 |
| 10  | `roles`            | VARCHAR(2048) | 已授予角色 |
| 11  | `sec_levels`       | BINARY(5)     | 安全等级区间 |

## INS_USERS_FULL

提供用户的完整配置信息，包含密码策略、会话限制、主机/时间白名单与角色等。也可以使用 [`SHOW USERS FULL`](./03-show.md#show-users) 查询。SYSINFO 为 0 的用户不能查看此表。

| #   | **列名** | **数据类型** | **说明** |
| --- | ----------------------- | ------------- | --- |
| 1   | `name`                  | VARCHAR(24)   | 名称 |
| 2   | `super`                 | TINYINT       | super |
| 3   | `enable`                | TINYINT       | 是否启用 |
| 4   | `sysinfo`               | TINYINT       | sysinfo |
| 5   | `createdb`              | TINYINT       | createdb |
| 6   | `create_time`           | TIMESTAMP     | 创建时间 |
| 7   | `totp`                  | TINYINT       | totp |
| 8   | `change_pass`           | TINYINT       | change_pass |
| 9   | `encrypted_pass`        | VARCHAR(32)   | encrypted_pass |
| 10  | `session_per_user`      | INT           | session_per_user |
| 11  | `connect_time`          | INT           | connect_time |
| 12  | `connect_idle_timeout`  | INT           | connect_idle_timeout |
| 13  | `call_per_session`      | INT           | call_per_session |
| 14  | `vnode_per_call`        | INT           | vnode_per_call |
| 15  | `failed_login_attempts` | INT           | failed_login_attempts |
| 16  | `password_life_time`    | INT           | password_life_time |
| 17  | `password_reuse_time`   | INT           | password_reuse_time |
| 18  | `password_reuse_max`    | INT           | password_reuse_max |
| 19  | `password_lock_time`    | INT           | password_lock_time |
| 20  | `password_grace_time`   | INT           | password_grace_time |
| 21  | `inactive_account_time` | INT           | inactive_account_time |
| 22  | `allow_token_num`       | INT           | allow_token_num |
| 23  | `allowed_host`          | VARCHAR(48)   | allowed_host |
| 24  | `allowed_datetime`      | VARCHAR(48)   | allowed_datetime |
| 25  | `roles`                 | VARCHAR(2048) | roles |
| 26  | `sec_levels`            | BINARY(5)     | sec_levels |

## INS_USER_PRIVILEGES

提供用户权限明细，包括权限类型、作用域、库表范围与列权限等。也可以使用 [`SHOW USER PRIVILEGES`](./03-show.md#show-user-privileges) 查询。SYSINFO 为 0 的用户不能查看此表。

| #   | **列名** | **数据类型** | **说明** |
| --- | ------------- | ------------ | --- |
| 1   | `user_name`   | VARCHAR(24)  | 用户名 |
| 2   | `priv_type`   | VARCHAR(128) | 权限类型 |
| 3   | `priv_scope`  | VARCHAR(32)  | 权限作用域 |
| 4   | `db_name`     | VARCHAR(65)  | 数据库名 |
| 5   | `table_name`  | VARCHAR(193) | 表名 |
| 6   | `condition`   | VARCHAR(48)  | 子表权限过滤条件 |
| 7   | `notes`       | VARCHAR(64)  | 备注 |
| 8   | `columns`     | VARCHAR(12)  | 列权限相关信息 |
| 9   | `update_time` | VARCHAR(40)  | 更新时间 |

## INS_VGROUPS

提供各 `vgroup` 的成员分布、副本状态、缓存与 WAL 相关位点等信息。也可以使用 [`SHOW VGROUPS`](./03-show.md#show-vgroups) 查询。

| #   | **列名** | **数据类型** | **说明** |
| --- | ---------------------- | ------------ | --- |
| 1   | `vgroup_id`            | INT          | vgroup id |
| 2   | `db_name`              | VARCHAR(64)  | 数据库名 |
| 3   | `tables`               | INT          | 此 vgroup 内有多少表。关键字列，查询时需用反引号转义（如 `` `tables` ``）。 |
| 4   | `v1_dnode`             | SMALLINT     | 第一个成员所在的 dnode 的 id |
| 5   | `v1_status`            | VARCHAR(9)   | 第一个成员的状态 |
| 6   | `v1_applied/committed` | VARCHAR(100) | 第一个成员的 applied/committed 位点 |
| 7   | `v2_dnode`             | SMALLINT     | 第二个成员所在的 dnode 的 id |
| 8   | `v2_status`            | VARCHAR(9)   | 第二个成员的状态 |
| 9   | `v2_applied/committed` | VARCHAR(100) | 第二个成员的 applied/committed 位点 |
| 10  | `v3_dnode`             | SMALLINT     | 第三个成员所在的 dnode 的 id |
| 11  | `v3_status`            | VARCHAR(9)   | 第三个成员的状态 |
| 12  | `v3_applied/committed` | VARCHAR(100) | 第三个成员的 applied/committed 位点 |
| 13  | `v4_dnode`             | SMALLINT     | 第四个成员所在的 `dnode` ID |
| 14  | `v4_status`            | VARCHAR(9)   | 第四个成员的状态 |
| 15  | `v4_applied/committed` | VARCHAR(100) | 第四个成员的 applied/committed 位点 |
| 16  | `is_ready`             | BOOL         | 是否就绪 |
| 17  | `cacheload`            | BIGINT       | 缓存加载量 |
| 18  | `cacheelements`        | INT          | 缓存元素个数 |
| 19  | `tsma`                 | TINYINT      | 此 vgroup 是否专用于 Time-range-wise SMA，1: 是，0: 否 |
| 20  | `mount_vgroup_id`      | INT          | 挂载对应的 `vgroup` ID |
| 21  | `keep_version`         | BIGINT       | 此 vgroup 大于等于 keep_version 的 wal 日志不会被自动删除 |
| 22  | `keep_version_time`    | TIMESTAMP    | 此 vgroup 在 keep_version 上次被修改的时间 |
| 23  | `compact_start_time`   | TIMESTAMP    | 压缩开始时间 |

## INS_VIEWS

提供视图的名称、所属库、定义 SQL、参数与目标表等信息。也可以使用 [`SHOW VIEWS`](./03-show.md#show-views) 查询。

| #   | **列名** | **数据类型** | **说明** |
| --- | ---------------- | ------------- | --- |
| 1   | `view_name`      | VARCHAR(192)  | 视图名 |
| 2   | `db_name`        | VARCHAR(64)   | 数据库名 |
| 3   | `effective_user` | VARCHAR(24)   | effective_user |
| 4   | `create_time`    | TIMESTAMP     | 创建时间 |
| 5   | `type`           | VARCHAR(128)  | 类型 |
| 6   | `query_sql`      | VARCHAR(2048) | query_sql |
| 7   | `parameters`     | VARCHAR(2048) | parameters |
| 8   | `default_values` | VARCHAR(2048) | default_values |
| 9   | `target_table`   | VARCHAR(192)  | target_table |
| 10  | `column_list`    | VARCHAR(2048) | column_list |

## INS_VIRTUAL_CHILD_COLUMNS

提供虚拟子表各列与源表列的引用关系，以及引用版本等信息。

| #   | **列名** | **数据类型** | **说明** |
| --- | ------------- | ------------ | --- |
| 1   | `table_name`  | VARCHAR(192) | 表名 |
| 2   | `stable_name` | VARCHAR(192) | 超级表名 |
| 3   | `db_name`     | VARCHAR(64)  | 数据库名 |
| 4   | `col_name`    | VARCHAR(64)  | col_name |
| 5   | `uid`         | BIGINT       | UID |
| 6   | `col_id`      | INT          | col_id |
| 7   | `col_source`  | VARCHAR(258) | col_source |
| 8   | `vgroup_id`   | INT          | `vgroup` ID |
| 9   | `ref_version` | INT          | ref_version |
| 10  | `col_type`    | INT          | col_type |

## INS_VIRTUAL_TABLES_REFERENCING

提供虚拟表列对源表列的引用关系，以及校验错误码与错误信息。也可以使用 [`SHOW VTABLE VALIDATE`](./03-show.md#show-vtable-validate) 查询。SYSINFO 为 0 的用户不能查看此表。

| #   | **列名** | **数据类型** | **说明** |
| --- | --------------------- | ------------ | --- |
| 1   | `virtual_db_name`     | VARCHAR(64)  | virtual_db_name |
| 2   | `virtual_stable_name` | VARCHAR(192) | virtual_stable_name |
| 3   | `virtual_table_name`  | VARCHAR(192) | virtual_table_name |
| 4   | `virtual_col_name`    | VARCHAR(64)  | virtual_col_name |
| 5   | `src_db_name`         | VARCHAR(64)  | src_db_name |
| 6   | `src_table_name`      | VARCHAR(192) | src_table_name |
| 7   | `src_column_name`     | VARCHAR(64)  | src_column_name |
| 8   | `type`                | INT          | 类型 |
| 9   | `err_code`            | BIGINT       | err_code |
| 10  | `err_msg`             | VARCHAR(512) | err_msg |

## INS_VNODES

提供各 `vnode` 所在 `dnode`、所属库、角色状态与恢复进度等信息。也可以使用 [`SHOW VNODES`](./03-show.md#show-vnodes) 查询。SYSINFO 为 0 的用户不能查看此表。

| #   | **列名** | **数据类型** | **说明** |
| --- | --------------------- | ----------- | --- |
| 1   | `dnode_id`            | INT         | `dnode` ID |
| 2   | `vgroup_id`           | INT         | `vgroup` ID |
| 3   | `db_name`             | BINARY(64)  | 数据库名 |
| 4   | `status`              | VARCHAR(9)  | `vnode` 状态 |
| 5   | `role_time`           | TIMESTAMP   | 最近选举时间 |
| 6   | `start_time`          | TIMESTAMP   | `vnode` 启动时间 |
| 7   | `restored`            | BOOL        | 是否已恢复 |
| 8   | `apply_finish_time`   | VARCHAR(18) | 恢复完成时间 |
| 9   | `unapplied`           | INT         | 未应用的请求个数 |
| 10  | `buffer_segment_used` | BIGINT      | Buffer 段已用字节数 |
| 11  | `buffer_segment_size` | BIGINT      | Buffer 段总字节数 |

## INS_VSTABLE_INHERITS

提供虚拟超级表之间的继承关系，包括父子超级表名称与 UID 等。也可以使用 [`SHOW VTABLE INHERITS`](./03-show.md#show-vtable-inherits) 查询。

| #   | **列名** | **数据类型** | **说明** |
| --- | -------------------- | ------------ | --- |
| 1   | `db_name`            | VARCHAR(64)  | 数据库名 |
| 2   | `parent_stable_name` | VARCHAR(192) | 父超级表名 |
| 3   | `parent_uid`         | BIGINT       | 父超级表 UID |
| 4   | `child_stable_name`  | VARCHAR(192) | 子超级表名 |
| 5   | `child_uid`          | BIGINT       | 子超级表 UID |
| 6   | `create_time`        | TIMESTAMP    | 创建时间 |

## INS_XNODES

提供 Xnode 数据接入节点的地址、状态与创建/更新时间等信息。也可以使用 [`SHOW XNODES`](./03-show.md#show-xnodes) 查询。SYSINFO 为 0 的用户不能查看此表。

| #   | **列名** | **数据类型** | **说明** |
| --- | ------------- | ------------ | --- |
| 1   | `id`          | INT          | ID |
| 2   | `url`         | VARCHAR(256) | URL |
| 3   | `status`      | VARCHAR(16)  | 当前状态 |
| 4   | `create_time` | TIMESTAMP    | 创建时间 |
| 5   | `update_time` | TIMESTAMP    | 更新时间 |

## INS_XNODE_AGENTS

提供 Xnode Agent 的名称、令牌、状态与创建/更新时间等信息。也可以使用 [`SHOW XNODE AGENTS`](./03-show.md#show-xnode-agents) 查询。SYSINFO 为 0 的用户不能查看此表。

| #   | **列名** | **数据类型** | **说明** |
| --- | ------------- | ------------ | --- |
| 1   | `id`          | INT          | ID |
| 2   | `name`        | VARCHAR(64)  | 名称 |
| 3   | `token`       | VARCHAR(512) | 令牌 |
| 4   | `status`      | VARCHAR(16)  | 当前状态 |
| 5   | `create_time` | TIMESTAMP    | 创建时间 |
| 6   | `update_time` | TIMESTAMP    | 更新时间 |

## INS_XNODE_JOBS

提供 Xnode 任务分片（Job）的配置、所属任务、状态与失败原因等信息。也可以使用 [`SHOW XNODE JOBS`](./03-show.md#show-xnode-jobs) 查询。SYSINFO 为 0 的用户不能查看此表。

| #   | **列名** | **数据类型** | **说明** |
| --- | ------------- | ------------- | --- |
| 1   | `id`          | INT           | ID |
| 2   | `task_id`     | INT           | 任务 ID |
| 3   | `config`      | VARCHAR(48)   | 配置 |
| 4   | `via`         | INT           | 经由节点 |
| 5   | `xnode_id`    | INT           | Xnode ID |
| 6   | `status`      | VARCHAR(16)   | 当前状态 |
| 7   | `reason`      | VARCHAR(1024) | 原因 |
| 8   | `create_time` | TIMESTAMP     | 创建时间 |
| 9   | `update_time` | TIMESTAMP     | 更新时间 |

## INS_XNODE_TASKS

提供 Xnode 数据接入任务的源/目标、状态、标签与创建者等信息。也可以使用 [`SHOW XNODE TASKS`](./03-show.md#show-xnode-tasks) 查询。SYSINFO 为 0 的用户不能查看此表。

| #   | **列名** | **数据类型** | **说明** |
| --- | ------------- | ------------- | --- |
| 1   | `id`          | INT           | ID |
| 2   | `name`        | VARCHAR(64)   | 名称 |
| 3   | `from`        | VARCHAR(4096) | 源 |
| 4   | `to`          | VARCHAR(2048) | 目标 |
| 5   | `parser`      | VARCHAR(48)   | 解析配置 |
| 6   | `via`         | INT           | 经由节点 |
| 7   | `xnode_id`    | INT           | Xnode ID |
| 8   | `status`      | VARCHAR(16)   | 当前状态 |
| 9   | `reason`      | VARCHAR(1024) | 原因 |
| 10  | `created_by`  | VARCHAR(24)   | 创建者 |
| 11  | `labels`      | VARCHAR(4096) | 标签 |
| 12  | `create_time` | TIMESTAMP     | 创建时间 |
| 13  | `update_time` | TIMESTAMP     | 更新时间 |
