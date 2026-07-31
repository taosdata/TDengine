---
title: Metadata
sidebar_label: Metadata
description: INFORMATION_SCHEMA system tables and columns
---

TDengine includes the built-in `INFORMATION_SCHEMA` database for accessing database metadata, system information, and status, such as database and table names and currently executing SQL statements. It contains multiple read-only tables. These tables are views rather than base tables and have no associated data files, so they can only be queried; write operations such as `INSERT` are not supported.

`INFORMATION_SCHEMA` provides, in a more consistent form, information equivalent to various [SHOW commands](./03-show.md), such as [`SHOW TABLES`](./03-show.md#show-tables) and [`SHOW DATABASES`](./03-show.md#show-databases). Compared with `SHOW`, using `SELECT ... FROM INFORMATION_SCHEMA.tablename` has the following advantages:

1. You can use `USE` to set `INFORMATION_SCHEMA` as the default database.
2. You can use familiar `SELECT` syntax and only need to know the table and column names.
3. You can filter and sort results and use any `SELECT` functionality supported by TDengine.
4. Columns can be added to existing tables in the future without affecting existing applications.
5. The query model is closer to the data dictionaries of other database systems, such as Oracle.

:::info

- `SHOW` statements are retained for users who are familiar with them.
- Some column names in system tables are keywords and must be escaped with backticks in queries. For example, to query the number of `vgroup`s in the `test` database:

```sql
SELECT `vgroups` FROM information_schema.ins_databases WHERE name = 'test';
```

:::

The following sections describe each table in `INFORMATION_SCHEMA` and its column structure.

## INS_ANODES

Provides the address, status, creation time, and update time of analysis nodes (anodes). The same information can also be queried using [`SHOW ANODES`](./03-show.md#show-anodes). Users with `SYSINFO` set to 0 cannot view this table.

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ------------- | ------------ | --- |
| 1   | `id`          | INT          | ID |
| 2   | `url`         | VARCHAR(128) | URL |
| 3   | `status`      | VARCHAR(10)  | Current status |
| 4   | `create_time` | TIMESTAMP    | Creation time |
| 5   | `update_time` | TIMESTAMP    | Update time |

## INS_ANODES_FULL

Provides details about algorithms loaded on analysis nodes, including type, name, status, and notes. The same information can also be queried using [`SHOW ANODES FULL`](./03-show.md#show-anodes). Users with `SYSINFO` set to 0 cannot view this table.

| #   | **Column Name** | **Data Type** | **Description** |
| --- | -------- | ------------ | --- |
| 1   | `id`     | INT          | ID |
| 2   | `type`   | VARCHAR(24)  | Type |
| 3   | `algo`   | VARCHAR(64)  | Algorithm |
| 4   | `status` | VARCHAR(10)  | Current status |
| 5   | `note`   | VARCHAR(256) | Notes |

## INS_ARBGROUPS

Provides information about replica `dnode`s, synchronization status, and assigned tokens in arbitrator groups. The same information can also be queried using [`SHOW ARBGROUPS`](./03-show.md#show-arbgroups). Users with `SYSINFO` set to 0 cannot view this table.

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ----------------- | ----------- | --- |
| 1   | `db_name`         | VARCHAR(64) | Database name |
| 2   | `vgroup_id`       | INT         | `vgroup` ID |
| 3   | `v1_dnode`        | SMALLINT    | v1_dnode |
| 4   | `v2_dnode`        | SMALLINT    | v2_dnode |
| 5   | `is_sync`         | BOOL        | is_sync |
| 6   | `check_sync_code` | VARCHAR(98) | check_sync_code |
| 7   | `assigned_dnode`  | SMALLINT    | assigned_dnode |
| 8   | `assigned_token`  | VARCHAR(32) | assigned_token |
| 9   | `assigned_acked`  | SMALLINT    | assigned_acked |

## INS_BNODES

Provides the ID, address, protocol, and creation time of each `bnode` (bridge node) in the cluster. The same information can also be queried using [`SHOW BNODES`](./03-show.md#show-bnodes). Users with `SYSINFO` set to 0 cannot view this table.

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ------------- | ------------ | --- |
| 1   | `id`          | INT          | ID |
| 2   | `endpoint`    | VARCHAR(134) | Address |
| 3   | `protocol`    | VARCHAR(14)  | Protocol |
| 4   | `create_time` | TIMESTAMP    | Creation time |

## INS_CLUSTER

Provides the current cluster ID, name, uptime, version, and license expiration time. The same information can also be queried using [`SHOW CLUSTER`](./03-show.md#show-cluster). Users with `SYSINFO` set to 0 cannot view this table.

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ------------- | ----------- | --- |
| 1   | `id`          | BIGINT      | Cluster ID |
| 2   | `name`        | VARCHAR(40) | Cluster name |
| 3   | `uptime`      | INT         | Uptime (seconds) |
| 4   | `create_time` | TIMESTAMP   | Creation time |
| 5   | `version`     | VARCHAR(10) | Version |
| 6   | `expire_time` | TIMESTAMP   | Expiration time |

## INS_COLUMNS

Provides table-column names, types, lengths, precision, and the data sources of virtual-table columns.

| #   | **Column Name** | **Data Type** | **Description** |
| --- | --------------- | ------------ | --- |
| 1   | `table_name`    | VARCHAR(192) | Table name |
| 2   | `db_name`       | VARCHAR(64)  | Name of the database containing the table |
| 3   | `table_type`    | VARCHAR(21)  | Table type |
| 4   | `col_name`      | VARCHAR(64)  | Column name |
| 5   | `col_type`      | VARCHAR(32)  | Column type |
| 6   | `col_length`    | INT          | Column length |
| 7   | `col_precision` | INT          | Column precision |
| 8   | `col_scale`     | INT          | Column scale |
| 9   | `col_nullable`  | INT          | Whether the column can be null |
| 10  | `col_source`    | VARCHAR(258) | Column data source. This value is present only for virtual-table columns and identifies the virtual table's data source as db_name.table_name.col_name |
| 11  | `col_id`        | SMALLINT     | Column ID |

## INS_COMPACTS

Provides the ID, target database, and start time of data compaction tasks. The same information can also be queried using [`SHOW COMPACTS`](./03-show.md#show-compacts).

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ------------ | ----------- | --- |
| 1   | `compact_id` | INT         | Compaction task ID |
| 2   | `db_name`    | VARCHAR(64) | Database name |
| 3   | `start_time` | TIMESTAMP   | Start time |

## INS_COMPACT_DETAILS

Provides compaction-task details for each `vgroup` and `dnode`, including progress, completion status, and remaining time. The same information can also be queried using [`SHOW COMPACT`](./03-show.md#show-compacts).

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ---------------- | --------- | --- |
| 1   | `compact_id`     | INT       | Compaction task ID |
| 2   | `vgroup_id`      | INT       | `vgroup` ID |
| 3   | `dnode_id`       | INT       | `dnode` ID |
| 4   | `number_fileset` | INT       | number_fileset |
| 5   | `finished`       | INT       | finished |
| 6   | `start_time`     | TIMESTAMP | Start time |
| 7   | `progress(%)`    | INT       | progress(%) |
| 8   | `remain_time(s)` | BIGINT    | remain_time(s) |

## INS_CONFIGS

Provides the names and values of currently effective system configuration parameters. The same information can also be queried using [`SHOW CLUSTER VARIABLES`](./03-show.md#show-cluster-variables).

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ------- | ----------- | --- |
| 1   | `name`  | VARCHAR(32) | Configuration item name |
| 2   | `value` | VARCHAR(64) | Value of the configuration item. This column is a keyword and must be escaped with backticks in queries (for example, `` `value` ``). |

## INS_CPU_ALLOCATION

Provides CPU core allocation for management, write, and read threads on each `dnode`. The data is valid when `enableCpuAffinity` is enabled. The same information can also be queried using [`SHOW CPU_ALLOCATION`](./03-show.md#show-cpu_allocation). Users with `SYSINFO` set to 0 cannot view this table.

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ----------------- | ------------ | --- |
| 1   | `dnode_id`        | INT          | Dnode identifier |
| 2   | `thread_category` | VARCHAR(16)  | Thread category: `management`, `write`, or `read` |
| 3   | `cores`           | INT          | Number of CPU cores allocated to this category (0 when disabled) |
| 4   | `core_ids`        | VARCHAR(256) | Comma-separated list of assigned core IDs; `"-"` when disabled |
| 5   | `enabled`         | BOOL         | Whether CPU affinity is enabled for this category |

## INS_DATABASES

Provides configuration and status information for user databases, such as replica count, retention policies, cache settings, and WAL-related parameters. The same information can also be queried using [`SHOW DATABASES`](./03-show.md#show-databases).

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ---------------------- | ----------- | --- |
| 1   | `name`                 | VARCHAR(64) | Database name |
| 2   | `create_time`          | TIMESTAMP   | Creation time |
| 3   | `vgroups`              | INT         | Number of `vgroup`s in the database. This column is a keyword and must be escaped with backticks in queries (for example, `` `vgroups` ``). |
| 4   | `ntables`              | BIGINT      | Number of tables in the database, including subtables and regular tables but excluding supertables |
| 5   | `replica`              | TINYINT     | Number of replicas. This column is a keyword and must be escaped with backticks in queries (for example, `` `replica` ``). |
| 6   | `strict`               | VARCHAR(4)  | Deprecated parameter |
| 7   | `duration`             | VARCHAR(10) | Time span of data stored in a single file. This column is a keyword and must be escaped with backticks in queries (for example, `` `duration` ``). It is stored internally in minutes and may be displayed in days or hours in query results. |
| 8   | `keep`                 | VARCHAR(32) | Data retention duration. This column is a keyword and must be escaped with backticks in queries (for example, `` `keep` ``). It is stored internally in minutes and may be displayed in days or hours in query results. |
| 9   | `buffer`               | INT         | Size of each `vnode` write-cache memory block, in MB. This column is a keyword and must be escaped with backticks in queries (for example, `` `buffer` ``). |
| 10  | `pagesize`             | INT         | Page size of the metadata storage engine in each VNODE, in KB. This column is a keyword and must be escaped with backticks in queries (for example, `` `pagesize` ``). |
| 11  | `pages`                | INT         | Number of cache pages in each `vnode` metadata storage engine. This column is a keyword and must be escaped with backticks in queries (for example, `` `pages` ``). |
| 12  | `minrows`              | INT         | Minimum number of records in a file block. This column is a keyword and must be escaped with backticks in queries (for example, `` `minrows` ``). |
| 13  | `maxrows`              | INT         | Maximum number of records in a file block. This column is a keyword and must be escaped with backticks in queries (for example, `` `maxrows` ``). |
| 14  | `comp`                 | TINYINT     | Data compression method. This column is a keyword and must be escaped with backticks in queries (for example, `` `comp` ``). |
| 15  | `precision`            | VARCHAR(2)  | Time precision. This column is a keyword and must be escaped with backticks in queries (for example, `` `precision` ``). |
| 16  | `status`               | VARCHAR(10) | Database status |
| 17  | `retentions`           | VARCHAR(60) | Data aggregation interval and retention duration. This column is a keyword and must be escaped with backticks in queries (for example, `` `retentions` ``). |
| 18  | `single_stable`        | BOOL        | Whether only one supertable can be created in this database. This column is a keyword and must be escaped with backticks in queries (for example, `` `single_stable` ``). |
| 19  | `cachemodel`           | VARCHAR(11) | Whether recent subtable data is cached in memory. This column is a keyword and must be escaped with backticks in queries (for example, `` `cachemodel` ``). |
| 20  | `cachesize`            | INT         | Amount of memory in each `vnode` used to cache recent subtable data. This column is a keyword and must be escaped with backticks in queries (for example, `` `cachesize` ``). |
| 21  | `cacheshardbits`       | INT         | Number of shard bits for the last-value LRU cache. The actual number of shards is `2^cacheshardbits`; -1 means the system calculates it automatically from `cachesize`. This column is a keyword and must be escaped with backticks in queries (for example, `` `cacheshardbits` ``). |
| 22  | `wal_level`            | TINYINT     | WAL level. This column is a keyword and must be escaped with backticks in queries (for example, `` `wal_level` ``). |
| 23  | `wal_fsync_period`     | INT         | Data flush period. This column is a keyword and must be escaped with backticks in queries (for example, `` `wal_fsync_period` ``). |
| 24  | `wal_retention_period` | INT         | WAL retention duration, in seconds. This column is a keyword and must be escaped with backticks in queries (for example, `` `wal_retention_period` ``). |
| 25  | `wal_retention_size`   | BIGINT      | WAL retention limit. This column is a keyword and must be escaped with backticks in queries (for example, `` `wal_retention_size` ``). |
| 26  | `stt_trigger`          | SMALLINT    | Number of persisted files that triggers file merging. This column is a keyword and must be escaped with backticks in queries (for example, `` `stt_trigger` ``). |
| 27  | `table_prefix`         | SMALLINT    | Length of the prefix ignored when the internal storage engine assigns a VNODE for table data based on the table name. This column is a keyword and must be escaped with backticks in queries (for example, `` `table_prefix` ``). |
| 28  | `table_suffix`         | SMALLINT    | Length of the suffix ignored when the internal storage engine assigns a VNODE for table data based on the table name. This column is a keyword and must be escaped with backticks in queries (for example, `` `table_suffix` ``). |
| 29  | `tsdb_pagesize`        | INT         | Page size in the time-series data storage engine. This column is a keyword and must be escaped with backticks in queries (for example, `` `tsdb_pagesize` ``). |
| 30  | `keep_time_offset`     | INT         | `KEEP` time offset |
| 31  | `ss_chunkpages`        | INT         | Number of shared-storage chunk pages |
| 32  | `ss_keeplocal`         | VARCHAR(10) | Shared-storage local retention policy |
| 33  | `ss_compact`           | TINYINT     | Shared-storage compaction configuration |
| 34  | `with_arbitrator`      | TINYINT     | Whether arbitration is enabled |
| 35  | `encrypt_algorithm`    | VARCHAR(16) | Encryption algorithm |
| 36  | `compact_interval`     | VARCHAR(12) | Automatic compaction interval |
| 37  | `compact_time_range`   | VARCHAR(24) | Automatic compaction time range |
| 38  | `compact_time_offset`  | VARCHAR(4)  | Automatic compaction time offset |
| 39  | `is_audit`             | BOOL        | Whether this is an audit database |
| 40  | `owner`                | VARCHAR(24) | Owner |
| 41  | `allow_drop`           | BOOL        | Whether deletion is allowed |
| 42  | `sec_level`            | TINYINT     | Security level |

## INS_DISK_USAGE

Summarizes disk usage for WAL, multi-tier storage, cache, and metadata by database and `vgroup`, in KB. The same information can also be queried using [`SHOW DISK_INFO`](./03-show.md#show-disk_info).

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ------------ | ----------- | --- |
| 1   | `db_name`    | VARCHAR(32) | Database name |
| 2   | `vgroup_id`  | INT         | `vgroup` ID |
| 3   | `wal_size`   | BIGINT      | WAL file size, in KB |
| 4   | `data1`      | BIGINT      | Data-file size on primary storage, in KB |
| 5   | `data2`      | BIGINT      | Data-file size on secondary storage, in KB |
| 6   | `data3`      | BIGINT      | Data-file size on tertiary storage, in KB |
| 7   | `cache_rdb`  | BIGINT      | Size of last/last_row files, in KB |
| 8   | `table_meta` | BIGINT      | Metadata file size, in KB |
| 9   | `ss`         | BIGINT      | Space used on shared storage, in KB |
| 10  | `raw_data`   | BIGINT      | Estimated raw-data size, in KB |

## INS_DNODES

Provides the ID, address, `vnode` capacity, and operating status of each `dnode` (data node) in the cluster. The same information can also be queried using [`SHOW DNODES`](./03-show.md#show-dnodes). Users with `SYSINFO` set to 0 cannot view this table.

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ---------------- | ------------ | --- |
| 1   | `id`             | INT          | `dnode` ID |
| 2   | `endpoint`       | VARCHAR(134) | `dnode` Address |
| 3   | `vnodes`         | SMALLINT     | Actual number of `vnode`s. This column is a keyword and must be escaped with backticks in queries (for example, `` `vnodes` ``) |
| 4   | `support_vnodes` | SMALLINT     | Maximum number of supported `vnode`s |
| 5   | `status`         | VARCHAR(10)  | Current status |
| 6   | `create_time`    | TIMESTAMP    | Creation time |
| 7   | `reboot_time`    | TIMESTAMP    | Most recent restart time |
| 8   | `note`           | VARCHAR(256) | Information such as the reason for being offline |
| 9   | `machine_id`     | VARCHAR(24)  | Machine ID (Enterprise Edition) |

## INS_DNODE_VARIABLES

Provides configuration parameters on each `dnode`, including their scope, category, and description. The same information can also be queried using [`SHOW DNODE VARIABLES`](./03-show.md#show-cluster-variables). Users with `SYSINFO` set to 0 cannot view this table.

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ---------- | ------------- | --- |
| 1   | `dnode_id` | INT           | `dnode` ID |
| 2   | `name`     | VARCHAR(32)   | Configuration item name |
| 3   | `value`    | VARCHAR(4096) | Value of the configuration item. This column is a keyword and must be escaped with backticks in queries (for example, `` `value` ``). |
| 4   | `scope`    | VARCHAR(8)    | Configuration scope |
| 5   | `category` | VARCHAR(8)    | Configuration category |
| 6   | `info`     | VARCHAR(64)   | Configuration description |

## INS_ENCRYPTIONS

Provides the encryption-key status of each `dnode`. The same information can also be queried using [`SHOW ENCRYPTIONS`](./03-show.md#show-encryptions). Users with `SYSINFO` set to 0 cannot view this table.

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ------------ | ----------- | --- |
| 1   | `dnode_id`   | INT         | `dnode` ID |
| 2   | `key_status` | VARCHAR(12) | key_status |

## INS_ENCRYPT_ALGORITHMS

Provides the names, types, sources, and descriptions of encryption algorithms available in the system. The same information can also be queried using [`SHOW ENCRYPT_ALGORITHMS`](./03-show.md#show-encrypt_algorithms).

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ---------------- | ------------ | --- |
| 1   | `id`             | INT          | ID |
| 2   | `algorithm_id`   | VARCHAR(64)  | algorithm_id |
| 3   | `name`           | VARCHAR(64)  | Name |
| 4   | `desc`           | VARCHAR(128) | Description |
| 5   | `type`           | VARCHAR(198) | Type |
| 6   | `source`         | VARCHAR(198) | source |
| 7   | `ossl_algr_name` | VARCHAR(64)  | ossl_algr_name |

## INS_ENCRYPT_STATUS

Provides the current encryption scope, algorithm, and encryption status. The same information can also be queried using [`SHOW ENCRYPT_STATUS`](./03-show.md#show-encrypt_status).

| #   | **Column Name** | **Data Type** | **Description** |
| --- | --------------- | ----------- | --- |
| 1   | `encrypt_scope` | VARCHAR(32) | encrypt_scope |
| 2   | `algorithm`     | VARCHAR(32) | algorithm |
| 3   | `status`        | VARCHAR(16) | Current status |

## INS_EXT_SOURCES

Provides connection information, database/schema, and options for external data sources used in federated queries. The same information can also be queried using [`SHOW EXTERNAL SOURCES`](./03-show.md#show-external-sources).

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ------------- | ------------- | --- |
| 1   | `source_name` | VARCHAR(64)   | source_name |
| 2   | `type`        | VARCHAR(16)   | Type |
| 3   | `host`        | VARCHAR(256)  | Host |
| 4   | `port`        | INT           | Port |
| 5   | `user`        | VARCHAR(128)  | Username |
| 6   | `password`    | VARCHAR(8)    | Password |
| 7   | `database`    | VARCHAR(64)       | Database |
| 8   | `schema`      | VARCHAR(64)       | schema |
| 9   | `options`     | VARCHAR(8191) | Options |
| 10  | `create_time` | TIMESTAMP     | Creation time |

## INS_FILESETS

Provides the time range, size, most recent compaction time, and compaction requirement of data filesets.

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ---------------- | ------------ | --- |
| 1   | `db_name`        | VARCHAR(64)  | Database name |
| 2   | `vgroup_id`      | INT          | vgroup id |
| 3   | `fileset_id`     | INT          | Fileset ID |
| 4   | `start_time`     | TIMESTAMP    | Start of the data time range covered by the fileset |
| 5   | `end_time`       | TIMESTAMP    | End of the data time range covered by the fileset |
| 6   | `total_size`     | BIGINT       | Total fileset size |
| 7   | `last_compact`   | TIMESTAMP    | Time of the last compaction |
| 8   | `should_compact` | BOOL         | Whether compaction is required: true for required, false for not required |
| 9   | `details`        | VARCHAR(256) | Detailed information |

## INS_FUNCTIONS

Provides the name, type, language, function body, and version of user-defined functions (UDFs). The same information can also be queried using [`SHOW FUNCTIONS`](./03-show.md#show-functions).

| #   | **Column Name** | **Data Type** | **Description** |
| --- | --------------- | -------------- | --- |
| 1   | `name`          | VARCHAR(64)    | Function name |
| 2   | `comment`       | VARCHAR(4095)  | Additional description. This column is a keyword and must be escaped with backticks in queries (for example, `` `comment` ``). |
| 3   | `aggregate`     | INT            | Whether the function is an aggregate function. This column is a keyword and must be escaped with backticks in queries (for example, `` `aggregate` ``). |
| 4   | `output_type`   | VARCHAR(31)    | Output type |
| 5   | `create_time`   | TIMESTAMP      | Creation time |
| 6   | `code_len`      | INT            | Code length |
| 7   | `bufsize`       | INT            | Buffer size |
| 8   | `func_language` | VARCHAR(31)    | UDF programming language |
| 9   | `func_body`     | VARCHAR(65517) | Function body definition |
| 10  | `func_version`  | INT            | Function version. The initial version is 0 and increases by 1 each time the function is replaced. |

## INS_GRANTS

Provides an overview of Enterprise Edition licensing, including license status, time-series points, `dnode` and `vnode` quotas, and storage limits. The same information can also be queried using [`SHOW LICENCES`](./03-show.md#show-licences). Users with `SYSINFO` set to 0 cannot view this table.

| #   | **Column Name** | **Data Type** | **Description** |
| --- | -------------- | ----------- | --- |
| 1   | `version`      | VARCHAR(64) | Enterprise Edition license version description |
| 2   | `expire_time`  | VARCHAR(19) | Expiration time |
| 3   | `service_time` | VARCHAR(19) | Service time |
| 4   | `expired`      | VARCHAR(5)  | Whether expired |
| 5   | `state`        | VARCHAR(9)  | License status |
| 6   | `timeseries`   | VARCHAR(43) | Licensed time-series point count |
| 7   | `dnodes`       | VARCHAR(21) | Licensed `dnode` count. This column is a keyword and must be escaped with backticks in queries (for example, `` `dnodes` ``) |
| 8   | `cpu_cores`    | VARCHAR(21) | Licensed CPU core count |
| 9   | `vnodes`       | VARCHAR(21) | Licensed `vnode` count. This column is a keyword and must be escaped with backticks in queries (for example, `` `vnodes` ``) |
| 10  | `storage_size` | VARCHAR(43) | Licensed storage size |

## INS_GRANTS_FULL

Provides details for each Enterprise Edition license item, including display name, expiration time, and limits. The same information can also be queried using [`SHOW GRANTS FULL`](./03-show.md#show-licences). Users with `SYSINFO` set to 0 cannot view this table.

| #   | **Column Name** | **Data Type** | **Description** |
| --- | -------------- | ------------ | --- |
| 1   | `grant_name`   | VARCHAR(32)  | License item name |
| 2   | `display_name` | VARCHAR(256) | Display name |
| 3   | `expire`       | VARCHAR(32)  | Expiration time (seconds) |
| 4   | `limits`       | VARCHAR(512) | limits |

## INS_GRANTS_LOGS

Provides logs related to license activation, revocation, and machine binding. The same information can also be queried using [`SHOW GRANTS LOGS`](./03-show.md#show-licences). Users with `SYSINFO` set to 0 cannot view this table.

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ------------- | -------------- | --- |
| 1   | `state`       | VARCHAR(1536)  | state |
| 2   | `active`      | VARCHAR(512)   | active |
| 3   | `machine`     | VARCHAR(15600) | machine |
| 4   | `active_info` | VARCHAR(512)   | active_info |
| 5   | `revoke_info` | VARCHAR(30)    | revoke_info |

## INS_INDEXES

Provides the names of created indexes, their databases and tables, column names, and index types. The same information can also be queried using [`SHOW INDEXES`](./03-show.md#show-indexes).

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ------------- | ------------ | --- |
| 1   | `index_name`  | VARCHAR(192) | Index name |
| 2   | `db_name`     | VARCHAR(64)  | Database containing the indexed table |
| 3   | `table_name`  | VARCHAR(192) | Indexed table name |
| 4   | `vgroup_id`   | INT          | `vgroup` ID |
| 5   | `create_time` | TIMESTAMP    | Creation time |
| 6   | `column_name` | VARCHAR(192) | Indexed column name |
| 7   | `index_type`  | VARCHAR(192) | Index type (such as SMA or tag) |

## INS_MACHINES

Provides the IDs, `dnode` counts, and versions of machines bound to licenses. The same information can also be queried using [`SHOW CLUSTER MACHINES`](./03-show.md#show-cluster-machines). Users with `SYSINFO` set to 0 cannot view this table.

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ----------- | ------------- | --- |
| 1   | `id`        | VARCHAR(41)   | ID |
| 2   | `dnode_num` | INT           | dnode_num |
| 3   | `machine`   | VARCHAR(7552) | machine |
| 4   | `version`   | VARCHAR(32)   | version |

## INS_MNODES

Provides the ID, address, role, and status of each `mnode` (management node) in the cluster. The same information can also be queried using [`SHOW MNODES`](./03-show.md#show-mnodes). Users with `SYSINFO` set to 0 cannot view this table.

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ------------- | ------------ | --- |
| 1   | `id`          | INT          | mnode id |
| 2   | `endpoint`    | VARCHAR(134) | `mnode` address |
| 3   | `role`        | VARCHAR(12)  | Current role |
| 4   | `status`      | VARCHAR(9)   | Current status |
| 5   | `create_time` | TIMESTAMP    | Creation time |
| 6   | `role_time`   | TIMESTAMP    | Time when the current role was assumed |

## INS_MOUNTS

Provides database mount names, their `dnode`s, paths, and creation times. The same information can also be queried using [`SHOW MOUNTS`](./03-show.md#show-mounts). Users with `SYSINFO` set to 0 cannot view this table.

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ------------- | ------------ | --- |
| 1   | `name`        | VARCHAR(77)  | Name |
| 2   | `dnode`       | INT          | dnode |
| 3   | `create_time` | TIMESTAMP    | Creation time |
| 4   | `path`        | VARCHAR(128) | Path |

## INS_QNODES

Provides the ID, address, and creation time of each `qnode` (query node) in the cluster. The same information can also be queried using [`SHOW QNODES`](./03-show.md#show-qnodes). Users with `SYSINFO` set to 0 cannot view this table.

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ------------- | ------------ | --- |
| 1   | `id`          | INT          | qnode id |
| 2   | `endpoint`    | VARCHAR(134) | `qnode` address |
| 3   | `create_time` | TIMESTAMP    | Creation time |

## INS_RETENTIONS

Provides the ID, target database, trigger mode, and type of data-retention tasks. The same information can also be queried using [`SHOW RETENTIONS`](./03-show.md#show-retentions).

| #   | **Column Name** | **Data Type** | **Description** |
| --- | -------------- | ----------- | --- |
| 1   | `retention_id` | INT         | Retention task ID |
| 2   | `db_name`      | VARCHAR(64) | Database name |
| 3   | `start_time`   | TIMESTAMP   | Start time |
| 4   | `trigger_mode` | VARCHAR(10) | trigger_mode |
| 5   | `type`         | VARCHAR(10) | Type |

## INS_RETENTION_DETAILS

Provides retention-task details for each `vgroup` and `dnode`, including progress and remaining time. The same information can also be queried using [`SHOW RETENTION`](./03-show.md#show-retentions).

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ---------------- | --------- | --- |
| 1   | `retention_id`   | INT       | Retention task ID |
| 2   | `vgroup_id`      | INT       | `vgroup` ID |
| 3   | `dnode_id`       | INT       | `dnode` ID |
| 4   | `number_fileset` | INT       | number_fileset |
| 5   | `finished`       | INT       | finished |
| 6   | `start_time`     | TIMESTAMP | Start time |
| 7   | `progress(%)`    | INT       | progress(%) |
| 8   | `remain_time(s)` | BIGINT    | remain_time(s) |

## INS_ROLES

Provides role names, enablement status, types, and subroles. The same information can also be queried using [`SHOW ROLES`](./03-show.md#show-roles). Users with `SYSINFO` set to 0 cannot view this table.

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ------------- | ------------- | --- |
| 1   | `name`        | VARCHAR(64)   | Name |
| 2   | `enable`      | TINYINT       | Whether enabled |
| 3   | `create_time` | TIMESTAMP     | Creation time |
| 4   | `update_time` | TIMESTAMP     | Update time |
| 5   | `role_type`   | VARCHAR(7)    | role_type |
| 6   | `subroles`    | VARCHAR(2048) | subroles |

## INS_ROLE_COLUMN_PRIVILEGES

Provides details of column-level privileges granted to roles. The same information can also be queried using [`SHOW ROLE COLUMN PRIVILEGES`](./03-show.md#show-role-column-privileges). Users with `SYSINFO` set to 0 cannot view this table.

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ------------- | ------------ | --- |
| 1   | `role_name`   | VARCHAR(24)  | role_name |
| 2   | `priv_type`   | VARCHAR(128) | priv_type |
| 3   | `priv_scope`  | VARCHAR(32)  | priv_scope |
| 4   | `db_name`     | VARCHAR(65)  | Database name |
| 5   | `table_name`  | VARCHAR(193) | Table name |
| 6   | `column_name` | VARCHAR(65)  | column_name |
| 7   | `condition`   | VARCHAR(48)  | condition |
| 8   | `update_time` | VARCHAR(40)  | Update time |
| 9   | `notes`       | VARCHAR(64)  | notes |

## INS_ROLE_PRIVILEGES

Provides role privilege details, including privilege type, scope, database and table range, and conditions. The same information can also be queried using [`SHOW ROLE PRIVILEGES`](./03-show.md#show-role-privileges). Users with `SYSINFO` set to 0 cannot view this table.

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ------------- | ------------ | --- |
| 1   | `role_name`   | VARCHAR(24)  | role_name |
| 2   | `priv_type`   | VARCHAR(128) | priv_type |
| 3   | `priv_scope`  | VARCHAR(32)  | priv_scope |
| 4   | `db_name`     | VARCHAR(65)  | Database name |
| 5   | `table_name`  | VARCHAR(193) | Table name |
| 6   | `condition`   | VARCHAR(48)  | condition |
| 7   | `notes`       | VARCHAR(64)  | notes |
| 8   | `columns`     | VARCHAR(12)  | columns |
| 9   | `update_time` | VARCHAR(40)  | Update time |

## INS_RSMAS

Provides RSMA definition names, source tables, aggregation intervals, and function lists. The same information can also be queried using [`SHOW RSMAS`](./03-show.md#show-rsmas).

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ------------- | ------------- | --- |
| 1   | `rsma_name`   | VARCHAR(192)  | rsma_name |
| 2   | `rsma_id`     | BIGINT        | rsma_id |
| 3   | `db_name`     | VARCHAR(64)   | Database name |
| 4   | `table_name`  | VARCHAR(192)  | Table name |
| 5   | `table_type`  | VARCHAR(21)   | table_type |
| 6   | `create_time` | TIMESTAMP     | Creation time |
| 7   | `interval`    | VARCHAR(64)   | interval |
| 8   | `func_list`   | VARCHAR(2048) | func_list |

## INS_SCANS

Provides the ID, target database, and start time of data-scanning tasks. The same information can also be queried using [`SHOW SCANS`](./03-show.md#show-scans).

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ------------ | ----------- | --- |
| 1   | `scan_id`    | INT         | Scan task ID |
| 2   | `db_name`    | VARCHAR(64) | Database name |
| 3   | `start_time` | TIMESTAMP   | Start time |

## INS_SCAN_DETAILS

Provides scan-task details for each `vgroup` and `dnode`, including progress and remaining time. The same information can also be queried using [`SHOW SCAN`](./03-show.md#show-scans).

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ---------------- | --------- | --- |
| 1   | `scan_id`        | INT       | Scan task ID |
| 2   | `vgroup_id`      | INT       | `vgroup` ID |
| 3   | `dnode_id`       | INT       | `dnode` ID |
| 4   | `number_fileset` | INT       | number_fileset |
| 5   | `finished`       | INT       | finished |
| 6   | `start_time`     | TIMESTAMP | Start time |
| 7   | `progress(%)`    | INT       | progress(%) |
| 8   | `remain_time(s)` | BIGINT    | remain_time(s) |

## INS_SECURITY_POLICIES

Provides security-policy names, modes, operators, and most recent update times. The same information can also be queried using [`SHOW SECURITY_POLICIES`](./03-show.md#show-security_policies). Users with `SYSINFO` set to 0 cannot view this table.

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ------------- | ------------ | --- |
| 1   | `name`        | VARCHAR(3)   | Name |
| 2   | `mode`        | VARCHAR(30)  | Mode |
| 3   | `operator`    | VARCHAR(24)  | Operator |
| 4   | `last_update` | TIMESTAMP    | Most recent update time |
| 5   | `desc`        | VARCHAR(128) | Description |

## INS_SNAP_SEND_FILESETS

Provides read/write volume, progress, and transfer-type details by fileset during snapshot replication.

| #   | **Column Name** | **Data Type** | **Description** |
| --- | --------------------- | ----------- | --- |
| 1   | `vgroup_id`           | INT         | `vgroup` ID |
| 2   | `fid`                 | INT         | fid |
| 3   | `file_count`          | INT         | file_count |
| 4   | `finished_file_count` | INT         | finished_file_count |
| 5   | `total_size`          | BIGINT      | total_size |
| 6   | `read_size`           | BIGINT      | read_size |
| 7   | `start_time`          | TIMESTAMP   | Start time |
| 8   | `elapsed`             | VARCHAR(32) | Elapsed time |
| 9   | `start_index`         | BIGINT      | start_index |
| 10  | `end_index`           | BIGINT      | end_index |
| 11  | `transfer_type`       | VARCHAR(4)  | transfer_type |

## INS_SNAP_SEND_VNODES

Provides fileset transfer progress and elapsed time by `vnode` during snapshot replication.

| #   | **Column Name** | **Data Type** | **Description** |
| --- | -------------------- | ----------- | --- |
| 1   | `vgroup_id`          | INT         | `vgroup` ID |
| 2   | `dnode_id`           | INT         | `dnode` ID |
| 3   | `total_file_sets`    | INT         | total_file_sets |
| 4   | `finished_file_sets` | INT         | finished_file_sets |
| 5   | `start_time`         | TIMESTAMP   | Start time |
| 6   | `elapsed`            | VARCHAR(32) | Elapsed time |

## INS_SNODES

Provides the ID, address, and replica relationships of each `snode` (stream-processing node) in the cluster. The same information can also be queried using [`SHOW SNODES`](./03-show.md#show-snodes). Users with `SYSINFO` set to 0 cannot view this table.

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ------------- | ------------ | --- |
| 1   | `id`          | INT          | `snode` ID |
| 2   | `endpoint`    | VARCHAR(134) | `snode` Address |
| 3   | `create_time` | TIMESTAMP    | Creation time |
| 4   | `replicaId`   | INT          | Replica ID |
| 5   | `asReplicaOf` | VARCHAR(64)  | `snode` for which this node acts as a replica |

## INS_SSMIGRATES

Provides shared-storage migration progress, including migrated `vgroup`s and filesets. The same information can also be queried using [`SHOW SSMIGRATES`](./03-show.md#show-ssmigrates).

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ------------------ | ----------- | --- |
| 1   | `ssmigrate_id`     | INT         | Shared-storage migration task ID |
| 2   | `db_name`          | VARCHAR(64) | Database name |
| 3   | `start_time`       | TIMESTAMP   | Start time |
| 4   | `number_vgroup`    | INT         | number_vgroup |
| 5   | `migrated_vgroup`  | INT         | migrated_vgroup |
| 6   | `vgroup_id`        | INT         | `vgroup` ID |
| 7   | `number_fileset`   | INT         | number_fileset |
| 8   | `migrated_fileset` | INT         | migrated_fileset |
| 9   | `fileset_id`       | INT         | fileset_id |

## INS_STABLES

Provides supertable structure and properties, such as column and tag counts, comments, owner, and security level. The same information can also be queried using [`SHOW STABLES`](./03-show.md#show-stables).

| #   | **Column Name** | **Data Type** | **Description** |
| --- | --------------- | ------------- | --- |
| 1   | `stable_name`   | VARCHAR(192)  | Supertable name |
| 2   | `db_name`       | VARCHAR(64)   | Name of the database containing the supertable |
| 3   | `create_time`   | TIMESTAMP     | Creation time |
| 4   | `columns`       | INT           | Number of columns |
| 5   | `tags`          | INT           | Number of tags. This column is a keyword and must be escaped with backticks in queries (for example, `` `tags` ``). |
| 6   | `last_update`   | TIMESTAMP     | Last update time |
| 7   | `table_comment` | VARCHAR(1024) | Table comment |
| 8   | `watermark`     | VARCHAR(64)   | Window close time. This column is a keyword and must be escaped with backticks in queries (for example, `` `watermark` ``). |
| 9   | `max_delay`     | VARCHAR(64)   | Maximum delay for pushing calculation results. This column is a keyword and must be escaped with backticks in queries (for example, `` `max_delay` ``). |
| 10  | `rollup`        | VARCHAR(128)  | Rollup aggregate function. This column is a keyword and must be escaped with backticks in queries (for example, `` `rollup` ``). |
| 11  | `uid`           | BIGINT        | Supertable UID |
| 12  | `isvirtual`     | BOOL          | Whether this is a virtual supertable |
| 13  | `keep`          | BIGINT        | Data retention duration |
| 14  | `owner`         | VARCHAR(24)   | Owner |
| 15  | `sec_level`     | TINYINT       | Security level |

## INS_STREAMS

Provides stream-processing task names, databases, status, `snode` distribution, and error information. The same information can also be queried using [`SHOW STREAMS`](./03-show.md#show-streams).

| #   | **Column Name** | **Data Type** | **Description** |
| --- | -------------- | -------------- | --- |
| 1   | `stream_name`  | VARCHAR(192)   | Stream name |
| 2   | `db_name`      | VARCHAR(64)    | Database containing the stream |
| 3   | `create_time`  | TIMESTAMP      | Creation time |
| 4   | `stream_id`    | VARCHAR(19)    | Stream ID |
| 5   | `sql`          | VARCHAR(49152) | SQL used to create the stream |
| 6   | `status`       | VARCHAR(20)    | Current status |
| 7   | `snodeLeader`  | INT            | Leader `snode` |
| 8   | `snodeReplica` | INT            | Replica `snode` |
| 9   | `message`      | VARCHAR(256)   | Status or error information |

## INS_STREAM_RECALCULATES

Provides the time range, progress, and IDs of stream recalculation tasks.

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ------------- | ------------ | --- |
| 1   | `stream_name` | VARCHAR(192) | Stream name |
| 2   | `stream_id`   | VARCHAR(19)  | Stream ID |
| 3   | `recalc_id`   | VARCHAR(19)  | Recalculation ID |
| 4   | `start`       | TIMESTAMP    | start |
| 5   | `end`         | TIMESTAMP    | end |
| 6   | `progress`    | VARCHAR(20)  | Progress |

## INS_STREAM_TASKS

Provides deployment locations, types, status, and most recent update times for internal stream-processing tasks.

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ------------- | ------------ | --- |
| 1   | `stream_name` | VARCHAR(192) | Stream name |
| 2   | `stream_id`   | VARCHAR(19)  | Stream ID |
| 3   | `task_id`     | VARCHAR(19)  | Task ID |
| 4   | `type`        | VARCHAR(20)  | Type |
| 5   | `serious_id`  | VARCHAR(19)  | serious_id |
| 6   | `deploy_id`   | INT          | deploy_id |
| 7   | `node_type`   | VARCHAR(10)  | node_type |
| 8   | `node_id`     | INT          | node_id |
| 9   | `task_idx`    | INT          | task_idx |
| 10  | `status`      | VARCHAR(20)  | Current status |
| 11  | `start_time`  | TIMESTAMP    | Start time |
| 12  | `last_update` | TIMESTAMP    | Most recent update time |
| 13  | `extra_info`  | VARCHAR(64)  | extra_info |
| 14  | `message`     | VARCHAR(256) | Information |

## INS_SUBSCRIPTIONS

Provides topic subscription relationships, including consumer groups, assigned `vgroup`s, consumption progress, and consumed row counts. The same information can also be queried using [`SHOW SUBSCRIPTIONS`](./03-show.md#show-subscriptions).

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ---------------- | ----------- | --- |
| 1   | `topic_name`     | BINARY(205) | Subscribed topic |
| 2   | `consumer_group` | BINARY(193) | Subscriber consumer group |
| 3   | `vgroup_id`      | INT         | `vgroup` ID assigned to the consumer |
| 4   | `consumer_id`    | BINARY(32)  | Unique consumer ID |
| 5   | `user`           | BINARY(24)  | Username used by the consumer to log in |
| 6   | `fqdn`           | BINARY(128) | FQDN of the machine hosting the consumer |
| 7   | `offset`         | BINARY(64)  | Consumer progress |
| 8   | `rows`           | BIGINT      | Number of rows consumed |

## INS_TABLES

Provides the structure and properties of regular tables and subtables, such as their supertable, `vgroup`, TTL, and table type. The same information can also be queried using [`SHOW TABLES`](./03-show.md#show-tables).

| #   | **Column Name** | **Data Type** | **Description** |
| --- | --------------- | ------------- | --- |
| 1   | `table_name`    | VARCHAR(192)  | Table name |
| 2   | `db_name`       | VARCHAR(64)   | Database name |
| 3   | `create_time`   | TIMESTAMP     | Creation time |
| 4   | `columns`       | INT           | Number of columns |
| 5   | `stable_name`   | VARCHAR(192)  | Name of the associated supertable |
| 6   | `uid`           | BIGINT        | Table ID |
| 7   | `vgroup_id`     | INT           | vgroup id |
| 8   | `ttl`           | INT           | Table lifecycle. This column is a keyword and must be escaped with backticks in queries (for example, `` `ttl` ``). |
| 9   | `table_comment` | VARCHAR(1024) | Table comment |
| 10  | `type`          | VARCHAR(21)   | Table type |

## INS_TABLE_FIXED_DISTRIBUTED

Provides statistics about table data-block distribution and compression for analyzing data skew and storage characteristics. The same information can also be queried using [`SHOW TABLE DISTRIBUTED`](./03-show.md#show-table-distributed).

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ------------------- | ------------ | --- |
| 1   | `db_name`           | VARCHAR(64)  | Database name |
| 2   | `table_name`        | VARCHAR(192) | Table name |
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

Provides table tag names, types, and values for tag-based retrieval and metadata verification. The same information can also be queried using [`SHOW TAGS`](./03-show.md#show-tags).

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ------------- | -------------- | --- |
| 1   | `table_name`  | VARCHAR(192)   | Table name |
| 2   | `db_name`     | VARCHAR(64)    | Name of the database containing the table |
| 3   | `stable_name` | VARCHAR(192)   | Name of the associated supertable |
| 4   | `tag_name`    | VARCHAR(64)    | Tag name |
| 5   | `tag_type`    | VARCHAR(32)    | Tag type |
| 6   | `tag_value`   | VARCHAR(16384) | Tag value |

## INS_TOKENS

Provides user access-token names, users, providers, enablement status, and expiration times. The same information can also be queried using [`SHOW TOKENS`](./03-show.md#show-tokens).

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ------------- | ------------- | --- |
| 1   | `name`        | VARCHAR(32)       | Name |
| 2   | `user`        | VARCHAR(24)   | Username |
| 3 | `provider` | VARCHAR(64) | Token provider |
| 4   | `enable`      | TINYINT       | Whether enabled |
| 5   | `create_time` | TIMESTAMP     | Creation time |
| 6 | `expire_time` | TIMESTAMP | Expiration time |
| 7 | `extra_info` | VARCHAR(1024) | Additional information |

## INS_TOPICS

Provides the names, databases, creation SQL, and schemas of created data-subscription topics. The same information can also be queried using [`SHOW TOPICS`](./03-show.md#show-topics).

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ------------- | ------------- | --- |
| 1   | `topic_name`  | BINARY(192)   | Topic name |
| 2   | `db_name`     | BINARY(64)    | Database associated with the topic |
| 3   | `create_time` | TIMESTAMP     | Topic creation time |
| 4   | `sql`         | BINARY(2048)  | SQL statement used to create the topic |
| 5   | `schema`      | BINARY(65517) | Topic schema |

## INS_TRANSACTIONS

Provides the stages, operation objects, failure counts, and latest execution information of currently executing metadata transactions. The same information can also be queried using [`SHOW TRANSACTIONS`](./03-show.md#show-transactions).

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ------------------ | ------------ | --- |
| 1   | `id`               | BIGINT       | ID |
| 2   | `create_time`      | TIMESTAMP    | Creation time |
| 3   | `stage`            | VARCHAR(12)  | Current stage |
| 4   | `oper`             | VARCHAR(22)  | Operation |
| 5   | `db`               | VARCHAR(64)  | Associated database |
| 6   | `stable`           | VARCHAR(192) | Associated supertable |
| 7   | `killable`         | VARCHAR(10)  | Whether terminable |
| 8   | `failed_times`     | INT          | Failure count |
| 9   | `last_exec_time`   | TIMESTAMP    | Last execution time |
| 10  | `last_action_info` | VARCHAR(511) | Details of the last execution failure |
| 11  | `type`             | VARCHAR(10)  | Type |

## INS_TRANSACTION_DETAILS

Provides the object type, target, result, and details of each action in a metadata transaction. The same information can also be queried using [`SHOW TRANSACTION`](./03-show.md#show-transactions).

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ---------------- | ------------ | --- |
| 1   | `transaction_id` | INT          | transaction_id |
| 2   | `action`         | VARCHAR(30)  | action |
| 3   | `obj_type`       | VARCHAR(40)  | obj_type |
| 4   | `result`         | VARCHAR(100) | result |
| 5   | `target`         | VARCHAR(300) | target |
| 6   | `detail`         | VARCHAR(100) | detail |

## INS_TRANSACTION_LOGS

Provides the history of completed metadata transactions, including creator, status, type, and completion time. The same information can also be queried using [`SHOW TRANSACTION LOGS`](./03-show.md#show-transaction-logs).

| #   | **Column Name** | **Data Type** | **Description** |
| --- | --------------- | ------------ | --- |
| 1   | `id`            | BIGINT       | ID |
| 2   | `create_user`   | VARCHAR(24)  | Creating user |
| 3   | `create_time`   | TIMESTAMP    | Creation time |
| 4   | `complete_time` | TIMESTAMP    | Completion time |
| 5   | `status`        | VARCHAR(12)  | Current status |
| 6   | `comment`       | VARCHAR(128) | Comment |
| 7   | `type`          | VARCHAR(10)  | Type |

## INS_TRANSACTION_ORPHANS

Provides orphan-transaction detection results, including the associated `vgroup`, first and most recent detection times, and report count. The same information can also be queried using [`SHOW TRANSACTION ORPHANS`](./03-show.md#show-transaction-orphans).

| #   | **Column Name** | **Data Type** | **Description** |
| --- | -------------- | --------- | --- |
| 1   | `id`           | BIGINT    | ID |
| 2   | `vgroup_id`    | INT       | `vgroup` ID |
| 3   | `first_seen`   | TIMESTAMP | First detected time |
| 4   | `last_seen`    | TIMESTAMP | Most recently detected time |
| 5   | `report_count` | INT       | Report count |

## INS_TSMAS

Provides time-series SMA (TSMA) names, source and target tables, aggregation intervals, and creation SQL. The same information can also be queried using [`SHOW TSMAS`](./03-show.md#show-tsmas).

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ------------- | ------------- | --- |
| 1   | `tsma_name`   | VARCHAR(192)  | tsma_name |
| 2   | `db_name`     | VARCHAR(64)   | Database name |
| 3   | `table_name`  | VARCHAR(192)  | Table name |
| 4   | `target_db`   | VARCHAR(64)   | target_db |
| 5   | `target_stb`  | VARCHAR(192)  | target_stb |
| 6   | `stream_name` | VARCHAR(64)   | Stream name |
| 7   | `create_time` | TIMESTAMP     | Creation time |
| 8   | `interval`    | VARCHAR(64)   | interval |
| 9   | `create_sql`  | VARCHAR(2048) | create_sql |
| 10  | `func_list`   | VARCHAR(2048) | func_list |

## INS_USERS

Provides basic system-user attributes, such as whether the user is a superuser, enabled, or permitted to view system information. The same information can also be queried using [`SHOW USERS`](./03-show.md#show-users). Users with `SYSINFO` set to 0 cannot view this table.

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ------------------ | ------------- | --- |
| 1   | `name`             | VARCHAR(24)   | Username |
| 2   | `super`            | TINYINT       | Whether the user is a superuser: `1` for yes, `0` for no |
| 3   | `enable`           | TINYINT       | Whether the user is enabled: `1` for yes, `0` for no |
| 4   | `sysinfo`          | TINYINT       | Whether the user can view system information: `1` for yes, `0` for no |
| 5   | `createdb`         | TINYINT       | Whether the user can create databases |
| 6   | `create_time`      | TIMESTAMP     | Creation time |
| 7   | `totp`             | TINYINT       | Whether TOTP is enabled |
| 8   | `allowed_host`     | VARCHAR(48)   | IP allowlist |
| 9   | `allowed_datetime` | VARCHAR(48)   | Permitted login time window |
| 10  | `roles`            | VARCHAR(2048) | Granted roles |
| 11  | `sec_levels`       | BINARY(5)     | Security-level range |

## INS_USERS_FULL

Provides complete user configuration, including password policies, session limits, host and time allowlists, and roles. The same information can also be queried using [`SHOW USERS FULL`](./03-show.md#show-users). Users with `SYSINFO` set to 0 cannot view this table.

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ----------------------- | ------------- | --- |
| 1   | `name`                  | VARCHAR(24)   | Name |
| 2   | `super`                 | TINYINT       | super |
| 3   | `enable`                | TINYINT       | Whether enabled |
| 4   | `sysinfo`               | TINYINT       | sysinfo |
| 5   | `createdb`              | TINYINT       | createdb |
| 6   | `create_time`           | TIMESTAMP     | Creation time |
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

Provides user privilege details, including privilege type, scope, database and table range, and column privileges. The same information can also be queried using [`SHOW USER PRIVILEGES`](./03-show.md#show-user-privileges). Users with `SYSINFO` set to 0 cannot view this table.

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ------------- | ------------ | --- |
| 1   | `user_name`   | VARCHAR(24)  | Username |
| 2   | `priv_type`   | VARCHAR(128) | Privilege type |
| 3   | `priv_scope`  | VARCHAR(32)  | Privilege scope |
| 4   | `db_name`     | VARCHAR(65)  | Database name |
| 5   | `table_name`  | VARCHAR(193) | Table name |
| 6   | `condition`   | VARCHAR(48)  | Subtable privilege filter condition |
| 7   | `notes`       | VARCHAR(64)  | Notes |
| 8   | `columns`     | VARCHAR(12)  | Column privilege information |
| 9   | `update_time` | VARCHAR(40)  | Update time |

## INS_VGROUPS

Provides member distribution, replica status, cache information, and WAL-related positions for each `vgroup`. The same information can also be queried using [`SHOW VGROUPS`](./03-show.md#show-vgroups).

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ---------------------- | ------------ | --- |
| 1   | `vgroup_id`            | INT          | vgroup id |
| 2   | `db_name`              | VARCHAR(64)  | Database name |
| 3   | `tables`               | INT          | Number of tables in this `vgroup`. This column is a keyword and must be escaped with backticks in queries (for example, `` `tables` ``). |
| 4   | `v1_dnode`             | SMALLINT     | ID of the `dnode` hosting the first member |
| 5   | `v1_status`            | VARCHAR(9)   | Status of the first member |
| 6   | `v1_applied/committed` | VARCHAR(100) | Applied/committed position of the first member |
| 7   | `v2_dnode`             | SMALLINT     | ID of the `dnode` hosting the second member |
| 8   | `v2_status`            | VARCHAR(9)   | Status of the second member |
| 9   | `v2_applied/committed` | VARCHAR(100) | Applied/committed position of the second member |
| 10  | `v3_dnode`             | SMALLINT     | ID of the `dnode` hosting the third member |
| 11  | `v3_status`            | VARCHAR(9)   | Status of the third member |
| 12  | `v3_applied/committed` | VARCHAR(100) | Applied/committed position of the third member |
| 13  | `v4_dnode`             | SMALLINT     | ID of the `dnode` hosting the fourth member |
| 14  | `v4_status`            | VARCHAR(9)   | Status of the fourth member |
| 15  | `v4_applied/committed` | VARCHAR(100) | Applied/committed position of the fourth member |
| 16  | `is_ready`             | BOOL         | Whether ready |
| 17  | `cacheload`            | BIGINT       | Cache load |
| 18  | `cacheelements`        | INT          | Number of cache elements |
| 19  | `tsma`                 | TINYINT      | Whether this `vgroup` is dedicated to Time-range-wise SMA: 1 for yes, 0 for no |
| 20  | `mount_vgroup_id`      | INT          | Mounted `vgroup` ID |
| 21  | `keep_version`         | BIGINT       | WAL logs in this `vgroup` at or above `keep_version` are not automatically deleted |
| 22  | `keep_version_time`    | TIMESTAMP    | Time when `keep_version` was last modified for this `vgroup` |
| 23  | `compact_start_time`   | TIMESTAMP    | Compaction start time |

## INS_VIEWS

Provides view names, databases, definition SQL, parameters, and target tables. The same information can also be queried using [`SHOW VIEWS`](./03-show.md#show-views).

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ---------------- | ------------- | --- |
| 1   | `view_name`      | VARCHAR(192)  | View name |
| 2   | `db_name`        | VARCHAR(64)   | Database name |
| 3   | `effective_user` | VARCHAR(24)   | effective_user |
| 4   | `create_time`    | TIMESTAMP     | Creation time |
| 5   | `type`           | VARCHAR(128)  | Type |
| 6   | `query_sql`      | VARCHAR(2048) | query_sql |
| 7   | `parameters`     | VARCHAR(2048) | parameters |
| 8   | `default_values` | VARCHAR(2048) | default_values |
| 9   | `target_table`   | VARCHAR(192)  | target_table |
| 10  | `column_list`    | VARCHAR(2048) | column_list |

## INS_VIRTUAL_CHILD_COLUMNS

Provides references between virtual-subtable columns and source-table columns, including reference versions.

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ------------- | ------------ | --- |
| 1   | `table_name`  | VARCHAR(192) | Table name |
| 2   | `stable_name` | VARCHAR(192) | Supertable name |
| 3   | `db_name`     | VARCHAR(64)  | Database name |
| 4   | `col_name`    | VARCHAR(64)  | col_name |
| 5   | `uid`         | BIGINT       | UID |
| 6   | `col_id`      | INT          | col_id |
| 7   | `col_source`  | VARCHAR(258) | col_source |
| 8   | `vgroup_id`   | INT          | `vgroup` ID |
| 9   | `ref_version` | INT          | ref_version |
| 10  | `col_type`    | INT          | col_type |

## INS_VIRTUAL_TABLES_REFERENCING

Provides references from virtual-table columns to source-table columns, including validation error codes and messages. The same information can also be queried using [`SHOW VTABLE VALIDATE`](./03-show.md#show-vtable-validate). Users with `SYSINFO` set to 0 cannot view this table.

| #   | **Column Name** | **Data Type** | **Description** |
| --- | --------------------- | ------------ | --- |
| 1   | `virtual_db_name`     | VARCHAR(64)  | virtual_db_name |
| 2   | `virtual_stable_name` | VARCHAR(192) | virtual_stable_name |
| 3   | `virtual_table_name`  | VARCHAR(192) | virtual_table_name |
| 4   | `virtual_col_name`    | VARCHAR(64)  | virtual_col_name |
| 5   | `src_db_name`         | VARCHAR(64)  | src_db_name |
| 6   | `src_table_name`      | VARCHAR(192) | src_table_name |
| 7   | `src_column_name`     | VARCHAR(64)  | src_column_name |
| 8   | `type`                | INT          | Type |
| 9   | `err_code`            | BIGINT       | err_code |
| 10  | `err_msg`             | VARCHAR(512) | err_msg |

## INS_VNODES

Provides each `vnode`'s `dnode`, database, role status, and recovery progress. The same information can also be queried using [`SHOW VNODES`](./03-show.md#show-vnodes). Users with `SYSINFO` set to 0 cannot view this table.

| #   | **Column Name** | **Data Type** | **Description** |
| --- | --------------------- | ----------- | --- |
| 1   | `dnode_id`            | INT         | `dnode` ID |
| 2   | `vgroup_id`           | INT         | `vgroup` ID |
| 3   | `db_name`             | BINARY(64)  | Database name |
| 4   | `status`              | VARCHAR(9)  | `vnode` status |
| 5   | `role_time`           | TIMESTAMP   | Most recent election time |
| 6   | `start_time`          | TIMESTAMP   | `vnode` start time |
| 7   | `restored`            | BOOL        | Whether restored |
| 8   | `apply_finish_time`   | VARCHAR(18) | Recovery completion time |
| 9   | `unapplied`           | INT         | Number of unapplied requests |
| 10  | `buffer_segment_used` | BIGINT      | Bytes used in the buffer segment |
| 11  | `buffer_segment_size` | BIGINT      | Total buffer-segment bytes |

## INS_VSTABLE_INHERITS

Provides inheritance relationships between virtual supertables, including parent and child supertable names and UIDs. The same information can also be queried using [`SHOW VTABLE INHERITS`](./03-show.md#show-vtable-inherits).

| #   | **Column Name** | **Data Type** | **Description** |
| --- | -------------------- | ------------ | --- |
| 1   | `db_name`            | VARCHAR(64)  | Database name |
| 2   | `parent_stable_name` | VARCHAR(192) | Parent supertable name |
| 3   | `parent_uid`         | BIGINT       | Parent supertable UID |
| 4   | `child_stable_name`  | VARCHAR(192) | Child supertable name |
| 5   | `child_uid`          | BIGINT       | Child supertable UID |
| 6   | `create_time`        | TIMESTAMP    | Creation time |

## INS_XNODES

Provides the address, status, creation time, and update time of Xnode data-ingestion nodes. The same information can also be queried using [`SHOW XNODES`](./03-show.md#show-xnodes). Users with `SYSINFO` set to 0 cannot view this table.

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ------------- | ------------ | --- |
| 1   | `id`          | INT          | ID |
| 2   | `url`         | VARCHAR(256) | URL |
| 3   | `status`      | VARCHAR(16)  | Current status |
| 4   | `create_time` | TIMESTAMP    | Creation time |
| 5   | `update_time` | TIMESTAMP    | Update time |

## INS_XNODE_AGENTS

Provides Xnode Agent names, tokens, status, creation times, and update times. The same information can also be queried using [`SHOW XNODE AGENTS`](./03-show.md#show-xnode-agents). Users with `SYSINFO` set to 0 cannot view this table.

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ------------- | ------------ | --- |
| 1   | `id`          | INT          | ID |
| 2   | `name`        | VARCHAR(64)  | Name |
| 3   | `token`       | VARCHAR(512) | Token |
| 4   | `status`      | VARCHAR(16)  | Current status |
| 5   | `create_time` | TIMESTAMP    | Creation time |
| 6   | `update_time` | TIMESTAMP    | Update time |

## INS_XNODE_JOBS

Provides the configuration, parent task, status, and failure reason of Xnode job shards. The same information can also be queried using [`SHOW XNODE JOBS`](./03-show.md#show-xnode-jobs). Users with `SYSINFO` set to 0 cannot view this table.

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ------------- | ------------- | --- |
| 1   | `id`          | INT           | ID |
| 2   | `task_id`     | INT           | Task ID |
| 3   | `config`      | VARCHAR(48)   | Configuration |
| 4   | `via`         | INT           | Transit node |
| 5   | `xnode_id`    | INT           | Xnode ID |
| 6   | `status`      | VARCHAR(16)   | Current status |
| 7   | `reason`      | VARCHAR(1024) | Reason |
| 8   | `create_time` | TIMESTAMP     | Creation time |
| 9   | `update_time` | TIMESTAMP     | Update time |

## INS_XNODE_TASKS

Provides the source, target, status, labels, and creator of Xnode data-ingestion tasks. The same information can also be queried using [`SHOW XNODE TASKS`](./03-show.md#show-xnode-tasks). Users with `SYSINFO` set to 0 cannot view this table.

| #   | **Column Name** | **Data Type** | **Description** |
| --- | ------------- | ------------- | --- |
| 1   | `id`          | INT           | ID |
| 2   | `name`        | VARCHAR(64)   | Name |
| 3   | `from`        | VARCHAR(4096) | Source |
| 4   | `to`          | VARCHAR(2048) | Target |
| 5   | `parser`      | VARCHAR(48)   | Parser configuration |
| 6   | `via`         | INT           | Transit node |
| 7   | `xnode_id`    | INT           | Xnode ID |
| 8   | `status`      | VARCHAR(16)   | Current status |
| 9   | `reason`      | VARCHAR(1024) | Reason |
| 10  | `created_by`  | VARCHAR(24)   | Creator |
| 11  | `labels`      | VARCHAR(4096) | Labels |
| 12  | `create_time` | TIMESTAMP     | Creation time |
| 13  | `update_time` | TIMESTAMP     | Update time |
