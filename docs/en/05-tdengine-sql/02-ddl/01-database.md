---
title: Databases
---

## Create Database

```sql
CREATE DATABASE [IF NOT EXISTS] db_name [database_options]

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
  | SS_KEEPLOCAL value
  | SS_CHUNKPAGES value
  | SS_COMPACT value
  | COMPACT_INTERVAL value
  | COMPACT_TIME_RANGE value
  | COMPACT_TIME_OFFSET value
  | ENCRYPT_ALGORITHM value
  | IS_AUDIT {0 | 1}
  | ALLOW_DROP {0 | 1}
  | SECURE_DELETE {0 | 1}
  | SECURITY_LEVEL value
}
```

### Parameter Description

#### VGROUPS

The number of initial vgroups in the database.

#### PRECISION

The timestamp precision of the database. ms for milliseconds, us for microseconds, ns for nanoseconds, default is ms.

#### REPLICA

Indicates the number of database replicas, which can be 1, 2, or 3, default is 1; 2 is only available in the enterprise version 3.3.0.0 and later. In a cluster, the number of replicas must be less than or equal to the number of DNODEs. `REPLICAS` is an alias of `REPLICA`. The following restrictions apply:

- A single-replica database can be changed to a double-replica database, but changing from double replicas to other numbers of replicas, or from three replicas to double replicas is not supported.

#### BUFFER

The size of the memory pool for writing into a VNODE, in MB, default is 256, minimum is 3, maximum is 16384.

#### PAGES

The number of cache pages in a VNODE's metadata storage engine, default is 256, minimum 64. A VNODE's metadata storage occupies PAGESIZE * PAGES, which by default is 1MB of memory.

#### PAGESIZE

The page size of a VNODE's metadata storage engine, in KB, default is 4 KB. Range is 1 to 16384, i.e., 1 KB to 16 MB.

#### CACHEMODEL

Indicates whether to cache the latest data of subtables in memory. Default is none.

- none: Indicates no caching.
- last_row: Indicates caching the latest row of data of subtables. This will significantly improve the performance of the LAST_ROW function.
- last_value: Indicates caching the latest non-NULL value of each column of subtables. This will significantly improve the performance of the LAST function without special effects (WHERE, ORDER BY, GROUP BY, INTERVAL).
- both: Indicates enabling caching of both the latest row and column.

:::note
Switching CacheModel values back and forth may cause inaccurate results for last/last_row queries, please operate with caution. It is recommended to keep it turned on.
:::

#### CACHESIZE

The size of memory used for caching the latest data of subtables in each vnode. Default is 1, range is [1, 65536], in MB.

#### CACHESHARDBITS

The number of shard bits for the last-value LRU cache, which controls the internal lock granularity for concurrent cache access. Default is -1 (auto-calculated), range is [-1, 19].

- The actual number of shards equals `2^CACHESHARDBITS`. For example, CACHESHARDBITS=3 means 8 shards, CACHESHARDBITS=6 means 64 shards.
- When set to -1, the system automatically calculates the shard bits based on CACHESIZE using the following rules:
  - Each shard is at least 512 KB, so the theoretical maximum number of shards = `CACHESIZE / 512KB`.
  - The shard bits equal `floor(log₂(theoretical maximum shards))`, with an upper limit of 6 (i.e., at most 64 shards).
  - When CACHESIZE < 512 KB, the shard bits is 0, resulting in a single shard.
  - Auto-calculation examples:

  | CACHESIZE | Theoretical max shards | Shard bits | Actual shards |
  |-----------|------------------------|------------|---------------|
  | 1 MB      | 2                      | 1          | 2             |
  | 4 MB      | 8                      | 3          | 8             |
  | 32 MB     | 64                     | 6          | 64            |
  | 256 MB    | 512                    | 6 (capped) | 64            |

- More shards reduce lock contention during concurrent cache writes, which is suitable for high-concurrency scenarios. However, too many shards may increase memory management overhead.
- **Warning:** Modifying CACHESHARDBITS immediately invalidates all last-value cache entries in all vnodes of the database. The cached data will be reloaded from disk on subsequent queries, which may temporarily increase query latency.

#### COMP

Indicates the compression flag for database files, default value is 2, range is [0, 2].

- 0: Indicates no compression.
- 1: Indicates first-stage compression.
- 2: Indicates two-stage compression.

#### DURATION

The time span for storing data in data files.default value is 10d, range [60m, 3650d] Can use unit-specified formats, such as DURATION 100h, DURATION 10d, etc., supports m (minutes), h (hours), and d (days) three units. If no time unit is added, the default unit is days, e.g., DURATION 50 means 50 days.

#### MAXROWS

The maximum number of records in a file block, default is 4096.

#### MINROWS

The minimum number of records in a file block, default is 100.

#### KEEP

Indicates the number of days data files are kept, default value is 3650, range [1, 365000], and must be greater than or equal to 3 times the DURATION parameter value. The database will automatically delete data that has been saved for longer than the KEEP value to free up storage space. KEEP can use unit-specified formats, such as KEEP 100h, KEEP 10d, etc., supports m (minutes), h (hours), and d (days) three units. It can also be written without a unit, like KEEP 50, where the default unit is days. The enterprise version supports [multi-tier storage](../../12-operations-and-tooling/02-operations/01-planning.md#multi-tier-storage), thus, multiple retention times can be set (multiple separated by commas, up to 3, satisfying keep0 \<= keep1 \<= keep2, such as KEEP 100h,100d,3650d); the community version does not support multi-tier storage (even if multiple retention times are configured, it will not take effect, KEEP will take the longest retention time). For more information about how retention interacts with the primary timestamp, see [Insert Statements](../03-data-write/01-insert.md).

#### KEEP_TIME_OFFSET

Effective from version 3.2.0.0. The delay execution time for deleting or migrating data that has been saved for longer than the KEEP value, default value is 0 (hours). After the data file's save time exceeds KEEP, the deletion or migration operation will not be executed immediately, but will wait an additional interval specified by this parameter, to avoid peak business periods.

#### RETENTIONS

Specifies aggregation intervals and retention periods for database data, and is used to retain multiple levels of aggregated results. This option is related to downsampling storage. For details, see [Downsampling Storage](../05-materialized-agg/02-rsma.md).

#### SCHEMALESS

Specifies whether schemaless writes are allowed in the database.

- 0: Not allowed.
- 1: Allowed.

#### STT_TRIGGER

Indicates the number of file merges triggered by disk files. For scenarios with few tables and high-frequency writing, this parameter is recommended to use the default configuration; for scenarios with many tables and low-frequency writing, this parameter is recommended to be set to a larger value.

#### SINGLE_STABLE

Indicates whether only one supertable can be created in this database, used in cases where the supertable has a very large number of columns.

- 0: Indicates that multiple supertables can be created.
- 1: Indicates that only one supertable can be created.

#### TABLE_PREFIX

When it is a positive value, it ignores the specified length prefix of the table name when deciding which vgroup to allocate a table to; when it is a negative value, it only uses the specified length prefix of the table name when deciding which vgroup to allocate a table to; for example, assuming the table name is "v30001", when TSDB_PREFIX = 2, use "0001" to decide which vgroup to allocate to, when TSDB_PREFIX = -2, use "v3" to decide which vgroup to allocate to.

#### TABLE_SUFFIX

When it is a positive value, it ignores the specified length suffix of the table name when deciding which vgroup to allocate a table to; when it is a negative value, it only uses the specified length suffix of the table name when deciding which vgroup to allocate a table to; for example, assuming the table name is "v30001", when TSDB_SUFFIX = 2, use "v300" to decide which vgroup to allocate to, when TSDB_SUFFIX = -2, use "01" to decide which vgroup to allocate to.

#### TSDB_PAGESIZE

The page size of a VNODE's time-series data storage engine, in KB, default is 4 KB. Range is 1 to 16384, i.e., 1 KB to 16 MB.

#### DNODES

Specifies the list of DNODEs where the VNODE is located, such as '1,2,3', separated by commas and without spaces between characters, only supported in the enterprise version.

#### WAL_LEVEL

WAL level, default is 1.

- 1: Write WAL, but do not perform fsync.
- 2: Write WAL and perform fsync.

#### WAL_FSYNC_PERIOD

When the WAL_LEVEL parameter is set to 2, it is used to set the disk writing period. Default is 3000, in milliseconds. Minimum is 0, meaning immediate disk writing upon each write; maximum is 180000, i.e., three minutes.

#### WAL_RETENTION_PERIOD

For data subscription consumption, the maximum duration strategy for additional retention of WAL log files. WAL log cleaning is not affected by the consumption status of subscription clients. In seconds. Default is 3600, meaning WAL retains the most recent 3600 seconds of data, please modify this parameter to an appropriate value according to the needs of data subscription.

#### WAL_RETENTION_SIZE

For data subscription consumption, the maximum cumulative size strategy for additional retention of WAL log files. In KB. Default is 0, meaning there is no upper limit on cumulative size.

#### WAL_ROLL_PERIOD

The WAL file roll period. In most cases, the default setting is sufficient.

#### WAL_SEGMENT_SIZE

The size of a single WAL file segment. In most cases, the default setting is sufficient.

#### SS_KEEPLOCAL

When shared storage is enabled, data will be kept local at least this duration before being migrated to shared storage, only available in the enterprise version 3.3.7.0 and later. Minimum is 1 day, maximum is 36500 days, default is 365 days, and it must be greater than or equal to 3 times `DURATION`.

#### SS_CHUNKPAGES

When shared storage is enabled, data files larger than this size will be migrated to shared storage, only available in the enterprise version 3.3.7.0 and later. Minimum is 131072, maximum is 1048576, default is 131072. The unit is TSDB page, which is typically 4 KB. This option can only be specified when creating the database and cannot be altered later.

#### SS_COMPACT

When shared storage is enabled, if set to 1, a file will be compacted before its first migration; if set to 0, compaction is skipped. Only available in the enterprise version 3.3.7.0 and later. The default value is 1.

:::note

The following shared-storage and automatic compaction parameters are available in TDengine Enterprise only.

:::

#### COMPACT_INTERVAL

Interval at which to trigger automatic database compaction. The default value is 0, which disables automatic database compaction. To enable automatic database compaction, specify a value between 10m and `KEEP2`. The time unit of the value can be minutes (m), hours (h), or days (d), and the default unit is days.

- Note that time slices start from 1970-01-01T00:00:00Z.
- Automatic database compaction is not triggered when an existing compaction task is already running on the database.

#### COMPACT_TIME_RANGE

Time range for automatic compact tasks. The default value is `0, 0`, which indicates the range from `-KEEP2` to `-DURATION`. You can specify a custom time range starting at or after `-KEEP2` and ending at or before `-DURATION`. The time unit of the values in this range can be minutes (m), hours (h), or days (d), and the default unit is days.

For example, `-300, -200` would compact data between 300 and 200 days in the past each time automatic compaction is triggered. If the duration parameter of the database is the default 10 days, `-300, -5` would return an error because the second value (5 days in the past) is more recent than the value of `-DURATION` (10 days in the past).

Note that these values are negative numbers, indicating that the time range to be compacted is in the past.

#### COMPACT_TIME_OFFSET

Time offset relative to local time at which to trigger automatic database compaction. The default value is 0. You can enter an offset between 0 and 23 to trigger compaction after the specified number of hours.

For example, if `COMPACT_INTERVAL` is `1d` and `COMPACT_TIME_OFFSET` is `0`, automatic compact is triggered at 00:00 every day. If `COMPACT_TIME_OFFSET` is `2`, automatic compact is triggered at 02:00 every day.

#### ENCRYPT_ALGORITHM

Specifies the database encryption algorithm. This option is related to data-at-rest security. For details, see [Data Security](../../11-security-guide/06-data-security.md#create-encrypted-database).

#### IS_AUDIT

Specifies whether the database is created as an audit database.

- 0: Normal database.
- 1: Audit database.

Audit databases have additional parameter constraints. For details, see [Audit and Compliance](../../11-security-guide/07-audit-and-compliance.md#create-audit-database).

#### ALLOW_DROP

Specifies whether the database can be dropped. This option is primarily used for security-sensitive scenarios such as audit databases.

- 0: Dropping the database is not allowed.
- 1: Dropping the database is allowed.

#### SECURE_DELETE

Specifies whether secure delete is enabled. Valid values are 0 (default) and 1.

- 0: Delete operations write a delete marker, but data blocks on disk are not physically overwritten immediately.
- 1: In addition to writing the delete marker, the corresponding ranges in on-disk DATA/STT files are physically overwritten to reduce the risk of recovering deleted data directly from the filesystem.

This can be set through `CREATE DATABASE` or `ALTER DATABASE`; a single `DELETE` statement can also append the `SECURE_DELETE` keyword. For behavior, limitations, and examples, see [Data Security · Secure Delete](../../11-security-guide/06-data-security.md#secure-delete).

#### SECURITY_LEVEL

Specifies the database security level used by MAC. For syntax and rules, see [Mandatory Access Control (MAC)](../07-user-and-privilege/02-grant.md#mandatory-access-control-mac).

### Database Creation Example

```sql
create database if not exists db vgroups 10 buffer 10
```

The above example creates a database named db with 10 vgroups, where each vnode is allocated 10MB of write buffer.

### Using the Database

```sql
USE db_name;
```

Use or switch the current database. This statement is invalid over REST connections.

## Delete Database

```sql
DROP DATABASE [IF EXISTS] db_name
```

Deletes the database. All tables contained in the Database will be deleted, and all vgroups of that database will also be destroyed, so use with caution!

## Modify Database Parameters

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
  | MAXROWS value
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

### Modify REPLICA with PARALLEL

When changing the number of replicas for a database, you can optionally specify the `PARALLEL` parameter to control the concurrency of the replica change operation:

```sql
ALTER DATABASE db_name REPLICA value [PARALLEL parallel_value];
```

- `value`: The target replica count (1, 2, or 3).
- `parallel_value`: Controls how many vgroups can perform replica changes concurrently. Default is 0 (unlimited).
  - `0`: Unlimited concurrency. All vgroups perform replica changes simultaneously. This is the fastest but may consume more resources.
  - `1`: Serial execution. Only one vgroup performs replica change at a time. This is the slowest but most resource-friendly.
  - `N` (where N > 1): Limited concurrency. At most N vgroups perform replica changes concurrently. This provides a balance between speed and resource usage.

**Note:** The `PARALLEL` parameter only applies when modifying `REPLICA`. It cannot be used with other ALTER DATABASE options.

**Example:**

```sql
-- Change to 3 replicas with unlimited concurrency (default)
ALTER DATABASE db_name REPLICA 3;

-- Change to 3 replicas with serial execution (one vgroup at a time)
ALTER DATABASE db_name REPLICA 3 PARALLEL 1;

-- Change to 3 replicas with limited concurrency (max 2 vgroups at a time)
ALTER DATABASE db_name REPLICA 3 PARALLEL 2;
```

### Modify CACHESHARDBITS

```sql
ALTER DATABASE db_name CACHESHARDBITS value;
```

- `value` range is [-1, 19]. -1 means the system automatically calculates the shard bits based on CACHESIZE.
- The actual number of shards equals `2^value`. For example, value=3 corresponds to 8 shards, and value=6 corresponds to 64 shards.
- The change takes effect immediately, but **invalidates all last-value LRU cache entries** in every vnode of the database. Cached data will be reloaded from disk on subsequent queries, which may temporarily increase query latency.

### Modify CACHESIZE

The command to modify database parameters is simple, but the difficulty lies in determining whether a modification is needed and how to modify it. This section describes how to judge whether the cachesize is sufficient.

1. How to view `CACHESIZE`?

   ```sql
   SELECT * FROM INFORMATION_SCHEMA.INS_DATABASES;
   ```

   This displays the configured `CACHESIZE` value in MB.

2. How to view `cacheload`?

   ```sql
   SHOW db_name.VGROUPS;
   ```

   This displays `cacheload` in bytes.

3. Determine if `CACHESIZE` is sufficient.

If cacheload is very close to cachesize, then cachesize may be too small. If cacheload is significantly less than cachesize, then cachesize is sufficient. You can decide whether to modify cachesize based on this principle. The specific modification value can be determined based on the available system memory, whether to double it or increase it several times.

:::note
Other parameters are not supported for modification in version 3.0.0.0

:::

## View Database

### View all databases in the system

```sql
SHOW DATABASES;
```

### Display a database's creation statement

```sql
SHOW CREATE DATABASE db_name \G;
```

Commonly used for database migration. For an existing database, it returns its creation statement; executing this statement in another cluster will result in a Database with the exact same settings.

### View Database Parameters

```sql
SELECT * FROM INFORMATION_SCHEMA.INS_DATABASES WHERE NAME='db_name' \G;
```

Lists the configuration parameters of the specified database, displaying one parameter per line.

## Database Operations

### Delete Expired Data

```sql
TRIM DATABASE db_name;
```

Deletes expired data and reorganizes data according to the multi-level storage configuration.

### Delete Expired WAL

```sql
TRIM DATABASE db_name WAL;
```

Delete expired WAL logs. Using `trim wal` ignores the vgroup `keep_version` restriction.

### Flush Memory Data to Disk

```sql
FLUSH DATABASE db_name;
```

Flushes data in memory to disk. Executing this command before shutting down a node can avoid data replay after restart, speeding up the startup process.

### Adjust the Distribution of VNODEs in VGROUP

```sql
REDISTRIBUTE VGROUP vgroup_no DNODE dnode_id1 [DNODE dnode_id2] [DNODE dnode_id3]
```

Adjusts the distribution of vnodes in a vgroup according to the given list of dnodes. Since the maximum number of replicas is 3, a maximum of 3 dnodes can be entered.

### Automatically Adjust the Distribution of VNODEs in VGROUP

```sql
BALANCE VGROUP
```

Automatically adjusts the distribution of vnodes in all vgroups of the cluster, equivalent to performing data load balancing at the vnode level for the cluster.

### Automatically Re-elect VGROUP Leaders

```sql
BALANCE VGROUP LEADER
```

Triggers leader re-election in all vgroups of the cluster to rebalance load across nodes. This command is available in TDengine Enterprise only.

### Check Database Working Status

```sql
SHOW db_name.ALIVE;
```

Query the availability status of the database db_name, with return values of 0 (unavailable), 1 (fully available), or 2 (partially available, indicating that some VNODEs in the database are available while others are not).

### View DB Disk Usage

```sql
select * from  INFORMATION_SCHEMA.INS_DISK_USAGE where db_name = 'db_name'   
```

View the disk usage of each module in the DB.

```sql
SHOW db_name.disk_info;
```

View the compression ratio and disk usage of the database db_name

This command is essentially equivalent to `select sum(data1 + data2 + data3)/sum(raw_data), sum(data1 + data2 + data3) from information_schema.ins_disk_usage where db_name="dbname"`
