---
title: Databases
slug: /tdengine-reference/sql-manual/manage-databases
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
  | BUFFER value
  | PAGES value
  | PAGESIZE  value
  | CACHEMODEL {'none' | 'last_row' | 'last_value' | 'both'}
  | CACHESIZE value
  | COMP {0 | 1 | 2}
  | DURATION value
  | MAXROWS value
  | MINROWS value
  | KEEP value
  | KEEP_TIME_OFFSET value
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
  | SS_KEEPLOCAL value
  | SS_CHUNKPAGES value
  | SS_COMPACT value
}
```

### Parameter Description

- VGROUPS: The number of initial vgroups in the database.
- PRECISION: The timestamp precision of the database. ms for milliseconds, us for microseconds, ns for nanoseconds, default is ms.
- REPLICA: Indicates the number of database replicas, which can be 1, 2, or 3, default is 1; 2 is only available in the enterprise version 3.3.0.0 and later. In a cluster, the number of replicas must be less than or equal to the number of DNODEs. The following restrictions apply:
  - A single-replica database can be changed to a double-replica database, but changing from double replicas to other numbers of replicas, or from three replicas to double replicas is not supported.
- BUFFER: The size of the memory pool for writing into a VNODE, in MB, default is 256, minimum is 3, maximum is 16384.
- PAGES: The number of cache pages in a VNODE's metadata storage engine, default is 256, minimum 64. A VNODE's metadata storage occupies PAGESIZE * PAGES, which by default is 1MB of memory.
- PAGESIZE: The page size of a VNODE's metadata storage engine, in KB, default is 4 KB. Range is 1 to 16384, i.e., 1 KB to 16 MB.
- CACHEMODEL: Indicates whether to cache the latest data of subtables in memory. Default is none.
  - none: Indicates no caching.
  - last_row: Indicates caching the latest row of data of subtables. This will significantly improve the performance of the LAST_ROW function.
  - last_value: Indicates caching the latest non-NULL value of each column of subtables. This will significantly improve the performance of the LAST function without special effects (WHERE, ORDER BY, GROUP BY, INTERVAL).
  - both: Indicates enabling caching of both the latest row and column.
    Note: Switching CacheModel values back and forth may cause inaccurate results for last/last_row queries, please operate with caution. It is recommended to keep it turned on.
- CACHESIZE: The size of memory used for caching the latest data of subtables in each vnode. Default is 1, range is [1, 65536], in MB.
- COMP: Indicates the compression flag for database files, default value is 2, range is [0, 2].
  - 0: Indicates no compression.
  - 1: Indicates first-stage compression.
  - 2: Indicates two-stage compression.
- DURATION: The time span for storing data in data files.default value is 10d, range [60m, 3650d] Can use unit-specified formats, such as DURATION 100h, DURATION 10d, etc., supports m (minutes), h (hours), and d (days) three units. If no time unit is added, the default unit is days, e.g., DURATION 50 means 50 days.
- MAXROWS: The maximum number of records in a file block, default is 4096.
- MINROWS: The minimum number of records in a file block, default is 100.
- KEEP: Indicates the number of days data files are kept, default value is 3650, range [1, 365000], and must be greater than or equal to 3 times the DURATION parameter value. The database will automatically delete data that has been saved for longer than the KEEP value to free up storage space. KEEP can use unit-specified formats, such as KEEP 100h, KEEP 10d, etc., supports m (minutes), h (hours), and d (days) three units. It can also be written without a unit, like KEEP 50, where the default unit is days. The enterprise version supports multi-tier storage feature, thus, multiple retention times can be set (multiple separated by commas, up to 3, satisfying keep 0 \<= keep 1 \<= keep 2, such as KEEP 100h,100d,3650d); the community version does not support multi-tier storage feature (even if multiple retention times are configured, it will not take effect, KEEP will take the longest retention time).
- KEEP_TIME_OFFSET: Effective from version 3.2.0.0. The delay execution time for deleting or migrating data that has been saved for longer than the KEEP value, default value is 0 (hours). After the data file's save time exceeds KEEP, the deletion or migration operation will not be executed immediately, but will wait an additional interval specified by this parameter, to avoid peak business periods.
- STT_TRIGGER: Indicates the number of file merges triggered by disk files. For scenarios with few tables and high-frequency writing, this parameter is recommended to use the default configuration; for scenarios with many tables and low-frequency writing, this parameter is recommended to be set to a larger value.
- SINGLE_STABLE: Indicates whether only one supertable can be created in this database, used in cases where the supertable has a very large number of columns.
  - 0: Indicates that multiple supertables can be created.
  - 1: Indicates that only one supertable can be created.
- TABLE_PREFIX: When it is a positive value, it ignores the specified length prefix of the table name when deciding which vgroup to allocate a table to; when it is a negative value, it only uses the specified length prefix of the table name when deciding which vgroup to allocate a table to; for example, assuming the table name is "v30001", when TSDB_PREFIX = 2, use "0001" to decide which vgroup to allocate to, when TSDB_PREFIX = -2, use "v3" to decide which vgroup to allocate to.
- TABLE_SUFFIX: When it is a positive value, it ignores the specified length suffix of the table name when deciding which vgroup to allocate a table to; when it is a negative value, it only uses the specified length suffix of the table name when deciding which vgroup to allocate a table to; for example, assuming the table name is "v30001", when TSDB_SUFFIX = 2, use "v300" to decide which vgroup to allocate to, when TSDB_SUFFIX = -2, use "01" to decide which vgroup to allocate to.
- TSDB_PAGESIZE: The page size of a VNODE's time-series data storage engine, in KB, default is 4 KB. Range is 1 to 16384, i.e., 1 KB to 16 MB.
- DNODES: Specifies the list of DNODEs where the VNODE is located, such as '1,2,3', separated by commas and without spaces between characters, only supported in the enterprise version.
- WAL_LEVEL: WAL level, default is 1.
  - 1: Write WAL, but do not perform fsync.
  - 2: Write WAL and perform fsync.
- WAL_FSYNC_PERIOD: When the WAL_LEVEL parameter is set to 2, it is used to set the disk writing period. Default is 3000, in milliseconds. Minimum is 0, meaning immediate disk writing upon each write; maximum is 180000, i.e., three minutes.
- WAL_RETENTION_PERIOD: For data subscription consumption, the maximum duration strategy for additional retention of WAL log files. WAL log cleaning is not affected by the consumption status of subscription clients. In seconds. Default is 3600, meaning WAL retains the most recent 3600 seconds of data, please modify this parameter to an appropriate value according to the needs of data subscription.
- WAL_RETENTION_SIZE: For data subscription consumption, the maximum cumulative size strategy for additional retention of WAL log files. In KB. Default is 0, meaning there is no upper limit on cumulative size.
- SS_KEEPLOCAL: When shared storage is enabled, data will be kept local at least this duration before being migrated to shared storage, only available in the enterprise version 3.3.7.0 and later. Minimum is 1 day, maximum is 36500 days, default is 365 days.
- SS_CHUNKPAGES: When shared storage is enabled, data files larger than this size will be migrated to shared storage, only available in the enterprise version 3.3.7.0 and later. Minimum is 131072, maximum is 1048576, default is 131072. The unit is TSDB page, which is typically 4KB.
- SS_COMPACT: When shared storage is enabled, if set to 1, file will be compacted before its first migration; if set to 0, compact is skipped. Only available in the enterprise version 3.3.7.0 and later.

### Database Creation Example

```sql
create database if not exists db vgroups 10 buffer 10
```

The above example creates a database named db with 10 vgroups, where each vnode is allocated 10MB of write buffer.

### Using the Database

```sql
USE db_name;
```

Use/switch database (not valid in REST connection mode).

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
  | BUFFER value
  | PAGES value
  | REPLICA value
  | STT_TRIGGER value
  | WAL_LEVEL value
  | WAL_FSYNC_PERIOD value
  | KEEP value
  | WAL_RETENTION_PERIOD value
  | WAL_RETENTION_SIZE value
  | MINROWS value
}
```

### Modify CACHESIZE

The command to modify database parameters is simple, but the difficulty lies in determining whether a modification is needed and how to modify it. This section describes how to judge whether the cachesize is sufficient.

1. How to view cachesize?

You can view the specific values of these cachesize through select * from information_schema.ins_databases;.

1. How to view cacheload?

You can view cacheload through show \<db_name>.vgroups;

1. Determine if cachesize is sufficient

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

## Delete Expired Data

```sql
TRIM DATABASE db_name;
```

Deletes expired data and reorganizes data according to the multi-level storage configuration.

## Delete Expired WAL

```sql
TRIM DATABASE db_name WAL;
```
Delete expired WAL logs. Using `trim wal` ignores the vgroup `keep_version` restriction.

## Flush Memory Data to Disk

```sql
FLUSH DATABASE db_name;
```

Flushes data in memory to disk. Executing this command before shutting down a node can avoid data replay after restart, speeding up the startup process.

## Adjust the Distribution of VNODEs in VGROUP

```sql
REDISTRIBUTE VGROUP vgroup_no DNODE dnode_id1 [DNODE dnode_id2] [DNODE dnode_id3]
```

Adjusts the distribution of vnodes in a vgroup according to the given list of dnodes. Since the maximum number of replicas is 3, a maximum of 3 dnodes can be entered.

## Automatically Adjust the Distribution of VNODEs in VGROUP

```sql
BALANCE VGROUP
```

Automatically adjusts the distribution of vnodes in all vgroups of the cluster, equivalent to performing data load balancing at the vnode level for the cluster.

## Check Database Working Status

```sql
SHOW db_name.ALIVE;
```

Query the availability status of the database db_name, with return values of 0 (unavailable), 1 (fully available), or 2 (partially available, indicating that some VNODEs in the database are available while others are not).

## View DB Disk Usage

```sql
select * from  INFORMATION_SCHEMA.INS_DISK_USAGE where db_name = 'db_name'   
```

View the disk usage of each module in the DB.

```sql
SHOW db_name.disk_info;
```

View the compression ratio and disk usage of the database db_name

This command is essentially equivalent to `select sum(data1 + data2 + data3)/sum(raw_data), sum(data1 + data2 + data3) from information_schema.ins_disk_usage where db_name="dbname"`
