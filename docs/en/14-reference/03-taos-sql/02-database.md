---
title: Manage Databases
description: "Create, delete databases, view and modify database parameters"
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
  | PAGESIZE value
  | CACHEMODEL {'none' | 'last_row' | 'last_value' | 'both'}
  | CACHESIZE value
  | COMP {0 | 1 | 2}
  | DURATION value
  | MAXROWS value
  | MINROWS value
  | KEEP value
  | STT_TRIGGER value
  | SINGLE_STABLE {0 | 1}
  | TABLE_PREFIX value
  | TABLE_SUFFIX value
  | TSDB_PAGESIZE value
  | WAL_LEVEL {1 | 2}
  | WAL_FSYNC_PERIOD value
  | WAL_RETENTION_PERIOD value
  | WAL_RETENTION_SIZE value
}
```

### Parameter Description

- VGROUPS: The initial number of vgroups in the database.
- PRECISION: The timestamp precision of the database. 'ms' means milliseconds, 'us' means microseconds, 'ns' means nanoseconds, with a default of milliseconds.
- REPLICA: Indicates the number of replicas for the database, with values of 1, 2, or 3, defaulting to 1; 2 is only available in the enterprise version 3.3.0.0 and later. In a cluster, the number of replicas must be less than or equal to the number of DNODEs, and there are the following restrictions:
  - Operations such as SPLITE VGROUP or REDISTRIBUTE VGROUP are currently not supported for dual-replica databases.
  - A single-replica database can be changed to a dual-replica database, but not the other way around, and a three-replica database cannot be changed to a dual-replica.
- BUFFER: The size of the memory pool for a VNODE write, measured in MB, defaulting to 256, with a minimum of 3 and a maximum of 16384.
- PAGES: The number of cache pages for the metadata storage engine in a VNODE, defaulting to 256, with a minimum of 64. The metadata storage of a VNODE occupies PAGESIZE * PAGES, which is 1MB of memory by default.
- PAGESIZE: The page size of the metadata storage engine in a VNODE, measured in KB, defaulting to 4 KB. The range is from 1 to 16384, i.e., from 1 KB to 16 MB.
- CACHEMODEL: Indicates whether to cache the recent data of the subtables in memory. The default is none.
  - none: Indicates no caching.
  - last_row: Indicates caching of the most recent row of the subtable. This significantly improves the performance of the LAST_ROW function.
  - last_value: Indicates caching of the most recent non-NULL value for each column in the subtable. This significantly improves the performance of the LAST function under normal conditions (without special influences such as WHERE, ORDER BY, GROUP BY, INTERVAL).
  - both: Indicates that both the caching of the most recent row and column functions are enabled.
    Note: Switching CacheModel values back and forth may lead to inaccurate results for last/last_row queries, so please proceed with caution. It is recommended to keep it enabled.
- CACHESIZE: Indicates the memory size used for caching the recent data of the subtables in each vnode. The default is 1, with a range of [1, 65536], measured in MB.
- COMP: Indicates the database file compression flag. The default value is 2, with a range of [0, 2].
  - 0: Indicates no compression.
  - 1: Indicates one-stage compression.
  - 2: Indicates two-stage compression.
- DURATION: The time span for storing data in the data file. You can use units, such as DURATION 100h, DURATION 10d, etc., supporting m (minutes), h (hours), and d (days). If no time unit is specified, the default unit is days; for example, DURATION 50 indicates 50 days.
- MAXROWS: The maximum number of records in a file block, defaulting to 4096 records.
- MINROWS: The minimum number of records in a file block, defaulting to 100 records.
- KEEP: Indicates the number of days to retain the data file. The default value is 3650, with a range of [1, 365000], and it must be at least three times the DURATION parameter value. The database will automatically delete data that exceeds the KEEP time. KEEP can also use units, such as KEEP 100h, KEEP 10d, etc., supporting m (minutes), h (hours), and d (days). It can also be specified without a unit, such as KEEP 50, where the default unit is days. The enterprise version supports [multi-level storage](https://docs.taosdata.com/tdinternal/arch/#%E5%A4%9A%E7%BA%A7%E5%AD%98%E5%82%A8) functionality, allowing multiple retention times (separated by commas, with a maximum of 3, satisfying keep 0 \<= keep 1 \<= keep 2, e.g., KEEP 100h,100d,3650d); the community version does not support multi-level storage (even if multiple retention times are configured, they will not take effect, and KEEP will take the maximum retention time).
- STT_TRIGGER: Indicates the number of on-disk files that trigger file merging. The default is 1, with a range of 1 to 16. For high-frequency scenarios with few tables, it is recommended to use the default configuration or a smaller value; for low-frequency scenarios with many tables, it is recommended to configure a larger value.
- SINGLE_STABLE: Indicates whether only one supertable can be created in this database, suitable for cases where there are very many columns in the supertable.
  - 0: Indicates that multiple supertables can be created.
  - 1: Indicates that only one supertable can be created.
- TABLE_PREFIX: When positive, it ignores the specified prefix length in the table name when deciding which vgroup to allocate a table to; when negative, it only uses the specified prefix length in the table name. For example, assuming the table name is "v30001", when TSDB_PREFIX = 2, it uses "0001" to determine which vgroup to allocate; when TSDB_PREFIX = -2, it uses "v3" to determine which vgroup to allocate.
- TABLE_SUFFIX: When positive, it ignores the specified suffix length in the table name when deciding which vgroup to allocate a table to; when negative, it only uses the specified suffix length in the table name. For example, assuming the table name is "v30001", when TSDB_SUFFIX = 2, it uses "v300" to determine which vgroup to allocate; when TSDB_SUFFIX = -2, it uses "01" to determine which vgroup to allocate.
- TSDB_PAGESIZE: The page size of the time series data storage engine in a VNODE, measured in KB, defaulting to 4 KB. The range is from 1 to 16384, i.e., from 1 KB to 16 MB.
- WAL_LEVEL: WAL level, defaulting to 1.
  - 1: Write WAL but do not perform fsync.
  - 2: Write WAL and perform fsync.
- WAL_FSYNC_PERIOD: When the WAL_LEVEL parameter is set to 2, it is used to set the write-back period. The default is 3000 milliseconds, with a minimum of 0 (indicating immediate write-back on each write) and a maximum of 180000 (i.e., three minutes).
- WAL_RETENTION_PERIOD: To facilitate data subscription consumption, the maximum retention duration policy for WAL log files is to be kept. The WAL log cleanup is not affected by the subscription client consumption status. The unit is seconds. The default is 3600, meaning that the most recent 3600 seconds of data in the WAL will be retained. Please modify this parameter to an appropriate value according to the data subscription needs.
- WAL_RETENTION_SIZE: To facilitate data subscription consumption, the maximum cumulative size policy for WAL log files is to be kept. The unit is KB. The default is 0, indicating that the cumulative size has no upper limit.

### Example of Creating a Database

```sql
create database if not exists db vgroups 10 buffer 10
```

The above example creates a database named db with 10 vgroups, with each vnode allocated 10MB of write cache.

### Using the Database

```sql
USE db_name;
```

Use/switch the database (invalid in REST connection mode).

## Delete Database

```sql
DROP DATABASE [IF EXISTS] db_name
```

Delete the database. All data tables contained in the specified database will be deleted, and all vgroups of that database will also be completely destroyed. Please use with caution!

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
}
```

### Modify CACHESIZE

Modifying database parameters is straightforward; however, determining whether to modify and how to modify can be challenging. This section describes how to assess whether the database's cachesize is sufficient.

1. How to check cachesize?

You can check the specific values of cachesize by executing `select * from information_schema.ins_databases;`.

2. How to check cacheload?

You can view cacheload by executing `show <db_name>.vgroups;`.

3. Assessing whether cachesize is sufficient

If cacheload is very close to cachesize, then cachesize may be too small. If cacheload is significantly less than cachesize, then cachesize is sufficient. You can use this principle to determine whether a modification is necessary. The specific modified value can be determined based on the available system memory.

4. STT_TRIGGER

Please stop database writes before modifying the stt_trigger parameter.

:::note
Other parameters are not supported for modification in version 3.0.0.0.

:::

## View Databases

### View All Databases in the System

```sql
SHOW DATABASES;
```

### Show the Creation Statement of a Database

```sql
SHOW CREATE DATABASE db_name \G;
```

Commonly used for database migration. For an existing database, it returns its creation statement; executing this statement in another cluster will create a Database with identical settings.

### View Database Parameters

```sql
SELECT * FROM INFORMATION_SCHEMA.INS_DATABASES WHERE NAME='db_name' \G;
```

This will list the configuration parameters of the specified database, with each row showing a single parameter.

## Delete Expired Data

```sql
TRIM DATABASE db_name;
```

Delete expired data and reorganize data based on the multi-level storage configuration.

## Flush In-Memory Data

```sql
FLUSH DATABASE db_name;
```

Flush data in memory. Executing this command before shutting down a node can avoid data playback after a restart, speeding up the startup process.

## Adjust the Distribution of VNODEs in a VGROUP

```sql
REDISTRIBUTE VGROUP vgroup_no DNODE dnode_id1 [DNODE dnode_id2] [DNODE dnode_id3]
```

Adjust the distribution of vnodes in the vgroup according to the given dnode list. Since the maximum number of replicas is 3, a maximum of 3 dnodes can be input.

## Automatically Adjust the Distribution of VNODEs in a VGROUP

```sql
BALANCE VGROUP
```

Automatically adjusts the distribution of vnodes in all vgroups in the cluster, performing data load balancing at the vnode level.

## View Database Work Status

```sql
SHOW db_name.ALIVE;
```

Query the availability status of the database db_name, with return values of 0 (unavailable), 1 (fully available), or 2 (partially available, indicating that some VNODEs in the database are available while others are not).
