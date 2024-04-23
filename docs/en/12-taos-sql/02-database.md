---
title: Database
sidebar_label: Database
description: This document describes how to create and perform operations on databases.
---

## Create a Database

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
  | WAL_LEVEL {1 | 2}
  | VGROUPS value
  | SINGLE_STABLE {0 | 1}
  | STT_TRIGGER value
  | TABLE_PREFIX value
  | TABLE_SUFFIX value
  | TSDB_PAGESIZE value
  | WAL_RETENTION_PERIOD value
  | WAL_RETENTION_SIZE value
}
```

## Parameters

- BUFFER: specifies the size (in MB) of the write buffer for each vnode. Enter a value between 3 and 16384. The default value is 256.
- CACHEMODEL: specifies how the latest data in subtables is stored in the cache. The default value is none.
  - none: The latest data is not cached.
  - last_row: The last row of each subtable is cached. This option significantly improves the performance of the LAST_ROW function.
  - last_value: The last non-null value of each column in each subtable is cached. This option significantly improves the performance of the LAST function under normal circumstances, such as statements including the WHERE, ORDER BY, GROUP BY, and INTERVAL keywords.
  - both: The last row of each subtable and the last non-null value of each column in each subtable are cached.
    Note: If you turn on cachemodel, then turn off, and turn on again, the result of last/last_row may be wrong, don't do like this, it's strongly recommended to always turn on the cache using "both".
- CACHESIZE: specifies the amount (in MB) of memory used for subtable caching on each vnode. Enter a value between 1 and 65536. The default value is 1.
- COMP: specifies how databases are compressed. The default value is 2.
  - 0: Compression is disabled.
  - 1: One-pass compression is enabled.
  - 2: Two-pass compression is enabled.
- DURATION: specifies the time period contained in each data file. After the time specified by this parameter has elapsed, TDengine creates a new data file to store incoming data. You can use m (minutes), h (hours), and d (days) as the unit, for example DURATION 100h or DURATION 10d. If you do not include a unit, d is used by default.
- WAL_FSYNC_PERIOD: specifies the interval (in milliseconds) at which data is written from the WAL to disk. This parameter takes effect only when the WAL parameter is set to 2. The default value is 3000. Enter a value between 0 and 180000. The value 0 indicates that incoming data is immediately written to disk.
- MAXROWS: specifies the maximum number of rows recorded in a block. The default value is 4096.
- MINROWS: specifies the minimum number of rows recorded in a block. The default value is 100.
- KEEP: specifies the time for which data is retained. Enter a value between 1 and 365000. The default value is 3650. The value of the KEEP parameter must be greater than or equal to three times of the value of the DURATION parameter. TDengine automatically deletes data that is older than the value of the KEEP parameter. You can use m (minutes), h (hours), and d (days) as the unit, for example KEEP 100h or KEEP 10d. If you do not include a unit, d is used by default. TDengine Enterprise supports [Tiered Storage](https://docs.tdengine.com/tdinternal/arch/#tiered-storage) function, thus multiple KEEP values (comma separated and up to 3 values supported, and meet keep 0 &lt;= keep 1 &lt;= keep 2, e.g. KEEP 100h,100d,3650d) are supported; TDengine OSS does not support Tiered Storage function (although multiple keep values are configured, they do not take effect, only the maximum keep value is used as KEEP).
- PAGES: specifies the number of pages in the metadata storage engine cache on each vnode. Enter a value greater than or equal to 64. The default value is 256. The space occupied by metadata storage on each vnode is equal to the product of the values of the PAGESIZE and PAGES parameters. The space occupied by default is 1 MB.
- PAGESIZE: specifies the size (in KB) of each page in the metadata storage engine cache on each vnode. The default value is 4. Enter a value between 1 and 16384.
- PRECISION: specifies the precision at which a database records timestamps. Enter ms for milliseconds, us for microseconds, or ns for nanoseconds. The default value is ms.
- REPLICA: specifies the number of replicas that are made of the database. Enter 1 or 3. The default value is 1. The value of the REPLICA parameter cannot exceed the number of dnodes in the cluster.
- WAL_LEVEL: specifies whether fsync is enabled. The default value is 1.
  - 1: WAL is enabled but fsync is disabled.
  - 2: WAL and fsync are both enabled.
- VGROUPS: specifies the initial number of vgroups when a database is created.
- SINGLE_STABLE: specifies whether the database can contain more than one supertable.
  - 0: The database can contain multiple supertables.
  - 1: The database can contain only one supertable.
- STT_TRIGGER: specifies the number of file merges triggered by flushed files. The default is 8, ranging from 1 to 16. For high-frequency scenarios with few tables, it is recommended to use the default configuration or a smaller value for this parameter; For multi-table low-frequency scenarios, it is recommended to configure this parameter with a larger value.
- TABLE_PREFIX: The prefix in the table name that is ignored when distributing a table to a vgroup when it's a positive number, or only the prefix is used when distributing a table to a vgroup, the default value is 0; For example, if the table name v30001, then "0001" is used if TSDB_PREFIX is set to 2 but "v3" is used if TSDB_PREFIX is set to -2; It can help you to control the distribution of tables.
- TABLE_SUFFIX: The suffix in the table name that is ignored when distributing a table to a vgroup when it's a positive number, or only the suffix is used when distributing a table to a vgroup, the default value is 0; For example, if the table name v30001, then "v300" is used if TSDB_SUFFIX is set to 2 but "01" is used if TSDB_SUFFIX is set to -2; It can help you to control the distribution of tables. 
- TSDB_PAGESIZE: The page size of the data storage engine in a vnode. The unit is KB. The default is 4 KB. The range is 1 to 16384, that is, 1 KB to 16 MB.
- WAL_RETENTION_PERIOD: specifies the maximum time of which WAL files are to be kept for consumption. This parameter is used for data subscription. Enter a time in seconds. The default value is 3600, which means the data in latest 3600 seconds will be kept in WAL for data subscription. Please adjust this parameter to a more proper value for your data subscription.
- WAL_RETENTION_SIZE: specifies the maximum total size of which WAL files are to be kept for consumption. This parameter is used for data subscription. Enter a size in KB. The default value is 0. A value of 0 indicates that the total size of WAL files to keep for consumption has no upper limit.
### Example Statement

```sql
create database if not exists db vgroups 10 buffer 10

```

The preceding SQL statement creates a database named db that has 10 vgroups and whose vnodes have a 10 MB cache.

### Specify the Database in Use

```
USE db_name;
```

The preceding SQL statement switches to the specified database. (If you connect to TDengine over the REST API, this statement does not take effect.)

## Drop a Database

```
DROP DATABASE [IF EXISTS] db_name
```

The preceding SQL statement deletes the specified database. This statement will delete all tables in the database and destroy all vgroups associated with it. Exercise caution when using this statement.

## Change Database Configuration

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

###  ALTER CACHESIZE

The command of changing database configuration parameters is easy to use, but it's hard to determine whether a parameter is proper or not. In this section we will describe how to determine whether cachesize is big enough.

1. How to check cachesize?

You can use  `select * from information_schema.ins_databases;` to get the value of cachesize.

2. How to check cacheload?

You can use `show <db_name>.vgroups;` to check the value of cacheload.

3. Determine whether cachesize is big engough

If the value of `cacheload` is very close to the value of `cachesize`, then it's very probably that `cachesize` is too small. If the value of `cacheload` is much smaller than the value of `cachesize`, then `cachesize` is big enough. You can use this simple principle to determine. Depending on how much memory is available in your system, you can choose to double `cachesize` or incrase it by even 5 or more times.

4. stt_trigger

Pleae make sure stopping data writing before trying to alter stt_trigger parameter. 

:::note
Other parameters cannot be modified after the database has been created.

:::

## View Databases

### View All Databases

```
SHOW DATABASES;
```

### View the CREATE Statement for a Database

```
SHOW CREATE DATABASE db_name;
```

The preceding SQL statement can be used in migration scenarios. This command can be used to get the CREATE statement, which can be used in another TDengine instance to create the exact same database.

### View Database Configuration

```sql
SELECT * FROM INFORMATION_SCHEMA.INS_DATABASES WHERE NAME='DBNAME' \G;
```

The preceding SQL statement shows the value of each parameter for the specified database. One value is displayed per line.

## Delete Expired Data

```sql
TRIM DATABASE db_name;
```

The preceding SQL statement deletes data that has expired and orders the remaining data in accordance with the storage configuration.

## Flush Data

```sql
FLUSH DATABASE db_name;
```

Flush data from memory onto disk. Before shutting down a node, executing this command can avoid data restore after restarting and speed up the startup process.

## Redistribute Vgroup

```sql
REDISTRIBUTE VGROUP vgroup_no DNODE dnode_id1 [DNODE dnode_id2] [DNODE dnode_id3]
```

Adjust the distribution of vnodes in the vgroup according to the given list of dnodes. 

## Balance Vgroup

```sql
BALANCE VGROUP
```

Automatically adjusts the distribution of vnodes in all vgroups of the cluster, which is equivalent to load balancing the data of the cluster at the vnode level.
