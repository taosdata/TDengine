---
sidebar_label: Database
title: Database
description: "create and drop database, show or change database parameters"
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

## Parameters

- BUFFER: specifies the size (in MB) of the write buffer for each vnode. Enter a value between 3 and 16384. The default value is 96.
- CACHEMODEL: specifies how the latest data in subtables is stored in the cache. The default value is none.
  - none: The latest data is not cached.
  - last_row: The last row of each subtable is cached. This option significantly improves the performance of the LAST_ROW function.
  - last_value: The last non-null value of each column in each subtable is cached. This option significantly improves the performance of the LAST function under normal circumstances, such as statements including the WHERE, ORDER BY, GROUP BY, and INTERVAL keywords.
  - both: The last row of each subtable and the last non-null value of each column in each subtable are cached.
- CACHESIZE: specifies the amount (in MB) of memory used for subtable caching on each vnode. Enter a value between 1 and 65536. The default value is 1.
- COMP: specifies how databases are compressed. The default value is 2.
  - 0: Compression is disabled.
  - 1: One-pass compression is enabled.
  - 2: Two-pass compression is enabled.
- DURATION: specifies the time period contained in each data file. After the time specified by this parameter has elapsed, TDengine creates a new data file to store incoming data. You can use m (minutes), h (hours), and d (days) as the unit, for example DURATION 100h or DURATION 10d. If you do not include a unit, d is used by default.
- WAL_FSYNC_PERIOD: specifies the interval (in milliseconds) at which data is written from the WAL to disk. This parameter takes effect only when the WAL parameter is set to 2. The default value is 3000. Enter a value between 0 and 180000. The value 0 indicates that incoming data is immediately written to disk.
- MAXROWS: specifies the maximum number of rows recorded in a block. The default value is 4096.
- MINROWS: specifies the minimum number of rows recorded in a block. The default value is 100.
- KEEP: specifies the time for which data is retained. Enter a value between 1 and 365000. The default value is 3650. The value of the KEEP parameter must be greater than or equal to the value of the DURATION parameter. TDengine automatically deletes data that is older than the value of the KEEP parameter. You can use m (minutes), h (hours), and d (days) as the unit, for example KEEP 100h or KEEP 10d. If you do not include a unit, d is used by default.
- PAGES: specifies the number of pages in the metadata storage engine cache on each vnode. Enter a value greater than or equal to 64. The default value is 256. The space occupied by metadata storage on each vnode is equal to the product of the values of the PAGESIZE and PAGES parameters. The space occupied by default is 1 MB.
- PAGESIZE: specifies the size (in KB) of each page in the metadata storage engine cache on each vnode. The default value is 4. Enter a value between 1 and 16384.
- PRECISION: specifies the precision at which a database records timestamps. Enter ms for milliseconds, us for microseconds, or ns for nanoseconds. The default value is ms.
- REPLICA: specifies the number of replicas that are made of the database. Enter 1 or 3. The default value is 1. The value of the REPLICA parameter cannot exceed the number of dnodes in the cluster.
- RETENTIONS: specifies the retention period for data aggregated at various intervals. For example, RETENTIONS 15s:7d,1m:21d,15m:50d indicates that data aggregated every 15 seconds is retained for 7 days, data aggregated every 1 minute is retained for 21 days, and data aggregated every 15 minutes is retained for 50 days. You must enter three aggregation intervals and corresponding retention periods.
- STRICT: specifies whether strong data consistency is enabled. The default value is off.
  - on: Strong consistency is enabled and implemented through the Raft consensus algorithm. In this mode, an operation is considered successful once it is confirmed by half of the nodes in the cluster.
  - off: Strong consistency is disabled. In this mode, an operation is considered successful when it is initiated by the local node.
- WAL_LEVEL: specifies whether fsync is enabled. The default value is 1.
  - 1: WAL is enabled but fsync is disabled.
  - 2: WAL and fsync are both enabled.
- VGROUPS: specifies the initial number of vgroups when a database is created.
- SINGLE_STABLE: specifies whether the database can contain more than one supertable.
  - 0: The database can contain multiple supertables.
  - 1: The database can contain only one supertable.
- WAL_RETENTION_PERIOD: specifies the time after which WAL files are deleted. This parameter is used for data subscription. Enter a time in seconds. The default value of single copy is 0. A value of 0 indicates that each WAL file is deleted immediately after its contents are written to disk. -1: WAL files are never deleted. The default value of multiple copy is 4 days.
- WAL_RETENTION_SIZE: specifies the size at which WAL files are deleted. This parameter is used for data subscription. Enter a size in KB. The default value of single copy is 0. A value of 0 indicates that each WAL file is deleted immediately after its contents are written to disk. -1: WAL files are never deleted. The default value of multiple copy is -1.
- WAL_ROLL_PERIOD: specifies the time after which WAL files are rotated. After this period elapses, a new WAL file is created. The default value of single copy is 0. A value of 0 indicates that a new WAL file is created only after the previous WAL file was written to disk. The default values of multiple copy is 1 day.
- WAL_SEGMENT_SIZE: specifies the maximum size of a WAL file. After the current WAL file reaches this size, a new WAL file is created. The default value is 0. A value of 0 indicates that a new WAL file is created only after the previous WAL file was written to disk.

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
  | WAL_LEVEL value
  | WAL_FSYNC_PERIOD value
  | KEEP value
}
```

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
