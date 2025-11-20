---
title: Metadata
slug: /tdengine-reference/sql-manual/metadata
---

TDengine has a built-in database called `INFORMATION_SCHEMA`, which provides access to database metadata, system information, and status, such as the names of databases or tables, the SQL statements currently being executed, etc. This database stores information about all other databases maintained by TDengine. It contains multiple read-only tables. In fact, these tables are views, not base tables, so there are no files associated with them. Therefore, you can only query these tables, and cannot perform write operations like INSERT. The `INFORMATION_SCHEMA` database is designed to provide access to the information provided by various SHOW statements supported by TDengine (such as SHOW TABLES, SHOW DATABASES) in a more consistent way. Compared to SHOW statements, using SELECT ... FROM INFORMATION_SCHEMA.tablename has the following advantages:

1. You can use the USE statement to set INFORMATION_SCHEMA as the default database
2. You can use the familiar syntax of the SELECT statement, only needing to learn some table names and column names
3. You can filter, sort, and perform other operations on the query results. In fact, you can use any SELECT statement supported by TDengine to query the tables in INFORMATION_SCHEMA
4. TDengine can flexibly add columns to the existing tables in INFORMATION_SCHEMA in future evolutions without worrying about impacting existing business systems
5. More interoperable with other database systems. For example, Oracle database users are familiar with querying tables in the Oracle data dictionary

:::info

- Since SHOW statements are already familiar and widely used by developers, they are still retained.
- Some columns in the system tables may be keywords, and need to use the escape character '`' when querying, for example, to query how many VGROUPs the database test has:

```sql
   select `vgroups` from ins_databases where name = 'test';
```

:::

This chapter will detail the tables and table structures in the built-in metadata database `INFORMATION_SCHEMA`.

## INS_DNODES

Provides information about dnodes. You can also use SHOW DNODES to query this information. Users with SYSINFO set to 0 cannot view this table.

| #    | **Column Name** | **Data Type** | **Description**                                              |
| ---- | :-------------: | ------------- | ------------------------------------------------------------ |
| 1    |     vnodes      | SMALLINT      | The actual number of vnodes in the dnode. Note, `vnodes` is a TDengine keyword, and needs to be escaped with ` when used as a column name. |
| 2    | support_vnodes  | SMALLINT      | Maximum number of supported vnodes                           |
| 3    |     status      | BINARY(10)    | Current status                                               |
| 4    |      note       | BINARY(256)   | Information such as the reason for being offline             |
| 5    |       id        | SMALLINT      | dnode id                                                     |
| 6    |    endpoint     | BINARY(134)   | Address of the dnode                                         |
| 7    |     create      | TIMESTAMP     | Creation time                                                |

## INS_MNODES

Provides information about mnodes. You can also use SHOW MNODES to query this information. Users with SYSINFO set to 0 cannot view this table.

| #    | **Column Name** | **Data Type** | **Description**                    |
| ---- | :-------------: | ------------- | ---------------------------------- |
| 1    |       id        | SMALLINT      | mnode id                           |
| 2    |    endpoint     | BINARY(134)   | address of the mnode               |
| 3    |      role       | BINARY(10)    | current role                       |
| 4    |    role_time    | TIMESTAMP     | time when current role was assumed |
| 5    |   create_time   | TIMESTAMP     | creation time                      |

## INS_QNODES

Information about QNODEs in the current system. You can also use SHOW QNODES to query this information. Users with SYSINFO set to 0 cannot view this table.

| #    | **Column Name** | **Data Type** | **Description**      |
| ---- | :-------------: | ------------- | -------------------- |
| 1    |       id        | SMALLINT      | qnode id             |
| 2    |    endpoint     | VARCHAR(134)  | address of the qnode |
| 3    |   create_time   | TIMESTAMP     | creation time        |

## INS_SNODES

Information about SNODEs in the current system. You can also use SHOW SNODES to query this information. Users with SYSINFO set to 0 cannot view this table.

| #    | **Column Name** | **Data Type** | **Description**      |
| ---- | :-------------: | ------------- | -------------------- |
| 1    |       id        | SMALLINT      | snode id             |
| 2    |    endpoint     | VARCHAR(134)  | address of the snode |
| 3    |   create_time   | TIMESTAMP     | creation time        |

## INS_CLUSTER

Stores cluster-related information. Users with SYSINFO set to 0 cannot view this table.

| #    | **Column Name** | **Data Type** | **Description** |
| ---- | :-------------: | ------------- | --------------- |
| 1    |       id        | BIGINT        | cluster id      |
| 2    |      name       | VARCHAR(134)  | cluster name    |
| 3    |   create_time   | TIMESTAMP     | creation time   |

## INS_DATABASES

Provides information about database objects created by users. You can also use SHOW DATABASES to query this information.

| #    |   **Column Name**    | **Data Type** | **Description**                                              |
| ---- | :------------------: | ------------- | ------------------------------------------------------------ |
| 1    |         name         | VARCHAR(64)   | database name                                                |
| 2    |     create_time      | TIMESTAMP     | creation time                                                |
| 3    |       ntables        | INT           | number of tables in the database, including subtables and basic tables but excluding supertables |
| 4    |       vgroups        | INT           | number of vgroups in the database. Note, `vgroups` is a TDengine keyword and must be escaped with ` when used as a column name. |
| 6    |       replica        | INT           | number of replicas. Note, `replica` is a TDengine keyword and must be escaped with ` when used as a column name. |
| 7    |        strict        | VARCHAR(4)    | deprecated parameter                                         |
| 8    |       duration       | VARCHAR(10)   | time span for storing data in a single file. Note, `duration` is a TDengine keyword and must be escaped with ` when used as a column name. Internally stored in minutes, may be displayed in days or hours when queried |
| 9    |         keep         | VARCHAR(32)   | data retention duration. Note, `keep` is a TDengine keyword and must be escaped with ` when used as a column name. Internally stored in minutes, may be displayed in days or hours when queried |
| 10   |        buffer        | INT           | size of the write cache memory block for each vnode, in MB. Note, `buffer` is a TDengine keyword and must be escaped with ` when used as a column name. |
| 11   |       pagesize       | INT           | page size for the metadata storage engine in each VNODE, in KB. Note, `pagesize` is a TDengine keyword and must be escaped with ` when used as a column name. |
| 12   |        pages         | INT           | number of cache pages for the metadata storage engine in each vnode. Note, `pages` is a TDengine keyword and must be escaped with ` when used as a column name. |
| 13   |       minrows        | INT           | minimum number of records in a file block. Note, `minrows` is a TDengine keyword and must be escaped with ` when used as a column name. |
| 14   |       maxrows        | INT           | maximum number of records in a file block. Note, `maxrows` is a TDengine keyword and must be escaped with ` when used as a column name. |
| 15   |         comp         | INT           | data compression method. Note, `comp` is a TDengine keyword and must be escaped with ` when used as a column name. |
| 16   |      precision       | VARCHAR(2)    | time resolution. Note, `precision` is a TDengine keyword and must be escaped with ` when used as a column name. |
| 17   |        status        | VARCHAR(10)   | database status                                              |
| 18   |      retentions      | VARCHAR(60)   | data aggregation period and retention duration. Note, `retentions` is a TDengine keyword and must be escaped with ` when used as a column name. |
| 19   |    single_stable     | BOOL          | indicates whether only one supertable can be created in this database. Note, `single_stable` is a TDengine keyword and must be escaped with ` when used as a column name. |
| 20   |      cachemodel      | VARCHAR(60)   | indicates whether to cache recent data of subtables in memory. Note, `cachemodel` is a TDengine keyword and must be escaped with ` when used as a column name. |
| 21   |      cachesize       | INT           | size of memory used to cache recent data of subtables in each vnode. Note, `cachesize` is a TDengine keyword and must be escaped with ` when used as a column name. |
| 22   |      wal_level       | INT           | WAL level. Note, `wal_level` is a TDengine keyword and must be escaped with ` when used as a column name. |
| 23   |   wal_fsync_period   | INT           | data write-to-disk period. Note, `wal_fsync_period` is a TDengine keyword and must be escaped with ` when used as a column name. |
| 24   | wal_retention_period | INT           | WAL retention duration, in seconds. Note, `wal_retention_period` is a TDengine keyword and must be escaped with ` when used as a column name. |
| 25   |  wal_retention_size  | INT           | WAL retention limit. Note, `wal_retention_size` is a TDengine keyword and must be escaped with ` when used as a column name. |
| 26   |     stt_trigger      | SMALLINT      | number of disk files that trigger file merging. Note, `stt_trigger` is a TDengine keyword and must be escaped with ` when used as a column name. |
| 27   |     table_prefix     | SMALLINT      | length of the prefix to ignore when the internal storage engine allocates a VNODE to store data for a table based on its name. Note, `table_prefix` is a TDengine keyword and must be escaped with ` when used as a column name. |
| 28   |     table_suffix     | SMALLINT      | length of the suffix to ignore when the internal storage engine allocates a VNODE to store data for a table based on its name. Note, `table_suffix` is a TDengine keyword and must be escaped with ` when used as a column name. |
| 29   |    tsdb_pagesize     | INT           | page size in the time-series data storage engine. Note, `tsdb_pagesize` is a TDengine keyword and must be escaped with ` when used as a column name. |

## INS_FUNCTIONS

Information about user-created custom functions.

| #    | **Column Name** | **Data Type**  | **Description**                                              |
| ---- | :-------------: | -------------- | ------------------------------------------------------------ |
| 1    |      name       | VARCHAR(64)    | Function name                                                |
| 2    |     comment     | VARCHAR(255)   | Additional explanation. Note that `comment` is a TDengine keyword and needs to be escaped with ``. |
| 3    |    aggregate    | INT            | Whether it is an aggregate function. Note that `aggregate` is a TDengine keyword and needs to be escaped with ``. |
| 4    |   output_type   | VARCHAR(31)    | Output type                                                  |
| 5    |   create_time   | TIMESTAMP      | Creation time                                                |
| 6    |    code_len     | INT            | Code length                                                  |
| 7    |     bufsize     | INT            | Buffer size                                                  |
| 8    |  func_language  | VARCHAR(31)    | Programming language of the custom function                  |
| 9    |    func_body    | VARCHAR(16384) | Function body definition                                     |
| 10   |  func_version   | INT            | Function version number. Initial version is 0, and the version number increases by 1 with each update. |

## INS_INDEXES

Provides information about indexes created by the user. You can also use SHOW INDEX to query this information.

| #    | **Column Name**  | **Data Type** | **Description**                                              |
| ---- | :--------------: | ------------- | ------------------------------------------------------------ |
| 1    |     db_name      | VARCHAR(32)   | Database name containing the table with this index           |
| 2    |    table_name    | VARCHAR(192)  | Name of the table containing this index                      |
| 3    |    index_name    | VARCHAR(192)  | Index name                                                   |
| 4    |   column_name    | VARCHAR(64)   | Column name on which the index is built                      |
| 5    |    index_type    | VARCHAR(10)   | Currently includes SMA and tag                               |
| 6    | index_extensions | VARCHAR(256)  | Additional information about the index. For SMA/tag types of indexes, it is a list of function names. |

## INS_STABLES

Provides information about supertables created by users.

| #    | **Column Name** | **Data Type** | **Description**                                              |
| ---- | :-------------: | ------------- | ------------------------------------------------------------ |
| 1    |   stable_name   | VARCHAR(192)  | Supertable name                                              |
| 2    |     db_name     | VARCHAR(64)   | Name of the database containing the supertable               |
| 3    |   create_time   | TIMESTAMP     | Creation time                                                |
| 4    |     columns     | INT           | Number of columns                                            |
| 5    |      tags       | INT           | Number of tags. Note, `tags` is a TDengine keyword, use ` to escape when using it as a column name. |
| 6    |   last_update   | TIMESTAMP     | Last update time                                             |
| 7    |  table_comment  | VARCHAR(1024) | Table comment                                                |
| 8    |    watermark    | VARCHAR(64)   | Window closing time. Note, `watermark` is a TDengine keyword, use ` to escape when using it as a column name. |
| 9    |    max_delay    | VARCHAR(64)   | Maximum delay for pushing calculation results. Note, `max_delay` is a TDengine keyword, use ` to escape when using it as a column name. |
| 10   |     rollup      | VARCHAR(128)  | rollup aggregation function. Note, `rollup` is a TDengine keyword, use ` to escape when using it as a column name. |

## INS_TABLES

Provides information about basic tables and subtables created by users

| #    | **Column Name** | **Data Type** | **Description**                                              |
| ---- | :-------------: | ------------- | ------------------------------------------------------------ |
| 1    |   table_name    | VARCHAR(192)  | Table name                                                   |
| 2    |     db_name     | VARCHAR(64)   | Database name                                                |
| 3    |   create_time   | TIMESTAMP     | Creation time                                                |
| 4    |     columns     | INT           | Number of columns                                            |
| 5    |   stable_name   | VARCHAR(192)  | Name of the supertable it belongs to                         |
| 6    |       uid       | BIGINT        | Table ID                                                     |
| 7    |    vgroup_id    | INT           | vgroup ID                                                    |
| 8    |       ttl       | INT           | Table's time to live. Note, `ttl` is a TDengine keyword, use ` to escape when using it as a column name. |
| 9    |  table_comment  | VARCHAR(1024) | Table comment                                                |
| 10   |      type       | VARCHAR(21)   | Table type                                                   |

## INS_TAGS

| #    | **Column Name** | **Data Type**  | **Description**                           |
| ---- | :-------------: | -------------- | ----------------------------------------- |
| 1    |   table_name    | VARCHAR(192)   | Table name                                |
| 2    |     db_name     | VARCHAR(64)    | Name of the database the table belongs to |
| 3    |   stable_name   | VARCHAR(192)   | Name of the supertable                    |
| 4    |    tag_name     | VARCHAR(64)    | Name of the tag                           |
| 5    |    tag_type     | VARCHAR(64)    | Type of the tag                           |
| 6    |    tag_value    | VARCHAR(16384) | Value of the tag                          |

## INS_COLUMNS

| #    | **Column Name** | **Data Type** | **Description**                           |
| ---- | :-------------: | ------------- | ----------------------------------------- |
| 1    |   table_name    | VARCHAR(192)  | Table name                                |
| 2    |     db_name     | VARCHAR(64)   | Name of the database the table belongs to |
| 3    |   table_type    | VARCHAR(21)   | Table type                                |
| 4    |    col_name     | VARCHAR(64)   | Name of the column                        |
| 5    |    col_type     | VARCHAR(32)   | Type of the column                        |
| 6    |   col_length    | INT           | Length of the column                      |
| 7    |  col_precision  | INT           | Precision of the column                   |
| 8    |    col_scale    | INT           | Scale of the column                       |
| 9    |  col_nullable   | INT           | Whether the column can be null            |

## INS_USERS

Provides information about users created in the system. Users with SYSINFO attribute as 0 cannot view this table.

| #    | **Column Name** | **Data Type**  | **Description**                                             |
| ---- | :-------------: | -------------- | ----------------------------------------------------------- |
| 1    |      name       | VARCHAR(24)    | Username                                                    |
| 2    |      super      | TINYINT        | Whether the user is a superuser, 1: Yes, 0: No              |
| 3    |     enable      | TINYINT        | Whether the user is enabled, 1: Yes, 0: No                  |
| 4    |     sysinfo     | TINYINT        | Whether the user can view system information, 1: Yes, 0: No |
| 5    |   create_time   | TIMESTAMP      | Creation time                                               |
| 6    |  allowed_host   | VARCHAR(49152) | IP whitelist                                                |

## INS_GRANTS

Provides information about TSDB-Enterprise licenses. Users with SYSINFO attribute as 0 cannot view this table.

| #    | **Column Name** | **Data Type** | **Description**                                              |
| ---- | :-------------: | ------------- | ------------------------------------------------------------ |
| 1    |     version     | VARCHAR(9)    | Enterprise license description: official (officially licensed)/trial (trial) |
| 2    |    cpu_cores    | VARCHAR(9)    | Number of CPU cores licensed for use                         |
| 3    |     dnodes      | VARCHAR(10)   | Number of dnode nodes licensed for use. Note, `dnodes` is a TDengine keyword, use ` to escape when used as a column name. |
| 4    |     streams     | VARCHAR(10)   | Number of streams licensed for creation. Note, `streams` is a TDengine keyword, use ` to escape when used as a column name. |
| 5    |      users      | VARCHAR(10)   | Number of users licensed for creation. Note, `users` is a TDengine keyword, use ` to escape when used as a column name. |
| 6    |    accounts     | VARCHAR(10)   | Number of accounts licensed for creation. Note, `accounts` is a TDengine keyword, use ` to escape when used as a column name. |
| 7    |     storage     | VARCHAR(21)   | Amount of storage space licensed for use. Note, `storage` is a TDengine keyword, use ` to escape when used as a column name. |
| 8    |   connections   | VARCHAR(21)   | Number of client connections licensed for use. Note, `connections` is a TDengine keyword, use ` to escape when used as a column name. |
| 9    |    databases    | VARCHAR(11)   | Number of databases licensed for use. Note, `databases` is a TDengine keyword, use ` to escape when used as a column name. |
| 10   |      speed      | VARCHAR(9)    | Number of data points per second licensed for writing        |
| 11   |    querytime    | VARCHAR(9)    | Total duration of queries licensed for use                   |
| 12   |   timeseries    | VARCHAR(21)   | Number of time-series licensed for use                       |
| 13   |     expired     | VARCHAR(5)    | Whether expired, true: expired, false: not expired           |
| 14   |   expire_time   | VARCHAR(19)   | Trial period expiration time                                 |

## INS_VGROUPS

Information on all vgroups in the system. Users with SYSINFO property set to 0 cannot view this table.

| #    | **Column Name** | **Data Type** | **Description**                                              |
| ---- | :-------------: | ------------- | ------------------------------------------------------------ |
| 1    |    vgroup_id    | INT           | vgroup id                                                    |
| 2    |     db_name     | VARCHAR(32)   | Database name                                                |
| 3    |     tables      | INT           | Number of tables in this vgroup. Note that `tables` is a TDengine keyword and must be escaped with ` when used as a column name. |
| 4    |     status      | VARCHAR(10)   | Status of this vgroup                                        |
| 5    |    v1_dnode     | INT           | ID of the dnode of the first member                          |
| 6    |    v1_status    | VARCHAR(10)   | Status of the first member                                   |
| 7    |    v2_dnode     | INT           | ID of the dnode of the second member                         |
| 8    |    v2_status    | VARCHAR(10)   | Status of the second member                                  |
| 9    |    v3_dnode     | INT           | ID of the dnode of the third member                          |
| 10   |    v3_status    | VARCHAR(10)   | Status of the third member                                   |
| 11   |     nfiles      | INT           | Number of data/metadata files in this vgroup                 |
| 12   |    file_size    | INT           | Size of data/metadata files in this vgroup                   |
| 13   |      tsma       | TINYINT       | Whether this vgroup is dedicated to Time-range-wise SMA, 1: Yes, 0: No |
| 14   | keep_version        | INT     | WAL logs for this vgroup that are greater than or equal to `keep_version` will not be automatically deleted. |
| 15   | keep_version_time   | INT     | The time when `keep_version` was last modified for this vgroup.                                            |



## INS_CONFIGS

System configuration parameters.

| #    | **Column Name** | **Data Type** | **Description**                                              |
| ---- | :-------------: | ------------- | ------------------------------------------------------------ |
| 1    |      name       | VARCHAR(32)   | Configuration item name                                      |
| 2    |      value      | VARCHAR(64)   | Value of the configuration item. Note that `value` is a TDengine keyword and must be escaped with ` when used as a column name. |

## INS_DNODE_VARIABLES

Configuration parameters for each dnode in the system. Users with SYSINFO attribute 0 cannot view this table.

| #    | **Column Name** | **Data Type** | **Description**                                              |
| ---- | :-------------: | ------------- | ------------------------------------------------------------ |
| 1    |    dnode_id     | INT           | ID of the dnode                                              |
| 2    |      name       | VARCHAR(32)   | Configuration item name                                      |
| 3    |      value      | VARCHAR(64)   | The value of the configuration item. Note, `value` is a TDengine keyword, use ` to escape when used as a column name. |

## INS_TOPICS

| #    | **Column Name** | **Data Type** | **Description**                        |
| ---- | :-------------: | ------------- | -------------------------------------- |
| 1    |   topic_name    | VARCHAR(192)  | Name of the topic                      |
| 2    |     db_name     | VARCHAR(64)   | DB associated with the topic           |
| 3    |   create_time   | TIMESTAMP     | Creation time of the topic             |
| 4    |       sql       | VARCHAR(1024) | SQL statement used to create the topic |

## INS_SUBSCRIPTIONS

| #    | **Column Name** | **Data Type** | **Description**                              |
| ---- | :-------------: | ------------- | -------------------------------------------- |
| 1    |   topic_name    | VARCHAR(204)  | Subscribed topic                             |
| 2    | consumer_group  | VARCHAR(193)  | Consumer group of the subscriber             |
| 3    |    vgroup_id    | INT           | vgroup id assigned to the consumer           |
| 4    |   consumer_id   | BIGINT        | Unique id of the consumer                    |
| 5    |      user       | VARCHAR(24)   | Username of the consumer logging in          |
| 6    |      fqdn       | VARCHAR(128)  | fqdn of the consumer's machine               |
| 7    |     offset      | VARCHAR(64)   | Consumption progress of the consumer         |
| 8    |      rows       | BIGINT        | Number of data rows consumed by the consumer |

## INS_STREAMS

| #    | **Column Name** | **Data Type** | **Description**                                              |
| ---- | :-------------: | ------------- | ------------------------------------------------------------ |
| 1    |   stream_name   | VARCHAR(64)   | Stream computing name                                        |
| 2    |   create_time   | TIMESTAMP     | Creation time                                                |
| 3    |       sql       | VARCHAR(1024) | SQL statement provided for creating stream computing         |
| 4    |     status      | VARCHAR(20)   | Current status of the stream                                 |
| 5    |    source_db    | VARCHAR(64)   | Source database                                              |
| 6    |    target_db    | VARCHAR(64)   | Destination database                                         |
| 7    |  target_table   | VARCHAR(192)  | Target table for stream computing output                     |
| 8    |    watermark    | BIGINT        | Watermark, see SQL manual for stream computing. Note, `watermark` is a TDengine keyword, use ` to escape when used as a column name. |
| 9    |     trigger     | INT           | Mode of pushing computation results, see SQL manual for stream computing. Note, `trigger` is a TDengine keyword, use ` to escape when used as a column name. |

## INS_USER_PRIVILEGES

Note: Users with SYSINFO property set to 0 cannot view this table.

| #    | **Column Name** | **Data Type**  | **Description**                      |
|:-----|:----------------|:---------------|:-------------------------------------|
| 1    |    user_name    | VARCHAR(24)    | Username                             |
| 2    |    privilege    | VARCHAR(10)    | Permission description               |
| 3    |     db_name     | VARCHAR(65)    | Database name                        |
| 4    |   table_name    | VARCHAR(193)   | Table name                           |
| 5    |    condition    | VARCHAR(49152) | Subtable permission filter condition |

## INS_DISK_USAGE

| # | **Column Name** | **Data type** | **Description**|
|:----|:-----------|:-----------|:--------------------|
| 1   | db_name    | VARCHAR(32) | Database name                              |
| 2   | vgroup_id  | INT         | vgroup ID                                  |
| 3   | wal_size   | BIGINT      | WAL file size, in KB                       |
| 4   | data1      | BIGINT      | Data file size on primary storage, in KB   |
| 5   | data2      | BIGINT      | Data file size on secondary storage, in KB |
| 6   | data3      | BIGINT      | Data file size on tertiary storage, in KB  |
| 7   | cache_rdb  | BIGINT      | Size of last/last_row files, in KB         |
| 8   | table_meta | BIGINT      | Size of meta files, in KB                  |
| 9   | ss         | BIGINT      | Size occupied on shared storage, in KB                 |
| 10  | raw_data   | BIGINT      | Estimated size of raw data, in KB          |

note:

## INS_FILESETS

Provides information about file sets.

| #   |   **Column**   | **Data Type** | **Description**                                      |
| --- | :------------: | ------------- | ---------------------------------------------------- |
| 1   |    db_name     | VARCHAR(65)   | Database name                                        |
| 2   |   vgroup_id    | INT           | Vgroup ID                                            |
| 3   |   fileset_id   | INT           | File set ID                                          |
| 4   |   start_time   | TIMESTAMP     | Start time of the time range covered by the file set |
| 5   |    end_time    | TIMESTAMP     | End time of the time range covered by the file set   |
| 6   |   total_size   | BIGINT        | Total size of the file set                           |
| 7   |  last_compact  | TIMESTAMP     | Time of the last compaction                          |
| 8   | should_compact | bool          | Whether the file set should be compacted             |

## INS_VNODES

Provides information about vnodes in the system. Users with SYSINFO property set to 0 cannot view this table.

| #   |   **Column Name**   | **Data Type** | **Description**                   |
| --- | :-----------------: | ------------- | --------------------------------- |
| 1   |      dnode_id       | INT           | Dnode id                          |
| 2   |      vgroup_id      | INT           | Vgroup id                         |
| 3   |       db_name       | VARCHAR(66)   | Database name                     |
| 4   |       status        | VARCHAR(11)   | Status of this vnode              |
| 5   |      role_time      | TIMESTAMP     | Election time                     |
| 6   |     start_time      | TIMESTAMP     | Vnode start time                  |
| 7   |      restored       | BOOL          | Restored or not                   |
| 8   |  apply_finish_time  | VARCHAR(20)   | Restore finish time               |
| 9   |      unapplied      | INT           | Number of unapplied request items |
| 10  | buffer_segment_used | BIGINT        | Buffer segment used size in bytes |
| 11  | buffer_segment_size | BIGINT        | Buffer segment size in bytes      |
