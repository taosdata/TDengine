---
title: Metadata
description: The INFORMATION_SCHEMA database stores all metadata information in the system.
slug: /tdengine-reference/sql-manual/metadata
---

TDengine has a built-in database called `INFORMATION_SCHEMA`, which provides access to database metadata, system information, and status, such as the names of databases or tables, and currently executed SQL statements. This database stores information about all other databases maintained by TDengine. It contains several read-only tables. In fact, these tables are views rather than base tables, so there are no associated files with them. Therefore, you can only query these tables and cannot perform write operations such as INSERT. The `INFORMATION_SCHEMA` database is designed to provide access to information supported by various SHOW statements (like SHOW TABLES, SHOW DATABASES) in a more consistent manner. Compared to SHOW statements, using SELECT ... FROM INFORMATION_SCHEMA.tablename has the following advantages:

1. You can use the USE statement to set `INFORMATION_SCHEMA` as the default database.
2. You can use the familiar SELECT statement syntax, only needing to learn some table names and column names.
3. You can filter, sort, and perform other operations on the query results. In fact, you can use any SELECT statement supported by TDengine to query the tables in `INFORMATION_SCHEMA`.
4. TDengine can flexibly add columns to existing tables in `INFORMATION_SCHEMA` in future versions without worrying about affecting existing business systems.
5. It is more interoperable with other database systems. For example, Oracle database users are familiar with querying the Oracle data dictionary tables.

:::info

- Since SHOW statements have already been familiar and widely used by developers, they are still retained.
- Some columns in system tables may be keywords, and you need to use the escape character '\`' when querying, for example, to query how many VGROUPs database test has:

  ```sql
  select `vgroups` from ins_databases where name = 'test';
  ```

:::

This chapter will detail the tables and table structures in the built-in metadata database `INFORMATION_SCHEMA`.

## INS_DNODES

Provides information related to dnodes. You can also use SHOW DNODES to query this information. Users with SYSINFO set to 0 cannot view this table.

| #   |    **Column Name**    | **Data Type** | **Description**                                                                                              |
| --- | :-------------------: | ------------ | ----------------------------------------------------------------------------------------------------- |
| 1   |     vnodes            | SMALLINT     | The actual number of vnodes in the dnode. Note that `vnodes` is a TDengine keyword and needs to be escaped with \`. |
| 2   | support_vnodes        | SMALLINT     | The maximum number of supported vnodes                                                                         |
| 3   |     status            | BINARY(10)   | The current status                                                                                              |
| 4   |      note             | BINARY(256)  | Reason for offline and other information                                                                        |
| 5   |       id              | SMALLINT     | dnode id                                                                                                      |
| 6   |    endpoint           | BINARY(134)  | The address of the dnode                                                                                      |
| 7   |     create            | TIMESTAMP    | Creation time                                                                                                  |

## INS_MNODES

Provides information related to mnodes. You can also use SHOW MNODES to query this information. Users with SYSINFO set to 0 cannot view this table.

| #   |  **Column Name** | **Data Type** | **Description**           |
| --- | :--------------: | ------------ | ------------------ |
| 1   |     id           | SMALLINT     | mnode id           |
| 2   |  endpoint        | BINARY(134)  | The address of the mnode       |
| 3   |    role          | BINARY(10)   | Current role           |
| 4   |  role_time       | TIMESTAMP    | The time when it became the current role |
| 5   | create_time      | TIMESTAMP    | Creation time           |

## INS_QNODES

Information about QNODEs in the current system. You can also use SHOW QNODES to query this information. Users with SYSINFO set to 0 cannot view this table.

| #   |  **Column Name** | **Data Type** | **Description**     |
| --- | :--------------: | ------------ | ------------ |
| 1   |     id           | SMALLINT     | qnode id     |
| 2   |  endpoint        | VARCHAR(134)  | The address of the qnode |
| 3   | create_time      | TIMESTAMP    | Creation time     |

## INS_SNODES

Information about SNODEs in the current system. You can also use SHOW SNODES to query this information. Users with SYSINFO set to 0 cannot view this table.

| #   |  **Column Name** | **Data Type** | **Description**     |
| --- | :--------------: | ------------ | ------------ |
| 1   |     id           | SMALLINT     | snode id     |
| 2   |  endpoint        | VARCHAR(134)  | The address of the snode |
| 3   | create_time      | TIMESTAMP    | Creation time     |

## INS_CLUSTER

Stores cluster-related information. Users with SYSINFO set to 0 cannot view this table.

| #   |  **Column Name** | **Data Type** | **Description**   |
| --- | :--------------: | ------------ | ---------- |
| 1   |     id           | BIGINT       | Cluster id |
| 2   |    name          | VARCHAR(134)  | Cluster name   |
| 3   | create_time      | TIMESTAMP    | Creation time   |

## INS_DATABASES

Provides information about user-created database objects. You can also use SHOW DATABASES to query this information.

| #   |       **Column Name**       | **Data Type**     | **Description**                                         |
| --- | :-------------------------: | ---------------- | ------------------------------------------------------ |
| 1   |         name                | VARCHAR(64)      | Database name                                         |
| 2   |     create_time             | TIMESTAMP        | Creation time                                         |
| 3   |       ntables               | INT              | Number of tables in the database, including subtables and basic tables but excluding supertables |
| 4   |       vgroups               | INT              | Number of vgroups in the database. Note that `vgroups` is a TDengine keyword and needs to be escaped with \`. |
| 5   |       replica               | INT              | Number of replicas. Note that `replica` is a TDengine keyword and needs to be escaped with \`. |
| 6   |        strict               | VARCHAR(4)       | Deprecated parameter |
| 7   |       duration              | VARCHAR(10)      | Duration of time data is stored in a single file. Note that `duration` is a TDengine keyword and needs to be escaped with \`. The internal storage unit is minutes, and it may be converted to days or hours during queries. |
| 8   |         keep                | VARCHAR(32)      | Data retention duration. Note that `keep` is a TDengine keyword and needs to be escaped with \`. The internal storage unit is minutes, and it may be converted to days or hours during queries. |
| 9   |        buffer               | INT              | Size of the memory block for writing caches per vnode, in MB. Note that `buffer` is a TDengine keyword and needs to be escaped with \`. |
| 10  |       pagesize              | INT              | Page size of the metadata storage engine in each VNODE, in KB. Note that `pagesize` is a TDengine keyword and needs to be escaped with \`. |
| 11  |        pages                | INT              | Number of cached pages for metadata storage engine in each vnode. Note that `pages` is a TDengine keyword and needs to be escaped with \`. |
| 12  |       minrows               | INT              | Minimum number of records in a file block. Note that `minrows` is a TDengine keyword and needs to be escaped with \`. |
| 13  |       maxrows               | INT              | Maximum number of records in a file block. Note that `maxrows` is a TDengine keyword and needs to be escaped with \`. |
| 14  |         comp                | INT              | Data compression method. Note that `comp` is a TDengine keyword and needs to be escaped with \`. |
| 15  |      precision              | VARCHAR(2)       | Time resolution. Note that `precision` is a TDengine keyword and needs to be escaped with \`. |
| 16  |        status               | VARCHAR(10)      | Database status                                       |
| 17  |      retentions              | VARCHAR(60)     | Data aggregation cycle and retention duration. Note that `retentions` is a TDengine keyword and needs to be escaped with \`. |
| 18  |    single_stable            | BOOL             | Indicates whether only one supertable can be created in this database. Note that `single_stable` is a TDengine keyword and needs to be escaped with \`. |
| 19  |      cachemodel             | VARCHAR(60)      | Indicates whether to cache the most recent data of subtables in memory. Note that `cachemodel` is a TDengine keyword and needs to be escaped with \`. |
| 20  |      cachesize              | INT              | Size of memory used for caching the most recent data of subtables in each vnode. Note that `cachesize` is a TDengine keyword and needs to be escaped with \`. |
| 21  |      wal_level              | INT              | WAL level. Note that `wal_level` is a TDengine keyword and needs to be escaped with \`. |
| 22  |   wal_fsync_period          | INT              | Data persistence period. Note that `wal_fsync_period` is a TDengine keyword and needs to be escaped with \`. |
| 23  | wal_retention_period        | INT              | WAL retention duration, in seconds. Note that `wal_retention_period` is a TDengine keyword and needs to be escaped with \`. |
| 24  | wal_retention_size          | INT              | WAL retention limit. Note that `wal_retention_size` is a TDengine keyword and needs to be escaped with \`. |
| 25  |   stt_trigger               | SMALLINT | Number of persistent files that trigger file merging. Note that `stt_trigger` is a TDengine keyword and needs to be escaped with \`. |
| 26  |   table_prefix              | SMALLINT | Length of prefix to ignore when assigning storage of table data by table name in the internal storage engine. Note that `table_prefix` is a TDengine keyword and needs to be escaped with \`. |
| 27  |   table_suffix              | SMALLINT | Length of suffix to ignore when assigning storage of table data by table name in the internal storage engine. Note that `table_suffix` is a TDengine keyword and needs to be escaped with \`. |
| 28  |   tsdb_pagesize             | INT | Page size in the time-series data storage engine. Note that `tsdb_pagesize` is a TDengine keyword and needs to be escaped with \`. |

## INS_FUNCTIONS

Information about user-created custom functions.

| #   |   **Column Name**    | **Data Type**  | **Description**                                                                                      |
| --- | :------------------: | ------------- | ----------------------------------------------------------------------------------------------------- |
| 1   |     name             | VARCHAR(64)    | Function name                                                                                        |
| 2   |    comment           | VARCHAR(255)   | Additional description. Note that `comment` is a TDengine keyword and needs to be escaped with \`.         |
| 3   |   aggregate          | INT           | Indicates whether it is an aggregate function. Note that `aggregate` is a TDengine keyword and needs to be escaped with \`. |
| 4   |  output_type         | VARCHAR(31)    | Output type                                                                                          |
| 5   |  create_time         | TIMESTAMP     | Creation time                                                                                       |
| 6   |   code_len           | INT           | Code length                                                                                         |
| 7   |    bufsize           | INT           | Buffer size                                                                                         |
| 8   | func_language        | VARCHAR(31)    | Programming language of the custom function                                                          |
| 9   |   func_body          | VARCHAR(16384) | Function body definition                                                                             |
| 10  | func_version         | INT           | Function version number. Initial version is 0, and each update increments the version number.          |

## INS_INDEXES

Provides information related to indexes created by users. You can also use SHOW INDEX to query this information.

| #   |     **Column Name**     | **Data Type** | **Description**                                                |
| --- | :---------------------: | ------------ | ------------------------------------------------------------- |
| 1   |     db_name            | VARCHAR(32)   | Database name containing this index                            |
| 2   |    table_name          | VARCHAR(192)  | Name of the table containing this index                        |
| 3   |    index_name          | VARCHAR(192)  | Index name                                                    |
| 4   |   column_name          | VARCHAR(64)   | Name of the column indexed                                    |
| 5   |    index_type          | VARCHAR(10)   | Currently, there are SMA and tag                              |
| 6   | index_extensions       | VARCHAR(256)  | Additional information about the index. For SMA/tag indexes, this is a list of function names. |

## INS_STABLES

Provides information about supertables created by users.

| #   |   **Column Name**    | **Data Type** | **Description**                                                                                              |
| --- | :------------------: | ------------ | ----------------------------------------------------------------------------------------------------- |
| 1   |  stable_name        | VARCHAR(192)  | Supertable name                                                                                        |
| 2   |    db_name          | VARCHAR(64)   | Name of the database where the supertable is located                                                  |
| 3   |  create_time        | TIMESTAMP     | Creation time                                                                                          |
| 4   |    columns          | INT           | Number of columns                                                                                      |
| 5   |     tags            | INT           | Number of tags. Note that `tags` is a TDengine keyword and needs to be escaped with \`.                    |
| 6   |  last_update        | TIMESTAMP     | Last update time                                                                                      |
| 7   | table_comment       | VARCHAR(1024) | Table comment                                                                                        |
| 8   |   watermark         | VARCHAR(64)   | Window closing time. Note that `watermark` is a TDengine keyword and needs to be escaped with \`.         |
| 9   |   max_delay         | VARCHAR(64)   | Maximum delay for pushing computation results. Note that `max_delay` is a TDengine keyword and needs to be escaped with \`. |
| 10  |    rollup           | VARCHAR(128)  | Rollup aggregation function. Note that `rollup` is a TDengine keyword and needs to be escaped with \`.           |

## INS_TABLES

Provides information about basic tables and subtables created by users.

| #   |   **Column Name**    | **Data Type** | **Description**                                                                              |
| --- | :------------------: | ------------ | ------------------------------------------------------------------------------------- |
| 1   |  table_name         | VARCHAR(192)  | Table name                                                                                       |
| 2   |    db_name          | VARCHAR(64)   | Name of the database where the table is located                                                  |
| 3   |  create_time        | TIMESTAMP     | Creation time                                                                                   |
| 4   |    columns          | INT           | Number of columns                                                                                |
| 5   |  stable_name        | VARCHAR(192)  | Name of the supertable to which it belongs                                                     |
| 6   |      uid            | BIGINT        | Table id                                                                                         |
| 7   |   vgroup_id         | INT           | vgroup id                                                                                        |
| 8   |      ttl            | INT           | Lifecycle of the table. Note that `ttl` is a TDengine keyword and needs to be escaped with \`.         |
| 9   | table_comment       | VARCHAR(1024) | Table comment                                                                                   |
| 10  |     type            | VARCHAR(21)   | Table type                                                                                      |

## INS_TAGS

| #   |  **Column Name**  | **Data Type**  | **Description**               |
| --- | :--------------: | ------------- | ----------------------------- |
| 1   | table_name       | VARCHAR(192)   | Table name                    |
| 2   |   db_name        | VARCHAR(64)    | Name of the database where this table is located |
| 3   | stable_name      | VARCHAR(192)   | Name of the supertable to which it belongs |
| 4   |  tag_name        | VARCHAR(64)    | Name of the tag               |
| 5   |  tag_type        | VARCHAR(64)    | Type of the tag               |
| 6   |  tag_value       | VARCHAR(16384) | Value of the tag              |

## INS_COLUMNS

| #   |   **Column Name**    | **Data Type** | **Description**               |
| --- | :------------------: | ------------ | ----------------------------- |
| 1   |  table_name         | VARCHAR(192)  | Table name                    |
| 2   |    db_name          | VARCHAR(64)   | Name of the database where this table is located |
| 3   |  table_type         | VARCHAR(21)   | Table type                    |
| 4   |   col_name          | VARCHAR(64)   | Name of the column            |
| 5   |   col_type          | VARCHAR(32)   | Type of the column            |
| 6   |  col_length         | INT           | Length of the column          |
| 7   | col_precision       | INT           | Precision of the column       |
| 8   |   col_scale         | INT           | Scale of the column           |
| 9   | col_nullable        | INT           | Indicates whether the column can be null |

## INS_USERS

Provides information about users created in the system. Users with SYSINFO set to 0 cannot view this table.

| #   |  **Column Name**  | **Data Type** | **Description**               |
| --- | :--------------: | ------------ | ----------------------------- |
| 1   |    name          | VARCHAR(24)   | Username                      |
| 2   |    super         | TINYINT       | Whether the user is a superuser, 1: yes, 0: no |
| 3   |   enable         | TINYINT       | Whether the user is enabled, 1: yes, 0: no |
| 4   |   sysinfo        | TINYINT       | Whether the user can view system information, 1: yes, 0: no |
| 5   | create_time      | TIMESTAMP     | Creation time                 |
| 6   | allowed_host     | VARCHAR(49152)| IP whitelist                  |

## INS_GRANTS

Provides information about enterprise edition authorizations. Users with SYSINFO set to 0 cannot view this table.

| #   |  **Column Name**  | **Data Type** | **Description**                                                                                                  |
| --- | :--------------: | ------------ | --------------------------------------------------------------------------------------------------------- |
| 1   |   version        | VARCHAR(9)    | Enterprise edition authorization description: official (officially authorized) / trial (trial version)                                                        |
| 2   |  cpu_cores       | VARCHAR(9)    | Number of CPU cores authorized for use                                                                                   |
| 3   |   dnodes         | VARCHAR(10)   | Number of dnode nodes authorized for use. Note that `dnodes` is a TDengine keyword and needs to be escaped with \`.     |
| 4   |   streams        | VARCHAR(10)   | Number of streams authorized for creation. Note that `streams` is a TDengine keyword and needs to be escaped with \`.             |
| 5   |    users         | VARCHAR(10)   | Number of users authorized for creation. Note that `users` is a TDengine keyword and needs to be escaped with \`.             |
| 6   |  accounts        | VARCHAR(10)   | Number of accounts authorized for creation. Note that `accounts` is a TDengine keyword and needs to be escaped with \`.          |
| 7   |   storage        | VARCHAR(21)   | Size of storage space authorized for use. Note that `storage` is a TDengine keyword and needs to be escaped with \`.       |
| 8   | connections      | VARCHAR(21)   | Number of client connections authorized for use. Note that `connections` is a TDengine keyword and needs to be escaped with \`. |
| 9   |  databases       | VARCHAR(11)   | Number of databases authorized for use. Note that `databases` is a TDengine keyword and needs to be escaped with \`.       |
| 10  |    speed         | VARCHAR(9)    | Number of data points per second authorized for use                                                                              |
| 11  |  querytime       | VARCHAR(9)    | Total duration for queries authorized for use                                                                                      |
| 12  |  timeseries      | VARCHAR(21)   | Number of measurement points authorized for use                                                                                        |
| 13  |   expired        | VARCHAR(5)    | Whether it has expired, true: expired, false: not expired                                                                       |
| 14  | expire_time      | VARCHAR(19)   | Trial expiration date                                                                                                    |

## INS_VGROUPS

Information about all vgroups in the system. Users with SYSINFO set to 0 cannot view this table.

| #   | **Column Name** | **Data Type** | **Description**                                                                                         |
| --- | :------------: | ------------ | ------------------------------------------------------------------------------------------------ |
| 1   | vgroup_id     | INT          | vgroup id                                                                                        |
| 2   |  db_name      | VARCHAR(32)   | Database name                                                                                         |
| 3   |  tables       | INT          | Number of tables in this vgroup. Note that `tables` is a TDengine keyword and needs to be escaped with \`. |
| 4   |  status       | VARCHAR(10)   | Status of this vgroup                                                                                 |
| 5   | v1_dnode      | INT          | ID of the dnode where the first member is located                                                     |
| 6   | v1_status     | VARCHAR(10)   | Status of the first member                                                                             |
| 7   | v2_dnode      | INT          | ID of the dnode where the second member is located                                                   |
| 8   | v2_status     | VARCHAR(10)   | Status of the second member                                                                            |
| 9   | v3_dnode      | INT          | ID of the dnode where the third member is located                                                    |
| 10  | v3_status     | VARCHAR(10)   | Status of the third member                                                                             |
| 11  |  nfiles       | INT          | Number of data/metadata files in this vgroup                                                            |
| 12  | file_size     | INT          | Size of data/metadata files in this vgroup                                                              |
| 13  |   tsma        | TINYINT      | Indicates whether this vgroup is specifically for Time-range-wise SMA, 1: yes, 0: no                        |

## INS_CONFIGS

System configuration parameters.

| #   | **Column Name** | **Data Type** | **Description**                                                                                |
| --- | :------------: | ------------ | --------------------------------------------------------------------------------------- |
| 1   |   name         | VARCHAR(32)   | Configuration item name                                                                  |
| 2   |  value         | VARCHAR(64)   | Value of the configuration item. Note that `value` is a TDengine keyword and needs to be escaped with \`. |

## INS_DNODE_VARIABLES

Configuration parameters for each dnode in the system. Users with SYSINFO set to 0 cannot view this table.

| #   | **Column Name** | **Data Type** | **Description**                                                                                |
| --- | :------------: | ------------ | --------------------------------------------------------------------------------------- |
| 1   | dnode_id       | INT          | ID of the dnode                                                                          |
| 2   |   name         | VARCHAR(32)   | Configuration item name                                                                  |
| 3   |  value         | VARCHAR(64)   | Value of the configuration item. Note that `value` is a TDengine keyword and needs to be escaped with \`. |

## INS_TOPICS

| #   |  **Column Name**   | **Data Type** | **Description**                       |
| --- | :---------------: | ------------ | ------------------------------ |
| 1   | topic_name        | VARCHAR(192)  | Topic name                     |
| 2   |   db_name         | VARCHAR(64)   | Database related to the topic   |
| 3   | create_time       | TIMESTAMP    | Creation time of the topic      |
| 4   |     sql           | VARCHAR(1024) | SQL statement used to create this topic |

## INS_SUBSCRIPTIONS

| #   |    **Column Name**    | **Data Type** | **Description**                 |
| --- | :-------------------: | ------------ | ---------------------- |
| 1   |   topic_name          | VARCHAR(204)  | Subscribed topic           |
| 2   | consumer_group        | VARCHAR(193)  | Subscriber's consumer group  |
| 3   |   vgroup_id           | INT          | vgroup id assigned to the consumer |
| 4   |  consumer_id          | BIGINT       | Unique id of the consumer     |
| 5   |     user               | VARCHAR(24)   | Username of the consumer       |
| 6   |      fqdn              | VARCHAR(128)  | FQDN of the machine where the consumer is located |
| 7   |     offset             | VARCHAR(64)   | Consumer's consumption progress |
| 8   |      rows              | BIGINT       | Number of data records consumed   |

## INS_STREAMS

| #   |   **Column Name**   | **Data Type** | **Description**                                                                                                             |
| --- | :----------------: | ------------ | -------------------------------------------------------------------------------------------------------------------- |
| 1   | stream_name        | VARCHAR(64)   | Stream calculation name                                                                                                           |
| 2   | create_time        | TIMESTAMP    | Creation time                                                                                                             |
| 3   |     sql            | VARCHAR(1024) | SQL statement provided when creating the stream calculation                                                                                          |
| 4   |    status          | VARCHAR(20)   | Current status of the stream                                                                                                           |
| 5   |  source_db         | VARCHAR(64)   | Source database                                                                                                           |
| 6   |  target_db         | VARCHAR(64)   | Target database                                                                                                           |
| 7   | target_table       | VARCHAR(192)  | Target table where the stream calculation writes results                                                                                                   |
| 8   |  watermark         | BIGINT       | Watermark, see SQL manual for stream calculations. Note that `watermark` is a TDengine keyword and needs to be escaped with \`.      |
| 9   |   trigger          | INT          | Push result computation mode, see SQL manual for stream calculations. Note that `trigger` is a TDengine keyword and needs to be escaped with \`. |

## INS_USER_PRIVILEGES

Note: Users with SYSINFO set to 0 cannot view this table.

| #    | **Column Name** | **Data Type**  | **Description**                      |
| ---- | :-------------: | -------------- | ------------------------------------ |
| 1    |    user_name    | VARCHAR(24)    | Username                             |
| 2    |    privilege    | VARCHAR(10)    | Permission description               |
| 3    |     db_name     | VARCHAR(65)    | Database name                        |
| 4    |   table_name    | VARCHAR(193)   | Table name                           |
| 5    |    condition    | VARCHAR(49152) | Subtable permission filter condition |


## INS_DISK_USAGE

| #   |   **Column Name**    | **Data type** | **Description**|
| --- | :----------: | ------------ | ------------------------|
| 1   | db_name      | VARCHAR(32)       | Database name 
| 2   | vgroup_id    | INT       | vgroup ID 
| 3   | wal          | BIGINT    | WAL file size, in KB
| 4   | data1        | BIGINT    | Data file size on primary storage, in KB 
| 5   | data2        | BIGINT    | Data file size on secondary storage, in KB
| 6   | data3        | BIGINT    | Data file size on tertiary storage, in KB 
| 7   | cache_rdb    | BIGINT    | Size of last/last_row files, in KB
| 8   | table_meta   | BIGINT    | Size of meta files, in KB 
| 9   | s3           | BIGINT    | Size occupied on S3, in KB
| 10  | raw_data     | BIGINT    | Estimated size of raw data, in KB

note: 