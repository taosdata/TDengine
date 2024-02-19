---
title: Information_Schema Database
sidebar_label: Metadata
description: This document describes how to use the INFORMATION_SCHEMA database in TDengine.
---

TDengine includes a built-in database named `INFORMATION_SCHEMA` to provide access to database metadata, system information, and status information. This information includes database names, table names, and currently running SQL statements. All information related to TDengine maintenance is stored in this database. It contains several read-only tables. These tables are more accurately described as views, and they do not correspond to specific files. You can query these tables but cannot write data to them. The INFORMATION_SCHEMA database is intended to provide a unified method for SHOW commands to access data. However, using SELECT ... FROM INFORMATION_SCHEMA.tablename offers several advantages over SHOW commands:

1. You can use a USE statement to specify the INFORMATION_SCHEMA database as the current database.
2. You can use the familiar SELECT syntax to access information, provided that you know the table and column names.
3. You can filter and order the query results. More generally, you can use any SELECT syntax that TDengine supports to query the INFORMATION_SCHEMA database.
4. Future versions of TDengine can add new columns to INFORMATION_SCHEMA tables without affecting existing business systems.
5. It is easier for users coming from other database management systems. For example, Oracle users can query data dictionary tables.

:::info

- SHOW statements are still supported for the convenience of existing users.
- Some columns in the system table may be keywords, and you need to use the escape character '\`' when querying, for example, to query the VGROUPS in the database `test`:
```sql 
   select `vgroups` from ins_databases where name = 'test';
``` 

:::

This document introduces the tables of INFORMATION_SCHEMA and their structure.

## INS_DNODES

Provides information about dnodes. Similar to SHOW DNODES. Users whose SYSINFO attribute is 0 can't view this table.

| #   |   **Column**   | **Data Type** | **Description**                                                                                                                                          |
| --- | :------------: | ------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   |     vnodes     | SMALLINT      | Current number of vnodes on the dnode. It should be noted that `vnodes` is a TDengine keyword and needs to be escaped with ` when used as a column name. |
| 2   | support_vnodes | SMALLINT      | Maximum number of vnodes on the dnode                                                                                                                    |
| 3   |     status     | VARCHAR(10)    | Current status                                                                                                                                           |
| 4   |      note      | VARCHAR(256)   | Reason for going offline or other information                                                                                                            |
| 5   |       id       | SMALLINT      | Dnode ID                                                                                                                                                 |
| 6   |    endpoint    | VARCHAR(134)   | Dnode endpoint                                                                                                                                           |
| 7   |     create     | TIMESTAMP     | Creation time                                                                                                                                            |

## INS_MNODES

Provides information about mnodes. Similar to SHOW MNODES. Users whose SYSINFO attribute is 0 can't view this table.

| #   | **Column**  | **Data Type** | **Description**                            |
| --- | :---------: | ------------- | ------------------------------------------ |
| 1   |     id      | SMALLINT      | Mnode ID                                   |
| 2   |  endpoint   | VARCHAR(134)   | Mnode endpoint                             |
| 3   |    role     | VARCHAR(10)    | Current role                               |
| 4   |  role_time  | TIMESTAMP     | Time at which the current role was assumed |
| 5   | create_time | TIMESTAMP     | Creation time                              |

## INS_QNODES

Provides information about qnodes. Similar to SHOW QNODES. Users whose SYSINFO attribute is 0 can't view this table.

| #   | **Column**  | **Data Type** | **Description** |
| --- | :---------: | ------------- | --------------- |
| 1   |     id      | SMALLINT      | Qnode ID        |
| 2   |  endpoint   | VARCHAR(134)   | Qnode endpoint  |
| 3   | create_time | TIMESTAMP     | Creation time   |

## INS_SNODES

Provides information about snodes. Similar to SHOW SNODES. Users whose SYSINFO attribute is 0 can't view this table.

| #   | **Column**  | **Data Type** | **Description** |
| --- | :---------: | ------------- | --------------- |
| 1   |     id      | SMALLINT      | Snode ID        |
| 2   |  endpoint   | VARCHAR(134)   | Snode endpoint  |
| 3   | create_time | TIMESTAMP     | Creation time   |

## INS_CLUSTER

Provides information about the cluster. Users whose SYSINFO attribute is 0 can't view this table.

| #   | **Column**  | **Data Type** | **Description** |
| --- | :---------: | ------------- | --------------- |
| 1   |     id      | BIGINT        | Cluster ID      |
| 2   |    name     | VARCHAR(134)   | Cluster name    |
| 3   | create_time | TIMESTAMP     | Creation time   |

## INS_DATABASES

Provides information about user-created databases. Similar to SHOW DATABASES.

| #   |    **Column**        | **Data Type**    | **Description**                  |
| --- | :------------------: | ---------------- | ------------------------------------------------ |
| 1   | name                 | VARCHAR(64)      | Database name |
| 2   | create_time | TIMESTAMP    | Creation time           |
| 3   |       ntables        | INT              | Number of standard tables and subtables (not including supertables) |
| 4   |       vgroups        | INT              | Number of vgroups. It should be noted that `vnodes` is a TDengine keyword and needs to be escaped with ` when used as a column name.                            |
| 6   |       replica        | INT              | Number of replicas. It should be noted that `replica` is a TDengine keyword and needs to be escaped with ` when used as a column name.                                            |
| 7   |        strict        | VARCHAR(4)       | Obsoleted |
| 8   |       duration       | VARCHAR(10)      | Duration for storage of single files. It should be noted that `duration` is a TDengine keyword and needs to be escaped with ` when used as a column name.                          |
| 9   |         keep         | VARCHAR(32)      | Data retention period. It should be noted that `keep` is a TDengine keyword and needs to be escaped with ` when used as a column name.                                      |
| 10  |        buffer        | INT              | Write cache size per vnode, in MB. It should be noted that `buffer` is a TDengine keyword and needs to be escaped with ` when used as a column name.            |
| 11  |       pagesize       | INT              | Page size for vnode metadata storage engine, in KB. It should be noted that `pagesize` is a TDengine keyword and needs to be escaped with ` when used as a column name.    |
| 12  |        pages         | INT              | Number of pages per vnode metadata storage engine. It should be noted that `pages` is a TDengine keyword and needs to be escaped with ` when used as a column name.             |
| 13  |       minrows        | INT              | Maximum number of records per file block. It should be noted that `minrows` is a TDengine keyword and needs to be escaped with ` when used as a column name.                            |
| 14  |       maxrows        | INT              | Minimum number of records per file block. It should be noted that `maxrows` is a TDengine keyword and needs to be escaped with ` when used as a column name.                            |
| 15  |         comp         | INT              | Compression method. It should be noted that `comp` is a TDengine keyword and needs to be escaped with ` when used as a column name.                                      |
| 16  |      precision       | VARCHAR(2)       | Time precision. It should be noted that `precision` is a TDengine keyword and needs to be escaped with ` when used as a column name.                                       |
| 17  |        status        | VARCHAR(10)      | Current database status                                       |
| 18  |      retentions       | VARCHAR(60)     | Aggregation interval and retention period. It should be noted that `retentions` is a TDengine keyword and needs to be escaped with ` when used as a column name.                          |
| 19  |    single_stable     | BOOL             | Whether the database can contain multiple supertables. It should be noted that `single_stable` is a TDengine keyword and needs to be escaped with ` when used as a column name.            |
| 20  |      cachemodel      | VARCHAR(60)      | Caching method for the newest data. It should be noted that `cachemodel` is a TDengine keyword and needs to be escaped with ` when used as a column name.                |
| 21  |      cachesize       | INT              | Memory per vnode used for caching the newest data. It should be noted that `cachesize` is a TDengine keyword and needs to be escaped with ` when used as a column name.   |
| 22  |      wal_level       | INT              | WAL level. It should be noted that `wal_level` is a TDengine keyword and needs to be escaped with ` when used as a column name.                                      |
| 23  |   wal_fsync_period   | INT              | Interval at which WAL is written to disk. It should be noted that `wal_fsync_period` is a TDengine keyword and needs to be escaped with ` when used as a column name.   |
| 24  | wal_retention_period | INT              | WAL retention period, in second. It should be noted that `wal_retention_period` is a TDengine keyword and needs to be escaped with ` when used as a column name.                                    |
| 25  |  wal_retention_size  | INT              | Maximum WAL size. It should be noted that `wal_retention_size` is a TDengine keyword and needs to be escaped with ` when used as a column name.                                    |
| 26  |   stt_trigger   | SMALLINT | The threshold for number of files to trigger file merging. It should be noted that `stt_trigger` is a TDengine keyword and needs to be escaped with ` when used as a column name.  |
| 27  |   table_prefix   | SMALLINT | The prefix length in the table name that is ignored when distributing table to vnode based on table name. It should be noted that `table_prefix` is a TDengine keyword and needs to be escaped with ` when used as a column name.  |
| 28  |   table_suffix   | SMALLINT | The suffix length in the table name that is ignored when distributing table to vnode based on table name. It should be noted that `table_suffix` is a TDengine keyword and needs to be escaped with ` when used as a column name.  |
| 29  |   tsdb_pagesize   | INT | The page size for internal storage engine, its unit is KB. It should be noted that `tsdb_pagesize` is a TDengine keyword and needs to be escaped with ` when used as a column name.  |

## INS_FUNCTIONS

Provides information about user-defined functions.

| #   |  **Column**   | **Data Type** | **Description**                                                                                                                                                |
| --- | :-----------: | ------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   |     name      | VARCHAR(64)    | Function name                                                                                                                                                  |
| 2   |    comment    | VARCHAR(255)   | Function description. It should be noted that `comment` is a TDengine keyword and needs to be escaped with ` when used as a column name.                       |
| 3   |   aggregate   | INT           | Whether the UDF is an aggregate function. It should be noted that `aggregate` is a TDengine keyword and needs to be escaped with ` when used as a column name. |
| 4   |  output_type  | VARCHAR(31)    | Output data type                                                                                                                                               |
| 5   |  create_time  | TIMESTAMP     | Creation time                                                                                                                                                  |
| 6   |   code_len    | INT           | Length of the source code                                                                                                                                      |
| 7   |    bufsize    | INT           | Buffer size                                                                                                                                                    |
| 8   | func_language | VARCHAR(31)    | UDF programming language                                                                                                                                       |
| 9   |   func_body   | VARCHAR(16384) | UDF function body                                                                                                                                              |
| 10  | func_version  | INT           | UDF function version. starting from 0. Increasing by 1 each time it is updated                                                                                 |

## INS_INDEXES

Provides information about user-created indices. Similar to SHOW INDEX.

| #   |    **Column**    | **Data Type** | **Description**                                                       |
| --- | :--------------: | ------------- | --------------------------------------------------------------------- |
| 1   |     db_name      | VARCHAR(32)    | Database containing the table with the specified index                |
| 2   |    table_name    | VARCHAR(192)   | Table containing the specified index                                  |
| 3   |    index_name    | VARCHAR(192)   | Index name                                                            |
| 4   |     db_name      | VARCHAR(64)    | Index column                                                          |
| 5   |    index_type    | VARCHAR(10)    | SMA or tag index                                                      |
| 6   | index_extensions | VARCHAR(256)   | Other information For SMA/tag indices, this shows a list of functions |

## INS_STABLES

Provides information about supertables.

| #   |  **Column**   | **Data Type** | **Description**                                                                                                                                                           |
| --- | :-----------: | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   |  stable_name  | VARCHAR(192)   | Supertable name                                                                                                                                                           |
| 2   |    db_name    | VARCHAR(64)    | All databases in the supertable                                                                                                                                           |
| 3   |  create_time  | TIMESTAMP     | Creation time                                                                                                                                                             |
| 4   |    columns    | INT           | Number of columns                                                                                                                                                         |
| 5   |     tags      | INT           | Number of tags. It should be noted that `tags` is a TDengine keyword and needs to be escaped with ` when used as a column name.                                           |
| 6   |  last_update  | TIMESTAMP     | Last updated time                                                                                                                                                         |
| 7   | table_comment | VARCHAR(1024)  | Table description                                                                                                                                                         |
| 8   |   watermark   | VARCHAR(64)    | Window closing time. It should be noted that `watermark` is a TDengine keyword and needs to be escaped with ` when used as a column name.                                 |
| 9   |   max_delay   | VARCHAR(64)    | Maximum delay for pushing stream processing results. It should be noted that `max_delay` is a TDengine keyword and needs to be escaped with ` when used as a column name. |
| 10  |    rollup     | VARCHAR(128)   | Rollup aggregate function. It should be noted that `rollup` is a TDengine keyword and needs to be escaped with ` when used as a column name.                              |

## INS_TABLES

Provides information about standard tables and subtables.

| #   |  **Column**   | **Data Type** | **Description**                                                                                                                    |
| --- | :-----------: | ------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| 1   |  table_name   | VARCHAR(192)   | Table name                                                                                                                         |
| 2   |    db_name    | VARCHAR(64)    | Database name                                                                                                                      |
| 3   |  create_time  | TIMESTAMP     | Creation time                                                                                                                      |
| 4   |    columns    | INT           | Number of columns                                                                                                                  |
| 5   |  stable_name  | VARCHAR(192)   | Supertable name                                                                                                                    |
| 6   |      uid      | BIGINT        | Table ID                                                                                                                           |
| 7   |   vgroup_id   | INT           | Vgroup ID                                                                                                                          |
| 8   |      ttl      | INT           | Table time-to-live. It should be noted that `ttl` is a TDengine keyword and needs to be escaped with ` when used as a column name. |
| 9   | table_comment | VARCHAR(1024)  | Table description                                                                                                                  |
| 10  |     type      | VARCHAR(20)    | Table type                                                                                                                         |

## INS_TAGS

| #   | **Column**  | **Data Type** | **Description** |
| --- | :---------: | ------------- | --------------- |
| 1   | table_name  | VARCHAR(192)   | Table name      |
| 2   |   db_name   | VARCHAR(64)    | Database name   |
| 3   | stable_name | VARCHAR(192)   | Supertable name |
| 4   |  tag_name   | VARCHAR(64)    | Tag name        |
| 5   |  tag_type   | VARCHAR(64)    | Tag type        |
| 6   |  tag_value  | VARCHAR(16384) | Tag value       |

## INS_COLUMNS

| #   |  **Column**   | **Data Type** | **Description**  |
| --- | :-----------: | ------------- | ---------------- |
| 1   |  table_name   | VARCHAR(192)   | Table name       |
| 2   |    db_name    | VARCHAR(64)    | Database name    |
| 3   |  table_type   | VARCHAR(21)    | Table type       |
| 4   |   col_name    | VARCHAR(64)    | Column name      |
| 5   |   col_type    | VARCHAR(32)    | Column type      |
| 6   |  col_length   | INT           | Column length    |
| 7   | col_precision | INT           | Column precision |
| 8   |   col_scale   | INT           | Column scale     |
| 9   | col_nullable  | INT           | Column nullable  |

## INS_USERS

Provides information about TDengine users. Users whose SYSINFO attribute is 0 can't view this table.

| #   | **Column**  | **Data Type** | **Description**  |
| --- | :---------: | ------------- | ---------------- |
| 1   |    name      | VARCHAR(24)   | User name       |
| 2   |    super     | TINYINT       | Wether user is super user. 1 means yes; 0 means no. |
| 3   |   enable     | TINYINT       | Wether user is enabled. 1 means yes; 0 means no.    |
| 4   |   sysinfo    | TINYINT       | Wether user can query system info. 1 means yes; 0 means no. |
| 5   | create_time  | TIMESTAMP     | Create time     |
| 6   | allowed_host | VARCHAR(49152)| IP whitelist    |


## INS_GRANTS

Provides information about TDengine Enterprise Edition permissions. Users whose SYSINFO attribute is 0 can't view this table.

| #   | **Column**  | **Data Type** | **Description**                                                                                                                                                |
| --- | :---------: | ------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   |   version   | VARCHAR(9)     | Whether the deployment is a licensed or trial version                                                                                                          |
| 2   |  cpu_cores  | VARCHAR(9)     | CPU cores included in license                                                                                                                                  |
| 3   |   dnodes    | VARCHAR(10)    | Dnodes included in license. It should be noted that `dnodes` is a TDengine keyword and needs to be escaped with ` when used as a column name.                  |
| 4   |   streams   | VARCHAR(10)    | Streams included in license. It should be noted that `streams` is a TDengine keyword and needs to be escaped with ` when used as a column name.                |
| 5   |    users    | VARCHAR(10)    | Users included in license. It should be noted that `users` is a TDengine keyword and needs to be escaped with ` when used as a column name.                    |
| 6   |  accounts   | VARCHAR(10)    | Accounts included in license. It should be noted that `accounts` is a TDengine keyword and needs to be escaped with ` when used as a column name.              |
| 7   |   storage   | VARCHAR(21)    | Storage space included in license. It should be noted that `storage` is a TDengine keyword and needs to be escaped with ` when used as a column name.          |
| 8   | connections | VARCHAR(21)    | Client connections included in license. It should be noted that `connections` is a TDengine keyword and needs to be escaped with ` when used as a column name. |
| 9   |  databases  | VARCHAR(11)    | Databases included in license. It should be noted that `databases` is a TDengine keyword and needs to be escaped with ` when used as a column name.            |
| 10  |    speed    | VARCHAR(9)     | Write speed specified in license (data points per second)                                                                                                      |
| 11  |  querytime  | VARCHAR(9)     | Total query time specified in license                                                                                                                          |
| 12  | timeseries  | VARCHAR(21)    | Number of metrics included in license                                                                                                                          |
| 13  |   expired   | VARCHAR(5)     | Whether the license has expired                                                                                                                                |
| 14  | expire_time | VARCHAR(19)    | When the trial period expires                                                                                                                                  |

## INS_VGROUPS

Provides information about vgroups. Users whose SYSINFO attribute is 0 can't view this table.

| #   | **Column** | **Data Type** | **Description**                                                                                                                     |
| --- | :--------: | ------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| 1   | vgroup_id  | INT           | Vgroup ID                                                                                                                           |
| 2   |  db_name   | VARCHAR(32)    | Database name                                                                                                                       |
| 3   |   tables   | INT           | Tables in vgroup. It should be noted that `tables` is a TDengine keyword and needs to be escaped with ` when used as a column name. |
| 4   |   status   | VARCHAR(10)    | Vgroup status                                                                                                                       |
| 5   |  v1_dnode  | INT           | Dnode ID of first vgroup member                                                                                                     |
| 6   | v1_status  | VARCHAR(10)    | Status of first vgroup member                                                                                                       |
| 7   |  v2_dnode  | INT           | Dnode ID of second vgroup member                                                                                                    |
| 8   | v2_status  | VARCHAR(10)    | Status of second vgroup member                                                                                                      |
| 9   |  v3_dnode  | INT           | Dnode ID of third vgroup member                                                                                                     |
| 10  | v3_status  | VARCHAR(10)    | Status of third vgroup member                                                                                                       |
| 11  |   nfiles   | INT           | Number of data and metadata files in the vgroup                                                                                     |
| 12  | file_size  | INT           | Size of the data and metadata files in the vgroup                                                                                   |
| 13  |    tsma    | TINYINT       | Whether time-range-wise SMA is enabled. 1 means enabled; 0 means disabled.                                                          |

## INS_CONFIGS

Provides system configuration information.

| #   | **Column** | **Data Type** | **Description**                                                                                                         |
| --- | :--------: | ------------- | ----------------------------------------------------------------------------------------------------------------------- |
| 1   |    name    | VARCHAR(32)    | Parameter                                                                                                               |
| 2   |   value    | VARCHAR(64)    | Value. It should be noted that `value` is a TDengine keyword and needs to be escaped with ` when used as a column name. |

## INS_DNODE_VARIABLES

Provides dnode configuration information. Users whose SYSINFO attribute is 0 can't view this table.

| #   | **Column** | **Data Type** | **Description**                                                                                                         |
| --- | :--------: | ------------- | ----------------------------------------------------------------------------------------------------------------------- |
| 1   |  dnode_id  | INT           | Dnode ID                                                                                                                |
| 2   |    name    | VARCHAR(32)    | Parameter                                                                                                               |
| 3   |   value    | VARCHAR(64)    | Value. It should be noted that `value` is a TDengine keyword and needs to be escaped with ` when used as a column name. |

## INS_TOPICS

| #   | **Column**  | **Data Type** | **Description**                        |
| --- | :---------: | ------------- | -------------------------------------- |
| 1   | topic_name  | VARCHAR(192)   | Topic name                             |
| 2   |   db_name   | VARCHAR(64)    | Database for the topic                 |
| 3   | create_time | TIMESTAMP     | Creation time                          |
| 4   |     sql     | VARCHAR(1024)  | SQL statement used to create the topic |

## INS_SUBSCRIPTIONS

| #   |   **Column**   | **Data Type** | **Description**             |
| --- | :------------: | ------------- | --------------------------- |
| 1   |   topic_name   | VARCHAR(204)   | Subscribed topic            |
| 2   | consumer_group | VARCHAR(193)   | Subscribed consumer group   |
| 3   |   vgroup_id    | INT           | Vgroup ID for the consumer  |
| 4   |  consumer_id   | BIGINT        | Consumer ID                 |
| 5   |     offset     | VARCHAR(64)    | Consumption progress        |
| 6   |      rows      | BIGINT        | Number of consumption items |

## INS_STREAMS

| #   |  **Column**  | **Data Type** | **Description**                                                                                                                                                                                |
| --- | :----------: | ------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | stream_name  | VARCHAR(64)    | Stream name                                                                                                                                                                                    |
| 2   | create_time  | TIMESTAMP     | Creation time                                                                                                                                                                                  |
| 3   |     sql      | VARCHAR(1024)  | SQL statement used to create the stream                                                                                                                                                        |
| 4   |    status    | VARCHAR(20)    | Current status                                                                                                                                                                                 |
| 5   |  source_db   | VARCHAR(64)    | Source database                                                                                                                                                                                |
| 6   |  target_db   | VARCHAR(64)    | Target database                                                                                                                                                                                |
| 7   | target_table | VARCHAR(192)   | Target table                                                                                                                                                                                   |
| 8   |  watermark   | BIGINT        | Watermark (see stream processing documentation). It should be noted that `watermark` is a TDengine keyword and needs to be escaped with ` when used as a column name.                          |
| 9   |   trigger    | INT           | Method of triggering the result push (see stream processing documentation). It should be noted that `trigger` is a TDengine keyword and needs to be escaped with ` when used as a column name. |

## INS_USER_PRIVILEGES

Users whose SYSINFO attribute is 0 can't view this table.

| #   |   **Column**   | **Data Type** | **Description**                         |**                                                                               |
| --- | :----------: | ------------ | -------------------------------------------|
| 1   | user_name    | VARCHAR(24)       | Username                              |
| 2   | privilege    | VARCHAR(10)       | Privilege description                 |
| 3   | db_name      | VARCHAR(65)       | Database name                         |
| 4   | table_name   | VARCHAR(193)      | Table name                            |
| 5   | condition    | VARCHAR(49152)    | The privilege filter for child tables |
