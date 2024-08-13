---
title: Changes in TDengine 3.0
sidebar_label: Changes in TDengine 3.0
description: This document describes how TDengine SQL has changed in version 3.0 compared with previous versions.
---

## Basic SQL Elements

| # | **Element**  | **<div style={{width: 60}}>Change</div>** | **Description** |
| - | :------- | :-------- | :------- |
| 1 | VARCHAR | Added | Alias of BINARY.
| 2 | TIMESTAMP literal | Added | TIMESTAMP 'timestamp format' syntax now supported.
| 3 | _ROWTS pseudocolumn | Added | Indicates the primary key. Alias of _C0.
| 4 | _IROWTS pseudocolumn | Added | Used to retrieve timestamps with INTERP function.
| 5 | INFORMATION_SCHEMA | Added |	Database for system metadata containing all schema definitions
| 6 | PERFORMANCE_SCHEMA | Added | Database for system performance information.
| 7 | Connection queries | Deprecated | Connection queries are no longer supported. The syntax and interfaces are deprecated.
| 8 | Mixed operations | Enhanced | Mixing scalar and vector operations in queries has been enhanced and is supported in all SELECT clauses.
| 9 | Tag operations | Added | Tag columns can be used in queries and clauses like data columns.
| 10 | Timeline clauses and time functions in supertables | Enhanced | When PARTITION BY is not used, data in supertables is merged into a single timeline.
| 11 | GEOMETRY | Added | Geometry

## SQL Syntax

The following data types can be used in the schema for standard tables.

| # | **Statement**  | **<div style={{width: 60}}>Change</div>** | **Description** |
| - | :------- | :-------- | :------- |
| 1 | ALTER ACCOUNT | Deprecated| This Enterprise Edition-only statement has been removed. It returns the error "This statement is no longer supported."
| 2 | ALTER ALL DNODES | Added | Modifies the configuration of all dnodes.
| 3 | ALTER DATABASE | Modified	| Deprecated<ul><li>QUORUM: Specified the required number of confirmations. TDengine 3.0 provides strict consistency by default and doesn't allow to change to weak consistency. </li><li>BLOCKS: Specified the memory blocks used by each vnode. BUFFER is now used to specify the size of the write cache pool for each vnode. </li><li>UPDATE: Specified whether update operations were supported. All databases now support updating data in certain columns. </li><li>CACHELAST: Specified how to cache the newest row of data. CACHEMODEL now replaces CACHELAST. </li><li>COMP: Cannot be modified. <br/>Added</li><li>CACHEMODEL: Specifies whether to cache the latest subtable data. </li><li>CACHESIZE: Specifies the size of the cache for the newest subtable data. </li><li>WAL_FSYNC_PERIOD: Replaces the FSYNC parameter. </li><li>WAL_LEVEL: Replaces the WAL parameter. </li><li>WAL_RETENTION_PERIOD: specifies the time after which WAL files are deleted. This parameter is used for data subscription. </li><li>WAL_RETENTION_SIZE: specifies the size at which WAL files are deleted. This parameter is used for data subscription. <br/>Modified</li><li>REPLICA: Cannot be modified. </li><li>KEEP: Now supports units. </li></ul>
| 4 | ALTER STABLE | Modified | Deprecated<ul><li>CHANGE TAG: Modified the name of a tag. Replaced by RENAME TAG. <br/>Added</li><li>RENAME TAG: Replaces CHANGE TAG. </li><li>COMMENT: Specifies comments for a supertable. </li></ul>
| 5 | ALTER TABLE | Modified | Deprecated<ul><li>CHANGE TAG: Modified the name of a tag. Replaced by RENAME TAG. <br/>Added</li><li>RENAME TAG: Replaces CHANGE TAG. </li><li>COMMENT: Specifies comments for a standard table. </li><li>TTL: Specifies the time-to-live for a standard table. </li></ul>
| 6 | ALTER USER | Modified | Deprecated<ul><li>PRIVILEGE: Specified user permissions. Replaced by GRANT and REVOKE. <br/>Added</li><li>ENABLE: Enables or disables a user. </li><li>SYSINFO: Specifies whether a user can query system information. </li></ul>
| 7 | COMPACT VNODES | Not supported | Compacted the data on a vnode. Not supported.
| 8 | CREATE ACCOUNT | Deprecated| This Enterprise Edition-only statement has been removed. It returns the error "This statement is no longer supported."
| 9 | CREATE DATABASE | Modified	| Deprecated<ul><li>BLOCKS: Specified the number of blocks for each vnode. BUFFER is now used to specify the size of the write cache pool for each vnode. </li><li>CACHE: Specified the size of the memory blocks used by each vnode. BUFFER is now used to specify the size of the write cache pool for each vnode. </li><li>CACHELAST: Specified how to cache the newest row of data. CACHEMODEL now replaces CACHELAST. </li><li>DAYS: The length of time to store in a single file. Replaced by DURATION. </li><li>FSYNC: Specified the fsync interval when WAL was set to 2. Replaced by WAL_FSYNC_PERIOD. </li><li>QUORUM: Specified the number of confirmations required. STRICT is now used to specify strong or weak consistency. </li><li>UPDATE: Specified whether update operations were supported. All databases now support updating data in certain columns. </li><li>WAL: Specified the WAL level. Replaced by WAL_LEVEL. <br/>Added</li><li>BUFFER: Specifies the size of the write cache pool for each vnode. </li><li>CACHEMODEL: Specifies whether to cache the latest subtable data. </li><li>CACHESIZE: Specifies the size of the cache for the newest subtable data. </li><li>DURATION: Replaces DAYS. Now supports units. </li><li>PAGES: Specifies the number of pages in the metadata storage engine cache on each vnode. </li><li>PAGESIZE: specifies the size (in KB) of each page in the metadata storage engine cache on each vnode. </li><li>RETENTIONS: Specifies the aggregation interval and retention period </li><li>STRICT: Specifies whether strong data consistency is enabled. </li><li>SINGLE_STABLE: Specifies whether a database can contain multiple supertables. </li><li>VGROUPS: Specifies the initial number of vgroups when a database is created. </li><li>WAL_FSYNC_PERIOD: Replaces the FSYNC parameter. </li><li>WAL_LEVEL: Replaces the WAL parameter. </li><li>WAL_RETENTION_PERIOD: specifies the time after which WAL files are deleted. This parameter is used for data subscription. </li><li>WAL_RETENTION_SIZE: specifies the size at which WAL files are deleted. This parameter is used for data subscription. <br/>Modified</li><li>KEEP: Now supports units. </li></ul>
| 10 | CREATE DNODE | Modified | Now supports specifying hostname and port separately<ul><li>CREATE DNODE dnode_host_name PORT port_val</li></ul>
| 11 | CREATE INDEX	| Added | Creates an SMA index.
| 12 | CREATE MNODE	| Added | Creates an mnode.
| 13 | CREATE QNODE	| Added | Creates a qnode.
| 14 | CREATE STABLE | Modified	| New parameter added<li>COMMENT: Specifies comments for the supertable. </li>
| 15 | CREATE STREAM | Added | Creates a stream.
| 16 | CREATE TABLE | Modified | New parameters added<ul><li>COMMENT: Specifies comments for the table </li><li>WATERMARK: Specifies the window closing time. </li><li>MAX_DELAY: Specifies the maximum delay for pushing stream processing results. </li><li>ROLLUP: Specifies aggregate functions to roll up. Rolling up a function provides downsampled results based on multiple axes. </li><li>SMA: Provides user-defined precomputation of aggregates based on data blocks. </li><li>TTL: Specifies the time-to-live for a standard table. </li></ul>
| 17 | CREATE TOPIC | Added | Creates a topic.
| 18 | DROP ACCOUNT | Deprecated| This Enterprise Edition-only statement has been removed. It returns the error "This statement is no longer supported."
| 19 | DROP CONSUMER GROUP | Added | Deletes a consumer group.
| 20 | DROP INDEX	| Added | Deletes an index.
| 21 | DROP MNODE	| Added | Creates an mnode.
| 22 | DROP QNODE	| Added | Creates a qnode.
| 23 | DROP STREAM	| Added | Deletes a stream.
| 24 | DROP TABLE | Modified | Added batch deletion syntax.
| 25 | DROP TOPIC | Added | Deletes a topic.
| 26 | EXPLAIN | Added | Query the execution plan of a query statement.
| 27 | GRANT | Added | Grants permissions to a user.
| 28 | KILL TRANSACTION | Added | Terminates an mnode transaction.
| 29 | KILL STREAM | Deprecated | Terminated a continuous query. The continuous query feature has been replaced with the stream processing feature.
| 31 | REVOKE | Added | Revokes permissions from a user.
| 32 | SELECT	| Modified | <ul><li>SELECT does not use the implicit results column. Output columns must be specified in the SELECT clause. </li><li>DISTINCT support is enhanced. In previous versions, DISTINCT only worked on the tag column and could not be used with JOIN or GROUP BY. </li><li>JOIN support is enhanced. The following are now supported after JOIN: a WHERE clause with OR, operations on multiple tables, and GROUP BY on multiple tables. </li><li>Subqueries after FROM are enhanced. Levels of nesting are no longer restricted. Subqueries can be used with UNION ALL. Other syntax restrictions are eliminated. </li><li>All scalar functions can be used after WHERE. </li><li>GROUP BY is enhanced. You can group by any scalar expression or combination thereof. </li><li>SESSION can be used on supertables. When PARTITION BY is not used, data in supertables is merged into a single timeline. </li><li>STATE_WINDOW can be used on supertables. When PARTITION BY is not used, data in supertables is merged into a single timeline. </li><li>ORDER BY is enhanced. It is no longer required to use ORDER BY and GROUP BY together. There is no longer a restriction on the number of order expressions. NULLS FIRST and NULLS LAST syntax has been added. Any expression that conforms to the ORDER BY semantics can be used. </li><li>Added PARTITION BY syntax. PARTITION BY replaces GROUP BY tags. </li></ul>
| 33 | SHOW ACCOUNTS | Deprecated | This Enterprise Edition-only statement has been removed. It returns the error "This statement is no longer supported."
| 34 | SHOW APPS | Added | Shows all clients (such as applications) that connect to the cluster.
| 35 | SHOW CONSUMERS	| Added | Shows information about all active consumers in the system.
| 36 | SHOW DATABASES	| Modified | Only shows database names.
| 37 | SHOW FUNCTIONS	| Modified | Only shows UDF names.
| 38 | SHOW LICENCE | Added | Alias of SHOW GRANTS.
| 39 | SHOW INDEXES | Added | Shows indices that have been created.
| 40 | SHOW LOCAL VARIABLES | Added | Shows the working configuration of the client.
| 41 | SHOW MODULES	| Deprecated | Shows information about modules installed in the system.
| 42 | SHOW QNODES	| Added | Shows information about qnodes in the system.
| 43 | SHOW STABLES	| Modified | Only shows supertable names.
| 44 | SHOW STREAMS	| Modified | This statement previously showed continuous queries. The continuous query feature has been replaced with the stream processing feature. This statement now shows streams that have been created.
| 45 | SHOW SUBSCRIPTIONS | Added | Shows all subscriptions in the current database.
| 46 | SHOW TABLES | Modified | Only shows table names.
| 47 | SHOW TABLE DISTRIBUTED | Added | Shows how table data is distributed. This replaces the `SELECT _block_dist() FROM &lcub; tb_name | stb_name &rcub;` command.
| 48 | SHOW TOPICS | Added | Shows all subscribed topics in the current database.
| 49 | SHOW TRANSACTIONS | Added | Shows all running transactions in the system.
| 50 | SHOW DNODE VARIABLES | Added | Shows the configuration of the specified dnode.
| 51 | SHOW VNODES | Not supported | Shows information about vnodes in the system. Not supported.
| 52 | TRIM DATABASE | Added | Deletes data that has expired and orders the remaining data in accordance with the storage configuration.
| 53 | REDISTRIBUTE VGROUP | Added | Adjust the distribution of VNODES in VGROUP.
| 54 | BALANCE VGROUP | Added | Auto adjust the distribution of VNODES in VGROUP.

## SQL Functions

| # | **Function**  | ** <div style={{width: 60}}>Change</div> ** | **Description** |
| - | :------- | :-------- | :------- |
| 1 | TWA	| Added | Can be used on supertables. When PARTITION BY is not used, data in supertables is merged into a single timeline.
| 2 | IRATE | Enhanced | Can be used on supertables. When PARTITION BY is not used, data in supertables is merged into a single timeline.
| 3 | LEASTSQUARES | Enhanced | Can be used on supertables.
| 4 | ELAPSED | Enhanced | Can be used on supertables. When PARTITION BY is not used, data in supertables is merged into a single timeline.
| 5 | DIFF | Enhanced | Can be used on supertables. When PARTITION BY is not used, data in supertables is merged into a single timeline.
| 6 | DERIVATIVE | Enhanced | Can be used on supertables. When PARTITION BY is not used, data in supertables is merged into a single timeline.
| 7 | CSUM | Enhanced | Can be used on supertables. When PARTITION BY is not used, data in supertables is merged into a single timeline.
| 8 | MAVG | Enhanced | Can be used on supertables. When PARTITION BY is not used, data in supertables is merged into a single timeline.
| 9 | SAMPLE | Enhanced | Can be used on supertables. When PARTITION BY is not used, data in supertables is merged into a single timeline.
| 10 | STATECOUNT | Enhanced | Can be used on supertables. When PARTITION BY is not used, data in supertables is merged into a single timeline.
| 11 | STATEDURATION | Enhanced | Can be used on supertables. When PARTITION BY is not used, data in supertables is merged into a single timeline.
