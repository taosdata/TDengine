---
title: Syntax Changes in TDengine 3.0
description: "Syntax change notes for TDengine version 3.0"
slug: /tdengine-reference/sql-manual/syntax-changes-in-tdengine-3
---

## Changes to SQL Basic Elements

| #    | **Element**                                               | **Difference** | **Description**                                              |
| ---- | :-------------------------------------------------------- | :------------- | :----------------------------------------------------------- |
| 1    | VARCHAR                                                   | New            | Alias for BINARY type.                                       |
| 2    | TIMESTAMP Literal                                         | New            | Supports TIMESTAMP 'timestamp format' syntax.                |
| 3    | _ROWTS Pseudo Column                                      | New            | Represents the timestamp primary key. Alias for _C0 pseudo column. |
| 4    | _IROWTS Pseudo Column                                     | New            | Used to return the timestamp column corresponding to the interp function interpolation result. |
| 5    | INFORMATION_SCHEMA                                        | New            | System database containing various SCHEMA definitions.       |
| 6    | PERFORMANCE_SCHEMA                                        | New            | System database containing runtime information.              |
| 7    | Continuous Query                                          | Deprecated     | Continuous queries are no longer supported. Related syntax and interfaces are deprecated. |
| 8    | Mixed Operations                                          | Enhanced       | Mixed operations (scalar and vector operations) in queries are fully enhanced, with all subclauses of SELECT fully supporting syntax semantics of mixed operations. |
| 9    | Tag Operations                                            | New            | In queries, tag columns can participate in various operations like ordinary columns for various clauses. |
| 10   | Timeline Clause and Time Functions for Supertable Queries | Enhanced       | Without PARTITION BY, the data of supertables will be merged into one timeline. |
| 11   | GEOMETRY                                                  | New            | Geometry type.                                               |

## Changes to SQL Statements

In TDengine, the data model of basic tables can use the following data types.

| #    | **Statement**          | **Difference** | **Description**                                              |
| ---- | :--------------------- | :------------- | :----------------------------------------------------------- |
| 1    | ALTER ACCOUNT          | Deprecated     | This was a feature in the enterprise version of 2.x, not supported in 3.0. The syntax is temporarily retained, executing results in "This statement is no longer supported" error. |
| 2    | ALTER ALL DNODES       | New            | Modify parameters for all DNODEs.                            |
| 3    | ALTER DATABASE         | Adjusted       | <p>Deprecated</p><ul><li>QUORUM: The number of replicas required for write confirmation. The default behavior in version 3.0 is strong consistency and does not support modification to weak consistency.</li><li>BLOCKS: The number of memory blocks used by VNODE. Version 3.0 uses BUFFER to represent the size of the VNODE write memory pool.</li><li>UPDATE: Support mode for update operations. Version 3.0 supports partial column updates for all databases.</li><li>CACHELAST: Mode for caching the latest row of data. Version 3.0 replaces it with CACHEMODEL.</li><li>COMP: Not supported for modification in version 3.0.</li></ul><p>New</p><ul><li>CACHEMODEL: Indicates whether to cache the most recent data of subtables in memory.</li><li>CACHESIZE: Indicates the memory size used to cache the most recent data of subtables.</li><li>WAL_FSYNC_PERIOD: Replaces the original FSYNC parameter.</li><li>WAL_LEVEL: Replaces the original WAL parameter.</li><li>WAL_RETENTION_PERIOD: New in version 3.0.4.0, additional retention strategy for wal files, used for data subscription.</li><li>WAL_RETENTION_SIZE: New in version 3.0.4.0, additional retention strategy for wal files, used for data subscription.</li></ul><p>Adjusted</p><ul><li>KEEP: Version 3.0 adds support for setting with units.</li></ul> |
| 4    | ALTER STABLE           | Adjusted       | Deprecated<ul><li>CHANGE TAG: Modify the name of a tag column. Version 3.0 uses RENAME TAG instead.<br/>New</li><li>RENAME TAG: Replaces the original CHANGE TAG clause.</li><li>COMMENT: Modifies the comment of the supertable.</li></ul> |
| 5    | ALTER TABLE            | Adjusted       | Deprecated<ul><li>CHANGE TAG: Modify the name of a tag column. Version 3.0 uses RENAME TAG instead.<br/>New</li><li>RENAME TAG: Replaces the original CHANGE TAG clause.</li><li>COMMENT: Modifies the table's comment.</li><li>TTL: Modifies the table's lifecycle.</li></ul> |
| 6    | ALTER USER             | Adjusted       | Deprecated<ul><li>PRIVILEGE: Modify user permissions. Version 3.0 uses GRANT and REVOKE to grant and revoke permissions.<br/>New</li><li>ENABLE: Enable or disable this user.</li><li>SYSINFO: Modify whether the user can view system information.</li></ul> |
| 7    | COMPACT VNODES         | Not Supported  | Organize the data of the specified VNODE. Not supported in version 3.0.0. |
| 8    | CREATE ACCOUNT         | Deprecated     | This was a feature in the enterprise version of 2.x, not supported in 3.0. The syntax is temporarily retained, executing results in "This statement is no longer supported" error. |
| 9    | CREATE DATABASE        | Adjusted       | <p>Deprecated</p><ul><li>BLOCKS: The number of memory blocks used by VNODE. Version 3.0 uses BUFFER to represent the size of the VNODE write memory pool.</li><li>CACHE: The size of the memory blocks used by VNODE. Version 3.0 uses BUFFER to represent the size of the VNODE write memory pool.</li><li>CACHELAST: Mode for caching the latest row of data. Version 3.0 replaces it with CACHEMODEL.</li><li>DAYS: The time span for data file storage. Version 3.0 replaces it with DURATION.</li><li>FSYNC: The period of executing fsync when WAL is set to 2. Version 3.0 replaces it with WAL_FSYNC_PERIOD.</li><li>QUORUM: The number of replicas required for write confirmation. Version 3.0 uses STRICT to specify strong or weak consistency.</li><li>UPDATE: Support mode for update operations. Version 3.0 supports partial column updates for all databases.</li><li>WAL: WAL level. Version 3.0 replaces it with WAL_LEVEL.</li></ul><p>New</p><ul><li>BUFFER: The size of a VNODE's write memory pool.</li><li>CACHEMODEL: Indicates whether to cache the most recent data of subtables in memory.</li><li>CACHESIZE: Indicates the memory size used to cache the most recent data of subtables.</li><li>DURATION: Replaces the original DAYS parameter. Adds support for setting with units.</li><li>PAGES: The number of cached pages in a VNODE's metadata storage engine.</li><li>PAGESIZE: The page size of the metadata storage engine in a VNODE.</li><li>RETENTIONS: Indicates the aggregation cycle and retention duration of the data.</li><li>STRICT: Indicates the consistency requirements for data synchronization.</li><li>SINGLE_STABLE: Indicates whether only one supertable can be created in this database.</li><li>VGROUPS: The initial number of VGROUPs in the database.</li><li>WAL_FSYNC_PERIOD: Replaces the original FSYNC parameter.</li><li>WAL_LEVEL: Replaces the original WAL parameter.</li><li>WAL_RETENTION_PERIOD: Additional retention strategy for wal files, used for data subscription.</li><li>WAL_RETENTION_SIZE: Additional retention strategy for wal files, used for data subscription.</li></ul><p>Adjusted</p><ul><li>KEEP: Version 3.0 adds support for setting with units.</li></ul> |
| 10   | CREATE DNODE           | Adjusted       | New syntax for specifying hostname and port separately<ul><li>CREATE DNODE dnode_host_name PORT port_val</li></ul> |
| 11   | CREATE INDEX           | New            | Create SMA index.                                            |
| 12   | CREATE MNODE           | New            | Create management node.                                      |
| 13   | CREATE QNODE           | New            | Create query node.                                           |
| 14   | CREATE STABLE          | Adjusted       | New table parameter syntax<li>COMMENT: Table comment.</li>   |
| 15   | CREATE STREAM          | New            | Create stream.                                               |
| 16   | CREATE TABLE           | Adjusted       | New table parameter syntax<ul><li>COMMENT: Table comment.</li><li>WATERMARK: Specify the closing time of the window.</li><li>MAX_DELAY: Control the maximum delay for pushing computation results.</li><li>ROLLUP: Specify aggregation functions, providing down-sampling aggregation results based on multiple levels.</li><li>SMA: Provide custom pre-calculation functions based on data blocks.</li><li>TTL: Parameter used to specify the lifecycle of the table.</li></ul> |
| 17   | CREATE TOPIC           | New            | Create subscription topic.                                   |
| 18   | DROP ACCOUNT           | Deprecated     | This was a feature in the enterprise version of 2.x, not supported in 3.0. The syntax is temporarily retained, executing results in "This statement is no longer supported" error. |
| 19   | DROP CONSUMER GROUP    | New            | Delete consumer group.                                       |
| 20   | DROP INDEX             | New            | Delete index.                                                |
| 21   | DROP MNODE             | New            | Create management node.                                      |
| 22   | DROP QNODE             | New            | Create query node.                                           |
| 23   | DROP STREAM            | New            | Delete stream.                                               |
| 24   | DROP TABLE             | Adjusted       | New syntax for batch deletion.                               |
| 25   | DROP TOPIC             | New            | Delete subscription topic.                                   |
| 26   | EXPLAIN                | New            | View the execution plan of a query statement.                |
| 27   | GRANT                  | New            | Grant user permissions.                                      |
| 28   | KILL TRANSACTION       | New            | Terminate the transaction management node.                   |
| 29   | KILL STREAM            | Deprecated     | Terminate continuous queries. Continuous queries are no longer supported in version 3.0 and are replaced by a more generic stream computation. |
| 31   | REVOKE                 | New            | Revoke user permissions.                                     |
| 32   | SELECT                 | Adjusted       | <ul><li>SELECT closes implicit result columns; all output columns must be specified in the SELECT clause.</li><li>DISTINCT functionality is fully supported. Version 2.x only supported deduplication of tag columns and could not be mixed with JOIN, GROUP BY, etc.</li><li>JOIN functionality is enhanced. Added support for: WHERE conditions with OR after JOIN; multi-table operations after JOIN; multi-table GROUP BY after JOIN.</li><li>Subquery functionality after FROM is significantly enhanced. No restriction on the number of nested subqueries; supports mixing subqueries with UNION ALL; removes some syntax restrictions from previous versions.</li><li>Arbitrary scalar expressions can be used after WHERE.</li><li>GROUP BY functionality is enhanced. Supports grouping by any scalar expression and their combinations.</li><li>SESSION can now be used for supertables. Without PARTITION BY, the data of supertables will be merged into one timeline.</li><li>STATE_WINDOW can now be used for supertables. Without PARTITION BY, the data of supertables will be merged into one timeline.</li><li>ORDER BY functionality is greatly enhanced. No longer must be used together with the GROUP BY clause; no longer has a limit on the number of sorting expressions; adds support for NULLS FIRST/LAST syntax; supports any expression that meets the syntax semantics.</li><li>New PARTITION BY syntax replaces the original GROUP BY tags.</li></ul> |
| 33   | SHOW ACCOUNTS          | Deprecated     | This was a feature in the enterprise version of 2.x, not supported in 3.0. The syntax is temporarily retained, executing results in "This statement is no longer supported" error. |
| 34   | SHOW APPS              | New            | Displays information about applications (clients) accessing the cluster. |
| 35   | SHOW CONSUMERS         | New            | Displays information about all active consumers in the current database. |
| 36   | SHOW DATABASES         | Adjusted       | Version 3.0 only displays database names.                    |
| 37   | SHOW FUNCTIONS         | Adjusted       | Version 3.0 only displays custom function names.             |
| 38   | SHOW LICENCE           | New            | Equivalent to the SHOW GRANTS command.                       |
| 39   | SHOW INDEXES           | New            | Displays the indexes that have been created.                 |
| 40   | SHOW LOCAL VARIABLES   | New            | Displays the runtime values of the current client configuration parameters. |
| 41   | SHOW MODULES           | Deprecated     | Displays information about the components installed in the current system. |
| 42   | SHOW QNODES            | New            | Displays information about QNODEs in the current system.     |
| 43   | SHOW STABLES           | Adjusted       | Version 3.0 only displays supertable names.                  |
| 44   | SHOW STREAMS           | Adjusted       | In version 2.x, this command displayed information about the continuous queries created in the system. Continuous queries are deprecated in version 3.0, replaced by streams. This command displays the streams that have been created. |
| 45   | SHOW SUBSCRIPTIONS     | New            | Displays all subscription relationships in the current database. |
| 46   | SHOW TABLES            | Adjusted       | Version 3.0 only displays table names.                       |
| 47   | SHOW TABLE DISTRIBUTED | New            | Displays the data distribution information of a table. Replaces the SELECT \_block_dist() FROM \{ tb_name |
| 48   | SHOW TOPICS            | New            | Displays all subscription topics in the current database.    |
| 49   | SHOW TRANSACTIONS      | New            | Displays information about transactions currently being executed in the system. |
| 50   | SHOW DNODE VARIABLES   | New            | Displays configuration parameters for the specified DNODE.   |
| 51   | SHOW VNODES            | Not Supported  | Displays information about VNODES in the current system. Not supported in version 3.0.0. |
| 52   | TRIM DATABASE          | New            | Deletes expired data and organizes data based on multi-level storage configuration. |
| 53   | REDISTRIBUTE VGROUP    | New            | Adjusts the distribution of VNODES in VGROUP.                |
| 54   | BALANCE VGROUP         | New            | Automatically adjusts the distribution of VNODES in VGROUP.  |

## Changes to SQL Functions

| #    | **Function**  | **Difference** | **Description**                                              |
| ---- | :------------ | :------------- | :----------------------------------------------------------- |
| 1    | TWA           | Enhanced       | Can now be used directly with supertables. Without PARTITION BY, the data of supertables will be merged into one timeline. |
| 2    | IRATE         | Enhanced       | Can now be used directly with supertables. Without PARTITION BY, the data of supertables will be merged into one timeline. |
| 3    | LEASTSQUARES  | Enhanced       | Can be used with supertables.                                |
| 4    | ELAPSED       | Enhanced       | Can now be used directly with supertables. Without PARTITION BY, the data of supertables will be merged into one timeline. |
| 5    | DIFF          | Enhanced       | Can now be used directly with supertables. Without PARTITION BY, the data of supertables will be merged into one timeline. |
| 6    | DERIVATIVE    | Enhanced       | Can now be used directly with supertables. Without PARTITION BY, the data of supertables will be merged into one timeline. |
| 7    | CSUM          | Enhanced       | Can now be used directly with supertables. Without PARTITION BY, the data of supertables will be merged into one timeline. |
| 8    | MAVG          | Enhanced       | Can now be used directly with supertables. Without PARTITION BY, the data of supertables will be merged into one timeline. |
| 9    | SAMPLE        | Enhanced       | Can now be used directly with supertables. Without PARTITION BY, the data of supertables will be merged into one timeline. |
| 10   | STATECOUNT    | Enhanced       | Can now be used directly with supertables. Without PARTITION BY, the data of supertables will be merged into one timeline. |
| 11   | STATEDURATION | Enhanced       | Can now be used directly with supertables. Without PARTITION BY, the data of supertables will be merged into one timeline. |
| 12   | TIMETRUNCATE  | Enhanced       | Adds ignore_timezone parameter, which is optional and defaults to 1. |

## Changes to SCHEMALESS

| #    | **Element**                    | **Difference** | **Description**                                              |
| ---- | :----------------------------- | :------------- | :----------------------------------------------------------- |
| 1    | Primary Key ts changed to \_ts | Change         | Automatically created column names in schemaless begin with an underscore, unlike 2.x. |
