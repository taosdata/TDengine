---
title: Syntax Changes in TDengine 3.0
slug: /tdengine-reference/sql-manual/syntax-changes
---

## Changes to SQL Basic Elements

| #    | **Element**                                               | **<div style={{width: 60}}>Difference</div>** | **Description**                                              |
| ---- | :-------------------------------------------------------- | :-------------------------------------------- | :----------------------------------------------------------- |
| 1    | VARCHAR                                                   | Added                                         | Alias for the BINARY type.                                   |
| 2    | TIMESTAMP literal                                         | Added                                         | Supports the syntax TIMESTAMP 'timestamp format'.            |
| 3    | _ROWTS pseudocolumn                                      | Added                                         | Represents the timestamp primary key. Alias for the _C0 pseudo column. |
| 4    | _IROWTS pseudocolumn                                     | Added                                         | Used to return the timestamp column corresponding to the interpolation result of the interp function. |
| 5    | INFORMATION_SCHEMA                                        | Added                                         | System database containing various SCHEMA definitions.       |
| 6    | PERFORMANCE_SCHEMA                                        | Added                                         | System database containing operational information.          |
| 7    | Continuous Query                                          | Deprecated                                    | Continuous queries are no longer supported. Various related syntax and interfaces are deprecated. |
| 8    | Mixed Operations                                          | Enhanced                                      | Comprehensive enhancement of mixed operations (scalar and vector operations mixed) in queries, all clauses of SELECT fully support mixed operations complying with syntax semantics. |
| 9    | Tag Operations                                            | Added                                         | In queries, tag columns can participate in various operations like normal columns, used in various clauses. |
| 10   | Timeline Clause and Time Functions for Supertable Queries | Enhanced                                      | Without PARTITION BY, the data of the supertable will be merged into one timeline. |
| 11   | GEOMETRY                                                  | Added                                         | Geometry type.                                               |

## SQL Statement Changes

In TDengine, the following data types can be used in the data model of basic tables.

| #    | **Statement**          | **<div style={{width: 60}}>Difference</div>** | **Description**                                              |
| ---- | :--------------------- | :-------------------------------------------- | :----------------------------------------------------------- |
| 1    | ALTER ACCOUNT          | Deprecated                                    | Was an enterprise feature in 2.x, no longer supported in 3.0. Syntax is temporarily retained, execution reports "This statement is no longer supported" error. |
| 2    | ALTER ALL DNODES       | Added                                         | Modify parameters of all DNODEs.                             |
| 3    | ALTER DATABASE         | Adjusted                                      | <p>Deprecated</p><ul><li>QUORUM: The number of replica confirmations needed for writing. The default behavior in version 3.0 is strong consistency, and weak consistency is not supported.</li><li>BLOCKS: The number of memory blocks used by VNODE. Version 3.0 uses BUFFER to represent the size of the VNODE write memory pool.</li><li>UPDATE: Support mode for update operations. All databases in version 3.0 support partial column updates.</li><li>CACHELAST: Mode for caching the latest row of data. Replaced by CACHEMODEL in version 3.0.</li><li>COMP: Not supported for modification in version 3.0.</li></ul><p>Added</p><ul><li>CACHEMODEL: Indicates whether to cache recent data of subtables in memory.</li><li>CACHESIZE: Indicates the memory size for caching recent data of subtables.</li><li>WAL_FSYNC_PERIOD: Replaces the original FSYNC parameter.</li><li>WAL_LEVEL: Replaces the original WAL parameter.</li><li>WAL_RETENTION_PERIOD: Added in version 3.0.4.0, extra retention strategy for wal files, used for data subscription.</li><li>WAL_RETENTION_SIZE: Added in version 3.0.4.0, extra retention strategy for wal files, used for data subscription.</li></ul><p>Adjusted</p><ul><li>KEEP: Version 3.0 added support for settings with units.</li></ul> |
| 4    | ALTER STABLE           | Adjusted                                      | Deprecated<ul><li>CHANGE tag: Change the name of the tag column. Replaced by RENAME tag in version 3.0.<br/>Added</li><li>RENAME tag: Replaces the original CHANGE tag clause.</li><li>COMMENT: Modify the comment of the supertable.</li></ul> |
| 5    | ALTER TABLE            | Adjusted                                      | Deprecated<ul><li>CHANGE tag: Change the name of the tag column. Replaced by RENAME tag in version 3.0.<br/>Added</li><li>RENAME tag: Replaces the original CHANGE tag clause.</li><li>COMMENT: Modify the comment of the table.</li><li>TTL: Modify the lifecycle of the table.</li></ul> |
| 6    | ALTER USER             | Adjusted                                      | Deprecated<ul><li>PRIVILEGE: Modify user permissions. Version 3.0 uses GRANT and REVOKE to grant and revoke permissions.<br/>Added</li><li>ENABLE: Enable or disable this user.</li><li>SYSINFO: Modify whether the user can view system information.</li></ul> |
| 7    | COMPACT VNODES         | Not supported                                 | Compact data of specified VNODE. Not supported in version 3.0.0. |
| 8    | CREATE ACCOUNT         | Deprecated                                    | Was an enterprise feature in 2.x, no longer supported in 3.0. Syntax is temporarily retained, execution reports "This statement is no longer supported" error. |
| 9    | CREATE DATABASE        | Adjusted                                      | <p>Deprecated</p><ul><li>BLOCKS: The number of memory blocks used by VNODE. Version 3.0 uses BUFFER to represent the size of the VNODE write memory pool.</li><li>CACHE: The size of memory blocks used by VNODE. Version 3.0 uses BUFFER to represent the size of the VNODE write memory pool.</li><li>CACHELAST: Mode for caching the latest row of data. Replaced by CACHEMODEL in version 3.0.</li><li>DAYS: The time span for storing data in data files. Replaced by DURATION in version 3.0.</li><li>FSYNC: When WAL is set to 2, the period for performing fsync. Replaced by WAL_FSYNC_PERIOD in version 3.0.</li><li>QUORUM: The number of replica confirmations needed for writing. Version 3.0 uses STRICT to specify strong or weak consistency.</li><li>UPDATE: Support mode for update operations. All databases in version 3.0 support partial column updates.</li><li>WAL: WAL level. Replaced by WAL_LEVEL in version 3.0.</li></ul><p>Added</p><ul><li>BUFFER: The size of the memory pool for a VNODE write.</li><li>CACHEMODEL: Indicates whether to cache recent data of subtables in memory.</li><li>CACHESIZE: Indicates the memory size for caching recent data of subtables.</li><li>DURATION: Replaces the original DAYS parameter. Added support for settings with units.</li><li>PAGES: The number of cache pages in the metadata storage engine of a VNODE.</li><li>PAGESIZE: The page size in the metadata storage engine of a VNODE.</li><li>RETENTIONS: Indicates the data aggregation period and retention duration.</li><li>STRICT: Indicates the consistency requirement for data synchronization.</li><li>SINGLE_STABLE: Indicates whether only one supertable can be created in this database.</li><li>VGROUPS: The initial number of VGROUPs in the database.</li><li>WAL_FSYNC_PERIOD: Replaces the original FSYNC parameter.</li><li>WAL_LEVEL: Replaces the original WAL parameter.</li><li>WAL_RETENTION_PERIOD: Extra retention strategy for wal files, used for data subscription.</li><li>WAL_RETENTION_SIZE: Extra retention strategy for wal files, used for data subscription.</li></ul><p>Adjusted</p><ul><li>KEEP: Version 3.0 added support for settings with units.</li></ul> |
| 10   | CREATE DNODE           | Adjusted                                      | Added syntax to specify hostname and port number separately<ul><li>CREATE DNODE dnode_host_name PORT port_val</li></ul> |
| 11   | CREATE INDEX           | Added                                         | Create SMA index.                                            |
| 12   | CREATE MNODE           | Added                                         | Create management node.                                      |
| 13   | CREATE QNODE           | Added                                         | Create query node.                                           |
| 14   | CREATE STABLE          | Adjusted                                      | Added table parameter syntax<li>COMMENT: Table comment.</li> |
| 15   | CREATE STREAM          | Added                                         | Create stream.                                               |
| 16   | CREATE TABLE           | Adjusted                                      | Added table parameter syntax<ul><li>COMMENT: Table comment.</li><li>WATERMARK: Specifies the closing time of the window.</li><li>MAX_DELAY: Used to control the maximum delay in pushing calculation results.</li><li>ROLLUP: Specifies the aggregation function, providing multi-level downsampling aggregation results.</li><li>SMA: Provides custom pre-computation based on data blocks.</li><li>TTL: Parameter used to specify the lifecycle of the table.</li></ul> |
| 17   | CREATE TOPIC           | Added                                         | Create subscription topic.                                   |
| 18   | DROP ACCOUNT           | Deprecated                                    | Was an enterprise feature in 2.x, no longer supported in 3.0. Syntax is temporarily retained, execution reports "This statement is no longer supported" error. |
| 19   | DROP CONSUMER GROUP    | Added                                         | Delete consumer group.                                       |
| 20   | DROP INDEX             | Added                                         | Delete index.                                                |
| 21   | DROP MNODE             | Added                                         | Create management node.                                      |
| 22   | DROP QNODE             | Added                                         | Create query node.                                           |
| 23   | DROP STREAM            | Added                                         | Delete stream.                                               |
| 24   | DROP TABLE             | Adjusted                                      | Added batch delete syntax                                    |
| 25   | DROP TOPIC             | Added                                         | Delete subscription topic.                                   |
| 26   | EXPLAIN                | Added                                         | View the execution plan of a query statement.                |
| 27   | GRANT                  | Added                                         | Grant user permissions.                                      |
| 28   | KILL TRANSACTION       | Added                                         | Terminate the transaction of the management node.            |
| 29   | KILL STREAM            | Deprecated                                    | Terminate continuous query. Version 3.0 no longer supports continuous queries, replaced by more general stream computing. |
| 31   | REVOKE                 | Added                                         | Revoke user permissions.                                     |
| 32   | SELECT                 | Adjusted                                      | <ul><li>SELECT closes implicit result columns, output columns must be specified by the SELECT clause.</li><li>DISTINCT functionality fully supported. Version 2.x only supported deduplication of tag columns and could not be mixed with JOIN, GROUP BY, etc.</li><li>JOIN functionality enhanced. Added support: OR conditions in WHERE conditions after JOIN; multi-table operations after JOIN; multi-table GROUP BY after JOIN.</li><li>Subquery functionality after FROM significantly enhanced. No limit on subquery nesting levels; supports mixed use of subqueries and UNION ALL; removes some other previous version syntax restrictions.</li><li>Any scalar expression can be used after WHERE.</li><li>GROUP BY functionality enhanced. Supports any scalar expression and their combinations for grouping.</li><li>SESSION can be used for supertables now. Without PARTITION BY, the data of the supertable will be merged into one timeline.</li><li>STATE_WINDOW can be used for supertables now. Without PARTITION BY, the data of the supertable will be merged into one timeline.</li><li>ORDER BY functionality significantly enhanced. No longer must be used with the GROUP BY clause; no longer has a limit on the number of sorting expressions; added support for NULLS FIRST/LAST syntax functionality; supports any expression complying with syntax semantics.</li><li>Added PARTITION BY syntax. Replaces the original GROUP BY tags.</li></ul> |
| 33   | SHOW ACCOUNTS          | Deprecated                                    | Was an enterprise feature in 2.x, no longer supported in 3.0. Syntax is temporarily retained, execution reports "This statement is no longer supported" error. |
| 34   | SHOW APPS              | Added                                         | Display information about applications (clients) accessing the cluster. |
| 35   | SHOW CONSUMERS         | Added                                         | Display information about all active consumers in the current database. |
| 36   | SHOW DATABASES         | Adjusted                                      | Version 3.0 only displays database names.                    |
| 37   | SHOW FUNCTIONS         | Adjusted                                      | Version 3.0 only displays custom function names.             |
| 38   | SHOW LICENCE           | Added                                         | Equivalent to the SHOW GRANTS command.                       |
| 39   | SHOW INDEXES           | Added                                         | Display created indexes.                                     |
| 40   | SHOW LOCAL VARIABLES   | Added                                         | Display runtime values of the current client's configuration parameters. |
| 41   | SHOW MODULES           | Deprecated                                    | Display information about components installed in the current system. |
| 42   | SHOW QNODES            | Added                                         | Display information about QNODEs in the current system.      |
| 43   | SHOW STABLES           | Adjusted                                      | Version 3.0 only displays supertable names.                  |
| 44   | SHOW STREAMS           | Adjusted                                      | Version 2.x this command displayed information about continuous queries created in the system. Version 3.0 has deprecated continuous queries, replaced by streams. This command displays created streams. |
| 45   | SHOW SUBSCRIPTIONS     | Added                                         | Display all subscription relationships in the current database |
| 46   | SHOW TABLES            | Adjusted                                      | Version 3.0 only displays table names.                       |
| 47   | SHOW TABLE DISTRIBUTED | Added                                         | Display table data distribution information. Replaces the SELECT _block_dist() FROM \{ tb_name |
| 48   | SHOW TOPICS            | Added                                         | Display all subscription topics in the current database.     |
| 49   | SHOW TRANSACTIONS      | Added                                         | Display information about transactions currently being executed in the system. |
| 50   | SHOW DNODE VARIABLES   | Added                                         | Display configuration parameters of a specified DNODE.       |
| 51   | SHOW VNODES            | Not supported                                 | Display information about VNODEs in the current system. Not supported in version 3.0.0. |
| 52   | TRIM DATABASE          | Added                                         | Delete expired data and reorganize data according to multi-level storage configuration. |
| 53   | REDISTRIBUTE VGROUP    | Added                                         | Adjust the distribution of VNODEs in a VGROUP.               |
| 54   | BALANCE VGROUP         | Added                                         | Automatically adjust the distribution of VNODEs in a VGROUP. |

## SQL Function Changes

| #    | **Function**  | **<div style={{width: 60}}>Differences</div>** | **Description**                                              |
| ---- | :------------ | :--------------------------------------------- | :----------------------------------------------------------- |
| 1    | TWA           | Enhanced                                       | Can now be used directly on supertables. Without PARTITION BY, the data of the supertable will be merged into one timeline. |
| 2    | IRATE         | Enhanced                                       | Can now be used directly on supertables. Without PARTITION BY, the data of the supertable will be merged into one timeline. |
| 3    | LEASTSQUARES  | Enhanced                                       | Can now be used on supertables.                              |
| 4    | ELAPSED       | Enhanced                                       | Can now be used directly on supertables. Without PARTITION BY, the data of the supertable will be merged into one timeline. |
| 5    | DIFF          | Enhanced                                       | Can now be used directly on supertables. Without PARTITION BY, the data of the supertable will be merged into one timeline. |
| 6    | DERIVATIVE    | Enhanced                                       | Can now be used directly on supertables. Without PARTITION BY, the data of the supertable will be merged into one timeline. |
| 7    | CSUM          | Enhanced                                       | Can now be used directly on supertables. Without PARTITION BY, the data of the supertable will be merged into one timeline. |
| 8    | MAVG          | Enhanced                                       | Can now be used directly on supertables. Without PARTITION BY, the data of the supertable will be merged into one timeline. |
| 9    | SAMPLE        | Enhanced                                       | Can now be used directly on supertables. Without PARTITION BY, the data of the supertable will be merged into one timeline. |
| 10   | STATECOUNT    | Enhanced                                       | Can now be used directly on supertables. Without PARTITION BY, the data of the supertable will be merged into one timeline. |
| 11   | STATEDURATION | Enhanced                                       | Can now be used directly on supertables. Without PARTITION BY, the data of the supertable will be merged into one timeline. |
| 12   | TIMETRUNCATE  | Enhanced                                       | Added ignore_timezone parameter, optional use, default value is 1. |

## SCHEMALESS Changes

| #    | **Element**                   | **<div style={{width: 60}}>Differences</div>** | **Description**                                              |
| ---- | :---------------------------- | :--------------------------------------------- | :----------------------------------------------------------- |
| 1    | Primary key ts changed to _ts | Changed                                        | Schemaless automatically created column names start with _, different from 2.x. |
