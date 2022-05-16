---
title: Performance Optimization
---

After a TDengine cluster has been running for long enough time, because of updating data, deleting tables and deleting expired data, there may be fragments in data files and query performance may be impacted. To resolve the problem of fragments, from version 2.1.3.0 a new SQL command `COMPACT` can be used to defragment the data files.

```sql
COMPACT VNODES IN (vg_id1, vg_id2, ...)
```

`COMPACT` can be used to defragment one or more vgroups. The defragmentation work will be put in task queue for scheduling execution by TDengine. `SHOW VGROUPS` command can be used to get the vgroup ids to be used in `COMPACT` command. There is a column `compacting` in the output of `SHOW GROUPS` to indicate the compacting status of the vgroup: 2 means the vgroup is waiting in task queue for compacting, 1 means compacting is in progress, and 0 means the vgroup has nothing to do with compacting.

Please be noted that a lot of disk I/O is required for defragementation operation, during which the performance may be impacted significantly for data insertion and query, data insertion may be blocked shortly in extreme cases.

## Optimize Storage Parameters

The data in different use cases may have different characteristics, such as the days to keep, number of replicas, collection interval, record size, number of collection points, compression or not, etc. To achieve best efficiency in storage, the parameters in below table can be used, all of them can be either configured in `taos.cfg` as default configuration or in the command `create database`. For detailed definition of these parameters please refer to [Configuration Parameters](/reference/config/).

| #   | Parameter | Unit | Definition                                                                     | **Value Range**                                                                                 | **Default Value** |
| --- | --------- | ---- | ------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------- | ----------------- |
| 1   | days      | Day  | The time range of the data stored in a single data file                        | 1-3650                                                                                          | 10                |
| 2   | keep      | Day  | The number of days the data is kept in the database                            | 1-36500                                                                                         | 3650              |
| 3   | cache     | MB   | The size of each memory block                                                  | 1-128                                                                                           | 16                |
| 4   | blocks    | None | The number of memory blocks used by each vnode                                 | 3-10000                                                                                         | 6                 |
| 5   | quorum    | None | The number of required confirmation in case of multiple replicas               | 1-2                                                                                             | 1                 |
| 6   | minRows   | None | The minimum number of rows in a data file                                      | 10-1000                                                                                         | 100               |
| 7   | maxRows   | None | The maximum number of rows in a daa file                                       | 200-10000                                                                                       | 4096              |
| 8   | comp      | None | Whether to compress the data                                                   | 0：uncompressed; 1: One Phase compression; 2: Two Phase compression                             | 2                 |
| 9   | walLevel  | None | wal sync level (named as "wal" in create database )                            | 1：wal enabled without fsync; 2：wal enabled with fsync                                         | 1                 |
| 10  | fsync     | ms   | The time to wait for invoking fsync when walLevel is set to 2; 0 means no wait | 3000                                                                                            |
| 11  | replica   | none | The number of replications                                                     | 1-3                                                                                             | 1                 |
| 12  | precision | none | Time precision                                                                 | ms: millisecond; us: microsecond;ns: nanosecond                                                 | ms                |
| 13  | update    | none | Whether to allow updating data                                                 | 0: not allowed; 1: a row must be updated as whole; 2: a part of columns in a row can be updated | 0                 |
| 14  | cacheLast | none | Whether the latest data of a table is cached in memory                         | 0: not cached; 1: the last row is cached; 2: the latest non-NULL value of each column is cached | 0                 |

For a specific use case, there may be multiple kinds of data with different characteristics, it's best to put data with same characteristics in same database. So there may be multiple databases in a system while each database can be configured with different storage parameters to achieve best performance. The above parameters can be used when creating a database to override the default setting in configuration file.

```sql
 CREATE DATABASE demo DAYS 10 CACHE 32 BLOCKS 8 REPLICA 3 UPDATE 1;
```

The above SQL statement creates a database named as `demo`, in which each data file stores data across 10 days, the size of each memory block is 32 MB and each vnode is allocated with 8 blocks, the replica is set to 3, update operation is allowed, and all other parameters not specified in the command follow the default configuration in `taos.cfg`.

Once a database is created, only some parameters can be changed and be effective immediately while others are can't.

| **Parameter** | **Alterable** | **Value Range**  | **Syntax**                             |
| ------------- | ------------- | ---------------- | -------------------------------------- |
| name          |               |                  |                                        |
| create time   |               |                  |                                        |
| ntables       |               |                  |                                        |
| vgroups       |               |                  |                                        |
| replica       | **YES**       | 1-3              | ALTER DATABASE <dbname\> REPLICA _n_   |
| quorum        | **YES**       | 1-2              | ALTER DATABASE <dbname\> QUORUM _n_    |
| days          |               |                  |                                        |
| keep          | **YES**       | days-365000      | ALTER DATABASE <dbname\> KEEP _n_      |
| cache         |               |                  |                                        |
| blocks        | **YES**       | 3-1000           | ALTER DATABASE <dbname\> BLOCKS _n_    |
| minrows       |               |                  |                                        |
| maxrows       |               |                  |                                        |
| wal           |               |                  |                                        |
| fsync         |               |                  |                                        |
| comp          | **YES**       | 0-2              | ALTER DATABASE <dbname\> COMP _n_      |
| precision     |               |                  |                                        |
| status        |               |                  |                                        |
| update        |               |                  |                                        |
| cachelast     | **YES**       | 0 \| 1 \| 2 \| 3 | ALTER DATABASE <dbname\> CACHELAST _n_ |

**Explanation：** Prior to version 2.1.3.0, `taosd` server process needs to be restarted for these parameters to take in effect if they are changed using `ALTER DATABASE`.

When trying to join a new dnode into a running TDengine cluster, all the parameters related to cluster in the new dnode configuration must be consistent with the cluster, otherwise it can't join the cluster. The parameters that are checked when joining a dnode are as below. For detailed definition of these parameters please refer to [Configuration Parameters](/reference/config/).

- numOfMnodes
- mnodeEqualVnodeNum
- offlineThreshold
- statusInterval
- maxTablesPerVnode
- maxVgroupsPerDb
- arbitrator
- timezone
- balance
- flowctrl
- slaveQuery
- adjustMaster

For the convenience of debugging, the log setting of a dnode can be changed temporarily. The temporary change will be lost once the server is restarted.

```sql
ALTER DNODE <dnode_id> <config>
```

- dnode_id: from output of "SHOW DNODES"
- config: the parameter to be changed, as below
  - resetlog: close the old log file and create the new on
  - debugFlag: 131 (INFO/ERROR/WARNING), 135 (DEBUG), 143 (TRACE)

For example

```
alter dnode 1 debugFlag 135;
```
