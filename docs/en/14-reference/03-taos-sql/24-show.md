---
sidebar_label: SHOW Commands
title: SHOW Commands
description: Complete list of SHOW commands
slug: /tdengine-reference/sql-manual/show-commands
---

The SHOW command can be used to obtain brief system information. For detailed various metadata, system information, and status in the system, please use the SELECT statement to query tables in the INFORMATION_SCHEMA database.

## SHOW APPS

```sql
SHOW APPS;
```

Displays information about applications (clients) accessing the cluster.

## SHOW CLUSTER

```sql
SHOW CLUSTER;
```

Displays information about the current cluster.

## SHOW CLUSTER ALIVE

```sql
SHOW CLUSTER ALIVE;
```

Queries whether the current cluster is available, returning: 0: unavailable, 1: fully available, 2: partially available (some nodes in the cluster are offline, but other nodes can still function normally).

## SHOW CLUSTER MACHINES

```sql
SHOW CLUSTER MACHINES; // Supported from TDengine version 3.2.3.0
```

Displays information such as machine codes in the cluster.

Note: Exclusive to the enterprise edition.

## SHOW CONNECTIONS

```sql
SHOW CONNECTIONS;
```

Displays information about the existing connections in the current system.

## SHOW CONSUMERS

```sql
SHOW CONSUMERS;
```

Displays information about all consumers in the current database.

## SHOW CREATE DATABASE

```sql
SHOW CREATE DATABASE db_name;
```

Displays the creation statement of the database specified by db_name.

## SHOW CREATE STABLE

```sql
SHOW CREATE STABLE [db_name.]stb_name;
```

Displays the creation statement of the supertable specified by stb_name.

## SHOW CREATE TABLE

```sql
SHOW CREATE TABLE [db_name.]tb_name;
```

Displays the creation statement of the table specified by tb_name. Supports basic tables, supertables, and subtables.

## SHOW DATABASES

```sql
SHOW [USER | SYSTEM] DATABASES;
```

Displays all defined databases. SYSTEM specifies to only show system databases. USER specifies to only show user-created databases.

## SHOW DNODES

```sql
SHOW DNODES;
```

Displays information about the DNODEs in the current system.

## SHOW FUNCTIONS

```sql
SHOW FUNCTIONS;
```

Displays user-defined custom functions.

## SHOW LICENCES

```sql
SHOW LICENCES;
SHOW GRANTS;
SHOW GRANTS FULL; // Supported from TDengine version 3.2.3.0
```

Displays information about enterprise edition license authorization.

Note: Exclusive to the enterprise edition.

## SHOW INDEXES

```sql
SHOW INDEXES FROM tbl_name [FROM db_name];
SHOW INDEXES FROM [db_name.]tbl_name;
```

Displays the indexes that have been created.

## SHOW LOCAL VARIABLES

```sql
SHOW LOCAL VARIABLES;
```

Displays the running values of the current client configuration parameters.

## SHOW MNODES

```sql
SHOW MNODES;
```

Displays information about the MNODEs in the current system.

## SHOW QNODES

```sql
SHOW QNODES;
```

Displays information about the QNODEs (query nodes) in the current system.

## SHOW QUERIES

```sql
SHOW QUERIES;
```

Displays the current queries being executed in the system.

## SHOW SCORES

```sql
SHOW SCORES;
```

Displays information about the capacity authorized to the system.

Note: Exclusive to the enterprise edition.

## SHOW STABLES

```sql
SHOW [db_name.]STABLES [LIKE 'pattern'];
```

Displays information about all supertables in the current database. You can use LIKE for fuzzy matching on table names.

## SHOW STREAMS

```sql
SHOW STREAMS;
```

Displays information about all stream calculations in the current system.

## SHOW SUBSCRIPTIONS

```sql
SHOW SUBSCRIPTIONS;
```

Displays all subscription relationships in the current system.

## SHOW TABLES

```sql
SHOW [NORMAL | CHILD] [db_name.]TABLES [LIKE 'pattern'];
```

Displays information about all basic tables and subtables in the current database. You can use LIKE for fuzzy matching on table names. NORMAL specifies to show only basic table information, CHILD specifies to show only subtable information.

## SHOW TABLE DISTRIBUTED

```sql
SHOW TABLE DISTRIBUTED table_name;
```

Displays data distribution information for the table.

Example explanation:

Statement: show table distributed d0\G;   Displays the block distribution of table d0 in a vertical format.

<details>
<summary>Display Example</summary>

```text
*************************** 1.row ***************************

_block_dist: Total_Blocks=[5] Total_Size=[93.65 KB] Average_size=[18.73 KB] Compression_Ratio=[23.98 %]

Total_Blocks:  The number of blocks occupied by table d0 is 5.

Total_Size:    The size of all blocks occupied by table d0 in files is 93.65 KB.

Average_size:  The average space occupied by each block in files is 18.73 KB.

Compression_Ratio: Data compression ratio 23.98%.


*************************** 2.row ***************************

_block_dist: Total_Rows=[20000] Inmem_Rows=[0] MinRows=[3616] MaxRows=[4096] Average_Rows=[4000]

Total_Rows:  The number of rows stored on disk for table d0 is 20000 (this value is for reference only and is not an accurate count. To get an accurate count, the count function should be used).

Inmem_Rows: The number of data rows stored in write cache (not yet on disk), 0 rows indicates that there is no data in the memory cache.

MinRows:    The minimum number of rows in a BLOCK is 3616.

MaxRows:    The maximum number of rows in a BLOCK is 4096.

Average_Rows: The average number of rows in each BLOCK at this time is 4000.


*************************** 3.row ***************************

_block_dist: Total_Tables=[1] Total_Files=[2] Total_Vgroups=[1]

Total_Tables:   The number of sub-tables, which is 1 here.

Total_Files: The number of data files where the table's data is stored separately, here it is 2 files.

Total_Vgroups: The number of virtual nodes (vnodes) that distribute the table's data.


*************************** 5.row ***************************

_block_dist: 0100 | 

*************************** 6.row ***************************

_block_dist: 0299 | 

......

*************************** 22.row ***************************

_block_dist: 3483 |||||||||||||||||  1 (20.00%)

*************************** 23.row ***************************

_block_dist: 3682 | 

*************************** 24.row ***************************

_block_dist: 3881 |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||  4 (80.00%)

Query OK, 24 row(s) in set (0.002444s)
```

</details>

The above shows the distribution of blocks containing the number of data rows. Here, 0100 0299 0498 … indicates the number of data rows in each block. The meaning above is that among the 5 blocks of this table, the block that distributes between 3483 and 3681 has 1 block, occupying 20% of the total blocks, and the blocks that distribute between 3881 and 4096 (the maximum number of rows) is 4 blocks, occupying 80% of the total blocks, while the number of blocks distributed in other areas is 0.

It is important to note that only the data block information in the data files will be displayed here; information in the stt files will not be displayed.

## SHOW TAGS

```sql
SHOW TAGS FROM child_table_name [FROM db_name];
SHOW TAGS FROM [db_name.]child_table_name;
```

Displays tag information of the subtable.

## SHOW TOPICS

```sql
SHOW TOPICS;
```

Displays information about all topics in the current database.

## SHOW TRANSACTIONS

```sql
SHOW TRANSACTIONS;
```

Displays information about the transactions currently being executed in the system (this transaction only pertains to metadata level, not basic tables).

## SHOW USERS

```sql
SHOW USERS;
```

Displays information about all users in the current system, including user-defined users and system default users.

## SHOW CLUSTER VARIABLES (before version 3.0.1.6 it was SHOW VARIABLES)

```sql
SHOW CLUSTER VARIABLES;
SHOW DNODE dnode_id VARIABLES;
```

Displays the running values of configuration parameters that need to be the same across nodes in the current system, and you can specify DNODE to view its configuration parameters.

## SHOW VGROUPS

```sql
SHOW [db_name.]VGROUPS;
```

Displays information about all VGROUPs in the current database.

## SHOW VNODES

```sql
SHOW VNODES [ON DNODE dnode_id];
```

Displays information about all VNODES in the current system or the VNODES on a specific DNODE.
