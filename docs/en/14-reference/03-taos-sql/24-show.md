---
title: SHOW Commands
slug: /tdengine-reference/sql-manual/show-commands
---

SHOW commands can be used to obtain brief system information. To get detailed metadata, system information, and status within the system, use the select statement to query tables in the INFORMATION_SCHEMA database.

## SHOW APPS

```sql
SHOW APPS;
```

Displays information about applications (clients) connected to the cluster.

## SHOW CLUSTER

```sql
SHOW CLUSTER;
```

Displays information about the current cluster.

## SHOW CLUSTER ALIVE

```sql
SHOW CLUSTER ALIVE;
```

Queries whether the current cluster is available, return values: 0: unavailable, 1: fully available, 2: partially available (some nodes in the cluster are offline, but other nodes can still be used normally).

## SHOW CLUSTER MACHINES

```sql
SHOW CLUSTER MACHINES; // Supported starting from TDengine version 3.2.3.0
```

Displays information about the cluster's machine codes.

Note: Exclusive to the enterprise edition.

## SHOW CONNECTIONS

```sql
SHOW CONNECTIONS;
```

Displays information about the connections that exist in the current system.

## SHOW CONSUMERS

```sql
SHOW CONSUMERS;
```

Displays information about all consumers in the current database.

## SHOW CREATE DATABASE

```sql
SHOW CREATE DATABASE db_name;
```

Displays the creation statement for the database specified by db_name.

## SHOW CREATE STABLE

```sql
SHOW CREATE STABLE [db_name.]stb_name;
```

Displays the creation statement for the supertable specified by stb_name.

## SHOW CREATE TABLE

```sql
SHOW CREATE TABLE [db_name.]tb_name
```

Displays the creation statement for the table specified by tb_name. Supports basic tables, supertables, and subtables.

## SHOW DATABASES

```sql
SHOW [USER | SYSTEM] DATABASES;
```

Displays all defined databases. SYSTEM specifies to only show system databases. USER specifies to only show user-created databases.

## SHOW DNODES

```sql
SHOW DNODES;
```

Displays information about DNODEs in the current system.

## SHOW FUNCTIONS

```sql
SHOW FUNCTIONS;
```

Displays user-defined custom functions.

## SHOW LICENCES

```sql
SHOW LICENCES;
SHOW GRANTS;
SHOW GRANTS FULL; // Supported starting from TDengine version 3.2.3.0
```

Displays information about enterprise edition license authorizations.

Note: Exclusive to the enterprise edition.

## SHOW INDEXES

```sql
SHOW INDEXES FROM tbl_name [FROM db_name];
SHOW INDEXES FROM [db_name.]tbl_name;
```

Displays created indexes.

## SHOW LOCAL VARIABLES

```sql
SHOW LOCAL VARIABLES [like pattern];
```

Displays the runtime values of configuration parameters for the current client.
You can use the like pattern to filter by name.

## SHOW MNODES

```sql
SHOW MNODES;
```

Displays information about MNODEs in the current system.

## SHOW QNODES

```sql
SHOW QNODES;
```

Displays information about QNODEs (query nodes) in the current system.

## SHOW QUERIES

```sql
SHOW QUERIES;
```

Displays ongoing queries in the current system.

## SHOW SCORES

```sql
SHOW SCORES;
```

Displays information about the capacity authorized by the license.

Note: Exclusive to the enterprise edition.

## SHOW STABLES

```sql
SHOW [db_name.]STABLES [LIKE 'pattern'];
```

Displays information about all supertables in the current database. You can use LIKE for fuzzy matching of table names.

## SHOW STREAMS

```sql
SHOW STREAMS;
```

Displays information about all stream computations in the current system.

## SHOW SUBSCRIPTIONS

```sql
SHOW SUBSCRIPTIONS;
```

Displays all subscription relationships in the current system.

## SHOW TABLES

```sql
SHOW [NORMAL | CHILD] [db_name.]TABLES [LIKE 'pattern'];
```

Displays information about all normal and child tables in the current database. You can use LIKE for fuzzy matching of table names. NORMAL specifies to display only basic table information, CHILD specifies to display only child table information.

## SHOW TABLE DISTRIBUTED

```sql
SHOW TABLE DISTRIBUTED table_name;
```

Displays the data distribution information of the table.

Example explanation:

Statement: show table distributed d0\G;   Displays the BLOCK distribution of table d0 vertically

<details>
 <summary>Display example</summary>

```text
*************************** 1.row ***************************

_block_dist: Total_Blocks=[5] Total_Size=[93.65 KB] Average_size=[18.73 KB] Compression_Ratio=[23.98 %]

Total_Blocks:  The number of blocks occupied by table d0 is 5

Total_Size:    The total size occupied by all blocks of table d0 in the file is 93.65 KB

Average_size:  The average space occupied by each block in the file is 18.73 KB

Compression_Ratio: Data compression ratio 23.98%

*************************** 2.row ***************************

_block_dist: Total_Rows=[20000] Inmem_Rows=[0] MinRows=[3616] MaxRows=[4096] Average_Rows=[4000]

Total_Rows:  Counts the number of rows stored on disk for table d0, 20000 rows (this number is for reference only, not an exact count. To get an exact count, use the count function)

Inmem_Rows:  Number of data rows stored in the write cache (not written to disk), 0 rows indicate there is no data in the memory cache

MinRows:    The minimum number of rows in a BLOCK, which is 3616 rows

MaxRows:    The maximum number of rows in a BLOCK, which is 4096 rows

Average_Rows: The average number of rows per BLOCK, currently 4000 rows

*************************** 3.row ***************************

_block_dist: Total_Tables=[1] Total_Files=[2] Total_Vgroups=[1]

Total_Tables:   Number of subtables, here is 1

Total_Files:   Number of data files in which table data is saved, here are 2 files

Total_Vgroups: Number of virtual nodes (vnode) the table data is distributed across

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

The above is a diagram showing the distribution of data rows in blocks. The numbers 0100, 0299, 0498, etc., represent the number of data rows in each block. It means that among the 5 blocks of this table, there is 1 block distributed between 3483 and 3681 rows, accounting for 20% of the total blocks, and 4 blocks are distributed between 3881 and 4096 (maximum number of rows), accounting for 80% of the total blocks, with 0 blocks in other areas.

Note that this will only display information about data blocks in the data file; information about data in the stt file will not be shown.

## SHOW TAGS

```sql
SHOW TAGS FROM child_table_name [FROM db_name];
SHOW TAGS FROM [db_name.]child_table_name;
```

Displays tag information for the child table.

## SHOW TOPICS

```sql
SHOW TOPICS;
```

Displays information about all topics in the current database.

## SHOW TRANSACTIONS

```sql
SHOW TRANSACTIONS;
SHOW TRANSACTION [transaction_id];
```

Displays information about one of or all transaction(s) currently being executed in the system (these transactions are only for metadata level, not for regular tables).

## SHOW USERS

```sql
SHOW USERS;
```

Displays information about all users in the current system, including user-defined users and system default users.

## SHOW CLUSTER VARIABLES (before version 3.0.1.6 it was SHOW VARIABLES)

```sql
SHOW CLUSTER VARIABLES [like pattern];
SHOW DNODE dnode_id VARIABLES [like pattern];
```

Displays the runtime values of configuration parameters that need to be the same across nodes in the current system, or you can specify a DNODE to view its configuration parameters. And you can use the like pattern to filter by name.

## SHOW VGROUPS

```sql
SHOW [db_name.]VGROUPS;
```

Displays information about all VGROUPs in the current database.

## SHOW VNODES

```sql
SHOW VNODES [ON DNODE dnode_id];
```

Displays information about all VNODEs or the VNODEs of a specific DNODE in the current system.
