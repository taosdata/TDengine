---
title: SHOW Statement for Metadata
sidebar_label: SHOW Statement
description: This document describes how to use the SHOW statement in TDengine.
---

`SHOW` command can be used to get brief system information. To get details about metadata, information, and status in the system, please use `select` to query the tables in database `INFORMATION_SCHEMA`.

## SHOW APPS

```sql
SHOW APPS;
```

Shows all clients (such as applications) that connect to the cluster.

## SHOW CLUSTER

```sql
SHOW CLUSTER;
```

Shows information about the current cluster.

## SHOW CLUSTER ALIVE

```sql
SHOW CLUSTER ALIVE;
```

It is used to check whether the cluster is available or not. Return value: 0 means unavailable, 1 means available, 2 means partially available (some dnodes are offline, the other dnodes are available)

## SHOW CONNECTIONS

```sql
SHOW CONNECTIONS;
```

Shows information about connections to the system.

## SHOW CONSUMERS

```sql
SHOW CONSUMERS;
```

Shows information about all consumers in the system.

## SHOW CREATE DATABASE

```sql
SHOW CREATE DATABASE db_name;
```

Shows the SQL statement used to create the specified database.

## SHOW CREATE STABLE

```sql
SHOW CREATE STABLE [db_name.]stb_name;
```

Shows the SQL statement used to create the specified supertable.

## SHOW CREATE TABLE

```sql
SHOW CREATE TABLE [db_name.]tb_name
```

Shows the SQL statement used to create the specified table. This statement can be used on supertables, standard tables, and subtables.

## SHOW DATABASES

```sql
SHOW [USER | SYSTEM] DATABASES;
```

Shows all databases. The `USER` qualifier specifies only user-created databases. The `SYSTEM` qualifier specifies only system databases.

## SHOW DNODES

```sql
SHOW DNODES;
```

Shows all dnodes in the system.

## SHOW FUNCTIONS

```sql
SHOW FUNCTIONS;
```

Shows all user-defined functions in the system.

## SHOW LICENCES

```sql
SHOW LICENCES;
SHOW GRANTS;
```

Shows information about the TDengine Enterprise Edition license.

Note: TDengine Enterprise Edition only.

## SHOW INDEXES

```sql
SHOW INDEXES FROM tbl_name [FROM db_name];
SHOW INDEXES FROM [db_name.]tbl_name;
```

Shows indices that have been created.

## SHOW LOCAL VARIABLES

```sql
SHOW LOCAL VARIABLES;
```

Shows the working configuration of the client.

## SHOW MNODES

```sql
SHOW MNODES;
```

Shows information about mnodes in the system.

## SHOW QNODES

```sql
SHOW QNODES;
```

Shows information about qnodes in the system.

## SHOW QUERIES

```sql
SHOW QUERIES;
```

Shows the queries in progress in the system.

## SHOW SCORES

```sql
SHOW SCORES;
```

Shows information about the storage space allowed by the license.

Note: TDengine Enterprise Edition only.

## SHOW STABLES

```sql
SHOW [db_name.]STABLES [LIKE 'pattern'];
```

Shows all supertables in the current database. You can use LIKE for fuzzy matching.

## SHOW STREAMS

```sql
SHOW STREAMS;
```

Shows information about streams in the system.

## SHOW SUBSCRIPTIONS

```sql
SHOW SUBSCRIPTIONS;
```

Shows all subscriptions in the system.

## SHOW TABLES

```sql
SHOW [NORMAL | CHILD] [db_name.]TABLES [LIKE 'pattern'];
```

Shows all standard tables and subtables in the current database. You can use LIKE for fuzzy matching. The `Normal` qualifier specifies standard tables. The `CHILD` qualifier specifies subtables.

## SHOW TABLE DISTRIBUTED

```sql
SHOW TABLE DISTRIBUTED table_name;
```

Shows how table data is distributed.

Examples: Below is an example of this command to display the block distribution of table `d0` in detailed format.

```sql
show table distributed d0\G;
```

<details>
 <summary> Show Example </summary>
 <pre><code>
*************************** 1.row ***************************
_block_dist: Total_Blocks=[5] Total_Size=[93.65 KB] Average_size=[18.73 KB] Compression_Ratio=[23.98 %]

Total_Blocks :  Table `d0` contains total 5 blocks

Total_Size:  The total size of all the data blocks in table `d0` is 93.65 KB

Average_size:  The average size of each block is 18.73 KB

Compression_Ratio: The data compression rate is 23.98%

*************************** 2.row ***************************
_block_dist: Total_Rows=[20000] Inmem_Rows=[0] MinRows=[3616] MaxRows=[4096] Average_Rows=[4000]

Total_Rows: Table `d0` contains 20,000 rows

Inmem_Rows: The rows still in memory, i.e. not committed in disk, is 0, i.e. none such rows

MinRows: The minimum number of rows in a block is 3,616

MaxRows: The maximum number of rows in a block is 4,096B

Average_Rows: The average number of rows in a block is 4,000

*************************** 3.row ***************************
_block_dist: Total_Tables=[1] Total_Files=[2]

Total_Tables: The number of child tables, 1 in this example

Total_Files: The number of files storing the table's data, 2 in this example

*************************** 4.row ***************************

_block_dist: --------------------------------------------------------------------------------

*************************** 5.row ***************************

_block_dist: 0100 |

*************************** 6.row ***************************

_block_dist: 0299 |

*************************** 7.row ***************************

_block_dist: 0498 |

*************************** 8.row ***************************

_block_dist: 0697 |

*************************** 9.row ***************************

_block_dist: 0896 |

*************************** 10.row ***************************

_block_dist: 1095 |

*************************** 11.row ***************************

_block_dist: 1294 |

*************************** 12.row ***************************

_block_dist: 1493 |

*************************** 13.row ***************************

_block_dist: 1692 |

*************************** 14.row ***************************

_block_dist: 1891 |

*************************** 15.row ***************************

_block_dist: 2090 |

*************************** 16.row ***************************

_block_dist: 2289 |

*************************** 17.row ***************************

_block_dist: 2488 |

*************************** 18.row ***************************

_block_dist: 2687 |

*************************** 19.row ***************************

_block_dist: 2886 |

*************************** 20.row ***************************

_block_dist: 3085 |

*************************** 21.row ***************************

_block_dist: 3284 |

*************************** 22.row ***************************

_block_dist: 3483 |||||||||||||||||  1 (20.00%)

*************************** 23.row ***************************

_block_dist: 3682 |

*************************** 24.row ***************************

_block_dist: 3881 |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||  4 (80.00%)

Query OK, 24 row(s) in set (0.002444s)

</code></pre>
</details>

The above show the block distribution percentage according to the number of rows in each block. In the above example, we can get below information:
- `_block_dist: 3483 |||||||||||||||||  1 (20.00%)` means there is one block whose rows is between 3,483 and 3,681.
-  `_block_dist: 3881 |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||  4 (80.00%)` means there are 4 blocks whose rows is between 3,881 and 4,096.   -  The number of blocks whose rows fall in other range is zero.

Note that only the information about the data blocks in the data file will be displayed here, and the information about the data in the stt file will not be displayed.

## SHOW TAGS

```sql
SHOW TAGS FROM child_table_name [FROM db_name];
SHOW TAGS FROM [db_name.]child_table_name;
```

Shows all tag information in a subtable.

## SHOW TOPICS

```sql
SHOW TOPICS;
```

Shows all topics in the current database.

## SHOW TRANSACTIONS

```sql
SHOW TRANSACTIONS;
```

Shows all running transactions in the system.

## SHOW USERS

```sql
SHOW USERS;
```

Shows information about users on the system. This includes user-created users and system-defined users.

## SHOW VARIABLES

```sql
SHOW VARIABLES;
SHOW DNODE dnode_id VARIABLES;
```

Shows the working configuration of the parameters that must be the same on each node. You can also specify a dnode to show the working configuration for that node.

## SHOW VGROUPS

```sql
SHOW [db_name.]VGROUPS;
```

Shows information about all vgroups in the current database.

## SHOW VNODES

```sql
SHOW VNODES [ON DNODE dnode_id];
```

Shows information about all vnodes in the system or about the vnodes for a specified dnode.
