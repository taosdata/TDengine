---
sidebar_label: SHOW Commands
title: SHOW Commands
description: SHOW statements for cluster and system information
---

TDengine provides `SHOW` commands for obtaining brief system information. For more detailed metadata, system information, and status, use `SELECT` to query tables in `INFORMATION_SCHEMA` (see [Metadata Views](./01-meta.md)) or performance statistics views in `PERFORMANCE_SCHEMA` (see [Performance Data Views](./02-perf.md)).

The `SHOW` statements supported by the current version are listed below in alphabetical order by command name.

## SHOW ALIVE

```sql
SHOW [db_name.]ALIVE;
```

Queries whether the specified database is available. If no database name is specified, queries the current database. The return values have the same meanings as those of [`SHOW CLUSTER ALIVE`](#show-cluster-alive): `0` means unavailable, `1` means fully available, and `2` means partially available.

## SHOW ANODES

```sql
SHOW ANODES;
SHOW ANODES FULL;
```

Displays information about analysis nodes (anodes). `SHOW ANODES FULL` additionally displays details about the algorithms loaded on each node. For the complete set of fields, see [`INS_ANODES`](./01-meta.md#ins_anodes) / [`INS_ANODES_FULL`](./01-meta.md#ins_anodes_full).

## SHOW APPS

```sql
SHOW APPS;
```

Displays information about applications (clients) connected to the cluster. For the complete set of fields, see [`PERF_APPS`](./02-perf.md#perf_apps).

## SHOW ARBGROUPS

```sql
SHOW ARBGROUPS;
```

Displays information about arbitration groups (arbgroups), including replica `dnode` information, synchronization status, and assigned tokens. For the complete set of fields, see [`INS_ARBGROUPS`](./01-meta.md#ins_arbgroups).

## SHOW BNODES

```sql
SHOW BNODES;
```

Displays information about bridge nodes (bnodes). For the complete set of fields, see [`INS_BNODES`](./01-meta.md#ins_bnodes).

## SHOW CLUSTER

```sql
SHOW CLUSTER;
```

Displays information about the current cluster. For the complete set of fields, see [`INS_CLUSTER`](./01-meta.md#ins_cluster).

## SHOW CLUSTER ALIVE

```sql
SHOW CLUSTER ALIVE;
```

Queries whether the current cluster is available. The return values are as follows:

- `0`: Unavailable
- `1`: Fully available
- `2`: Partially available (some nodes are offline, but the other nodes remain available)

## SHOW CLUSTER MACHINES

```sql
SHOW CLUSTER MACHINES;
```

Displays cluster machine codes and related information. For the complete set of fields, see [`INS_MACHINES`](./01-meta.md#ins_machines).

**Notes**

- Enterprise Edition only
- Available since `v3.2.3.0`

## SHOW CLUSTER VARIABLES

```sql
SHOW VARIABLES [LIKE 'pattern'];
SHOW CLUSTER VARIABLES [LIKE 'pattern'];
SHOW DNODE dnode_id VARIABLES [LIKE 'pattern'];
```

Displays the runtime values of configuration parameters that must remain consistent across nodes. You can also specify a `dnode` to view its configuration. Use `LIKE` to filter by parameter name. `SHOW VARIABLES` and `SHOW CLUSTER VARIABLES` are equivalent.

**Notes**

- Before `v3.0.1.6`, only `SHOW VARIABLES` was supported.

## SHOW COMPACTS

```sql
SHOW COMPACTS;
SHOW COMPACT compact_id;
```

Displays the list of data compaction tasks. `SHOW COMPACT compact_id` displays detailed progress for the specified task on each `vgroup`/`dnode`. For the complete set of fields, see [`INS_COMPACTS`](./01-meta.md#ins_compacts) / [`INS_COMPACT_DETAILS`](./01-meta.md#ins_compact_details).

## SHOW CONNECTIONS

```sql
SHOW CONNECTIONS;
```

Displays information about connections in the current system. For the complete set of fields, see [`PERF_CONNECTIONS`](./02-perf.md#perf_connections).

## SHOW CONSUMERS

```sql
SHOW CONSUMERS;
```

Displays information about all consumers in the current database. For the complete set of fields, see [`PERF_CONSUMERS`](./02-perf.md#perf_consumers).

## SHOW CPU_ALLOCATION

```sql
SHOW CPU_ALLOCATION;
```

Displays the CPU core allocation status of the three thread categories (management, write, and read) on all `dnode`s in the cluster. This information is meaningful only when the `enableCpuAffinity` configuration parameter is enabled. Each `dnode` returns three rows with the following columns:

| **Column Name** | **Data Type** | **Description** |
| --- | --- | --- |
| `dnode_id` | INT | `dnode` identifier |
| `thread_category` | VARCHAR(16) | Thread category: `management`, `write`, or `read` |
| `cores` | INT | Number of CPU cores allocated to this category (`0` when disabled) |
| `core_ids` | VARCHAR(256) | Comma-separated list of assigned core IDs; `"-"` when disabled |
| `enabled` | BOOL | Whether CPU affinity is enabled for this category |

When `enableCpuAffinity` is disabled (the default), all rows show `enabled=false`, `cores=0`, and `core_ids="-"`. For the complete set of fields, see [`INS_CPU_ALLOCATION`](./01-meta.md#ins_cpu_allocation).

## SHOW CREATE DATABASE

```sql
SHOW CREATE DATABASE db_name;
```

Displays the creation statement for the database specified by `db_name`.

## SHOW CREATE RSMA

```sql
SHOW CREATE RSMA [db_name.]rsma_name;
```

Displays the creation statement for the specified RSMA.

## SHOW CREATE STABLE

```sql
SHOW CREATE STABLE [db_name.]stb_name;
```

Displays the creation statement for the supertable specified by `stb_name`.

## SHOW CREATE STREAM

```sql
SHOW CREATE STREAM [db_name.]stream_name;
```

Displays the creation statement for the stream specified by `stream_name`.

**Notes**

- Available since `v3.4.1.13`

## SHOW CREATE TABLE

```sql
SHOW CREATE TABLE [db_name.]tb_name;
```

Displays the creation statement for the table specified by `tb_name`. Normal tables, supertables, and subtables are supported.

## SHOW CREATE VIEW

```sql
SHOW CREATE VIEW [db_name.]view_name;
```

Displays the creation statement for the specified view.

## SHOW CREATE VTABLE

```sql
SHOW CREATE VTABLE [db_name.]vtable_name;
```

Displays the creation statement for a virtual table. For virtual subtables that use tag references, the result preserves the corresponding tag reference definitions.

## SHOW DATABASES

```sql
SHOW [USER | SYSTEM] DATABASES;
```

Displays the list of databases. `SYSTEM` displays only system databases, and `USER` displays only user-created databases. For the complete set of fields, see [`INS_DATABASES`](./01-meta.md#ins_databases).

## SHOW DISK_INFO

```sql
SHOW [db_name.]DISK_INFO;
```

Displays database disk usage information, including WAL, multi-tier storage, cache, and metadata usage. For the complete set of fields, see [`INS_DISK_USAGE`](./01-meta.md#ins_disk_usage).

## SHOW DNODES

```sql
SHOW DNODES;
```

Displays information about `dnode`s in the current system. For the complete set of fields, see [`INS_DNODES`](./01-meta.md#ins_dnodes).

## SHOW ENCRYPTIONS

```sql
SHOW ENCRYPTIONS;
```

Displays the encryption key status of each `dnode`. For the complete set of fields, see [`INS_ENCRYPTIONS`](./01-meta.md#ins_encryptions).

## SHOW ENCRYPT_ALGORITHMS

```sql
SHOW ENCRYPT_ALGORITHMS;
```

Displays the list of available encryption algorithms. For the complete set of fields, see [`INS_ENCRYPT_ALGORITHMS`](./01-meta.md#ins_encrypt_algorithms).

## SHOW ENCRYPT_STATUS

```sql
SHOW ENCRYPT_STATUS;
```

Displays the current encryption scope, algorithm, and status. For the complete set of fields, see [`INS_ENCRYPT_STATUS`](./01-meta.md#ins_encrypt_status).

## SHOW EXTERNAL SOURCES

```sql
SHOW EXTERNAL SOURCES;
```

Displays information about external data sources for federated queries. For the complete set of fields, see [`INS_EXT_SOURCES`](./01-meta.md#ins_ext_sources).

## SHOW FUNCTIONS

```sql
SHOW FUNCTIONS;
```

Displays user-defined functions. For the complete set of fields, see [`INS_FUNCTIONS`](./01-meta.md#ins_functions).

## SHOW INDEXES

```sql
SHOW INDEXES FROM tbl_name [FROM db_name];
SHOW INDEXES FROM [db_name.]tbl_name;
```

Displays created indexes. For the complete set of fields, see [`INS_INDEXES`](./01-meta.md#ins_indexes).

## SHOW INSTANCES

```sql
SHOW INSTANCES [LIKE 'pattern'];
```

Displays registration information for instances connected to the cluster. For the complete set of fields, see [`PERF_INSTANCES`](./02-perf.md#perf_instances).

## SHOW LICENCES

```sql
SHOW LICENCES;
SHOW GRANTS;
SHOW GRANTS FULL;
SHOW GRANTS LOGS;
```

Displays Enterprise Edition license authorization information. `SHOW LICENCES` and `SHOW GRANTS` are equivalent. `SHOW GRANTS FULL` displays details of authorization items, and `SHOW GRANTS LOGS` displays authorization-related logs. For the complete set of fields, see [`INS_GRANTS`](./01-meta.md#ins_grants) / [`INS_GRANTS_FULL`](./01-meta.md#ins_grants_full) / [`INS_GRANTS_LOGS`](./01-meta.md#ins_grants_logs).

**Notes**

- Enterprise Edition only
- `SHOW GRANTS FULL` is available since `v3.2.3.0`

## SHOW LOCAL VARIABLES

```sql
SHOW LOCAL VARIABLES [LIKE 'pattern'];
```

Displays the runtime values of configuration parameters for the current client. Use `LIKE` to filter by parameter name.

## SHOW MNODES

```sql
SHOW MNODES;
```

Displays information about `mnode`s in the current system. For the complete set of fields, see [`INS_MNODES`](./01-meta.md#ins_mnodes).

## SHOW MOUNTS

```sql
SHOW MOUNTS;
```

Displays database mount information. For the complete set of fields, see [`INS_MOUNTS`](./01-meta.md#ins_mounts).

## SHOW QNODES

```sql
SHOW QNODES;
```

Displays information about `qnode`s (query nodes) in the current system. For the complete set of fields, see [`INS_QNODES`](./01-meta.md#ins_qnodes).

## SHOW QUERIES

```sql
SHOW QUERIES;
```

Displays information about ongoing write (update), query, and delete operations in the current system. These operations are collectively called `QUERIES` because of internal API naming. For the complete set of fields, see [`PERF_QUERIES`](./02-perf.md#perf_queries).

## SHOW RETENTIONS

```sql
SHOW [db_name.]RETENTIONS;
SHOW RETENTION retention_id;
```

Displays the list of data retention tasks. `SHOW RETENTION retention_id` displays details of the specified task. For the complete set of fields, see [`INS_RETENTIONS`](./01-meta.md#ins_retentions) / [`INS_RETENTION_DETAILS`](./01-meta.md#ins_retention_details).

## SHOW ROLE COLUMN PRIVILEGES

```sql
SHOW ROLE COLUMN PRIVILEGES;
```

Displays role column-level privileges. For the complete set of fields, see [`INS_ROLE_COLUMN_PRIVILEGES`](./01-meta.md#ins_role_column_privileges).

## SHOW ROLE PRIVILEGES

```sql
SHOW ROLE PRIVILEGES;
```

Displays role privilege details. For the complete set of fields, see [`INS_ROLE_PRIVILEGES`](./01-meta.md#ins_role_privileges).

## SHOW ROLES

```sql
SHOW ROLES;
```

Displays the list of roles. For the complete set of fields, see [`INS_ROLES`](./01-meta.md#ins_roles).

## SHOW RSMAS

```sql
SHOW [db_name.]RSMAS;
```

Displays RSMA definition information. For the complete set of fields, see [`INS_RSMAS`](./01-meta.md#ins_rsmas).

## SHOW SCANS

```sql
SHOW SCANS;
SHOW SCAN scan_id;
```

Displays the list of data scan tasks. `SHOW SCAN scan_id` displays details of the specified task. For the complete set of fields, see [`INS_SCANS`](./01-meta.md#ins_scans) / [`INS_SCAN_DETAILS`](./01-meta.md#ins_scan_details).

## SHOW SCORES

```sql
SHOW SCORES;
```

Displays information about the capacity authorized by the system license.

**Notes**

- Enterprise Edition only

## SHOW SECURITY_POLICIES

```sql
SHOW SECURITY_POLICIES;
```

Displays security policy definitions. For the complete set of fields, see [`INS_SECURITY_POLICIES`](./01-meta.md#ins_security_policies).

## SHOW SNODES

```sql
SHOW SNODES;
```

Displays information about `snode`s (stream processing nodes) in the current system. For the complete set of fields, see [`INS_SNODES`](./01-meta.md#ins_snodes).

## SHOW SSMIGRATES

```sql
SHOW SSMIGRATES;
```

Displays the progress of shared-storage migration tasks. For the complete set of fields, see [`INS_SSMIGRATES`](./01-meta.md#ins_ssmigrates).

## SHOW STABLES

```sql
SHOW [NORMAL | CHILD | VIRTUAL] [db_name.]STABLES [LIKE 'pattern'];
```

Displays supertables in the current database. Use `LIKE` for fuzzy matching by table name. Use the optional `NORMAL`, `CHILD`, or `VIRTUAL` keyword to filter by type. For the complete set of fields, see [`INS_STABLES`](./01-meta.md#ins_stables).

## SHOW STREAMS

```sql
SHOW [db_name.]STREAMS [LIKE 'pattern'];
```

Displays stream processing information. If no database is specified, displays streams in all databases. Use `LIKE` for fuzzy matching by stream name. For the complete set of fields, see [`INS_STREAMS`](./01-meta.md#ins_streams).

## SHOW SUBSCRIPTIONS

```sql
SHOW SUBSCRIPTIONS;
```

Displays all subscription relationships in the current system. For the complete set of fields, see [`INS_SUBSCRIPTIONS`](./01-meta.md#ins_subscriptions).

## SHOW TABLE DISTRIBUTED

```sql
SHOW TABLE DISTRIBUTED [db_name.]table_name;
```

Displays table data distribution information. For the complete set of fields, see [`INS_TABLE_FIXED_DISTRIBUTED`](./01-meta.md#ins_table_fixed_distributed).

**Example**

Statement: `SHOW TABLE DISTRIBUTED d0\G;` (vertically displays the BLOCK distribution of table `d0`)

<details>
<summary>Display example</summary>

```text
*************************** 1.row ***************************

_block_dist: Total_Blocks=[5] Total_Size=[93.65 KB] Average_size=[18.73 KB] Compression_Ratio=[23.98 %]

Total_Blocks: Table d0 occupies 5 blocks

Total_Size: All blocks of table d0 occupy 93.65 KB in the file

Average_size: Each block occupies an average of 18.73 KB in the file

Compression_Ratio: Data compression ratio: 23.98%

*************************** 2.row ***************************

_block_dist: Total_Rows=[20000] Inmem_Rows=[0] MinRows=[3616] MaxRows=[4096] Average_Rows=[4000]

Total_Rows: Table d0 has 20,000 rows stored on disk (this value is for reference only and is not an exact row count. Use the count function to obtain an exact row count)

Inmem_Rows: Number of data rows stored in the write cache (not persisted to disk); 0 rows means that there is no data in the memory cache

MinRows: The minimum number of rows in a BLOCK, which is 3,616

MaxRows: The maximum number of rows in a BLOCK, which is 4,096

Average_Rows: The average number of rows in each BLOCK, which is 4,000

*************************** 3.row ***************************

_block_dist: Total_Tables=[1] Total_Files=[2] Total_Vgroups=[1]

Total_Tables: Number of subtables, which is 1 here

Total_Files: Number of data files in which the table data is stored, which is 2 here

Total_Vgroups: Number of virtual nodes (vnodes) across which the table data is distributed

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

The preceding example illustrates the distribution of the number of data rows in blocks. `0100`, `0299`, `0498`, and so on represent ranges of data row counts in each block. In this example, the table has five blocks: one block (about 20%) falls in the range from `3483` to `3681` rows, and four blocks (about 80%) fall in the range from `3881` to `4096` rows (the maximum number of rows). The other ranges contain no blocks.

Only data block information in data files is displayed. Data in stt files is not included.

## SHOW TABLE TAGS

```sql
SHOW TABLE TAGS [tag_name [, ...]] FROM table_name [FROM db_name];
SHOW TABLE TAGS [tag_name [, ...]] FROM [db_name.]table_name;
```

Displays tag values for a supertable/subtable as columns. You can specify the tag columns to display. Unlike [`SHOW TAGS`](#show-tags), this command is more suitable for viewing a projection of selected tag columns.

## SHOW TABLES

```sql
SHOW [NORMAL | CHILD | VIRTUAL] [db_name.]TABLES [LIKE 'pattern'];
```

Displays normal tables and subtables in the current database. Use `LIKE` for fuzzy matching by table name. `NORMAL` displays only normal tables, `CHILD` displays only subtables, and `VIRTUAL` displays only virtual-table-related objects. To display virtual normal tables and virtual subtables, prefer [`SHOW VTABLES`](#show-vtables). For the complete set of fields, see [`INS_TABLES`](./01-meta.md#ins_tables).

## SHOW TAGS

```sql
SHOW TAGS FROM child_table_name [FROM db_name];
SHOW TAGS FROM [db_name.]child_table_name;
```

Displays tag information for a subtable; basic tables and virtual basic tables with tags are also supported. For a virtual subtable or virtual basic table that uses tag references, the result contains the currently resolved tag values; owned tags of basic tables and virtual basic tables show the table's own value. For the complete set of fields, see [`INS_TAGS`](./01-meta.md#ins_tags).

## SHOW TOKENS

```sql
SHOW TOKENS;
```

Displays user access token information. For the complete set of fields, see [`INS_TOKENS`](./01-meta.md#ins_tokens).

## SHOW TOPICS

```sql
SHOW TOPICS;
```

Displays information about all topics in the current database. For the complete set of fields, see [`INS_TOPICS`](./01-meta.md#ins_topics).

## SHOW TRANSACTION LOGS

```sql
SHOW TRANSACTION LOGS;
```

Displays the history of completed metadata transactions. For the complete set of fields, see [`INS_TRANSACTION_LOGS`](./01-meta.md#ins_transaction_logs).

## SHOW TRANSACTION ORPHANS

```sql
SHOW TRANSACTION ORPHANS;
```

Displays orphan transaction detection results. For the complete set of fields, see [`INS_TRANSACTION_ORPHANS`](./01-meta.md#ins_transaction_orphans).

## SHOW TRANSACTIONS

```sql
SHOW TRANSACTIONS;
SHOW TRANSACTION transaction_id;
```

`SHOW TRANSACTIONS` displays the list of metadata transactions currently being executed (for metadata-level operations other than normal-table operations). `SHOW TRANSACTION transaction_id` displays action details for the specified transaction, corresponding to [`INS_TRANSACTION_DETAILS`](./01-meta.md#ins_transaction_details). For the complete list fields, see [`INS_TRANSACTIONS`](./01-meta.md#ins_transactions) / [`PERF_TRANS`](./02-perf.md#perf_trans).

## SHOW TSMAS

```sql
SHOW [db_name.]TSMAS;
```

Displays time-series SMA (TSMA) definition information. For the complete set of fields, see [`INS_TSMAS`](./01-meta.md#ins_tsmas).

## SHOW USER PRIVILEGES

```sql
SHOW USER PRIVILEGES;
```

Displays user privilege details. For the complete set of fields, see [`INS_USER_PRIVILEGES`](./01-meta.md#ins_user_privileges).

## SHOW USERS

```sql
SHOW USERS;
SHOW USERS FULL;
```

Displays all users in the current system. `SHOW USERS FULL` returns more complete user security and policy configuration. For the complete set of fields, see [`INS_USERS`](./01-meta.md#ins_users) / [`INS_USERS_FULL`](./01-meta.md#ins_users_full).

## SHOW VGROUPS

```sql
SHOW [db_name.]VGROUPS;
```

Displays information about all `vgroup`s in the current database. For the complete set of fields, see [`INS_VGROUPS`](./01-meta.md#ins_vgroups).

## SHOW VIEWS

```sql
SHOW [db_name.]VIEWS [LIKE 'pattern'];
```

Displays the list of views. For the complete set of fields, see [`INS_VIEWS`](./01-meta.md#ins_views).

## SHOW VNODES

```sql
SHOW VNODES;
SHOW VNODES ON DNODE dnode_id;
```

Displays information about all `vnode`s in the current system or the `vnode`s on a specified `dnode`. For the complete set of fields, see [`INS_VNODES`](./01-meta.md#ins_vnodes).

## SHOW VTABLE INHERITS

```sql
SHOW VTABLE INHERITS;
```

Displays virtual supertable inheritance relationships. For the complete set of fields, see [`INS_VSTABLE_INHERITS`](./01-meta.md#ins_vstable_inherits).

## SHOW VTABLE VALIDATE

```sql
SHOW VTABLE VALIDATE FOR [db_name.]vtable_name;
```

Validates column/tag reference relationships for a virtual normal table or virtual subtable. To query validation results in batches, query [`INS_VIRTUAL_TABLES_REFERENCING`](./01-meta.md#ins_virtual_tables_referencing).

## SHOW VTABLES

```sql
SHOW [NORMAL | CHILD | VIRTUAL] [db_name.]VTABLES [LIKE 'pattern'];
```

Displays virtual normal tables and virtual subtables in the specified database. `SHOW TABLES` does not return these objects through the virtual-table view by default. Use this command to display them.

## SHOW XNODE AGENTS

```sql
SHOW XNODE AGENTS [WHERE condition];
SHOW XNODE AGENT [WHERE condition];
```

Displays Xnode Agent information. For the complete set of fields, see [`INS_XNODE_AGENTS`](./01-meta.md#ins_xnode_agents).

## SHOW XNODE JOBS

```sql
SHOW XNODE JOBS [WHERE condition];
SHOW XNODE JOB [WHERE condition];
```

Displays Xnode Job shard information. For the complete set of fields, see [`INS_XNODE_JOBS`](./01-meta.md#ins_xnode_jobs).

## SHOW XNODE TASKS

```sql
SHOW XNODE TASKS [WHERE condition];
SHOW XNODE TASK [WHERE condition];
```

Displays information about Xnode data ingestion tasks. For the complete set of fields, see [`INS_XNODE_TASKS`](./01-meta.md#ins_xnode_tasks).

## SHOW XNODES

```sql
SHOW XNODES [WHERE condition];
```

Displays information about Xnode data ingestion nodes. For the complete set of fields, see [`INS_XNODES`](./01-meta.md#ins_xnodes).
