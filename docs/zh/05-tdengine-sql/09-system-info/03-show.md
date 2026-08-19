---
sidebar_label: SHOW 命令
title: SHOW 命令
description: 通过 SHOW 查看集群、库表、连接、权限与配置等简要系统信息
---

TDengine 提供 `SHOW` 命令，用于获取简要系统信息。若需更详细的元数据、系统信息与状态，请使用 `SELECT` 查询 `INFORMATION_SCHEMA`（见 [元数据视图](./01-meta.md)）中的表，或查询 `PERFORMANCE_SCHEMA`（见 [性能数据视图](./02-perf.md)）中的性能统计视图。

下文按命令名字母顺序列出当前版本支持的 `SHOW` 语句。

## SHOW ALIVE

```sql
SHOW [db_name.]ALIVE;
```

查询指定数据库是否可用；未指定数据库名时查询当前库。返回值含义与 [`SHOW CLUSTER ALIVE`](#show-cluster-alive) 相同：`0` 不可用，`1` 完全可用，`2` 部分可用。

## SHOW ANODES

```sql
SHOW ANODES;
SHOW ANODES FULL;
```

显示分析节点（anode）信息。`SHOW ANODES FULL` 额外显示节点上已加载的算法明细。更完整字段见 [`INS_ANODES`](./01-meta.md#ins_anodes) / [`INS_ANODES_FULL`](./01-meta.md#ins_anodes_full)。

## SHOW APPS

```sql
SHOW APPS;
```

显示接入集群的应用（客户端）信息。更完整字段见 [`PERF_APPS`](./02-perf.md#perf_apps)。

## SHOW ARBGROUPS

```sql
SHOW ARBGROUPS;
```

显示仲裁组（arbgroup）信息，包括副本 `dnode`、同步状态与指派令牌等。更完整字段见 [`INS_ARBGROUPS`](./01-meta.md#ins_arbgroups)。

## SHOW BNODES

```sql
SHOW BNODES;
```

显示桥接节点（bnode）信息。更完整字段见 [`INS_BNODES`](./01-meta.md#ins_bnodes)。

## SHOW CLUSTER

```sql
SHOW CLUSTER;
```

显示当前集群信息。更完整字段见 [`INS_CLUSTER`](./01-meta.md#ins_cluster)。

## SHOW CLUSTER ALIVE

```sql
SHOW CLUSTER ALIVE;
```

查询当前集群是否可用，返回值含义如下：

- `0`：不可用
- `1`：完全可用
- `2`：部分可用（部分节点下线，其它节点仍可正常使用）

## SHOW CLUSTER MACHINES

```sql
SHOW CLUSTER MACHINES;
```

显示集群机器码等信息。更完整字段见 [`INS_MACHINES`](./01-meta.md#ins_machines)。

**备注**

- 企业版功能
- 自 `v3.2.3.0` 起支持

## SHOW CLUSTER VARIABLES

```sql
SHOW VARIABLES [LIKE 'pattern'];
SHOW CLUSTER VARIABLES [LIKE 'pattern'];
SHOW DNODE dnode_id VARIABLES [LIKE 'pattern'];
```

显示各节点需保持一致的配置参数运行值；也可指定 `dnode` 查看其配置。可用 `LIKE` 按参数名过滤。`SHOW VARIABLES` 与 `SHOW CLUSTER VARIABLES` 等价。

**备注**

- `v3.0.1.6` 之前仅支持 `SHOW VARIABLES`

## SHOW COMPACTS

```sql
SHOW COMPACTS;
SHOW COMPACT compact_id;
```

显示数据压缩（compact）任务列表；`SHOW COMPACT compact_id` 显示指定任务在各 `vgroup`/`dnode` 上的明细进度。更完整字段见 [`INS_COMPACTS`](./01-meta.md#ins_compacts) / [`INS_COMPACT_DETAILS`](./01-meta.md#ins_compact_details)。

## SHOW CONNECTIONS

```sql
SHOW CONNECTIONS;
```

显示当前系统中的连接信息。更完整字段见 [`PERF_CONNECTIONS`](./02-perf.md#perf_connections)。

## SHOW CONSUMERS

```sql
SHOW CONSUMERS;
```

显示当前数据库下所有消费者的信息。更完整字段见 [`PERF_CONSUMERS`](./02-perf.md#perf_consumers)。

## SHOW CPU_ALLOCATION

```sql
SHOW CPU_ALLOCATION;
```

显示集群中所有 `dnode` 三类线程（管理、写入、读取）的 CPU 核心分配状态。仅在配置参数 `enableCpuAffinity` 启用时有实际意义。每个 `dnode` 返回 3 行，列如下：

| **列名** | **数据类型** | **说明** |
| --- | --- | --- |
| `dnode_id` | INT | `dnode` 标识 |
| `thread_category` | VARCHAR(16) | 线程类别：`management`（管理）、`write`（写入）或 `read`（读取） |
| `cores` | INT | 分配给该类别的 CPU 核心数（禁用时为 `0`） |
| `core_ids` | VARCHAR(256) | 已分配核心 ID 列表（逗号分隔）；禁用时为 `"-"` |
| `enabled` | BOOL | 该类别是否启用了 CPU 亲和性 |

当 `enableCpuAffinity` 关闭（默认）时，所有行显示 `enabled=false`、`cores=0`、`core_ids="-"`。更完整字段见 [`INS_CPU_ALLOCATION`](./01-meta.md#ins_cpu_allocation)。

## SHOW CREATE DATABASE

```sql
SHOW CREATE DATABASE db_name;
```

显示 `db_name` 指定数据库的创建语句。

## SHOW CREATE RSMA

```sql
SHOW CREATE RSMA [db_name.]rsma_name;
```

显示指定 RSMA 的创建语句。

## SHOW CREATE STABLE

```sql
SHOW CREATE STABLE [db_name.]stb_name;
```

显示 `stb_name` 指定超级表的创建语句。

## SHOW CREATE STREAM

```sql
SHOW CREATE STREAM [db_name.]stream_name;
```

显示 `stream_name` 指定流的创建语句。

**备注**

- 自 `v3.4.1.13` 起支持

## SHOW CREATE TABLE

```sql
SHOW CREATE TABLE [db_name.]tb_name;
```

显示 `tb_name` 指定表的创建语句。支持普通表、超级表和子表。

## SHOW CREATE VIEW

```sql
SHOW CREATE VIEW [db_name.]view_name;
```

显示指定视图的创建语句。

## SHOW CREATE VTABLE

```sql
SHOW CREATE VTABLE [db_name.]vtable_name;
```

显示虚拟表的创建语句。对使用 tag-ref 的虚拟子表，结果会保留对应的标签引用定义。

## SHOW DATABASES

```sql
SHOW [USER | SYSTEM] DATABASES;
```

显示数据库列表。`SYSTEM` 仅显示系统数据库；`USER` 仅显示用户创建的数据库。更完整字段见 [`INS_DATABASES`](./01-meta.md#ins_databases)。

## SHOW DISK_INFO

```sql
SHOW [db_name.]DISK_INFO;
```

显示数据库磁盘占用信息（WAL、多级存储、缓存与元数据等）。更完整字段见 [`INS_DISK_USAGE`](./01-meta.md#ins_disk_usage)。

## SHOW DNODES

```sql
SHOW DNODES;
```

显示当前系统中 `dnode` 的信息。更完整字段见 [`INS_DNODES`](./01-meta.md#ins_dnodes)。

## SHOW ENCRYPTIONS

```sql
SHOW ENCRYPTIONS;
```

显示各 `dnode` 的加密密钥状态。更完整字段见 [`INS_ENCRYPTIONS`](./01-meta.md#ins_encryptions)。

## SHOW ENCRYPT_ALGORITHMS

```sql
SHOW ENCRYPT_ALGORITHMS;
```

显示可用的加密算法列表。更完整字段见 [`INS_ENCRYPT_ALGORITHMS`](./01-meta.md#ins_encrypt_algorithms)。

## SHOW ENCRYPT_STATUS

```sql
SHOW ENCRYPT_STATUS;
```

显示当前加密范围、算法与状态。更完整字段见 [`INS_ENCRYPT_STATUS`](./01-meta.md#ins_encrypt_status)。

## SHOW EXTERNAL SOURCES

```sql
SHOW EXTERNAL SOURCES;
```

显示联邦查询外部数据源信息。更完整字段见 [`INS_EXT_SOURCES`](./01-meta.md#ins_ext_sources)。

## SHOW FUNCTIONS

```sql
SHOW FUNCTIONS;
```

显示用户定义的自定义函数。更完整字段见 [`INS_FUNCTIONS`](./01-meta.md#ins_functions)。

## SHOW INDEXES

```sql
SHOW INDEXES FROM tbl_name [FROM db_name];
SHOW INDEXES FROM [db_name.]tbl_name;
```

显示已创建的索引。更完整字段见 [`INS_INDEXES`](./01-meta.md#ins_indexes)。

## SHOW INSTANCES

```sql
SHOW INSTANCES [LIKE 'pattern'];
```

显示接入集群的实例注册信息。更完整字段见 [`PERF_INSTANCES`](./02-perf.md#perf_instances)。

## SHOW LICENCES

```sql
SHOW LICENCES;
SHOW GRANTS;
SHOW GRANTS FULL;
SHOW GRANTS LOGS;
```

显示企业版许可授权信息。`SHOW LICENCES` 与 `SHOW GRANTS` 等价；`SHOW GRANTS FULL` 显示授权项明细；`SHOW GRANTS LOGS` 显示授权相关日志。更完整字段见 [`INS_GRANTS`](./01-meta.md#ins_grants) / [`INS_GRANTS_FULL`](./01-meta.md#ins_grants_full) / [`INS_GRANTS_LOGS`](./01-meta.md#ins_grants_logs)。

**备注**

- 企业版功能
- `SHOW GRANTS FULL` 自 `v3.2.3.0` 起支持

## SHOW LOCAL VARIABLES

```sql
SHOW LOCAL VARIABLES [LIKE 'pattern'];
```

显示当前客户端配置参数的运行值；可用 `LIKE` 按参数名过滤。

## SHOW MNODES

```sql
SHOW MNODES;
```

显示当前系统中 `mnode` 的信息。更完整字段见 [`INS_MNODES`](./01-meta.md#ins_mnodes)。

## SHOW MOUNTS

```sql
SHOW MOUNTS;
```

显示数据库挂载（mount）信息。更完整字段见 [`INS_MOUNTS`](./01-meta.md#ins_mounts)。

## SHOW QNODES

```sql
SHOW QNODES;
```

显示当前系统中 `qnode`（查询节点）的信息。更完整字段见 [`INS_QNODES`](./01-meta.md#ins_qnodes)。

## SHOW QUERIES

```sql
SHOW QUERIES;
```

显示当前系统中正在进行的写入（更新）、查询、删除操作信息（内部 API 命名原因，统称 `QUERIES`）。更完整字段见 [`PERF_QUERIES`](./02-perf.md#perf_queries)。

## SHOW RETENTIONS

```sql
SHOW [db_name.]RETENTIONS;
SHOW RETENTION retention_id;
```

显示数据保留（retention）任务列表；`SHOW RETENTION retention_id` 显示指定任务明细。更完整字段见 [`INS_RETENTIONS`](./01-meta.md#ins_retentions) / [`INS_RETENTION_DETAILS`](./01-meta.md#ins_retention_details)。

## SHOW ROLE COLUMN PRIVILEGES

```sql
SHOW ROLE COLUMN PRIVILEGES;
```

显示角色列级权限。更完整字段见 [`INS_ROLE_COLUMN_PRIVILEGES`](./01-meta.md#ins_role_column_privileges)。

## SHOW ROLE PRIVILEGES

```sql
SHOW ROLE PRIVILEGES;
```

显示角色权限明细。更完整字段见 [`INS_ROLE_PRIVILEGES`](./01-meta.md#ins_role_privileges)。

## SHOW ROLES

```sql
SHOW ROLES;
```

显示角色列表。更完整字段见 [`INS_ROLES`](./01-meta.md#ins_roles)。

## SHOW RSMAS

```sql
SHOW [db_name.]RSMAS;
```

显示 RSMA 定义信息。更完整字段见 [`INS_RSMAS`](./01-meta.md#ins_rsmas)。

## SHOW SCANS

```sql
SHOW SCANS;
SHOW SCAN scan_id;
```

显示数据扫描任务列表；`SHOW SCAN scan_id` 显示指定任务明细。更完整字段见 [`INS_SCANS`](./01-meta.md#ins_scans) / [`INS_SCAN_DETAILS`](./01-meta.md#ins_scan_details)。

## SHOW SCORES

```sql
SHOW SCORES;
```

显示系统被许可授权的容量信息。

**备注**

- 企业版功能

## SHOW SECURITY_POLICIES

```sql
SHOW SECURITY_POLICIES;
```

显示安全策略定义。更完整字段见 [`INS_SECURITY_POLICIES`](./01-meta.md#ins_security_policies)。

## SHOW SNODES

```sql
SHOW SNODES;
```

显示当前系统中 `snode`（流式计算节点）的信息。更完整字段见 [`INS_SNODES`](./01-meta.md#ins_snodes)。

## SHOW SSMIGRATES

```sql
SHOW SSMIGRATES;
```

显示共享存储迁移任务进度。更完整字段见 [`INS_SSMIGRATES`](./01-meta.md#ins_ssmigrates)。

## SHOW STABLES

```sql
SHOW [NORMAL | CHILD | VIRTUAL] [db_name.]STABLES [LIKE 'pattern'];
```

显示当前数据库下的超级表。可用 `LIKE` 对表名模糊匹配；可选 `NORMAL` / `CHILD` / `VIRTUAL` 过滤类型。更完整字段见 [`INS_STABLES`](./01-meta.md#ins_stables)。

## SHOW STREAMS

```sql
SHOW [db_name.]STREAMS [LIKE 'pattern'];
```

显示流式计算信息。未指定数据库时显示所有库下的流；可用 `LIKE` 对流名模糊匹配。更完整字段见 [`INS_STREAMS`](./01-meta.md#ins_streams)。

## SHOW SUBSCRIPTIONS

```sql
SHOW SUBSCRIPTIONS;
```

显示当前系统内所有订阅关系。更完整字段见 [`INS_SUBSCRIPTIONS`](./01-meta.md#ins_subscriptions)。

## SHOW TABLE DISTRIBUTED

```sql
SHOW TABLE DISTRIBUTED [db_name.]table_name;
```

显示表的数据分布信息。更完整字段见 [`INS_TABLE_FIXED_DISTRIBUTED`](./01-meta.md#ins_table_fixed_distributed)。

**示例说明**

语句：`SHOW TABLE DISTRIBUTED d0\G;`（竖排显示表 `d0` 的 BLOCK 分布）

<details>
<summary>显示示例</summary>

```text
*************************** 1.row ***************************

_block_dist: Total_Blocks=[5] Total_Size=[93.65 KB] Average_size=[18.73 KB] Compression_Ratio=[23.98 %]

Total_Blocks：表 d0 占用的 block 个数为 5 个

Total_Size：表 d0 所有 block 在文件中占用的大小为 93.65 KB

Average_size：平均每个 block 在文件中占用的空间大小为 18.73 KB

Compression_Ratio: 数据压缩率 23.98%

*************************** 2.row ***************************

_block_dist: Total_Rows=[20000] Inmem_Rows=[0] MinRows=[3616] MaxRows=[4096] Average_Rows=[4000]

Total_Rows：统计表 d0 的存储在磁盘上行数 20000 行（该数值仅供参考，不是精确的行数。获得精确的行数需要使用 count 函数）

Inmem_Rows：存储在写缓存中的数据行数（没有落盘），0 行表示内存缓存中没有数据

MinRows：BLOCK 中最小的行数，为 3616 行

MaxRows：BLOCK 中最大的行数，为 4096 行

Average_Rows：每个 BLOCK 中的平均行数，此时为 4000 行

*************************** 3.row ***************************

_block_dist: Total_Tables=[1] Total_Files=[2] Total_Vgroups=[1]

Total_Tables：子表的个数，这里为 1

Total_Files：表数据被分别保存的数据文件数量，这里是 2 个文件

Total_Vgroups：表数据分布的虚拟节点（vnode）数量

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

上例为块中包含数据行数的分布示意。其中 `0100`、`0299`、`0498` 等表示每个块中包含的数据行数区间。上例中，表有 5 个块：落在 `3483 ~ 3681` 行区间的块有 1 个（约 20%），落在 `3881 ~ 4096`（最大行数）区间的块有 4 个（约 80%），其它区间为 0。

仅显示 data 文件中数据块的信息，不包含 stt 文件中的数据信息。

## SHOW TABLE TAGS

```sql
SHOW TABLE TAGS [tag_name [, ...]] FROM table_name [FROM db_name];
SHOW TABLE TAGS [tag_name [, ...]] FROM [db_name.]table_name;
```

以列形式显示超级表/子表的标签值；可指定要展示的标签列。与 [`SHOW TAGS`](#show-tags) 不同，本命令更适合按标签列投影查看。

## SHOW TABLES

```sql
SHOW [NORMAL | CHILD | VIRTUAL] [db_name.]TABLES [LIKE 'pattern'];
```

显示当前数据库下的普通表和子表。可用 `LIKE` 对表名模糊匹配。`NORMAL` 仅显示普通表，`CHILD` 仅显示子表，`VIRTUAL` 仅显示虚拟表相关对象。虚拟普通表/子表请优先使用 [`SHOW VTABLES`](#show-vtables)。更完整字段见 [`INS_TABLES`](./01-meta.md#ins_tables)。

## SHOW TAGS

```sql
SHOW TAGS FROM child_table_name [FROM db_name];
SHOW TAGS FROM [db_name.]child_table_name;
```

显示子表的标签信息，也支持带标签的普通表和虚拟普通表。对使用 tag-ref 的虚拟子表或虚拟普通表，结果为当前解析后的标签值；普通表和虚拟普通表的自有标签显示本表的值。更完整字段见 [`INS_TAGS`](./01-meta.md#ins_tags)。

## SHOW TOKENS

```sql
SHOW TOKENS;
```

显示用户访问令牌信息。更完整字段见 [`INS_TOKENS`](./01-meta.md#ins_tokens)。

## SHOW TOPICS

```sql
SHOW TOPICS;
```

显示当前数据库下的所有主题信息。更完整字段见 [`INS_TOPICS`](./01-meta.md#ins_topics)。

## SHOW TRANSACTION LOGS

```sql
SHOW TRANSACTION LOGS;
```

显示已完成元数据事务的历史记录。更完整字段见 [`INS_TRANSACTION_LOGS`](./01-meta.md#ins_transaction_logs)。

## SHOW TRANSACTION ORPHANS

```sql
SHOW TRANSACTION ORPHANS;
```

显示孤儿事务检测结果。更完整字段见 [`INS_TRANSACTION_ORPHANS`](./01-meta.md#ins_transaction_orphans)。

## SHOW TRANSACTIONS

```sql
SHOW TRANSACTIONS;
SHOW TRANSACTION transaction_id;
```

`SHOW TRANSACTIONS` 显示当前正在执行的元数据事务列表（针对除普通表以外的元数据级操作）。`SHOW TRANSACTION transaction_id` 显示指定事务的动作明细，对应 [`INS_TRANSACTION_DETAILS`](./01-meta.md#ins_transaction_details)。列表字段见 [`INS_TRANSACTIONS`](./01-meta.md#ins_transactions) / [`PERF_TRANS`](./02-perf.md#perf_trans)。

## SHOW TSMAS

```sql
SHOW [db_name.]TSMAS;
```

显示时序 SMA（TSMA）定义信息。更完整字段见 [`INS_TSMAS`](./01-meta.md#ins_tsmas)。

## SHOW USER PRIVILEGES

```sql
SHOW USER PRIVILEGES;
```

显示用户权限明细。更完整字段见 [`INS_USER_PRIVILEGES`](./01-meta.md#ins_user_privileges)。

## SHOW USERS

```sql
SHOW USERS;
SHOW USERS FULL;
```

显示当前系统中所有用户。`SHOW USERS FULL` 返回更完整的用户安全与策略配置。更完整字段见 [`INS_USERS`](./01-meta.md#ins_users) / [`INS_USERS_FULL`](./01-meta.md#ins_users_full)。

## SHOW VGROUPS

```sql
SHOW [db_name.]VGROUPS;
```

显示当前数据库中所有 `vgroup` 的信息。更完整字段见 [`INS_VGROUPS`](./01-meta.md#ins_vgroups)。

## SHOW VIEWS

```sql
SHOW [db_name.]VIEWS [LIKE 'pattern'];
```

显示视图列表。更完整字段见 [`INS_VIEWS`](./01-meta.md#ins_views)。

## SHOW VNODES

```sql
SHOW VNODES;
SHOW VNODES ON DNODE dnode_id;
```

显示当前系统中全部 `vnode`，或指定 `dnode` 上的 `vnode` 信息。更完整字段见 [`INS_VNODES`](./01-meta.md#ins_vnodes)。

## SHOW VTABLE INHERITS

```sql
SHOW VTABLE INHERITS;
```

显示虚拟超级表继承关系。更完整字段见 [`INS_VSTABLE_INHERITS`](./01-meta.md#ins_vstable_inherits)。

## SHOW VTABLE VALIDATE

```sql
SHOW VTABLE VALIDATE FOR [db_name.]vtable_name;
```

校验虚拟普通表或虚拟子表的列/标签引用关系。若需批量查询校验结果，可查询 [`INS_VIRTUAL_TABLES_REFERENCING`](./01-meta.md#ins_virtual_tables_referencing)。

## SHOW VTABLES

```sql
SHOW [NORMAL | CHILD | VIRTUAL] [db_name.]VTABLES [LIKE 'pattern'];
```

显示指定数据库下的虚拟普通表和虚拟子表。`SHOW TABLES` 默认不会按虚拟表视图返回这些对象，请使用本命令查看。

## SHOW XNODE AGENTS

```sql
SHOW XNODE AGENTS [WHERE condition];
SHOW XNODE AGENT [WHERE condition];
```

显示 Xnode Agent 信息。更完整字段见 [`INS_XNODE_AGENTS`](./01-meta.md#ins_xnode_agents)。

## SHOW XNODE JOBS

```sql
SHOW XNODE JOBS [WHERE condition];
SHOW XNODE JOB [WHERE condition];
```

显示 Xnode Job 分片信息。更完整字段见 [`INS_XNODE_JOBS`](./01-meta.md#ins_xnode_jobs)。

## SHOW XNODE TASKS

```sql
SHOW XNODE TASKS [WHERE condition];
SHOW XNODE TASK [WHERE condition];
```

显示 Xnode 数据接入任务信息。更完整字段见 [`INS_XNODE_TASKS`](./01-meta.md#ins_xnode_tasks)。

## SHOW XNODES

```sql
SHOW XNODES [WHERE condition];
```

显示 Xnode 数据接入节点信息。更完整字段见 [`INS_XNODES`](./01-meta.md#ins_xnodes)。
