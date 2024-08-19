---
sidebar_label: SHOW 命令
title: SHOW 命令
description: SHOW 命令的完整列表
---

SHOW 命令可以用来获取简要的系统信息。若想获取系统中详细的各种元数据、系统信息和状态，请使用 select 语句查询 INFORMATION_SCHEMA 数据库中的表。

## SHOW APPS

```sql
SHOW APPS;
```

显示接入集群的应用（客户端）信息。

## SHOW CLUSTER

```sql
SHOW CLUSTER;
```

显示当前集群的信息

## SHOW CLUSTER ALIVE

```sql
SHOW CLUSTER ALIVE;
```

查询当前集群的状态是否可用，返回值： 0：不可用 1：完全可用 2：部分可用（集群中部分节点下线，但其它节点仍可以正常使用）

## SHOW CONNECTIONS

```sql
SHOW CONNECTIONS;
```

显示当前系统中存在的连接的信息。

## SHOW CONSUMERS

```sql
SHOW CONSUMERS;
```

显示当前数据库下所有消费者的信息。

## SHOW CREATE DATABASE

```sql
SHOW CREATE DATABASE db_name;
```

显示 db_name 指定的数据库的创建语句。

## SHOW CREATE STABLE

```sql
SHOW CREATE STABLE [db_name.]stb_name;
```

显示 tb_name 指定的超级表的创建语句

## SHOW CREATE TABLE

```sql
SHOW CREATE TABLE [db_name.]tb_name
```

显示 tb_name 指定的表的创建语句。支持普通表、超级表和子表。

## SHOW DATABASES

```sql
SHOW [USER | SYSTEM] DATABASES;
```

显示定义的所有数据库。SYSTEM 指定只显示系统数据库。USER 指定只显示用户创建的数据库。

## SHOW DNODES

```sql
SHOW DNODES;
```

显示当前系统中 DNODE 的信息。

## SHOW FUNCTIONS

```sql
SHOW FUNCTIONS;
```

显示用户定义的自定义函数。

## SHOW LICENCES

```sql
SHOW LICENCES;
SHOW GRANTS;
```

显示企业版许可授权的信息。

注：企业版独有

## SHOW INDEXES

```sql
SHOW INDEXES FROM tbl_name [FROM db_name];
SHOW INDEXES FROM [db_name.]tbl_name;
```

显示已创建的索引。

## SHOW LOCAL VARIABLES

```sql
SHOW LOCAL VARIABLES;
```

显示当前客户端配置参数的运行值。

## SHOW MNODES

```sql
SHOW MNODES;
```

显示当前系统中 MNODE 的信息。

## SHOW QNODES

```sql
SHOW QNODES;
```

显示当前系统中 QNODE （查询节点）的信息。

## SHOW QUERIES

```sql
SHOW QUERIES;
```

显示当前系统中正在进行的查询。

## SHOW SCORES

```sql
SHOW SCORES;
```

显示系统被许可授权的容量的信息。

注：企业版独有。

## SHOW STABLES

```sql
SHOW [db_name.]STABLES [LIKE 'pattern'];
```

显示当前数据库下的所有超级表的信息。可以使用 LIKE 对表名进行模糊匹配。

## SHOW STREAMS

```sql
SHOW STREAMS;
```

显示当前系统内所有流计算的信息。

## SHOW SUBSCRIPTIONS

```sql
SHOW SUBSCRIPTIONS;
```

显示当前系统内所有的订阅关系

## SHOW TABLES

```sql
SHOW [NORMAL | CHILD] [db_name.]TABLES [LIKE 'pattern'];
```

显示当前数据库下的所有普通表和子表的信息。可以使用 LIKE 对表名进行模糊匹配。NORMAL 指定只显示普通表信息， CHILD 指定只显示子表信息。

## SHOW TABLE DISTRIBUTED

```sql
SHOW TABLE DISTRIBUTED table_name;
```

显示表的数据分布信息。

示例说明：

语句： show table distributed d0\G;   竖行显示表 d0 的 BLOCK 分布情况

<details>
 <summary>显示示例</summary>
 <pre><code>

*************************** 1.row ***************************

_block_dist: Total_Blocks=[5] Total_Size=[93.65 KB] Average_size=[18.73 KB] Compression_Ratio=[23.98 %]

Total_Blocks:  表 d0 占用的 block 个数为 5 个

Total_Size:    表 d0 所有 block 在文件中占用的大小为 93.65 KB 

Average_size:  平均每个 block 在文件中占用的空间大小为 18.73 KB

Compression_Ratio: 数据压缩率 23.98%

 
*************************** 2.row ***************************

_block_dist: Total_Rows=[20000] Inmem_Rows=[0] MinRows=[3616] MaxRows=[4096] Average_Rows=[4000]

Total_Rows:  统计表 d0 的存储在磁盘上行数 20000 行（该数值仅供参考，不是精确的行数。获得精确的行数需要使用 count 函数）

Inmem_Rows： 存储在写缓存中的数据行数（没有落盘），0 行表示内存缓存中没有数据

MinRows：    BLOCK 中最小的行数，为 3616 行

MaxRows：    BLOCK 中最大的行数，为 4096 行

Average_Rows： 每个 BLOCK 中的平均行数，此时为 4000 行


*************************** 3.row ***************************

_block_dist: Total_Tables=[1] Total_Files=[2] Total_Vgroups=[1]

Total_Tables:   子表的个数，这里为 1

Total_Files：   表数据被分别保存的数据文件数量，这里是 2 个文件

Total_Vgroups： 表数据分布的虚拟节点（vnode）数量


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

</code></pre>
 </details>

上面是块中包含数据行数的块儿分布情况图，这里的 0100 0299 0498 … 表示的是每个块中包含的数据行数，上面的意思就是这个表的 5 个块，分布在 3483 ~3681 行的块有 1 个，占整个块的 20%，分布在 3881 ~ 4096（最大行数）的块数为 4 个，占整个块的 80%， 其它区域内分布块数为 0。

需要注意，这里只会显示 data 文件中数据块的信息，stt 文件中的数据的信息不会被显示。

## SHOW TAGS

```sql
SHOW TAGS FROM child_table_name [FROM db_name];
SHOW TAGS FROM [db_name.]child_table_name;
```

显示子表的标签信息。

## SHOW TOPICS

```sql
SHOW TOPICS;
```

显示当前数据库下的所有主题的信息。

## SHOW TRANSACTIONS

```sql
SHOW TRANSACTIONS;
```

显示当前系统中正在执行的事务的信息(该事务仅针对除普通表以外的元数据级别)

## SHOW USERS

```sql
SHOW USERS;
```

显示当前系统中所有用户的信息。包括用户自定义的用户和系统默认用户。

## SHOW CLUSTER VARIABLES(3.0.1.6 之前为 SHOW VARIABLES)

```sql
SHOW CLUSTER VARIABLES;
SHOW DNODE dnode_id VARIABLES;
```

显示当前系统中各节点需要相同的配置参数的运行值，也可以指定 DNODE 来查看其的配置参数。

## SHOW VGROUPS

```sql
SHOW [db_name.]VGROUPS;
```

显示当前数据库中所有 VGROUP 的信息。

## SHOW VNODES

```sql
SHOW VNODES [ON DNODE dnode_id];
```

显示当前系统中所有 VNODE 或某个 DNODE 的 VNODE 的信息。
