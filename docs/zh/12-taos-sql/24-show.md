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

## SHOW CONNECTIONS

```sql
SHOW CONNECTIONS;
```

显示当前系统中存在的连接的信息。

## SHOW CONSUMERS

```sql
SHOW CONSUMERS;
```

显示当前数据库下所有活跃的消费者的信息。

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
SHOW DATABASES;
```

显示用户定义的所有数据库。

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

## SHOW LICENSE

```sql
SHOW LICENSE;
SHOW GRANTS;
```

显示企业版许可授权的信息。

注：企业版独有

## SHOW INDEXES

```sql
SHOW INDEXES FROM tbl_name [FROM db_name];
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
SHOW [db_name.]TABLES [LIKE 'pattern'];
```

显示当前数据库下的所有普通表和子表的信息。可以使用 LIKE 对表名进行模糊匹配。

## SHOW TABLE DISTRIBUTED

```sql
SHOW TABLE DISTRIBUTED table_name;
```

显示表的数据分布信息。

## SHOW TAGS

```sql
SHOW TAGS FROM child_table_name [FROM db_name];
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

显示当前系统中所有 VGROUP 或某个 db 的 VGROUPS 的信息。

## SHOW VNODES

```sql
SHOW VNODES [dnode_name];
```

显示当前系统中所有 VNODE 或某个 DNODE 的 VNODE 的信息。
