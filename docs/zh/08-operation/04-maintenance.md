---
toc_max_heading_level: 4
title: 集群维护
sidebar_label: 集群维护
---

本节介绍 TDengine Enterprise 中提供的高阶集群维护手段，能够使 TDengine 集群长期运行得更健壮和高效。

## 节点管理

如何管理集群节点请参考[节点管理](../../reference/taos-sql/node)

## 数据重整

TDengine 面向多种写入场景，而很多写入场景下，TDengine 的存储会导致数据存储的放大或数据文件的空洞等。这一方面影响数据的存储效率，另一方面也会影响查询效率。为了解决上述问题，TDengine 企业版提供了对数据的重整功能，即 DATA COMPACT 功能，将存储的数据文件重新整理，删除文件空洞和无效数据，提高数据的组织度，从而提高存储和查询的效率。数据重整功能在 3.0.3.0 版本第一次发布，此后又经过了多次迭代优化，建议使用最新版本。

### 语法

```SQL
COMPACT DATABASE db_name [start with 'XXXX'] [end with 'YYYY']； 
SHOW COMPACTS [compact_id]；
KILL COMPACT compact_id；
```

### 效果

-   扫描并压缩指定的 DB 中所有 VGROUP 中 VNODE 的所有数据文件
-   COMPCAT 会删除被删除数据以及被删除的表的数据
-   COMPACT 会合并多个 STT 文件
-   可通过 start with 关键字指定 COMPACT 数据的起始时间
-   可通过 end with 关键字指定 COMPACT 数据的终止时间
-   COMPACT 命令会返回 COMPACT 任务的 ID
-   COMPACT 任务会在后台异步执行，可以通过 SHOW COMPACTS 命令查看 COMPACT 任务的进度
-   SHOW 命令会返回 COMPACT 任务的 ID，可以通过 KILL COMPACT 命令终止 COMPACT 任务


### 补充说明

-   COMPACT 为异步，执行 COMPACT 命令后不会等 COMPACT 结束就会返回。如果上一个 COMPACT 没有完成则再发起一个 COMPACT 任务，则会等上一个任务完成后再返回。
-   COMPACT 可能阻塞写入，尤其是在 stt_trigger = 1 的数据库中，但不阻塞查询。

## Vgroup Leader 再平衡

当多副本集群中的一个或多个节点因为升级或其它原因而重启后，有可能出现集群中各个 dnode 负载不均衡的现象，极端情况下会出现所有 vgroup 的 leader 都位于同一个 dnode 的情况。为了解决这个问题，可以使用下面的命令，该命令在 3.0.4.0 版本中首次发布，建议尽可能使用最新版本。

```SQL
balance vgroup leader; # 再平衡所有 vgroup 的 leader
balance vgroup leader on <vgroup_id>; # 再平衡一个 vgroup 的 leader
balance vgroup leader database <database_name>; # 再平衡一个 database 内所有 vgroup 的 leader
```

### 功能

尝试让一个或所有 vgroup 的 leader在各自的replica节点上均匀分布。这个命令会让 vgroup 强制重新选举，通过重新选举，在选举的过程中，改变 vgroup 的leader，通过这个方式，最终让leader均匀分布。

### 注意

Vgroup 选举本身带有随机性，所以通过选举的重新分布产生的均匀分布也是带有一定的概率，不会完全的均匀。该命令的副作用是影响查询和写入，在vgroup重新选举时，从开始选举到选举出新的 leader 这段时间，这 个vgroup 无法写入和查询。选举过程一般在秒级完成。所有的vgroup会依次逐个重新选举。

## 恢复数据节点

当集群中的某个数据节点（dnode）的数据全部丢失或被破坏，比如磁盘损坏或者目录被误删除，可以通过 restore dnode 命令来恢复该数据节点上的部分或全部逻辑节点，该功能依赖多副本中的其它副本进行数据复制，所以只在集群中 dnode 数量大于等于 3 且副本数为 3 的情况下能够工作。

```sql
restore dnode <dnode_id>；# 恢复dnode上的mnode，所有vnode和qnode
restore mnode on dnode <dnode_id>；# 恢复dnode上的mnode
restore vnode on dnode <dnode_id> ；# 恢复dnode上的所有vnode
restore qnode on dnode <dnode_id>；# 恢复dnode上的qnode
```

### 限制

- 该功能是基于已有的复制功能的恢复，不是灾难恢复或者备份恢复，所以对于要恢复的 mnode 和 vnode来说，使用该命令的前提是还存在该 mnode 或 vnode 的其它两个副本仍然能够正常工作。
- 该命令不能修复数据目录中的个别文件的损坏或者丢失。例如，如果某个 mnode 或者 vnode 中的个别文件或数据损坏，无法单独恢复损坏的某个文件或者某块数据。此时，可以选择将该  mnode/vnode 的数据全部清空再进行恢复。

## 分裂虚拟组

当一个 vgroup 因为子表数过多而导致 CPU 或 Disk 资源使用量负载过高时，增加 dnode 节点后，可通过split vgroup命令把该vgroup分裂为两个虚拟组。分裂完成后，新产生的两个 vgroup 承担原来由一个 vgroup 提供的读写服务。该命令在 3.0.6.0 版本第一次发布，建议尽可能使用最新版本。

```sql
split vgroup <vgroup_id>
```

### 注意

- 单副本库虚拟组，在分裂完成后，历史时序数据总磁盘空间使用量，可能会翻倍。所以，在执行该操作之前，通过增加 dnode 节点方式，确保集群中有足够的 CPU 和磁盘资源，避免资源不足现象发生。
- 该命令为 DB 级事务；执行过程，当前DB的其它管理事务将会被拒绝。集群中，其它DB不受影响。
- 分裂任务执行过程中，可持续提供读写服务；期间，可能存在可感知的短暂的读写业务中断。
- 在分裂过程中，不支持流和订阅。分裂结束后，历史 WAL 会清空。
- 分裂过程中，可支持节点宕机重启容错；但不支持节点磁盘故障容错。

## 在线更新集群配置

从 3.1.1.0 版本开始，TDengine Enterprise 支持在线热更新 `supportVnodes` 这个很重要的 dnode 配置参数。这个参数的原始配置方式是在 `taos.cfg` 配置文件中，表示该 dnode 能够支持的最大的 vnode 数量。当创建一个数据库时需要分配新的 vnode，当删除一个数据库时其 vnode 都会被销毁。

但在线更新 `supportVnodes` 不会产生持久化，当系统重启后，允许的最大 vnode 数量仍然由 taos.cfg 中配置的 `supportVnodes` 决定。

如果通过在线更新或配置文件方式设置的 `supportVnodes` 小于 dnode 当前已经实际存在的 vnode 数量，已经存在的 vnode 不会受影响。但当尝试创建新的 database 时，是否能够创建成功则仍然受实际生效的 `supportVnodes` 参数决定。

## 双副本

双副本是一种特殊的数据库高可用配置，本节对它的使用和维护操作进行特别说明。该功能在 3.3.0.0 版本中第一次发布，建议尽可能使用最新版本。

### 查看 Vgroups 的状态

通过以下 SQL 命令参看双副本数据库中各 Vgroup 的状态：

```sql
show arbgroups;

select * from information_schema.ins_arbgroups;
            db_name             |  vgroup_id  | v1_dnode | v2_dnode | is_sync | assigned_dnode |         assigned_token         |
=================================================================================================================================
 db                             |           2 |        2 |        3 |       0 | NULL           | NULL                           |
 db                             |           3 |        1 |        2 |       0 |              1 | d1#g3#1714119404630#663        |
 db                             |           4 |        1 |        3 |       1 | NULL           | NULL                           |

```
is_sync 有以下两种取值：
- 0: Vgroup 数据未达成同步。在此状态下，如果 Vgroup 中的某一 Vnode 不可访问，另一个 Vnode 无法被指定为 `AssignedLeader` role，该 Vgroup 将无法提供服务。
- 1: Vgroup 数据达成同步。在此状态下，如果 Vgroup 中的某一 Vnode 不可访问，另一个 Vnode 可以被指定为 `AssignedLeader` role，该 Vgroup 可以继续提供服务。

assigned_dnode：
- 标识被指定为 AssignedLeader 的 Vnode 的 DnodeId
- 未指定 AssignedLeader时，该列显示 NULL

assigned_token：
- 标识被指定为 AssignedLeader 的 Vnode 的 Token
- 未指定 AssignedLeader时，该列显示 NULL

### 最佳实践

1. 全新部署

双副本的主要价值在于节省存储成本的同时能够有一定的高可用和高可靠能力。在实践中，推荐配置为：
- N 节点集群 （其中 N>=3）
- 其中 N-1 个 dnode 负责存储时序数据
- 第 N 个 dnode 不参与时序数据的存储和读取，即其上不保存副本；可以通过 `supportVnodes` 这个参数为 0 来实现这个目标
- 不存储数据副本的 dnode 对 CPU/Memory 资源的占用也较低，可以使用较低配置服务器

2. 从单副本升级

假定已经有一个单副本集群，其结点数为 N (N>=1)，欲将其升级为双副本集群，升级后需要保证 N>=3，且新加入的某个节点的 `supportVnodes` 参数配置为 0。在集群升级完成后使用  `alter database replica 2` 的命令修改某个特定数据库的副本数。
