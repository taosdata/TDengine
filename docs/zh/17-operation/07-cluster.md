---
title: 集群运维
description: TDengine 提供了多种集群运维手段以使集群运行更健康更高效
---

为了使集群运行更健康更高效，TDengine 企业版提供了一些运维手段来帮助系统管理员更好地运维集群。

## 数据重整

TDengine 面向多种写入场景，在有些写入场景下，TDengine 的存储会导致数据存储的放大或数据文件的空洞等。这一方面影响数据的存储效率，另一方面也会影响查询效率。为了解决上述问题，TDengine 企业版提供了对数据的重整功能，即 DATA COMPACT 功能，将存储的数据文件重新整理，删除文件空洞和无效数据，提高数据的组织度，从而提高存储和查询的效率。

**语法**

```sql
COMPACT DATABASE db_name [start with 'XXXX'] [end with 'YYYY']； 
```

**效果**

-   扫描并压缩指定的 DB 中所有 VGROUP 中 VNODE 的所有数据文件
-   COMPCAT 会删除被删除数据以及被删除的表的数据
-   COMPACT 会合并多个 STT 文件
-   可通过 start with 关键字指定 COMPACT 数据的起始时间
-   可通过 end with 关键字指定 COMPACT 数据的终止时间

**补充说明**

-   COMPACT 为异步，执行 COMPACT 命令后不会等 COMPACT 结束就会返回。如果上一个 COMPACT 没有完成则再发起一个 COMPACT 任务，则会等上一个任务完成后再返回。
-   COMPACT 可能阻塞写入，但不阻塞查询
-   COMPACT 的进度不可观测

## 集群负载再平衡

当多副本集群中的一个或多个节点因为升级或其它原因而重启后，有可能出现集群中各个 dnode 负载不均衡的现象，极端情况下会出现所有 vgroup 的 leader 都位于同一个 dnode 的情况。为了解决这个问题，可以使用下面的命令

```sql
balance vgroup leader;
```

**功能**

让所有的 vgroup 的 leade r在各自的replica节点上均匀分布。这个命令会让 vgroup 强制重新选举，通过重新选举，在选举的过程中，变换 vgroup 的leader，通过这个方式，最终让leader均匀分布。

**注意**

Raft选举本身带有随机性，所以通过选举的重新分布产生的均匀分布也是带有一定的概率，不会完全的均匀。**该命令的副作用是影响查询和写入**，在vgroup重新选举时，从开始选举到选举出新的 leader 这段时间，这 个vgroup 无法写入和查询。选举过程一般在秒级完成。所有的vgroup会依次逐个重新选举。

## 恢复数据节点

在多节点三副本的集群环境中，如果某个 dnode 的磁盘损坏，该 dnode 会自动退出，但集群中其它的 dnode 仍然能够继续提供写入和查询服务。

在更换了损坏的磁盘后，如果想要让曾经主动退出的 dnode 重新加入集群提供服务，可以通过 `restore dnode` 命令来恢复该数据节点上的部分或全部逻辑节点，该功能依赖多副本中的其它副本进行数据复制，所以只在集群中 dnode 数量大于等于 3 且副本数为 3 的情况下能够工作。


```sql
restore dnode <dnode_id>；# 恢复dnode上的mnode，所有vnode和qnode
restore mnode on dnode <dnode_id>；# 恢复dnode上的mnode
restore vnode on dnode <dnode_id> ；# 恢复dnode上的所有vnode
restore qnode on dnode <dnode_id>；# 恢复dnode上的qnode
```

**限制**
- 该功能是基于已有的复制功能的恢复，不是灾难恢复或者备份恢复，所以对于要恢复的 mnode 和 vnode来说，使用该命令的前提是还存在该 mnode 或 vnode 的其它两个副本仍然能够正常工作。
- 该命令不能修复数据目录中的个别文件的损坏或者丢失。例如，如果某个 mnode 或者 vnode 中的个别文件或数据损坏，无法单独恢复损坏的某个文件或者某块数据。此时，可以选择将该  mnode/vnode 的数据全部清空再进行恢复。


## 虚拟组分裂 （Scale Out）

当一个 vgroup 因为子表数过多而导致 CPU 或 Disk 资源使用量负载过高时，增加 dnode 节点后，可通过 `split vgroup` 命令把该 vgroup 分裂为两个虚拟组。分裂完成后，新产生的两个 vgroup 承担原来由一个 vgroup 提供的读写服务。这也是 TDengine 为企业版用户提供的 scale out 集群的能力。

```sql
split vgroup <vgroup_id>
```

**注意**
- 单副本库虚拟组，在分裂完成后，历史时序数据总磁盘空间使用量，可能会翻倍。所以，在执行该操作之前，通过增加 dnode 节点方式，确保集群中有足够的 CPU 和磁盘资源，避免资源不足现象发生。
- 该命令为 DB 级事务；执行过程，当前DB的其它管理事务将会被拒绝。集群中，其它DB不受影响。
- 分裂任务执行过程中，可持续提供读写服务；期间，可能存在可感知的短暂的读写业务中断。
- 在分裂过程中，不支持流和订阅。分裂结束后，历史 WAL 会清空。
- 分裂过程中，可支持节点宕机重启容错；但不支持节点磁盘故障容错。