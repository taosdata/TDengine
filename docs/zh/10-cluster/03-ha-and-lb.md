---
title: 高可用与负载均衡
---

## Vnode 的高可用性

TDengine 通过多副本的机制来提供系统的高可用性，包括 vnode 和 mnode 的高可用性。

vnode 的副本数是与 DB 关联的，一个集群里可以有多个 DB，根据运营的需求，每个 DB 可以配置不同的副本数。创建数据库时，通过参数 replica 指定副本数（缺省为 1）。如果副本数为 1，系统的可靠性无法保证，只要数据所在的节点宕机，就将无法提供服务。集群的节点数必须大于等于副本数，否则创建表时将返回错误“more dnodes are needed”。比如下面的命令将创建副本数为 3 的数据库 demo：

```sql
CREATE DATABASE demo replica 3;
```

一个 DB 里的数据会被切片分到多个 vnode group，vnode group 里的 vnode 数目就是 DB 的副本数，同一个 vnode group 里各 vnode 的数据是完全一致的。为保证高可用性，vnode group 里的 vnode 一定要分布在不同的数据节点 dnode 里（实际部署时，需要在不同的物理机上），只要一个 vnode group 里超过半数的 vnode 处于工作状态，这个 vnode group 就能正常的对外服务。

一个数据节点 dnode 里可能有多个 DB 的数据，因此一个 dnode 离线时，可能会影响到多个 DB。如果一个 vnode group 里的一半或一半以上的 vnode 不工作，那么该 vnode group 就无法对外服务，无法插入或读取数据，这样会影响到它所属的 DB 的一部分表的读写操作。

因为 vnode 的引入，无法简单地给出结论：“集群中过半数据节点 dnode 工作，集群就应该工作”。但是对于简单的情形，很好下结论。比如副本数为 3，只有三个 dnode，那如果仅有一个节点不工作，整个集群还是可以正常工作的，但如果有两个数据节点不工作，那整个集群就无法正常工作了。

## Mnode 的高可用性

TDengine 集群是由 mnode（taosd 的一个模块，管理节点）负责管理的，为保证 mnode 的高可用，可以配置多个 mnode 副本，副本数由系统配置参数 numOfMnodes 决定，有效范围为 1-3。为保证元数据的强一致性，mnode 副本之间是通过同步的方式进行数据复制的。

一个集群有多个数据节点 dnode，但一个 dnode 至多运行一个 mnode 实例。多个 dnode 情况下，哪个 dnode 可以作为 mnode 呢？这是完全由系统根据整个系统资源情况，自动指定的。用户可通过 CLI 程序 taos，在 TDengine 的 console 里，执行如下命令：

```sql
SHOW MNODES;
```

来查看 mnode 列表，该列表将列出 mnode 所处的 dnode 的 End Point 和角色（master，slave，unsynced 或 offline）。当集群中第一个数据节点启动时，该数据节点一定会运行一个 mnode 实例，否则该数据节点 dnode 无法正常工作，因为一个系统是必须有至少一个 mnode 的。如果 numOfMnodes 配置为 2，启动第二个 dnode 时，该 dnode 也将运行一个 mnode 实例。

为保证 mnode 服务的高可用性，numOfMnodes 必须设置为 2 或更大。因为 mnode 保存的元数据必须是强一致的，如果 numOfMnodes 大于 2，复制参数 quorum 自动设为 2，也就是说，至少要保证有两个副本写入数据成功，才通知客户端应用写入成功。

:::note
一个 TDengine 高可用系统，无论是 vnode 还是 mnode，都必须配置多个副本。

:::

## 负载均衡

有三种情况，将触发负载均衡，而且都无需人工干预。

当一个新数据节点添加进集群时，系统将自动触发负载均衡，一些节点上的数据将被自动转移到新数据节点上，无需任何人工干预。
当一个数据节点从集群中移除时，系统将自动把该数据节点上的数据转移到其他数据节点，无需任何人工干预。
如果一个数据节点过热（数据量过大），系统将自动进行负载均衡，将该数据节点的一些 vnode 自动挪到其他节点。
当上述三种情况发生时，系统将启动各个数据节点的负载计算，从而决定如何挪动。

:::tip
负载均衡由参数 balance 控制，它决定是否启动自动负载均衡，0 表示禁用，1 表示启用自动负载均衡。

:::

## 数据节点离线处理

如果一个数据节点离线，TDengine 集群将自动检测到。有如下两种情况：

该数据节点离线超过一定时间（taos.cfg 里配置参数 offlineThreshold 控制时长），系统将自动把该数据节点删除，产生系统报警信息，触发负载均衡流程。如果该被删除的数据节点重新上线时，它将无法加入集群，需要系统管理员重新将其添加进集群才会开始工作。

离线后，在 offlineThreshold 的时长内重新上线，系统将自动启动数据恢复流程，等数据完全恢复后，该节点将开始正常工作。

:::note
如果一个虚拟节点组（包括 mnode 组）里所归属的每个数据节点都处于离线或 unsynced 状态，必须等该虚拟节点组里的所有数据节点都上线、都能交换状态信息后，才能选出 Master，该虚拟节点组才能对外提供服务。比如整个集群有 3 个数据节点，副本数为 3，如果 3 个数据节点都宕机，然后 2 个数据节点重启，是无法工作的，只有等 3 个数据节点都重启成功，才能对外服务。

:::

## Arbitrator 的使用

如果副本数为偶数，当一个 vnode group 里一半或超过一半的 vnode 不工作时，是无法从中选出 master 的。同理，一半或超过一半的 mnode 不工作时，是无法选出 mnode 的 master 的，因为存在“split brain”问题。

为解决这个问题，TDengine 引入了 Arbitrator 的概念。Arbitrator 模拟一个 vnode 或 mnode 在工作，但只简单的负责网络连接，不处理任何数据插入或访问。只要包含 Arbitrator 在内，超过半数的 vnode 或 mnode 工作，那么该 vnode group 或 mnode 组就可以正常的提供数据插入或查询服务。比如对于副本数为 2 的情形，如果一个节点 A 离线，但另外一个节点 B 正常，而且能连接到 Arbitrator，那么节点 B 就能正常工作。

总之，在目前版本下，TDengine 建议在双副本环境要配置 Arbitrator，以提升系统的可用性。

Arbitrator 的执行程序名为 tarbitrator。该程序对系统资源几乎没有要求，只需要保证有网络连接，找任何一台 Linux 服务器运行它即可。以下简要描述安装配置的步骤：

请点击 安装包下载，在 TDengine Arbitrator Linux 一节中，选择合适的版本下载并安装。
该应用的命令行参数 -p 可以指定其对外服务的端口号，缺省是 6042。

修改每个 taosd 实例的配置文件，在 taos.cfg 里将参数 arbitrator 设置为 tarbitrator 程序所对应的 End Point。（如果该参数配置了，当副本数为偶数时，系统将自动连接配置的 Arbitrator。如果副本数为奇数，即使配置了 Arbitrator，系统也不会去建立连接。）

在配置文件中配置了的 Arbitrator，会出现在 SHOW DNODES 指令的返回结果中，对应的 role 列的值会是“arb”。
查看集群 Arbitrator 的状态【2.0.14.0 以后支持】

```sql
SHOW DNODES;
```
