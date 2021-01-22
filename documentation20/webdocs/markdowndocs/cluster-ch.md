# TDengine 集群安装、管理

多个TDengine服务器，也就是多个taosd的运行实例可以组成一个集群，以保证TDengine的高可靠运行，并提供水平扩展能力。要了解TDengine 2.0的集群管理，需要对集群的基本概念有所了解，请看TDengine 2.0整体架构一章。而且在安装集群之前，先请按照[《立即开始》](https://www.taosdata.com/cn/getting-started20/)一章安装并体验单节点功能。

集群的每个数据节点是由End Point来唯一标识的，End Point是由FQDN(Fully Qualified Domain Name)外加Port组成，比如 h1.taosdata.com:6030。一般FQDN就是服务器的hostname，可通过Linux命令`hostname -f`获取（如何配置FQDN，请参考：[一篇文章说清楚TDengine的FQDN](https://www.taosdata.com/blog/2020/09/11/1824.html)）。端口是这个数据节点对外服务的端口号，缺省是6030，但可以通过taos.cfg里配置参数serverPort进行修改。一个物理节点可能配置了多个hostname, TDengine会自动获取第一个，但也可以通过taos.cfg里配置参数fqdn进行指定。如果习惯IP地址直接访问，可以将参数fqdn设置为本节点的IP地址。

TDengine的集群管理极其简单，除添加和删除节点需要人工干预之外，其他全部是自动完成，最大程度的降低了运维的工作量。本章对集群管理的操作做详细的描述。

关于集群搭建请参考<a href="https://www.taosdata.com/blog/2020/11/11/1961.html">视频教程</a>。

## 准备工作

**第零步**：规划集群所有物理节点的FQDN，将规划好的FQDN分别添加到每个物理节点的/etc/hostname；修改每个物理节点的/etc/hosts，将所有集群物理节点的IP与FQDN的对应添加好。【如部署了DNS，请联系网络管理员在DNS上做好相关配置】

**第一步**：如果搭建集群的物理节点中，存有之前的测试数据、装过1.X的版本，或者装过其他版本的TDengine，请先将其删除，并清空所有数据，具体步骤请参考博客[《TDengine多种安装包的安装和卸载》](https://www.taosdata.com/blog/2019/08/09/566.html ) 
**注意1：**因为FQDN的信息会写进文件，如果之前没有配置或者更改FQDN，且启动了TDengine。请一定在确保数据无用或者备份的前提下，清理一下之前的数据（rm -rf /var/lib/taos/）；
**注意2：**客户端也需要配置，确保它可以正确解析每个节点的FQDN配置，不管是通过DNS服务，还是 Host 文件。

**第二步**：建议关闭所有物理节点的防火墙，至少保证端口：6030 - 6042的TCP和UDP端口都是开放的。**强烈建议**先关闭防火墙，集群搭建完毕之后，再来配置端口；

**第三步**：在所有物理节点安装TDengine，且版本必须是一致的，**但不要启动taosd**。安装时，提示输入是否要加入一个已经存在的TDengine集群时，第一个物理节点直接回车创建新集群，后续物理节点则输入该集群任何一个在线的物理节点的FQDN:端口号(默认6030)；

**第四步**：检查所有数据节点，以及应用程序所在物理节点的网络设置：

1. 每个物理节点上执行命令`hostname -f`，查看和确认所有节点的hostname是不相同的(应用驱动所在节点无需做此项检查)；
2. 每个物理节点上执行`ping host`, 其中host是其他物理节点的hostname, 看能否ping通其它物理节点; 如果不能ping通，需要检查网络设置, 或/etc/hosts文件(Windows系统默认路径为C:\Windows\system32\drivers\etc\hosts)，或DNS的配置。如果无法ping通，是无法组成集群的；
3. 从应用运行的物理节点，ping taosd运行的数据节点，如果无法ping通，应用是无法连接taosd的，请检查应用所在物理节点的DNS设置或hosts文件；
4. 每个数据节点的End Point就是输出的hostname外加端口号，比如h1.taosdata.com:6030

**第五步**：修改TDengine的配置文件（所有节点的文件/etc/taos/taos.cfg都需要修改）。假设准备启动的第一个数据节点End Point为 h1.taosdata.com:6030, 其与集群配置相关参数如下：

```
// firstEp 是每个数据节点首次启动后连接的第一个数据节点
firstEp               h1.taosdata.com:6030

// 必须配置为本数据节点的FQDN，如果本机只有一个hostname, 可注释掉本配置
fqdn                  h1.taosdata.com  

// 配置本数据节点的端口号，缺省是6030
serverPort            6030

// 使用场景，请参考《Arbitrator的使用》的部分
arbitrator            ha.taosdata.com:6042
```

一定要修改的参数是firstEp和fqdn。在每个数据节点，firstEp需全部配置成一样，**但fqdn一定要配置成其所在数据节点的值**。其他参数可不做任何修改，除非你很清楚为什么要修改。

**加入到集群中的数据节点dnode，涉及集群相关的下表11项参数必须完全相同，否则不能成功加入到集群中。**

| **#** | **配置参数名称**   | **含义**                                 |
| ----- | ------------------ | ---------------------------------------- |
| 1     | numOfMnodes        | 系统中管理节点个数                       |
| 2     | mnodeEqualVnodeNum | 一个mnode等同于vnode消耗的个数           |
| 3     | offlineThreshold   | dnode离线阈值，超过该时间将导致Dnode离线 |
| 4     | statusInterval     | dnode向mnode报告状态时长                 |
| 5     | arbitrator         | 系统中裁决器的end point                  |
| 6     | timezone           | 时区                                     |
| 7     | locale             | 系统区位信息及编码格式                   |
| 8     | charset            | 字符集编码                               |
| 9     | balance            | 是否启动负载均衡                         |
| 10    | maxTablesPerVnode  | 每个vnode中能够创建的最大表个数          |
| 11    | maxVgroupsPerDb    | 每个DB中 能够使用的最大vnode个数         |

 

## 启动第一个数据节点

按照[《立即开始》](https://www.taosdata.com/cn/getting-started20/)里的指示，启动第一个数据节点，例如h1.taosdata.com，然后执行taos, 启动taos shell，从shell里执行命令"show dnodes;"，如下所示：

   ```
Welcome to the TDengine shell from Linux, Client Version:2.0.0.0
Copyright (c) 2017 by TAOS Data, Inc. All rights reserved.

taos> show dnodes;
 id |       end_point    | vnodes | cores | status | role |      create_time        |
=====================================================================================
  1 |  h1.taos.com:6030  |      0 |     2 |  ready |  any | 2020-07-31 03:49:29.202 |
Query OK, 1 row(s) in set (0.006385s)

taos>
   ```

上述命令里，可以看到这个刚启动的这个数据节点的End Point是：h1.taos.com:6030，就是这个新集群的firstEP。

## 启动后续数据节点

将后续的数据节点添加到现有集群，具体有以下几步：

1. 按照["立即开始“](https://www.taosdata.com/cn/getting-started/)一章的方法在每个物理节点启动taosd；

2. 在第一个数据节点，使用CLI程序taos, 登录进TDengine系统, 执行命令:

   ```
   CREATE DNODE "h2.taos.com:6030"; 
   ```

   将新数据节点的End Point (准备工作中第四步获知的) 添加进集群的EP列表。**"fqdn:port"需要用双引号引起来**，否则出错。请注意将示例的“h2.taos.com:6030" 替换为这个新数据节点的End Point。

3. 然后执行命令

   ```
   SHOW DNODES;
   ```

   查看新节点是否被成功加入。如果该被加入的数据节点处于离线状态，请做两个检查

   - 查看该数据节点的taosd是否正常工作，如果没有正常运行，需要先检查为什么
   - 查看该数据节点taosd日志文件taosdlog.0里前面几行日志(一般在/var/log/taos目录)，看日志里输出的该数据节点fqdn以及端口号是否为刚添加的End Point。如果不一致，需要将正确的End Point添加进去。

按照上述步骤可以源源不断的将新的数据节点加入到集群。

**提示：**

- 任何已经加入集群在线的数据节点，都可以作为后续待加入节点的firstEP。
- firstEp这个参数仅仅在该数据节点首次加入集群时有作用，加入集群后，该数据节点会保存最新的mnode的End Point列表，不再依赖这个参数。
- 两个没有配置firstEp参数的数据节点dnode启动后，会独立运行起来。这个时候，无法将其中一个数据节点加入到另外一个数据节点，形成集群。**无法将两个独立的集群合并成为新的集群**。

## 数据节点管理

上面已经介绍如何从零开始搭建集群。集群组建完后，还可以随时添加新的数据节点进行扩容，或删除数据节点，并检查集群当前状态。

### 添加数据节点

执行CLI程序taos, 使用root账号登录进系统, 执行:

```
CREATE DNODE "fqdn:port"; 
```

将新数据节点的End Point添加进集群的EP列表。**"fqdn:port"需要用双引号引起来**，否则出错。一个数据节点对外服务的fqdn和port可以通过配置文件taos.cfg进行配置，缺省是自动获取。【强烈不建议用自动获取方式来配置FQDN，可能导致生成的数据节点的End Point不是所期望的】

### 删除数据节点

执行CLI程序taos, 使用root账号登录进TDengine系统，执行：

```
DROP DNODE "fqdn:port";
```

其中fqdn是被删除的节点的FQDN，port是其对外服务器的端口号

<font color=green>**【注意】**</font>

  - 一个数据节点一旦被drop之后，不能重新加入集群。需要将此节点重新部署（清空数据文件夹）。集群在完成drop dnode操作之前，会将该dnode的数据迁移走。

  - 请注意 drop dnode 和 停止taosd进程是两个不同的概念，不要混淆：因为删除dnode之前要执行迁移数据的操作，因此被删除的dnode必须保持在线状态。待删除操作结束之后，才能停止taosd进程。

  - 一个数据节点被drop之后，其他节点都会感知到这个dnodeID的删除操作，任何集群中的节点都不会再接收此dnodeID的请求。

  - dnodeID的是集群自动分配的，不得人工指定。它在生成时递增的，不会重复。

### 查看数据节点

执行CLI程序taos,使用root账号登录进TDengine系统，执行：

```
SHOW DNODES;
```

它将列出集群中所有的dnode,每个dnode的fqdn:port, 状态(ready, offline等），vnode数目，还未使用的vnode数目等信息。在添加或删除一个数据节点后，可以使用该命令查看。

### 查看虚拟节点组

为充分利用多核技术，并提供scalability，数据需要分片处理。因此TDengine会将一个DB的数据切分成多份，存放在多个vnode里。这些vnode可能分布在多个数据节点dnode里，这样就实现了水平扩展。一个vnode仅仅属于一个DB，但一个DB可以有多个vnode。vnode的是mnode根据当前系统资源的情况，自动进行分配的，无需任何人工干预。

执行CLI程序taos,使用root账号登录进TDengine系统，执行：

```
SHOW VGROUPS;
```

## vnode的高可用性

TDengine通过多副本的机制来提供系统的高可用性，包括vnode和mnode的高可用性。

vnode的副本数是与DB关联的，一个集群里可以有多个DB，根据运营的需求，每个DB可以配置不同的副本数。创建数据库时，通过参数replica 指定副本数（缺省为1）。如果副本数为1，系统的可靠性无法保证，只要数据所在的节点宕机，就将无法提供服务。集群的节点数必须大于等于副本数，否则创建表时将返回错误“more dnodes are needed"。比如下面的命令将创建副本数为3的数据库demo：

```
CREATE DATABASE demo replica 3;
```

一个DB里的数据会被切片分到多个vnode group，vnode group里的vnode数目就是DB的副本数，同一个vnode group里各vnode的数据是完全一致的。为保证高可用性，vnode group里的vnode一定要分布在不同的数据节点dnode里（实际部署时，需要在不同的物理机上），只要一个vgroup里超过半数的vnode处于工作状态，这个vgroup就能正常的对外服务。

一个数据节点dnode里可能有多个DB的数据，因此一个dnode离线时，可能会影响到多个DB。如果一个vnode group里的一半或一半以上的vnode不工作，那么该vnode group就无法对外服务，无法插入或读取数据，这样会影响到它所属的DB的一部分表的读写操作。

因为vnode的引入，无法简单的给出结论：“集群中过半数据节点dnode工作，集群就应该工作”。但是对于简单的情形，很好下结论。比如副本数为3，只有三个dnode，那如果仅有一个节点不工作，整个集群还是可以正常工作的，但如果有两个数据节点不工作，那整个集群就无法正常工作了。

## Mnode的高可用性

TDengine集群是由mnode (taosd的一个模块，管理节点) 负责管理的，为保证mnode的高可用，可以配置多个mnode副本，副本数由系统配置参数numOfMnodes决定，有效范围为1-3。为保证元数据的强一致性，mnode副本之间是通过同步的方式进行数据复制的。

一个集群有多个数据节点dnode, 但一个dnode至多运行一个mnode实例。多个dnode情况下，哪个dnode可以作为mnode呢？这是完全由系统根据整个系统资源情况，自动指定的。用户可通过CLI程序taos，在TDengine的console里，执行如下命令：

```
SHOW MNODES;
```

来查看mnode列表，该列表将列出mnode所处的dnode的End Point和角色(master, slave, unsynced 或offline)。
当集群中第一个数据节点启动时，该数据节点一定会运行一个mnode实例，否则该数据节点dnode无法正常工作，因为一个系统是必须有至少一个mnode的。如果numOfMnodes配置为2，启动第二个dnode时，该dnode也将运行一个mnode实例。

为保证mnode服务的高可用性，numOfMnodes必须设置为2或更大。因为mnode保存的元数据必须是强一致的，如果numOfMnodes大于2，复制参数quorum自动设为2，也就是说，至少要保证有两个副本写入数据成功，才通知客户端应用写入成功。

**注意：**一个TDengine高可用系统，无论是vnode还是mnode, 都必须配置多个副本。

## 负载均衡

有三种情况，将触发负载均衡，而且都无需人工干预。

- 当一个新数据节点添加进集群时，系统将自动触发负载均衡，一些节点上的数据将被自动转移到新数据节点上，无需任何人工干预。
- 当一个数据节点从集群中移除时，系统将自动把该数据节点上的数据转移到其他数据节点，无需任何人工干预。
- 如果一个数据节点过热（数据量过大），系统将自动进行负载均衡，将该数据节点的一些vnode自动挪到其他节点。

当上述三种情况发生时，系统将启动一各个数据节点的负载计算，从而决定如何挪动。

**【提示】负载均衡由参数balance控制，它决定是否启动自动负载均衡。**

## 数据节点离线处理

如果一个数据节点离线，TDengine集群将自动检测到。有如下两种情况：

- 该数据节点离线超过一定时间（taos.cfg里配置参数offlineThreshold控制时长)，系统将自动把该数据节点删除，产生系统报警信息，触发负载均衡流程。如果该被删除的数据节点重新上线时，它将无法加入集群，需要系统管理员重新将其添加进集群才会开始工作。
- 离线后，在offlineThreshold的时长内重新上线，系统将自动启动数据恢复流程，等数据完全恢复后，该节点将开始正常工作。

**注意：**如果一个虚拟节点组（包括mnode组）里所归属的每个数据节点都处于离线或unsynced状态，必须等该虚拟节点组里的所有数据节点都上线、都能交换状态信息后，才能选出Master，该虚拟节点组才能对外提供服务。比如整个集群有3个数据节点，副本数为3，如果3个数据节点都宕机，然后2个数据节点重启，是无法工作的，只有等3个数据节点都重启成功，才能对外服务。

## Arbitrator的使用

如果副本数为偶数，当一个vnode group里一半vnode不工作时，是无法从中选出master的。同理，一半mnode不工作时，是无法选出mnode的master的，因为存在“split brain”问题。为解决这个问题，TDengine引入了Arbitrator的概念。Arbitrator模拟一个vnode或mnode在工作，但只简单的负责网络连接，不处理任何数据插入或访问。只要包含Arbitrator在内，超过半数的vnode或mnode工作，那么该vnode group或mnode组就可以正常的提供数据插入或查询服务。比如对于副本数为2的情形，如果一个节点A离线，但另外一个节点B正常，而且能连接到Arbitrator，那么节点B就能正常工作。

TDengine提供一个执行程序，名为 tarbitrator，找任何一台Linux服务器运行它即可。请点击[安装包下载](https://www.taosdata.com/cn/all-downloads/)，在TDengine Arbitrator Linux一节中，选择适合的版本下载并安装。该程序对系统资源几乎没有要求，只需要保证有网络连接即可。该应用的命令行参数`-p`可以指定其对外服务的端口号，缺省是6042。配置每个taosd实例时，可以在配置文件taos.cfg里将参数arbitrator设置为Arbitrator的End Point。如果该参数配置了，当副本数为偶数时，系统将自动连接配置的Arbitrator。如果副本数为奇数，即使配置了Arbitrator，系统也不会去建立连接。

