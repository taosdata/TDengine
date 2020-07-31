# 集群管理

多个taosd的运行实例可以组成一个集群，以保证TDengine的高可靠运行，并提供水平扩展能力。要了解TDengine 2.0的集群管理，需要对集群的基本概念有所了解，请看TDengine 2.0整体架构一章。

TDengine的集群管理极其简单，除添加和删除节点需要人工干预之外，其他全部是自动完成，最大程度的降低了运维的工作量。本章对集群管理的操作做详细的描述。

## 创建第一个节点
集群是由一个一个dnode组成的，是从一个dnode的创建开始的。创建第一个节点很简单，确保系统配置文件taos.cfg里的参数first与second没有进行设置后，简单的运行taosd即可。配置文件taos.cfg里参数first与second缺省都是设置为空，因此无需特殊操作。如果为了统一配置，可以将参数first配置为第一个dnode的End Point。

这个节点创建后，它是集群中的第一个节点。如果该节点收到一个新节点加入集群的请求，它将检查这个新节点的End Point是否在集群的EP列表中，如果在，就容许其加入。如果不在列表中，就拒绝其加入。

## 节点管理
### 添加节点
具体有以下几个步骤：

1. 修改新节点配置文件taos.cfg, 将其中参数first与second设置为现有集群中运行节点的End Point, 然后启动taosd。second可以不用设置，目的是为集群规模较大时，first无法访问时，该节点将尝试访问second节点。
2. 使用CLI程序taos, 登录进系统, 使用命令:
   ```
   CREATE DNODE "fqdn:port"； 
   ```
   将新节点的End Point添加进集群的EP列表。**"fqdn:port"需要用双引号引起来**，否则出错。一个节点对外服务的fqdn和port可以通过配置文件taos.cfg进行配置。
3. 使用命令
   ```
   SHOW DNODES
   ```
   查看新节点是否被成功加入。`

**示例：**假设有两个节点，其EP分别为 host1:6030, host2:6050. 可以按照以下步骤创建集群

1. 在host1, 启动taosd;
2. 修改host2:6050的配置文件taos.cfg, 将参数first设置为host1:6030，然后启动host2:6050这个taosd实例;
3. 执行“taos -h host1" 链接到host1, 在TDengine console里执行命令：
   ```
   CREATE DNODE "host2:6050";
   ```
4. 在TDengine console里执行`"show dnodes"`查看节点是否加入成功.
5. 按照上述第2到第5步，逐步把其他节点添加到集群中。

**提示：**

- first, second这两个参数仅仅在该节点第一次加入集群时有作用，加入集群后，该节点会保存最新的mnode的End Point列表，不再依赖这两个参数。
- 两个没有配置first, second参数的dnode启动后，会独立运行起来。这个时候，无法将其中一个节点加入到另外一个节点，形成集群。**无法将两个独立的集群合并成为新的集群**。

### 删除节点
执行CLI程序taos, 使用root账号登录进TDengine系统，执行：
```
DROP DNODE "fqdn:port";
```
其中fqdn是被删除的节点的FQDN，port是其对外服务器的端口号

### 查看节点
执行CLI程序taos,使用root账号登录进TDengine系统，执行：
```
SHOW DNODES;
```
它将列出集群中所有的dnode,每个dnode的fqdn:port, 状态(ready, offline等），vnode数目，还未使用的vnode数目等信息。在添加或删除一个节点后，可以使用该命令查看。

### 查看虚拟节点组

为充分利用多核技术，并提供scalability，数据需要分片处理。因此TDengine会将一个DB的数据切分成多份，存放在多个vnode里。这些vnode可能分布在多个dnode里，这样就实现了水平扩展。一个vnode仅仅属于一个DB，但一个DB可以有多个vnode。vnode的是mnode根据当前系统资源的情况，自动进行分配的，无需任何人工干预。

执行CLI程序taos,使用root账号登录进TDengine系统，执行：
```
SHOW VGROUPS;
```
## 高可用性
TDengine通过多副本的机制来提供系统的高可用性。副本数是与DB关联的，一个集群里可以有多个DB，根据运营的需求，每个DB可以配置不同的副本数。创建数据库时，通过参数replica 指定副本数（缺省为1）。如果副本数为1，系统的可靠性无法保证，只要数据所在的节点宕机，就将无法提供服务。集群的节点数必须大于等于副本数，否则创建表时将返回错误“more dnodes are needed"。比如下面的命令将创建副本数为3的数据库demo：
```
CREATE DATABASE demo replica 3;
```
一个DB里的数据会被切片分到多个vnode group，vnode group里的vnode数目就是DB的副本数，同一个vnode group里各vnode的数据是完全一致的。为保证高可用性，vnode group里的vnode一定要分布在不同的dnode里（实际部署时，需要在不同的物理机上），只要一个vgroup里超过半数的vnode处于工作状态，这个vgroup就能正常的对外服务。

一个dnode里可能有多个DB的数据，因此一个dnode离线时，可能会影响到多个DB。如果一个vnode group里的一半或一半以上的vnode不工作，那么该vnode group就无法对外服务，无法插入或读取数据，这样会影响到它所属的DB的一部分表的d读写操作。

因为vnode的引入，无法简单的给出结论：“集群中过半dnode工作，集群就应该工作”。但是对于简单的情形，很好下结论。比如副本数为3，只有三个dnode，那如果仅有一个节点不工作，整个集群还是可以正常工作的，但如果有两个节点不工作，那整个集群就无法正常工作了。

## Mnode的高可用
TDengine集群是由mnode (taosd的一个模块，逻辑节点) 负责管理的，为保证mnode的高可用，可以配置多个mnode副本，副本数由系统配置参数numOfMnodes决定，有效范围为1-3。为保证元数据的强一致性，mnode副本之间是通过同步的方式进行数据复制的。

一个集群有多个dnode, 但一个dnode至多运行一个mnode实例。多个dnode情况下，哪个dnode可以作为mnode呢？这是完全由系统根据整个系统资源情况，自动指定的。用户可通过CLI程序taos，在TDengine的console里，执行如下命令：
```
SHOW MNODES;
```
来查看mnode列表，该列表将列出mnode所处的dnode的End Point和角色(master, slave, unsynced 或offline)。
当集群中第一个节点启动时，该节点一定会运行一个mnode实例，否则该dnode无法正常工作，因为一个系统是必须有至少一个mnode的。如果numOfMnodes配置为2，启动第二个dnode时，该dnode也将运行一个mnode实例。

为保证mnode服务的高可用性，numOfMnodes必须设置为2或更大。因为mnode保存的元数据必须是强一致的，如果numOfMnodes大于2，复制参数quorum自动设为2，也就是说，至少要保证有两个副本写入数据成功，才通知客户端应用写入成功。

## 负载均衡

有三种情况，将触发负载均衡，而且都无需人工干预。

- 当一个新节点添加进集群时，系统将自动触发负载均衡，一些节点上的数据将被自动转移到新节点上，无需任何人工干预。
- 当一个节点从集群中移除时，系统将自动把该节点上的数据转移到其他节点，无需任何人工干预。
- 如果一个节点过热（数据量过大），系统将自动进行负载均衡，将该节点的一些vnode自动挪到其他节点。

当上述三种情况发生时，系统将启动一各个节点的负载计算，从而决定如何挪动。

## 节点离线处理
如果一个节点离线，TDengine集群将自动检测到。有如下两种情况：
- 改节点离线超过一定时间（taos.cfg里配置参数offlineThreshold控制时长)，系统将自动把该节点删除，产生系统报警信息，触发负载均衡流程。如果该被删除的节点重现上线时，它将无法加入集群，需要系统管理员重新将其添加进集群才会开始工作。
- 离线后，在offlineThreshold的时长内重新上线，系统将自动启动数据恢复流程，等数据完全恢复后，该节点将开始正常工作。

## Arbitrator的使用

如果副本数为偶数，当一个vnode group里一半vnode不工作时，是无法从中选出master的。同理，一半mnode不工作时，是无法选出mnode的master的，因为存在“split brain”问题。为解决这个问题，TDengine引入了arbitrator的概念。Arbitrator模拟一个vnode或mnode在工作，但只简单的负责网络连接，不处理任何数据插入或访问。只要包含arbitrator在内，超过半数的vnode或mnode工作，那么该vnode group或mnode组就可以正常的提供数据插入或查询服务。比如对于副本数为2的情形，如果一个节点A离线，但另外一个节点B正常，而且能连接到arbitrator, 那么节点B就能正常工作。

TDengine安装包里带有一个执行程序tarbitrator, 找任何一台Linux服务器运行它即可。该程序对系统资源几乎没有要求，只需要保证有网络连接即可。该应用的命令行参数`-p`可以指定其对外服务的端口号，缺省是6030。配置每个taosd实例时，可以在配置文件taos.cfg里将参数arbitrator设置为arbitrator的End Point。如果该参数配置了，当副本数为偶数数，系统将自动连接配置的arbitrator。
