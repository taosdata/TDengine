#  TDengine 2.0 执行代码taosd的设计

逻辑上，TDengine系统包含dnode, taosc和App，dnode是服务器侧执行代码taosd的一个运行实例，因此taosd是TDengine的核心，本文对taosd的设计做一简单的介绍，模块内的实现细节请见其他文档。

## 系统模块图

taosd包含rpc, dnode, vnode, tsdb, query, cq, sync, wal, mnode, http, monitor等模块，具体如下图：

![modules.png](page://images/architecture/modules.png)

taosd的启动入口是dnode模块，dnode然后启动其他模块，包括可选配置的http, monitor模块。taosc或dnode之间交互的消息都是通过rpc模块进行，dnode模块根据接收到的消息类型，将消息分发到vnode或mnode的消息队列，或由dnode模块自己消费。dnode的工作线程(worker)消费消息队列里的消息，交给mnode或vnode进行处理。下面对各个模块做简要说明。

## RPC模块

该模块负责taosd与taosc, 以及其他数据节点之间的通讯。TDengine没有采取标准的HTTP或gRPC等第三方工具，而是实现了自己的通讯模块RPC。

考虑到物联网场景下，数据写入的包一般不大，因此除支持TCP连接之外，RPC还支持UDP连接。当数据包小于15K时，RPC将采用UDP方式进行连接，否则将采用TCP连接。对于查询类的消息，RPC不管包的大小，总是采取TCP连接。对于UDP连接，RPC实现了自己的超时、重传、顺序检查等机制，以保证数据可靠传输。

RPC模块还提供数据压缩功能，如果数据包的字节数超过系统配置参数compressMsgSize, RPC在传输中将自动压缩数据，以节省带宽。

为保证数据的安全和数据的integrity, RPC模块采用MD5做数字签名，对数据的真实性和完整性进行认证。

## DNODE模块

该模块是整个taosd的入口，它具体负责如下任务：

- 系统的初始化，包括
  - 从文件taos.cfg读取系统配置参数，从文件dnodeCfg.json读取数据节点的配置参数；
  - 启动RPC模块，并建立起与taosc通讯的server连接，与其他数据节点通讯的server连接；
  - 启动并初始化dnode的内部管理, 该模块将扫描该数据节点已有的vnode，并打开它们；
  - 初始化可配置的模块，如mnode, http, monitor等。
- 数据节点的管理，包括
  - 定时的向mnode发送status消息，报告自己的状态；
  - 根据mnode的指示，创建、改变、删除vnode；
  - 根据mnode的指示，修改自己的配置参数；
- 消息的分发、消费，包括
  - 为每一个vnode和mnode的创建并维护一个读队列、一个写队列；
  - 将从taosc或其他数据节点来的消息，根据消息类型，将其直接分发到不同的消息队列，或由自己的管理模块直接消费；
  - 维护一个读的线程池，消费读队列的消息，交给vnode或mnode处理。为支持高并发，一个读线程(Worker)可以消费多个队列的消息，一个读队列可以由多个worker消费；
  - 维护一个写的线程池，消费写队列的消息，交给vnode或mnode处理。为保证写操作的序列化，一个写队列只能由一个写线程负责，但一个写线程可以负责多个写队列。

taosd的消息消费由dnode通过读写线程池进行控制，是系统的中枢。该模块内的结构体图如下：

![dnode.png](page://images/architecture/dnode.png)

## VNODE模块

vnode是一独立的数据存储查询逻辑单元，但因为一个vnode只能容许一个DB，因此vnode内部没有account, DB, user等概念。为实现更好的模块化、封装以及未来的扩展，它有很多子模块，包括负责存储的TSDB，负责查询的Query, 负责数据复制的sync，负责数据库日志的的wal, 负责连续查询的cq(continuous query), 负责事件触发的流计算的event等模块，这些子模块只与vnode模块发生关系，与其他模块没有任何调用关系。模块图如下：

![vnode.png](page://images/architecture/vnode.png)

vnode模块向下，与dnodeVRead，dnodeVWrite发生互动，向上，与子模块发生互动。它主要的功能有：

- 协调各个子模块的互动。各个子模块之间都不直接调用，都需要通过vnode模块进行；
- 对于来自taosc或mnode的写操作，vnode模块将其分解为写日志(wal), 转发(sync), 本地存储(tsdb)子模块的操作；
- 对于查询操作，分发到query模块进行。

一个数据节点里有多个vnode, 因此vnode模块是有多个运行实例的。每个运行实例是完全独立的。

vnode与其子模块是通过API直接调用，而不是通过消息队列传递。而且各个子模块只与vnode模块有交互，不与dnode, rpc等模块发生任何直接关联。

## MNODE模块

mnode是整个系统的大脑，负责整个系统的资源调度，负责meta data的管理与存储。

一个运行的系统里，只有一个mnode，但它有多个副本（由系统配置参数numOfMnodes控制）。这些副本分布在不同的dnode里，目的是保证系统的高可靠运行。副本之间的数据复制是采用同步而非异步的方式，以确保数据的一致性，确保数据不会丢失。这些副本会自动选举一个Master，其他副本是slave。所有数据更新类的操作，都只能在master上进行，而查询类的可以在slave节点上进行。代码实现上，同步模块与vnode共享，但mnode被分配一个特殊的vgroup ID: 1，而且quorum大于1。整个集群系统是由多个dnode组成的，运行的mnode的副本数不可能超过dnode的个数，但不会超过配置的副本数。如果某个mnode副本宕机一段时间，只要超过半数的mnode副本仍在运行，运行的mnode会自动根据整个系统的资源情况，在其他dnode里再启动一个mnode, 以保证运行的副本数。

各个dnode通过信息交换，保存有mnode各个副本的End Point列表，并向其中的master节点定时（间隔由系统配置参数statusInterval控制）发送status消息，消息体里包含该dnode的CPU、内存、剩余存储空间、vnode个数，以及各个vnode的状态(存储空间、原始数据大小、记录条数、角色等）。这样mnode就了解整个系统的资源情况，如果用户创建新的表，就可以决定需要在哪个dnode创建；如果增加或删除dnode, 或者监测到某dnode数据过热、或离线太长，就可以决定需要挪动那些vnode，以实现负载均衡。

mnode里还负责account, user, DB, stable, table, vgroup, dnode的创建、删除与更新。mnode不仅把这些entity的meta data保存在内存，还做持久化存储。但为节省内存，各个表的标签值不保存在mnode（保存在vnode)，而且子表不维护自己的schema, 而是与stable共享。为减小mnode的查询压力，taosc会缓存table、stable的schema。对于查询类的操作，各个slave mnode也可以提供，以减轻master压力。

## TSDB模块

TSDB模块是VNODE中的负责快速高并发地存储和读取属于该VNODE的表的元数据及采集的时序数据的引擎。除此之外，TSDB还提供了表结构的修改、表标签值的修改等功能。TSDB提供API供VNODE和Query等模块调用。TSDB中存储了两类数据，1：元数据信息；2：时序数据

### 元数据信息

TSDB中存储的元数据包含属于其所在的VNODE中表的类型，schema的定义等。对于超级表和超级表下的子表而言，又包含了tag的schema定义以及子表的tag值等。对于元数据信息而言，TSDB就相当于一个全内存的KV型数据库，属于该VNODE的表对象全部在内存中，方便快速查询表的信息。除此之外，TSDB还对其中的子表，按照tag的第一列取值做了全内存的索引，大大加快了对于标签的过滤查询。TSDB中的元数据的最新状态在落盘时，会以追加（append-only）的形式，写入到meta文件中。meta文件只进行追加操作，即便是元数据的删除，也会以一条记录的形式写入到文件末尾。TSDB也提供了对于元数据的修改操作，如表schema的修改，tag schema的修改以及tag值的修改等。

### 时序数据

每个TSDB在创建时，都会事先分配一定量的内存缓冲区，且内存缓冲区的大小可配可修改。表采集的时序数据，在写入TSDB时，首先以追加的方式写入到分配的内存缓冲区中，同时建立基于时间戳的内存索引，方便快速查询。当内存缓冲区的数据积累到一定的程度时（达到内存缓冲区总大小的1/3），则会触发落盘操作，将缓冲区中的数据持久化到硬盘文件上。时序数据在内存缓冲区中是以行（row）的形式存储的。

而时序数据在写入到TSDB的数据文件时，是以列（column）的形式存储的。TSDB中的数据文件包含多个数据文件组，每个数据文件组中又包含.head、.data和.last三个文件，如（v2f1801.head、v2f1801.data、v2f1801.last）数据文件组。TSDB中的数据文件组是按照时间跨度进行分片的，默认是10天一个文件组，且可通过配置文件及建库选项进行配置。分片的数据文件组又按照编号递增排列，方便快速定位某一时间段的时序数据，高效定位数据文件组。时序数据在TSDB的数据文件中是以块的形式进行列式存储的，每个块中只包含一张表的数据，且数据在一个块中是按照时间顺序递增排列的。在一个数据文件组中，.head文件负责存储数据块的索引及统计信息，如每个块的位置，压缩算法，时间戳范围等。存储在.head文件中一张表的索引信息是按照数据块中存储的数据的时间递增排列的，方便进行折半查找等工作。.head和.last文件是存储真实数据块的文件，若数据块中的数据累计到一定程度，则会写入.data文件中，否则，会写入.last文件中，等待下次落盘时合并数据写入.data文件中，从而大大减少文件中块的个数，避免数据的过度碎片化。

## Query模块

该模块负责整体系统的查询处理。客户端调用该该模块进行SQL语法解析，并将查询或写入请求发送到vnode，同时负责针对超级表的查询进行二阶段的聚合操作。在Vnode端，该模块调用TSDB模块读取系统中存储的数据进行查询处理。Query模块还定义了系统能够支持的全部查询函数，查询函数的实现机制与查询框架无耦合，可以在不修改查询流程的情况下动态增加查询函数。详细的设计请参见《TDengine 2.0查询模块设计》。

## SYNC模块

该模块实现数据的多副本复制，包括vnode与mnode的数据复制，支持异步和同步两种复制方式，以满足meta data与时序数据不同复制的需求。因为它为mnode与vnode共享，系统为mnode副本预留了一个特殊的vgroup ID:1。因此vnode group的ID是从2开始的。

每个vnode/mnode模块实例会有一对应的sync模块实例，他们是一一对应的。详细设计请见[TDengine 2.0 数据复制模块设计](https://www.taosdata.com/cn/documentation/architecture/replica/)

## WAL模块

该模块负责将新插入的数据写入write ahead log(WAL), 为vnode, mnode共享。以保证服务器crash或其他故障，能从WAL中恢复数据。

每个vnode/mnode模块实例会有一对应的wal模块实例，是完全一一对应的。WAL的落盘操作由两个参数walLevel, fsync控制。看具体场景，如果要100%保证数据不会丢失，需要将walLevel配置为2，fsync设置为0，每条数据插入请求，都会实时落盘后，才会给应用确认

## HTTP模块

该模块负责处理系统对外的RESTful接口，可以通过配置，由dnode启动或停止。

该模块将接收到的RESTful请求，做了各种合法性检查后，将其变成标准的SQL语句，通过taosc的异步接口，将请求发往整个系统中的任一dnode。收到处理后的结果后，再翻译成HTTP协议，返回给应用。

如果HTTP模块启动，就意味着启动了一个taosc的实例。任一一个dnode都可以启动该模块，以实现对RESTful请求的分布式处理。

## Monitor模块

该模块负责检测一个dnode的运行状态，可以通过配置，由dnode启动或停止。原则上，每个dnode都应该启动一个monitor实例。

Monitor采集TDengine里的关键操作，比如创建、删除、更新账号、表、库等，而且周期性的收集CPU、内存、网络等资源的使用情况（采集周期由系统配置参数monitorInterval控制）。获得这些数据后，monitor模块将采集的数据写入系统的日志库(DB名字由系统配置参数monitorDbName控制）。

Monitor模块使用taosc来将采集的数据写入系统，因此每个monitor实例，都有一个taosc运行实例。

