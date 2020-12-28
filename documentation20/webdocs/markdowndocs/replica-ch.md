# TDengine 2.0 数据复制模块设计

## 数据复制概述

数据复制(Replication)是指同一份数据在多个物理地点保存。它的目的是防止数据丢失，提高系统的高可用性(High Availability)，而且通过应用访问多个副本，提升数据查询性能。

在高可靠的大数据系统里，数据复制是必不可少的一大功能。数据复制又分为实时复制与非实时复制。实时复制是指任何数据的更新(包括数据的增加、删除、修改）操作，会被实时的复制到所有副本，这样任何一台机器宕机或网络出现故障，整个系统还能提供最新的数据，保证系统的正常工作。而非实时复制，是指传统的数据备份操作，按照固定的时间周期，将一份数据全量或增量复制到其他地方。如果主节点宕机，副本是很大可能没有最新数据，因此在有些场景是无法满足要求的。

TDengine面向的是物联网场景，需要支持数据的实时复制，来最大程度保证系统的可靠性。实时复制有两种方式，一种是异步复制，一种是同步复制。异步复制(Asynchronous Replication)是指数据由Master转发给Slave后，Master并不需要等待Slave回复确认，这种方式效率高，但有极小的概率会丢失数据。同步复制是指Master将数据转发给Slave后，需要等待Slave的回复确认，才会通知应用写入成功，这种方式效率偏低，但能保证数据绝不丢失。

数据复制是与数据存储（写入、读取）密切相关的，但两者又是相对独立，可以完全脱耦的。在TDengine系统中，有两种不同类型的数据，一种是时序数据，由TSDB模块负责；一种是元数据(Meta Data), 由MNODE负责。这两种性质不同的数据都需要同步功能。数据复制模块通过不同的实例启动配置参数，为这两种类型数据都提供同步功能。

在阅读本文之前，请先阅读《<a href="../architecture/ ">TDengine 2.0 整体架构</a >》，了解TDengine的集群设计和基本概念

特别注明：本文中提到数据更新操作包括数据的增加、删除与修改。

## 基本概念和定义

TDengine里存在vnode, mnode, vnode用来存储时序数据，mnode用来存储元数据。但从同步数据复制的模块来看，两者没有本质的区别，因此本文里的虚拟节点不仅包括vnode, 也包括mnode, vgoup也指mnode group, 除非特别注明。

**版本(version)**：

一个虚拟节点组里多个虚拟节点互为备份，来保证数据的有效与可靠，是依靠虚拟节点组的数据版本号来维持的。TDengine2.0设计里，对于版本的定义如下：客户端发起增加、删除、修改的流程，无论是一条记录还是多条，只要是在一个请求里，这个数据更新请求被TDengine的一个虚拟节点收到后，经过合法性检查后，可以被写入系统时，就会被分配一个版本号。这个版本号在一个虚拟节点里从1开始，是单调连续递增的。无论这条记录是采集的时序数据还是meta data, 一样处理。当Master转发一个写入请求到slave时，必须带上版本号。一个虚拟节点将一数据更新请求写入WAL时，需要带上版本号。

不同虚拟节点组的数据版本号是完全独立的，互不相干的。版本号本质上是数据更新记录的transaction ID，但用来标识数据集的版本。

**角色(role)：**

一个虚拟节点可以是master, slave, unsynced或offline状态。

- master： 具有最新的数据，容许客户端往里写入数据，一个虚拟节点组，至多一个master.
- slave：与master是同步的，但不容许客户端往里写入数据，根据配置，可以容许客户端对其进行查询。
- unsynced: 节点处于非同步状态，比如虚拟节点刚启动、或与其他虚拟节点的连接出现故障等。处于该状态时，该虚拟节点既不能提供写入，也不能提供查询服务。
- offline: 由于宕机或网络原因，无法访问到某虚拟节点时，其他虚拟节点将该虚拟节点标为离线。但请注意，该虚拟节点本身的状态可能是unsynced或其他，但不会是离线。

**Quorum:**

指数据写入成功所需要的确认数。对于异步复制，quorum设为1，具有master角色的虚拟节点自己确认即可。对于同步复制，需要至少大于等于2。原则上，Quorum >=1 并且 Quorum <= replication(副本数）。这个参数在启动一个同步模块实例时需要提供。

**WAL：**

TDengine的WAL(Write Ahead Log)与cassandra的commit log, mySQL的bin log, Postgres的WAL没本质区别。没有写入数据库文件，还保存在内存的数据都会先存在WAL。当数据已经成功写入数据库数据文件，相应的WAL会被删除。但需要特别指明的是，在TDengine系统里，有几点：

- 每个虚拟节点有自己独立的wal
- WAL里包含而且仅仅包含来自客户端的数据更新操作，每个更新操作都会被打上一个版本号

**复制实例：**

复制模块只是一可执行的代码，复制实例是指正在运行的复制模块的一个实例，一个节点里，可以存在多个实例。原则上，一个节点有多少虚拟节点，就可以启动多少实例。对于副本数为1的场景，应用可以决定是否需要启动同步实例。应用启动一个同步模块的实例时，需要提供的就是虚拟节点组的配置信息，包括：

- 虚拟节点个数，即replication number
- 各虚拟节点所在节点的信息，包括node的end point
- quorum, 需要的数据写入成功的确认数
- 虚拟节点的初始版本号

## 数据复制模块的基本工作原理

TDengine采取的是Master-Slave模式进行同步，与流行的RAFT一致性算法比较一致。总结下来，有几点：

1. 一个vgroup里有一到多个虚拟节点，每个虚拟节点都有自己的角色
2. 客户端只能向角色是master的虚拟节点发起数据更新操作，因为master具有最新版本的数据，如果向非Master发起数据更新操作，会直接收到错误
3. 客户端可以向master, 也可以向角色是Slave的虚拟节点发起查询操作，但不能对unsynced的虚拟节点发起任何操作
4. 如果master不存在，这个vgroup是不能对外提供数据更新和查询服务的
5. master收到客户端的数据更新操作时，会将其转发给slave节点
6. 一个虚拟节点的版本号比master低的时候，会发起数据恢复流程，成功后，才会成为slave

数据实时复制有三个主要流程：选主、数据转发、数据恢复。后续做详细讨论。

## 虚拟节点之间的网络连接

虚拟节点之间通过TCP进行连接，节点之间的状态交换、数据包的转发都是通过这个TCP连接(peerFd)进行。为避免竞争，两个虚拟节点之间的TCP连接，总是由IP地址(UINT32)小的节点作为TCP客户端发起。一旦TCP连接被中断，虚拟节点能通过TCP socket自动检测到，将对方标为offline。如果监测到任何错误（比如数据恢复流程），虚拟节点将主动重置该连接。

一旦作为客户端的节点连接不成或中断，它将周期性的每隔一秒钟去试图去连接一次。因为TCP本身有心跳机制，虚拟节点之间不再另行提供心跳。

如果一个unsynced节点要发起数据恢复流程，它与Master将建立起专有的TCP连接(syncFd)。数据恢复完成后，该连接会被关闭。而且为限制资源的使用，系统只容许一定数量(配置参数tsMaxSyncNum)的数据恢复的socket存在。如果超过这个数字，系统会将新的数据恢复请求延后处理。

任意一个节点，无论有多少虚拟节点，都会启动而且只会启动一个TCP server, 来接受来自其他虚拟节点的上述两类TCP的连接请求。当TCP socket建立起来，客户端侧发送的消息体里会带有vgId（全局唯一的vgroup ID), TCP 服务器侧会检查该vgId是否已经在该节点启动运行。如果已经启动运行，就接受其请求。如果不存在，就直接将连接请求关闭。在TDengine代码里，mnode group的vgId设置为1。 

## 选主流程

当同一组的两个虚拟节点之间(vnode A, vnode B)建立连接后，他们互换status消息。status消息里包含本地存储的同一虚拟节点组内所有虚拟节点的role和version。

如果一个虚拟节点(vnode A)检测到与同一虚拟节点组内另外一虚拟节点（vnode B）的连接中断，vnode A将立即把vnode B的role设置为offline。无论是接收到另外一虚拟节点发来的status消息，还是检测与另外一虚拟节点的连接中断，该虚拟节点都将进入状态处理流程。状态处理流程的规则如下：

1. 如果检测到在线的节点数没有超过一半，则将自己的状态设置为unsynced.
2. 如果在线的虚拟节点数超过一半，会检查master节点是否存在，如果存在，则会决定是否将自己状态改为slave或启动数据恢复流程。
3. 如果master不存在，则会检查自己保存的各虚拟节点的状态信息与从另一节点接收到的是否一致，如果一致，说明节点组里状态已经稳定一致，则会触发选举流程。如果不一致，说明状态还没趋于一致，即使master不存在，也不进行选主。由于要求状态信息一致才进行选举，每个虚拟节点根据同样的信息，会选出同一个虚拟节点做master，无需投票表决。
4. 自己的状态是根据规则自己决定并修改的，并不需要其他节点同意，包括成为master。一个节点无权修改其他节点的状态。
5. 如果一个虚拟节点检测到自己或其他虚拟节点的role发生改变，该节点会广播它自己保存的各个虚拟节点的状态信息（role和version)。

具体的流程图如下：

<center> <img src="../assets/replica-master.png"> </center>

选择Master的具体规则如下：

1. 如果只有一个副本，该副本永远就是master
2. 所有副本都在线时，版本最高的被选为master
3. 在线的虚拟节点数过半，而且有虚拟节点是slave的话，该虚拟节点自动成为master
4. 对于2和3，如果多个虚拟节点满足成为master的要求，那么虚拟节点组的节点列表里，最前面的选为master

按照上面的规则，如果所有虚拟节点都是unsynced(比如全部重启），只有所有虚拟节点上线，才能选出master，该虚拟节点组才能开始对外提供服务。当一个虚拟节点的role发生改变时，sync模块回通过回调函数notifyRole通知应用。

## 数据转发流程

如果vnode A是master, vnode B是slave, vnode A能接受客户端的写请求，而vnode B不能。当vnode A收到写的请求后，遵循下面的流程：

<center> <img src="../assets/replica-forward.png"> </center>

1. 应用对写请求做基本的合法性检查，通过，则给改请求包打上一个版本号(version, 单调递增）
2. 应用将打上版本号的写请求封装一个WAL Head, 写入WAL(Write Ahead Log)
3. 应用调用API syncForwardToPeer，如多vnode B是slave状态，sync模块将包含WAL Head的数据包通过Forward消息发送给vnode B，否则就不转发。
4. vnode B收到Forward消息后，调用回调函数writeToCache, 交给应用处理
5. vnode B应用在写入成功后，都需要调用syncAckForward通知sync模块已经写入成功。
6. 如果quorum大于1，vnode B需要等待应用的回复确认，收到确认后，vnode B发送Forward Response消息给node A。
7. 如果quorum大于1，vnode A需要等待vnode B或其他副本对Forward消息的确认。
8. 如果quorum大于1，vnode A收到quorum-1条确认消息后，调用回调函数confirmForward，通知应用写入成功。
9. 如果quorum为1，上述6，7，8步不会发生。
10. 如果要等待slave的确认，master会启动2秒的定时器（可配置），如果超时，则认为失败。

对于回复确认，sync模块提供的是异步回调函数，因此APP在调用syncForwardToPeer之后，无需等待，可以处理下一个操作。在Master与Slave的TCP连接管道里，可能有多个Forward消息，这些消息是严格按照应用提供的顺序排好的。对于Forward Response也是一样，TCP管道里存在多个，但都是排序好的。这个顺序，SYNC模块并没有做特别的事情，是由APP单线程顺序写来保证的(TDengine里每个vnode的写数据，都是单线程）。

## 数据恢复流程

如果一虚拟节点(vnode B) 处于unsynced状态，master存在（vnode A)，而且其版本号比master的低，它将立即启动数据恢复流程。在理解恢复流程时，需要澄清几个关于文件的概念和处理规则。

1. 每个文件（无论是archived data的file还是wal)都有一个index, 这需要应用来维护(vnode里，该index就是fileId*3 + 0/1/2, 对应data, head与last三个文件）。如果index为0，表示系统里最老的数据文件。对于mode里的文件，数量是固定的，对应于acct, user, db, table等文件。
2. 任何一个数据文件(file)有名字、大小，还有一个magic number。只有文件名、大小与magic number一致时，两个文件才判断是一样的，无需同步。Magic number可以是checksum, 也可以是简单的文件大小。怎么计算magic，换句话说，如何检测数据文件是否有效，完全由应用决定。
3. 文件名的处理有点复杂，因为每台服务器的路径可能不一致。比如node A的TDengine的数据文件存放在 /etc/taos目录下，而node B的数据存放在 /home/jhtao目录下。因此同步模块需要应用在启动一个同步实例时提供一个path，这样两台服务器的绝对路径可以不一样，但仍然可以做对比，做同步。
4. 当sync模块调用回调函数getFileInfo获得数据文件信息时，有如下的规则
   1. index 为0，表示获取最老的文件，同时修改index返回给sync模块。如果index不为0，表示获取指定位置的文件。
   2. 如果name为空，表示sync想获取位于index位置的文件信息，包括magic, size。Master节点会这么调用
   3. 如果name不为空，表示sync想获取指定文件名和index的信息，slave节点会这么调用
   4. 如果某个index的文件不存在，magic返回0，表示文件已经是最后一个。因此整个系统里，文件的index必须是连续的一段整数。
5. 当sync模块调用回调函数getWalInfo获得wal信息时，有如下规则
   1. index为0，表示获得最老的WAL文件, 返回时，index更新为具体的数字
   2. 如果返回0，表示这是最新的一个WAL文件，如果返回值是1，表示后面还有更新的WAL文件
   3. 返回的文件名为空，那表示没有WAL文件
6. 无论是getFileInfo, 还是getWalInfo, 只要获取出错（不是文件不存在），返回-1即可，系统会报错，停止同步

整个数据恢复流程分为两大步骤，第一步，先恢复archived data(file), 然后恢复wal。具体流程如下：

<center> <img src="../assets/replica-restore.png"> </center>

1. 通过已经建立的TCP连接，发送sync req给master节点
2. master收到sync req后，以client的身份，向vnode B主动建立一新的专用于同步的TCP连接（syncFd)
3. 新的TCP连接建立成功后，master将开始retrieve流程，对应的，vnode B将同步启动restore流程
4. Retrieve/Restore流程里，先处理所有archived data (vnode里的data, head, last文件），后处理WAL data。
5. 对于archived data，master将通过回调函数getFileInfo获取数据文件的基本信息，包括文件名、magic以及文件大小。
6. master 将获得的文件名、magic以及文件大小发给vnode B
7. vnode B将回调函数getFile获得magic和文件大小，如果两者一致，就认为无需同步，如果两者不一致 ，就认为需要同步。vnode B将结果通过消息FileAck发回master
8. 如果文件需要同步，master就调用sendfile把整个文件发往vnode B
9. 如果文件不需要同步，master(vnode A)就重复5，6，7，8，直到所有文件被处理完

对于WAL同步，流程如下：

1. master节点调用回调函数getWalInfo，获取WAL的文件名。
2. 如果getWalInfo返回值大于0，表示该文件还不是最后一个WAL，因此master调用sendfile一下把该文件发送给vnode B
3. 如果getWalInfo返回时为0，表示该文件是最后一个WAL，因为文件可能还处于写的状态中，sync模块要根据WAL Head的定义逐条读出记录，然后发往vnode B。
4. vnode A读取TCP连接传来的数据，按照WAL Head，逐条读取，如果版本号比现有的大，调用回调函数writeToCache，交给应用处理。如果小，直接扔掉。
5. 上述流程循环，直到所有WAL文件都被处理完。处理完后，master就会将新来的数据包通过Forward消息转发给slave。

从同步文件启动起，sync模块会通过inotify监控所有处理过的file以及wal。一旦发现被处理过的文件有更新变化，同步流程将中止，会重新启动。因为有可能落盘操作正在进行（比如历史数据导入，内存数据落盘），把已经处理过的文件进行了修改，需要重新同步才行。

对于最后一个WAL (LastWal)的处理逻辑有点复杂，因为这个文件往往是打开写的状态，有很多场景需要考虑，比如：

- LastWal文件size在增长，需要重新读；
- LastWal文件虽然已经打开写，但内容为空；
- LastWal文件已经被关闭，应用生成了新的Last WAL文件;
- LastWal文件没有被关闭，但数据落盘的原因，没有读到完整的一条记录;
- LastWal文件没有被关闭，但数据落盘的原因，还有部分记录暂时读取不到；

sync模块通过inotify监控LastWal文件的更新和关闭操作。而且在确认已经尽可能读完LastWal的数据后，会将对方同步状态设置为SYNC_CACHE。该状态下，master节点会将新的记录转发给vnode B，而此时vnode B并没有完成同步，需要把这些转发包先存在recv buffer里，等WAL处理完后，vnode A再把recv buffer里的数据包通过回调writeToCache交给应用处理。

等vnode B把这些buffered forwards处理完，同步流程才算结束，vnode B正式变为slave。

## Master分布均匀性问题

因为Master负责写、转发，消耗的资源会更多，因此Master在整个集群里分布均匀比较理想。

但在TDengine的设计里，如果多个虚拟节点都符合master条件，TDengine选在列表中最前面的做Master, 这样是否导致在集群里，Master数量的分布不均匀问题呢？这取决于应用的设计。

给一个具体例子，系统里仅仅有三个节点，IP地址分别为IP1, IP2, IP3. 在各个节点上，TDengine创建了多个虚拟节点组，每个虚拟节点组都有三个副本。如果三个副本的顺序在所有虚拟节点组里都是IP1, IP2, IP3, 那毫无疑问，master将集中在IP1这个节点，这是我们不想看到的。

但是，如果在创建虚拟节点组时，增加随机性，这个问题就不存在了。比如在vgroup 1, 顺序是IP1, IP2, IP3, 在vgroup 2里，顺序是IP2, IP3, IP1, 在vgroup 3里，顺序是IP3, IP1, IP2。最后master的分布会是均匀的。

因此在创建一个虚拟节点组时，应用需要保证节点的顺序是round robin或完全随机。

## 少数虚拟节点写入成功的问题

在某种情况下，写入成功的确认数大于0，但小于配置的Quorum, 虽然有虚拟节点数据更新成功，master仍然会认为数据更新失败，并通知客户端写入失败。

这个时候，系统存在数据不一致的问题，因为有的虚拟节点已经写入成功，而有的写入失败。一个处理方式是，Master重置(reset)与其他虚拟节点的连接，该虚拟节点组将自动进入选举流程。按照规则，已经成功写入数据的虚拟节点将成为新的master，组内的其他虚拟节点将从master那里恢复数据。

因为写入失败，客户端会重新写入数据。但对于TDengine而言，是OK的。因为时序数据都是有时间戳的，时间戳相同的数据更新操作，第一次会执行，但第二次会自动扔掉。对于Meta Data(增加、删除库、表等等）的操作，也是OK的。一张表、库已经被创建或删除，再创建或删除，不会被执行的。

在TDengine的设计里，虚拟节点与虚拟节点之间，是一个TCP连接，是一个pipeline，数据块一个接一个按顺序在这个pipeline里等待处理。一旦某个数据块的处理失败，这个连接会被重置，后续的数据块的处理都会失败。因此不会存在Pipeline里一个数据块更新失败，但下一个数据块成功的可能。

## Split Brain的问题

选举流程中，有个强制要求，那就是一定有超过半数的虚拟节点在线。但是如果replication正好是偶数，这个时候，完全可能存在splt brain问题。

为解决这个问题，TDengine提供Arbitrator的解决方法。Arbitrator是一个节点，它的任务就是接受任何虚拟节点的连接请求，并保持它。

在启动复制模块实例时，在配置参数中，应用可以提供Arbitrator的IP地址。如果是奇数个副本，复制模块不会与这个arbitrator去建立连接，但如果是偶数个副本，就会主动去建立连接。

Arbitrator的程序tarbitrator.c在复制模块的同一目录, 编译整个系统时，会在bin目录生成。命令行参数“-？”查看可以配置的参数，比如绑定的IP地址，监听的端口号。

## 与RAFT相比的异同

数据一致性协议流行的有两种，Paxos与Raft. 本设计的实现与Raft有很多类同之处，下面做一些比较

相同之处：

- 三大流程一致：Raft里有Leader election, replication, safety，完全对应TDengine的选举、数据转发、数据恢复三个流程。
- 节点状态定义一致：Raft里每个节点有Leader, Follower, Candidate三个状态，TDengine里是Master, Slave, Unsynced, Offline。多了一个offlince, 但本质上是一样的，因为offline是外界看一个节点的状态，但该节点本身是处于master, slave 或unsynced的。
- 数据转发流程完全一样，Master(leader)需要等待回复确认。
- 数据恢复流程几乎一样，Raft没有涉及历史数据同步问题，只考虑了WAL数据同步。

不同之处：

- 选举流程不一样：Raft里任何一个节点是candidate时，主动向其他节点发出vote request, 如果超过半数回答Yes, 这个candidate就成为Leader,开始一个新的term. 而TDengine的实现里，节点上线、离线或角色改变都会触发状态消息在节点组类传播，等节点组里状态稳定一致之后才触发选举流程，因为状态稳定一致，基于同样的状态信息，每个节点做出的决定会是一致的，一旦某个节点符合成为master的条件，无需其他节点认可，它会自动将自己设为master。TDengine里，任何一个节点检测到其他节点或自己的角色发生改变，就会给节点组内其他节点进行广播的。Raft里不存在这样的机制，因此需要投票来解决。
- 对WAL的一条记录，Raft用term + index来做唯一标识。但TDengine只用version（类似index)，在TDengine实现里，仅仅用version是完全可行的, 因为TDengine的选举机制，没有term的概念。

如果整个虚拟节点组全部宕机，重启，但不是所有虚拟节点都上线，这个时候TDengine是不会选出master的，因为未上线的节点有可能有最高version的数据。而RAFT协议，只要超过半数上线，就会选出Leader。

## Meta Data的数据复制

TDengine里存在时序数据，也存在Meta Data。Meta Data对数据的可靠性要求更高，那么TDengine设计能否满足要求呢？下面做个仔细分析。

TDengine里Meta Data包括以下：

- account 信息
- 一个account下面，可以有多个user, 多个DB
- 一个DB下面有多个vgroup
- 一个DB下面有多个stable
- 一个vgroup下面有多个table
- 整个系统有多个mnode, dnode
- 一个dnode可以有多个vnode

上述的account, user, DB, vgroup, table, stable, mnode, dnode都有自己的属性，这些属性是TDengine自己定义的，不会开放给用户进行修改。这些Meta Data的查询都比较简单，都可以采用key-value模型进行存储。这些Meta Data还具有几个特点：

1. 上述的Meta Data之间有一定的层级关系，比如必须先创建DB，才能创建table, stable。只有先创建dnode，才可能创建vnode, 才可能创建vgroup。因此他们创建的顺序是绝对不能错的。
2. 在客户端应用的数据更新操作得到TDengine服务器侧确认后，所执行的数据更新操作绝对不能丢失。否则会造成客户端应用与服务器的数据不一致。
3. 上述的Meta Data是容许重复操作的。比如插入新记录后，再插入一次，删除一次后，再删除一次，更新一次后，再更新一次，不会对系统产生任何影响，不会改变系统任何状态。

对于特点1，本设计里，数据的写入是单线程的，按照到达的先后顺序，给每个数据更新操作打上版本号，版本号大的记录一定是晚于版本号小的写入系统，数据写入顺序是100%保证的，绝对不会让版本号大的记录先写入。复制过程中，数据块的转发也是严格按照顺序进行的，因此TDengine的数据复制设计是能保证Meta Data的创建顺序的。

对于特点2，只要Quorum数设置等于replica，那么一定能保证回复确认过的数据更新操作不会在服务器侧丢失。即使某节点永不起来，只要超过一半的节点还是online, 查询服务不会受到任何影响。这时，如果某个节点离线超过一定时长，系统可以自动补充新的节点，以保证在线的节点数在绝大部分时间是100%的。

对于特点3，完全可能发生，服务器确实持久化存储了某一数据更新操作，但客户端应用出了问题，认为操作不成功，它会重新发起操作。但对于Meta Data而言，没有关系，客户端可以再次发起同样的操作，不会有任何影响。

总结来看，只要quorum设置大于一，本数据复制的设计是能满足Meta Data的需求的。目前，还没有发现漏洞。
