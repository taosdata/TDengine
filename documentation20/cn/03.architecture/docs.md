# 数据模型和整体架构

## <a class="anchor" id="model"></a>数据模型

### 物联网典型场景

在典型的物联网、车联网、运维监测场景中，往往有多种不同类型的数据采集设备，采集一个到多个不同的物理量。而同一种采集设备类型，往往又有多个具体的采集设备分布在不同的地点。大数据处理系统就是要将各种采集的数据汇总，然后进行计算和分析。对于同一类设备，其采集的数据都是很规则的。以智能电表为例，假设每个智能电表采集电流、电压、相位三个量，其采集的数据类似如下的表格：

<figure><table>
<thead><tr>
    <th style="text-align:center;">设备ID</th>
    <th style="text-align:center;">时间戳</th>
    <th style="text-align:center;" colspan="3">采集量</th>
    <th style="text-align:center;" colspan="2">标签</th>
    </tr>

<tr>
<th style="text-align:center;">Device ID</th>
<th style="text-align:center;">Time Stamp</th>
<th style="text-align:center;">current</th>
<th style="text-align:center;">voltage</th>
<th style="text-align:center;">phase</th>
<th style="text-align:center;">location</th>
<th style="text-align:center;">groupId</th>
</tr>
</thead>
<tbody>
<tr>
<td style="text-align:center;">d1001</td>
<td style="text-align:center;">1538548685000</td>
<td style="text-align:center;">10.3</td>
<td style="text-align:center;">219</td>
<td style="text-align:center;">0.31</td>
<td style="text-align:center;">Beijing.Chaoyang</td>
<td style="text-align:center;">2</td>
</tr>
<tr>
<td style="text-align:center;">d1002</td>
<td style="text-align:center;">1538548684000</td>
<td style="text-align:center;">10.2</td>
<td style="text-align:center;">220</td>
<td style="text-align:center;">0.23</td>
<td style="text-align:center;">Beijing.Chaoyang</td>
<td style="text-align:center;">3</td>
</tr>
<tr>
<td style="text-align:center;">d1003</td>
<td style="text-align:center;">1538548686500</td>
<td style="text-align:center;">11.5</td>
<td style="text-align:center;">221</td>
<td style="text-align:center;">0.35</td>
<td style="text-align:center;">Beijing.Haidian</td>
<td style="text-align:center;">3</td>
</tr>
<tr>
<td style="text-align:center;">d1004</td>
<td style="text-align:center;">1538548685500</td>
<td style="text-align:center;">13.4</td>
<td style="text-align:center;">223</td>
<td style="text-align:center;">0.29</td>
<td style="text-align:center;">Beijing.Haidian</td>
<td style="text-align:center;">2</td>
</tr>
<tr>
<td style="text-align:center;">d1001</td>
<td style="text-align:center;">1538548695000</td>
<td style="text-align:center;">12.6</td>
<td style="text-align:center;">218</td>
<td style="text-align:center;">0.33</td>
<td style="text-align:center;">Beijing.Chaoyang</td>
<td style="text-align:center;">2</td>
</tr>
<tr>
<td style="text-align:center;">d1004</td>
<td style="text-align:center;">1538548696600</td>
<td style="text-align:center;">11.8</td>
<td style="text-align:center;">221</td>
<td style="text-align:center;">0.28</td>
<td style="text-align:center;">Beijing.Haidian</td>
<td style="text-align:center;">2</td>
</tr>
<tr>
<td style="text-align:center;">d1002</td>
<td style="text-align:center;">1538548696650</td>
<td style="text-align:center;">10.3</td>
<td style="text-align:center;">218</td>
<td style="text-align:center;">0.25</td>
<td style="text-align:center;">Beijing.Chaoyang</td>
<td style="text-align:center;">3</td>
</tr>
<tr>
<td style="text-align:center;">d1001</td>
<td style="text-align:center;">1538548696800</td>
<td style="text-align:center;">12.3</td>
<td style="text-align:center;">221</td>
<td style="text-align:center;">0.31</td>
<td style="text-align:center;">Beijing.Chaoyang</td>
<td style="text-align:center;">2</td>
</tr>
</tbody>
</table></figure>

<center> 表1：智能电表数据示例</center>

每一条记录都有设备ID，时间戳，采集的物理量(如上图中的电流、电压、相位），还有与每个设备相关的静态标签（如上述表一中的位置Location和分组groupId）。每个设备是受外界的触发，或按照设定的周期采集数据。采集的数据点是时序的，是一个数据流。

### 数据特征

除时序特征外，仔细研究发现，物联网、车联网、运维监测类数据还具有很多其他明显的特征：

1. 数据高度结构化；
2. 数据极少有更新或删除操作；
3. 无需传统数据库的事务处理；
4. 相对互联网应用，写多读少；
5. 流量平稳，根据设备数量和采集频次，可以预测出来；
6. 用户关注的是一段时间的趋势，而不是某一特定时间点的值；
7. 数据有保留期限；
8. 数据的查询分析一定是基于时间段和空间区域；
9. 除存储、查询操作外，还需要各种统计和实时计算操作；
10. 数据量巨大，一天可能采集的数据就可以超过100亿条。

充分利用上述特征，TDengine 采取了经特殊优化的存储和计算设计来处理时序数据，它将系统处理能力显著提高，同时大幅降低了系统运维的复杂度。

### 关系型数据库模型

因为采集的数据一般是结构化数据，同时为降低学习门槛，TDengine采用传统的关系型数据库模型管理数据。因此用户需要先创建库，然后创建表，之后才能插入或查询数据。TDengine采用的是结构化存储，而不是NoSQL的key-value存储。

### 一个数据采集点一张表

为充分利用其数据的时序性和其他数据特点，TDengine要求**对每个数据采集点单独建表**（比如有一千万个智能电表，就需创建一千万张表，上述表格中的d1001, d1002, d1003, d1004都需单独建表），用来存储这个采集点所采集的时序数据。这种设计有几大优点：

1. 能保证一个采集点的数据在存储介质上是以块为单位连续存储的。如果读取一个时间段的数据，它能大幅减少随机读取操作，成数量级的提升读取和查询速度。
2. 由于不同采集设备产生数据的过程完全独立，每个设备的数据源是唯一的，一张表也就只有一个写入者，这样就可采用无锁方式来写，写入速度就能大幅提升。
3. 对于一个数据采集点而言，其产生的数据是时序的，因此写的操作可用追加的方式实现，进一步大幅提高数据写入速度。

如果采用传统的方式，将多个设备的数据写入一张表，由于网络延时不可控，不同设备的数据到达服务器的时序是无法保证的，写入操作是要有锁保护的，而且一个设备的数据是难以保证连续存储在一起的。**采用一个数据采集点一张表的方式，能最大程度的保证单个数据采集点的插入和查询的性能是最优的。**

TDengine 建议用数据采集点的名字(如上表中的D1001)来做表名。每个数据采集点可能同时采集多个物理量(如上表中的curent, voltage, phase)，每个物理量对应一张表中的一列，数据类型可以是整型、浮点型、字符串等。除此之外，表的第一列必须是时间戳，即数据类型为 timestamp。对采集的数据，TDengine将自动按照时间戳建立索引，但对采集的物理量不建任何索引。数据用列式存储方式保存。

### 超级表：同一类型数据采集点的集合

由于一个数据采集点一张表，导致表的数量巨增，难以管理，而且应用经常需要做采集点之间的聚合操作，聚合的操作也变得复杂起来。为解决这个问题，TDengine引入超级表(Super Table，简称为STable)的概念。

超级表是指某一特定类型的数据采集点的集合。同一类型的数据采集点，其表的结构是完全一样的，但每个表（数据采集点）的静态属性（标签）是不一样的。描述一个超级表（某一特定类型的数据采集点的结合），除需要定义采集量的表结构之外，还需要定义其标签的schema，标签的数据类型可以是整数、浮点数、字符串，标签可以有多个，可以事后增加、删除或修改。 如果整个系统有N个不同类型的数据采集点，就需要建立N个超级表。

在TDengine的设计里，**表用来代表一个具体的数据采集点，超级表用来代表一组相同类型的数据采集点集合**。当为某个具体数据采集点创建表时，用户使用超级表的定义做模板，同时指定该具体采集点（表）的标签值。与传统的关系型数据库相比，表（一个数据采集点）是带有静态标签的，而且这些标签可以事后增加、删除、修改。**一张超级表包含有多张表，这些表具有相同的时序数据schema，但带有不同的标签值**。

当对多个具有相同数据类型的数据采集点进行聚合操作时，TDengine会先把满足标签过滤条件的表从超级表中找出来，然后再扫描这些表的时序数据，进行聚合操作，这样需要扫描的数据集会大幅减少，从而显著提高聚合计算的性能。

## <a class="anchor" id="cluster"></a>集群与基本逻辑单元

TDengine 的设计是基于单个硬件、软件系统不可靠，基于任何单台计算机都无法提供足够计算能力和存储能力处理海量数据的假设进行设计的。因此 TDengine 从研发的第一天起，就按照分布式高可靠架构进行设计，是支持水平扩展的，这样任何单台或多台服务器发生硬件故障或软件错误都不影响系统的可用性和可靠性。同时，通过节点虚拟化并辅以自动化负载均衡技术，TDengine 能最高效率地利用异构集群中的计算和存储资源降低硬件投资。

### 主要逻辑单元

TDengine 分布式架构的逻辑结构图如下：

![TDengine架构示意图](page://images/architecture/structure.png)
<center> 图 1 TDengine架构示意图  </center>

一个完整的 TDengine 系统是运行在一到多个物理节点上的，逻辑上，它包含数据节点(dnode)、TDengine应用驱动(taosc)以及应用(app)。系统中存在一到多个数据节点，这些数据节点组成一个集群(cluster)。应用通过taosc的API与TDengine集群进行互动。下面对每个逻辑单元进行简要介绍。

**物理节点(pnode):** pnode是一独立运行、拥有自己的计算、存储和网络能力的计算机，可以是安装有OS的物理机、虚拟机或Docker容器。物理节点由其配置的 FQDN(Fully Qualified Domain Name)来标识。TDengine完全依赖FQDN来进行网络通讯，如果不了解FQDN，请看博文[《一篇文章说清楚TDengine的FQDN》](https://www.taosdata.com/blog/2020/09/11/1824.html)。

**数据节点(dnode):** dnode 是 TDengine 服务器侧执行代码 taosd 在物理节点上的一个运行实例，一个工作的系统必须有至少一个数据节点。dnode包含零到多个逻辑的虚拟节点(VNODE)，零或者至多一个逻辑的管理节点(mnode)。dnode在系统中的唯一标识由实例的End Point (EP )决定。EP是dnode所在物理节点的FQDN (Fully Qualified Domain Name)和系统所配置的网络端口号(Port)的组合。通过配置不同的端口，一个物理节点(一台物理机、虚拟机或容器）可以运行多个实例，或有多个数据节点。

**虚拟节点(vnode)**: 为更好的支持数据分片、负载均衡，防止数据过热或倾斜，数据节点被虚拟化成多个虚拟节点(vnode，图中V2, V3, V4等)。每个 vnode 都是一个相对独立的工作单元，是时序数据存储的基本单元，具有独立的运行线程、内存空间与持久化存储的路径。一个 vnode 包含一定数量的表（数据采集点）。当创建一张新表时，系统会检查是否需要创建新的 vnode。一个数据节点上能创建的 vnode 的数量取决于该数据节点所在物理节点的硬件资源。一个 vnode 只属于一个DB，但一个DB可以有多个 vnode。一个 vnode 除存储的时序数据外，也保存有所包含的表的schema、标签值等。一个虚拟节点由所属的数据节点的EP，以及所属的VGroup ID在系统内唯一标识，由管理节点创建并管理。

**管理节点(mnode):** 一个虚拟的逻辑单元，负责所有数据节点运行状态的监控和维护，以及节点之间的负载均衡(图中M)。同时，管理节点也负责元数据(包括用户、数据库、表、静态标签等)的存储和管理，因此也称为 Meta Node。TDengine 集群中可配置多个(开源版最多不超过3个) mnode，它们自动构建成为一个虚拟管理节点组(图中M0, M1, M2)。mnode 间采用 master/slave 的机制进行管理，而且采取强一致方式进行数据同步, 任何数据更新操作只能在 Master 上进行。mnode 集群的创建由系统自动完成，无需人工干预。每个dnode上至多有一个mnode，由所属的数据节点的EP来唯一标识。每个dnode通过内部消息交互自动获取整个集群中所有 mnode 所在的 dnode 的EP。

**虚拟节点组(VGroup):** 不同数据节点上的 vnode 可以组成一个虚拟节点组(vnode group)来保证系统的高可靠。虚拟节点组内采取master/slave的方式进行管理。写操作只能在 master vnode 上进行，系统采用异步复制的方式将数据同步到 slave vnode，这样确保了一份数据在多个物理节点上有拷贝。一个 vgroup 里虚拟节点个数就是数据的副本数。如果一个DB的副本数为N，系统必须有至少N个数据节点。副本数在创建DB时通过参数 replica 可以指定，缺省为1。使用 TDengine 的多副本特性，可以不再需要昂贵的磁盘阵列等存储设备，就可以获得同样的数据高可靠性。虚拟节点组由管理节点创建、管理，并且由管理节点分配一个系统唯一的ID，VGroup ID。如果两个虚拟节点的vnode group ID相同，说明他们属于同一个组，数据互为备份。虚拟节点组里虚拟节点的个数是可以动态改变的，容许只有一个，也就是没有数据复制。VGroup ID是永远不变的，即使一个虚拟节点组被删除，它的ID也不会被收回重复利用。

**TAOSC:** taosc是TDengine给应用提供的驱动程序(driver)，负责处理应用与集群的接口交互，提供C/C++语言原生接口，内嵌于JDBC、C#、Python、Go、Node.js语言连接库里。应用都是通过taosc而不是直接连接集群中的数据节点与整个集群进行交互的。这个模块负责获取并缓存元数据；将插入、查询等请求转发到正确的数据节点；在把结果返回给应用时，还需要负责最后一级的聚合、排序、过滤等操作。对于JDBC, C/C++/C#/Python/Go/Node.js接口而言，这个模块是在应用所处的物理节点上运行。同时，为支持全分布式的RESTful接口，taosc在TDengine集群的每个dnode上都有一运行实例。

### 节点之间的通讯

**通讯方式：**TDengine系统的各个数据节点之间，以及应用驱动与各数据节点之间的通讯是通过TCP/UDP进行的。因为考虑到物联网场景，数据写入的包一般不大，因此TDengine 除采用TCP做传输之外，还采用UDP方式，因为UDP 更加高效，而且不受连接数的限制。TDengine实现了自己的超时、重传、确认等机制，以确保UDP的可靠传输。对于数据量不到15K的数据包，采取UDP的方式进行传输，超过15K的，或者是查询类的操作，自动采取TCP的方式进行传输。同时，TDengine根据配置和数据包，会自动对数据进行压缩/解压缩，数字签名/认证等处理。对于数据节点之间的数据复制，只采用TCP方式进行数据传输。

**FQDN配置**：一个数据节点有一个或多个FQDN，可以在系统配置文件taos.cfg通过参数“fqdn"进行指定，如果没有指定，系统将自动获取计算机的hostname作为其FQDN。如果节点没有配置FQDN，可以直接将该节点的配置参数fqdn设置为它的IP地址。但不建议使用IP，因为IP地址可变，一旦变化，将让集群无法正常工作。一个数据节点的EP(End Point)由FQDN + Port组成。采用FQDN，需要保证DNS服务正常工作，或者在节点以及应用所在的节点配置好hosts文件。

**端口配置：**一个数据节点对外的端口由TDengine的系统配置参数serverPort决定，对集群内部通讯的端口是serverPort+5。集群内数据节点之间的数据复制操作还占有一个TCP端口，是serverPort+10. 为支持多线程高效的处理UDP数据，每个对内和对外的UDP连接，都需要占用5个连续的端口。因此一个数据节点总的端口范围为serverPort到serverPort + 10，总共11个TCP/UDP端口。（另外还可能有 RESTful、Arbitrator 所使用的端口，那样的话就一共是 13 个。）使用时，需要确保防火墙将这些端口打开，以备使用。每个数据节点可以配置不同的serverPort。（详细的端口情况请参见 [TDengine 2.0 端口说明](https://www.taosdata.com/cn/documentation/faq#port)）

**集群对外连接:** TDengine集群可以容纳单个、多个甚至几千个数据节点。应用只需要向集群中任何一个数据节点发起连接即可，连接需要提供的网络参数是一数据节点的End Point(FQDN加配置的端口号）。通过命令行CLI启动应用taos时，可以通过选项-h来指定数据节点的FQDN, -P来指定其配置的端口号，如果端口不配置，将采用TDengine的系统配置参数serverPort。

**集群内部通讯**: 各个数据节点之间通过TCP/UDP进行连接。一个数据节点启动时，将获取mnode所在的dnode的EP信息，然后与系统中的mnode建立起连接，交换信息。获取mnode的EP信息有三步，1：检查mnodeEpSet文件是否存在，如果不存在或不能正常打开获得mnode EP信息，进入第二步；2：检查系统配置文件taos.cfg, 获取节点配置参数firstEp, secondEp，（这两个参数指定的节点可以是不带mnode的普通节点，这样的话，节点被连接时会尝试重定向到mnode节点）如果不存在或者taos.cfg里没有这两个配置参数，或无效，进入第三步；3：将自己的EP设为mnode EP, 并独立运行起来。获取mnode EP列表后，数据节点发起连接，如果连接成功，则成功加入进工作的集群，如果不成功，则尝试mnode EP列表中的下一个。如果都尝试了，但连接都仍然失败，则休眠几秒后，再进行尝试。

**MNODE的选择:** TDengine逻辑上有管理节点，但没有单独的执行代码，服务器侧只有一套执行代码taosd。那么哪个数据节点会是管理节点呢？这是系统自动决定的，无需任何人工干预。原则如下：一个数据节点启动时，会检查自己的End Point, 并与获取的mnode EP List进行比对，如果在其中，该数据节点认为自己应该启动mnode模块，成为mnode。如果自己的EP不在mnode EP List里，则不启动mnode模块。在系统的运行过程中，由于负载均衡、宕机等原因，mnode有可能迁移至新的dnode，但一切都是透明的，无需人工干预，配置参数的修改，是mnode自己根据资源做出的决定。

**新数据节点的加入**：系统有了一个数据节点后，就已经成为一个工作的系统。添加新的节点进集群时，有两个步骤，第一步：使用TDengine CLI连接到现有工作的数据节点，然后用命令”create dnode"将新的数据节点的End Point添加进去; 第二步：在新的数据节点的系统配置参数文件taos.cfg里，将firstEp, secondEp参数设置为现有集群中任意两个数据节点的EP即可。具体添加的详细步骤请见详细的用户手册。这样就把集群一步一步的建立起来。

**重定向**：无论是dnode还是taosc，最先都是要发起与mnode的连接，但mnode是系统自动创建并维护的，因此对于用户来说，并不知道哪个dnode在运行mnode。TDengine只要求向系统中任何一个工作的dnode发起连接即可。因为任何一个正在运行的dnode，都维护有目前运行的mnode EP List。当收到一个来自新启动的dnode或taosc的连接请求，如果自己不是mnode，则将mnode EP List回复给对方，taosc或新启动的dnode收到这个list, 就重新尝试建立连接。当mnode EP List发生改变，通过节点之间的消息交互，各个数据节点就很快获取最新列表，并通知taosc。

### 一个典型的消息流程

为解释vnode, mnode, taosc和应用之间的关系以及各自扮演的角色，下面对写入数据这个典型操作的流程进行剖析。

![TDengine典型的操作流程](page://images/architecture/message.png)
<center> 图 2 TDengine典型的操作流程 </center>

1. 应用通过JDBC、ODBC或其他API接口发起插入数据的请求。
2. taosc会检查缓存，看是否保存有该表的meta data。如果有，直接到第4步。如果没有，taosc将向mnode发出get meta-data请求。
3. mnode将该表的meta-data返回给taosc。Meta-data包含有该表的schema, 而且还有该表所属的vgroup信息（vnode ID以及所在的dnode的End Point，如果副本数为N，就有N组End Point)。如果taosc迟迟得不到mnode回应，而且存在多个mnode, taosc将向下一个mnode发出请求。
4. taosc向master vnode发起插入请求。
5. vnode插入数据后，给taosc一个应答，表示插入成功。如果taosc迟迟得不到vnode的回应，taosc会认为该节点已经离线。这种情况下，如果被插入的数据库有多个副本，taosc将向vgroup里下一个vnode发出插入请求。
6. taosc通知APP，写入成功。

对于第二和第三步，taosc启动时，并不知道mnode的End Point，因此会直接向配置的集群对外服务的End Point发起请求。如果接收到该请求的dnode并没有配置mnode，该dnode会在回复的消息中告知mnode EP列表，这样taosc会重新向新的mnode的EP发出获取meta-data的请求。

对于第四和第五步，没有缓存的情况下，taosc无法知道虚拟节点组里谁是master，就假设第一个vnodeID就是master,向它发出请求。如果接收到请求的vnode并不是master,它会在回复中告知谁是master，这样taosc就向建议的master vnode发出请求。一旦得到插入成功的回复，taosc会缓存master节点的信息。

上述是插入数据的流程，查询、计算的流程也完全一致。taosc把这些复杂的流程全部封装屏蔽了，对于应用来说无感知也无需任何特别处理。

通过taosc缓存机制，只有在第一次对一张表操作时，才需要访问mnode,因此mnode不会成为系统瓶颈。但因为schema有可能变化，而且vgroup有可能发生改变（比如负载均衡发生），因此taosc会定时和mnode交互，自动更新缓存。

## <a class="anchor" id="sharding"></a>存储模型与数据分区、分片

### 存储模型

TDengine存储的数据包括采集的时序数据以及库、表相关的元数据、标签数据等，这些数据具体分为三部分：

- 时序数据：存放于vnode里，由data、head和last三个文件组成，数据量大，查询量取决于应用场景。容许乱序写入，但暂时不支持删除操作，并且仅在update参数设置为1时允许更新操作。通过采用一个采集点一张表的模型，一个时间段的数据是连续存储，对单张表的写入是简单的追加操作，一次读，可以读到多条记录，这样保证对单个采集点的插入和查询操作，性能达到最优。
- 标签数据：存放于vnode里的meta文件，支持增删改查四个标准操作。数据量不大，有N张表，就有N条记录，因此可以全内存存储。如果标签过滤操作很多，查询将十分频繁，因此TDengine支持多核多线程并发查询。只要计算资源足够，即使有数千万张表，过滤结果能毫秒级返回。
- 元数据：存放于mnode里，包含系统节点、用户、DB、Table Schema等信息，支持增删改查四个标准操作。这部分数据的量不大，可以全内存保存，而且由于客户端有缓存，查询量也不大。因此目前的设计虽是集中式存储管理，但不会构成性能瓶颈。

与典型的NoSQL存储模型相比，TDengine将标签数据与时序数据完全分离存储，它具有两大优势：

- 能够极大地降低标签数据存储的冗余度：一般的NoSQL数据库或时序数据库，采用的K-V存储，其中的Key包含时间戳、设备ID、各种标签。每条记录都带有这些重复的内容，浪费存储空间。而且如果应用要在历史数据上增加、修改或删除标签，需要遍历数据，重写一遍，操作成本极其昂贵。
- 能够实现极为高效的多表之间的聚合查询：做多表之间聚合查询时，先把符合标签过滤条件的表查找出来，然后再查找这些表相应的数据块，这样大幅减少要扫描的数据集，从而大幅提高查询效率。而且标签数据采用全内存的结构进行管理和维护，千万级别规模的标签数据查询可以在毫秒级别返回。

### 数据分片

对于海量的数据管理，为实现水平扩展，一般都需要采取分片(Sharding)分区(Partitioning)策略。TDengine是通过vnode来实现数据分片的，通过一个时间段一个数据文件来实现时序数据分区的。

vnode(虚拟数据节点)负责为采集的时序数据提供写入、查询和计算功能。为便于负载均衡、数据恢复、支持异构环境，TDengine将一个数据节点根据其计算和存储资源切分为多个vnode。这些vnode的管理是TDengine自动完成的，对应用完全透明。

对于单独一个数据采集点，无论其数据量多大，一个vnode（或vnode group, 如果副本数大于1）有足够的计算资源和存储资源来处理（如果每秒生成一条16字节的记录，一年产生的原始数据不到0.5G），因此TDengine将一张表（一个数据采集点）的所有数据都存放在一个vnode里，而不会让同一个采集点的数据分布到两个或多个dnode上。而且一个vnode可存储多个数据采集点(表）的数据，一个vnode可容纳的表的数目的上限为一百万。设计上，一个vnode里所有的表都属于同一个DB。一个数据节点上，除非特殊配置，一个DB拥有的vnode数目不会超过系统核的数目。

创建DB时，系统并不会马上分配资源。但当创建一张表时，系统将看是否有已经分配的vnode, 且该vnode是否有空余的表空间，如果有，立即在该有空位的vnode创建表。如果没有，系统将从集群中，根据当前的负载情况，在一个dnode上创建一新的vnode, 然后创建表。如果DB有多个副本，系统不是只创建一个vnode，而是一个vgroup(虚拟数据节点组)。系统对vnode的数目没有任何限制，仅仅受限于物理节点本身的计算和存储资源。

每张表的meda data（包含schema, 标签等）也存放于vnode里，而不是集中存放于mnode，实际上这是对Meta数据的分片，这样便于高效并行的进行标签过滤操作。

### 数据分区

TDengine除vnode分片之外，还对时序数据按照时间段进行分区。每个数据文件只包含一个时间段的时序数据，时间段的长度由DB的配置参数days决定。这种按时间段分区的方法还便于高效实现数据的保留策略，只要数据文件超过规定的天数（系统配置参数keep)，将被自动删除。而且不同的时间段可以存放于不同的路径和存储介质，以便于大数据的冷热管理，实现多级存储。

总的来说，**TDengine是通过vnode以及时间两个维度，对大数据进行切分**，便于并行高效的管理，实现水平扩展。

### 负载均衡

每个dnode都定时向 mnode(虚拟管理节点)报告其状态（包括硬盘空间、内存大小、CPU、网络、虚拟节点个数等），因此mnode了解整个集群的状态。基于整体状态，当mnode发现某个dnode负载过重，它会将dnode上的一个或多个vnode挪到其他dnode。在挪动过程中，对外服务继续进行，数据插入、查询和计算操作都不受影响。

如果mnode一段时间没有收到dnode的状态报告，mnode会认为这个dnode已经离线。如果离线时间超过一定时长（时长由配置参数offlineThreshold决定），该dnode将被mnode强制剔除出集群。该dnode上的vnodes如果副本数大于一，系统将自动在其他dnode上创建新的副本，以保证数据的副本数。如果该dnode上还有mnode, 而且mnode的副本数大于一，系统也将自动在其他dnode上创建新的mnode, 以保证mnode的副本数。

当新的数据节点被添加进集群，因为新的计算和存储被添加进来，系统也将自动启动负载均衡流程。

负载均衡过程无需任何人工干预，应用也无需重启，将自动连接新的节点，完全透明。
**提示：负载均衡由参数balance控制，决定开启/关闭自动负载均衡。**

## <a class="anchor" id="replication"></a>数据写入与复制流程

如果一个数据库有N个副本，那一个虚拟节点组就有N个虚拟节点，但是只有一个是Master，其他都是slave。当应用将新的记录写入系统时，只有Master vnode能接受写的请求。如果slave vnode收到写的请求，系统将通知taosc需要重新定向。

### Master vnode写入流程

Master Vnode遵循下面的写入流程：

![TDengine Master写入流程](page://images/architecture/write_master.png)
<center> 图 3 TDengine Master写入流程  </center>

1. Master vnode收到应用的数据插入请求，验证OK，进入下一步；
2. 如果系统配置参数walLevel大于0，vnode将把该请求的原始数据包写入数据库日志文件WAL。如果walLevel设置为2，而且fsync设置为0，TDengine还将WAL数据立即落盘，以保证即使宕机，也能从数据库日志文件中恢复数据，避免数据的丢失；
3. 如果有多个副本，vnode将把数据包转发给同一虚拟节点组内slave vnodes, 该转发包带有数据的版本号(version)；
4. 写入内存，并将记录加入到skip list；
5. Master vnode返回确认信息给应用，表示写入成功。
6. 如果第2，3，4步中任何一步失败，将直接返回错误给应用。

### Slave vnode写入流程

对于slave vnode, 写入流程是：

![TDengine Slave写入流程](page://images/architecture/write_slave.png)
<center> 图 4 TDengine Slave写入流程  </center>

1. Slave vnode收到Master vnode转发了的数据插入请求。
2. 如果系统配置参数walLevel大于0，vnode将把该请求的原始数据包写入数据库日志文件WAL。如果walLevel设置为2，而且fsync设置为0，TDengine还将WAL数据立即落盘，以保证即使宕机，也能从数据库日志文件中恢复数据，避免数据的丢失；
3. 写入内存，更新内存中的skip list。

与Master vnode相比，slave vnode不存在转发环节，也不存在回复确认环节，少了两步。但写内存与WAL是完全一样的。

### 异地容灾、IDC迁移

从上述Master和Slave流程可以看出，TDengine采用的是异步复制的方式进行数据同步。这种方式能够大幅提高写入性能，网络延时对写入速度不会有大的影响。通过配置每个物理节点的IDC和机架号，可以保证对于一个虚拟节点组，虚拟节点由来自不同IDC、不同机架的物理节点组成，从而实现异地容灾。因此TDengine原生支持异地容灾，无需再使用其他工具。

另外一方面，TDengine支持动态修改副本数，一旦副本数增加，新加入的虚拟节点将立即进入数据同步流程，同步结束后，新加入的虚拟节点即可提供服务。而在同步过程中，master以及其他已经同步的虚拟节点都可以对外提供服务。利用这一特性，TDengine可以实现无服务中断的IDC机房迁移。只需要将新IDC的物理节点加入现有集群，等数据同步完成后，再将老的IDC的物理节点从集群中剔除即可。

但是，这种异步复制的方式，存在极小的时间窗口，丢失写入的数据。具体场景如下：

1. Master vnode完成了它的5步操作，已经给APP确认写入成功，然后宕机；
2. Slave vnode收到写入请求后，在第2步写入日志之前，处理失败
3. Slave vnode将成为新的master, 从而丢失了一条记录

理论上，只要是异步复制，就无法保证100%不丢失。但是这个窗口极小，mater与slave要同时发生故障，而且发生在刚给应用确认写入成功之后。

注：异地容灾、IDC无中断迁移，仅仅企业版支持。
**提示：该功能暂未提供**

### 主从选择

Vnode会保持一个数据版本号(Version)，对内存数据进行持久化存储时，对该版本号也进行持久化存储。每个数据更新操作，无论是采集的时序数据还是元数据，这个版本号将增一。

一个vnode启动时，角色(master、slave) 是不定的，数据是处于未同步状态，它需要与虚拟节点组内其他节点建立TCP连接，并互相交换status，其中包括version和自己的角色。通过status的交换，系统进入选主流程，规则如下：

1. 如果只有一个副本，该副本永远就是master
2. 所有副本都在线时，版本最高的被选为master
3. 在线的虚拟节点数过半，而且有虚拟节点是slave的话，该虚拟节点自动成为master
4. 对于2和3，如果多个虚拟节点满足成为master的要求，那么虚拟节点组的节点列表里，最前面的选为master

更多的关于数据复制的流程，请见[TDengine 2.0数据复制模块设计](https://www.taosdata.com/cn/documentation/architecture/replica/)。

### 同步复制

对于数据一致性要求更高的场景，异步数据复制无法满足要求，因为有极小的概率丢失数据，因此TDengine提供同步复制的机制供用户选择。在创建数据库时，除指定副本数replica之外，用户还需要指定新的参数quorum。如果quorum大于一，它表示每次Master转发给副本时，需要等待quorum-1个回复确认，才能通知应用，数据在slave已经写入成功。如果在一定的时间内，得不到quorum-1个回复确认，master vnode将返回错误给应用。

采用同步复制，系统的性能会有所下降，而且latency会增加。因为元数据要强一致，mnode之间的数据同步缺省就是采用的同步复制。

注：vnode之间的同步复制仅仅企业版支持

## <a class="anchor" id="persistence"></a>缓存与持久化

### 缓存

TDengine采用时间驱动缓存管理策略（First-In-First-Out，FIFO），又称为写驱动的缓存管理机制。这种策略有别于读驱动的数据缓存模式（Least-Recent-Used，LRU），直接将最近写入的数据保存在系统的缓存中。当缓存达到临界值的时候，将最早的数据批量写入磁盘。一般意义上来说，对于物联网数据的使用，用户最为关心的是刚产生的数据，即当前状态。TDengine充分利用这一特性，将最近到达的（当前状态）数据保存在缓存中。

TDengine通过查询函数向用户提供毫秒级的数据获取能力。直接将最近到达的数据保存在缓存中，可以更加快速地响应用户针对最近一条或一批数据的查询分析，整体上提供更快的数据库查询响应能力。从这个意义上来说，**可通过设置合适的配置参数将TDengine作为数据缓存来使用，而不需要再部署Redis或其他额外的缓存系统**，可有效地简化系统架构，降低运维的成本。需要注意的是，TDengine重启以后系统的缓存将被清空，之前缓存的数据均会被批量写入磁盘，缓存的数据将不会像专门的Key-value缓存系统再将之前缓存的数据重新加载到缓存中。

每个vnode有自己独立的内存，而且由多个固定大小的内存块组成，不同vnode之间完全隔离。数据写入时，类似于日志的写法，数据被顺序追加写入内存，但每个vnode维护有自己的skip list，便于迅速查找。当三分之一以上的内存块写满时，启动落盘操作，而且后续写的操作在新的内存块进行。这样，一个vnode里有三分之一内存块是保留有最近的数据的，以达到缓存、快速查找的目的。一个vnode的内存块的个数由配置参数blocks决定，内存块的大小由配置参数cache决定。

### 持久化存储

TDengine采用数据驱动的方式让缓存中的数据写入硬盘进行持久化存储。当vnode中缓存的数据达到一定规模时，为了不阻塞后续数据的写入，TDengine也会拉起落盘线程将缓存的数据写入持久化存储。TDengine在数据落盘时会打开新的数据库日志文件，在落盘成功后则会删除老的数据库日志文件，避免日志文件无限制的增长。

为充分利用时序数据特点，TDengine将一个vnode保存在持久化存储的数据切分成多个文件，每个文件只保存固定天数的数据，这个天数由系统配置参数days决定。切分成多个文件后，给定查询的起止日期，无需任何索引，就可以立即定位需要打开哪些数据文件，大大加快读取速度。

对于采集的数据，一般有保留时长，这个时长由系统配置参数keep决定。超过这个设置天数的数据文件，将被系统自动删除，释放存储空间。

给定days与keep两个参数，一个典型工作状态的vnode中总的数据文件数为：`向上取整(keep/days)+1`个。总的数据文件个数不宜过大，也不宜过小。10到100以内合适。基于这个原则，可以设置合理的days。 目前的版本，参数keep可以修改，但对于参数days，一但设置后，不可修改。

在每个数据文件里，一张表的数据是一块一块存储的。一张表可以有一到多个数据文件块。在一个文件块里，数据是列式存储的，占用的是一片连续的存储空间，这样大大提高读取速度。文件块的大小由系统参数maxRows（每块最大记录条数）决定，缺省值为4096。这个值不宜过大，也不宜过小。过大，定位具体时间段的数据的搜索时间会变长，影响读取速度；过小，数据块的索引太大，压缩效率偏低，也影响读取速度。

每个数据文件(.data结尾)都有一个对应的索引文件（.head结尾），该索引文件对每张表都有一数据块的摘要信息，记录了每个数据块在数据文件中的偏移量，数据的起止时间等信息，以帮助系统迅速定位需要查找的数据。每个数据文件还有一对应的last文件(.last结尾)，该文件是为防止落盘时数据块碎片化而设计的。如果一张表落盘的记录条数没有达到系统配置参数minRows（每块最小记录条数），将被先存储到last文件，等下次落盘时，新落盘的记录将与last文件的记录进行合并，再写入数据文件。

数据写入磁盘时，根据系统配置参数comp决定是否压缩数据。TDengine提供了三种压缩选项：无压缩、一阶段压缩和两阶段压缩，分别对应comp值为0、1和2的情况。一阶段压缩根据数据的类型进行了相应的压缩，压缩算法包括delta-delta编码、simple 8B方法、zig-zag编码、LZ4等算法。二阶段压缩在一阶段压缩的基础上又用通用压缩算法进行了压缩，压缩率更高。

### 多级存储

在默认配置下，TDengine会将所有数据保存在/var/lib/taos目录下，而且每个vnode的数据文件保存在该目录下的不同目录。为扩大存储空间，尽量减少文件读取的瓶颈，提高数据吞吐率 TDengine可通过配置系统参数dataDir让多个挂载的硬盘被系统同时使用。除此之外，TDengine也提供了数据分级存储的功能，即根据数据文件的新老程度存储在不同的存储介质上。比如最新的数据存储在SSD上，超过一周的数据存储在本地硬盘上，超过4周的数据存储在网络存储设备上，这样来降低存储成本，而又保证高效的访问数据。数据在不同存储介质上的移动是由系统自动完成的，对应用是完全透明的。数据的分级存储也是通过系统参数dataDir来配置。

dataDir的配置格式如下：
```
dataDir data_path [tier_level]
```
其中data_path为挂载点的文件夹路径，tier_level为介质存储等级。介质存储等级越高，盛放数据文件越老。同一存储等级可挂载多个硬盘，同一存储等级上的数据文件分布在该存储等级的所有硬盘上。TDengine最多支持3级存储，所以tier_level的取值为0、1和2。在配置dataDir时，必须存在且只有一个挂载路径不指定tier_level，称之为特殊挂载盘（路径）。该挂载路径默认为0级存储介质，且包含特殊文件链接，不可被移除，否则会对写入的数据产生毁灭性影响。

假设一物理节点有六个可挂载的硬盘/mnt/disk1、/mnt/disk2、…、/mnt/disk6，其中disk1和disk2需要被指定为0级存储介质，disk3和disk4为1级存储介质， disk5和disk6为2级存储介质。disk1为特殊挂载盘，则可在/etc/taos/taos.cfg中做如下配置：

```
dataDir /mnt/disk1/taos
dataDir /mnt/disk2/taos 0
dataDir /mnt/disk3/taos 1
dataDir /mnt/disk4/taos 1
dataDir /mnt/disk5/taos 2
dataDir /mnt/disk6/taos 2
```

挂载的盘也可以是非本地的网络盘，只要系统能访问即可。

注：多级存储功能仅企业版支持

## <a class="anchor" id="query"></a>数据查询

TDengine提供了多种多样针对表和超级表的查询处理功能，除了常规的聚合查询之外，还提供针对时序数据的窗口查询、统计聚合等功能。TDengine的查询处理需要客户端、vnode, mnode节点协同完成。

### 单表查询

SQL语句的解析和校验工作在客户端完成。解析SQL语句并生成抽象语法树(Abstract Syntax Tree, AST)，然后对其进行校验和检查。以及向管理节点(mnode)请求查询中指定表的元数据信息(table metadata)。

根据元数据信息中的End Point信息，将查询请求序列化后发送到该表所在的数据节点（dnode）。dnode接收到查询请求后，识别出该查询请求指向的虚拟节点（vnode），将消息转发到vnode的查询执行队列。vnode的查询执行线程建立基础的查询执行环境，并立即返回该查询请求，同时开始执行该查询。

客户端在获取查询结果的时候，dnode的查询执行队列中的工作线程会等待vnode执行线程执行完成，才能将查询结果返回到请求的客户端。

### 按时间轴聚合、降采样、插值

时序数据有别于普通数据的显著特征是每条记录均具有时间戳，因此针对具有时间戳数据在时间轴上进行聚合是不同于普通数据库的重要功能。从这点上来看，与流计算引擎的窗口查询有相似的地方。

在TDengine中引入关键词interval来进行时间轴上固定长度时间窗口的切分，并按照时间窗口对数据进行聚合，对窗口范围内的数据按需进行聚合。例如：
```mysql
select count(*) from d1001 interval(1h);
```

针对d1001设备采集的数据，按照1小时的时间窗口返回每小时存储的记录数量。

在需要连续获得查询结果的应用场景下，如果给定的时间区间存在数据缺失，会导致该区间数据结果也丢失。TDengine提供策略针对时间轴聚合计算的结果进行插值，通过使用关键词Fill就能够对时间轴聚合结果进行插值。例如：
```mysql
select count(*) from d1001 interval(1h) fill(prev);
```

针对d1001设备采集数据统计每小时记录数，如果某一个小时不存在数据，则返回之前一个小时的统计数据。TDengine提供前向插值(prev)、线性插值(linear)、NULL值填充(NULL)、特定值填充(value)。

### 多表聚合查询

TDengine对每个数据采集点单独建表，但在实际应用中经常需要对不同的采集点数据进行聚合。为高效的进行聚合操作，TDengine引入超级表（STable）的概念。超级表用来代表一特定类型的数据采集点，它是包含多张表的表集合，集合里每张表的模式（schema）完全一致，但每张表都带有自己的静态标签，标签可以多个，可以随时增加、删除和修改。 应用可通过指定标签的过滤条件，对一个STable下的全部或部分表进行聚合或统计操作，这样大大简化应用的开发。其具体流程如下图所示：

![多表聚合查询原理图](page://images/architecture/multi_tables.png)
<center> 图 5 多表聚合查询原理图  </center>

1. 应用将一个查询条件发往系统；
2. taosc将超级表的名字发往 Meta Node（管理节点)；
3. 管理节点将超级表所拥有的 vnode 列表发回 taosc；
4. taosc将计算的请求连同标签过滤条件发往这些vnode对应的多个数据节点；
5. 每个vnode先在内存里查找出自己节点里符合标签过滤条件的表的集合，然后扫描存储的时序数据，完成相应的聚合计算，将结果返回给taosc；
6. taosc将多个数据节点返回的结果做最后的聚合，将其返回给应用。 

由于TDengine在vnode内将标签数据与时序数据分离存储，通过在内存里过滤标签数据，先找到需要参与聚合操作的表的集合，将需要扫描的数据集大幅减少，大幅提升聚合计算速度。同时，由于数据分布在多个vnode/dnode，聚合计算操作在多个vnode里并发进行，又进一步提升了聚合的速度。 对普通表的聚合函数以及绝大部分操作都适用于超级表，语法完全一样，细节请看 TAOS SQL。

### 预计算

为有效提升查询处理的性能，针对物联网数据的不可更改的特点，在数据块头部记录该数据块中存储数据的统计信息：包括最大值、最小值、和。我们称之为预计算单元。如果查询处理涉及整个数据块的全部数据，直接使用预计算结果，完全不需要读取数据块的内容。由于预计算数据量远小于磁盘上存储的数据块数据的大小，对于磁盘IO为瓶颈的查询处理，使用预计算结果可以极大地减小读取IO压力，加速查询处理的流程。预计算机制与Postgre SQL的索引BRIN（block range index）有异曲同工之妙。
