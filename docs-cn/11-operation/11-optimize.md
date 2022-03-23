# 性能优化

因数据行 [update](https://www.taosdata.com/cn/documentation/faq#update)、表删除、数据过期等原因，TDengine 的磁盘存储文件有可能出现数据碎片，影响查询操作的性能表现。从 2.1.3.0 版本开始，新增 SQL 指令 COMPACT 来启动碎片重整过程：

```mysql
COMPACT VNODES IN (vg_id1, vg_id2, ...)
```

COMPACT 命令对指定的一个或多个 VGroup 启动碎片重整，系统会通过任务队列尽快安排重整操作的具体执行。COMPACT 指令所需的 VGroup id，可以通过 `SHOW VGROUPS;` 指令的输出结果获取；而且在 `SHOW VGROUPS;` 中会有一个 compacting 列，值为 2 时表示对应的 VGroup 处于排队等待进行重整的状态，值为 1 时表示正在进行碎片重整，为 0 时则表示并没有处于重整状态（未要求进行重整或已经完成重整）。

需要注意的是，碎片重整操作会大幅消耗磁盘 I/O。因此在重整进行期间，有可能会影响节点的写入和查询性能，甚至在极端情况下导致短时间的阻写。


## 存储参数优化

不同应用场景的数据往往具有不同的数据特征，比如保留天数、副本数、采集频次、记录大小、采集点的数量、压缩等都可完全不同。为获得在存储上的最高效率，TDengine 提供如下存储相关的系统配置参数（既可以作为 create database 指令的参数，也可以写在 taos.cfg 配置文件中用来设定创建新数据库时所采用的默认值）：

| #   | 配置参数名称 | 单位 | 含义                                                                                                                                                                                                                                                                 | **取值范围**                                                                                         | **缺省值** |
| --- | ------------ | ---- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- | ---------- |
| 1   | days         | 天   | 一个数据文件存储数据的时间跨度                                                                                                                                                                                                                                       |                                                                                                      | 10         |
| 2   | keep         | 天   | （可通过 alter database 修改）数据库中数据保留的天数。                                                                                                                                                                                                               | 3650                                                                                                 |
| 3   | cache        | MB   | 内存块的大小                                                                                                                                                                                                                                                         |                                                                                                      | 16         |
| 4   | blocks       |      | （可通过 alter database 修改）每个 VNODE（TSDB）中有多少个 cache 大小的内存块。因此一个 VNODE 使用的内存大小粗略为（cache \* blocks）。                                                                                                                              |                                                                                                      | 6          |
| 5   | quorum       |      | （可通过 alter database 修改）多副本环境下指令执行的确认数要求                                                                                                                                                                                                       | 1-2                                                                                                  | 1          |
| 6   | minRows      |      | 文件块中记录的最小条数                                                                                                                                                                                                                                               |                                                                                                      | 100        |
| 7   | maxRows      |      | 文件块中记录的最大条数                                                                                                                                                                                                                                               |                                                                                                      | 4096       |
| 8   | comp         |      | （可通过 alter database 修改）文件压缩标志位                                                                                                                                                                                                                         | 0：关闭，1:一阶段压缩，2:两阶段压缩                                                                  | 2          |
| 9   | walLevel     |      | （作为 database 的参数时名为 wal；在 taos.cfg 中作为参数时需要写作 walLevel）WAL 级别                                                                                                                                                                                | 1：写 wal，但不执行 fsync；2：写 wal, 而且执行 fsync                                                 | 1          |
| 10  | fsync        | 毫秒 | 当 wal 设置为 2 时，执行 fsync 的周期。设置为 0，表示每次写入，立即执行 fsync。                                                                                                                                                                                      |                                                                                                      | 3000       |
| 11  | replica      |      | （可通过 alter database 修改）副本个数                                                                                                                                                                                                                               | 1-3                                                                                                  | 1          |
| 12  | precision    |      | 时间戳精度标识（2.1.2.0 版本之前、2.0.20.7 版本之前在 taos.cfg 文件中不支持此参数。）（从 2.1.5.0 版本开始，新增对纳秒时间精度的支持）                                                                                                                               | ms 表示毫秒，us 表示微秒，ns 表示纳秒                                                                | ms         |
| 13  | update       |      | 是否允许数据更新（从 2.1.7.0 版本开始此参数支持 0 ～ 2 的取值范围，在此之前取值只能是 [0, 1]；而 2.0.8.0 之前的版本在 SQL 指令中不支持此参数。）                                                                                                                     | 0：不允许；1：允许更新整行；2：允许部分列更新。                                                      | 0          |
| 14  | cacheLast    |      | （可通过 alter database 修改）是否在内存中缓存子表的最近数据（从 2.1.2.0 版本开始此参数支持 0 ～ 3 的取值范围，在此之前取值只能是 [0, 1]；而 2.0.11.0 之前的版本在 SQL 指令中不支持此参数。）（2.1.2.0 版本之前、2.0.20.7 版本之前在 taos.cfg 文件中不支持此参数。） | 0：关闭；1：缓存子表最近一行数据；2：缓存子表每一列的最近的非 NULL 值；3：同时打开缓存最近行和列功能 | 0          |

对于一个应用场景，可能有多种数据特征的数据并存，最佳的设计是将具有相同数据特征的表放在一个库里，这样一个应用有多个库，而每个库可以配置不同的存储参数，从而保证系统有最优的性能。TDengine 允许应用在创建库时指定上述存储参数，如果指定，该参数就将覆盖对应的系统配置参数。举例，有下述 SQL：

```mysql
 CREATE DATABASE demo DAYS 10 CACHE 32 BLOCKS 8 REPLICA 3 UPDATE 1;
```

该 SQL 创建了一个库 demo, 每个数据文件存储 10 天数据，内存块为 32 兆字节，每个 VNODE 占用 8 个内存块，副本数为 3，允许更新，而其他参数与系统配置完全一致。

一个数据库创建成功后，仅部分参数可以修改并实时生效，其余参数不能修改：

| **参数名**  | **能否修改** | **范围**                                   | **修改语法示例**                       |
| ----------- | ------------ | ------------------------------------------ | -------------------------------------- |
| name        |              |                                            |                                        |
| create time |              |                                            |                                        |
| ntables     |              |                                            |                                        |
| vgroups     |              |                                            |                                        |
| replica     | **YES**      | 在线 dnode 数目为 1：1-1；2：1-2；>=3：1-3 | ALTER DATABASE <dbname\> REPLICA _n_   |
| quorum      | **YES**      | 1-2                                        | ALTER DATABASE <dbname\> QUORUM _n_    |
| days        |              |                                            |                                        |
| keep        | **YES**      | days-365000                                | ALTER DATABASE <dbname\> KEEP _n_      |
| cache       |              |                                            |                                        |
| blocks      | **YES**      | 3-1000                                     | ALTER DATABASE <dbname\> BLOCKS _n_    |
| minrows     |              |                                            |                                        |
| maxrows     |              |                                            |                                        |
| wal         |              |                                            |                                        |
| fsync       |              |                                            |                                        |
| comp        | **YES**      | 0-2                                        | ALTER DATABASE <dbname\> COMP _n_      |
| precision   |              |                                            |                                        |
| status      |              |                                            |                                        |
| update      |              |                                            |                                        |
| cachelast   | **YES**      | 0 \| 1 \| 2 \| 3                           | ALTER DATABASE <dbname\> CACHELAST _n_ |

**说明：**在 2.1.3.0 版本之前，通过 ALTER DATABASE 语句修改这些参数后，需要重启服务器才能生效。

TDengine 集群中加入一个新的 dnode 时，涉及集群相关的一些参数必须与已有集群的配置相同，否则不能成功加入到集群中。会进行校验的参数如下：

- numOfMnodes：系统中管理节点个数。默认值：3。（2.0 版本从 2.0.20.11 开始、2.1 及以上版本从 2.1.6.0 开始，numOfMnodes 默认值改为 1。）
- mnodeEqualVnodeNum: 一个 mnode 等同于 vnode 消耗的个数。默认值：4。
- offlineThreshold: dnode 离线阈值，超过该时间将导致该 dnode 从集群中删除。单位为秒，默认值：86400\*10（即 10 天）。
- statusInterval: dnode 向 mnode 报告状态时长。单位为秒，默认值：1。
- maxTablesPerVnode: 每个 vnode 中能够创建的最大表个数。默认值：1000000。
- maxVgroupsPerDb: 每个数据库中能够使用的最大 vgroup 个数。
- arbitrator: 系统中裁决器的 end point，缺省为空。
- timezone、locale、charset 的配置见客户端配置。（2.0.20.0 及以上的版本里，集群中加入新节点已不要求 locale 和 charset 参数取值一致）
- balance：是否启用负载均衡。0：否，1：是。默认值：1。
- flowctrl：是否启用非阻塞流控。0：否，1：是。默认值：1。
- slaveQuery：是否启用 slave vnode 参与查询。0：否，1：是。默认值：1。
- adjustMaster：是否启用 vnode master 负载均衡。0：否，1：是。默认值：1。

为方便调试，可通过 SQL 语句临时调整每个 dnode 的日志配置，系统重启后会失效：

```mysql
ALTER DNODE <dnode_id> <config>
```

- dnode_id: 可以通过 SQL 语句"SHOW DNODES"命令获取
- config: 要调整的日志参数，在如下列表中取值
  > resetlog 截断旧日志文件，创建一个新日志文件
  > debugFlag < 131 | 135 | 143 > 设置 debugFlag 为 131、135 或者 143

例如：

```
alter dnode 1 debugFlag 135;
```

