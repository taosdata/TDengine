# OpenTSDB 应用迁移到 TDengine 的最佳实践

作为一个分布式、可伸缩、基于HBase 的分布式时序数据库系统，得益于其先发优势，OpenTSDB被 DevOps 领域的人员引入并广泛地应用在了运维监控领域。但最近几年，随着云计算、微服务、容器化等新技术快速落地发展，企业级服务种类变得越来越多，架构也越来越复杂，应用运行基础环境日益多样化，给系统和运行监控带来的压力也越来越大。从这一现状出发，使用 OpenTSDB 作为DevOps的监控后端存储，越来越受困于其性能问题以及迟缓的功能升级，以及由此而衍生出来的应用部署成本上升和运行效率降低等问题，这些问题随着系统规模的扩大日益严重。

在这一背景下，为满足高速增长的物联网大数据市场和技术需求，在吸取众多传统关系型数据库、NoSQL 数据库、流计算引擎、消息队列等软件的优点之后，涛思数据自主开发出创新型大数据处理产品TDengine。在时序大数据处理上，TDengine 有着自己独特的优势。就 OpenTSDB 当前遇到的问题来说，TDengine 能够有效解决。

相对于 OpenTSDB，TDengine 具有如下显著特点：

- 数据写入和查询的性能远超 OpenTSDB；
- 针对时序数据的高效压缩机制，压缩后在磁盘上的存储空间不到 1/5；
- 安装部署非常简单，单一安装包完成安装部署，不依赖其他的第三方软件，整个安装部署过程秒级搞定;
- 提供的内建函数覆盖 OpenTSDB 支持的全部查询函数，还支持更多的时序数据查询函数、标量函数及聚合函数，支持多种时间窗口聚合、连接查询、表达式运算、多种分组聚合、用户定义排序、以及用户定义函数等高级查询功能。采用类 SQL 的语法规则，更加简单易学，基本上没有学习成本。
- 支持多达 128 个标签，标签总长度可达到 16 KB；
- 除 HTTP 之外，还提供 Java、Python、C、Rust、Go 等多种语言的接口，支持 JDBC 等多种企业级标准连接器协议。

如果我们将原本运行在 OpenTSDB 上的应用迁移到 TDengine 上，不仅可以有效地降低计算和存储资源的占用、减少部署服务器的规模，还能够极大减少运行维护的成本的输出，让运维管理工作更简单、更轻松，大幅降低总拥有成本。与OpenTSDB一样，TDengine也已经进行了开源，不同的是，除了单机版，后者还实现了集群版开源，被厂商绑定的顾虑一扫而空。

在下文中我们将就“使用最典型并广泛应用的运维监控（DevOps）场景”来说明，如何在不编码的情况下将 OpenTSDB 的应用快速、安全、可靠地迁移到 TDengine之上。后续的章节会做更深度的介绍，以便于进行非DevOps场景的迁移。

## DevOps应用快速迁移

### 1、典型应用场景

一个典型的 DevOps 应用场景的系统整体的架构如下图（图1） 所示。

![IT-DevOps-Solutions-Immigrate-OpenTSDB-Arch](../../images/IT-DevOps-Solutions-Immigrate-OpenTSDB-Arch.jpg)
<div align = center>图1. DevOps场景中典型架构</div>

在该应用场景中，包含了部署在应用环境中负责收集机器度量（Metrics）、网络度量（Metrics）以及应用度量（Metrics）的 Agent 工具、汇聚agent收集信息的数据收集器，数据持久化存储和管理的系统以及监控数据可视化工具（例如：Grafana等）。

其中，部署在应用节点的 Agents 负责向 collectd/Statsd 提供不同来源的运行指标，collectd/StatsD则负责将汇聚的数据推送到 OpenTSDB 集群系统，然后使用可视化看板 Grafana 将数据可视化呈现出来。

### 2、迁移服务

- **TDengine 安装部署**

首先是TDengine的安装，从官网上下载TDengine最新稳定版，解压缩后运行install.sh进行安装。各种安装包的使用帮助请参见博客[《 TDengine多种安装包的安装和卸载》](https://www.taosdata.com/blog/2019/08/09/566.html)。

注意，安装完成以后，不要立即启动 taosd 服务，在正确配置完成参数以后再启动。

- **调整数据收集器配置**

在TDengine 2.3版本中，我们发布了taosAdapter ，taosAdapter 是一个无状态、可快速弹性伸缩的组件，它可以兼容 Influxdb 的 Line Protocol 和 OpenTSDB 的 telnet/JSON 写入协议规范，提供了丰富的数据接入能力，有效的节省用户迁移成本，降低用户应用迁移的难度。

用户可以根据需求弹性部署 taosAdapter 实例，结合场景的需要，快速提升数据写入的吞吐量，为不同应用场景下的数据写入提供保障。

通过taosAdapter，用户可以将 collectd 和 StatsD 收集的数据直接推送到TDengine ，实现应用场景的无缝迁移，非常的轻松便捷。taosAdapter还支持Telegraf、Icinga、TCollector 、node_exporter的数据接入，使用详情参考[taosAdapter](https://www.taosdata.com/cn/documentation/tools/adapter)。

如果使用 collectd，修改其默认位置 `/etc/collectd/collectd.conf` 的配置文件为指向 taosAdapter 部署的节点 IP 地址和端口。假设 taosAdapter 的 IP 地址为192.168.1.130，端口为 6046，配置如下：

```html
LoadPlugin write_tsdb
<Plugin write_tsdb>
    <Node>
        Host "192.168.1.130"
        Port "6046"
        HostTags "status=production"
        StoreRates false
        AlwaysAppendDS false
    </Node>
</Plugin>
```

即可让 collectd 将数据使用推送到 OpenTSDB 的插件方式推送到 taosAdapter， taosAdapter 将调用 API 将数据写入到 taosd 中，从而完成数据的写入工作。如果你使用的是 StatsD 相应地调整配置文件信息。

- **调整看板（Dashborad）系统**

在数据能够正常写入TDengine 后，可以调整适配 Grafana 将写入 TDengine 的数据可视化呈现出来。获取和使用TDengine提供的Grafana插件请参考[与其他工具的连接](https://www.taosdata.com/cn/documentation/connections#grafana)。

TDengine 提供了默认的两套Dashboard 模板，用户只需要将 Grafana 目录下的模板导入到Grafana中即可激活使用。

![](../../images/IT-DevOps-Solutions-Immigrate-OpenTSDB-Dashboard.jpg)

<div align = center>图2. 导入Grafana模板</div>

操作完以上步骤后，就完成了将OpenTSDB替换成为TDengine的迁移工作。可以看到整个流程非常简单，不需要写代码，只需要对某些配置文件进行调整即可完成全部的迁移工作。

### 3、迁移后架构

完成迁移以后，此时的系统整体的架构如下图（图3）所示，而整个过程中采集端、数据写入端、以及监控呈现端均保持了稳定，除了极少的配置调整外，不涉及任何重要的更改和变动。OpenTSDB 大量的应用场景均为 DevOps ，这种场景下，简单的参数设置即可完成 OpenTSDB 到 TDengine 迁移动作，使用上 TDengine 更加强大的处理能力和查询性能。

在绝大多数的 DevOps 场景中，如果你拥有一个小规模的 OpenTSDB 集群（3台及以下的节点）作为 DevOps 的存储端，依赖于 OpenTSDB 为系统持久化层提供数据存储和查询功能，那么你可以安全地将其替换为 TDengine，并节约更多的计算和存储资源。在同等计算资源配置情况下，单台 TDengine 即可满足 3 ~ 5 台 OpenTSDB 节点提供的服务能力。如果规模比较大，那便需要采用TDengine集群。

如果你的应用特别复杂，或者应用领域并不是 DevOps 场景，你可以继续阅读后续的章节，更加全面深入地了解将 OpenTSDB 的应用迁移到 TDengine 的高级话题。

![IT-DevOps-Solutions-Immigrate-TDengine-Arch](../../images/IT-DevOps-Solutions-Immigrate-TDengine-Arch.jpg)

<div align = center>图3. 迁移完成后的系统架构</div>

## 其他场景的迁移评估与策略

### 1、TDengine 与 OpenTSDB 的差异

本章将详细介绍 OpenTSDB 与 TDengine 在系统功能层面上存在的差异。阅读完本章的内容，你可以全面地评估是否能够将某些基于 OpenTSDB 的复杂应用迁移到TDengine上，以及迁移之后应该注意的问题。

TDengine 当前只支持 Grafana 的可视化看板呈现，所以如果你的应用中使用了 Grafana 以外的前端看板（例如[TSDash](https://github.com/facebook/tsdash)、[Status Wolf](https://github.com/box/StatusWolf)等），那么前端看板将无法直接迁移到 TDengine，需要将前端看板重新适配到 Grafana 才可以正常运行。

截止到 2.3.0.x 版本，TDengine 只能够支持 collectd 和 StatsD 作为数据收集汇聚软件，当然后面会陆续提供更多的数据收集聚合软件的接入支持。如果您的收集端使用了其他类型的数据汇聚器，您的应用需要适配到这两个数据汇聚端系统，才能够将数据正常写入。除了上述两个数据汇聚端软件协议以外，TDengine 还支持通过 InfluxDB 的行协议和 OpenTSDB 的数据写入协议、JSON 格式将数据直接写入，您可以重写数据推送端的逻辑，使用 TDengine 支持的行协议来写入数据。

此外，如果你的应用中使用了 OpenTSDB 以下特性，在将应用迁移到 TDengine 之前你还需要了解以下注意事项：

1. ` /api/stats`：如果你的应用中使用了该项特性来监控OpenTSDB的服务状态，并在应用中建立了相关的逻辑来联动处理，那么这部分状态读取和获取的逻辑需要重新适配到TDengine。TDengine 提供了全新的处理集群状态监控机制，来满足你的应用对其进行的监控和维护的需求。
2.  `/api/tree`：如果你依赖于OpenTSDB的该项特性来进行时间线的层级化组织和维护，那么便无法将其直接迁移至TDengine。TDengine 采用了数据库->超级表->子表这样的层级来组织和维护时间线，归属于同一个超级表的所有的时间线在系统中同一个层级，但是可以通过不同标签值的特殊构造来模拟应用逻辑上的多级结构。
3. `Rollup And PreAggregates`：采用了 Rollup 和 PreAggregates 需要应用来决定在合适的地方访问 Rollup 的结果，在某些场景下又要访问原始的结果，这种结构的不透明性让应用处理逻辑变得极为复杂而且完全不具有移植性。我们认为这种策略是时序数据库无法提供高性能聚合情况下的妥协与折中。TDengine 暂不支持多个时间线的自动降采样和（时间段范围的）预聚合，由于 其拥有的高性能查询处理逻辑，即使不依赖于Rollup 和 （时间段）预聚合计算结果，也能够提供很高性能的查询响应，而且让你的应用查询处理逻辑更加简单。
4. `Rate`: TDengine提供了两个计算数值变化率的函数，分别是Derivative（其计算结果与InfluxDB的Derivative行为一致）和IRate（其计算结果与Prometheus中的IRate函数计算结果一致）。但是这两个函数的计算结果与 Rate 有细微的差别，但整体上功能更强大。此外，**OpenTSDB提供的所有计算函数，TDengine 均有对应的查询函数支持，并且TDengine的查询函数功能远超过OpenTSDB支持的查询函数，**可以极大地简化你的应用处理逻辑。

通过上面的介绍，相信你应该能够了解OpenTSDB迁移到TDengine带来的变化，这些信息也有助于你正确地判断是否可以接受将应用 迁移到TDengine之上，体验TDengine提供的强大的时序数据处理能力和便捷的使用体验。

### 2、迁移策略

首先将基于OpenTSDB的系统进行迁移涉及到的数据模式设计、系统规模估算、数据写入端改造，进行数据分流、应用适配工作；之后将两个系统并行运行一段时间，再将历史数据迁移到 TDengine 中。当然如果你的应用中有部分功能强依赖于上述OpenTSDB特性，同时又不希望停止使用，可以考虑保持原有的OpenTSDB系统运行，同时启动 TDengine来提供主要的服务。

## 数据模型设计

一方面，TDengine 要求其入库的数据具有严格的模式定义。另一方面，TDengine 的数据模型相对于 OpenTSDB 来说又更加丰富，多值模型能够兼容全部的单值模型的建立需求。

现在让我们假设一个 DevOps 的场景，我们使用了 collectd 收集设备的基础度量（metrics），包含了 memory 、swap、disk 等几个度量，其在 OpenTSDB 中的模式如下：

| 序号 | 测量（metric） | 值名称 | 类型   | tag1 | tag2        | tag3                 | tag4      | tag5   |
| ---- | -------------- | ------ | ------ | ---- | ----------- | -------------------- | --------- | ------ |
| 1    | memory         | value  | double | host | memory_type | memory_type_instance | source    |   n/a     |
| 2    | swap           | value  | double | host | swap_type   | swap_type_instance   | source    |      n/a  |
| 3    | disk           | value  | double | host | disk_point  | disk_instance        | disk_type | source |



TDengine 要求存储的数据具有数据模式，即写入数据之前需创建超级表并指定超级表的模式。对于数据模式的建立，你有两种方式来完成此项工作：1）充分利用TDengine对 OpenTSDB 的数据原生写入的支持，调用 TDengine 提供的 API 将（文本行或 JSON 格式）数据写入，并自动化地建立单值模型。采用这种方式不需要对数据写入应用进行较大的调整，也不需要对写入的数据格式进行转换。

在 C 语言层面，TDengine提供了 taos_insert_lines 来直接写入OpenTSDB格式的数据（在2.3.x 版本中该函数对应的是 taos_schemaless_insert ）。其代码参考示例请参见安装包目录下示例代码 schemaless.c。

 2）在充分理解 TDengine 的数据模型基础上，结合生成数据的特点，手动方式建立 OpenTSDB 到 TDengine 的数据模型调整的映射关系。TDengine 能够支持多值模型和单值模型，考虑到OpenTSDB 均为单值映射模型，这里推荐使用单值模型在 TDengine 中进行建模。

- **单值模型**。

具体步骤如下：将度量（metrics）的名称作为 TDengine 超级表的名称，该超级表建成后具有两个基础的数据列—时间戳（timestamp）和值（value），超级表的标签等效于 度量 的标签信息，标签数量等同于度量 的标签的数量。子表的表名采用具有固定规则的方式进行命名：`metric + '_' + tags1_value + '_' + tag2_value + '_' + tag3_value ... `作为子表名称。

在TDengine中建立3个超级表：

```sql
create stable memory(ts timestamp, val float) tags(host binary(12)，memory_type binary(20), memory_type_instance binary(20), source binary(20));
create stable swap(ts timestamp, val double) tags(host binary(12), swap_type binary(20), swap_type_binary binary(20), source binary(20));
create stable disk(ts timestamp, val double) tags(host binary(12), disk_point binary(20), disk_instance binary(20), disk_type binary(20), source binary(20));
```



对于子表使用动态建表的方式创建如下所示：

```sql
insert into memory_vm130_memory_bufferred_collectd  using memory tags(‘vm130’, ‘memory’, 'buffer', 'collectd') values(1632979445, 3.0656);
```

最终系统中会建立 340 个左右的子表，3个超级表。需要注意的是，如果采用串联标签值的方式导致子表名称超过系统限制（191字节），那么需要采用一定的编码方式（例如 MD5）将其转化为可接受长度。

- **多值模型**

如果你想要利用 TDengine 的多值模型能力，需要首先满足以下要求：不同的采集量具有相同的采集频率，且能够通过消息队列**同时到达**数据写入端，从而确保使用SQL语句将多个指标一次性写入。将度量的名称作为超级表的名称，建立具有相同采集频率且能够同时到达的数据多列模型。子表的表名采用具有固定规则的方式进行命名。上述每个度量均只包含一个测量值，因此无法将其转化为多值模型。



## 数据分流与应用适配

从消息队列中订阅数据，并启动调整后的写入程序写入数据。

数据开始写入持续一段时间后，可以采用SQL语句检查写入的数据量是否符合预计的写入要求。统计数据量使用如下SQL语句：

```sql
select count(*) from memory
```

完成查询后，如果写入的数据与预期的相比没有差别，同时写入程序本身没有异常的报错信息，那么可用确认数据写入是完整有效的。

TDengine不支持采用OpenTSDB的查询语法进行查询或数据获取处理，但是针对OpenTSDB的每种查询都提供对应的支持。可以用检查附录1获取对应的查询处理的调整和应用使用的方式，如果需要全面了解TDengine支持的查询类型，请参阅TDengine的用户手册。

TDengine支持标准的JDBC 3.0接口操纵数据库，你也可以使用其他类型的高级语言的连接器来查询读取数据，以适配你的应用。具体的操作和使用帮助也请参阅用户手册。

## 历史数据迁移

### 1、使用工具自动迁移数据

为了方便历史数据的迁移工作，我们为数据同步工具DataX提供了插件，能够将数据自动写入到TDengine中，需要注意的是DataX的自动化数据迁移只能够支持单值模型的数据迁移过程。

DataX 具体的使用方式及如何使用DataX将数据写入TDengine请参见[基于DataX的TDeninge数据迁移工具](https://www.taosdata.com/blog/2021/10/26/3156.html)。 

在对DataX进行迁移实践后，我们发现通过启动多个进程，同时迁移多个metric 的方式，可以大幅度的提高迁移历史数据的效率，下面是迁移过程中的部分记录，希望这些能为应用迁移工作带来参考。

| datax实例个数 (并发进程个数) | 迁移记录速度 （条/秒) | 
| ---- | -------------- | 
| 1    | 约13.9万        | 
| 2    | 约21.8万        | 
| 3    | 约24.9万        | 
| 5    | 约29.5万        | 
| 10   | 约33万          | 


<br/>（注：测试数据源自 单节点 Intel(R) Core(TM) i7-10700 CPU@2.90GHz 16核64G硬件设备，channel和batchSize 分别为8和1000，每条记录包含10个tag)


### 2、手动迁移数据

如果你需要使用多值模型进行数据写入，就需要自行开发一个将数据从OpenTSDB导出的工具，然后确认哪些时间线能够合并导入到同一个时间线，再将可以同时导入的时间通过SQL语句的写入到数据库中。

手动迁移数据需要注意以下两个问题：

1）在磁盘中存储导出数据时，磁盘需要有足够的存储空间以便能够充分容纳导出的数据文件。为了避免全量数据导出后导致磁盘文件存储紧张，可以采用部分导入的模式，对于归属于同一个超级表的时间线优先导出，然后将导出部分的数据文件导入到TDengine系统中。

2）在系统全负载运行下，如果有足够的剩余计算和IO资源，可以建立多线程的导入机制，最大限度地提升数据迁移的效率。考虑到数据解析对于CPU带来的巨大负载，需要控制最大的并行任务数量，以避免因导入历史数据而触发的系统整体过载。

由于TDegnine本身操作简易性，所以不需要在整个过程中进行索引维护、数据格式的变化处理等工作，整个过程只需要顺序执行即可。

当历史数据完全导入到TDengine以后，此时两个系统处于同时运行的状态，之后便可以将查询请求切换到TDengine上，从而实现无缝的应用切换。

## 附录1: OpenTSDB查询函数对应表

**Avg**

等效函数：avg

示例：

SELECT avg(val) FROM (SELECT first(val) FROM super_table WHERE ts >= startTime and ts <= endTime INTERVAL(20s) Fill(linear)) INTERVAL(20s)

备注：

1. Interval内的数值与外层查询的 interval 数值需要相同。
2. 在TDengine中插值处理需要使用子查询来协助完成，如上所示，在内层查询中指明插值类型即可，由于 OpenTSDB 中数值的插值使用了线性插值，因此在插值子句中使用fill(linear) 来声明插值类型。以下有相同插值计算需求的函数，均采用该方法处理。
3. Interval中参数20s表示将内层查询按照20秒一个时间窗口生成结果。在真实的查询中，需要调整为不同的记录之间的时间间隔。这样可确保等效于原始数据生成了插值结果。
4. 由于 OpenTSDB 特殊的插值策略和机制，聚合查询（Aggregate）中先插值再计算的方式导致其计算结果与 TDengine 不可能完全一致。但是在降采样（Downsample）的情况下，TDengine 和 OpenTSDB 能够获得一致的结果（由于 OpenTSDB 在聚合查询和降采样查询中采用了完全不同的插值策略）。

 

**Count**

等效函数：count

示例：

select count(*) from super_table_name;

 

**Dev**

等效函数：stddev

示例：

Select stddev(val) from table_name

 

**Estimated percentiles**

等效函数：apercentile

示例：

Select apercentile(col1, 50, “t-digest”) from table_name

备注：

1. 近似查询处理过程中，OpenTSDB默认采用t-digest算法，所以为了获得相同的计算结果，需要在apercentile函数中指明使用的算法。TDengine能够支持两种不同的近似处理算法，分别通过”default”和”t-digest”来声明。

 

**First**

等效函数：first

示例：

Select first(col1) from table_name

 

**Last**

等效函数：last

示例：

Select last(col1) from table_name

 

**Max**

等效函数：max

示例：

Select max(value) from (select first(val) value from table_name interval(10s) fill(linear)) interval(10s)

备注：Max函数需要插值，原因见上。

 

**Min**

等效函数：min

示例：

Select min(value) from (select first(val) value from table_name interval(10s) fill(linear)) interval(10s);

 

**MinMax**

等效函数：max

Select max(val) from table_name

备注：该函数无插值需求，因此可用直接计算。

 

**MimMin**

等效函数：min

Select min(val) from table_name

备注：该函数无插值需求，因此可用直接计算。

 

**Percentile**

等效函数：percentile

备注：

 

**Sum**

等效函数：sum

Select max(value) from (select first(val) value from table_name interval(10s) fill(linear)) interval(10s)

备注：该函数无插值需求，因此可用直接计算。

 

**Zimsum**

等效函数：sum

Select sum(val) from table_name

备注：该函数无插值需求，因此可用直接计算。

 

完整示例：

```json
//OpenTSDB查询JSON
query = {
“start”:1510560000,
“end”: 1515000009,
“queries”:[{
“aggregator”: “count”,
“metric”:”cpu.usage_user”,
}]
}
 
//等效查询SQL:
SELECT count(*)
FROM `cpu.usage_user`
WHERE ts>=1510560000 AND ts<=1515000009
```

 

## 附录2: 资源估算方法

### 数据生成环境

我们仍然使用第 4 章中的假设环境，3个测量值。分别是：温度和湿度的数据写入的速率是每 5 秒一条记录，时间线 10万个。空气质量的写入速率是10 秒一条记录，时间线1万个，查询的请求频率 500 QPS。

### 存储资源估算

假设产生数据并需要存储的传感器设备数量为 `n`，数据生成的频率为` t `条/秒，每条记录的长度为 `L` bytes，则每天产生的数据规模为 `n×t×L` bytes。假设压缩比为 C，则每日产生数据规模为 `(n×t×L)/C` bytes。存储资源预估为能够容纳1.5年的数据规模，生产环境下 TDengine 的压缩比 C 一般在 5 ~ 7 之间，同时为最后结果增加 20% 的冗余，可计算得到需要存储资源：

```matlab
(n×t×L)×(365×1.5)×(1+20%)/C
```

结合以上的计算公式，将参数带入计算公式，在不考虑标签信息的情况下，每年产生的原始数据规模是11.8TB。需要注意的是，由于标签信息在TDengine中关联到每个时间线，并不是每条记录。所以需要记录的数据量规模相对于产生的数据有一定的降低，而这部分标签数据整体上可以忽略不记。假设压缩比为5，则保留的数据规模最终为 2.56 TB。

### 存储设备选型考虑

硬盘应该选用具有较好随机读性能的硬盘设备，如果能够有SSD，尽可能考虑使用SSD。较好的随机读性能的磁盘对于提升系统查询性能具有极大的帮助，能够整体上提升系统的查询响应性能。为了获得较好的查询性能，硬盘设备的单线程随机读IOPS的性能指标不应该低于1000，能够达到5000 IOPS以上为佳。为了获得当前的设备随机读取的IO性能的评估，建议使用fio软件对其进行运行性能评估（具体的使用方式请参阅附录1），确认其是否能够满足大文件随机读性能要求。

硬盘写性能对于TDengine的影响不大。TDengine写入过程采用了追加写的模式，所以只要有较好的顺序写性能即可，一般意义上的SAS硬盘和SSD均能够很好地满足TDengine对于磁盘写入性能的要求。

### 计算资源估算

由于物联网数据的特殊性，数据产生的频率固定以后，TDengine写入的过程对于（计算和存储）资源消耗都保持一个相对固定的量。《[TDengine的运营与维护](https://www.taosdata.com/cn/documentation/administrator)》上的描述，该系统中每秒 22000个写入，消耗CPU不到 1个核。

在针对查询所需要消耗的CPU资源的估算上，假设应用要求数据库提供的QPS为 10000，每次查询消耗的CPU时间约 1 ms，那么每个核每秒提供的查询为 1000 QPS，满足10000 QPS的查询请求，至少需要10个核。为了让系统整体上CPU负载小于 50%，整个集群需要10个核的两倍，即 20 个核。

### 内存资源估算

数据库默认为每个Vnode分配内存 16MB*3缓冲区，集群系统包括22个CPU核，则默认会建立22个虚拟节点Vnode，每个Vnode包含1000张表，则可以容纳所有的表。则约1个半小时写满一个block，从而触发落盘，可以不做调整。22个Vnode共计需要内存缓存约 1GB。考虑到查询所需要的内存，假设每次查询的内存开销约50MB，则500个查询并发需要的内存约25GB。

综上所述，可使用单台16核32GB的机器，或者使用2台 8核 16GB机器构成的集群。

## 附录3: 集群部署及启动

TDengine提供了丰富的帮助文档说明集群安装、部署的诸多方面的内容，这里提供响应的文档索引，供你参考。

### 集群部署

首先是安装 TDengine，从官网上下载 TDengine 最新稳定版，解压缩后运行 install.sh 进行安装。各种安装包的使用帮助请参见博客[《 TDengine多种安装包的安装和卸载》](https://www.taosdata.com/blog/2019/08/09/566.html)。

注意安装完成以后，不要立即启动taosd服务，在正确配置完成参数以后才启动taosd服务。

### 设置运行参数并启动服务

为确保系统能够正常获取运行的必要信息。请在服务端正确设置以下关键参数：

FQDN、firstEp、secondEP、dataDir、logDir、tmpDir、serverPort。各参数的具体含义及设置的要求，可参见文档《[TDengine 集群安装、管理](https://www.taosdata.com/cn/documentation/cluster)》

按照相同的步骤，在需要运行的节点上设置参数，并启动taosd服务，然后添加Dnode到集群中。

最后启动taos，执行命令 show dnodes，如果能看到所有的加入集群的节点，那么集群顺利搭建完成。具体的操作流程及注意事项，请参阅文档《[TDengine 集群安装、管理](https://www.taosdata.com/cn/documentation/cluster)》

## 附录4: 超级表名称

由于OpenTSDB的metric名称中带有点号（“.“），例如”cpu.usage_user”这种名称的metric。但是点号在TDengine中具有特殊含义，是用来分隔数据库和表名称的分隔符。TDengine也提供转义符，以允许用户在（超级）表名称中使用关键词或特殊分隔符（如：点号）。为了使用特殊字符，需要采用转义字符将表的名称括起来，例如：`cpu.usage_user`这样就是合法的（超级）表名称。

## 附录5：参考文章

1. [使用 TDengine + collectd/StatsD + Grafana 快速搭建 IT 运维监控系统](https://www.taosdata.com/cn/documentation20/devops/collectd)
2. [通过 collectd 将采集数据直接写入TDengine](https://www.taosdata.com/cn/documentation20/insert#collectd)
