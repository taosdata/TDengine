---
sidebar_label: OpenTSDB 迁移到 TDengine
title: OpenTSDB 应用迁移到 TDengine 的最佳实践
---

作为一个分布式、可伸缩、基于 HBase 的分布式时序数据库系统，得益于其先发优势，OpenTSDB 被 DevOps 领域的人员引入并广泛地应用在了运维监控领域。但最近几年，随着云计算、微服务、容器化等新技术快速落地发展，企业级服务种类变得越来越多，架构也越来越复杂，应用运行基础环境日益多样化，给系统和运行监控带来的压力也越来越大。从这一现状出发，使用 OpenTSDB 作为 DevOps 的监控后端存储，越来越受困于其性能问题以及迟缓的功能升级，以及由此而衍生出来的应用部署成本上升和运行效率降低等问题，这些问题随着系统规模的扩大日益严重。

在这一背景下，为满足高速增长的物联网大数据市场和技术需求，在吸取众多传统关系型数据库、NoSQL 数据库、流计算引擎、消息队列等软件的优点之后，涛思数据自主开发出创新型大数据处理产品 TDengine。在时序大数据处理上，TDengine 有着自己独特的优势。就 OpenTSDB 当前遇到的问题来说，TDengine 能够有效解决。

相对于 OpenTSDB，TDengine 具有如下显著特点：

- 数据写入和查询的性能远超 OpenTSDB；
- 针对时序数据的高效压缩机制，压缩后在磁盘上的存储空间不到 1/5；
- 安装部署非常简单，单一安装包完成安装部署，不依赖其他的第三方软件，整个安装部署过程秒级搞定;
- 提供的内建函数覆盖 OpenTSDB 支持的全部查询函数，还支持更多的时序数据查询函数、标量函数及聚合函数，支持多种时间窗口聚合、连接查询、表达式运算、多种分组聚合、用户定义排序、以及用户定义函数等高级查询功能。采用类 SQL 的语法规则，更加简单易学，基本上没有学习成本。
- 支持多达 128 个标签，标签总长度可达到 16 KB；
- 除 REST 接口之外，还提供 C/C++、Java、Python、Go、Rust、Node.js、C#、Lua（社区贡献）、PHP（社区贡献）等多种语言的接口，支持 JDBC 等多种企业级标准连接器协议。

如果我们将原本运行在 OpenTSDB 上的应用迁移到 TDengine 上，不仅可以有效地降低计算和存储资源的占用、减少部署服务器的规模，还能够极大减少运行维护的成本的输出，让运维管理工作更简单、更轻松，大幅降低总拥有成本。与 OpenTSDB 一样，TDengine 也已经进行了开源，不同的是，除了单机版，后者还实现了集群版开源，被厂商绑定的顾虑一扫而空。

在下文中我们将说明如何在不编码的情况下将 OpenTSDB 的应用快速、安全、可靠地迁移到 TDengine 之上。

## TDengine 与 OpenTSDB 的差异

本节将详细介绍 OpenTSDB 与 TDengine 在系统功能层面上存在的差异。阅读完本节的内容，你可以全面地评估是否能够将某些基于 OpenTSDB 的复杂应用迁移到 TDengine 上，以及迁移之后应该注意的问题。

TDengine 当前只支持 Grafana 的可视化看板呈现，所以如果你的应用中使用了 Grafana 以外的前端看板（例如[TSDash](https://github.com/facebook/tsdash)、[Status Wolf](https://github.com/box/StatusWolf)等），那么前端看板将无法直接迁移到 TDengine，需要将前端看板重新适配到 Grafana 才可以正常运行。

如果您的收集端使用了像 collectd 和 StatsD 这样的数据采集工具，要重新配置这些数据采集工具将数据写入到 TDengine。TDengine 还支持通过 InfluxDB 的行协议和 OpenTSDB 的数据写入协议、JSON 格式将数据直接写入，您可以重写数据推送端的逻辑，使用 TDengine 支持的行协议来写入数据。

此外，如果你的应用中使用了 OpenTSDB 以下特性，在将应用迁移到 TDengine 之前你还需要了解以下注意事项：

1. `/api/stats`：如果你的应用中使用了该项特性来监控 OpenTSDB 的服务状态，并在应用中建立了相关的逻辑来联动处理，那么这部分状态读取和获取的逻辑需要重新适配到 TDengine。TDengine 提供了全新的处理集群状态监控机制，来满足你的应用对其进行的监控和维护的需求。
2. `/api/tree`：如果你依赖于 OpenTSDB 的该项特性来进行时间线的层级化组织和维护，那么便无法将其直接迁移至 TDengine。TDengine 采用了数据库->超级表->子表这样的层级来组织和维护时间线，归属于同一个超级表的所有的时间线在系统中同一个层级，但是可以通过不同标签值的特殊构造来模拟应用逻辑上的多级结构。
3. `Rollup And PreAggregates`：采用了 Rollup 和 PreAggregates 需要应用来决定在合适的地方访问 Rollup 的结果，在某些场景下又要访问原始的结果，这种结构的不透明性让应用处理逻辑变得极为复杂而且完全不具有移植性。我们认为这种策略是时序数据库无法提供高性能聚合情况下的妥协与折中。TDengine 暂不支持多个时间线的自动降采样和（时间段范围的）预聚合，由于 其拥有的高性能查询处理逻辑，即使不依赖于 Rollup 和 （时间段）预聚合计算结果，也能够提供很高性能的查询响应，而且让你的应用查询处理逻辑更加简单。
4. `Rate`: TDengine 提供了两个计算数值变化率的函数，分别是 Derivative（其计算结果与 InfluxDB 的 Derivative 行为一致）和 IRate（其计算结果与 Prometheus 中的 IRate 函数计算结果一致）。但是这两个函数的计算结果与 Rate 有细微的差别，但整体上功能更强大。此外，**OpenTSDB 提供的所有计算函数，TDengine 均有对应的查询函数支持，并且 TDengine 的查询函数功能远超过 OpenTSDB 支持的查询函数，**可以极大地简化你的应用处理逻辑。

通过上面的介绍，相信你应该能够了解 OpenTSDB 迁移到 TDengine 带来的变化，这些信息也有助于你正确地判断是否可以接受将应用 迁移到 TDengine 之上，体验 TDengine 提供的强大的时序数据处理能力和便捷的使用体验。

## 迁移策略

首先将基于 OpenTSDB 的系统进行迁移涉及到的数据模式设计、系统规模估算、数据写入端改造，进行数据分流、应用适配工作；之后将两个系统并行运行一段时间，再将历史数据迁移到 TDengine 中。当然如果你的应用中有部分功能强依赖于上述 OpenTSDB 特性，同时又不希望停止使用，可以考虑保持原有的 OpenTSDB 系统运行，同时启动 TDengine 来提供主要的服务。

### 数据模型设计

一方面，TDengine 要求其入库的数据具有严格的模式定义。另一方面，TDengine 的数据模型相对于 OpenTSDB 来说又更加丰富，多值模型能够兼容全部的单值模型的建立需求。

现在让我们假设一个 DevOps 的场景，我们使用了 collectd 收集设备的基础度量（metrics），包含了 memory 、swap、disk 等几个度量，其在 OpenTSDB 中的模式如下：

| 序号 | 测量（metric） | 值名称 | 类型   | tag1 | tag2        | tag3                 | tag4      | tag5   |
| ---- | -------------- | ------ | ------ | ---- | ----------- | -------------------- | --------- | ------ |
| 1    | memory         | value  | double | host | memory_type | memory_type_instance | source    | n/a    |
| 2    | swap           | value  | double | host | swap_type   | swap_type_instance   | source    | n/a    |
| 3    | disk           | value  | double | host | disk_point  | disk_instance        | disk_type | source |

TDengine 要求存储的数据具有数据模式，即写入数据之前需创建超级表并指定超级表的模式。对于数据模式的建立，你有两种方式来完成此项工作：1）充分利用 TDengine 对 OpenTSDB 的数据原生写入的支持，调用 TDengine 提供的 API 将（文本行或 JSON 格式）数据写入，并自动化地建立单值模型。采用这种方式不需要对数据写入应用进行较大的调整，也不需要对写入的数据格式进行转换。

在 C 语言层面，TDengine 提供了 `taos_schemaless_insert()` 函数来直接写入 OpenTSDB 格式的数据（在更早版本中该函数名称是 `taos_insert_lines()`）。其代码参考示例请参见安装包目录下示例代码 schemaless.c。

2）在充分理解 TDengine 的数据模型基础上，结合生成数据的特点，手动方式建立 OpenTSDB 到 TDengine 的数据模型调整的映射关系。TDengine 能够支持多值模型和单值模型，考虑到 OpenTSDB 均为单值映射模型，这里推荐使用单值模型在 TDengine 中进行建模。

- **单值模型**。

具体步骤如下：将度量（metrics）的名称作为 TDengine 超级表的名称，该超级表建成后具有两个基础的数据列—时间戳（timestamp）和值（value），超级表的标签等效于 度量 的标签信息，标签数量等同于度量 的标签的数量。子表的表名采用具有固定规则的方式进行命名：`metric + '_' + tags1_value + '_' + tag2_value + '_' + tag3_value ...`作为子表名称。

在 TDengine 中建立 3 个超级表：

```sql
create stable memory(ts timestamp, val float) tags(host binary(12)，memory_type binary(20), memory_type_instance binary(20), source binary(20));
create stable swap(ts timestamp, val double) tags(host binary(12), swap_type binary(20), swap_type_binary binary(20), source binary(20));
create stable disk(ts timestamp, val double) tags(host binary(12), disk_point binary(20), disk_instance binary(20), disk_type binary(20), source binary(20));
```

对于子表使用动态建表的方式创建如下所示：

```sql
insert into memory_vm130_memory_buffered_collectd  using memory tags(‘vm130’, ‘memory’, 'buffer', 'collectd') values(1632979445, 3.0656);
```

最终系统中会建立 340 个左右的子表，3 个超级表。需要注意的是，如果采用串联标签值的方式导致子表名称超过系统限制（191 字节），那么需要采用一定的编码方式（例如 MD5）将其转化为可接受长度。

- **多值模型**

如果你想要利用 TDengine 的多值模型能力，需要首先满足以下要求：不同的采集量具有相同的采集频率，且能够通过消息队列**同时到达**数据写入端，从而确保使用 SQL 语句将多个指标一次性写入。将度量的名称作为超级表的名称，建立具有相同采集频率且能够同时到达的数据多列模型。子表的表名采用具有固定规则的方式进行命名。上述每个度量均只包含一个测量值，因此无法将其转化为多值模型。

### 数据分流与应用适配

从消息队列中订阅数据，并启动调整后的写入程序写入数据。

数据开始写入持续一段时间后，可以采用 SQL 语句检查写入的数据量是否符合预计的写入要求。统计数据量使用如下 SQL 语句：

```sql
select count(*) from memory
```

完成查询后，如果写入的数据与预期的相比没有差别，同时写入程序本身没有异常的报错信息，那么可用确认数据写入是完整有效的。

TDengine 不支持采用 OpenTSDB 的查询语法进行查询或数据获取处理，但是针对 OpenTSDB 的每种查询都提供对应的支持。可以用检查附录 1 获取对应的查询处理的调整和应用使用的方式，如果需要全面了解 TDengine 支持的查询类型，请参阅 TDengine 的用户手册。

TDengine 支持标准的 JDBC 3.0 接口操纵数据库，你也可以使用其他类型的高级语言的连接器来查询读取数据，以适配你的应用。具体的操作和使用帮助也请参阅用户手册。


## 使用 DataX 迁移数据

为了方便历史数据的迁移工作，我们为数据同步工具 DataX 提供了适配 TDengine 3.0 的插件，能够将数据自动写入到 TDengine 中，需要注意的是 DataX 的自动化数据迁移只能够支持单值模型的数据迁移过程。

### 安装和部署 TDengine 

在进行数据迁移之前，要有一个正确运行的 TDengine 集群。首先是 TDengine 的安装，从官网上下载 TDengine 最新稳定版进行安装。各种安装包的使用帮助请参考 [安装指南](../../get-started/package)

安装完成后，请根据 [部署指南](../../deployment/deploy) 配置集群。

### 插件功能介绍

1. TDengine30Reader 提供的功能：
   1. 支持通过 SQL 进行数据筛选；
   2. 根据时间间隔进行任务切分；
   3. 支持 TDengine 的全部数据类型；
   4. 支持批量读取，通过 batchSize 参数控制批量拉取结果集的大小，提高读取性能。
2. TDengine30Writer 支持的功能：
   1. 支持 OpenTSDB 的 json 格式的行协议，使用 TDengine 的 schemaless 方式写入 TDengine。
   2. 支持批量写入，通过 batchSize 参数控制批量写入的数量，提高写入性能。

### DataX 安装环境准备

1. 需要安装 TDengine 客户端
2. 需要安装 JDK 1.8 环境（运行 DataX）
3. 需要安装 Python 环境（运行 DataX）
4. 需要 maven 编译环境（如果不编译 DataX 则可以不安装 maven）

### 安装

1. 下载源码
~~~
git clone https://github.com/taosdata/DataX.git
~~~
2. 编译打包
~~~
cd DataX
mvn -U clean package assembly:assembly -Dmaven.test.skip=true
~~~
3. 安装
~~~
cp target/datax.tar.gz your_install_dir
cd your_install_dir
tar -zxvf dataX.tar.gz
~~~

### 数据迁移 Job 的配置

以一个从 OpenTSDB 到 TDengine 3.0 版本的数据迁移任务为例，配置文件 opentsdb2tdengine.json 如下：
~~~
{
   "job":{
     "content":[{
       "reader": {
         "name": "opentsdbreader",
         "parameter": {
           "endpoint": "http://192.168.1.180:4242",
           "column": ["weather_temperature"],
           "beginDateTime": "2021-01-01 00:00:00",
           "endDateTime": "2021-01-01 01:00:00"
         }
       },
     "writer": {
       "name": "tdengine30writer",
       "parameter": {
            "username": "root",
            "password": "taosdata",
            "connection": [
              {
                "table": [
                  "matric1"
                ],
                "jdbcUrl": "jdbc:TAOS://192.168.1.101:6030/test?timestampFormat=TIMESTAMP"
              }
            ],
            "batchSize": 1000,
            "ignoreTagsUnmatched": true
          }
       }
     }],
     "setting": {
       "speed": {
         "channel": 1
       }
     }
   }
 } 
~~~
配置说明：
1. 上面的配置表示，从 192.168.1.180 的 OpenTSDB，到 192.168.1.101 的 TDengine 的迁移。迁移 metric 为 weather_temperature，时间从 2021-01-01 00:00:00 开始，到 2021-01-01 01:00:00 结束的数据。
2. reader 使用 datax 的 opentsdbreader，parameter 的配置请参考：[opentsdbreader.md#配置参数](https://github.com/taosdata/DataX/blob/master/opentsdbreader/doc/opentsdbreader.md)
3. tdengine30writer 的 parameter 中，user，password 为必须项，没有默认值。batchSize 不是必须项，默认值为 1。详细参考：[tdengine30writer.md#配置参数](https://github.com/taosdata/DataX/blob/master/tdengine30writer/doc/tdengine30writer-CN.md)
4. TDengine 中，如果 dbname 指定的 database 不存在，则需要在迁移前创建数据库。

### 执行迁移任务

~~~
python bin/datax.py job/opentsdb2tdengine.json
~~~

### 限制条件

1. 目前，DataX 自带的 opentsdbreader 仅支持 OpenTSDB-2.3.X 版本。详细参考：[opentsdbreader#约束限制](https://github.com/alibaba/DataX/blob/master/opentsdbreader/doc/opentsdbreader.md#5-%E7%BA%A6%E6%9D%9F%E9%99%90%E5%88%B6)
2. 数据迁移工具依赖 TDengine 客户端中的 `libtaos.so/taos.dll/libtaos.dylib`，需要与服务端对应版本的 TDengine-client。

### FAQ

1. 如何估算一个数据迁移任务所需要的资源
   DataX 的每个 reader 按照自己的 task 切分策略进行任务划分，具体请参考 DataX 的任务调度规则。在估算资源是，需要按照数据迁移的数据量，任务切分规则和网络带宽限制等综合考虑，最好以实际数据迁移测试结果为准。
2. TDengine30Writer 的 batchSize 设置多大效率最高？
   batchSize 是控制批量写入的参数，在获取 batchSize 行纪录后，TDengineWriter 会向 TDengine 发送一次写入请求，这减少了与 TDengine 交互次数，从而提高了性能。从测试结果来看，batchSize 在 500-1000 范围内效率最高。
3. job 的配置中 channel 数为多少合适？
   job 中的 channel 数为流量控制的参数，每个 channel 都需要开辟一块内存，用来缓存数据。如果 channel 设置过大，会引起 OOM，所以 channel 数并不是越大越好。增加 channel 数后，需要提高 JVM 内存大小。从测试结果来看，channel 在 1～6 的范围内都是合适，能够保证 DataX 的流量最大化即可。
4. java.sql.SQLException: TDengine ERROR (8000060b): Invalid client value
   配置文件中 column 中没有配置 tbname，此时会触发行协议数据写入（行协议写入只会自动创建子表名，但需要提前创建好超级表），行协议写入的情况下不支持 TAG 数据类型为非 NCHAR，所以此种情况有两种解决方案：1.将 TAG 全部修改为 NCHAR 类型；2.在 Column 中配置好表名称这样不会触发行协议写入。
5. java.sql.SQLException: TDengine ERROR (8000060b): Timestamp data out of range
   配置文件中 column 中没有配置 tbname，此时会触发行协议数据写入，且 TAG 全部为 NCHAR 类型，此时需要保证时间戳的一列名称为 _ts，而不能是其他名称（行协议写入下，默认将最后的时间戳写入到 _ts 一列，且不能随意命名）。若想避免请使用 tbname 指定表名以避免触发行协议写入。

### 提升性能

 在对 DataX 进行迁移实践后，我们发现通过启动多个进程，同时迁移多个 metric 的方式，可以大幅度的提高迁移历史数据的效率，下面是迁移过程中的部分记录，希望这些能为应用迁移工作带来参考。

| DataX 实例个数 (并发进程个数) | 迁移记录速度 （条/秒) |
| ----------------------------- | --------------------- |
| 1                             | 约 13.9 万            |
| 2                             | 约 21.8 万            |
| 3                             | 约 24.9 万            |
| 5                             | 约 29.5 万            |
| 10                            | 约 33 万              |

（注：测试数据源自 单节点 Intel(R) Core(TM) i7-10700 CPU@2.90GHz 16 核 64G 硬件设备，channel 和 batchSize 分别为 8 和 1000，每条记录包含 10 个 tag)

## 手动迁移数据

如果你需要使用多值模型进行数据写入，就需要自行开发一个将数据从 OpenTSDB 导出的工具，然后确认哪些时间线能够合并导入到同一个时间线，再将可以同时导入的时间通过 SQL 语句的写入到数据库中。

手动迁移数据需要注意以下两个问题：

1）在磁盘中存储导出数据时，磁盘需要有足够的存储空间以便能够充分容纳导出的数据文件。为了避免全量数据导出后导致磁盘文件存储紧张，可以采用部分导入的模式，对于归属于同一个超级表的时间线优先导出，然后将导出部分的数据文件导入到 TDengine 系统中。

2）在系统全负载运行下，如果有足够的剩余计算和 IO 资源，可以建立多线程的导入机制，最大限度地提升数据迁移的效率。考虑到数据解析对于 CPU 带来的巨大负载，需要控制最大的并行任务数量，以避免因导入历史数据而触发的系统整体过载。

由于 TDengine 本身操作简易性，所以不需要在整个过程中进行索引维护、数据格式的变化处理等工作，整个过程只需要顺序执行即可。

当历史数据完全导入到 TDengine 以后，此时两个系统处于同时运行的状态，之后便可以将查询请求切换到 TDengine 上，从而实现无缝的应用切换。

## 附录 1: OpenTSDB 查询函数对应表

### Avg

等效函数：avg

示例：

```sql
SELECT avg(val) FROM (SELECT first(val) FROM super_table WHERE ts >= startTime and ts <= endTime INTERVAL(20s) Fill(linear)) INTERVAL(20s)
```

备注：

1. Interval 内的数值与外层查询的 interval 数值需要相同。
2. 在 TDengine 中插值处理需要使用子查询来协助完成，如上所示，在内层查询中指明插值类型即可，由于 OpenTSDB 中数值的插值使用了线性插值，因此在插值子句中使用 fill(linear) 来声明插值类型。以下有相同插值计算需求的函数，均采用该方法处理。
3. Interval 中参数 20s 表示将内层查询按照 20 秒一个时间窗口生成结果。在真实的查询中，需要调整为不同的记录之间的时间间隔。这样可确保等效于原始数据生成了插值结果。
4. 由于 OpenTSDB 特殊的插值策略和机制，聚合查询（Aggregate）中先插值再计算的方式导致其计算结果与 TDengine 不可能完全一致。但是在降采样（Downsample）的情况下，TDengine 和 OpenTSDB 能够获得一致的结果（由于 OpenTSDB 在聚合查询和降采样查询中采用了完全不同的插值策略）。

### Count

等效函数：count

示例：

```sql
select count(\*) from super_table_name;
```

### Dev

等效函数：stddev

示例：

```sql
Select stddev(val) from table_name
```

### Estimated percentiles

等效函数：apercentile

示例：

```sql
Select apercentile(col1, 50, “t-digest”) from table_name
```

备注：

1. 近似查询处理过程中，OpenTSDB 默认采用 t-digest 算法，所以为了获得相同的计算结果，需要在 apercentile 函数中指明使用的算法。TDengine 能够支持两种不同的近似处理算法，分别通过“default”和“t-digest”来声明。
### First

等效函数：first

示例：

```sql
Select first(col1) from table_name
```

### Last

等效函数：last

示例：

```sql
Select last(col1) from table_name
```

### Max

等效函数：max

示例：

```sql
Select max(value) from (select first(val) value from table_name interval(10s) fill(linear)) interval(10s)
```

备注：Max 函数需要插值，原因见上。

### Min

等效函数：min

示例：

```sql
Select min(value) from (select first(val) value from table_name interval(10s) fill(linear)) interval(10s);
```

### MinMax

等效函数：max

```sql
Select max(val) from table_name
```

备注：该函数无插值需求，因此可用直接计算。

### MimMin

等效函数：min

```sql
Select min(val) from table_name
```

备注：该函数无插值需求，因此可用直接计算。

### Percentile

等效函数：percentile

备注：

### Sum

等效函数：sum

```sql
Select sum(value) from (select first(val) value from table_name interval(10s) fill(linear)) interval(10s)
```

备注：该函数无插值需求，因此可用直接计算。

### Zimsum

等效函数：sum

```sql
Select sum(val) from table_name
```

备注：该函数无插值需求，因此可用直接计算。

完整示例：

```json
// OpenTSDB 查询 JSON
query = {
“start”:1510560000,
“end”: 1515000009,
“queries”:[{
“aggregator”: “count”,
“metric”:”cpu.usage_user”,
}]
}

//等效查询 SQL:
SELECT count(*)
FROM `cpu.usage_user`
WHERE ts>=1510560000 AND ts<=1515000009
```

## 附录 2: 资源估算方法

### 数据生成环境

我们仍然使用第 4 章中的假设环境，3 个测量值。分别是：温度和湿度的数据写入的速率是每 5 秒一条记录，时间线 10 万个。空气质量的写入速率是 10 秒一条记录，时间线 1 万个，查询的请求频率 500 QPS。

### 存储资源估算

假设产生数据并需要存储的传感器设备数量为 `n`，数据生成的频率为`t`条/秒，每条记录的长度为 `L` bytes，则每天产生的数据规模为 `86400×n×t×L` bytes。假设压缩比为 C，则每日产生数据规模为 `(86400×n×t×L)/C` bytes。存储资源预估为能够容纳 1.5 年的数据规模，生产环境下 TDengine 的压缩比 C 一般在 5 ~ 7 之间，同时为最后结果增加 20% 的冗余，可计算得到需要存储资源：

```matlab
(86400×n×t×L)×(365×1.5)×(1+20%)/C
```

结合以上的计算公式，将参数带入计算公式，在不考虑标签信息的情况下，每年产生的原始数据规模是 11.8TB。需要注意的是，由于标签信息在 TDengine 中关联到每个时间线，并不是每条记录。所以需要记录的数据量规模相对于产生的数据有一定的降低，而这部分标签数据整体上可以忽略不记。假设压缩比为 5，则保留的数据规模最终为 2.56 TB。

### 存储设备选型考虑

硬盘应该选用具有较好随机读性能的硬盘设备，如果能够有 SSD，尽可能考虑使用 SSD。较好的随机读性能的磁盘对于提升系统查询性能具有极大的帮助，能够整体上提升系统的查询响应性能。为了获得较好的查询性能，硬盘设备的单线程随机读 IOPS 的性能指标不应该低于 1000，能够达到 5000 IOPS 以上为佳。为了获得当前的设备随机读取的 IO 性能的评估，建议使用 `fio` 软件对其进行运行性能评估（具体的使用方式请参阅附录 1），确认其是否能够满足大文件随机读性能要求。

硬盘写性能对于 TDengine 的影响不大。TDengine 写入过程采用了追加写的模式，所以只要有较好的顺序写性能即可，一般意义上的 SAS 硬盘和 SSD 均能够很好地满足 TDengine 对于磁盘写入性能的要求。

### 计算资源估算

由于物联网数据的特殊性，数据产生的频率固定以后，TDengine 写入的过程对于（计算和存储）资源消耗都保持一个相对固定的量。《[TDengine 运维指南](/operation/)》上的描述，该系统中每秒 22000 个写入，消耗 CPU 不到 1 个核。

在针对查询所需要消耗的 CPU 资源的估算上，假设应用要求数据库提供的 QPS 为 10000，每次查询消耗的 CPU 时间约 1 ms，那么每个核每秒提供的查询为 1000 QPS，满足 10000 QPS 的查询请求，至少需要 10 个核。为了让系统整体上 CPU 负载小于 50%，整个集群需要 10 个核的两倍，即 20 个核。

### 内存资源估算

数据库默认为每个 Vnode 分配内存 16MB\*3 缓冲区，集群系统包括 22 个 CPU 核，则默认会建立 22 个虚拟节点 Vnode，每个 Vnode 包含 1000 张表，则可以容纳所有的表。则约 1 个半小时写满一个 block，从而触发落盘，可以不做调整。22 个 Vnode 共计需要内存缓存约 1GB。考虑到查询所需要的内存，假设每次查询的内存开销约 50MB，则 500 个查询并发需要的内存约 25GB。

综上所述，可使用单台 16 核 32GB 的机器，或者使用 2 台 8 核 16GB 机器构成的集群。

## 附录 3: 超级表名称

由于 OpenTSDB 的 metric 名称中带有点号（“.”），例如“cpu.usage_user”这种名称的 metric。但是点号在 TDengine 中具有特殊含义，是用来分隔数据库和表名称的分隔符。TDengine 也提供转义符，以允许用户在（超级）表名称中使用关键词或特殊分隔符（如：点号）。为了使用特殊字符，需要采用转义字符将表的名称括起来，例如：`cpu.usage_user`这样就是合法的（超级）表名称。

## 附录 4：参考文章

1. [使用 TDengine + collectd/StatsD + Grafana 快速搭建 IT 运维监控系统](../collectd/)
2. [通过 collectd 将采集数据直接写入 TDengine](../../third-party/collectd/)
