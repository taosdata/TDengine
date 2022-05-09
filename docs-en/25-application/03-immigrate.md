---
sidebar_label: OpenTSDB Migration to TDengine
title: Best Practices for Migrating OpenTSDB Applications to TDengine
---

As a distributed, scalable, HBase-based distributed temporal database system, OpenTSDB has been introduced and widely used in the field of operation and monitoring by people in DevOps thanks to its first-mover advantage. However, in recent years, with the rapid development of new technologies such as cloud computing, microservices and containerization, enterprise-level services are becoming more and more diverse and the architecture is becoming more and more complex.
With the rapid development of cloud computing, microservices, containerization and other new technologies, enterprise-class services are becoming more and more complex and the architecture is becoming more and more complex. From this situation, the use of OpenTSDB as a DevOps backend storage for monitoring is increasingly plagued by performance issues and delayed feature upgrades, as well as the resulting increase in application deployment costs and reduced operational efficiency.
These problems are becoming increasingly serious as the system scales up.

In this context, in order to meet the fast-growing IoT big data market and technical needs, TAOS Data has developed an innovative big data processing product TDengine after learning the advantages of many traditional relational databases, NoSQL databases, stream computing engines, message queues, etc. TDengine has its own unique advantages in temporal big data processing.
TDengine can effectively solve the problems currently encountered by OpenTSDB.

Compared with OpenTSDB, TDengine has the following distinctive features.

- Performance of data writing and querying far exceeds that of OpenTSDB.
- Efficient compression mechanism for time-series data, which compresses less than 1/5 of the storage space on disk.
- The installation and deployment is very simple, a single installation package to complete the installation and deployment, does not rely on other third-party software, the entire installation and deployment process in seconds;
- The built-in functions provided cover all query functions supported by OpenTSDB, and also support more time-series data query functions, scalar functions and aggregation functions, and support advanced query functions such as multiple time-window aggregation, join query, expression operation, multiple group aggregation, user-defined sorting, and user-defined functions. Adopting SQL-like syntax rules, it is simpler
Basically, there is no learning cost.
- Supports up to 128 tags, with a total tag length of 16 KB.
- In addition to HTTP, it also provides interfaces to Java, Python, C, Rust, Go, and other languages, and supports a variety of enterprise-class standard connector protocols such as JDBC.

If we migrate the applications originally running on OpenTSDB to TDengine, we can not only effectively reduce the compute and storage resource consumption and the scale of deployed servers, but also greatly reduce the output of operation and maintenance costs, making operation and maintenance management simpler and easier, and significantly reducing the total cost of ownership. Like OpenTSDB, TDengine is also
has been open sourced, but the difference is that in addition to the stand-alone version, the latter has also realized the cluster version open source, and the concern of being bound by the vendor has been removed.

In the following we will explain how to migrate OpenTSDB applications to TDengine quickly, securely and reliably without coding, using the most typical and widely used DevOps scenarios. Subsequent chapters will go into more depth to facilitate migration for non-DevOps scenarios.

## DevOps Application Quick Migration

### 1. Typical Application Scenarios

The overall architecture of the system for a typical DevOps application scenario is shown in the following figure (Figure 1).

**Figure 1. Typical architecture in a DevOps scenario**
Figure 1. [IT-DevOps-Solutions-Immigrate-OpenTSDB-Arch](/img/IT-DevOps-Solutions-Immigrate-OpenTSDB-Arch.jpg "Figure 1. Typical architecture in a DevOps scenario")

In this application scenario, there are Agent tools deployed in the application environment to collect machine metrics, network metrics, and application metrics, data collectors to aggregate information collected by agents, systems for persistent data storage and management, and tools for monitoring data visualization (e.g., Grafana, etc.).

The agents deployed in the application nodes are responsible for providing operational metrics from different sources to collectd/Statsd, and collectd/StatsD is responsible for pushing the aggregated data to the OpenTSDB cluster system and then visualizing the data using the visualization kanban board Grafana.

### 2. Migration Services

- **TDengine installation and deployment**

First of all, TDengine should be installed. Download the latest stable version of TDengine from the official website, unzip it and run install.sh to install it. For help on using various installation packages, please refer to the blog ["Installation and Uninstallation of TDengine Multiple Installation Packages"](https://www.taosdata.com/blog/2019/08/09/566.html).

Note that after the installation is complete, do not start the taosd service immediately, but after the parameters are properly configured.

- **Adjusting the data collector configuration**

In TDengine version 2.3, we released taosAdapter, a stateless, rapidly elastic and scalable component that is compatible with Influxdb's Line Protocol and OpenTSDB's telnet/JSON writing protocol specification, providing rich data access capabilities that effectively saves user migration costs and reduces the difficulty of user migration.
Migration difficulty.

Users can flexibly deploy taosAdapter instances according to their needs to rapidly improve the throughput of data writes in conjunction with the needs of scenarios and provide guarantees for data writes in different application scenarios.

Through taosAdapter, users can directly push the data collected by collectd and StatsD to TDengine to achieve seamless migration of application scenarios, which is very easy and convenient. taosAdapter also supports Telegraf, Icinga, TCollector, node_exporter data For more details, please refer to [taosAdapter](/reference/taosadapter/).

If using collectd, modify the configuration file in its default location `/etc/collectd/collectd.conf` to point to the IP address and port of the node where taosAdapter is deployed. Assuming the taosAdapter IP address is 192.168.1.130 and port 6046, configure it as follows.

```html
LoadPlugin write_tsdb
<Plugin write_tsdb>
  <Node>
    Host "192.168.1.130" Port "6046" HostTags "status=production" StoreRates
    false AlwaysAppendDS false
  </Node>
</Plugin>
```

That is, you can have collectd push the data to taosAdapter using the push to OpenTSDB plugin. taosAdapter will call the API to write the data to taosd, thus completing the writing of the data. If you are using StatsD adjust the profile information accordingly.

- **Tuning the Dashborad system**

After the data can be written to TDengine properly, you can adapt Grafana to visualize the data written to TDengine. To obtain and use the Grafana plug-in provided by TDengine, please refer to [Links to other tools](/third-party/grafana).

TDengine provides two sets of Dashboard templates by default, and users only need to import the templates from the Grafana directory into Grafana to activate their use.

**Importing Grafana Templates** Figure 2.
! [](/img/IT-DevOps-Solutions-Immigrate-OpenTSDB-Dashboard.jpg "Figure 2. Importing a Grafana Template")

After the above steps, the migration to replace OpenTSDB with TDengine is completed. You can see that the whole process is very simple, no code needs to be written, only some configuration files need to be adjusted to complete the migration work.

### 3. Post-migration architecture

After the migration is completed, the overall architecture of the system is shown in the figure below (Figure 3), and the whole process of the acquisition side, the data writing side, and the monitoring and presentation side are all kept stable, except for a few configuration adjustments, which do not involve any important changes or alterations. OpenTSDB to TDengine migration action, using TDengine more powerful processing power and query performance.

In most DevOps scenarios, if you have a small OpenTSDB cluster (3 or fewer nodes) as the storage side of DevOps and rely on OpenTSDB to provide data storage and query capabilities for the system persistence layer, you can safely replace it with TDengine and save more compute and storage resources. With the same compute
resource allocation, a single TDengine can meet the service capacity provided by 3 to 5 OpenTSDB nodes. If the scale is larger, then TDengine clustering is required.

If your application is particularly complex, or the application domain is not a DevOps scenario, you can continue reading subsequent chapters for a more comprehensive and in-depth look at the advanced topics of migrating an OpenTSDB application to TDengine.

**Figure 3. System architecture after migration**
! [IT-DevOps-Solutions-Immigrate-TDengine-Arch](/img/IT-DevOps-Solutions-Immigrate-TDengine-Arch.jpg "Figure 3. System architecture after migration completion")

## Migration evaluation and strategy for other scenarios

### 1. Differences between TDengine and OpenTSDB

This chapter describes in detail the differences between OpenTSDB and TDengine at the system functionality level. After reading this chapter, you can fully evaluate whether you can migrate some complex OpenTSDB-based applications to TDengine, and what you should pay attention to after migration.

TDengine currently only supports visual kanban rendering for Grafana, so if your application uses front-end kanban boards other than Grafana (e.g., [TSDash](https://github.com/facebook/tsdash), [Status Wolf](https://github) .com/box/StatusWolf), etc.), then the front-end kanban cannot be directly migrated to TDengine, and the front-end kanban will need to be re-adapted to Grafana in order to work properly.

As of version 2.3.0.x, TDengine can only support collectd and StatsD as data collection aggregation software, but more data collection aggregation software will be provided in the future. If you use other types of data aggregators on the collection side, your application needs to be adapted to these two data aggregation systems in order to write data properly.
. In addition to the two data aggregator software protocols mentioned above, TDengine also supports writing data directly via InfluxDB's row protocol and OpenTSDB's data writing protocol, JSON format, and you can rewrite the logic on the data push side to write data using the row protocols supported by TDengine.

In addition, if your application uses the following features of OpenTSDB, you need to understand the following considerations before migrating your application to TDengine.

1. ` /api/stats`: If your application uses this feature to monitor the service status of OpenTSDB, and you have built the relevant logic to link the processing in your application, then this part of the status reading and fetching logic needs to be re-adapted to TDengine, which provides a new mechanism to handle cluster status monitoring to meet your application's monitoring of its monitoring
TDengine provides a new mechanism for handling cluster state monitoring to meet the monitoring and maintenance needs of your application.
2. `/api/tree`: If you rely on this feature of OpenTSDB for the hierarchical organization and maintenance of timelines, you cannot migrate it directly to TDengine, which uses a database -> supertable -> sub-table hierarchy to organize and maintain timelines, with all timelines belonging to the same supertable in the system same hierarchy, but
It is possible to simulate a logical multi-level structure of the application through the special construction of different tag values.
3. `Rollup And PreAggregates`: The use of Rollup and PreAggregates requires the application to decide where to access the Rollup results and in some scenarios to access the original results. The opacity of this structure makes the application processing logic extremely complex and not at all portable. We see this strategy as a compromise when the temporal database does not
TDengine does not support automatic downsampling of multiple timelines and preaggregation (for a range of time periods) for the time being, but thanks to its high-performance query processing logic, it can provide very high-performance query responses even without relying on Rollup and preaggregation (for a range of time periods), and make your application query processing logic much simpler.
The logic is much simpler.
4. `Rate`: TDengine provides two functions to calculate the rate of change of values, namely Derivative (the result is consistent with the Derivative behavior of InfluxDB) and IRate (the result is consistent with the IRate function in Prometheus). However, the results of these two functions are slightly different from Rate, but overall
The functions are more powerful overall. In addition, TDengine supports all the calculation functions provided by OpenTSDB, and TDengine's query functions are much more powerful than those supported by OpenTSDB, which can greatly simplify the processing logic of your application.

Through the above introduction, I believe you should be able to understand the changes brought about by the migration of OpenTSDB to TDengine, and this information will also help you correctly determine whether it is acceptable to migrate your application to TDengine and experience the powerful temporal data processing capability and convenient usage experience provided by TDengine.

### 2. Migration strategy

First of all, the OpenTSDB-based system will be migrated involving data schema design, system scale estimation, data write end transformation, data streaming and application adaptation; after that, the two systems will run in parallel for a period of time, and then the historical data will be migrated to TDengine. Of course, if your application has some functions that strongly depend on the above OpenTSDB features and you do not want to stop using them, you can migrate the historical data to TDengine.
You can consider keeping the original OpenTSDB system running while starting TDengine to provide the main services.

## Data model design

On the one hand, TDengine requires a strict schema definition for its incoming data. On the other hand, the data model of TDengine is richer than that of OpenTSDB, and the multi-valued model is compatible with all single-valued model building requirements.

Let us now assume a DevOps scenario where we use collectd to collect the underlying metrics of the device, including memory, swap, disk, etc. The schema in OpenTSDB is as follows

On the one hand, TDengine requires a strict schema definition for its incoming data. On the other hand, the data model of TDengine is richer than that of OpenTSDB, and the multi-value model is compatible with all single-value model building requirements.

Let us now assume a DevOps scenario where we use collectd to collect the underlying metrics of the device, including several metrics such as memory, swap, disk, etc. The model in OpenTSDB is as follows.

| metric | value name | type | tag1 | tag2 | tag3 | tag4 | tag5 |
| ---- | -------------- | ------ | ------ | ---- | ----------- | -------------------- | --------- | ------ |
| 1 | memory | value | double | host | memory_type | memory_type_instance | source | n/a |
| 2 | swap | value | double | host | swap_type | swap_type_instance | source | n/a |
| 3 | disk | value | double | host | disk_point | disk_instance | disk_type | source |

TDengine requires the data stored to have a data schema, i.e., you need to create a super table and specify the schema of the super table before writing the data. For data schema creation, you have two ways to do this: 1) Take advantage of TDengine's native data writing support for OpenTSDB by calling the TDengine API to write (text line or JSON format)
and automate the creation of single-value models. This approach does not require major adjustments to the data writing application, nor does it require conversion of the written data format.

At the C level, TDengine provides the taos_insert_lines() function to write data in OpenTSDB format directly (in version 2.3.x this function corresponds to taos_schemaless_insert()). For code reference examples, please refer to the sample code schemaless.c in the installation package directory.

(2) On the basis of full understanding of TDengine's data model, establish the mapping relationship between OpenTSDB and TDengine's data model adjustment manually, taking into account that OpenTSDB is a single-value mapping model, it is recommended to use the single-value model in TDengine. TDengine can support both multi-value and single-value models.

- **Single-valued model**.

The steps are as follows: the name of the metrics is used as the name of the TDengine super table, which is built with two basic data columns - timestamp and value, and the label of the super table is equivalent to the label information of the metrics, and the number of labels is equal to the number of labels of the metrics. The names of the sub-tables are named using a fixed rule
The names of sub-tables are named with fixed rules: `metric + '_' + tags1_value + '_' + tag2_value + '_' + tag3_value ... ` as the sub-table name.l

Create 3 super tables in TDengine.

```sql
create stable memory(ts timestamp, val float) tags(host binary(12), memory_type binary(20), memory_type_instance binary(20), source binary(20)) ;
create stable swap(ts timestamp, val double) tags(host binary(12), swap_type binary(20), swap_type_binary binary(20), source binary(20));
create stable disk(ts timestamp, val double) tags(host binary(12), disk_point binary(20), disk_instance binary(20), disk_type binary(20), source binary(20));
```

For sub-tables use dynamic table creation as shown below.

```sql
insert into memory_vm130_memory_bufferred_collectd using memory tags('vm130', 'memory', ' buffer', 'collectd') values(1632979445, 3.0656);
```

The final system will have about 340 sub-tables and 3 super-tables. Note that if the use of concatenated tagged values causes the sub-table names to exceed the system limit (191 bytes), then some encoding (e.g. MD5) needs to be used to convert them to an acceptable length.

- **Multi-value model**

If you want to take advantage of TDengine's multi-value modeling capabilities, you need to first meet the requirements that different collection quantities have the same collection frequency and can reach the **data write side simultaneously via a message queue**, thus ensuring that multiple metrics are written at once using SQL statements. The name of the metric is used as the name of the super table to create a multi-column model with the same collection frequency and the ability to
The name of the metric is used as the name of the super table to create a multi-column model of data that has the same collection frequency and can arrive simultaneously. The names of the sub-tables are named using a fixed rule. Each of the above metrics contains only one measurement value, so it is not possible to convert it into a multi-value model.

## Data triage and application adaptation

Subscribe data from the message queue and start the adapted writer to write the data.

After the data starts to be written for a period of time, you can use SQL statements to check whether the amount of data written meets the expected writing requirements. The following SQL statement is used to count the amount of data.

```sql
select count(*) from memory
```

After completing the query, if the data written does not differ from what is expected and there are no abnormal error messages from the writing program itself, then you can confirm that the data write is complete and valid.

TDengine does not support querying or data fetching using the OpenTSDB query syntax, but does provide support for each of the OpenTSDB queries. The corresponding query processing can be adapted and applied in a manner that can be obtained by examining Appendix 1, or for a full understanding of the types of queries supported by TDengine, refer to the TDengine user manual
For a full understanding of the types of queries supported by TDengine, refer to the TDengine user manual.

TDengine supports the standard JDBC 3.0 interface for manipulating databases, but you can also use other types of high-level language connectors for querying and reading data to suit your application. See also the user manual for specific operation and usage help.

## Historical Data Migration

### 1. Use the tool to migrate data automatically

为了方便历史数据的迁移工作，我们为数据同步工具 DataX 提供了插件，能够将数据自动写入到 TDengine 中，需要注意的是 DataX 的自动化数据迁移只能够支持单值模型的数据迁移过程。

DataX 具体的使用方式及如何使用 DataX 将数据写入 TDengine 请参见[基于 DataX 的 TDeninge 数据迁移工具](https://www.taosdata.com/blog/2021/10/26/3156.html)。

在对 DataX 进行迁移实践后，我们发现通过启动多个进程，同时迁移多个 metric 的方式，可以大幅度的提高迁移历史数据的效率，下面是迁移过程中的部分记录，希望这些能为应用迁移工作带来参考。

| datax 实例个数 (并发进程个数) | 迁移记录速度 （条/秒) |
| ----------------------------- | --------------------- |
| 1                             | 约 13.9 万            |
| 2                             | 约 21.8 万            |
| 3                             | 约 24.9 万            |
| 5                             | 约 29.5 万            |
| 10                            | 约 33 万              |

<br/>（注：测试数据源自 单节点 Intel(R) Core(TM) i7-10700 CPU@2.90GHz 16 核 64G 硬件设备，channel 和 batchSize 分别为 8 和 1000，每条记录包含 10 个 tag)

### 2、手动迁移数据

如果你需要使用多值模型进行数据写入，就需要自行开发一个将数据从 OpenTSDB 导出的工具，然后确认哪些时间线能够合并导入到同一个时间线，再将可以同时导入的时间通过 SQL 语句的写入到数据库中。

手动迁移数据需要注意以下两个问题：

1）在磁盘中存储导出数据时，磁盘需要有足够的存储空间以便能够充分容纳导出的数据文件。为了避免全量数据导出后导致磁盘文件存储紧张，可以采用部分导入的模式，对于归属于同一个超级表的时间线优先导出，然后将导出部分的数据文件导入到 TDengine 系统中。

2）在系统全负载运行下，如果有足够的剩余计算和 IO 资源，可以建立多线程的导入机制，最大限度地提升数据迁移的效率。考虑到数据解析对于 CPU 带来的巨大负载，需要控制最大的并行任务数量，以避免因导入历史数据而触发的系统整体过载。

由于 TDengine 本身操作简易性，所以不需要在整个过程中进行索引维护、数据格式的变化处理等工作，整个过程只需要顺序执行即可。

当历史数据完全导入到 TDengine 以后，此时两个系统处于同时运行的状态，之后便可以将查询请求切换到 TDengine 上，从而实现无缝的应用切换。

## 附录 1: OpenTSDB 查询函数对应表

**Avg**

等效函数：avg

示例：

SELECT avg(val) FROM (SELECT first(val) FROM super_table WHERE ts >= startTime and ts <= endTime INTERVAL(20s) Fill(linear)) INTERVAL(20s)

备注：

1. Interval 内的数值与外层查询的 interval 数值需要相同。
2. 在 TDengine 中插值处理需要使用子查询来协助完成，如上所示，在内层查询中指明插值类型即可，由于 OpenTSDB 中数值的插值使用了线性插值，因此在插值子句中使用 fill(linear) 来声明插值类型。以下有相同插值计算需求的函数，均采用该方法处理。
3. Interval 中参数 20s 表示将内层查询按照 20 秒一个时间窗口生成结果。在真实的查询中，需要调整为不同的记录之间的时间间隔。这样可确保等效于原始数据生成了插值结果。
4. 由于 OpenTSDB 特殊的插值策略和机制，聚合查询（Aggregate）中先插值再计算的方式导致其计算结果与 TDengine 不可能完全一致。但是在降采样（Downsample）的情况下，TDengine 和 OpenTSDB 能够获得一致的结果（由于 OpenTSDB 在聚合查询和降采样查询中采用了完全不同的插值策略）。

**Count**

等效函数：count

示例：

select count(\*) from super_table_name;

**Dev**

等效函数：stddev

示例：

Select stddev(val) from table_name

**Estimated percentiles**

等效函数：apercentile

示例：

Select apercentile(col1, 50, “t-digest”) from table_name

备注：

1. 近似查询处理过程中，OpenTSDB 默认采用 t-digest 算法，所以为了获得相同的计算结果，需要在 apercentile 函数中指明使用的算法。TDengine 能够支持两种不同的近似处理算法，分别通过“default”和“t-digest”来声明。

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

备注：Max 函数需要插值，原因见上。

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

假设产生数据并需要存储的传感器设备数量为 `n`，数据生成的频率为`t`条/秒，每条记录的长度为 `L` bytes，则每天产生的数据规模为 `n×t×L` bytes。假设压缩比为 C，则每日产生数据规模为 `(n×t×L)/C` bytes。存储资源预估为能够容纳 1.5 年的数据规模，生产环境下 TDengine 的压缩比 C 一般在 5 ~ 7 之间，同时为最后结果增加 20% 的冗余，可计算得到需要存储资源：

```matlab
(n×t×L)×(365×1.5)×(1+20%)/C
```

结合以上的计算公式，将参数带入计算公式，在不考虑标签信息的情况下，每年产生的原始数据规模是 11.8TB。需要注意的是，由于标签信息在 TDengine 中关联到每个时间线，并不是每条记录。所以需要记录的数据量规模相对于产生的数据有一定的降低，而这部分标签数据整体上可以忽略不记。假设压缩比为 5，则保留的数据规模最终为 2.56 TB。

### 存储设备选型考虑

硬盘应该选用具有较好随机读性能的硬盘设备，如果能够有 SSD，尽可能考虑使用 SSD。较好的随机读性能的磁盘对于提升系统查询性能具有极大的帮助，能够整体上提升系统的查询响应性能。为了获得较好的查询性能，硬盘设备的单线程随机读 IOPS 的性能指标不应该低于 1000，能够达到 5000 IOPS 以上为佳。为了获得当前的设备随机读取的 IO 性能的评估，建议使用 fio 软件对其进行运行性能评估（具体的使用方式请参阅附录 1），确认其是否能够满足大文件随机读性能要求。

硬盘写性能对于 TDengine 的影响不大。TDengine 写入过程采用了追加写的模式，所以只要有较好的顺序写性能即可，一般意义上的 SAS 硬盘和 SSD 均能够很好地满足 TDengine 对于磁盘写入性能的要求。

### 计算资源估算

由于物联网数据的特殊性，数据产生的频率固定以后，TDengine 写入的过程对于（计算和存储）资源消耗都保持一个相对固定的量。《[TDengine 运维指南](/operation/)》上的描述，该系统中每秒 22000 个写入，消耗 CPU 不到 1 个核。

在针对查询所需要消耗的 CPU 资源的估算上，假设应用要求数据库提供的 QPS 为 10000，每次查询消耗的 CPU 时间约 1 ms，那么每个核每秒提供的查询为 1000 QPS，满足 10000 QPS 的查询请求，至少需要 10 个核。为了让系统整体上 CPU 负载小于 50%，整个集群需要 10 个核的两倍，即 20 个核。

### 内存资源估算

数据库默认为每个 Vnode 分配内存 16MB\*3 缓冲区，集群系统包括 22 个 CPU 核，则默认会建立 22 个虚拟节点 Vnode，每个 Vnode 包含 1000 张表，则可以容纳所有的表。则约 1 个半小时写满一个 block，从而触发落盘，可以不做调整。22 个 Vnode 共计需要内存缓存约 1GB。考虑到查询所需要的内存，假设每次查询的内存开销约 50MB，则 500 个查询并发需要的内存约 25GB。

综上所述，可使用单台 16 核 32GB 的机器，或者使用 2 台 8 核 16GB 机器构成的集群。

## 附录 3: 集群部署及启动

TDengine 提供了丰富的帮助文档说明集群安装、部署的诸多方面的内容，这里提供响应的文档索引，供你参考。

### 集群部署

首先是安装 TDengine，从官网上下载 TDengine 最新稳定版，解压缩后运行 install.sh 进行安装。各种安装包的使用帮助请参见博客[《TDengine 多种安装包的安装和卸载》](https://www.taosdata.com/blog/2019/08/09/566.html)。

注意安装完成以后，不要立即启动 taosd 服务，在正确配置完成参数以后才启动 taosd 服务。

### 设置运行参数并启动服务

为确保系统能够正常获取运行的必要信息。请在服务端正确设置以下关键参数：

FQDN、firstEp、secondEP、dataDir、logDir、tmpDir、serverPort。各参数的具体含义及设置的要求，可参见文档《[TDengine 集群安装、管理](/cluster/)》

按照相同的步骤，在需要运行的节点上设置参数，并启动 taosd 服务，然后添加 Dnode 到集群中。

最后启动 taos，执行命令 show dnodes，如果能看到所有的加入集群的节点，那么集群顺利搭建完成。具体的操作流程及注意事项，请参阅文档《[TDengine 集群安装、管理](/cluster/)》

## 附录 4: 超级表名称

由于 OpenTSDB 的 metric 名称中带有点号（“.“），例如“cpu.usage_user”这种名称的 metric。但是点号在 TDengine 中具有特殊含义，是用来分隔数据库和表名称的分隔符。TDengine 也提供转义符，以允许用户在（超级）表名称中使用关键词或特殊分隔符（如：点号）。为了使用特殊字符，需要采用转义字符将表的名称括起来，例如：`cpu.usage_user`这样就是合法的（超级）表名称。

## 附录 5：参考文章

1. [使用 TDengine + collectd/StatsD + Grafana 快速搭建 IT 运维监控系统](/application/collectd/)
2. [通过 collectd 将采集数据直接写入 TDengine](/third-party/collectd/)
