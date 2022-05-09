---
sidebar_label: OpenTSDB Migration to TDengine
title: Best Practices for Migrating OpenTSDB Applications to TDengine
---

As a distributed, scalable, HBase-based distributed temporal database system, OpenTSDB has been introduced and widely used in the field of operation and monitoring by people in DevOps thanks to its first-mover advantage. However, in recent years, with the rapid development of new technologies such as cloud computing, microservices and containerization, enterprise-level services are becoming more and more diverse and the architecture is becoming more and more complex.
With the rapid development of cloud computing, microservices, containerization and other new technologies, enterprise-class services are becoming more and more complex and the architecture is becoming more and more complex. From this situation, the use of OpenTSDB as a DevOps backend storage for monitoring is increasingly plagued by performance issues and delayed feature upgrades, as well as the resulting increase in application deployment costs and reduced operational efficiency.
These problems are becoming increasingly serious as the system scales up.

In this context, in order to meet the fast-growing IoT big data market and technical needs, TAOSData has developed an innovative big data processing product TDengine after learning the advantages of many traditional relational databases, NoSQL databases, stream computing engines, message queues, etc. TDengine has its own unique advantages in temporal big data processing.
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

In order to facilitate the migration of historical data, we provide a plug-in for the data synchronization tool DataX, which can automatically write data into TDengine. It should be noted that the automatic data migration of DataX can only support the data migration process of single value model.

For the specific usage of DataX and how to use DataX to write data to TDengine, please refer to [DataX-based TDeninge Data Migration Tool](https://www.taosdata.com/blog/2021/10/26/3156.html).

After migrating DataX, we found that by starting multiple processes and migrating multiple metrics at the same time, the efficiency of migrating historical data can be greatly improved. The following are some records of the migration process, I hope these can be used for application migration. Work brings references.

| Number of datax instances (number of concurrent processes) | Migration record speed (pieces/second) |
| ----------------------------- | ------------------- -- |
| 1 | About 139,000 |
| 2 | About 218,000 |
| 3 | About 249,000 |
| 5 | About 295,000 |
| 10 | About 330,000 |

<br/> (Note: The test data comes from a single-node Intel(R) Core(TM) i7-10700 CPU@2.90GHz 16-core 64G hardware device, the channel and batchSize are 8 and 1000 respectively, and each record contains 10 tags )

### 2. Manual data migration

If you need to use the multi-value model for data writing, you need to develop a tool to export data from OpenTSDB, and then confirm which timelines can be merged and imported into the same timeline, and then pass the time that can be imported at the same time through the SQL statement. written to the database.

Manual migration of data requires attention to the following two issues:

1) When storing the exported data in the disk, the disk needs to have enough storage space to be able to fully accommodate the exported data files. In order to avoid the shortage of disk file storage after the full amount of data is exported, the partial import mode can be adopted, and the timelines belonging to the same super table are preferentially exported, and then the exported data files are imported into the TDengine system
middle.

2) Under the full load of the system, if there are enough remaining computing and IO resources, a multi-threaded import mechanism can be established to maximize the efficiency of data migration. Considering the huge load that data parsing brings to the CPU, it is necessary to control the maximum number of parallel tasks to avoid the overall overload of the system triggered by importing historical data.

Due to the ease of operation of TDengine itself, there is no need to perform index maintenance and data format change processing in the entire process. The entire process only needs to be executed sequentially.

When the historical data is completely imported into TDengine, the two systems are running at the same time, and then the query request can be switched to TDengine to achieve seamless application switching.

## Appendix 1: OpenTSDB query function correspondence table

**Avg**

Equivalent function: avg

Example:

SELECT avg(val) FROM (SELECT first(val) FROM super_table WHERE ts >= startTime and ts <= endTime INTERVAL(20s) Fill(linear)) INTERVAL(20s)

Remark:

1. The value in Interval needs to be the same as the interval value in the outer query.
2. The interpolation processing in TDengine needs to use subqueries to assist in the completion. As shown above, it is enough to specify the interpolation type in the inner query. Since the interpolation of the values ​​in OpenTSDB uses linear interpolation, use fill( in the interpolation clause. linear) to declare the interpolation type. The following functions with the same interpolation calculation requirements are processed by this method.
3. The parameter 20s in Interval indicates that the inner query will generate results according to a time window of 20 seconds. In a real query, it needs to be adjusted to the time interval between different records. This ensures that interpolation results are produced equivalent to the original data.
4. Due to the special interpolation strategy and mechanism of OpenTSDB, the method of first interpolation and then calculation in the aggregate query (Aggregate) makes the calculation results impossible to be completely consistent with TDengine. But in the case of downsampling (Downsample), TDengine and OpenTSDB can obtain consistent results (due to the fact that OpenTSDB performs aggregation and downsampling queries).
uses a completely different interpolation strategy).

**Count**

Equivalent function: count

Example:

select count(\*) from super_table_name;

**Dev**

Equivalent function: stddev

Example:

Select stddev(val) from table_name

**Estimated percentiles**

Equivalent function: apercentile

Example:

Select apercentile(col1, 50, “t-digest”) from table_name

Remark:

1. During the approximate query processing, OpenTSDB uses the t-digest algorithm by default, so in order to obtain the same calculation result, the algorithm used needs to be specified in the apercentile function. TDengine can support two different approximation processing algorithms, declared by "default" and "t-digest" respectively.

**First**

Equivalent function: first

Example:

Select first(col1) from table_name

**Last**

Equivalent function: last

Example:

Select last(col1) from table_name

**Max**

Equivalent function: max

Example:

Select max(value) from (select first(val) value from table_name interval(10s) fill(linear)) interval(10s)

Note: The Max function requires interpolation for the reasons described above.

**Min**

Equivalent function: min

Example:

Select min(value) from (select first(val) value from table_name interval(10s) fill(linear)) interval(10s);

**MinMax**

Equivalent function: max

Select max(val) from table_name

Note: This function has no interpolation requirements, so it can be directly calculated.

**MimMin**

Equivalent function: min

Select min(val) from table_name

Note: This function has no interpolation requirements, so it can be directly calculated.

**Percentile**

Equivalent function: percentile

Remark:

**Sum**

Equivalent function: sum

Select max(value) from (select first(val) value from table_name interval(10s) fill(linear)) interval(10s)

Note: This function has no interpolation requirements, so it can be directly calculated.

**Zimsum**

Equivalent function: sum

Select sum(val) from table_name

Note: This function has no interpolation requirements, so it can be directly calculated.

Complete example:

````json
// OpenTSDB query JSON
query = {
"start": 1510560000,
"end": 1515000009,
"queries": [{
"aggregator": "count",
"metric": "cpu.usage_user",
}]
}

// Equivalent query SQL:
SELECT count(*)
FROM `cpu.usage_user`
WHERE ts>=1510560000 AND ts<=1515000009
````

## Appendix 2: Resource Estimation Methodology

### Data generation environment

We still use the hypothetical environment from Chapter 4, 3 measurements. Respectively: the data writing rate of temperature and humidity is one record every 5 seconds, and the timeline is 100,000. The writing rate of air quality is one record every 10 seconds, the timeline is 10,000, and the query request frequency is 500 QPS.

### Storage resource estimation

Assuming that the number of sensor devices that generate data and need to be stored is `n`, the frequency of data generation is `t` per second, and the length of each record is `L` bytes, the scale of data generated per day is `n×t× L` bytes. Assuming the compression ratio is C, the daily data size is `(n×t×L)/C` bytes. The storage resources are estimated to be able to accommodate 1.5 years of data scale. In the production environment, the compression ratio C of TDengine is generally between 5 and 7.
Adding 20% ​​redundancy, the required storage resources can be calculated:

```matlab
(n×t×L)×(365×1.5)×(1+20%)/C
````

Combined with the above calculation formula, the parameters are brought into the calculation formula, and the raw data scale generated every year is 11.8TB without considering the label information. It should be noted that since tag information is associated with each timeline in TDengine, not every record. Therefore, the scale of the amount of data to be recorded is somewhat reduced relative to the generated data, and this part of the label data can be ignored as a whole. Assuming a compression ratio of 5, the size of the retained data ends up being 2.56 TB.

### Storage Device Selection Considerations

The hard disk should be a hard disk device with better random read performance. If an SSD is available, consider using an SSD as much as possible. A disk with better random read performance is of great help in improving the query performance of the system, and can improve the query response performance of the system as a whole. In order to obtain better query performance, the performance index of the single-threaded random read IOPS of the hard disk device should not be lower than 1000, and it is better to reach 5000 IOPS or more. To get the current device random read
To evaluate the IO performance obtained, it is recommended to use fio software to evaluate its running performance (please refer to Appendix 1 for specific usage) to confirm whether it can meet the performance requirements of random read of large files.

Hard disk write performance has little effect on TDengine. The TDengine writing process adopts the additional write mode, so as long as it has good sequential write performance, both SAS hard disks and SSDs in the general sense can well meet TDengine's requirements for disk write performance.

### Computational resource estimates

Due to the particularity of IoT data, after the frequency of data generation is fixed, the writing process of TDengine maintains a relatively fixed amount of resource consumption (computing and storage). According to the description in the [TDengine Operation and Maintenance Guide](/operation/), the system consumes less than 1 CPU core at 22,000 writes per second.

In the estimation of the CPU resources consumed by the query, assuming that the application requires the database to provide 10,000 QPS, and the CPU time consumed by each query is about 1 ms, then the query provided by each core per second is 1,000 QPS, which satisfies 10,000 QPS. The query request requires at least 10 cores. In order for the system as a whole to have less than 50% CPU load, the entire cluster needs twice as many as 10 cores, or 20 cores.

### Memory resource estimation

The database allocates 16MB\*3 buffer memory for each Vnode by default. If the cluster system includes 22 CPU cores, 22 virtual node Vnodes will be created by default. Each Vnode contains 1000 tables, which can accommodate all the tables. Then it takes about 1.5 hours to write a block, which triggers the drop, and no adjustment is required. A total of 22 Vnodes require about 1GB of memory cache. Considering the memory required by the query, assuming that the memory overhead of each query is about 50MB, the memory required for 500 queries concurrently is about 25GB.

In summary, a single 16-core 32GB machine can be used, or a cluster of 2 8-core 16GB machines can be used.

## Appendix 3: Cluster Deployment and Startup

TDengine provides a wealth of help documents to explain many aspects of cluster installation and deployment. Here, the corresponding document index is provided for your reference.

### Cluster Deployment

The first is to install TDengine, download the latest stable version of TDengine from the official website, unzip it and run install.sh to install it. Please refer to the blog ["Installation and Uninstallation of Various Installation Packages of TDengine"](https://www.taosdata.com/blog/2019/08/09/566.html) for the help of various installation packages.

Note that after the installation is complete, do not start the taosd service immediately, and start the taosd service after the parameters are correctly configured.

### Set running parameters and start the service

To ensure that the system can obtain the necessary information for normal operation. Please set the following key parameters correctly on the server:

FQDN, firstEp, secondEP, dataDir, logDir, tmpDir, serverPort. For the specific meaning and setting requirements of each parameter, please refer to the document "[TDengine Cluster Installation and Management](/cluster/)"

Follow the same steps to set parameters on the nodes that need to be running, start the taosd service, and then add Dnodes to the cluster.

Finally, start taos and execute the command `show dnodes`. If you can see all the nodes that have joined the cluster, the cluster is successfully built. For specific operation procedures and precautions, please refer to the document "[TDengine Cluster Installation and Management](/cluster/)"

## Appendix 4: Super Table Names

Since OpenTSDB's metric name has a dot (".") in it, for example, a metric with a name like "cpu.usage_user". But the dot has a special meaning in TDengine and is a separator used to separate database and table names. TDengine also provides escape characters to allow users to use keywords or special separators (eg dots) in (super)table names. In order to use special characters, the table name needs to be enclosed in escape characters, eg: `cpu.usage_user` This is a valid (super) table name.

## Appendix 5: Reference Articles

1. [Using TDengine + collectd/StatsD + Grafana to quickly build an IT operation and maintenance monitoring system](/application/collectd/)
2. [Write collected data directly to TDengine through collectd](/third-party/collectd/)
