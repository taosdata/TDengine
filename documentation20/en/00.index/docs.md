# TDengine Documentation

TDengine is a highly efficient platform to store, query, and analyze time-series data. It is specially designed and optimized for IoT, Internet of Vehicles, Industrial IoT, IT Infrastructure and Application Monitoring, etc. It works like a relational database, such as MySQL, but you are strongly encouraged to read through the following documentation before you experience it, especially the [Data Model](../03.architecture/docs.md) and [Data Modeling](../04.model/docs.md) sections. In addition to this document, you should also download and read our technology white paper. For the older TDengine version 1.6 documentation, please click here.

## [TDengine Introduction](../01.evaluation/docs.md)

* [TDengine Introduction and Features](../01.evaluation/docs.md#intro)
* [TDengine Use Scenes](../01.evaluation/docs.md#scenes)
* [TDengine Performance Metrics and Verification](../01.evaluation/docs.md#)

## [Getting Started](../02.getting-started/docs.md)

- [Quickly Install](../02.getting-started/docs.md#install): install via source code/package / Docker within seconds
- [Easy to Launch](../02.getting-started/docs.md#start): start / stop TDengine with systemctl
- [Command-line](../02.getting-started/docs.md#console) : an easy way to access TDengine server
- [Experience Lightning Speed](../02.getting-started/docs.md#demo): running a demo, inserting/querying data to experience faster speed
- [List of Supported Platforms](../02.getting-started/docs.md#platforms): a list of platforms supported by TDengine server and client
- [Deploy to Kubernetes](https://taosdata.github.io/TDengine-Operator/en/index.html)：a detailed guide for TDengine deployment in Kubernetes environment

## [Overall Architecture](../03.architecture/docs.md)

- [Data Model](../03.architecture/docs.md#model): relational database model, but one table for one device with static tags
- [Cluster and Primary Logical Unit](../03.architecture/docs.md#cluster): Take advantage of NoSQL, support scale-out and high-reliability
- [Storage Model and Data Partitioning/Sharding](../03.architecture/docs.md#sharding): tag data will be separated from time-series data, segmented by vnode and time
- [Data Writing and Replication Process](../03.architecture/docs.md#replication): records received are written to WAL, cached, with acknowledgement is sent back to client, while supporting multi-replicas
- [Caching and Persistence](../03.architecture/docs.md#persistence): latest records are cached in memory, but are written in columnar format with an ultra-high compression ratio
- [Data Query](../03.architecture/docs.md#query): support various functions, time-axis aggregation, interpolation, and multi-table aggregation

## [Data Modeling](../04.model/docs.md)

- [Create a Library](../04.model/docs.md#create-db): create a library for all data collection points with similar features
- [Create a Super Table(STable)](../04.model/docs.md#create-stable): create a STable for all data collection points with the same type
- [Create a Table](../04.model/docs.md#create-table): use STable as the template, to create a table for each data collecting point

## [TAOS SQL](../12.taos-sql/docs.md)

- [Data Types](../12.taos-sql/docs.md#data-type): support timestamp, int, float, nchar, bool, and other types
- [Database Management](../12.taos-sql/docs.md#management): add, drop, check databases
- [Table Management](../12.taos-sql/docs.md#table): add, drop, check, alter tables
- [STable Management](../12.taos-sql/docs.md#super-table): add, drop, check, alter STables
- [Tag Management](../12.taos-sql/docs.md#tags): add, drop, alter tags
- [Inserting Records](../12.taos-sql/docs.md#insert): support to write single/multiple items per table, multiple items across tables, and support to write historical data
- [Data Query](../12.taos-sql/docs.md#select): support time segment, value filtering, sorting, manual paging of query results, etc
- [SQL Function](../12.taos-sql/docs.md#functions): support various aggregation functions, selection functions, and calculation functions, such as avg, min, diff, etc
- [Time Dimensions Aggregation](../12.taos-sql/docs.md#aggregation): aggregate and reduce the dimension after cutting table data by time segment
- [Boundary Restrictions](../12.taos-sql/docs.md#limitation): restrictions for the library, table, SQL, and others
- [Error Code](../12.taos-sql/01.error-code/docs.md): TDengine 2.0 error codes and corresponding decimal codes

## [Efficient Data Ingestion](../05.insert/docs.md)

- [SQL Ingestion](../05.insert/docs.md#sql): write one or multiple records into one or multiple tables via SQL insert command
- [Prometheus Ingestion](../05.insert/docs.md#prometheus): Configure Prometheus to write data directly without any code
- [Telegraf Ingestion](../05.insert/docs.md#telegraf): Configure Telegraf to write collected data directly without any code
- [EMQ X Broker](../05.insert/docs.md#emq): Configure EMQ X to write MQTT data directly without any code
- [HiveMQ Broker](../05.insert/docs.md#hivemq): Configure HiveMQ to write MQTT data directly without any code

## [Efficient Data Querying](../06.queries/docs.md)

- [Main Query Features](../06.queries/docs.md#queries): support various standard functions, setting filter conditions, and querying per time segment
- [Multi-table Aggregation Query](../06.queries/docs.md#aggregation): use STable and set tag filter conditions to perform efficient aggregation queries
- [Downsampling to Query Value](../06.queries/docs.md#sampling): aggregate data in successive time windows, support interpolation

## [Advanced Features](../07.advanced-features/docs.md)

- [Continuous Query](../07.advanced-features/docs.md#continuous-query): Based on sliding windows, the data stream is automatically queried and calculated at regular intervals
- [Data Publisher/Subscriber](../07.advanced-features/docs.md#subscribe): subscribe to the newly arrived data like a typical messaging system
- [Cache](../07.advanced-features/docs.md#cache): the newly arrived data of each device/table will always be cached
- [Alarm Monitoring](../07.advanced-features/docs.md#alert): automatically monitor out-of-threshold data, and actively push it based-on configuration rules

## [Connector](../08.connector/docs.md)

- [C/C++ Connector](../08.connector/docs.md#c-cpp): primary method to connect to TDengine server through libtaos client library
- [Java Connector(JDBC)]: driver for connecting to the server from Java applications using the JDBC API
- [Python Connector](../08.connector/docs.md#python): driver for connecting to TDengine server from Python applications
- [RESTful Connector](../08.connector/docs.md#restful): a simple way to interact with TDengine via HTTP
- [Go Connector](../08.connector/docs.md#go): driver for connecting to TDengine server from Go applications
- [Node.js Connector](../08.connector/docs.md#nodejs): driver for connecting to TDengine server from Node.js applications
- [C# Connector](../08.connector/docs.md#csharp): driver for connecting to TDengine server from C# applications
- [Windows Client](https://www.taosdata.com/blog/2019/07/26/514.html): compile your own Windows client, which is required by various connectors on the Windows environment

## [Connections with Other Tools](../09.connections/docs.md)

- [Grafana](../09.connections/docs.md#grafana): query the data saved in TDengine and provide visualization
- [Matlab](../09.connections/docs.md#matlab): access data stored in TDengine server via JDBC configured within Matlab
- [R](../09.connections/docs.md#r): access data stored in TDengine server via JDBC configured within R
- [IDEA Database](https://www.taosdata.com/blog/2020/08/27/1767.html): use TDengine visually through IDEA Database Management Tool

## [Installation and Management of TDengine Cluster](../10.cluster/docs.md)

- [Preparation](../10.cluster/docs.md#prepare): important considerations before deploying TDengine for production usage
- [Create Your First Node](../10.cluster/docs.md#node-one): simple to follow the quick setup
- [Create Subsequent Nodes](../10.cluster/docs.md#node-other): configure taos.cfg for new nodes to add more to the existing cluster
- [Node Management](../10.cluster/docs.md#management): add, delete, and check nodes in the cluster
- [High-availability of Vnode](../10.cluster/docs.md#high-availability): implement high-availability of Vnode through multi-replicas
- [Mnode Management](../10.cluster/docs.md#mnode): automatic system creation without any manual intervention
- [Load Balancing](../10.cluster/docs.md#load-balancing): automatically performed once the number of nodes or load changes
- [Offline Node Processing](../10.cluster/docs.md#offline): any node that offline for more than a certain period will be removed from the cluster
- [Arbitrator](../10.cluster/docs.md#arbitrator): used in the case of an even number of replicas to prevent split-brain

## [TDengine Operation and Maintenance](../11.administrator/docs.md)

- [Capacity Planning](../11.administrator/docs.md#planning): Estimating hardware resources based on scenarios
- [Fault Tolerance and Disaster Recovery](../11.administrator/docs.md#tolerance): set the correct WAL and number of data replicas
- [System Configuration](../11.administrator/docs.md#config): port, cache size, file block size, and other system configurations
- [User Management](../11.administrator/docs.md#user): add/delete TDengine users, modify user password
- [Import Data](../11.administrator/docs.md#import): import data into TDengine from either script or CSV file
- [Export Data](../11.administrator/docs.md#export): export data either from TDengine shell or from the taosdump tool
- [System Monitor](../11.administrator/docs.md#status): monitor the system connections, queries, streaming calculation, logs, and events
- [File Directory Structure](../11.administrator/docs.md#directories): directories where TDengine data files and configuration files located
- [Parameter Restrictions and Reserved Keywords](../11.administrator/docs.md#keywords): TDengine’s list of parameter restrictions and reserved keywords

## TDengine Technical Design

- [System Module]: taosd functions and modules partitioning
- [Data Replication]: support real-time synchronous/asynchronous replication, to ensure high-availability of the system
- [Technical Blog](https://www.taosdata.com/cn/blog/?categories=3): More technical analysis and architecture design articles

## Common Tools

- [TDengine sample import tools](https://www.taosdata.com/blog/2020/01/18/1166.html)
- [TDengine performance comparison test tools](https://www.taosdata.com/blog/2020/01/18/1166.html)
- [Use TDengine visually through IDEA Database Management Tool](https://www.taosdata.com/blog/2020/08/27/1767.html)

## Performance: TDengine vs Others

- [Performance: TDengine vs InfluxDB with InfluxDB’s open-source performance testing tool](https://www.taosdata.com/blog/2020/01/13/1105.html)
- [Performance: TDengine vs OpenTSDB](https://www.taosdata.com/blog/2019/08/21/621.html)
- [Performance: TDengine vs Cassandra](https://www.taosdata.com/blog/2019/08/14/573.html)
- [Performance: TDengine vs InfluxDB](https://www.taosdata.com/blog/2019/07/19/419.html)
- [Performance Test Reports of TDengine vs InfluxDB/OpenTSDB/Cassandra/MySQL/ClickHouse](https://www.taosdata.com/downloads/TDengine_Testing_Report_cn.pdf)

## More on IoT Big Data

- [Characteristics of IoT and Industry Internet Big Data](https://www.taosdata.com/blog/2019/07/09/characteristics-of-iot-big-data/)
- [Features and Functions of IoT Big Data platforms](https://www.taosdata.com/blog/2019/07/29/542.html)
- [Why don’t General Big Data Platforms Fit IoT Scenarios?](https://www.taosdata.com/blog/2019/07/09/why-does-the-general-big-data-platform-not-fit-iot-data-processing/)
- [Why TDengine is the best choice for IoT, Internet of Vehicles, and Industry Internet Big Data platforms?](https://www.taosdata.com/blog/2019/07/09/why-tdengine-is-the-best-choice-for-iot-big-data-processing/)

## FAQ

- [FAQ: Common questions and answers](../13.faq/docs.md)
