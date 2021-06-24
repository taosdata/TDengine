# TDengine Documentation

TDengine is a highly efficient platform to store, query, and analyze time-series data. It is specially designed and optimized for IoT, Internet of Vehicles, Industrial IoT, IT Infrastructure and Application Monitoring, etc. It works like a relational database, such as MySQL, but you are strongly encouraged to read through the following documentation before you experience it, especially the Data Model and Data Modeling sections. In addition to this document, you should also download and read our technology white paper. For the older TDengine version 1.6 documentation, please click here.

## [TDengine Introduction](/evaluation)

* [TDengine Introduction and Features](/evaluation#intro)
* [TDengine Use Scenes](/evaluation#scenes)
* [TDengine Performance Metrics and Verification]((/evaluation#))

## [Getting Started](/getting-started)

* [Quickly Install](/getting-started#install): install via source code/package / Docker within seconds

- [Easy to Launch](/getting-started#start): start / stop TDengine with systemctl
- [Command-line](/getting-started#console) : an easy way to access TDengine server
- [Experience Lightning Speed](/getting-started#demo): running a demo, inserting/querying data to experience faster speed
- [List of Supported Platforms](/getting-started#platforms): a list of platforms supported by TDengine server and client
- [Deploy to Kubernetes](https://taosdata.github.io/TDengine-Operator/en/index.html)：a detailed guide for TDengine deployment in Kubernetes environment

## [Overall Architecture](/architecture)

- [Data Model](/architecture#model): relational database model, but one table for one device with static tags
- [Cluster and Primary Logical Unit](/architecture#cluster): Take advantage of NoSQL, support scale-out and high-reliability
- [Storage Model and Data Partitioning/Sharding](/architecture#sharding): tag data will be separated from time-series data, segmented by vnode and time
- [Data Writing and Replication Process](/architecture#replication): records received are written to WAL, cached, with acknowledgement is sent back to client, while supporting multi-replicas
- [Caching and Persistence](/architecture#persistence): latest records are cached in memory, but are written in columnar format with an ultra-high compression ratio
- [Data Query](/architecture#query): support various functions, time-axis aggregation, interpolation, and multi-table aggregation

## [Data Modeling](/model)

- [Create a Database](/model#create-db): create a database for all data collection points with similar features
- [Create a Super Table(STable)](/model#create-stable): create a STable for all data collection points with the same type
- [Create a Table](/model#create-table): use STable as the template, to create a table for each data collecting point

## [TAOS SQL](/taos-sql)

- [Data Types](/taos-sql#data-type): support timestamp, int, float, nchar, bool, and other types
- [Database Management](/taos-sql#management): add, drop, check databases
- [Table Management](/taos-sql#table): add, drop, check, alter tables
- [STable Management](/taos-sql#super-table): add, drop, check, alter STables
- [Tag Management](/taos-sql#tags): add, drop, alter tags
- [Inserting Records](/taos-sql#insert): support to write single/multiple items per table, multiple items across tables, and support to write historical data
- [Data Query](/taos-sql#select): support time segment, value filtering, sorting, manual paging of query results, etc
- [SQL Function](/taos-sql#functions): support various aggregation functions, selection functions, and calculation functions, such as avg, min, diff, etc
- [Time Dimensions Aggregation](/taos-sql#aggregation): aggregate and reduce the dimension after cutting table data by time segment
- [Boundary Restrictions](/taos-sql#limitation): restrictions for the library, table, SQL, and others
- [Error Code](/taos-sql/error-code): TDengine 2.0 error codes and corresponding decimal codes

## [Efficient Data Ingestion](/insert)

- [SQL Ingestion](/insert#sql): write one or multiple records into one or multiple tables via SQL insert command
- [Prometheus Ingestion](/insert#prometheus): Configure Prometheus to write data directly without any code
- [Telegraf Ingestion](/insert#telegraf): Configure Telegraf to write collected data directly without any code
- [EMQ X Broker](/insert#emq): Configure EMQ X to write MQTT data directly without any code
- [HiveMQ Broker](/insert#hivemq): Configure HiveMQ to write MQTT data directly without any code

## [Efficient Data Querying](/queries)

- [Main Query Features](/queries#queries): support various standard functions, setting filter conditions, and querying per time segment
- [Multi-table Aggregation Query](/queries#aggregation): use STable and set tag filter conditions to perform efficient aggregation queries
- [Downsampling to Query Value](/queries#sampling): aggregate data in successive time windows, support interpolation

## [Advanced Features](/advanced-features)

- [Continuous Query](/advanced-features#continuous-query): Based on sliding windows, the data stream is automatically queried and calculated at regular intervals
- [Data Publisher/Subscriber](/advanced-features#subscribe): subscribe to the newly arrived data like a typical messaging system
- [Cache](/advanced-features#cache): the newly arrived data of each device/table will always be cached
- [Alarm Monitoring](/advanced-features#alert): automatically monitor out-of-threshold data, and actively push it based-on configuration rules

## [Connector](/connector)

- [C/C++ Connector](/connector#c-cpp): primary method to connect to TDengine server through libtaos client library
- [Java Connector(JDBC)]: driver for connecting to the server from Java applications using the JDBC API
- [Python Connector](/connector#python): driver for connecting to TDengine server from Python applications
- [RESTful Connector](/connector#restful): a simple way to interact with TDengine via HTTP
- [Go Connector](/connector#go): driver for connecting to TDengine server from Go applications
- [Node.js Connector](/connector#nodejs): driver for connecting to TDengine server from Node.js applications
- [C# Connector](/connector#csharp): driver for connecting to TDengine server from C# applications
- [Windows Client](https://www.taosdata.com/blog/2019/07/26/514.html): compile your own Windows client, which is required by various connectors on the Windows environment

## [Connections with Other Tools](/connections)

- [Grafana](/connections#grafana): query the data saved in TDengine and provide visualization
- [MATLAB](/connections#matlab): access data stored in TDengine server via JDBC configured within MATLAB
- [R](/connections#r): access data stored in TDengine server via JDBC configured within R
- [IDEA Database](https://www.taosdata.com/blog/2020/08/27/1767.html): use TDengine visually through IDEA Database Management Tool

## [Installation and Management of TDengine Cluster](/cluster)

- [Preparation](/cluster#prepare): important considerations before deploying TDengine for production usage
- [Create Your First Node](/cluster#node-one): simple to follow the quick setup
- [Create Subsequent Nodes](/cluster#node-other): configure taos.cfg for new nodes to add more to the existing cluster
- [Node Management](/cluster#management): add, delete, and check nodes in the cluster
- [High-availability of Vnode](/cluster#high-availability): implement high-availability of Vnode through multi-replicas
- [Mnode Management](/cluster#mnode): automatic system creation without any manual intervention
- [Load Balancing](/cluster#load-balancing): automatically performed once the number of nodes or load changes
- [Offline Node Processing](/cluster#offline): any node that offline for more than a certain period will be removed from the cluster
- [Arbitrator](/cluster#arbitrator): used in the case of an even number of replicas to prevent split-brain

## [TDengine Operation and Maintenance](/administrator)

- [Capacity Planning](/administrator#planning): Estimating hardware resources based on scenarios
- [Fault Tolerance and Disaster Recovery](/administrator#tolerance): set the correct WAL and number of data replicas
- [System Configuration](/administrator#config): port, cache size, file block size, and other system configurations
- [User Management](/administrator#user): add/delete TDengine users, modify user password
- [Import Data](/administrator#import): import data into TDengine from either script or CSV file
- [Export Data](/administrator#export): export data either from TDengine shell or from the taosdump tool
- [System Monitor](/administrator#status): monitor the system connections, queries, streaming calculation, logs, and events
- [File Directory Structure](/administrator#directories): directories where TDengine data files and configuration files located
- [Parameter Restrictions and Reserved Keywords](/administrator#keywords): TDengine’s list of parameter restrictions and reserved keywords

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

- [FAQ: Common questions and answers](/faq)
