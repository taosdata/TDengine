# TDengine Documentation

TDengine is a highly efficient platform to store, query, and analyze time-series data. It is specially designed and optimized for IoT, Internet of Vehicles, Industrial IoT, IT Infrastructure and Application Monitoring, etc. It works like a relational database, such as MySQL, but you are strongly encouraged to read through the following documentation before you experience it, especially the Data Modeling sections. In addition to this document, you should also download and read the technology white paper. For the older TDengine version 1.6 documentation, please click [here](https://www.taosdata.com/en/documentation16/).

## [TDengine Introduction](/evaluation)

* [TDengine Introduction and Features](/evaluation#intro)
* [TDengine Use Scenes](/evaluation#scenes)
* [TDengine Performance Metrics and Verification](/evaluation#)

## [Getting Started](/getting-started)

* [Quick Install](/getting-started#install): install via source code/package / Docker within seconds
* [Quick Launch](/getting-started#start): start / stop TDengine quickly with systemctl
* [Command-line](/getting-started#console) : an easy way to access TDengine server
* [Experience Lightning Speed](/getting-started#demo): running a demo, inserting/querying data to experience faster speed
* [List of Supported Platforms](/getting-started#platforms): a list of platforms supported by TDengine server and client
* [Deploy to Kubernetes](https://taosdata.github.io/TDengine-Operator/en/index.html)：a detailed guide for TDengine deployment in Kubernetes environment

## [Overall Architecture](/architecture)

- [Data Model](/architecture#model): relational database model, but one table for one data collection point with static tags
- [Cluster and Primary Logical Unit](/architecture#cluster): Take advantage of NoSQL architecture, high availability and horizontal scalability
- [Storage Model and Data Partitioning/Sharding](/architecture#sharding): tag data is separated from time-series data, sharded by vnodes and partitioned by time 
- [Data Writing and Replication Process](/architecture#replication): records received are written to WAL, cached, with acknowledgement sent back to client, while supporting data replications
- [Caching and Persistence](/architecture#persistence): latest records are cached in memory, but are written in columnar format with an ultra-high compression ratio
- [Data Query](/architecture#query): support various SQL functions, downsampling, interpolation, and multi-table aggregation

## [Data Modeling](/model)

- [Create a Database](/model#create-db): create a database for all data collection points with similar data characteristics
- [Create a Super Table(STable)](/model#create-stable): create a STable for all data collection points with the same type
- [Create a Table](/model#create-table): use STable as the template to create a table for each data collecting point

## [Efficient Data Ingestion](/insert)

- [Data Writing via SQL](/insert#sql): write one or multiple records into one or multiple tables via SQL insert command
- [Data Writing via Prometheus](/insert#prometheus): Configure Prometheus to write data directly without any code
- [Data Writing via Telegraf](/insert#telegraf): Configure Telegraf to write collected data directly without any code
- [Data Writing via EMQ X](/insert#emq): Configure EMQ X to write MQTT data directly without any code
- [Data Writing via HiveMQ Broker](/insert#hivemq): Configure HiveMQ to write MQTT data directly without any code

## [Efficient Data Querying](/queries)

- [Major Features](/queries#queries): support various standard query functions, setting filter conditions, and querying per time segment
- [Multi-table Aggregation](/queries#aggregation): use STable and set tag filter conditions to perform efficient aggregation
- [Downsampling](/queries#sampling): aggregate data in successive time windows, support interpolation

## [TAOS SQL](/taos-sql)

- [Data Types](/taos-sql#data-type): support timestamp, int, float, nchar, bool, and other types
- [Database Management](/taos-sql#management): add, drop, check databases
- [Table Management](/taos-sql#table): add, drop, check, alter tables
- [STable Management](/taos-sql#super-table): add, drop, check, alter STables
- [Tag Management](/taos-sql#tags): add, drop, alter tags
- [Inserting Records](/taos-sql#insert): write single/multiple records a table, multiple records across tables, and historical data
- [Data Query](/taos-sql#select): support time segment, value filtering, sorting, manual paging of query results, etc
- [SQL Function](/taos-sql#functions): support various aggregation functions, selection functions, and calculation functions, such as avg, min, diff, etc
- [Cutting and Aggregation](/taos-sql#aggregation): aggregate and reduce the dimension after cutting table data by time segment
- [Boundary Restrictions](/taos-sql#limitation): restrictions for the library, table, SQL, and others
- [Error Code](/taos-sql/error-code): TDengine 2.0 error codes and corresponding decimal codes

## [Advanced Features](/advanced-features)

- [Continuous Query](/advanced-features#continuous-query): Based on sliding windows, the data stream is automatically queried and calculated at regular intervals
- [Data Publisher/Subscriber](/advanced-features#subscribe): subscribe to the newly arrived data like a typical messaging system
- [Cache](/advanced-features#cache): the newly arrived data of each device/table will always be cached
- [Alarm Monitoring](/advanced-features#alert): automatically monitor out-of-threshold data, and actively push it based-on configuration rules

## [Connector](/connector)

- [C/C++ Connector](/connector#c-cpp): primary method to connect to TDengine server through libtaos client library
- [Java Connector(JDBC)](/connector/java): driver for connecting to the server from Java applications using the JDBC API
- [Python Connector](/connector#python): driver for connecting to TDengine server from Python applications
- [RESTful Connector](/connector#restful): a simple way to interact with TDengine via HTTP
- [Go Connector](/connector#go): driver for connecting to TDengine server from Go applications
- [Node.js Connector](/connector#nodejs): driver for connecting to TDengine server from Node.js applications
- [C# Connector](/connector#csharp): driver for connecting to TDengine server from C# applications
- [Windows Client](https://www.taosdata.com/blog/2019/07/26/514.html): compile your own Windows client, which is required by various connectors on the Windows environment
- [Rust Connector](/connector/rust): A taosc/RESTful API based TDengine client for Rust

## [Components and Tools](/tools/adapter)

* [taosAdapter](/tools/adapter)

## [Connections with Other Tools](/connections)

- [Grafana](/connections#grafana): query the data saved in TDengine and provide visualization
- [MATLAB](/connections#matlab): access data stored in TDengine server via JDBC configured within MATLAB
- [R](/connections#r): access data stored in TDengine server via JDBC configured within R
- [IDEA Database](https://www.taosdata.com/blog/2020/08/27/1767.html): use TDengine visually through IDEA Database Management Tool

## [Installation and Management of TDengine Cluster](/cluster)

- [Preparation](/cluster#prepare): important steps before deploying TDengine for production usage
- [Create the First Node](/cluster#node-one): just follow the steps in quick start 
- [Create Subsequent Nodes](/cluster#node-other): configure taos.cfg for new nodes to add more to the existing cluster
- [Node Management](/cluster#management): add, delete, and check nodes in the cluster
- [High-availability of Vnode](/cluster#high-availability): implement high-availability of Vnode through replicas
- [Mnode Management](/cluster#mnode): mnodes are created automatically without any manual intervention
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
- [System Connection and Task Query Management](/administrator#status): show the system connections, queries, streaming calculation and others
- [System Monitor](/administrator#monitoring): monitor TDengine cluster with log database and TDinsight.
- [File Directory Structure](/administrator#directories): directories where TDengine data files and configuration files located
- [Parameter Limits and Reserved Keywords](/administrator#keywords): TDengine’s list of parameter limits and reserved keywords

## Performance: TDengine vs Others

- [Performance: TDengine vs OpenTSDB](https://www.taosdata.com/blog/2019/09/12/710.html)
- [Performance: TDengine vs Cassandra](https://www.taosdata.com/blog/2019/09/12/708.html)
- [Performance: TDengine vs InfluxDB](https://www.taosdata.com/blog/2019/09/12/706.html)
- [Performance Test Reports of TDengine vs InfluxDB/OpenTSDB/Cassandra/MySQL/ClickHouse](https://www.taosdata.com/downloads/TDengine_Testing_Report_en.pdf)

## More on IoT Big Data

- [Characteristics of IoT and Industry Internet Big Data](https://www.taosdata.com/blog/2019/07/09/characteristics-of-iot-big-data/)
- [Features and Functions of IoT Big Data platforms](https://www.taosdata.com/blog/2019/07/29/542.html)
- [Why don’t General Big Data Platforms Fit IoT Scenarios?](https://www.taosdata.com/blog/2019/07/09/why-does-the-general-big-data-platform-not-fit-iot-data-processing/)
- [Why TDengine is the best choice for IoT, Internet of Vehicles, and Industry Internet Big Data platforms?](https://www.taosdata.com/blog/2019/07/09/why-tdengine-is-the-best-choice-for-iot-big-data-processing/)
- [Technical Blog](https://www.taosdata.com/cn/blog/?categories=3): More technical analysis and architecture design articles

## FAQ

- [FAQ: Common questions and answers](/faq)
