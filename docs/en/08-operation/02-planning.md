---
title: System Requirements
slug: /operations-and-maintenance/system-requirements
---

If you plan to use TDengine to build a time-series data platform, it is necessary to make detailed plans for computing resources, storage resources, and network resources in advance to ensure they meet the needs of your business scenarios. Typically, TDengine runs multiple processes, including taosd, taosadapter, taoskeeper, taos-explorer, and taosx.

Among these processes, taoskeeper, taos-explorer, taosadapter, and taosx consume relatively fewer resources and usually do not require special attention. Additionally, these processes have lower storage space requirements, and their CPU and memory usage is generally one-tenth to a fraction of that of the taosd process (except in special scenarios such as data synchronization and historical data migration. In these cases, the technical support team of TDengine will provide one-on-one services). System administrators should regularly monitor the resource consumption of these processes and handle them promptly.

In this section, we will focus on the resource planning for the core process of the TDengine database engine—taosd. Proper resource planning will ensure the efficient operation of the taosd process, thereby enhancing the performance and stability of the entire time-series data platform.

## Server Memory Requirements

Each database can create a fixed number of vgroups, with two being the default. When creating a database, you can specify the number of vgroups through the `vgroups<num>` parameter, while the number of replicas is determined by the `replica<num>` parameter. Since each replica in a vgroup corresponds to a vnode, the memory occupied by the database is determined by the parameters vgroups, replica, buffer, pages, pagesize, and cachesize.

To help users better understand and configure these parameters, the official documentation of TDengine's database management section provides detailed explanations. Based on these parameters, you can estimate the memory size required for a database, which is calculated as follows (specific values should be adjusted according to actual conditions).
vgroups × replica × (buffer + pages × pagesize + cachesize)

It should be noted that these memory resources are not borne by a single server but are shared among all dnodes in the cluster, meaning the burden of these resources is actually shared by the server cluster where these dnodes are located. If there are multiple databases within the cluster, the total memory required will also need to add up the demands of these databases. More complex situations arise if the dnodes in the cluster are not all deployed at the beginning but are added gradually as node loads increase during use. In such cases, new databases may cause an uneven load distribution between new and old dnodes. In this situation, simple theoretical calculations are not accurate enough, and it is necessary to consider the actual load conditions of each dnode for a comprehensive assessment.

System administrators can view the distribution of all databases' vnodes across various dnodes in the information_schema library's ins_vnodes table using the following SQL query.

```sql
select * from information_schema.ins_vnodes;
dnode_id |vgroup_id | db_name | status | role_time | start_time | restored |
===============================================================================================
 1| 3 | log | leader | 2024-01-16 13:52:13.618 | 2024-01-16 13:52:01.628 | true |
 1| 4 | log | leader | 2024-01-16 13:52:13.630 | 2024-01-16 13:52:01.702 | true |
```

## Client Memory Requirements

1. Native Connection Method

Since client applications use taosc to communicate with the server, this results in certain memory consumption. This memory consumption mainly comes from: SQL in write operations, caching of table metadata information, and inherent structural overhead. Assuming the maximum number of tables that the database service can support is N (metadata overhead for each table created through a supertable is about 256B), the maximum number of concurrent write threads is T, and the maximum length of SQL statements is S (usually 1MB). Based on these parameters, we can estimate the memory consumption of the client (in MB).
M = (T × S × 3 + (N / 4096) + 100)

For example, if the maximum number of concurrent write threads for a user is 100, and the number of subtables is 10,000,000, then the minimum memory requirement for the client is as follows:
100 × 3 + (10000000 / 4096) + 100 ≈ 2841 (MB)
That is, configuring 3GB of memory is the minimum requirement.

1. RESTful/WebSocket Connection Methods

When using WebSocket connection methods for data writing, it is usually not a concern if memory usage is not significant. However, when performing query operations, the WebSocket connection method consumes a certain amount of memory. Next, we will discuss the memory usage in query scenarios in detail.

When a client initiates a query request through a WebSocket connection, sufficient memory space must be reserved to receive and process the query results. Thanks to the characteristics of the WebSocket connection method, data can be received and decoded in batches, which allows handling large amounts of data while ensuring that the memory required for each connection is fixed.

The method to calculate client memory usage is relatively simple: just add up the capacity of the read/write buffer required for each connection. Typically, each connection consumes an additional 8MB of memory. Therefore, if there are C concurrent connections, the total additional memory requirement is 8×C (in MB).

For example, if the maximum number of concurrent connections for a user is 10, the minimum additional memory requirement for the client is 80 (8×10) MB.

Compared to the WebSocket connection method, the RESTful connection method consumes more memory, including the memory needed for the buffer and the memory overhead of each connection's response result. This memory overhead is closely related to the size of the JSON data in the response, especially when querying large amounts of data, which can consume a significant amount of memory.

Since the RESTful connection method does not support fetching query data in batches, this can lead to particularly large memory usage when retrieving very large result sets, potentially causing memory overflow. Therefore, in large projects, it is recommended to use the WebSocket connection method to implement streaming result sets and avoid the risk of memory overflow.

:::note

- It is recommended to use RESTful/WebSocket connection methods to access the TDengine cluster, rather than using the native taosc connection method.
- In most cases, RESTful/WebSocket connection methods meet the requirements for business writing and querying, and these methods do not depend on taosc, completely decoupling cluster server upgrades from client connection methods, making server maintenance and upgrades easier.

:::

## CPU Requirements

TDengine users' CPU requirements are mainly influenced by the following three factors:

- Data sharding: In TDengine, each CPU core can serve 1 to 2 vnodes. Assuming a cluster is configured with 100 vgroups and uses a three-replica strategy, it is recommended that the cluster has 150~300 CPU cores to achieve optimal performance.
- Data writing: TDengine's single core can handle at least 10,000 write requests per second. It is worth noting that each write request can contain multiple records, and writing one record at a time compared to writing 10 records simultaneously consumes roughly the same computing resources. Therefore, the more records written at once, the higher the writing efficiency. For example, if a write request contains more than 200 records, a single core can achieve a speed of writing 1 million records per second. However, this requires the front-end data collection system to have higher capabilities, as it needs to cache records and then write them in batches.
- Query requirements: Although TDengine provides efficient querying capabilities, the computational resources required for querying vary greatly depending on each application scenario and the frequency of queries, making it difficult to provide a specific number to measure the computational resources needed for querying. Users need to write some query statements based on their actual scenarios to more accurately determine the required computational resources.

In summary, the CPU requirements for data sharding and data writing can be estimated. However, the computational resources consumed by query requirements are difficult to predict. In practice, it is recommended to keep CPU usage below 50% to ensure system stability and performance. Once CPU usage exceeds this threshold, it may be necessary to consider adding new nodes or increasing the number of CPU cores to provide more computational resources.

## Storage Requirements

Compared to traditional general-purpose databases, TDengine excels in data compression, achieving a very high compression rate. In most application scenarios, TDengine's compression rate is usually no less than 5 times. In some specific cases, the compression rate can even reach 10 times or even hundreds of times, depending mainly on the data characteristics in the actual scenario.

To calculate the size of the original uncompressed data, the following method can be used:
RawDataSize = numOfTables × rowSizePerTable × rowsPerTable

Example: 10 million smart meters, each meter collects data every 15 minutes, and each data collection is 20B, then the original data volume for one year is about 7TB. TDengine would approximately require 1.4TB of storage space.

To cater to different users' personalized needs in terms of data storage duration and cost, TDengine provides users with great flexibility. Users can customize their storage strategy through a series of database parameter configuration options. Among these, the keep parameter is particularly noteworthy, as it allows users to independently set the maximum retention period for data in storage space. This feature design enables users to precisely control the storage lifecycle of data based on the importance of the business and the timeliness of the data, thereby achieving refined control of storage costs.

However, relying solely on the keep parameter to optimize storage costs is still insufficient. For this reason, TDengine further innovates by introducing a multi-level storage strategy.

Additionally, to accelerate the data processing process, TDengine specifically supports configuring multiple hard drives to achieve concurrent data writing and reading. This parallel processing mechanism maximizes the use of multi-core CPU processing power and hard disk I/O bandwidth, significantly increasing data transfer speed, perfectly addressing the challenges of high concurrency and large data volume application scenarios.

## How to Estimate TDengine Compression Ratio

Users can use the performance testing tool taosBenchmark to assess the data compression effect of TDengine. By using the -f option to specify the write configuration file, taosBenchmark can write a specified number of CSV sample data into the specified database parameters and table structure.

After completing the data writing, users can execute the flush database command in the TDengine CLI to force all data to be written to the disk. Then, use the du command of the Linux operating system to get the size of the data folder of the specified vnode. Finally, divide the original data size by the actual storage data size to calculate the real compression ratio.

The following command can be used to obtain the storage space occupied by TDengine.

```shell
taos> flush database <dbname>;
$ du -hd1 <dataDir>/vnode --exclude=wal
```

## Multi-Tier Storage

In addition to storage capacity needs, users may also want to reduce storage costs under specific capacities. To meet this need, TDengine has introduced a multi-tier storage feature. This feature allows data that is recently generated and accessed frequently to be stored on high-cost storage devices, while data that is older and accessed less frequently is stored on low-cost storage devices. Through this method, TDengine achieves the following goals.

- Reduce storage costs: By storing massive amounts of very cold data on cheap storage devices, storage costs can be significantly reduced.
- Improve write performance: Each storage tier supports multiple mount points, and WAL files also support parallel writing on multiple mount points at level 0, which greatly improves write performance (sustained write speeds of up to 300 million measurements per second) and achieves very high disk I/O throughput on mechanical hard drives (tested up to 2GB/s).

Users can decide the capacity allocation between high-speed and low-cost storage devices based on the ratio of hot and cold data.

TDengine's multi-tier storage feature also has the following advantages in use.

- Easy maintenance: After configuring the storage mount points for each tier, tasks such as data migration do not require manual intervention, making storage expansion more flexible and convenient.
- Transparent to SQL: Regardless of whether the queried data spans tiers, a single SQL can return all data, which is simple and efficient.

## Network Bandwidth Requirements

Network bandwidth requirements can be divided into two main parts—write queries and intra-cluster communication.

Write queries refer to the bandwidth requirements for business requests, i.e., the bandwidth needed for clients to send data to the server for writing operations.

Intra-cluster communication bandwidth requirements are further divided into two parts:

- The bandwidth needed for normal communication between nodes, for example, the leader distributing data to each follower, taosAdapter distributing write requests to each vgroup leader, etc.
- The additional internal communication bandwidth required by the cluster to respond to maintenance commands, such as data replication caused by switching from a single replica to three replicas, data replication triggered by repairing a specified dnode, etc.

To estimate inbound bandwidth requirements, we can use the following method:
Since taosc writing includes compression functionality during RPC communication, the bandwidth requirements for writing are relatively lower compared to RESTful/WebSocket connections. Here, we will estimate the bandwidth requirements for write requests based on the bandwidth requirements of RESTful/WebSocket connections.

Example: 10 million smart meters, each meter collects data every 15min, each data collection is 20B, which calculates an average bandwidth requirement of 0.22MB.

Considering the situation where smart meters report data in clusters, bandwidth requirements must be increased according to the actual project situation when calculating bandwidth requirements. Considering that RESTful requests are transmitted in plaintext, the actual bandwidth requirements should be multiplied by a factor to get the estimated value.

:::note

Network bandwidth and communication latency are crucial for the performance and stability of distributed systems, especially in terms of network connections between server nodes.
We strongly recommend allocating a dedicated VLAN for the network between server nodes to avoid interference from external communications. In terms of bandwidth selection, it is advisable to use a 10-gigabit network, or at least a gigabit network, and ensure that the packet loss rate is below one ten-thousandth.

If a distributed storage solution is adopted, the storage network and the intra-cluster communication network must be planned separately. A common practice is to use dual 10-gigabit networks, i.e., two sets of independent 10-gigabit networks. This ensures that storage data and intra-cluster communication do not interfere with each other, improving overall performance.

For inbound networks, in addition to ensuring sufficient access bandwidth, the packet loss rate must also be kept below one ten-thousandth. This will help reduce errors and retransmissions during data transmission, thereby improving the reliability and efficiency of the system.

:::

## Number of Servers

Based on the previous estimates of memory, CPU, storage, and network bandwidth, we can determine the total memory capacity, CPU cores, storage space, and network bandwidth required for the entire TDengine cluster. If the number of data replicas is not 1, the total demand must be multiplied by the number of replicas to obtain the actual required resources.

Thanks to TDengine's excellent horizontal scaling capabilities, we can easily calculate the total amount of resource requirements. Next, we only need to divide this total by the resource amount of a single physical or virtual machine to roughly determine how many physical or virtual machines need to be purchased to deploy the TDengine cluster.

## Network Port Requirements

The table below lists some common ports for TDengine interfaces or components, which can be modified through parameters in the configuration file.

|               Interface or Component Name                 |     Port   |   Protocol    |
|:---------------------------------------------------------:|:----------:|:-------------:|
|             Native Interface (taosc)                      |    6030    |   TCP         |
|              RESTful Interface                            |    6041    |   TCP         |
|             WebSocket Interface                           |    6041    |   TCP         |
|              taosKeeper                                   |    6043    |   TCP         |
|            StatsD Format Write Interface                  |    6044    | TCP/UDP       |
|           Collectd Format Write Interface                 |    6045    | TCP/UDP       |
|        OpenTSDB Telnet Format Write Interface             |    6046    |   TCP         |
|  Collectd using OpenTSDB Telnet Format Write Interface    |    6047    |   TCP         |
|  Icinga2 using OpenTSDB Telnet Format Write Interface     |    6048    |   TCP         |
| tcollector using OpenTSDB Telnet Format Write Interface   |    6049    |   TCP         |
|                taosX                                      | 6050, 6055 |   TCP         |
|             taosExplorer                                  |    6060    |   TCP         |
