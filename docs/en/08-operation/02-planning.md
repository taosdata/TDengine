---
title: System Requirements
slug: /operations-and-maintenance/system-requirements
---

If you plan to build a time-series data platform using TDengine, you must conduct detailed planning of computing resources, storage resources, and network resources in advance to ensure that they meet the needs of your business scenario. Typically, TDengine runs multiple processes, including taosd, taosadapter, taoskeeper, taos-explorer, and taosx.

Among these processes, the resource consumption of taoskeeper, taos-explorer, taosadapter, and taosx is relatively low and usually does not require special attention. Additionally, these processes have low storage space requirements, with their CPU and memory resource usage generally being one-tenth to a few tenths of that of the taosd process (except in special scenarios, such as data synchronization and historical data migration, where the technical support team of Taos Data will provide one-on-one service). System administrators should regularly monitor the resource consumption of these processes and take appropriate actions in a timely manner.

In this section, we will focus on the resource planning of the core process of the TDengine database engine—taosd. Reasonable resource planning will ensure the efficient operation of the taosd process, thereby improving the performance and stability of the entire time-series data platform.

## Server Memory Requirements

Each database can create a fixed number of vgroups, with a default of two. When creating a database, the number of vgroups can be specified through the `vgroups<num>` parameter, while the number of replicas is determined by the `replica<num>` parameter. Since each replica in a vgroup corresponds to a vnode, the memory occupied by the database is determined by the parameters vgroups, replica, buffer, pages, pagesize, and cachesize.

To help users better understand and configure these parameters, the official TDengine documentation on database management provides detailed explanations. Based on these parameters, the memory size required for a database can be estimated, and the specific calculation method is as follows (the specific values should be adjusted based on actual conditions):

```text
vgroups × replica × (buffer + pages × pagesize + cachesize)
```

It should be noted that these memory resources are not borne solely by a single server but are shared by all dnodes in the entire cluster. In other words, the burden of these resources is actually shared by the server cluster where these dnodes reside. If there are multiple databases within the cluster, the total memory required must also sum the requirements of these databases. More complex scenarios arise when the dnodes in the cluster are not all deployed from the beginning, but instead, servers and dnodes are gradually added as node loads increase. In this case, the newly added databases may cause an uneven load distribution between old and new dnodes. In such cases, simple theoretical calculations are insufficiently accurate; it is necessary to consider the actual load status of each dnode for a comprehensive assessment.

System administrators can use the following SQL to view the distribution of all vnodes from all databases on each dnode in the information_schema library.

```sql
select * from information_schema.ins_vnodes;
dnode_id | vgroup_id | db_name | status | role_time | start_time | restored |
===============================================================================================
 1 | 3 | log | leader | 2024-01-16 13:52:13.618 | 2024-01-16 13:52:01.628 | true |
 1 | 4 | log | leader | 2024-01-16 13:52:13.630 | 2024-01-16 13:52:01.702 | true |
```

## Client Memory Requirements

1. Native Connection Method

Since client applications communicate with the server using taosc, there will be a certain amount of memory consumption. This memory consumption mainly arises from: SQL in write operations, caching of table metadata information, and inherent structural overhead. Assuming the maximum number of tables supported by the database service is N (the metadata overhead for each table created via a supertable is about 256B), the maximum number of concurrent write threads is T, and the maximum SQL statement length is S (usually 1MB). Based on these parameters, we can estimate the client's memory consumption (in MB).

M = (T × S × 3 + (N / 4096) + 100)

For example, if the maximum number of concurrent write threads for a user is 100 and the number of subtables is 10,000,000, then the minimum memory requirement for the client is as follows:
100 × 3 + (10000000 / 4096) + 100 ≈ 2841 (MB)
That is, configuring 3GB of memory is the minimum requirement.

2. RESTful/WebSocket Connection Method

When using the WebSocket connection method for data writing, if the memory usage is not large, it is usually not a concern. However, when executing query operations, the WebSocket connection method will consume a certain amount of memory. Next, we will discuss memory usage in query scenarios in detail.

When the client initiates a query request via the WebSocket connection method, sufficient memory space must be reserved to receive and process the query results. Thanks to the characteristics of the WebSocket connection method, data can be received and decoded in batches, allowing for the processing of large amounts of data while ensuring that the memory required for each connection remains fixed.

The method for calculating client memory usage is relatively simple: it suffices to sum the read/write buffer capacity required for each connection. Typically, each connection will additionally occupy 8MB of memory. Therefore, if there are C concurrent connections, the total additional memory requirement is 8 × C (in MB).

For example, if the maximum number of concurrent connections for a user is 10, then the minimum additional memory requirement for the client is 80 (8 × 10) MB.

Compared to the WebSocket connection method, the RESTful connection method has a larger memory footprint. In addition to the memory required for the buffer, it also needs to consider the memory overhead for the response results for each connection. This memory overhead is closely related to the size of the JSON data in the response results, especially when querying a large amount of data, which can consume significant memory.

Since the RESTful connection method does not support batch retrieval of query data, this can lead to particularly large memory consumption when querying large result sets, potentially causing memory overflow. Therefore, in large projects, it is recommended to enable the batchfetch=true option to utilize the WebSocket connection method, allowing for stream-like result set returns to avoid the risk of memory overflow.

:::note

- It is recommended to use RESTful/WebSocket connection methods to access the TDengine cluster instead of the native taosc connection method.
- In the vast majority of cases, the RESTful/WebSocket connection methods meet the business requirements for writing and querying, and this connection method does not depend on taosc, making server maintenance and upgrades much easier.

:::

## CPU Requirements

The CPU requirements for TDengine users are mainly influenced by the following three factors:

- Data Sharding: In TDengine, each CPU core can serve 1 to 2 vnodes. If a cluster is configured with 100 vgroups and uses a three-replica strategy, the recommended number of CPU cores for that cluster is 150 to 300 to achieve optimal performance.
- Data Writing: A single core of TDengine can handle at least 10,000 write requests per second. Notably, each write request can contain multiple records, and the computational resources consumed are similar whether writing a single record or writing 10 records simultaneously. Therefore, the more records written in each request, the higher the writing efficiency. For example, if a write request contains over 200 records, a single core can achieve a writing speed of 1 million records per second. However, this requires the front-end data collection system to have higher capability, as it needs to buffer records and write them in batches.
- Query Demand: Although TDengine provides efficient query capabilities, the varying nature of queries in each application scenario and changes in query frequency make it difficult to provide a specific number to measure the computational resources required for queries. Users need to write some query statements based on their actual scenarios to more accurately determine the necessary computational resources.

In summary, the CPU requirements for data sharding and data writing can be estimated. However, the computational resources consumed by query demand are more challenging to predict. During actual operation, it is recommended to maintain CPU usage below 50% to ensure system stability and performance. Once CPU usage exceeds this threshold, consideration should be given to adding new nodes or increasing the number of CPU cores to provide more computational resources.

## Storage Requirements

Compared to traditional general-purpose databases, TDengine performs excellently in data compression, achieving extremely high compression ratios. In most application scenarios, the compression ratio of TDengine is usually no less than 5 times. In certain specific cases, the compression ratio can even reach 10 times or even hundreds of times, which mainly depends on the data characteristics in actual scenarios.

To calculate the raw data size before compression, the following method can be used:
RawDataSize = numOfTables × rowSizePerTable × rowsPerTable

Example: For 10 million smart meters, collecting data every 15 minutes with a data size of 20B each time results in an approximate raw data volume of 7TB per year. TDengine would require about 1.4TB of storage space.

To cater to different users' personalized needs for data storage duration and costs, TDengine provides users with great flexibility. Users can customize their storage strategies through a series of database parameter configuration options. Among them, the keep parameter is particularly noteworthy, as it empowers users to set the maximum retention period for data in storage. This design allows users to finely control the data storage lifecycle according to the importance of the business and the timeliness of the data, thus achieving precise control over storage costs.

However, relying solely on the keep parameter to optimize storage costs is still insufficient. Therefore, TDengine has further innovated by introducing a multi-tier storage strategy.

Additionally, to accelerate data processing, TDengine supports the configuration of multiple hard disks for concurrent data writing and reading. This parallel processing mechanism maximizes the processing capability of multi-core CPUs and the I/O bandwidth of hard disks, significantly improving data transfer speeds and effectively addressing the challenges of high concurrency and large data volumes in application scenarios.

:::tip

How to Estimate TDengine Compression Ratio

Users can utilize the performance testing tool taosBenchmark to assess the data compression effects of TDengine. By using the -f option to specify the write configuration file, taosBenchmark can write a specified number of CSV sample data into the specified library parameters and table structures. After completing the data write, users can execute the flush database command in the taos shell to force all data to be written to the hard disk. Then, using the du command in the Linux operating system, the size of the data folder for the specified vnode can be obtained. Finally, dividing the raw data size by the actual storage data size yields the true compression ratio.

::: tip

The following command can be used to obtain the storage space occupied by TDengine.

```shell
taos> flush database <dbname>;
$ du -hd1 <dataDir>/vnode --exclude=wal
```

## Multi-tier Storage

In addition to the demand for storage capacity, users may also wish to reduce storage costs at specific capacities. To meet this need, TDengine has introduced multi-tier storage functionality. This feature allows for high-frequency data generated recently to be stored on high-cost storage devices, while older data with lower access frequency is stored on low-cost storage devices. Through this approach, TDengine achieves the following objectives:

- Reduce Storage Costs: By storing vast amounts of cold data on inexpensive storage devices, storage costs can be significantly reduced.
- Improve Writing Performance: Each tier of storage supports multiple mount points, and the WAL files also support parallel writing with multiple mount points at level 0, greatly enhancing writing performance (sustained write speeds in actual scenarios can reach 300 million data points per second), achieving high I/O throughput even on mechanical hard disks (measured at up to 2GB/s).

Users can determine the capacity allocation between high-speed and low-cost storage devices based on the ratio of hot and cold data.

TDengine's multi-tier storage functionality also has the following advantages in use:

- Easy Maintenance: Once the mount points for each storage tier are configured, tasks like data migration do not require manual intervention, making storage expansion more flexible and convenient.
- Transparent to SQL: Whether the queried data spans across tiers or not, a single SQL statement can return all data, making it concise and efficient.

## Network Bandwidth Requirements

Network bandwidth requirements can be divided into two main parts—write queries and internal cluster communication.

Write queries refer to the bandwidth requirements for business requests, specifically the bandwidth needed for the client to send data to the server for writing operations.

The bandwidth requirements for internal cluster communication can be further divided into two parts:

- Bandwidth requirements for normal communication between nodes, such as leaders distributing data to followers, and taosAdapter distributing write requests to each vgroup leader, etc.
- Additional internal communication bandwidth required for the cluster to respond to maintenance instructions, such as data replication caused by switching from a single replica to three replicas, or data replication caused by repairs on specified dnodes.

To estimate inbound bandwidth requirements, we can use the following method:
Since the taosc writing includes built-in compression during RPC communication, the write bandwidth requirements are lower than those for RESTful/WebSocket connection methods. Here, we will base the bandwidth requirements for write requests on RESTful/WebSocket connection methods.

Example: For 10 million smart meters, collecting data every 15 minutes with each data collection having a size of 20B, the average bandwidth requirement can be calculated as 0.22MB.

Considering that there may be a situation where smart meters report data in a centralized manner, the bandwidth requirements should be increased based on the actual project situation. Since RESTful requests are transmitted in plaintext, the actual bandwidth requirements should also be multiplied by a factor to obtain the estimated value.

:::tip

Network bandwidth and communication latency are crucial for the performance and stability of distributed systems, especially in the network connections between server nodes. We strongly recommend allocating a dedicated VLAN for the network between server nodes to avoid external communication interference. When selecting bandwidth, it is advisable to use 10G networks, or at least 1G networks, ensuring a packet loss rate below 0.01%. If a distributed storage solution is adopted, the storage network and the internal communication network of the cluster must be planned separately. A common practice is to use dual 10G networks, which consist of two independent 10G networks. This can ensure that storage data and internal cluster communication do not interfere with each other, enhancing overall performance. For inbound networks, in addition to ensuring sufficient access bandwidth, the packet loss rate should also be kept below 0.01%. This will help reduce errors and retransmissions during data transmission, thus improving the reliability and efficiency of the system.

:::

## Number of Servers

Based on the previous estimates for memory, CPU, storage, and network bandwidth, we can derive the required memory capacity, CPU core count, storage space, and network bandwidth for the entire TDengine cluster. If the number of data replicas is not 1, the total demand must be multiplied by the number of replicas to determine the actual resource requirements.

Thanks to the excellent horizontal scalability of TDengine, we can easily calculate the total resource demand. Next, simply divide this total by the resource capacity of a single physical or virtual machine to roughly determine how many physical or virtual machines need to be purchased to deploy the TDengine cluster.

## Network Port Requirements

The table below lists some commonly used ports for TDengine interfaces or components, which can all be modified through parameters in the configuration file.

|  Interface or Component  |    Port    |
| :----------------------: | :--------: |
| Native Interface (taosc) |    6030    |
|    RESTful Interface     |    6041    |
|   WebSocket Interface    |    6041    |
|        taosKeeper        |    6043    |
|          taosX           | 6050, 6055 |
|       taosExplorer       |    6060    |
