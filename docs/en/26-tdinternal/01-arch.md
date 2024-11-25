---
title: Architecture
description: TDengine architecture design, including clusters, storage, caching and persistence, data backup, multi-level storage, and more.
slug: /inside-tdengine/architecture
---

import Image from '@theme/IdealImage';
import imgArch from '../assets/architecture-01.png';
import imgFlow from '../assets/architecture-02.png';
import imgLeader from '../assets/architecture-03.png';
import imgFollower from '../assets/architecture-04.png';

## Cluster and Basic Logical Units

TDengine's design assumes that individual hardware and software systems are unreliable and that no single computer can provide sufficient computing and storage capacity to handle massive data volumes. Therefore, from day one, TDengine has been designed as a distributed, highly reliable architecture, supporting horizontal scaling. This way, hardware failures or software errors on any single or multiple servers will not affect the availability and reliability of the system. Additionally, with node virtualization and load-balancing technology, TDengine efficiently utilizes heterogeneous clusters' computing and storage resources to reduce hardware investment.

### Main Logical Units

The logical structure of TDengine's distributed architecture is shown below:

<figure>
<Image img={imgArch} alt="TDengine architecture diagram"/>
<figcaption>Figure 1. TDengine architecture diagram</figcaption>
</figure>

A complete TDengine system runs on one or more physical nodes. Logically, it includes data nodes (dnode), TDengine application drivers (taosc), and applications (app). The system contains one or more data nodes, which form a cluster. Applications interact with the TDengine cluster through the taosc API. Below is a brief introduction to each logical unit.

**Physical Node (pnode):**  
A pnode is an independent computer with its own computing, storage, and networking capabilities. It can be a physical machine, virtual machine, or Docker container with an operating system installed. Each physical node is identified by its configured Fully Qualified Domain Name (FQDN). TDengine relies entirely on FQDN for network communication.

**Data Node (dnode):**  
A dnode is an instance of the TDengine server-side execution code (taosd) running on a physical node. At least one dnode is required for the proper functioning of a TDengine system. Each dnode contains zero or more logical virtual nodes (vnode). However, management, elastic computing, and stream computing nodes each have at most one logical instance.

A dnode is uniquely identified in a TDengine cluster by its endpoint (EP), which is a combination of the dnode's FQDN and configured network port. By configuring different ports, a single pnode (whether a physical machine, virtual machine, or Docker container) can run multiple instances, each with its own dnode.

**Virtual Node (vnode):**  
To better support data sharding, load balancing, and prevent data hotspots or skew, TDengine introduces the concept of virtual nodes (vnode). A vnode is virtualized into multiple independent instances (e.g., V2, V3, V4 in the architecture diagram), each responsible for storing and managing a portion of time-series data.

Each vnode operates with independent threads, memory space, and persistent storage paths to ensure data isolation and efficient access. A vnode can contain multiple tables (data collection points) distributed across different vnodes to achieve balanced load distribution.

When creating a new database in the cluster, the system automatically creates the corresponding vnodes. The number of vnodes that can be created on a dnode depends on the physical node's resources, such as CPU, memory, and storage capacity. Note that a vnode belongs to only one database, but a database may contain multiple vnodes.

In addition to storing time-series data, each vnode holds metadata such as schema information and tag values for the tables it manages. This metadata is essential for querying and managing data.

Within the cluster, a vnode is uniquely identified by the endpoint of its dnode and its vgroup ID. Management nodes oversee the creation and coordination of these vnodes to ensure smooth operation.

**Management Node (mnode):**  
The mnode, or management node, is the central logical unit within the TDengine cluster, responsible for monitoring the status of all dnodes and implementing load balancing (as shown by M1, M2, and M3 in Figure 15-1). It serves as the storage and management center for metadata (such as users, databases, and supertables) and is also known as the MetaNode.

To enhance availability and reliability, TDengine clusters allow multiple mnodes (up to three). These mnodes form a virtual mnode group that shares management responsibilities. TDengine employs the Raft consensus protocol to maintain consistency and reliability. Data updates in the mnode group must be performed on the leader node.

The first mnode is created automatically during cluster deployment, and additional mnodes can be added or removed manually through SQL commands. Each dnode can have one mnode, identified by its dnode's endpoint.

All dnodes in the cluster use internal messaging to automatically discover and communicate with the mnodes.

**Compute Node (qnode):**  
A qnode, or compute node, is a logical unit responsible for executing query tasks and processing `show` commands. Multiple qnodes can be configured within a cluster to enhance query performance and parallel processing capabilities (as shown by Q1, Q2, and Q3 in Figure 15-1).

Unlike dnodes, qnodes are not bound to specific databases, meaning they can process queries from multiple databases. Each dnode can host one qnode, identified by its endpoint.

When a client sends a query, it interacts with an mnode to obtain a list of available qnodes. If no qnode is available, query tasks are executed on vnodes. A scheduler allocates one or more qnodes to perform tasks based on the execution plan. Qnodes retrieve data from vnodes and forward results to other qnodes for further processing.

By introducing independent qnodes, TDengine separates storage from computation.

**Stream Compute Node (snode):**  
An snode, or stream compute node, is a specialized logical unit for handling stream processing tasks (as shown by S1, S2, and S3 in the architecture diagram). Multiple snodes can be configured across the cluster to support real-time data processing.

Similar to dnodes, snodes are not tied to specific streams, enabling them to handle multiple stream tasks concurrently. Each dnode can host one snode, identified by its endpoint.

When stream processing tasks are required, the mnode schedules available snodes to execute these tasks. If no snode is available, the tasks will be executed on vnodes.

Centralizing stream processing in snodes enhances real-time data handling capabilities.

**Virtual Node Group (VGroup):**  

A vgroup consists of vnodes spread across different dnodes. These vnodes use the Raft consensus protocol to ensure high availability and reliability. Write operations are performed on the leader vnode, and data is asynchronously replicated to follower vnodes, ensuring multiple physical copies.

The number of vnodes in a vgroup determines the replication factor. To create a database with N replicas, the cluster must have at least N dnodes. The replication factor can be specified during database creation, with the default value set to 1. TDengine's multi-replica design eliminates the need for expensive storage arrays while maintaining data reliability.

The mnode manages each vgroup and assigns a unique cluster-wide ID. Vnodes within the same vgroup share the same vgroup ID and act as backups. Although the number of vnodes in a vgroup can change dynamically, the vgroup ID remains constant, even if the vgroup is deleted.

This design ensures both data security and flexible replica management and scaling capabilities.

**Taosc**  

Taosc, the application driver for TDengine, bridges applications and the cluster. It provides native C/C++ interfaces and is embedded in connectors for various languages such as JDBC, C#, Python, Go, and Node.js, enabling these languages to interact with the database.

Applications communicate with the cluster through taosc rather than directly connecting to individual dnodes. Taosc retrieves and caches metadata, forwards queries and write requests to the appropriate dnode, and performs final aggregation, sorting, and filtering before returning results to the application. For JDBC, C/C++, C#, Python, Go, and Node.js interfaces, taosc runs on the physical node hosting the application.

Additionally, taosc can interact with taosAdapter, supporting distributed RESTful interfaces. This design ensures compatibility with multiple programming languages and interfaces while maintaining high performance and scalability.

### Communication Between Nodes

**Communication Method:**

Communication between dnodes within a TDengine cluster, as well as between application drivers and dnodes, is implemented using TCP. This method ensures stable and reliable data transmission.

To optimize network transmission performance and ensure data security, TDengine automatically compresses and decompresses transmitted data based on configuration, reducing network bandwidth usage. Additionally, TDengine supports digital signatures and authentication mechanisms to guarantee the integrity and confidentiality of data during transmission.

**FQDN Configuration:**

In a TDengine cluster, each dnode can have one or more Fully Qualified Domain Names (FQDN). To specify a dnode's FQDN, you can configure the `fqdn` parameter in the `taos.cfg` file. If not explicitly specified, the dnode will automatically use the hostname of the computer it runs on as its default FQDN.

While it's technically possible to set the FQDN parameter to an IP address in the `taos.cfg` file, this is not recommended. IP addresses may change with network configurations, potentially causing cluster disruptions. When using FQDN, ensure DNS services function properly, or configure the hosts file on relevant nodes to resolve the FQDN to the correct IP address. For better compatibility and portability, keep the `fqdn` value within 96 characters.

**Port Configuration:**

Each dnode in the TDengine cluster offers services using the port specified by the `serverPort` parameter. By default, this parameter is set to 6030. Adjusting the `serverPort` parameter allows flexible port configuration to meet various deployment environments and security policies.

**Cluster External Connection:**

A TDengine cluster can accommodate a single, multiple, or thousands of data nodes. Applications only need to connect to any one of the data nodes in the cluster. This design simplifies interactions between applications and the cluster, improving scalability and ease of use.

When launching the `taos` CLI, you can specify the connection information for a dnode using the following options:

- `-h`: Specifies the FQDN of the dnode, indicating which dnode the application should connect to.
- `-P`: Specifies the port for the dnode. If not provided, the default value from the `serverPort` parameter is used.

This method allows applications to connect flexibly to any dnode without needing to understand the cluster's topology.

**Internal Cluster Communication:**

Within the TDengine cluster, dnodes communicate with each other using TCP. When a dnode starts, it first obtains the endpoint information of the mnode. The newly started dnode then connects with the mnode in the cluster and exchanges information.

This process ensures the dnode can join the cluster and synchronize with the mnode, allowing it to receive and execute cluster-wide commands and tasks. Reliable data transmission between dnodes and between dnodes and mnodes guarantees the cluster's stable operation and efficient data processing.

The steps to obtain the mnode's endpoint information are as follows:

1. Check if the `dnode.json` file exists and can be opened to retrieve the mnode's endpoint information. If not, proceed to step 2.
2. Check the `taos.cfg` configuration file for the `firstEp` and `secondEp` parameters. These parameters may refer to ordinary nodes without mnodes, which will redirect to the appropriate mnode during connection. If these parameters are missing or invalid, proceed to step 3.
3. Set the dnode's own endpoint as the mnode endpoint and run independently.

Once the mnode's endpoint list is obtained, the dnode initiates a connection. If successful, it joins the working cluster. If not, it tries the next endpoint in the list. If all attempts fail, the dnode will pause for a few seconds before retrying.

**Mnode Selection:**

In a TDengine cluster, the mnode is a logical concept and does not correspond to a standalone code entity. Instead, the mnode's functions are managed by the `taosd` process on the server side.

During cluster deployment, the first dnode automatically assumes the mnode role. Users can later add or remove mnodes via SQL commands to meet cluster management needs. This design provides flexibility in the number and configuration of mnodes based on real-world application scenarios.

**Adding New Data Nodes:**

Once a dnode is started and running, the TDengine cluster becomes operational. To expand the cluster, follow these two steps to add new nodes:

1. Use the TDengine CLI to connect to an existing dnode. Then, execute the `create dnode` command to add a new dnode. This process guides users through the configuration and registration of the new dnode.
2. Set the `firstEp` and `secondEp` parameters in the `taos.cfg` file of the new dnode. These parameters should point to any two active dnodes in the existing cluster, ensuring the new dnode can join the cluster and communicate with other nodes.

**Redirection:**

In the TDengine cluster, newly started dnodes or `taosc` instances first need to connect with an mnode. However, users typically don't know which dnode is currently running the mnode. To solve this, TDengine employs a mechanism to ensure correct connections.

Rather than requiring dnodes or `taosc` to connect directly to a specific mnode, they only need to connect to any active dnode in the cluster. Each active dnode maintains a list of the current mnode endpoints and can forward connection requests to the appropriate mnode.

When a new dnode or `taosc` sends a connection request, if the contacted dnode is not the mnode, it replies with the mnode's endpoint list. The requester can then attempt to connect to the appropriate mnode using the provided list.

Additionally, TDengine uses an internal messaging mechanism to ensure all nodes in the cluster have the latest mnode endpoint list. When the mnode endpoint list changes, updates are quickly propagated to all dnodes and `taosc`.

### A Typical Message Flow

The following outlines the typical workflow for inserting data, illustrating the relationships between vnodes, mnodes, `taosc`, and applications.

<figure>
<Image img={imgFlow} alt="Typical operation flow in TDengine"/>
<figcaption>Figure 2. Typical operation flow in TDengine</figcaption>
</figure>

1. The application sends an insert request via JDBC or other API interfaces.
2. `Taosc` checks its cache to see if it has the vgroup-info of the database containing the table. If found, it proceeds to step 4. If not, `taosc` sends a `get vgroup-info` request to the mnode.
3. The mnode returns the vgroup-info for the database. This info includes the vgroup distribution (vnode IDs and corresponding dnode endpoints). If the database has N replicas, there will be N endpoints. The hash range of each table in the vgroup is also included. If `taosc` doesn’t receive a response from the mnode, it will try another mnode if available.
4. `Taosc` checks the cache for the table's metadata. If available, it proceeds to step 6. If not, it sends a `get meta-data` request to the vnode.
5. The vnode returns the table's metadata, including its schema.
6. `Taosc` sends an insert request to the leader vnode.
7. After inserting the data, the vnode responds to `taosc` with a success confirmation. If the vnode doesn’t respond, `taosc` assumes it is offline and, if the database has replicas, sends the request to another vnode in the vgroup.
8. `Taosc` notifies the application that the insertion was successful.

In step 2, since `taosc` does not initially know the mnode’s endpoint, it connects to the configured cluster service endpoint. If the contacted dnode isn’t an mnode, it returns the mnode endpoint list, and `taosc` retries the metadata request with the correct mnode.

For steps 4 and 6, if the cache doesn’t have the necessary info, `taosc` assumes the first vnode ID is the leader and sends the request. If the vnode isn’t the leader, it responds with the correct leader information, and `taosc` resends the request. Once successful, `taosc` caches the leader node information.

This process also applies to queries and computations. The complexity of these interactions is fully abstracted by `taosc`, making it transparent and easy for applications.

With `taosc` caching, the mnode is only accessed during the first operation on a table, preventing it from becoming a system bottleneck. However, as schemas or vgroups may change due to load balancing, `taosc` periodically communicates with the mnode to refresh its cache automatically.

## Storage Model and Data Partitioning

### Storage Model

The data stored in TDengine includes collected time-series data, as well as metadata and tag data related to databases and tables. This data is divided into three parts:

- **Time-series data:** This is the core storage object of TDengine, stored in `vnode` and consisting of `data`, `head`, and `last` files. The data volume is large, and query volume depends on the application scenario. It supports out-of-order writes but does not currently support delete operations. Updates are only allowed if the `update` parameter is set to 1. By adopting a one-collection-point-per-table model, data within a time range is stored sequentially. Writing to a table involves simple append operations, allowing multiple records to be read at once. This ensures optimal performance for inserts and queries on individual collection points.
- **Table metadata:** This includes tag information and table schema, stored in the `meta` file within `vnode`. It supports standard CRUD operations. The data volume is large, with one record per table. An LRU storage mechanism supports tag data indexing. TDengine enables multi-core, multi-threaded concurrent queries. With sufficient memory, metadata is fully stored in memory, allowing results from filtering millions of tags to return within milliseconds. Even with limited memory, TDengine supports fast queries across tens of millions of tables.
- **Database metadata:** This is stored in the `mnode` and includes system nodes, users, databases, and STable schema information. It supports CRUD operations. The data volume is relatively small and can be stored entirely in memory. Since clients cache this data, query volume is also low. Although managed centrally, this design does not create a performance bottleneck.

Compared to traditional NoSQL storage models, TDengine separates tag data from time-series data, offering two key advantages:

- **Reduced redundancy in tag data storage:** In conventional NoSQL or time-series databases, key-value models include timestamps, device IDs, and various tags in the key, leading to excessive duplication. Each record carries redundant tag information, wasting storage space. Modifying tags in historical data requires rewriting the entire dataset, which is costly. TDengine, by separating tag data from time-series data, avoids these issues, reducing storage space waste and lowering the cost of tag operations.
- **Efficient multi-table aggregation queries:** During multi-table aggregation, TDengine first identifies matching tables based on tag filters, then retrieves corresponding data blocks. This significantly reduces the dataset size to be scanned, greatly improving query efficiency. This strategy ensures TDengine maintains high-performance queries for large-scale time-series data, meeting complex analytical needs.

### Data Sharding

In managing large-scale data, horizontal scaling typically involves data sharding and partitioning strategies. TDengine implements data sharding using `vnode` and partitions time-series data by time range.

`Vnode` handles time-series data writes, queries, and computations, while also supporting load balancing, data recovery, and heterogeneous environments. To achieve this, each `dnode` is divided into multiple `vnode` based on its computing and storage resources. This process is transparent to applications and managed automatically by TDengine.

For a single collection point, regardless of data volume, a `vnode` has sufficient computing and storage capacity to handle it (e.g., one 16B record per second generates less than 0.5GB of raw data annually). Therefore, all data for a single table (or collection point) is stored within one `vnode`, avoiding data distribution across multiple `dnode`. Each `vnode` can store data for multiple collection points (tables) and can hold up to one million tables. All tables within a `vnode` belong to the same database.

TDengine 3.0 uses a consistent hashing algorithm to determine which `vnode` stores each table. Upon database creation, the cluster allocates the specified number of `vnode` and defines the range of tables each `vnode` handles. When a table is created, the cluster calculates the `vnode` ID based on the table name and creates the table on that `vnode`. If the database has multiple replicas, the cluster creates a `vgroup` instead of a single `vnode`. The number of `vnode` is limited only by the physical node's computing and storage resources.

Table metadata (e.g., schema, tags) is also stored within the `vnode`, not the `mnode`, effectively sharding metadata. This enhances parallel tag filtering and improves query performance.

### Data Partitioning

In addition to sharding through `vnode`, TDengine partitions time-series data by time periods. Each data file contains time-series data for a specific time range, with the range determined by the `duration` database parameter. This partitioning strategy simplifies data management and facilitates efficient data retention. Once data files exceed the retention period set by the `keep` parameter, they are automatically deleted.

TDengine also supports storing data from different time ranges on various paths and storage media. This flexibility simplifies hot and cold data management, allowing users to implement multi-tier storage based on actual needs, optimizing both storage costs and access performance.

In summary, TDengine efficiently slices large datasets across both `vnode` and time dimensions, achieving parallel management and horizontal scalability. This design accelerates data processing while providing flexible and scalable storage and query solutions, meeting diverse application requirements.

### Load Balancing and Scaling

Each `dnode` periodically reports its status to the `mnode`, including key metrics such as disk usage, memory size, CPU utilization, network conditions, and the number of `vnode`. This information is essential for health monitoring and resource scheduling.

Currently, load balancing is triggered manually. When new `dnode` are added to the cluster, users must initiate the load balancing process to maintain optimal cluster performance.

Over time, data distribution within the cluster may change, causing certain `vnode` to become hotspots. To address this, TDengine employs a Raft-based replica adjustment and data-splitting algorithm, enabling dynamic scaling and reallocation of data. This process is seamless during cluster operation, ensuring uninterrupted data writes and queries, maintaining system stability and availability.

## Data Write and Replication Process

In a database with **N** replicas, the corresponding `vgroup` will contain **N** `vnode` with the same ID. Among these `vnode`, one is designated as the **leader**, while the others act as **followers**. This master-slave architecture ensures data consistency and reliability.

When an application attempts to write a new record to the cluster, only the **leader vnode** can accept the write request. If a **follower vnode** accidentally receives the request, the cluster immediately informs `taosc` to redirect the request to the correct **leader vnode**, ensuring that all write operations occur on the correct node, maintaining data consistency and integrity.

This design guarantees data reliability and consistency in a distributed environment while providing high read-write performance.

### Leader Vnode Write Process

The **leader vnode** follows the write process outlined below:

<figure>
<Image img={imgLeader} alt="Leader write process"/>
<figcaption>Figure 3. Leader write process</figcaption>
</figure>

1. **Step 1**: The **leader vnode** receives the write request from the application, validates it, and proceeds to Step 2.
2. **Step 2**: The **vnode** writes the original data packet into the **Write-Ahead Log (WAL)**. If the `wal_level` is set to 2 and `wal_fsync_period` is set to 0, TDengine immediately writes the WAL data to disk, ensuring data recovery in case of a crash.
3. **Step 3**: If multiple replicas exist, the **leader vnode** forwards the data packet to the follower `vnode` in the same `vgroup`, including a version number.
4. **Step 4**: The data is written to memory and added to the **skip list**. If agreement is not reached, a rollback is triggered.
5. **Step 5**: The **leader vnode** returns a confirmation to the application, indicating a successful write.
6. **Step 6**: If any step from 2 to 4 fails, an error message is returned to the application.

### Follower Vnode Write Process

The write process for **follower vnode** is as follows:

<figure>
<Image img={imgFollower} alt="Follower write process"/>
<figcaption>Figure 4. Follower write process</figcaption>
</figure>

1. **Step 1**: The **follower vnode** receives the data insertion request forwarded by the **leader vnode**.
2. **Step 2**: The **vnode** writes the original data packet to the **WAL**. If `wal_level` is set to 2 and `wal_fsync_period` is 0, the data is immediately persisted to disk to prevent data loss during crashes.
3. **Step 3**: The data is written to memory, updating the **skip list**.

Compared to the **leader vnode**, the **follower vnode** process omits forwarding and confirmation steps. However, memory writes and WAL operations are identical.

### Leader Election

Each `vnode` maintains a **version number** that is persisted when data is stored in memory. Every time data, whether time-series or metadata, is updated, the version number increments to accurately track changes.

When a `vnode` starts, its role (leader or follower) is undetermined, and data is unsynchronized. To determine its role and synchronize data, the `vnode` establishes **TCP connections** with other nodes in the same `vgroup`. During these connections, the nodes exchange version numbers and term information. Using the Raft consensus algorithm, they determine the **leader** and assign follower roles.

This mechanism ensures coordination and consistency among `vnode` in a distributed environment, maintaining system stability.

### Synchronous Replication

In TDengine, when a **leader vnode** receives a write request and forwards it to other replicas, it does not immediately confirm the write success to the application. Instead, the **leader vnode** waits for more than half of the replicas (including itself) to reach consensus before confirming the write. If this consensus is not reached within a given time, the **leader vnode** returns an error to the application, indicating a failed write.

This synchronous replication ensures consistency and safety across replicas but presents performance challenges. To balance consistency and performance, TDengine implements a **pipeline replication algorithm**.

This algorithm allows multiple write confirmations across database connections to occur simultaneously rather than sequentially. Even if one replica’s confirmation is delayed, it does not block other writes. This approach ensures that TDengine achieves high write performance with strong consistency, meeting high throughput and low latency requirements.

### Membership Changes

During **scaling or node migration**, the number of replicas within a `vgroup` may need adjustment. The amount of data impacts the time required for replication between replicas, and excessive replication can block read and write processes.

To address this, TDengine extends the **Raft protocol** by introducing the **learner role**. Learners play a critical role during replication but do not participate in voting. They only receive replicated data. Since learners do not vote, their confirmation is not required to consider a write successful.

When the data difference between a learner and the leader is large, the learner synchronizes using a **snapshot**. After the snapshot synchronization, the learner catches up with the leader’s logs. Once the data gap is minimal, the learner transitions to a **follower** role and begins participating in writes and elections.

### Redirection

When `taosc` writes new records to the TDengine cluster, it must first identify the current **leader vnode** since only the leader can process write requests. If `taosc` sends the request to a **follower vnode**, the cluster immediately informs `taosc` to redirect to the correct **leader vnode**.

To ensure that write requests are correctly routed, `taosc` maintains a **local cache** of the node topology. Upon receiving cluster notifications, `taosc` updates the cache with the latest topology and resends the request to the identified **leader vnode**. The cache is also updated for future use.

This mechanism ensures that applications using `taosc` to access TDengine do not need to handle network retries. Regardless of node changes, `taosc` automatically manages the routing, ensuring write requests always reach the appropriate **leader vnode**.

## Cache and Persistence

### Time-Series Data Cache

TDengine adopts a unique **time-driven cache management strategy**, also known as the **write-driven cache management mechanism**. This approach differs from traditional read-driven caching models by prioritizing the storage of the most recently written data in the cluster’s cache. When the cache reaches its threshold, the system performs batch writes of the earliest data to disk. Specifically, each `vnode` has its own independent memory space divided into fixed-size blocks, with memory isolation across different `vnode`. During the write process, data is appended sequentially, similar to logging, and each `vnode` maintains its own **SkipList** structure for fast data retrieval. Once more than one-third of the memory blocks are full, the system triggers a **data flush to disk** and directs new writes to a fresh memory block. This strategy ensures that one-third of the memory blocks in a `vnode` retain the latest data, achieving caching while ensuring efficient querying. The size of each `vnode`’s memory can be configured using the **`buffer`** parameter.

Considering the characteristics of IoT data, where users often prioritize real-time insights, TDengine optimizes by storing the **most recent (current state) data** in the cache. Specifically, TDengine stores incoming data directly in memory to quickly respond to queries and analysis of the most recent records, improving the database’s response time. With proper configuration, TDengine can serve as a data cache, eliminating the need for Redis or other caching systems. This design simplifies the system architecture and reduces operational costs. However, note that when TDengine restarts, **cached data is cleared**, and previously cached data is batch-written to disk without being reloaded into memory, unlike specialized key-value cache systems.

### Persistent Storage

TDengine employs a **data-driven strategy** for persisting cached data. When the cache in a `vnode` reaches a certain level, TDengine launches a **flush thread** to write the cached data to persistent storage devices, preventing write operations from being blocked. During this process, new database log files are created for the flush operation, and old log files are deleted after a successful flush to avoid unrestricted log growth.

To leverage the characteristics of time-series data, TDengine divides each `vnode`’s data into **multiple files**, with each file storing data for a specific number of days, defined by the **`duration`** parameter. This segmented storage allows queries for specific time ranges to identify relevant files without relying on indexes, greatly improving read efficiency.

Collected data typically has a **retention period** determined by the **`keep`** parameter. When data exceeds this retention period, the cluster automatically removes the outdated files, freeing storage space.

When both `duration` and `keep` parameters are set, a typical `vnode` will have `(keep/duration) + 1` data files. The total number of files should be kept within a reasonable range, ideally between 10 and 100. This principle helps determine a suitable `duration`. While the `keep` parameter can be adjusted, the `duration` parameter is immutable once set.

Within each data file, table data is stored in **blocks**. A table may contain one or more data file blocks. Within each block, data is stored in a **columnar format** occupying contiguous storage space, significantly improving read performance. The block size is controlled by the **`maxRows`** parameter (default: 4096). Setting this value appropriately is crucial: a value too high may slow searches for specific time ranges, while a value too low may result in large indexes and reduced compression efficiency, affecting read speeds.

Each data block (with a `.data` extension) has an accompanying **index file** (with a `.head` extension) containing summary information such as the block’s offset, start and end times. Additionally, there is a **last file** (with a `.last` extension) to prevent fragmentation during flushes. If the number of records in a flush does not meet the **`minRows`** requirement, they are temporarily stored in the `last` file. These records are merged with new flushes before being written to the data block.

During data writes to disk, the compression of data depends on the **`comp`** parameter. TDengine offers three compression levels:

- **0**: No compression
- **1**: Level 1 compression using algorithms like delta-delta encoding, Simple8B, Zig-Zag encoding, and LZ4
- **2**: Level 2 compression, which applies additional general-purpose compression for higher efficiency

### Pre-Computations

To significantly improve query performance, TDengine stores **statistical summaries** (e.g., maximum, minimum, and total values) in the header of each data block. These **pre-computed units** allow queries involving these metrics to access the summary directly without reading the full block. This optimization reduces disk I/O bottlenecks, enhancing query speed in scenarios where I/O is critical.

In addition to pre-computed units, TDengine supports various **downsampling storage methods**:

- **Rollup SMA**: Automatically stores downsampled data with three levels of aggregation, each with configurable aggregation intervals and retention periods. This is ideal for scenarios focusing on data trends, as it reduces storage costs and improves query performance.
- **Time-Range-Wise SMA**: Stores data based on aggregation results, suitable for frequent **interval queries**. This method uses logic similar to standard stream processing and supports delayed data handling with **watermarks**, though the query results may exhibit some latency.

### Multi-Tier and Object Storage

By default, TDengine stores all data in the `/var/lib/taos` directory. To expand storage capacity, reduce file read bottlenecks, and increase data throughput, TDengine allows multiple mounted disks to be used via the **`dataDir`** configuration parameter.

TDengine also supports **tiered storage**, enabling users to store data from different time ranges on different storage devices. This approach separates **hot data** from **cold data**, optimizing resource utilization and reducing costs. For example, the latest data, which requires frequent access, can be stored on high-performance SSDs, while older, less frequently accessed data can be moved to more cost-effective HDDs.

To further reduce storage costs, TDengine supports storing time-series data in **object storage systems**. Thanks to its innovative design, querying time-series data from object storage often achieves **50% of the performance** of local disk storage and, in some cases, can even match local performance. TDengine also allows users to **delete and update** time-series data stored in object storage.
