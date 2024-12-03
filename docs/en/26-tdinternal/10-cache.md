---
title: Data Caching
slug: /inside-tdengine/data-caching
---

In modern Internet of Things (IoT) and Industrial Internet of Things (IIoT) applications, efficient data management is crucial for system performance and user experience. To address the real-time read and write demands in high concurrency environments, TDengine has designed a complete caching mechanism, including write cache, read cache, metadata cache, and file system cache. These caching mechanisms are closely integrated to optimize data query response speed and improve data writing efficiency, while ensuring data reliability and high system availability. By flexibly configuring cache parameters, TDengine offers users the best balance between performance and cost.

## Write Cache

TDengine employs an innovative time-driven cache management strategy, also known as write-driven cache management mechanism. This strategy differs from the traditional read-driven cache model, with the core idea being to prioritize the storage of newly written data in the cache. When the cache capacity reaches a preset threshold, the system will batch write the earliest stored data to the disk, thus achieving a dynamic balance between the cache and the disk.

In IoT data applications, users often focus most on the most recently generated data, i.e., the current status of the devices. TDengine takes full advantage of this business characteristic by prioritizing the storage of the most recently arrived current status data in the cache, allowing users to quickly access the information they need.

To achieve distributed storage and high availability of data, TDengine introduces the concept of virtual nodes (vnode). Each vnode can have up to 3 replicas, which together form a vnode group, abbreviated as vgroup. When creating a database, users need to determine the write cache size for each vnode to ensure reasonable data distribution and efficient storage.

The two key parameters `vgroups` and `buffer` during database creation determine how many vgroups handle the data in the database and how much write cache is allocated to each vnode. By properly configuring these two parameters, users can adjust the database's performance and storage capacity according to actual needs, thereby achieving optimal performance and cost benefits.

For example, the following SQL creates a database containing 10 vgroups, each vnode using 256MB of memory.

```sql
CREATE DATABASE POWER VGROUPS 10 BUFFER 256 CACHEMODEL 'NONE' PAGES 128 PAGESIZE 16;
```

The larger the cache, the better, but increasing the cache beyond a certain threshold no longer helps improve writing performance.

## Read Cache

TDengine's read cache mechanism is specifically designed for high-frequency real-time query scenarios, particularly suitable for IoT and IIoT businesses that need to keep real-time track of device status. In these scenarios, users are often most concerned with the latest data, such as the current readings or status of the devices.

By setting the cachemodel parameter, TDengine users can flexibly choose the suitable cache mode, including caching the latest row of data, the most recent non-NULL value of each column, or caching both row and column data. This flexibility allows TDengine to provide precise optimization based on specific business needs, especially prominent in IoT scenarios, helping users quickly access the latest status of devices.

This design not only reduces query response latency but also effectively alleviates the I/O pressure on the storage system. In high-concurrency scenarios, the read cache helps the system maintain higher throughput, ensuring the stability of query performance. With TDengine's read cache, users no longer need to integrate external cache systems like Redis, avoiding system architecture complexity and significantly reducing maintenance and deployment costs.

Furthermore, TDengine's read cache mechanism can also be flexibly adjusted according to actual business scenarios. In scenarios where data access hotspots are concentrated on the latest records, this built-in cache can significantly enhance user experience, making the acquisition of key data faster and more efficient. Compared to traditional caching solutions, this seamlessly integrated caching strategy not only simplifies the development process but also provides higher performance assurance for users.

For more detailed information about TDengine's read cache, see [Read Cache](../../advanced-features/caching/)

## Metadata Cache

To enhance the efficiency of query and write operations, each vnode is equipped with a caching mechanism for storing metadata it has previously accessed. The size of this metadata cache is determined by two parameters set during database creation: pages and pagesize. The pagesize parameter is in KB and specifies the size of each cache page. The following SQL creates a metadata cache for each vnode in the database power with 128 pages, each page being 16KB

```sql
CREATE DATABASE POWER PAGES 128 PAGESIZE 16;
```

## File System Cache

TDengine uses WAL technology as a basic data reliability guarantee. WAL is an advanced data protection mechanism designed to ensure rapid data recovery in the event of a failure. Its core principle is that before data is actually written to the data storage layer, it is first recorded in a log file. Thus, even if the cluster encounters a crash or other failure, data safety can still be ensured.

TDengine uses these log files to recover the state before the failure. During the writing process to WAL, data is written to disk files in a sequential append mode. Therefore, the file system cache plays a crucial role in this process, significantly impacting writing performance. To ensure data is truly written to disk, the system calls the fsync function, which is responsible for forcibly writing data from the file system cache to the disk.

Database parameters `wal_level` and `wal_fsync_period` together determine the behavior of WAL saving.

- `wal_level`: This parameter controls the level of WAL saving. Level 1 means data is only written to the WAL, but the fsync function is not immediately executed; level 2 means that the fsync function is executed simultaneously with the WAL write. By default, `wal_level` is set to 1. Although executing the fsync function can enhance data durability, it can also reduce write performance.
- `wal_fsync_period`: When `wal_level` is set to 2, this parameter controls the frequency of executing fsync. Setting it to 0 means fsync is executed immediately after each write, which can ensure data safety but may sacrifice some performance. When set to a value greater than 0, it represents the fsync period, with a default of 3000, ranging from [1, 180000], in milliseconds.

```sql
CREATE DATABASE POWER WAL_LEVEL 2 WAL_FSYNC_PERIOD 3000;
```

When creating a database, users can choose different parameter settings according to their needs to find the best balance between performance and reliability:

- Performance priority: Write data to the WAL, but do not immediately execute the fsync operation; the newly written data is only saved in the file system cache and has not yet been synchronized to the disk. This configuration can significantly improve write performance.
- Reliability priority: Write data to the WAL and execute the fsync operation simultaneously, immediately synchronizing the data to the disk, ensuring data persistence and higher reliability.
