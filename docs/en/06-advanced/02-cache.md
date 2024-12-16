---
title: Caching
slug: /advanced-features/caching
---

In the big data applications of the Internet of Things (IoT) and the Industrial Internet of Things (IIoT), the value of real-time data often far exceeds that of historical data. Enterprises not only need data processing systems to have efficient real-time writing capabilities but also need to quickly obtain the latest status of devices or perform real-time calculations and analyses on the latest data. Whether it's monitoring the status of industrial equipment, tracking vehicle locations in the Internet of Vehicles, or real-time readings of smart meters, current values are indispensable core data in business operations. These data are directly related to production safety, operational efficiency, and user experience.

For example, in industrial production, the current operating status of production line equipment is crucial. Operators need to monitor key indicators such as temperature, pressure, and speed in real-time. If there is an anomaly in the equipment, these data must be presented immediately so that process parameters can be quickly adjusted to avoid downtime or greater losses. In the field of the Internet of Vehicles, taking DiDi as an example, the real-time location data of vehicles is key to optimizing dispatch strategies and improving operational efficiency on the DiDi platform, ensuring that each passenger gets on the vehicle quickly and enjoys a higher quality travel experience.

At the same time, dashboard systems and smart meters, as windows for on-site operations and user ends, also need real-time data support. Whether it's factory managers obtaining real-time production indicators through dashboards or household users checking the usage of smart water and electricity meters at any time, real-time data not only affects operational and decision-making efficiency but also directly relates to user satisfaction with the service.

## Limitations of Traditional Caching Solutions

To meet these high-frequency real-time query needs, many enterprises choose to integrate caching technologies like Redis into their big data platforms, enhancing query performance by adding a caching layer between the database and applications. However, this approach also brings several problems:

- Increased system complexity: Additional deployment and maintenance of the cache cluster are required, raising higher demands on system architecture.
- Rising operational costs: Additional hardware resources are needed to support the cache, increasing maintenance and management expenses.
- Consistency issues: Data synchronization between the cache and the database requires additional mechanisms to ensure consistency, otherwise data inconsistencies may occur.

## TDengine's Solution: Built-in Read Cache

To address these issues, TDengine has designed and implemented a read cache mechanism specifically for high-frequency real-time query scenarios in IoT and IIoT. This mechanism automatically caches the last record of each table in memory, thus meeting users' real-time query needs for current values without introducing third-party caching technologies.

TDengine uses a time-driven cache management strategy, prioritizing the storage of the latest data in the cache, allowing for quick results without needing to access the hard disk. When the cache capacity reaches the set limit, the system will batch-write the earliest data to the disk, enhancing query efficiency and effectively reducing the disk's write load, thereby extending the hardware's lifespan.

Users can customize the cache mode by setting the `cachemodel` parameter, including caching the latest row of data, the most recent non-NULL value of each column, or caching both rows and columns. This flexible design is particularly important in IoT scenarios, making real-time queries of device status more efficient and accurate.

This built-in read cache mechanism significantly reduces query latency, avoids the complexity and operational costs of introducing external systems like Redis, and reduces the pressure of frequent queries on the storage system, greatly enhancing the overall throughput of the system. It ensures stable and efficient operation even in high-concurrency scenarios. Through read caching, TDengine provides a more lightweight real-time data processing solution, not only optimizing query performance but also reducing overall operational costs, providing strong technical support for IoT and IIoT users.

## TDengine's Read Cache Configuration

When creating a database, users can choose whether to enable the caching mechanism to store the latest data of each subtable in that database. This caching mechanism is controlled by the database creation parameter `cachemodel`. The parameter `cachemodel` has the following 4 options:

- none: no caching
- last_row: caches the most recent row of data from the subtable, significantly improving the performance of the `last_row` function
- last_value: caches the most recent non-NULL value of each column from the subtable, significantly improving the performance of the `last` function when there are no special effects (such as WHERE, ORDER BY, GROUP BY, INTERVAL)
- both: caches both the most recent row and column, equivalent to the behaviors of `last_row` and `last_value` simultaneously effective

When using database read caching, the `cachesize` parameter can be used to configure the memory size for each vnode.

- cachesize: represents the memory size used to cache the most recent data of subtables in each vnode. The default is 1, the range is [1, 65536], in MB. It should be configured reasonably according to the machine memory.

For specific database creation, related parameters, and operation instructions, please refer to [Creating a Database](../../tdengine-reference/sql-manual/manage-databases/)

## Caching Practices for Real-Time Data Queries

This section takes smart electric meters as an example to look in detail at how LAST caching improves the performance of real-time data queries. First, use the taosBenchmark tool to generate the time-series data of smart electric meters needed for this chapter.

```shell
# taosBenchmark -d power -Q --start-timestamp=1600000000000 --tables=10000 --records=10000 --time-step=10000 -y
```

The above command, the taosBenchmark tool in TDengine created a test database for electric meters named `power`, generating a total of 1 billion time-series data entries. The timestamp of the time-series data starts from `1600000000000 (2020-09-13T20:26:40+08:00)`, with the supertable `meters` containing 10,000 devices (subtables), each device having 10,000 data entries, and the data collection frequency is 10 seconds per entry.

To query the latest current and timestamp data of any electric meter, execute the following SQL:

```sql
taos> select last(ts,current) from meters;
        last(ts)         |    last(current)     |
=================================================
 2020-09-15 00:13:10.000 |            1.1294620 |
Query OK, 1 row(s) in set (0.353815s)

taos> select last_row(ts,current) from meters;
      last_row(ts)       |  last_row(current)   |
=================================================
 2020-09-15 00:13:10.000 |            1.1294620 |
Query OK, 1 row(s) in set (0.344070s)
```

If you want to use caching to query the latest timestamp data of any electric meter, execute the following SQL and check if the database cache is effective.

```sql
taos> alter database power cachemodel 'both' ;
Query OK, 0 row(s) affected (0.046092s)

taos> show create database power\G;
*************************** 1.row ***************************
       Database: power
Create Database: CREATE DATABASE `power` BUFFER 256 CACHESIZE 1 CACHEMODEL 'both' COMP 2 DURATION 14400m WAL_FSYNC_P...
Query OK, 1 row(s) in set (0.000282s)
```

Query the latest real-time data of the electric meter again; the first query will perform cache computation, significantly reducing the latency of subsequent queries.

```sql
taos> select last(ts,current) from meters;
        last(ts)         |    last(current)     |
=================================================
 2020-09-15 00:13:10.000 |            1.1294620 |
Query OK, 1 row(s) in set (0.044021s)

taos> select last_row(ts,current) from meters;
      last_row(ts)       |  last_row(current)   |
=================================================
 2020-09-15 00:13:10.000 |            1.1294620 |
Query OK, 1 row(s) in set (0.046682s)
```

As can be seen, the query latency has been reduced from 353/344ms to 44ms, an improvement of approximately 8 times.
