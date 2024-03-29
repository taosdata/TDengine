---
title: Fault Tolerance and Disaster Recovery
description: This document describes how TDengine provides fault tolerance and disaster recovery.
---

## Fault Tolerance

TDengine uses **WAL**, i.e. Write Ahead Log, to achieve fault tolerance and high reliability.

When a data block is received by TDengine, the original data block is first written into WAL. The log in WAL will be deleted only after the data has been written into data files in the database. Data can be recovered from WAL in case the server is stopped abnormally for any reason and then restarted.

There are 2 configuration parameters related to WAL:

- wal_level: Specifies the WAL level. 1 indicates that WAL is enabled but fsync is disabled. 2 indicates that WAL and fsync are both enabled. The default value is 1.
- wal_fsync_period: This parameter is only valid when wal_level is set to 2. It specifies the interval, in milliseconds, of invoking fsync. If set to 0, it means fsync is invoked immediately once WAL is written.

To achieve absolutely no data loss, set wal_level to 2 and wal_fsync_period to 0. There is a performance penalty to the data ingestion rate. However, if the concurrent data insertion threads on the client side can reach a big enough number, for example 50, the data ingestion performance will be still good enough. Our verification shows that the drop is only 30% when wal_fsync_period is set to 3000 milliseconds.

## Disaster Recovery

TDengine provides disaster recovery by using taosX to replicate data between two TDengine clusters which are deployed in two distant data centers. Assume there are two TDengine clusters, A and B, A is the source and B is the target, and A takes the workload of writing and querying. You can deploy `taosX` in the data center where cluster A resides in, `taosX` consumes the data written into cluster A and writes into cluster B. If the data center of cluster A is disrupted because of disaster, you can switch to cluster B to take the workload of data writing and querying, and deploy a `taosX` in the data center of cluster B to replicate data from cluster B to cluster A if cluster A has been recovered, or another cluster C if cluster A has not been recovered. 

You can use the data replication feature of `taosX` to build more complicated disaster recovery solution.

taosX is only provided in TDengine enterprise edition, for more details please contact business@tdengine.com.
