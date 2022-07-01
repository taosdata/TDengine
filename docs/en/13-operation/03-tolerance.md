---
sidebar_label: Fault Tolerance
title: Fault Tolerance & Disaster Recovery
---

## Fault Tolerance

TDengine uses **WAL**, i.e. Write Ahead Log, to achieve fault tolerance and high reliability.

When a data block is received by TDengine, the original data block is first written into WAL. The log in WAL will be deleted only after the data has been written into data files in the database. Data can be recovered from WAL in case the server is stopped abnormally for any reason and then restarted.

There are 2 configuration parameters related to WAL:

- walLevel：
  - 0：wal is disabled 
  - 1：wal is enabled without fsync 
  - 2：wal is enabled with fsync
- fsync：This parameter is only valid when walLevel is set to 2. It specifies the interval, in milliseconds, of invoking fsync. If set to 0, it means fsync is invoked immediately once WAL is written.

To achieve absolutely no data loss, walLevel should be set to 2 and fsync should be set to 1. There is a performance penalty to the data ingestion rate. However, if the concurrent data insertion threads on the client side can reach a big enough number, for example 50, the data ingestion performance will be still good enough. Our verification shows that the drop is only 30% when fsync is set to 3,000 milliseconds.

## Disaster Recovery

TDengine uses replication to provide high availability and disaster recovery capability.

A TDengine cluster is managed by mnode. To ensure the high availability of mnode, multiple replicas can be configured by the system parameter `numOfMnodes`. The data replication between mnode replicas is performed in a synchronous way to guarantee metadata consistency.

The number of replicas for time series data in TDengine is associated with each database. There can be many databases in a cluster and each database can be configured with a different number of replicas. When creating a database, parameter `replica` is used to configure the number of replications. To achieve high availability, `replica` needs to be higher than 1.

The number of dnodes in a TDengine cluster must NOT be lower than the number of replicas for any database, otherwise it would fail when trying to create a table.

As long as the dnodes of a TDengine cluster are deployed on different physical machines and the replica number is higher than 1, high availability can be achieved without any other assistance. For disaster recovery, dnodes of a TDengine cluster should be deployed in geographically different data centers.
