---
sidebar_label: Fault Tolerance
title: Fault Tolerance & Disaster Recovery
---

## Fault Tolerance

TDengine uses **WAL**, i.e. Write Ahead Log, to achieve fault tolerance and high reliability.

When a data block is received by TDengine, the original data block is firstly written into WAL. The log in WAL will be deleted only after the data has been written into data files in the database. Data can be recovered from WAL in case the server is stopped abnormally due to any reason and then restarted.

There are 2 configuration parameters related to WAL:

- walLevel：0：wal is disabled; 1：wal is enabled without fsync; 2：wal is enabled with fsync.
- fsync：only valid when walLevel is set to 2, it specified the interval of invoking fsync. If set to 0, it means fsync is invoked immediately once WAL is written.

To achieve absolutely no data loss, walLevel needs to be set to 2 and fsync needs to be set to 1. The penalty is the performance of data ingestion downgrades. However, if the concurrent threads of data insertion on the client side can reach a big enough number, for example 50, the data ingestion performance would be still good enough, our verification shows that the drop is only 30% compared to fsync is set to 3,000 milliseconds.

## Disaster Recovery

TDengine uses replications to provide high availability and disaster recovery capability.

TDengine cluster is managed by mnode. To make sure the high availability of mnode, multiple replicas can be configured by system parameter `numOfMnodes`. The data replication between mnode replicas is in synchronous way to guarantee the metadata consistency.

The number of replicas for time series data in TDengine is associated with each database, there can be a lot of databases in a cluster while each database can be configured with a different number of replicas. When creating a database, parameter `replica` is used to configure the number of replications. To achieve high availability, `replica` needs to be higher than 1.

The number of dnodes in a TDengine cluster must NOT be lower than the number of replicas for any database, otherwise it would fail when trying to create table.

As long as the dnodes of a TDengine cluster are deployed on different physical machines and replica number is set to bigger than 1, high availability can be achieved without any other assistance. If dnodes of TDengine cluster are deployed in geographically different data centers, disaster recovery can be achieved too.
