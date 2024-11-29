---
title: Fault Tolerance and Disaster Recovery
slug: /operations-and-maintenance/fault-tolerance-and-disaster-recovery
---

To prevent data loss or accidental deletion, TDengine provides comprehensive data backup, recovery, fault tolerance, and real-time remote data synchronization features to ensure the security of data storage. This section briefly describes the fault tolerance and disaster recovery features in TDengine.

## Fault Tolerance

TDengine supports the WAL (Write-Ahead Logging) mechanism to implement fault tolerance, ensuring high data reliability. When TDengine receives a request packet from an application, it first writes the raw data packet to a database log file, and only deletes the corresponding WAL after the data is successfully written to the database data file. This ensures that TDengine can recover data from the database log file in the event of a power outage or other factors that cause service restarts, avoiding data loss. The relevant configuration parameters are as follows:

- wal_level: WAL level, 1 means writing WAL but not executing fsync; 2 means writing WAL and executing fsync. The default value is 1.
- wal_fsync_period: When wal_level is set to 2, this specifies the fsync execution period. When wal_fsync_period is set to 0, fsync is executed immediately after each write.

To 100% guarantee no data loss, wal_level should be set to 2 and wal_fsync_period to 0. In this case, the write speed will decrease. However, if the number of write threads started by the application reaches a certain amount (more than 50), the performance of data writing will still be quite good, with only about a 30% drop compared to when wal_fsync_period is set to 3000ms.

## Disaster Recovery

To achieve disaster recovery, two TDengine Enterprise clusters can be deployed in two different data centers, utilizing their data replication capabilities. Suppose there are two clusters, Cluster A and Cluster B, where Cluster A is the source cluster, handling write requests and providing query services. Cluster B can consume the new data written to Cluster A in real-time and synchronize it to Cluster B. In the event of a disaster that renders the data center where Cluster A is located unavailable, Cluster B can be activated as the primary node for data writing and querying.

The following steps describe how to easily set up a disaster recovery system between two TDengine Enterprise clusters:

- Step 1: Create a database `db1` in Cluster A and continuously write data to this database.

- Step 2: Access the taosExplorer service of Cluster A through a web browser. The access address is usually the IP address of the TDengine cluster on port 6060, such as `http://localhost:6060`.

- Step 3: Access Cluster B and create a database `db2` with the same parameter configuration as database `db1` in Cluster A.

- Step 4: Through a web browser, access the taosExplorer service of Cluster B, find `db2` on the "Data Explorer" page, and retrieve the DSN of the database in the "View Database Configuration" option, such as `taos+ws://root:taosdata@clusterB:6041/db2`.

- Step 5: On the "System Management - Data Synchronization" page of the taosExplorer service, add a new data synchronization task. In the task configuration, enter the DSN of the source database `db1` and the target database `db2`. Once the task is created, you can start data synchronization.

- Step 6: Access Cluster B and observe that data from database `db1` in Cluster A is continuously written to database `db2` in Cluster B until the data in both clusters is roughly synchronized. At this point, a simple disaster recovery system based on TDengine Enterprise is set up.

The TDengine Enterprise disaster recovery system setup is complete.
