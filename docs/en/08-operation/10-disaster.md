---
title: Fault Tolerance and Disaster Recovery
slug: /operations-and-maintenance/fault-tolerance-and-disaster-recovery
---

To prevent data loss and accidental deletion, TDengine provides comprehensive data backup, recovery, fault tolerance, and real-time data synchronization across different locations to ensure the security of data storage. This section briefly explains fault tolerance and disaster recovery in TDengine.

## Fault Tolerance

TDengine supports the WAL mechanism to achieve data fault tolerance, ensuring high data reliability. When TDengine receives a data packet from an application, it first writes the original data packet to the database log file. After the data is successfully written to the database data file, the corresponding WAL is deleted. This ensures that TDengine can recover data from the database log file in the event of a power failure or other factors causing service restart, thus preventing data loss. The relevant configuration parameters are as follows:

- wal_level: WAL level, 1 means write WAL but do not execute fsync; 2 means write WAL and execute fsync. The default value is 1.
- wal_fsync_period: When wal_level is set to 2, the period for executing fsync; when wal_fsync_period is set to 0, it means execute fsync immediately after each write.

To guarantee 100% data integrity, set wal_level to 2 and wal_fsync_period to 0. This setting will reduce the write speed. However, if the number of threads writing data on the application side reaches a certain number (more than 50), the performance of writing data will also be quite good, only about 30% lower than when wal_fsync_period is set to 3000ms.

## Disaster Recovery

Deploy two TDengine Enterprise clusters in two different data centers to achieve data disaster recovery using their data replication capabilities. Assume the two clusters are Cluster A and Cluster B, where Cluster A is the source cluster, handling write requests and providing query services. Cluster B can consume new data written in Cluster A in real-time and synchronize it to Cluster B. In the event of a disaster causing the data center with Cluster A to be unavailable, Cluster B can be activated as the primary node for data writing and querying.

The following steps describe how to easily set up a data disaster recovery system between two TDengine Enterprise clusters:

- Step 1: Create a database db1 in Cluster A and continuously write data to it.

- Step 2: Access the taosExplorer service of Cluster A through a web browser, usually at the port 6060 of the IP address where the TDengine cluster is located, such as `http://localhost:6060`.

- Step 3: Access TDengine Cluster B, create a database db2 with the same parameter configuration as the database db1 in Cluster A.

- Step 4: Access the taosExplorer service of Cluster B through a web browser, find db2 on the "Data Browser" page, and obtain the DSN of the database in the "View Database Configuration" option, such as `taos+ws://root:taosdata@clusterB:6041/db2`

- Step 5: Add a data synchronization task on the "System Management - Data Synchronization" page of the taosExplorer service, fill in the configuration information of the task with the database db1 to be synchronized and the target database db2's DSN, and start data synchronization after creating the task.

- Step 6: Access Cluster B, and you can see that the database db2 in Cluster B continuously writes data from the database db1 in Cluster A until the data volumes of the databases in both clusters are roughly equal. At this point, a simple data disaster recovery system based on

TDengine Enterprise is set up.
