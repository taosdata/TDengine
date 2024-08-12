---
title: Load Balance
sidebar_label: Load Balance
description: This document describes how TDengine implements load balancing.
---

The load balance in TDengine is mainly about processing data series data. TDengine employes builtin hash algorithm to distribute all the tables, sub-tables and their data of a database across all the vgroups that belongs to the database. Each table or sub-table can only be handled by a single vgroup, while each vgroup can process multiple table or sub-table. 

The number of vgroup can be specified when creating a database, using the parameter `vgroups`.

```sql
create database db0 vgroups 100;
```

The proper value of `vgroups` depends on available system resources. Assuming there is only one database to be created in the system, then the number of `vgroups` is determined by the available resources from all dnodes. In principle more vgroups can be created if you have more CPU and memory. Disk I/O is another important factor to consider. Once the bottleneck shows on disk I/O, more vgroups may downgrad the system performance significantly. If multiple databases are to be created in the system, then the total number of `vroups` of all the databases are dependent on the available system resources. It needs to be careful to distribute vgroups among these databases, you need to consider the number of tables, data writing frequency, size of each data row for all these databases. A recommended practice is to firstly choose a starting number for `vgroups`, for example double of the number of CPU cores, then try to adjust and optimize system configurations to find the best setting for `vgroups`, then distribute these vgroups among databases.

Furthermode, TDengine distributes the vgroups of each database equally among all dnodes. In case of replica 3, the distribution is even more complex, TDengine tries its best to prevent any dnode from becoming a bottleneck.

TDegnine utilizes the above ways to achieve load balance in a cluster, and finally achieve higher throughput.

Once the load balance is achieved, after some operations like deleting tables or dropping databases, the load across all dnodes may become imbalanced, the method of rebalance will be provided in later versions. However, even without explicit rebalancing, TDengine will try its best to achieve new balance without manual interfering when a new database is created.
