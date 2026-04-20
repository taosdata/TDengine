---
sidebar_label: Data Replication
title: Data Replication
description: This document describes how to replicate data between different TDengine Cloud instances.
---

TDengine Cloud provides full support for data replication. You can replicate data from the current instance of TDengine Cloud to another instance, both instances can be in the same region or in different regions.

Customers just need to click **Add New Replication** button to open the dialog for creating data replication task. Firstly, you need to select the database of the current instance of TDengine Cloud in the **From Database** selection, and then copy the token of the target TDengine Cloud instance to the **To Instance Token** input. After that, TDengine Cloud will get the database list of the target instance automatically. Finally, customers only need to select a database of the target instance in the **To Database** selection, and then click the **Create** button to create a data replication task.

In the **Operate** area of each data replication task, customers can delete and check the status of the task. Also customers can globally refresh all data replication tasks after clicking the **Refresh** button.
