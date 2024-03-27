---
sidebar_label: OpenTSDB
title: OpenTSDB Data Source
description: This document describes how to extract data from OpenTSDB into a TDengine Cloud instance.
---

OpenTSDB data source is the process of writing data from the OpenTSDB server to the currently selected TDengine Cloud instance via a connection agent.

## Prerequisites

- Create an empty database to store your Kafka data. For more information, see [Database](../../../programming/model/#create-database).
- Ensure that the connection agent is running on a machine located on the same network as your OPC Server. For more information, see [Install Connection Agent](../install-agent/).

## Procedure

### 1. Add Source

In TDengine Cloud, open **Data In** page. On the **Data Sources** tab, click **Add Data Source** button to open the new data source page. In the **Name** input, fill in the name of the data source and select the type of **OpenTSDB**, and in the **Agent** selection, select the agent you have already created, or if you have not created a agent, click the **Create New Agent** button to create it.

In the **Target DB**, select the database of the current TDengine Cloud instance as the target database, since the time precision of data in OpenTSDB is millisecond, it is necessary to select a _`millisecond precision db`_ .

### 2. Configure Connection information

Fill in the _`Connection information of the source OpenTSDB`_ in the **Connection Configuration** area, as shown below:

![OpenTSDB-01-FillInTheConnectionInformation.png](./pic/OpenTSDB-03-FillInTheConnectionInformation.png 'Fill in the connection information of the source OpenTSDB')

There is a button **Connectivity Check** to check whether the connectivity between the Cloud instance and the OpenTSDB service is available.

### 3. Configure Task

**Metrics** is the list of data in the OpenTSDB, select one or more specified metrics to migrate, if empty, migrate all. You need to first click on the button **Get Metrics** on the right to obtain the metrics, and then select from the dropdown list, as shown below:
![OpenTSDB-02-GetAndSelectMetrics.png](./pic/OpenTSDB-06-GetAndSelectMetrics.png 'Get and select metrics')

**Data Begin Time** is the starting time of the data, the task only reads data from the specified time and after, The timezone used is consistent with explorer.

**Data End Time** is the stopping time of the data, the task only reads the data at the specified time and before, If a future time is specified, the task will continue until the deadline is reached. If not specified, the task will continue until it is manually terminated. The timezone used is consistent with explorer.

**Time range per read in minutes** is a maximum time range every time when retrieving data from OpenTSDB, it's an important parameter that needs to be determined by the user in combination with server performance and data storage density. If the range is too small, the execution speed of synchronization tasks will be slow; If the range is too large, it may cause the OpenTSDB system to malfunction due to excessive memory usage.

**Delay in seconds** is an integer ranging from 1 to 30, to migrate the out of order data, connector always waits for time specified here before reading them.

### 4. Configure Advanced Options

**Advanced Options** is folded by default, and clicking on the right side can expand it, as shown below:
![OpenTSDB-03-AdvancedOptionsExpandButton.png](./pic/OpenTSDB-07-AdvancedOptionsExpandButton.png 'Advanced options expand button')
![OpenTSDB-04-AdvancedOptionsExpand.png](./pic/OpenTSDB-08-AdvancedOptionsExpand.png 'Advanced options expand')

### 5. Finish

After filling in the above information, click the **Submit** button to start the data synchronization from Kafka to TDengine.
