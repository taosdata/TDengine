---
sidebar_label: OpenTSDB
title: OpenTSDB Data Source
description: This document describes how to extract data from OpenTSDB into a TDengine Cloud instance.
---

OpenTSDB data source is the process of writing data from the OpenTSDB server to the currently selected TDengine Cloud instance via a connection agent.

## Prerequisites

- Create an empty database to store your OpenTSDB data. For more information, see [Database](../../../programming/model/#create-database).
- (Optional) Ensure that the connection agent is running on a machine located on the same network as your OpenTSDB Server. For more information, see [Install Connection Agent](../install-agent/).

## Procedure

1. In TDengine Cloud, open **Data In** page. On the **Data Sources** tab, click **Add Data Source** button to open the new data source page. In the **Name** input, fill in the name of the data source and select the type of **OpenTSDB**, and in the **Agent** selection, select the agent you have already created, or if you have not created a agent, click the **Create New Agent** button to create it.
2. In the **Target DB**, select the database of the current TDengine Cloud instance as the target database, since the time precision of data in OpenTSDB is millisecond, it is necessary to select a _`millisecond precision db`_ .

3. Fill in the _`Connection information of the source OpenTSDB`_ in the **Connection Configuration** area.
   - in the _`Connection Protocol`_ dropdown list, supporting HTTP and HTTPS, the default is HTTP, and it is a required field.
   - in the _`IP address`_ area, enter the OpenTSDB address, a required field.
   - in the _`Port`_ area, enter the OpenTSDB server port, by default, OpenTSDB listens on port 4242, a required field.
4. There is a button **Connectivity Check** to check whether the connectivity between the Cloud instance and the OpenTSDB service is available.
5. Configure Task:

   - **Metrics** is the list of data in the OpenTSDB, select one or more specified metrics to migrate, if empty, migrate all. You need to first click on the button **Get Metrics** on the right to obtain the metrics, and then select from the dropdown list.
   - **Data Begin Time** is the starting time of the data, the task only reads data from the specified time and after, The timezone used is consistent with explorer.
   - **Data End Time** is the stopping time of the data, the task only reads the data at the specified time and before, If a future time is specified, the task will continue until the deadline is reached. If not specified, the task will continue until it is manually terminated. The timezone used is consistent with explorer.
   - **Time range per read in minutes** is a maximum time range every time when retrieving data from OpenTSDB, it's an important parameter that needs to be determined by the user in combination with server performance and data storage density. If the range is too small, the execution speed of synchronization tasks will be slow; If the range is too large, it may cause the OpenTSDB system to malfunction due to excessive memory usage.
   - **Delay in seconds** is an integer ranging from 1 to 30, to migrate the out of order data, connector always waits for time specified here before reading them.

6. **Advanced Options** is folded by default, and clicking on the right side can expand it.
   - **Log Level** is used to configure the log level of the connector, supporting error, warn, info, debug, trace 5 levels, the default value is info.
   - **Read Concurrency** is used to configure the maximum concurrency when reading data from the source OpenTSDB database, the default value is 50.
   - **Write Concurrency** is used to configure the maximum concurrency when writing data to the target TDengine database, the default value is 50.
   - **Batch Size** is used to configure the maximum batch size when writing data to the target TDengine database, the default value is 5000.
7. After filling in the above information, click the **Submit** button to start the data synchronization from OpenTSDB to TDengine.
