---
title: OpenTSDB
slug: /advanced-features/data-connectors/opentsdb
---

This section explains how to create a data migration task through the Explorer interface to migrate data from OpenTSDB to the current TDengine cluster.

## Function Overview

OpenTSDB is a real-time monitoring information collection and display platform built on top of the HBase system. TDengine can efficiently read data from OpenTSDB via the OpenTSDB connector and write it into TDengine to achieve historical data migration or real-time data synchronization.

During task execution, progress information is saved to disk, so if the task is paused and restarted, or it recovers automatically from an error, the task will not start from the beginning. More options can be found by reading the descriptions of each form field on the task creation page.

## Creating a Task

### 1. Add a Data Source

Click the **+Add Data Source** button in the top left of the data writing page to enter the Add Data Source page, as shown below:

![Common-zh00-EnterDataSourcePage.png](../../assets/opentsdb-01.png)

### 2. Configure Basic Information

In the **Name** field, enter a task name, such as *`test_opentsdb_01`*.

Select *`OpenTSDB`* from the **Type** dropdown box, as shown below (the fields on the page will change after selection).

The **Agent** field is optional. If needed, you can select a specified agent from the dropdown box, or click the **+Create New Agent** button on the right to create a new agent.

The **Target Database** is required. Since OpenTSDB stores data with a time precision of milliseconds, you need to select a *`millisecond-precision database`*. You can also click the **+Create Database** button on the right to create a new database.

![OpenTSDB-02zh-SelectTheTypeAsOpenTSDB.png](../../assets/opentsdb-02.png)

### 3. Configure Connection Information

In the **Connection Configuration** area, fill in the *`connection information of the source OpenTSDB database`*, as shown below:

![OpenTSDB-03zh-FillInTheConnectionInformation.png](../../assets/opentsdb-03.png)

Below the **Connection Configuration** area, there is a **Connectivity Check** button. Users can click this button to check whether the information entered above can correctly retrieve data from the source OpenTSDB database. The check results are shown below:  
  **Failure**  
  ![OpenTSDB-04zh-ConnectivityCheckFailed.png](../../assets/opentsdb-04.png)  
  **Success**  
  ![OpenTSDB-05zh-ConnectivityCheckSuccessful.png](../../assets/opentsdb-05.png)

### 4. Configure Task Information

**Metrics**: These are the physical quantities stored in the OpenTSDB database. Users can specify multiple metrics to synchronize; if not specified, all data in the database will be synchronized. If users specify metrics, they need to click the **Get Metrics** button on the right to fetch all metric information from the current source OpenTSDB database and then select from the dropdown box, as shown below:
![OpenTSDB-06zh-GetAndSelectMetrics.png](../../assets/opentsdb-06.png)

**Start Time**: This refers to the start time of the data in the source OpenTSDB database. The time zone of the start time uses the time zone selected in the explorer. This field is required.

**End Time**: This refers to the end time of the data in the source OpenTSDB database. If the end time is not specified, synchronization of the latest data will continue; if the end time is specified, synchronization will only occur up to that point. The time zone of the end time uses the time zone selected in the explorer. This field is optional.

**Time Range per Read (minutes)**: This defines the maximum time range for a single read from the source OpenTSDB database. This is an important parameter that users need to decide based on server performance and data storage density. If the range is too small, the synchronization task will execute slowly. If the range is too large, it may cause system failures in the OpenTSDB database due to high memory usage.

**Delay (seconds)**: This is an integer between 1 and 30. To eliminate the impact of out-of-order data, TDengine always waits for the time specified here before reading the data.

### 5. Configure Advanced Options

The **Advanced Options** section is collapsed by default. Click the `>` on the right to expand it, as shown below:
![OpenTSDB-07zh-AdvancedOptionsExpandButton.png](../../assets/opentsdb-07.png)
![OpenTSDB-08zh-AdvancedOptionsExpand.png](../../assets/opentsdb-08.png)

### 6. Completion

Click the **Submit** button to complete the creation of the OpenTSDB to TDengine data synchronization task. Go back to the **Data Sources List** page to view the execution status of the task.
