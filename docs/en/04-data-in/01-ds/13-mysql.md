---
sidebar_label: "MySQL"
title: "MySQL"
description: This document describes how to extract data from a MySQL server into a TDengine Cloud instance.
---

MySQL is one of the most popular RDBMS. Many application systems have been using MySQL to store time series data generated in IoT or Industry fields. With the number of devices in such environments grow, MySQL can't handle the data explosion. 

MySQL data source is the process of writing data from the MySQL server to the currently selected TDengine Cloud instance optional via a connection agent.

## Prerequisites

- Create an empty database to store your MySQL data. For more information, see [Database](../../../programming/model/#create-database).
- (Optional) Ensure that the connection agent is running on a machine located on the same network as your MySQL Server. For more information, see [Install Connection Agent](../install-agent/).

## Procedure

1. In TDengine Cloud, open **Data In** page. On the **Data Sources** tab, click **Add Data Source** button to open the new data source page.
   - In the **Name** field, fill in the name of the data source.
   - In the **Type** field, select the type of **MySQL**.
   - In the **Agent** selection which is not not a mandatory field, select the agent you have already created, or if you have not created a agent, click the **Create New Agent** button to create it.

2. In the **Target DB**, select the database of the current TDengine Cloud instance as the target database.

3. Configure Connection information

    - In the **Connection Configuration** area, fill in the **host** and **port**.
    - In the **Authentication** area, fill in the **Username** and **Password**.
    - In the **Configure Connection options** area, fill in the **Character Set** and **SSL Mode**.

4. Click the **Check Connectivity** button to check whether the information filled in above can normally obtain the data of the source MySQL database.

5. Configure Data Collection
   - **SQL Template**: The SQL statement template used for querying data in the source database. The SQL statement must contain a time range condition, and the start time and end time must appear in pairs, the time range is defined by specific column in the source database and place holders defined below, the place holder will be replaced by the real values specified in following input fields.
   > SQL uses different placeholders to represent different time format requirements, specifically the following placeholder formats:
   > 1. `${start}`, `${end}`: Represents the RFC3339 format timestamp, such as: 2024-03-14T08:00:00+0800
   > 2. `${start_no_tz}`, `${end_no_tz}`: Represents the RFC3339 string without a time zone: 2024-03-14T08:00:00
   > 3. `${start_date}`, `${end_date}`: Represents only the date, such as: 2024-03-14
   - **Start Time**: Start time for migrating data. This field is required.
   - **End Time**: End time for migrating data, it can be left as blank. If it is set, the migration task will stop automatically after all the data between the start time and end time is migrated. If it is left as blank, the task will replicate all existing data and new incoming data automatically until the user stops it manually.
   - **Query Interval**: The time interval for segmented queries. The default is 1 day. To avoid querying too much data, a data synchronization subtask will use the query interval to query data in time segments.
   - **Delay**: In the real-time data synchronization scenario, to avoid the loss of delayed written data, each synchronization task will read data before the delay time.

6. In the **Data Mapping** area, fill in the configuration parameters related to data mapping.

   - Click the **Retrieve from Server** button to get sample data from the MySQL server.
   - In the **Extract or Split From A column** field, fill in the fields to be extracted or split from the message body, for example: split the `vValue` field into `vValue_0` and `vValue_1` fields, with `split` Extractor, separator filled as `,`, number filled as `2`.
   - In the **Filter** field, fill in the filter condition, for example: fill in `Value > 0`, then only the data with Value greater than 0 will be written to TDengine.
   - In the **Mapping** area, select the super table to be mapped to TDengine, and the columns to be mapped to the super table.
   - Click **Preview** to view the mapping result.

7. In the **Advanced Options** area, fill in the configuration parameters related to advanced options.
   - **Read Concurrency**: The number of concurrent read requests. The default value is automatically set by collector. If the data source is slow to respond, you can increase this value appropriately.
   - **Batch Size**: The number of data points to be written in a single request. The default value is 10000. If the data source is slow to respond, you can reduce this value appropriately.

8. After filling in the above information, click the **Submit** button to start the data synchronization from MySQL to TDengine.
