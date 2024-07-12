---
sidebar_label: AVEVA Historian
title: AVEVA Historian Data Source
description: This document describes how to extract data from AVEVA Historian into a TDengine Cloud instance.
---

AVEVA Historian data source is the process of writing data from the AVEVA Historian server to the currently selected TDengine Cloud instance via a connection agent.

## Prerequisites

- Create an empty database to store your AVEVA Historian data. For more information, see [Database](../../../programming/model/#create-database).
- (Optional) Ensure that the connection agent is running on a machine located on the same network as your AVEVA Historian Server. For more information, see [Install Connection Agent](../install-agent/).

## Procedure

1. In TDengine Cloud, open **Data In** page. On the **Data Sources** tab, click **Add Data Source** button to open the new data source page. In the **Name** input, fill in the name of the data source and select the type of **AVEVA Historian**, and in the **Agent** selection, select the agent you have already created, or if you have not created a agent, click the **Create New Agent** button to create it.
2. In the **Target DB**, select the database of the current TDengine Cloud instance as the target database.
3. Configure Connection information
   - In the **Connection Configuration** area, fill in the **host** and **port**.
   - In the **Authentication** area, fill in the **Username** and **Password**.
4. Click the **Connectivity Check** button to check whether the data source is available.
5. In the **Collect** area, fill in the configuration parameters related to the collection task.
   - In the **Collection Mode** drop-down list, select **migrate**.
   - In the **Tags** field, fill in the list of tags to be migrated, separated by commas.
   - In the **Tag List Size** field, fill in the size of the tag group.
   - In the **Begin Time** field, fill in the start time of the data migration task.
   - In the **End Time** field, fill in the end time of the data migration task.
   - In the **Time Window** field, fill in a time interval, and the data migration task will divide the time window according to this time interval.
6. Synchronize **Runtime.dbo.History** table data to TDengine requires the following parameters to be configured:

   - In the **Collection Mode** drop-down list, select **synchronize**.
   - In the **Table** field, select **Runtime.dbo.History**.
   - In the **Tags** field, fill in the list of tags to be synchronized, separated by commas.
   - In the **Tag List Size** field, fill in the size of the tag group.
   - In the **Begin Time** field, fill in the start time of the data synchronization task.
   - In the **Time Window** field, fill in a time interval, and the historical data part will divide the time window according to this time interval.
   - In the **Retrieve Interval** field, fill in a time interval, and the real-time data part will poll data according to this time interval.
   - In the **Tolerance** field, fill in a time interval, and the data that is not written to the database until after this time interval may be lost.

7. Synchronize **Runtime.dbo.Live** table data to TDengine requires the following parameters to be configured:

   - In the **Collection Mode** drop-down list, select **synchronize**.
   - In the **Table** field, select **Runtime.dbo.Live**.
   - In the **Tags** field, fill in the list of tags to be synchronized, separated by commas.
   - In the **Retrieve Interval** field, fill in a time interval, and the real-time data part will poll data according to this time interval.

8. Configure Data Mapping
   - In the **Data Mapping** area, fill in the configuration parameters related to data mapping.
   - Click the **Retrieve from Server** button to get sample data from the AVEVA Historian server.
   - In the **Extract or Split From A column** field, fill in the fields to be extracted or split from the message body, for example: split the `vValue` field into `vValue_0` and `vValue_1` fields, with `split` Extractor, separator filled as `,`, number filled as `2`.
   - In the **Filter** field, fill in the filter condition, for example: fill in `Value > 0`, then only the data with Value greater than 0 will be written to TDengine.
   - In the **Mapping** area, select the super table to be mapped to TDengine, and the columns to be mapped to the super table.
   - Click **Preview** to view the mapping result.
9. In the **Advanced Options** area, fill in the configuration parameters related to advanced options.
   - Set the maximum number of concurrent reads in **Max Read Concurrency**. The default value is 0, which means auto, automatically configure the concurrency.
   - Set the batch size in **Batch Size**, which is the maximum number of messages sent in a single send.
   - Select whether to save the original data in **Keep Raw Data**. The default value is No. When saving the original data, the following two parameters are configured.
   - Set the maximum retention days of the original data in **Max Keep Days**.
   - Set the original data storage directory in **Raw Data Directory**.
10. After filling in the above information, click the **Submit** button to start the data synchronization from AVEVA Historian to TDengine.
