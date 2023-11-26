---
sidebar_label: OPC DA
title: OPC DA Data Source
description: This document describes how to establish a connection with your OPC DA server and extract data from OPC into a TDengine Cloud instance.
---

OPC DA data source is the process of writing data from the OPC UA server to the currently selected TDengine Cloud instance via a connection agent.

:::note IMPORTANT

There is an additional charge for extracting OPC UA . The charge depends on your TDengine pricing plan. For more information, [contact our team](https://tdengine.com/contact/) or your account representative.

:::

## Prerequisites

- Create an empty database to store your OPC DA data. For more information, see [Database](../../../programming/model/#create-database).
- Ensure that the connection agent is running on a machine located on the same network as your OPC  Server. For more information, see [Install Connection Agent](../install-agent/).

## Procedure

1. In TDengine Cloud, open **Data In** page. On the **Data Sources** tab, click **Add Data Source** button to open the new data source page. In the **Name** input, fill in the name of the data source and select the type of **OPC-DA**, and in the **Agent** selection, select the agent you have already created, or if you have not created a agent, click the **Create New Agent** button to create it.
2. In the **Target DB**, select the database of the current TDengine Cloud instance as the target database.
3. In the **Service endpoint** field, you need to configure the address of the OPC DA server, 127.0.0.1<,localhost>/Matrikon.OPC.Simulation.
4. In the **DataSet CSV config file** field, configure whether to enable the CSV config. If enabled, copy the specifics of the CSV profile to configure the dataset information, and the **Data Sets** and **Table Config** will no longer take effect.
5. in the **Connection** part, configure the Connection Timeout Interval and Acquisition Timeout Interval (in seconds), the default value is 10 seconds.
6. In the **Original Data Save** part, configure whether to enable save. If it is saved, please set the save path and the specific number of days to save.
7. In the **Others** part, configure the parallelism, single collection report batch (default value is 100), report timeout (unit: second, default value is 10), and whether to enable debug level logging.
8. In the **Table Config** part, configure the super table and sub-table structure information of the data stored in the target TDengine.
9. In the **Data Sets** field, configure the point information. Click the **Select** button to select the DA tags.
10. After completing the above information, click the **Add** button to directly initiate data synchronization from the OPC DA server to the TDengine Cloud instance.
