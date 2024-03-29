---
title: PI System Data Source
sidebar_label: PI System
description: This document describes how to establish a connection with your PI System deployment and extract data from PI System into a TDengine Cloud instance.
---

You can extract data from PI System into TDengine Cloud with the **Data Sources** feature. This integration uses the AF SDK to stream buffered data and query historical data from the PI Data Archive, set up PI and AF data pipes for streaming data, and connect to PI AF to query the AF structure. It also creates the corresponding tables and writes this data to TDengine Cloud over a secure RESTful API.

For more information about this solution, see [TDengine for PI System](https://tdengine.com/pi-system/).

:::note IMPORTANT

There is an additional charge for extracting PI System data. The charge depends on your TDengine pricing plan. For more information, [contact our team](https://tdengine.com/contact/) or your account representative.

:::

## Prerequisites

- Create an empty database to store your PI System data. For more information, see [Database](../../../programming/model/#create-database).
- Ensure that the connection agent is running on a machine located on the same network as your PI Data Archive and PI AF Server (optional). For more information, see [Install Connection Agent](../install-agent/).
- Obtain the name of your PI Data Archive server.
- (Optional) If you want to use PI AF, obtain the name of your AF database.

## Procedure

1. In TDengine Cloud, open **Data In** page.
2. On the **Data Sources** tab, click **Add Data Source**.
3. In the **Name** field, enter a name for your data source.
4. From the **Type** menu, select **PI**.
5. From the **Agent** menu, select the connection agent for your PI System.
   If you have not created a connection agent, see [Install Connection Agent](./00-install-agent).
6. In the **Target DB** field, select one database from the current instance that you created to store PI System data.
   If you have not created a database, please go to **Explorer** page to create it.
7. PI connector supports two connection modes:
   - **PI Data Archive Only**: Without using AF mode. In this mode, enter the **PI Server Name** (server address, usually using the hostname).
   - **PI Data Archive and Asset Framework (AF) Server**: Using AF SDK. In this mode, in addition to configuring the server name, you also need to configure the PI System (AF Server) name (hostname) and AF database name.
8. You can click the **Connectivity Check** button to check whether the communication between the Cloud instance and the PI server is available.
9. **Data Sets** section, select using CSV file template or **All Points**.
10. **Back Fill** section, lost connection or first start automatically backfill the maximum number of days. The default is 1 day.
11. **Advanced Options** section, you can configure the following information:
    - **Log Level**: Log level, optional values are DEBUG, INFO, WARN, ERROR.
    - **Batch Size**: The maximum number of messages or rows sent in a single batch.
    - **Batch Timeout**: The maximum delay for a single read (in seconds). When the timeout ends, as long as there is data, even if it does not meet the Batch Size, it is immediately sent.
12. Review the pricing notice and click **Confirm**.
