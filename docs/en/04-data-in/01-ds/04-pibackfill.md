---
sidebar_label: PI Backfill
title: PI Backfill Data Source
description: This document describes how to establish a connection with your PI System deployment and extract historical data from PI System into a TDengine Cloud instance.
---

You can extract history data from PI System into TDengine Cloud with the **Data Sources** feature. This integration uses the AF SDK to stream buffered data and query historical data from the PI Data Archive, set up PI and AF data pipes for streaming data, and connect to PI AF to query the AF structure. It also creates the corresponding tables and writes this data to TDengine Cloud over a secure RESTful API.

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
   If you have not created a connection agent, see [Install Connection Agent](../install-agent/).
6. In the **Target DB** field, select one database from the current instance that you created to store PI System data.
   If you have not created a database, please go to **Explorer** page to create it.
7. In the **PI Data Archive Server** field, enter the name of your PI Data Archive server.
8. (Optional) In the **AFDatabaseName** field, enter the name of your AF database.
9. In the **AF Server Name** field, enter the name of your AF server.
10. You can click the **Connectivity Check** button to check whether the communication between the Cloud instance and the PI server is available.
11. In the **Back Fill** field, you can set whether to start with the data that the current TDengine Cloud instance already has, and you can also set to end with the data that the current TDengine Cloud instance already has. In addition, you need to set the start time and end time of the history backfill, if the end time is not filled in, the default is the current time.
12. In the **Data Queue** section, configure the parameters as desired. You can also retain the default values.

    - **Max Wait Length** indicates the maximum rows of data to send in each HTTPS insert request.

    - **Update Interval** indicates how often data will be pulled from the PI system.

    - **Max Backfill Range (in days)** indicates the maximum number of days that will be automatically backfilled when reconnecting.

13. In the **Data Sets** section, select the ingestion method:
    - Select **Point List** to ingest PI Points.
    - Select **Template for PI Point** to ingest PI Points based on AF element templates.
    - Select **Template for AF Elements** to ingest elements from AF.
14. Enter a regular expression in the search field to find the desired PI Points or AF templates.
15. Select each desired PI Point or AF template and click **Add** under the selected item.
16. After adding all desired items, click **Add** under the **Data Sets** section.
17. Review the pricing notice and click **Confirm**.

The selected PI Points or AF templates start streaming data to TDengine, with tables in TDengine automatically created to match point names or AF template structure.

## What to Do Next

1. In TDengine Cloud, open **Explorer** and click the database that you created to store PI System data.
2. Verify that the specified data is being ingested into TDengine Cloud.
   - A supertable is created that acts as a schema for your PI System data.
   - Subtables are created that contain your PI System data and tags.
   - When **Template For AF Element** mode is used, the AF tree will be copied to a single metadata tag called `location`, and any static attributes in the AF elements will be copied to corresponding tags in the supertable.
