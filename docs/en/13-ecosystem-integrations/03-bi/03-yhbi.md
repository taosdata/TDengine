---
sidebar_label: YHBI
title: Integrate with YHBI
toc_max_heading_level: 4
---

YHBI is a business intelligence and data analytics platform. Its JDBC support enables you to add TDengine as a data source, read time-series data directly, and build visual reports without custom data-conversion code.

## Prerequisites

Prepare the following environment:

- Deploy and start TDengine v3.3.2.0 or later (Enterprise or Community Edition).
- Start taosAdapter. For details, see the [taosAdapter Reference](../../12-operations-and-tooling/03-components/03-taosadapter.md).
- Install and start YHBI.
- Download a recent TDengine JDBC connector from Maven Central. This example uses `taos-jdbcdriver-3.4.0-dist.jar`.

## Configure the Data Source

1. In YHBI, choose **Add Data Source**, and select **GENERIC** under SQL data sources.
1. Choose **Select Custom Driver**. In **Driver Management**, choose **+** next to the driver list and enter a name such as `MyTDengine`. Upload `taos-jdbcdriver-3.4.0-dist.jar`, select `com.taosdata.jdbc.ws.WebSocketDriver`, and confirm the driver.
1. Copy the following value into the **URL** field:

   ```text
   jdbc:TAOS-WS://127.0.0.1:6041?user=root&password=taosdata&conmode=1&varcharAsString=true
   ```

1. Under **Authentication Method**, select **No Authentication**.
1. In the advanced data-source settings, set **Quote Character** to a backtick (`` ` ``).
1. Test the connection. After the test succeeds, save the data source with a name such as `MyTDengine`.
1. Choose **Add Data Source**, expand the data source you created, and browse the TDengine supertables.
1. Load all data from a supertable or import a subset with custom SQL.
1. To avoid caching TDengine time-series data in YHBI, enable **In-Database Computing**. YHBI then sends SQL requests directly to TDengine for processing.

## Analyze Data

After importing data, YHBI classifies numeric columns as measures and text columns as dimensions. In a TDengine supertable, ordinary columns typically represent measures and tag columns represent dimensions, so you might need to adjust column properties when creating a dataset.

You can create parameters in YHBI and use them in SQL that is executed manually or on a schedule. The following query reads real-time data from TDengine and supports dynamic table, time-range, and interval parameters:

```sql
SELECT _wstart AS ws, count(*) AS cnt
FROM supertable
WHERE tbname = ?{metric} AND ts = ?{from} AND ts < ?{to}
INTERVAL(?{interval})
```

The parameters and result columns are as follows:

1. `_wstart` is the start time of each window.
1. `count(*)` is the aggregate value for the window.
1. `?{interval}` specifies the downsampling interval. For example, `1m` creates one-minute windows.
1. `?{metric}` specifies a table name. You can bind it to a drop-down parameter component whose ID is `metric`.
1. `?{from}` and `?{to}` specify the dataset time range and can be bound to text parameter components.

In the **Edit Parameter** dialog, configure each parameter's data type, range, and default value. You can then change the values dynamically in the visual report.

To create a report:

1. Choose **Create Report** in YHBI and create a canvas.
1. Drag a visualization component, such as a table, onto the canvas.
1. In the **Dataset** sidebar, select a dataset and bind its dimensions and measures to the component.
1. Save the report and view the result.
