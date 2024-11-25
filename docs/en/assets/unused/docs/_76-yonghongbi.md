---
sidebar_label: Yonghong BI
title: Yonghong
description: Using TDengine to connect to Yonghong BI
---

# Tool - Yonghong BI

![Yonghong BI use step](./yonghongbi-step-zh.png)

The [Yonghong All-in-One Big Data BI Platform](https://www.yonghongtech.com/) provides flexible and user-friendly big data analysis solutions for enterprises of all sizes, allowing every user to easily unlock the value of big data and gain deep insights. TDengine can be added as a data source to Yonghong BI through the JDBC connector. Once the data source configuration is completed, Yonghong BI can read data from TDengine and provide functionalities such as data display, analysis, and forecasting.

### Prerequisites

1. Yonghong Desktop Basic has been installed and is running (if not installed, download from the [Yonghong Technology official download page](https://www.yonghongtech.com/cp/desktop/)).
2. TDengine has been installed and is running, and make sure the taosadapter service is started on the TDengine server.

### Install JDBC Connector

Download the latest TDengine JDBC connector from [maven.org](https://central.sonatype.com/artifact/com.taosdata.jdbc/taos-jdbcdriver/versions) (currently version 3.2.7) and install it on the machine running the BI tool.

### Configure TDengine JDBC Data Source

1. In the opened Yonghong Desktop BI tool, click "Add Data Source" and select "GENERIC" under SQL data sources.
2. Click "Select Custom Driver", in the "Driver Management" dialog, click the "+" next to "Driver List", enter the name "MyTDengine". Then click the "Upload File" button to upload the TDengine JDBC connector file "taos-jdbcdriver-3.2.7-dist.jar" you just downloaded, and select the "com.taosdata.jdbc.rs.RestfulDriver" driver. Finally, click "OK" to complete the driver addition.
3. Copy the following content into the "URL" field:
   ```
   jdbc:TAOS-RS://localhost:6041?user=root&password=taosdata
   ```
4. Next, select "No Authentication" in the "Authentication Method".
5. In the advanced settings of the data source, change the value of "Quote Symbol" to the backtick "`".
6. Click "Test Connection", and a "Test Successful" dialog will pop up. Click the "Save" button and enter "MyTDengine" to save the TDengine data source.

### Create TDengine Dataset

1. In the BI tool, click "Add Data Source", expand the data source you just created, and browse the super tables in TDengine.
2. You can load all data from the super table into the BI tool or import partial data using custom SQL statements.
3. When the "Database Calculation" checkbox is checked, the BI tool will no longer cache the time series data from TDengine and will send SQL requests to TDengine for direct processing during query handling.

After importing the data, the BI tool will automatically set numeric types as "Measure" columns and text types as "Dimension" columns. In TDengine's super tables, ordinary columns are used as data measures, and tag columns are used as data dimensions, so you may need to change some column properties when creating the dataset. TDengine not only supports standard SQL but also provides a series of distinctive query syntax tailored for time series business scenarios, such as data slicing queries and window slicing queries. For specific operations, refer to the [TDengine distinctive query feature introduction](https://docs.taosdata.com/taos-sql/distinguished/). By using these distinctive queries, the BI tool can significantly improve data access speed and reduce network transmission bandwidth when sending SQL queries to the TDengine database.

In the BI tool, you can create "Parameters" and use them in SQL statements, dynamically executing these SQL statements manually or on a schedule, allowing for refreshing visual reports. For example, the following SQL statement can read data in real time from TDengine:

```sql
select _wstart ws, count(*) cnt from supertable where tbname=?{metric} and ts >= ?{from} and ts < ?{to} interval(?{interval})
```

Where:
- `_wstart`: indicates the start time of the time window.
- `count(*)`: indicates the aggregated value within the time window.
- `?{interval}`: indicates the introduction of a parameter named `interval` in the SQL statement. When the BI tool queries data, the value of the `interval` parameter is assigned. If the value is 1m, it means to downsample data by a 1-minute time window.
- `?{metric}`: this parameter is used to specify the name of the data table to be queried. When the ID of a "Dropdown Parameter Component" in the BI tool is also set to `metric`, the selected options of that "Dropdown Parameter Component" will be bound to this parameter for dynamic selection.
- `?{from}` and `?{to}`: these two parameters indicate the time range of the dataset to be queried and can be bound to a "Text Parameter Component".

You can modify the data type, data range, and default values of the parameters in the "Edit Parameters" dialog of the BI tool and dynamically set the values of these parameters in the "Visual Reports".

### Create Visual Reports

1. Click "Create Report" in the Yonghong BI tool to create a canvas.
2. Drag visualization components onto the canvas, such as a "Table Component".
3. In the "Dataset" sidebar, select the dataset to be bound and bind the "Dimensions" and "Measures" from the data columns as needed to the "Table Component".
4. Click "Save" to view the report.
5. For more information about the Yonghong BI tool, please refer to its [Help Documentation](https://www.yonghongtech.com/help/Z-Suite/10.0/ch/).
