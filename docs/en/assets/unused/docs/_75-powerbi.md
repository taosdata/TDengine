---
sidebar_label: Power BI
title: Power BI
description: How to use Power BI and TDengine for time series data analysis
---

# Tool - Power BI

![Power BI use step](./powerbi-step-zh.png)

[Power BI](https://powerbi.microsoft.com/) is a business analytics tool provided by Microsoft. By configuring the ODBC connector, Power BI can quickly access TDengine. You can import tag data, raw time series data, or time-aggregated data from TDengine into Power BI to create reports or dashboards without any coding.

### Prerequisites
1. The TDengine server software is installed and running.
2. Power BI Desktop software is installed and operational (if not installed, download the latest Windows X64 version from the [official site](https://www.microsoft.com/zh-cn/download/details.aspx?id=58494)).

### Install ODBC Connector
1. Windows platform only. You need to have the VC runtime library installed on Windows. You can download it [here](https://learn.microsoft.com/zh-cn/cpp/windows/latest-supported-vc-redist?view=msvc-170). If you have already installed VS development tools, you can ignore this step.
2. Download and install the [TDengine Windows client package](https://docs.taosdata.com/get-started/package/).

### Configure ODBC Data Source
1. Search for and open the "ODBC Data Sources (64-bit)" management tool from the Windows operating system's Start menu (be careful not to choose the 32-bit version).
2. Select the "User DSN" tab and click the "Add(D)" button to enter the "Create Data Source" interface.
3. Select the data source you want to add, then select "TDengine", click Finish, and you will enter the TDengine ODBC data source configuration page. Fill in the following necessary information:

   &emsp;&emsp;**DSN**: &emsp;&emsp;&emsp;&emsp;Data source name, required, e.g., "MyTDengine"

   &emsp;&emsp;**Connection Type**: &emsp;&emsp;Select "Websocket"

   &emsp;&emsp;**Service Address**: &emsp;&emsp;taos://localhost:6041

   &emsp;&emsp;**Database**: &emsp;&emsp;&emsp;The database you want to connect to, optional, e.g., "test"

   &emsp;&emsp;**Username**: &emsp;&emsp;&emsp;Enter the username. If not filled, the default is root.

   &emsp;&emsp;**Password**: &emsp;&emsp;&emsp;&emsp;Enter the user password. If not filled, the default is taosdata.

4. Click the "Test Connection" button to test the connection. If successful, a message will appear: "Successfully connected to taos://root:taosdata@localhost:6041".

### Import TDengine Data into Power BI
1. Open Power BI, log in, and add a data source using the following steps: "Home" -> "Get Data" -> "Other" -> "ODBC" -> "Connect".
2. Select the data source name you just created, e.g., "MyTDengine", and click the "OK" button. In the pop-up "ODBC Driver" dialog, select "Default or Custom" in the left menu, then click the "Connect" button to connect to the configured data source. Once in the "Navigator", you can browse the data tables in the corresponding database and load them.
3. If you need to enter an SQL statement, you can click "Advanced Options", where you can input and load your SQL.

To better analyze TDengine data using Power BI, you need to understand the concepts of dimensions, measures, time series, and correlations, and then import data using custom SQL statements.
1. **Dimension**: Typically categorical (text) data that describes categories such as devices, measurement points, models, etc. In TDengine's super table, dimension information is stored in tag columns, which can be quickly retrieved using SQL like `select distinct tbname, tag1, tag2 from supertable`.
2. **Measure**: Quantitative (numeric) fields that can be used for calculations, common calculations include sum, average, and minimum. If the collection frequency of a measurement point is one second, there will be 31,536,000 records in a year. Importing all this data into Power BI can severely affect performance. In TDengine, you can use data slicing queries, window slicing queries, etc., to import downsampled data into Power BI using relevant pseudo columns. For specific syntax, refer to the [TDengine distinctive query feature introduction](https://docs.taosdata.com/taos-sql/distinguished/).
   - **Window Slicing Query**: For instance, if a temperature sensor collects data once per second, but you need to query the average temperature every 10 minutes, you can use a window clause to achieve the desired downsampled query result. The corresponding SQL syntax is `select tbname, _wstart date, avg(temperature) temp from table interval(10m)`, where _wstart is a pseudo column indicating the start time of the time window, and 10m represents the duration of the time window, `avg(temperature)` indicates the aggregated value within the time window.
   - **Data Slicing Query**: If you need to retrieve aggregated values for many temperature sensors simultaneously, you can slice the data and perform a series of calculations within the sliced data space. The corresponding SQL syntax refers to `partition by part_list`. The most common usage of the data slicing clause is in super table queries, where subtable data is sliced by labels, allowing each subtable's data to be isolated, forming independent time series for various statistical analyses.
3. **Time Series**: When plotting curves or aggregating data over time, a date table is often needed. A date table can be imported from an Excel spreadsheet or generated in TDengine by executing an SQL query, such as `select _wstart date, count(*) cnt from test.meters where ts between A and B interval(1d) fill(0)`, where the fill clause specifies the fill mode in case of missing data, and the pseudo column _wstart serves as the date column to be retrieved.
4. **Correlation**: Indicates how data points are related; measures and dimensions can be associated through the tbname column, while date tables and measures can be associated through the date column to form visual reports.

### Smart Meter Example
TDengine has a unique data model that uses super tables as templates to create a table for each device, with each table capable of creating up to 4,096 data columns and 128 tag columns. In [the example of meters](https://docs.taosdata.com/concept/), suppose one meter generates one record per second, resulting in 86,400 records per day and 31,536,000 records per year. For 1,000 meters, this would consume approximately 600 GB of raw disk space. Therefore, the primary usage of Power BI is to map tag columns as dimension columns and import the aggregated results of data columns as measure columns, ultimately providing decision-makers with the metrics they need.
1. Import dimension data: In Power BI, import the tag columns of the table, naming it tags, with the following SQL:  
`select distinct tbname, groupid, location from test.meters;`
2. Import measure data: In Power BI, import the average current, average voltage, and average phase for each meter based on a one-hour time window, naming it data, with the following SQL:  
`select tbname, _wstart ws, avg(current), avg(voltage), avg(phase) from test.meters PARTITION by tbname interval(1h);`
3. Establish the relationship between dimensions and measures: In Power BI, open the model view and establish a relationship between the tags and data tables by setting tbname as the related data column. After this, you can use this data in bar charts, pie charts, and other controls. For more information on building visuals in Power BI, please refer to the [Power BI documentation](https://learn.microsoft.com/zh-cn/power-bi/).
