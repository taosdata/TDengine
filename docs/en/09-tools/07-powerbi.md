---
sidebar_label: Power BI
title: Use Power BI
description: This document describes how to integrate TDengine Cloud with Microsoft Power BI for data visualization.
---

Power BI is a business analytics tool provided by Microsoft. With TDengine ODBC driver, PowerBI can access time series data stored in the TDengine Cloud instance. You can import tag data, original time series data, or aggregated data into Power BI from a TDengine Cloud instance, to create reports or dashboard without any coding effort. The minimize TDengine version is 3.2.2.0.

## Prerequisite

Power BI Desktop has been installed and running. (If not, please download and install latest Windows X64 version from [PowerBI](https://www.microsoft.com/download/details.aspx?id=58494).)

## Install ODBC connector

1. Only support Windows operation system. And you need to install [VC Runtime Library](https://learn.microsoft.com/en-us/cpp/windows/latest-supported-vc-redist?view=msvc-170) first. If already installed, please ignore this step.
2. Install TDengine Windows client installation package as the following.

:::note IMPORTANT
Please login [TDengine Cloud](https://cloud.taosdata.com) and select "PowerBI" card of the "Tools" page. In the opened opage, please download the selected TDengine Cloud instance's TDengine Windows client in the "Install ODBC connector" part.
:::

## Configure ODBC DataSource

1. Click the "Start" Menu, and Search for "ODBC", and choose "ODBC Data Source (64-bit)" (Note: Don't choose 32-bit).
2. Select "User DSN" tab, and click "Add" to enter the page for "Create Data Source".
3. Choose the data source to be added, here we choose "TDengine" and click "Finish", and enter the configuration page for "TDengine ODBC Data Source", fill in required fields as the following:
    \[DSN\]: Data Source Name, required field, such as "MyTDengine"
    \[Connection Type\]: required field, we choose "WebSocket"
    \[URL\]: To obtain the URL, please login [TDengine Cloud](https://cloud.tdengine.com) and click "Tools", select "PowerBI" and then copy the related value of URL.
    \[Database\]: optional field, the default database to access, such as "test"
4. Click "Test Connection" to test whether the data source can be connectted; if successful, it will prompt "Successfully connected to the URL".

:::note IMPORTANT
Please log in [TDengine Cloud](https://cloud.taosdata.com) and select "PowerBI" card of the "Tools" page. In the opened opage, please copy the value in the "URL" field of the "Configure ODBC DataSource" part.
:::

## Import Data from TDengine to Power BI

1. Open Power BI and logon, add data source following steps "Home Page" -> "Get Data" -> "Others" -> "ODBC" -> "Connect".
2. Choose the created data source name, such as "MyTDengine", then click "OK" button to open the "ODBC Driver" dialog. In the dialog, select "Default or Custom" left menu and then click "Connect" button to connect to the configured data source. After go to the "Nativator", browse tables of the selected database and load data.
3. If you want to input some specific SQL, click "Advanced Options", and input your SQL in the open dialogue box and load the data.

To better use Power BI to analyze the data stored in TDengine, you need to understand the concepts of dimention, metric, time serie, correlation, and use your own SQL to import data:

1. Dimention: it's normally category (text) data to describe such information as device, collection point, model. In the supertable template of TDengine, we use tag columns to store the dimention information. You can use SQL like select distinct tbname, tag1, tag2 from supertable to get dimentions.
2. Metric: quantitive (numeric) fileds that can be calculated, like SUM, AVERAGE, MINIMUM. If the collecting frequency is 1 second, then there are 31,536,000 records in one year, it will be too low efficient to import so big data into Power BI. In TDengine, you can use data partition query, window partition query, in combination with pseudo columns related to window, to import downsampled data into Power BI. For more details, please refer to [TDengine Specialized Queries](https://docs.tdengine.com/cloud/taos-sql/distinguished/).
3. Window partition query: for example, thermal meters collect one data per second, but you need to query the average temperature every 10 minutes, you can use window subclause to get the downsampling data you need. The corresponding SQL is like select tbname, _wstart date，avg(temperature) temp from table interval(10m), in which \_wstart is a pseudo column indicting the start time of a widow, 10m is the duration of the window, avg(temperature) indicates the aggregate value inside a window.
4. Data partition query: If you want to get the aggregate value of a lot of thermal meters, you can first partition the data and then perform a series of calculation in the partitioned data spaces. The SQL you need to use is partition by part_list. The most common of data partition usage is that when querying a supertable, you can partition data by subtable according to tags to form the data of each subtable into a single time serie to facilitate analytical processing of time series data.
5. Time Serie: When curve plotting or aggregating data based on time lines, date is normally required. Data or time can be imported from Excel, or retrieved from TDengine using SQL statement like select _wstart date, count(*) cnt from test.meters where ts between A and B interval(1d) fill(0), in which the fill() subclause indicates the fill mode when there is data missing, pseudo column \_wstart indicates the date to retrieve.
6. Correlation: Indicates how to correlate data. Dimentions and Metrics can be correlated by tbname, dates and metrics can be correlated by date. All these can cooperate to form visual reports.

## Smart Meters Example

TDengine has its own specific data model, which uses supertable as template and creates a specific table for each device. Each table can have maximum 4,096 data columns and 128 tags. In the example of meters, assume each meter generates one record per second, then there will be 86,400 records each day and 31,536,000 records every year, then only 1,000 meters will occupy 500GB disk space. So, the common usage of Power BI should be mapping tags to dimention columns, mapping the aggregation of data columns to metric columns, to provide indicators for decision makers.

1. Import Dimentions: Import the tags of tables in PowerBI, and name as "tags", the SQL is like:

    ```sql
    select distinct tbname, groupid, location from test.meters;
    ```

2. Import Metrics: In Power BI, import the average current, average voltage, average phase with 1 hour window, and name it as "data", the SQL is like:

    ```sql
    select tbname, _wstart ws, avg(current), avg(voltage), avg(phase) from test.meters PARTITION by tbname interval(1h) ;
    ```

3. Correlate Dimentions and Metrics:
In Power BI, open model view, correlate "tags" and "data", and set "tabname" as the correlation column, then you can use the data in histogram, pie chart, etc. For more information about building visual reports in PowerBI, please refer to [Power BI](https://learn.microsoft.com/power-bi/)。
