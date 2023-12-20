---
sidebar_label: Power BI
title: Power BI
description: Use PowerBI and TDengine to analyze time series data
---

## Introduction

With TDengine ODBC driver, PowerBI can access time series data stored in TDengine. You can import tag data, original time series data, or aggregated data into PowerBI from TDengine, to create reports or dashboard without any coding effort.

## Steps
![Power BI use step](./powerbi-step-en.webp)

### Prerequisites

1. TDengine server has been installed and running well.
2. Power BI Desktop has been installed and running. (If not, please download and install latest Windows X64 version from [PowerBI](https://www.microsoft.com/download/details.aspx?id=58494).


## Install Driver

Depending on your TDengine server version, download appropriate version of TDengine client package from TDengine website [Download Link](https://docs.taosdata.com/get-started/package/), or TDengine explorer if you are using a local TDengine cluster. Install the TDengine client package on same Windows machine where PowerBI is running.

### Configure Data Source

Please refer to [ODBC](../../client-libraries/odbc) to configure TDengine ODBC Driver with WebSocket connection.

### Import Data from TDengine to Power BI

1. Open Power BI and logon, add data source following steps "Home Page" -> "Get Data" -> "Others" -> "ODBC" -> "Connect"

2. Choose data source name, connect to configured data source, go to the nativator, browse tables of the selected database and load data

3. If you want to input some specific SQL, click "Advanced Options", and input your SQL in the open dialogue box and load the data.


To better use Power BI to analyze the data stored in TDengine, you need to understand the concepts of dimention, metric, time serie, correlation, and use your own SQL to import data. 

1. Dimention: it's normally category (text) data to describe such information as device, collection point, model. In the supertable template of TDengine, we use tag columns to store the dimention information. You can use SQL like `select distinct tbname, tag1, tag2 from supertable` to get dimentions. 

2. Metric: quantitive (numeric) fileds that can be calculated, like SUM, AVERAGE, MINIMUM. If the collecting frequency is 1 second, then there are 31,536,000 records in one year, it will be too low efficient to import so big data into Power BI. In TDengine, you can use data partition query, window partition query, in combination with pseudo columns related to window, to import downsampled data into Power BI. For more details, please refer to [TDengine Specialized Queries](https://docs.taosdata.com/taos-sql/distinguished/)。

  - Window partition query: for example, thermal meters collect one data per second, but you need to query the average temperature every 10 minutes, you can use window subclause to get the downsampling data you need. The corresponding SQL is like `select tbname, _wstart date，avg(temperature) temp from table interval(10m)`, in which _wstart is a pseudo column indicting the start time of a widow, 10m is the duration of the window, `avg(temperature)` indicates the aggregate value inside a window. 

  - Data partition query: If you want to get the aggregate value of a lot of thermal meters, you can first partition the data and then perform a series of calculation in the partitioned data spaces. The SQL you need to use is `partition by part_list`. The most common of data partition usage is that when querying a supertable, you can partition data by subtable according to tags to form the data of each subtable into a single time serie to facilitate analytical processing of time series data.

3. Time Serie: When curve plotting or aggregating data based on time lines, date is normally required. Data or time can be imported from Excel, or retrieved from TDengine using SQL statement like `select _wstart date, count(*) cnt from test.meters where ts between A and B interval(1d) fill(0)`, in which the fill() subclause indicates the fill mode when there is data missing, pseudo column _wstart indicates the date to retrieve. 

4. Correlation: Indicates how to correlate data. Dimentions and Metrics can be correlated by tbname, dates and metrics can be correlated by date. All these can cooperate to form visual reports.

### Example - Meters

TDengine has its own specific data model, which uses supertable as template and creates a specific table for each device. Each table can have maximum 4,096 data columns and 128 tags. In the example of meters, assume each meter generates one record per second, then there will be 86,400 records each day and 31,536,000 records every year, then only 1,000 meters will occupy 500GB disk space. So, the common usage of Power BI should be mapping tags to dimention columns, mapping the aggregation of data columns to metric columns, to provide indicators for decision makers.

1. Import Dimentions

Import the tags of tables in PowerBI, and name as "tags", the SQL is like `select distinct tbname, groupid, location from test.meters;`. 

2. Import Metrics

In Power BI, import the average current, average voltage, average phase with 1 hour window, and name it as "data", the SQL is like `select tbname, _wstart ws, avg(current), avg(voltage), avg(phase) from test.meters PARTITION by tbname interval(1h)` .

3. Correlate Dimentions and Metrics

In Power BI, open model view, correlate "tags" and "data", and set "tabname" as the correlation column, then you can use the data in histogram, pie chart, etc. For more information about building visual reports in PowerBI, please refer to [Power BI](https://learn.microsoft.com/power-bi/)。