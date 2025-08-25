---
title: Microsoft Power BI
slug: /third-party-tools/analytics/power-bi
---

Power BI is a business analytics tool provided by Microsoft. By configuring the use of the ODBC connector, Power BI can quickly access data from TDengine. Users can import tag data, raw time-series data, or time-aggregated time series data from TDengine into Power BI to create reports or dashboards, all without the need for any coding.

## Prerequisites

- TDengine 3.3.4.0 and above version is installed and running normally (both Enterprise and Community versions are available).
- taosAdapter is running normally, refer to [taosAdapter Reference](../../../tdengine-reference/components/taosadapter/).
- Install and run Power BI Desktop software (if not installed, please download the latest version for Windows OS 32/64 bit from its official address).
- Download the latest Windows OS X64 client driver from the TDengine official website and install it on the machine running Power BI. After successful installation, the TDengine driver can be seen in the "ODBC Data Sources (32-bit)" or "ODBC Data Sources (64-bit)" management tool.

## Configure Data Source

**Step 1**, Search and open the [ODBC Data Source (64 bit)] management tool in the Start menu of the Windows operating system and configure it, refer to [Install ODBC Driver](../../../tdengine-reference/client-libraries/odbc/#installation).

**Step 2**, Open Power BI and log in, click [Home] -> [Get Data] -> [Other] -> [ODBC] -> [Connect], add data source.

**Step 3**, Select the data source name just created, such as [MyTDengine], if you need to enter SQL, you can click the [Advanced options] tab, in the expanded dialog box enter the SQL statement. Click the [OK] button to connect to the configured data source.  

**Step 4**, Enter the [Navigator], you can browse the corresponding database's tables/views and load data.

## Data Analysis

### Instructions for use

To fully leverage Power BI's advantages in analyzing data from TDengine, users need to first understand core concepts such as dimensions, metrics, window split queries, data split queries, time-series, and correlation, then import data through custom SQL.

- Dimensions: Usually categorical (text) data, describing device, measurement point, model, and other category information. In TDengine's supertables, dimension information is stored using tag columns, which can be quickly obtained through SQL syntax like "select distinct tbname, tag1, tag2 from supertable".
- Metrics: Quantitative (numeric) fields that can be used for calculations, common calculations include sum, average, and minimum. If the measurement point's collection period is 1s, then there are over 30 million records in a year, importing all these data into Power BI would severely affect its performance. In TDengine, users can use data split queries, window split queries, and other syntax combined with window-related pseudocolumns to import down-sampled data into Power BI, for specific syntax please refer to the TDengine official documentation's feature query function section.
- Window split query: For example, a temperature sensor collects data every second, but needs to query the average temperature every 10 minutes, in this scenario you can use the window clause to get the required down-sampling query result, corresponding SQL like `select tbname, _wstart date, avg(temperature) temp from table interval(10m)`, where `_wstart` is a pseudocolumn, representing the start time of the time window, 10m represents the duration of the time window, avg(temperature) represents the aggregate value within the time window.
- Data split query: If you need to obtain aggregate values for many temperature sensors at the same time, you can split the data, then perform a series of calculations within the split data space, corresponding SQL like `partition by part_list`. The most common use of the data split clause is to split subtable data by tags in supertable queries, isolating each subtable's data into independent time-series, facilitating statistical analysis for various time series scenarios.
- Time-Series: When drawing curves or aggregating data by time, it is usually necessary to introduce a date table. Date tables can be imported from Excel spreadsheets, or obtained in TDengine by executing SQL like `select _wstart date, count(*) cnt from test.meters where ts between A and B interval(1d) fill(0)`, where the fill clause represents the filling mode in case of data missing, and the pseudocolumn `_wstart` is the date column to be obtained.
- Correlation: Tells how data is related, such as metrics and dimensions can be associated together through the tbname column, date tables and metrics can be associated through the date column, combined to form visual reports.

### Smart Meter Example

TDengine employs a unique data model to optimize the storage and query performance of time-series data. This model uses supertables as templates to create an independent table for each device. Each table is designed with high scalability in mind, supporting up to 4096 data columns and 128 tag columns. This design enables TDengine to efficiently handle large volumes of time-series data while maintaining flexibility and ease of use.

Taking smart meters as an example, suppose each meter generates one record per second, resulting in 86,400 records per day. For 1,000 smart meters, the records generated per year would occupy about 600GB of storage space. Facing such a massive volume of data, business intelligence tools like Power BI play a crucial role in data analysis and visualization.

In Power BI, users can map the tag columns in TDengine tables to dimension columns for grouping and filtering data. Meanwhile, the aggregated results of the data columns can be imported as measure columns for calculating key indicators and generating reports. In this way, Power BI helps decision-makers quickly obtain the information they need, gain a deeper understanding of business operations, and make more informed decisions.

Follow the steps below to experience the functionality of generating time-series data reports through Power BI.  

**Step 1**, Use TDengine's taosBenchMark to quickly generate data for 1,000 smart meters over 3 days, with a collection frequency of 1s.

```shell
taosBenchmark -t 1000 -n 259200 -S 1000 -y
```

**Step 2**, Import dimension data. In Power BI, import the tag columns of the table, named as tags, using the following SQL to get the tag data of all smart meters under the supertable.

```sql
select distinct tbname device, groupId, location from test.meters
```

**Step 3**, Import measure data. In Power BI, import the average current, voltage, and phase of each smart meter in 1-hour time windows, named as data, with the following SQL.

```sql
select tbname, _wstart ws, avg(current), avg(voltage), avg(phase) from test.meters PARTITION by tbname interval(1h)
```

**Step 4**, Import date data. Using a 1-day time window, obtain the time range and data count of the time-series data, with the following SQL. In the Power Query editor, convert the format of the date column from "text" to "date".

```sql
select _wstart date, count(*) from test.meters interval(1d) having count(*)>0
```

**Step 5**, Establish the relationship between dimensions and measures. Open the model view and establish the relationship between the tags and data tables, setting tbname as the relationship data column.  

**Step 6**, Establish the relationship between date and measures. Open the model view and establish the relationship between the date dataset and data, with the relationship data columns being date and datetime.  

**Step 7**, Create reports. Use these data in bar charts, pie charts, and other controls.  

Due to TDengine's superior performance in handling time-series data, users can enjoy a very good experience during data import and daily regular data refreshes. For more information on building Power BI visual effects, please refer to the official Power BI documentation.
