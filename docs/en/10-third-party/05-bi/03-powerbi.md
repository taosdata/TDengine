---
title: Microsoft Power BI
slug: /third-party-tools/analytics/power-bi
---

Power BI is a business analytics tool provided by Microsoft. By configuring the ODBC connector, Power BI can quickly access data from TDengine. Users can import tag data, raw time-series data, or time-aggregated time-series data from TDengine into Power BI to create reports or dashboards, all without any coding involved.

## Prerequisites

Ensure that Power BI Desktop is installed and running (if not installed, please download the latest version for Windows operating system, 32/64-bit, from its official site).

## Install ODBC Driver

Download the latest Windows x64 client driver from the TDengine official website and install it on the machine running Power BI. Once installed successfully, the TDengine driver should be visible in the "ODBC Data Sources (32-bit)" or "ODBC Data Sources (64-bit)" management tools.

## Configure ODBC Data Source

The steps to configure the ODBC data source are as follows.

Step 1: Search for and open the "ODBC Data Sources (32-bit)" or "ODBC Data Sources (64-bit)" management tool in the Windows start menu.  

Step 2: Click the "User DSN" tab → "Add" button to enter the "Create New Data Source" dialog.  
Step 3: Select "TDengine" from the list of drivers and click "Finish" to enter the TDengine ODBC data source configuration page. Fill in the following required information.

- DSN: Data Source Name, required, for example, "MyTDengine".
- Connection Type: Check the "WebSocket" checkbox.
- URL: ODBC Data Source URL, required, for example, `http://127.0.0.1:6041`.
- Database: Indicates the database to connect to, optional, for example, "test".
- Username: Enter the username; if left blank, the default is "root".
- Password: Enter the user password; if left blank, the default is "taosdata".  

Step 4: Click the "Test Connection" button to test the connection. If successful, a message will prompt "Successfully connected to `http://127.0.0.1:6041`".  

Step 5: Click the "OK" button to save the configuration and exit.

## Import TDengine Data into Power BI

The steps to import TDengine data into Power BI are as follows:  

Step 1: Open Power BI, log in, then click "Home" → "Get Data" → "Other" → "ODBC" → "Connect" to add the data source.  

Step 2: Select the data source name you just created, for example, "MyTDengine". If you need to enter SQL, click on the "Advanced options" tab and enter the SQL statement in the editing box of the expanded dialog. Click the "OK" button to connect to the configured data source.  

Step 3: In the "Navigator", you can browse the corresponding data tables/views in the database and load the data.

To fully leverage Power BI's capabilities in analyzing data from TDengine, users need to understand core concepts such as dimensions, measures, windowed queries, data partitioning queries, time series, and correlations before importing data via custom SQL.

- Dimensions: Typically categorical (text) data that describe categories like device, measurement point, model, etc. In TDengine's supertable, dimension information is stored in the tag columns, which can be quickly retrieved with SQL syntax like `select distinct tbname, tag1, tag2 from supertable`.
- Measures: Quantitative (numeric) fields that can be used for calculations, common calculations include sum, average, and minimum values. If the collection frequency for measurement points is 1 second, then one year will have over 30 million records, which would severely impact performance if all this data were imported into Power BI. In TDengine, users can utilize data partitioning queries, windowed queries, and related pseudo-columns to import downsampled data into Power BI. For specific syntax, refer to the distinguished query section of the TDengine official documentation.
- Windowed Queries: For example, if a temperature sensor collects data every second but needs to query the average temperature every 10 minutes, a window clause can be used to obtain the required downsampled query result, corresponding SQL might look like `select tbname, _wstart date, avg(temperature) temp from table interval(10m)`, where `_wstart` is a pseudo-column representing the start time of the time window, and `10m` indicates the duration of the time window, with `avg(temperature)` being the aggregation value within that time window.
- Data Partitioning Queries: If you need to obtain aggregated values for many temperature sensors simultaneously, data can be partitioned and a series of calculations can be performed within the partitioned data space, corresponding SQL would look like `partition by part_list`. The most common use of data partitioning clauses is to segment subtable data in supertable queries by tags, allowing each subtable's data to be separated and forming independent time series for statistical analysis in various time-series scenarios.
- Time Series: When drawing curves or aggregating data by time, a date table is usually required. The date table can be imported from an Excel file or retrieved using SQL in TDengine, for example, `select _wstart date, count(*) cnt from test.meters where ts between A and B interval(1d) fill(0)`, where the `fill` clause indicates the fill mode for missing data, and the pseudo-column `_wstart` represents the date column to be obtained.
- Correlation: Indicates how data relates to one another, such as measures and dimensions being associated through the `tbname` column, while the date table and measures can be associated through the `date` column, facilitating the creation of visual reports.

## Smart Meter Example

TDengine employs a unique data model optimized for the storage and query performance of time-series data. This model uses supertables as templates to create independent tables for each device. Each table is designed with high scalability in mind, capable of containing up to 4096 data columns and 128 tag columns. This design allows TDengine to efficiently handle vast amounts of time-series data while maintaining data flexibility and usability.

For example, if each smart meter generates one record every second, it would produce 86,400 records per day. For 1,000 smart meters, this would result in approximately 600GB of storage space occupied by the records generated annually. Given this large volume of data, business intelligence tools like Power BI play a crucial role in data analysis and visualization.

In Power BI, users can map the tag columns from the TDengine table as dimension columns for data grouping and filtering. Meanwhile, the aggregated results of the data columns can be imported as measure columns for calculating key metrics and generating reports. This way, Power BI helps decision-makers quickly access the information they need, gain insights into business operations, and make more informed decisions.

By following these steps, users can experience the functionality of generating time-series data reports through Power BI.  

Step 1: Use TDengine's taosBenchMark to quickly generate data for 1,000 smart meters over 3 days, with a collection frequency of 1 second.

```shell
taosBenchmark -t 1000 -n 259200 -S 1000 -y
```

Step 2: Import dimension data. In Power BI, import the tag columns from the table, naming it `tags`, and use the following SQL to retrieve all smart meters' tag data under the supertable.

```sql
select distinct tbname device, groupId, location from test.meters
```

Step 3: Import measure data. In Power BI, import the average current, average voltage, and average phase of each smart meter based on a 1-hour time window, naming it `data`, with the following SQL.

```sql
select tbname, _wstart ws, avg(current), avg(voltage), avg(phase) from test.meters PARTITION by tbname interval(1h)
```

Step 4: Import date data. Based on a 1-day time window, retrieve the time range and data count of the time-series data with the following SQL. It is necessary to convert the `date` column format from "text" to "date" in the Power Query editor.

```sql
select _wstart date, count(*) from test.meters interval(1d) having count(*)>0
```

Step 5: Establish relationships between dimensions and measures. Open the model view and establish a relationship between the `tags` and `data` tables by setting `tbname` as the related data column.  

Step 6: Establish a relationship between the date and measure. Open the model view and establish a relationship between the `date` dataset and `data` by using the `date` and `datatime` data columns.  

Step 7: Create reports. Use these data in bar charts, pie charts, and other controls.  

Thanks to TDengine's powerful performance in handling time-series data, users can enjoy a great experience during data import and daily periodic data refreshes. For more information on building visual effects in Power BI, please refer to the official documentation of Power BI.
