---
sidebar_label: Spark
title: Integration with Spark
toc_max_heading_level: 5
---

Apache Spark is an open-source big data processing engine. It is based on in-memory computing and can be used in various scenarios such as batch processing, stream processing, machine learning, and graph computing. It supports the MapReduce computing model and a rich set of computing operators and functions, and has powerful distributed processing and computing capabilities on extremely large-scale data.

Through the `TDengine Java connector`, Spark can quickly read data from TDengine. Leveraging Spark's powerful engine, it can expand TDengine's data processing and computing capabilities. At the same time, through the `TDengine Java connector`, Spark can also write data to TDengine and subscribe to data from TDengine.

## Prerequisites

Prepare the following environment:

- A TDengine cluster of version 3.3.6.0 or higher has been deployed and is running normally (both enterprise and community editions are available).
- taosAdapter can run normally. For detailed reference, see [taosAdapter Reference Manual](../../../reference/components/taosadapter).
- Spark version 3.4.0 or higher ([Spark Download](https://spark.apache.org/downloads.html)).
- Install the JDBC driver. Download the `TDengine JDBC` connector file `taos-jdbcdriver-3.6.2-dist.jar` or a higher version from `maven.org`.

## Configure the Data Source

The following takes writing a Spark task in JAVA and submitting the task for execution via `spark-submit` as an example to introduce the docking process, followed by the complete example code.

**Step 1**: Register the TDengine syntax dialect. See the example `registerDialect()` for details.

**Step 2**: Create a Spark session instance. See the example `createSpark()` for details.

**Step 3**: Establish a JDBC WebSocket connection and prepare the demo data. See the example `prepareDemoData()` for details.

**Step 4**: Verify that data can be correctly retrieved from the data source. See the example `readFromTDengine()` for details.

## Data Analysis

### Data Preparation

Take the smart meter data written in the previous section `Configure the Data Source` as an example.

### Analyze the Weekly Voltage Change Rate
We present a data processing example that is not supported in TDengine but supported in Spark, aiming to illustrate the expansion of TDengine's capabilities after connecting to Spark. 

The `LAG()` function, provided by Spark, is used to obtain data from a previous row of the current row. This function is not supported in TDengine. The example uses this function to analyze the weekly voltage change rate.

**Step 1**: Obtain data through TDengine SQL and create a Spark View. See `createSparkView()` for details.
```sql
select tbname,* from test.meters where tbname='d0'
```

**Step 2**: Use Spark SQL to query the Spark View data and calculate the weekly voltage change rate. The SQL is as follows:
```sql
SELECT tbname, ts, voltage,
      (LAG(voltage, 7) OVER (ORDER BY tbname)) AS voltage_last_week, 
      CONCAT(ROUND(((voltage - voltage_last_week)/voltage_last_week * 100), 1),'%') AS weekly_growth_rate
      FROM sparkMeters
```

**Step 3**: Output the analysis results, as shown in the following figure:
![spark-result](./spark-result.png)

### More Scenario Usages

The following lists some functions that are restricted in TDengine SQL but can be supported by using Spark SQL after connecting Spark to TDengine:

#### Cross-database Data Analysis

TDengine SQL only supports operations within the same database and does not support cross-database data analysis. After connecting Spark to the TDengine data source, you can write Spark SQL for cross-database data analysis.

#### Rich Dataset Operations

TDengine only provides the union operation (union all) for datasets. After connecting Spark to the TDengine data source, you can perform various operations such as intersection and difference on datasets.

#### Subqueries in the Where Clause

TDengine does not support subquery statements in the Where clause. After connecting Spark to the TDengine data source, you can use subqueries in the where clause for filtering operations.

#### Support for Joins on Ordinary Columns
TDengine only supports joins on the primary time column. After connecting Spark to the TDengine data source, you can perform joins on ordinary columns or tag columns, breaking the limitation of only joining on the primary time column.

## Example Source Code
The example is written in JAVA. For compilation and running, refer to the README in the example source code directory.   
The example uses smart meter data as an example to demonstrate the following:

- Write data to TDengine. See writeToTDengine() for details.
- Read TDengine data into Spark. See readFromTDengine() for details.
- Subscribe to data from TDengine. See subscribeFromTDengine() for details.
- Use Spark SQL to analyze the weekly voltage change rate. See analysisDataWithSpark() for details.
  
[Complete Example Source Code](https://github.com/taosdata/tdengine-eco/tree/main/spark)