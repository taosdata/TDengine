---
sidebar_label: Query
title: Query Data Using SQL
description: Programming Guide for Querying Data using basic SQL.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Query Using SQL

SQL is used by TDengine as its query language. Application programs can send SQL statements to TDengine through REST API or connectors. TDengine's CLI `taos` can also be used to execute ad hoc SQL queries. Here is the list of major query functionalities supported by TDengine：

- Query on single column or multiple columns
- Filter on tags or data columns：>, <, =, <\>, like
- Grouping of results: `Group By`
- Sorting of results: `Order By`
- Limit the number of results: `Limit/Offset`
- Arithmetic on columns of numeric types or aggregate results
- Join query with timestamp alignment
- Aggregate functions: count, max, min, avg, sum, twa, stddev, leastsquares, top, bottom, first, last, percentile, apercentile, last_row, spread, diff

For example, the SQL statement below can be executed in TDengine CLI `taos` to select records with voltage greater than 215 and limit the output to only 2 rows.

```sql title="SQL"
select * from test.d101 where voltage > 100 order by ts desc limit 2;
```

```txt title="output"
           ts            |       current        |   voltage   |        phase         |
======================================================================================
 2018-10-03 14:38:16.800 |             12.30000 |         221 |              0.31000 |
 2018-10-03 14:38:15.000 |             12.60000 |         218 |              0.33000 |
Query OK, 2 row(s) in set (0.001100s)
```

To meet the requirements of varied use cases, some special functions have been added in TDengine. Some examples are `twa` (Time Weighted Average), `spread` (The difference between the maximum and the minimum), and `last_row` (the last row). Furthermore, continuous query is also supported in TDengine.

For detailed query syntax please refer to [Select](https://docs.tdengine.com/cloud/taos-sql/select).

## Aggregation among Tables

In most use cases, there are always multiple kinds of data collection points. A new concept, called STable (abbreviation for super table), is used in TDengine to represent one type of data collection point, and a subtable is used to represent a specific data collection point of that type. Tags are used by TDengine to represent the static properties of data collection points. A specific data collection point has its own values for static properties. By specifying filter conditions on tags, aggregation can be performed efficiently among all the subtables created via the same STable, i.e. same type of data collection points. Aggregate functions applicable for tables can be used directly on STables; the syntax is exactly the same.

In summary, records across subtables can be aggregated by a simple query on their STable. It is like a join operation. However, tables belonging to different STables can not be aggregated. 

### Example 1

In [TDengine CLI](../tool/cli) `taos`, use the SQL below to get the average voltage of all the meters in California grouped by location.

```sql title="SQL"
SELECT location, AVG(voltage) FROM test.meters GROUP BY location;
```

```txt title="output"
         location         |       avg(voltage)        |
=======================================================
 California.PaloAlto      |             109.507000000 |
 California.Sunnyvale     |             109.507000000 |
 California.MountainView  |             109.507000000 |
 California.SanFrancisco  |             109.507000000 |
 California.SanJose       |             109.507000000 |
 California.SanDiego      |             109.507000000 |
 California.SantaClara    |             109.507000000 |
 California.Cupertino     |             109.507000000 |
 California.Campbell      |             109.507000000 |
 California.LosAngles     |             109.507000000 |
Query OK, 10 row(s) in set
```

### Example 2

In TDengine CLI `taos`, use the SQL below to get the number of rows and the maximum current in the past 24 hours from meters whose groupId is 2.

```sql title="SQL"
SELECT count(*), max(current) FROM test.meters where groupId = 2 and ts > now - 24h;
```
```txt title="output"
     count(*)  |    max(current)  |
==================================
            5 |             13.4 |
Query OK, 1 row(s) in set (0.002136s)
```

Join queries are only allowed between subtables of the same STable. In [Select](https://docs.tdengine.com/cloud/taos-sql/select), all query operations are marked as to whether they support STables or not.

## Down Sampling and Interpolation

In IoT use cases, down sampling is widely used to aggregate data by time range. The `INTERVAL` keyword in TDengine can be used to simplify the query by time window. For example, the SQL statement below can be used to get the sum of current every 10 seconds from meters table d1001.

```sql title="SQL"
SELECT _wstart, sum(current) FROM test.d101 INTERVAL(10s) limit 3;
```

```txt title="output"
         _wstart         |       sum(current)        |
======================================================
 2017-07-14 10:40:00.000 |               9.920000076 |
 2017-07-14 10:55:00.000 |               9.840000153 |
 2017-07-14 11:10:00.000 |               9.840000153 |
Query OK, 3 row(s) in set
```

Down sampling can also be used for STable. For example, the below SQL statement can be used to get the sum of current from all meters in California.

```sql title="SQL"
SELECT _wstart, SUM(current) FROM test.meters where location like "California%" INTERVAL(1s) limit 5;
```
```txt title="output"
         _wstart         |       sum(current)        |
======================================================
 2017-07-14 10:40:00.000 |            9920.000076294 |
 2017-07-14 10:55:00.000 |            9840.000152588 |
 2017-07-14 11:10:00.000 |            9840.000152588 |
 2017-07-14 11:25:00.000 |           10119.999885559 |
 2017-07-14 11:40:00.000 |            9800.000190735 |
Query OK, 5 row(s) in set
```

Down sampling also supports time offset. For example, the below SQL statement can be used to get the sum of current from all meters but each time window must start at the boundary of 500 milliseconds.

```sql title="SQL"
SELECT _wstart, SUM(current) FROM test.meters INTERVAL(1s, 500a) limit 5;
```

```txt title="output"
         _wstart         |       sum(current)        |
======================================================
 2017-07-14 10:39:59.500 |            9920.000076294 |
 2017-07-14 10:54:59.500 |            9840.000152588 |
 2017-07-14 11:09:59.500 |            9840.000152588 |
 2017-07-14 11:24:59.500 |           10119.999885559 |
 2017-07-14 11:39:59.500 |            9800.000190735 |
Query OK, 5 row(s) in set
```

In many use cases, it's hard to align the timestamp of the data collected by each collection point. However, a lot of algorithms like FFT require the data to be aligned with same time interval and application programs have to handle this by themselves. In TDengine, it's easy to achieve the alignment using down sampling.

Interpolation can be performed in TDengine if there is no data in a time range.

For more details please refer to [Aggregate by Window](https://docs.tdengine.com/cloud/taos-sql/interval).

## Connector Examples

:::note
Before executing the sample code in this section, you need to firstly establish connection to TDegnine cloud service, please refer to [Connect to TDengine Cloud Service](../../programming/connect/).

:::

<Tabs>
<TabItem value="python" label="Python">

In this example, we use `query` method to execute SQL and get a `result` object. 

```python
{{#include docs/examples/python/develop_tutorial.py:query:nrc}}
```

Get column metadata(column name, column type and column length) from `result`:

```python
{{#include docs/examples/python/develop_tutorial.py:fields:nrc}}
```

Get total rows from `result`:

```python
{{#include docs/examples/python/develop_tutorial.py:rows:nrc}}
```

Iterate over each rows: 

```python
{{#include docs/examples/python/develop_tutorial.py:iter}}
```

</TabItem>
<TabItem value="java" label="Java">

In this example we use `executeQuery` method of `Statement` object and get a `ResultSet` object.

```java
{{#include docs/examples/java/src/main/java/com/taos/example/CloudTutorial.java:query:nrc}}
```

Get column meta from the result:

```java
{{#include docs/examples/java/src/main/java/com/taos/example/CloudTutorial.java:meta:nrc}}
```

Iterate over the result and print each row:

```java
{{#include docs/examples/java/src/main/java/com/taos/example/CloudTutorial.java:iter}}
```

</TabItem>
<TabItem value="go" label="Go">

In this example we use `Query` method to execute SQL and get a `sql.Rows` object.

```go
{{#include docs/examples/go/tutorial/main.go:query:nrc}}
```

Get column names from rows:

```go
{{#include docs/examples/go/tutorial/main.go:meta:nrc}}
```

Iterate over rows and print each row:

```go
{{#include docs/examples/go/tutorial/main.go:iter}}
```

</TabItem>
<TabItem value="rust" label="Rust">

In this example, we use query method to execute SQL and get a result object.

```rust
{{#include docs/examples/rust/cloud-example/examples/tutorial.rs:query:nrc}}
```

Get column meta from the result:

```rust
{{#include docs/examples/rust/cloud-example/examples/tutorial.rs:meta:nrc}}
```

Get all rows and print each row:

```rust
{{#include docs/examples/rust/cloud-example/examples/tutorial.rs:iter}}
```

</TabItem>
<TabItem value="node" label="Node.js">

```javascript
{{#include docs/examples/node/query.js}}
```

</TabItem>

<TabItem value="C#" label="C#">

In this example, we use query method to execute SQL and get a result object.

``` XML
{{#include docs/examples/csharp/cloud-example/inout/inout.csproj}}
```

```C#
{{#include docs/examples/csharp/cloud-example/inout/Program.cs:query}}
```

</TabItem>

</Tabs>
