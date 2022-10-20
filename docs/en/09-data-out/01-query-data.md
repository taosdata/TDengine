---
sidebar_label: SQL
title: Query Data Using SQL
description: Read data from TDengine using basic SQL.
---

# Query Data

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

```sql
select * from d1001 where voltage > 215 order by ts desc limit 2;
```

```title=Output
taos> select * from d1001 where voltage > 215 order by ts desc limit 2;
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

In TDengine CLI `taos`, use the SQL below to get the average voltage of all the meters in California grouped by location.

```
taos> SELECT AVG(voltage) FROM meters GROUP BY location;
       avg(voltage)        |            location            |
=============================================================
             222.000000000 | California.LosAngeles                |
             219.200000000 | California.SanFrancisco               |
Query OK, 2 row(s) in set (0.002136s)
```

### Example 2

In TDengine CLI `taos`, use the SQL below to get the number of rows and the maximum current in the past 24 hours from meters whose groupId is 2.

```
taos> SELECT count(*), max(current) FROM meters where groupId = 2 and ts > now - 24h;
     count(*)  |    max(current)  |
==================================
            5 |             13.4 |
Query OK, 1 row(s) in set (0.002136s)
```

Join queries are only allowed between subtables of the same STable. In [Select](https://docs.tdengine.com/cloud/taos-sql/select), all query operations are marked as to whether they support STables or not.

## Down Sampling and Interpolation

In IoT use cases, down sampling is widely used to aggregate data by time range. The `INTERVAL` keyword in TDengine can be used to simplify the query by time window. For example, the SQL statement below can be used to get the sum of current every 10 seconds from meters table d1001.

```
taos> SELECT sum(current) FROM d1001 INTERVAL(10s);
           ts            |       sum(current)        |
======================================================
 2018-10-03 14:38:00.000 |              10.300000191 |
 2018-10-03 14:38:10.000 |              24.900000572 |
Query OK, 2 row(s) in set (0.000883s)
```

Down sampling can also be used for STable. For example, the below SQL statement can be used to get the sum of current from all meters in California.

```
taos> SELECT SUM(current) FROM meters where location like "California%" INTERVAL(1s);
           ts            |       sum(current)        |
======================================================
 2018-10-03 14:38:04.000 |              10.199999809 |
 2018-10-03 14:38:05.000 |              32.900000572 |
 2018-10-03 14:38:06.000 |              11.500000000 |
 2018-10-03 14:38:15.000 |              12.600000381 |
 2018-10-03 14:38:16.000 |              36.000000000 |
Query OK, 5 row(s) in set (0.001538s)
```

Down sampling also supports time offset. For example, the below SQL statement can be used to get the sum of current from all meters but each time window must start at the boundary of 500 milliseconds.

```
taos> SELECT SUM(current) FROM meters INTERVAL(1s, 500a);
           ts            |       sum(current)        |
======================================================
 2018-10-03 14:38:04.500 |              11.189999809 |
 2018-10-03 14:38:05.500 |              31.900000572 |
 2018-10-03 14:38:06.500 |              11.600000000 |
 2018-10-03 14:38:15.500 |              12.300000381 |
 2018-10-03 14:38:16.500 |              35.000000000 |
Query OK, 5 row(s) in set (0.001521s)
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
