---
sidebar_label: 查询数据
title: 查询数据
description: "主要查询功能，通过连接器执行同步查询和异步查询"
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

## 主要查询功能

TDengine 采用 SQL 作为查询语言。应用程序可以通过 REST API 或连接器发送 SQL 语句，用户还可以通过 TDengine 命令行工具 taos 手动执行 SQL 即席查询（Ad-Hoc Query）。TDengine 支持如下查询功能：

- 单列、多列数据查询
- 标签和数值的多种过滤条件：>, <, =, <\>, like 等
- 聚合结果的分组（Group by）、排序（Order by）、约束输出（Limit/Offset）
- 时间窗口（Interval）、会话窗口（Session）和状态窗口（State_window）等窗口切分聚合查询
- 数值列及聚合结果的四则运算
- 时间戳对齐的连接查询（Join Query: 隐式连接）操作
- 多种聚合/计算函数: count, max, min, avg, sum, twa, stddev, leastsquares, top, bottom, first, last, percentile, apercentile, last_row, spread, diff 等

例如：在命令行工具 taos 中，从表 d1001 中查询出 voltage > 215 的记录，按时间降序排列，仅仅输出 2 条。

```sql title="SQL"
select * from test.d101 where voltage > 215 order by ts desc limit 2;
```

```txt title="output"
           ts            |       current        |   voltage   |        phase         |
======================================================================================
 2018-10-03 14:38:16.800 |             12.30000 |         221 |              0.31000 |
 2018-10-03 14:38:15.000 |             12.60000 |         218 |              0.33000 |
Query OK, 2 row(s) in set (0.001100s)
```

为满足物联网场景的需求，TDengine 支持几个特殊的函数，比如 twa(时间加权平均)，spread (最大值与最小值的差)，last_row(最后一条记录)等，更多与物联网场景相关的函数将添加进来。

具体的查询语法请看 [TDengine SQL 的数据查询](https://docs.taosdata.com/cloud/taos-sql/select) 章节。

## 多表聚合查询

物联网场景中，往往同一个类型的数据采集点有多个。TDengine 采用超级表(STable)的概念来描述某一个类型的数据采集点，一张普通的表来描述一个具体的数据采集点。同时 TDengine 使用标签来描述数据采集点的静态属性，一个具体的数据采集点有具体的标签值。通过指定标签的过滤条件，TDengine 提供了一高效的方法将超级表(某一类型的数据采集点)所属的子表进行聚合查询。对普通表的聚合函数以及绝大部分操作都适用于超级表，语法完全一样。

### 示例一

在 [TDengine CLI](../../tools/cli) ，查找加利福尼亚州所有智能电表采集的电压平均值，并按照 location 分组。

```sql title="SQL"
SELECT AVG(voltage), location FROM test.meters GROUP BY location;
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

### 示例二

在 TDengine CLI `taos`, 查找 groupId 为 2 的所有智能电表过去24小时的记录条数和电流的最大值。

```sql title="SQL"
SELECT count(*), max(current) FROM test.meters where groupId = 2 and ts > now - 24h;
```

```txt title="output"
     count(*)  |    max(current)  |
==================================
            5 |             13.4 |
Query OK, 1 row(s) in set (0.002136s)
```

在 [TDengine SQL 的数据查询](https://docs.taosdata.com/cloud/taos-sql/select) 一章，查询类操作都会注明是否支持超级表。

## 降采样查询、插值

物联网场景里，经常需要通过降采样（down sampling）将采集的数据按时间段进行聚合。TDengine 提供了一个简便的关键词 interval 让按照时间窗口的查询操作变得极为简单。比如，将智能电表 d1001 采集的电流值每 10 秒钟求和

```sql title="SQL"
taos> SELECT _wstart, sum(current) FROM test.d1001 INTERVAL(10s);
```

```txt title="output"
         _wstart         |       sum(current)        |
======================================================
 2018-10-03 14:38:00.000 |              10.300000191 |
 2018-10-03 14:38:10.000 |              24.900000572 |
Query OK, 2 rows in database (0.003139s)
```

降采样操作也适用于超级表，比如：将加利福尼亚州所有智能电表采集的电流值每秒钟求和

```sql title="SQL"
SELECT _wstart, SUM(current) FROM test.meters where location like "California%" INTERVAL(1s);
```

```txt title="output"
         _wstart         |       sum(current)        |
======================================================
 2018-10-03 14:38:04.000 |              10.199999809 |
 2018-10-03 14:38:05.000 |              23.699999809 |
 2018-10-03 14:38:06.000 |              11.500000000 |
 2018-10-03 14:38:15.000 |              12.600000381 |
 2018-10-03 14:38:16.000 |              34.400000572 |
Query OK, 5 rows in database (0.007413s)
```

降采样操作也支持时间偏移，比如：将所有智能电表采集的电流值每秒钟求和，但要求每个时间窗口从 500 毫秒开始

```sql title="SQL"
SELECT _wstart, SUM(current) FROM test.meters INTERVAL(1s, 500a);
```

```txt title="output"
         _wstart         |       sum(current)        |
======================================================
 2018-10-03 14:38:03.500 |              10.199999809 |
 2018-10-03 14:38:04.500 |              10.300000191 |
 2018-10-03 14:38:05.500 |              13.399999619 |
 2018-10-03 14:38:06.500 |              11.500000000 |
 2018-10-03 14:38:14.500 |              12.600000381 |
 2018-10-03 14:38:16.500 |              34.400000572 |
Query OK, 6 rows in database (0.005515s)
```

物联网场景里，每个数据采集点采集数据的时间是难同步的，但很多分析算法(比如 FFT)需要把采集的数据严格按照时间等间隔的对齐，在很多系统里，需要应用自己写程序来处理，但使用 TDengine 的降采样操作就轻松解决。

如果一个时间间隔里，没有采集的数据，TDengine 还提供插值计算的功能。

语法规则细节请见 [TDengine SQL 的按时间窗口切分聚合](https://docs.taosdata.com/cloud/taos-sql/interval) 章节。

## 连接器样例

:::note
在执行下面样例代码的之前，您必须首先建立和 TDengine Cloud 的连接，请参考 [连接 云服务](../../programming/connect/).

:::

<Tabs>
<TabItem value="python" label="Python">

在这个例子里面，我们使用 `query` 方法去执行 SQL，然后获取 `result` 对象。

```python
{{#include docs/examples/python/develop_tutorial.py:query:nrc}}
```

从 `result` 对象里面获取列的元数据，包括列名，列类型和列的长度。

```python
{{#include docs/examples/python/develop_tutorial.py:fields:nrc}}
```

从 `result` 获得总行数：

```python
{{#include docs/examples/python/develop_tutorial.py:rows:nrc}}
```

在每一行上面迭代：

```python
{{#include docs/examples/python/develop_tutorial.py:iter}}
```

</TabItem>
<TabItem value="java" label="Java">

在这个例子中，我们使用 `Statement` 对象的 `executeQuery` 方法并获取 `ResultSet` 对象。

```java
{{#include docs/examples/java/src/main/java/com/taos/example/CloudTutorial.java:query:nrc}}
```

从结果里面得到列的元数据：

```java
{{#include docs/examples/java/src/main/java/com/taos/example/CloudTutorial.java:meta:nrc}}
```

在结果上面迭代打印每一行数据：
```java
{{#include docs/examples/java/src/main/java/com/taos/example/CloudTutorial.java:iter}}
```

</TabItem>
<TabItem value="go" label="Go">

在这个例子中，我们使用 `Query` 方法执行 SQL 并获取了一个 `sql.Rows` 对象。

```go
{{#include docs/examples/go/tutorial/main.go:query:nrc}}
```

从结果行里面获取列名：

```go
{{#include docs/examples/go/tutorial/main.go:meta:nrc}}
```

在每一行上面迭代并打印每一行数据：

```go
{{#include docs/examples/go/tutorial/main.go:iter}}
```

</TabItem>
<TabItem value="rust" label="Rust">

在这个例子里面，我们使用查询方法来执行 SQL ，然后获取到 result 对象。

```rust
{{#include docs/examples/rust/cloud-example/examples/tutorial.rs:query:nrc}}
```

从结果里面获取列的元数据：

```rust
{{#include docs/examples/rust/cloud-example/examples/tutorial.rs:meta:nrc}}
```

获取所以的行数据并打印每一行数据：

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

在这个例子里面，我们使用查询方法来执行 SQL ，然后获取到 result 对象。

``` XML
{{#include docs/examples/csharp/cloud-example/inout/inout.csproj}}
```

```C#
{{#include docs/examples/csharp/cloud-example/inout/Program.cs:query}}
```

</TabItem>

</Tabs>