---
title: 执行 SQL
sidebar_label: 执行 SQL
toc_max_heading_level: 4
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

TDengine 对 SQL 语言提供了全面的支持，允许用户以熟悉的 SQL 语法进行数据的查询、插入和删除操作。 TDengine 的 SQL 还支持对数据库和数据表的管理操作，如创建、修改和删除数据库及数据表。TDengine 扩展了标准 SQL，引入了时序数据处理特有的功能，如时间序列数据的聚合查询、降采样、插值查询等，以适应时序数据的特点。这些扩展使得用户可以更高效地处理时间序列数据，进行复杂的数据分析和处理。 具体支持的 SQL 语法请参考  [TDengine SQL](../../reference/taos-sql/)  

下面介绍使用各语言连接器通过执行 SQL 完成建库、建表、写入数据和查询数据。


## 建库和表
以智能电表为例，展示如何使用连接器执行 SQL 来创建数据库和表。

<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">

```java
{{#include examples/JDBC/JDBCDemo/src/main/java/com/taosdata/example/JdbcCreatDBDemo.java:create_db_and_table}}
```
> **注意**：如果不使用 `USE power` 指定数据库，则后续对表的操作都需要增加数据库名称作为前缀，如 power.meters。

</TabItem>
<TabItem label="Python" value="python">
</TabItem>
<TabItem label="Go" value="go">
</TabItem>
<TabItem label="Rust" value="rust">
</TabItem>
<TabItem label="C#" value="csharp">
</TabItem>
<TabItem label="R" value="r">
</TabItem>
<TabItem label="C" value="c">
```c
{{#include docs/examples/c/CCreateDBDemo.c:create_db_and_table}}
```
> **注意**：如果不使用 `USE power` 指定数据库，则后续对表的操作都需要增加数据库名称作为前缀，如 power.meters。
</TabItem>
<TabItem label="PHP" value="php">
</TabItem>
</Tabs>

## 插入数据
以智能电表为例，展示如何使用连接器执行 SQL 来插入数据。  

<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">
```java
{{#include examples/JDBC/JDBCDemo/src/main/java/com/taosdata/example/JdbcInsertDataDemo.java:insert_data}}
```

**Note**
NOW 为系统内部函数，默认为客户端所在计算机当前时间。 NOW + 1s 代表客户端当前时间往后加 1 秒，数字后面代表时间单位：a（毫秒），s（秒），m（分），h（小时），d（天），w（周），n（月），y（年）。


</TabItem>
<TabItem label="Python" value="python">
</TabItem>
<TabItem label="Go" value="go">
</TabItem>
<TabItem label="Rust" value="rust">
</TabItem>
<TabItem label="C#" value="csharp">
</TabItem>
<TabItem label="R" value="r">
</TabItem>
<TabItem label="C" value="c">
```c
{{#include docs/examples/c/CInsertDataDemo.c:insert_data}}
```

**Note**
NOW 为系统内部函数，默认为客户端所在计算机当前时间。 NOW + 1s 代表客户端当前时间往后加 1 秒，数字后面代表时间单位：a（毫秒），s（秒），m（分），h（小时），d（天），w（周），n（月），y（年）。
</TabItem>
<TabItem label="PHP" value="php">
</TabItem>
</Tabs>


## 查询数据
以智能电表为例，展示如何使用各语言连接器执行 SQL 来查询数据，并将获取到的结果打印出来。  

<Tabs defaultValue="java" groupId="lang">
<TabItem label="Java" value="java">

```java
{{#include examples/JDBC/JDBCDemo/src/main/java/com/taosdata/example/JdbcQueryDemo.java:query_data}}
```

**Note** 查询和操作关系型数据库一致，使用下标获取返回字段内容时从 1 开始，建议使用字段名称获取。

</TabItem>
<TabItem label="Python" value="python">
</TabItem>
<TabItem label="Go" value="go">
</TabItem>
<TabItem label="Rust" value="rust">
</TabItem>
<TabItem label="C#" value="csharp">
</TabItem>
<TabItem label="R" value="r">
</TabItem>
<TabItem label="C" value="c">
```c
{{#include docs/examples/c/CQueryDataDemo.c:query_data}}
```
</TabItem>
<TabItem label="PHP" value="php">
</TabItem>
</Tabs>


## 执行带有 reqId 的 SQL

reqId 可用于请求链路追踪，reqId 就像分布式系统中的 traceId 作用一样。一个请求可能需要经过多个服务或者模块才能完成。reqId 用于标识和关联这个请求的所有相关操作，以便于我们可以追踪和分析请求的完整路径。

使用 reqId 有下面好处：
- 请求追踪：通过将同一个 reqId 关联到一个请求的所有相关操作，可以追踪请求在系统中的完整路径
- 性能分析：通过分析一个请求的 reqId，可以了解请求在各个服务和模块中的处理时间，从而找出性能瓶颈
- 故障诊断：当一个请求失败时，可以通过查看与该请求关联的 reqId 来找出问题发生的位置

如果用户不设置 reqId，连接器会在内部随机生成一个，但建议由显式用户设置以以更好地跟用户请求关联起来。

下面是各语言连接器设置 reqId 执行 SQL 的代码样例。

<Tabs defaultValue="java" groupId="lang">
<TabItem label="Java" value="java">

```java
{{#include examples/JDBC/JDBCDemo/src/main/java/com/taosdata/example/JdbcReqIdDemo.java:with_reqid}}
```

</TabItem>
<TabItem label="Python" value="python">
</TabItem>
<TabItem label="Go" value="go">
</TabItem>
<TabItem label="Rust" value="rust">
</TabItem>
<TabItem label="C#" value="csharp">
</TabItem>
<TabItem label="R" value="r">
</TabItem>
<TabItem label="C" value="c">
```c
{{#include docs/examples/c/CWithReqIdDemo.c:with_reqid}}
```
</TabItem>
<TabItem label="PHP" value="php">
</TabItem>
</Tabs>
