---
title: 执行 SQL
sidebar_label: 执行 SQL
toc_max_heading_level: 4
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

TDengine 对 SQL 语言提供了全面的支持，允许用户以熟悉的 SQL 语法进行数据的查询、插入和删除操作。 TDengine 的 SQL 还支持对数据库和数据表的管理操作，如创建、修改和删除数据库及数据表。TDengine 扩展了标准 SQL，引入了时序数据处理特有的功能，如时间序列数据的聚合查询、降采样、插值查询等，以适应时序数据的特点。这些扩展使得用户可以更高效地处理时间序列数据，进行复杂的数据分析和处理。 具体支持的 SQL 语法请参考  [TDengine SQL](../../reference/taos-sql/)  

下面介绍使用各语言连接器通过执行 SQL 完成建库、建表、写入数据和查询数据。

:::note

REST 连接：各编程语言的连接器封装使用 `HTTP` 请求的连接，支持数据写入和查询操作，开发者依然使用连接器提供的接口访问 `TDengine`。  
REST API：直接调用 `taosadapter` 提供的 REST API 接口，进行数据写入和查询操作。代码示例使用 `curl` 命令来演示。

:::


## 建库和表
下面以智能电表为例，展示使用各语言连接器如何执行 SQL 命令创建一个名为 `power` 的数据库，然后使用 `power` 数据库为默认数据库。
接着创建一个名为 `meters` 的超级表（STABLE），其表结构包含时间戳、电流、电压、相位等列，以及分组 ID 和位置作为标签。

<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">

```java
{{#include docs/examples/java/src/main/java/com/taos/example/JdbcCreatDBDemo.java:create_db_and_table}}
```

</TabItem>
<TabItem label="Python" value="python">

```python title="Websocket 连接"
{{#include docs/examples/python/create_db_ws.py}}
```

```python title="原生连接"
{{#include docs/examples/python/create_db_native.py}}
```

```python title="Rest 连接"
{{#include docs/examples/python/create_db_rest.py}}
```
</TabItem>
<TabItem label="Go" value="go">
```go
{{#include docs/examples/go/sqlquery/main.go:create_db_and_table}}
```
</TabItem>
<TabItem label="Rust" value="rust">

```rust
{{#include docs/examples/rust/nativeexample/examples/createdb.rs:create_db_and_table}}
```

</TabItem>
<TabItem label="Node.js" value="node">
```js
{{#include docs/examples/node/websocketexample/sql_example.js:create_db_and_table}}
```
</TabItem>
<TabItem label="C#" value="csharp">
```csharp
{{#include docs/examples/csharp/wsInsert/Program.cs:create_db_and_table}}
```
</TabItem>
<TabItem label="C" value="c">

```c  title="Websocket 连接"
{{#include docs/examples/c-ws/create_db_demo.c:create_db_and_table}}
```

```c  title="原生连接"
{{#include docs/examples/c/create_db_demo.c:create_db_and_table}}
```

</TabItem>
<TabItem label="REST API" value="rest">

创建数据库

```bash
curl --location -uroot:taosdata 'http://127.0.0.1:6041/rest/sql' \
--data 'CREATE DATABASE IF NOT EXISTS power'
```

创建表，在 url 中指定数据库为 `power`

```bash
curl --location -uroot:taosdata 'http://127.0.0.1:6041/rest/sql/power' \
--data 'CREATE STABLE IF NOT EXISTS meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))'
```

</TabItem>
</Tabs>
> **注意**：建议采用 `<dbName>.<tableName>` 的格式构造SQL语句，不推荐在应用中采用 `USE DBName` 方式访问。

## 插入数据
下面以智能电表为例，展示如何使用连接器执行 SQL 来插入数据到 `power` 数据库的 `meters` 超级表。样例使用 TDengine 自动建表 SQL 语法，写入 d1001 子表中 3 条数据，写入 d1002 子表中 1 条数据，然后打印出实际插入数据条数。

<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">
```java
{{#include docs/examples/java/src/main/java/com/taos/example/JdbcInsertDataDemo.java:insert_data}}
```

**Note**
NOW 为系统内部函数，默认为客户端所在计算机当前时间。 NOW + 1s 代表客户端当前时间往后加 1 秒，数字后面代表时间单位：a（毫秒），s（秒），m（分），h（小时），d（天），w（周），n（月），y（年）。


</TabItem>
<TabItem label="Python" value="python">

```python title="Websocket 连接"
{{#include docs/examples/python/insert_ws.py}}
```

```python title="原生连接"
{{#include docs/examples/python/insert_native.py}}
```

```python title="Rest 连接"
{{#include docs/examples/python/insert_rest.py}}
```

</TabItem>
<TabItem label="Go" value="go">
```go
{{#include docs/examples/go/sqlquery/main.go:insert_data}}
```
</TabItem>
<TabItem label="Rust" value="rust">

```rust
{{#include docs/examples/rust/nativeexample/examples/insert.rs:insert_data}}
```

</TabItem>
<TabItem label="Node.js" value="node">
```js
{{#include docs/examples/node/websocketexample/sql_example.js:insertData}}
```
</TabItem>
<TabItem label="C#" value="csharp">
```csharp
{{#include docs/examples/csharp/wsInsert/Program.cs:insert_data}}
```
</TabItem>
<TabItem label="C" value="c">

```c title="Websocket 连接"
{{#include docs/examples/c-ws/insert_data_demo.c:insert_data}}
```

```c title="原生连接"
{{#include docs/examples/c/insert_data_demo.c:insert_data}}
```

**Note**
NOW 为系统内部函数，默认为客户端所在计算机当前时间。 NOW + 1s 代表客户端当前时间往后加 1 秒，数字后面代表时间单位：a（毫秒），s（秒），m（分），h（小时），d（天），w（周），n（月），y（年）。
</TabItem>
<TabItem label="REST API" value="rest">

写入数据

```bash
curl --location -uroot:taosdata 'http://127.0.0.1:6041/rest/sql' \
--data 'INSERT INTO power.d1001 USING power.meters TAGS(2,'\''California.SanFrancisco'\'') VALUES (NOW + 1a, 10.30000, 219, 0.31000) (NOW + 2a, 12.60000, 218, 0.33000) (NOW + 3a, 12.30000, 221, 0.31000) power.d1002 USING power.meters TAGS(3, '\''California.SanFrancisco'\'') VALUES (NOW + 1a, 10.30000, 218, 0.25000)'
```

</TabItem>
</Tabs>

## 查询数据
下面以智能电表为例，展示如何使用各语言连接器执行 SQL 来查询数据，从 `power` 数据库 `meters` 超级表中查询最多 100 行数据，并将获取到的结果按行打印出来。  

<Tabs defaultValue="java" groupId="lang">
<TabItem label="Java" value="java">

```java
{{#include docs/examples/java/src/main/java/com/taos/example/JdbcQueryDemo.java:query_data}}
```

**Note** 查询和操作关系型数据库一致，使用下标获取返回字段内容时从 1 开始，建议使用字段名称获取。

</TabItem>
<TabItem label="Python" value="python">

```python title="Websocket 连接"
{{#include docs/examples/python/query_ws.py}}
```

```python title="原生连接"
{{#include docs/examples/python/query_native.py}}
```

```python title="Rest 连接"
{{#include docs/examples/python/query_rest.py}}
```
</TabItem>
<TabItem label="Go" value="go">
```go
{{#include docs/examples/go/sqlquery/main.go:select_data}}
```
</TabItem>
<TabItem label="Rust" value="rust">

```rust
{{#include docs/examples/rust/nativeexample/examples/query.rs:query_data}}
```

rust 连接器还支持使用 **serde** 进行反序列化行为结构体的结果获取方式：
```rust
{{#include docs/examples/rust/nativeexample/examples/query.rs:query_data_2}}
```

</TabItem>
<TabItem label="Node.js" value="node">
```js
{{#include docs/examples/node/websocketexample/sql_example.js:queryData}}
```
</TabItem>
<TabItem label="C#" value="csharp">
```csharp
{{#include docs/examples/csharp/wsInsert/Program.cs:select_data}}
```
</TabItem>
<TabItem label="C" value="c">

```c  title="Websocket 连接"
{{#include docs/examples/c-ws/query_data_demo.c:query_data}}
```

```c  title="原生连接"
{{#include docs/examples/c/query_data_demo.c:query_data}}
```
</TabItem>
<TabItem label="REST API" value="rest">

查询数据

```bash
curl --location -uroot:taosdata 'http://127.0.0.1:6041/rest/sql' \
--data 'SELECT ts, current, location FROM power.meters limit 100'
```

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
{{#include docs/examples/java/src/main/java/com/taos/example/JdbcReqIdDemo.java:with_reqid}}
```

</TabItem>
<TabItem label="Python" value="python">

```python title="Websocket 连接"
{{#include docs/examples/python/reqid_ws.py}}
```

```python title="原生连接"
{{#include docs/examples/python/reqid_native.py}}
```

```python title="Rest 连接"
{{#include docs/examples/python/reqid_rest.py}}
```

</TabItem>
<TabItem label="Go" value="go">
```go
{{#include docs/examples/go/queryreqid/main.go:query_id}}
```
</TabItem>
<TabItem label="Rust" value="rust">

```rust
{{#include docs/examples/rust/nativeexample/examples/query.rs:query_with_req_id}}
```

</TabItem>
<TabItem label="Node.js" value="node">
```js
{{#include docs/examples/node/websocketexample/sql_example.js:sqlWithReqid}}
```
</TabItem>
<TabItem label="C#" value="csharp">
```csharp
{{#include docs/examples/csharp/wsInsert/Program.cs:query_id}}
```
</TabItem>
<TabItem label="C" value="c">

```c "Websocket 连接"
{{#include docs/examples/c-ws/with_reqid_demo.c:with_reqid}}
```

```c "原生连接"
{{#include docs/examples/c/with_reqid_demo.c:with_reqid}}
```

</TabItem>
<TabItem label="REST API" value="rest">

查询数据，指定 reqId 为 3

```bash
curl --location -uroot:taosdata 'http://127.0.0.1:6041/rest/sql?req_id=3' \
--data 'SELECT ts, current, location FROM power.meters limit 1'
```

</TabItem>
</Tabs>
