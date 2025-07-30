---
title: Running SQL Statements
sidebar_label: Running SQL Statements
slug: /developer-guide/running-sql-statements
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

TDengine provides comprehensive support for the SQL language, allowing users to query, insert, and delete data using familiar SQL syntax. TDengine's SQL also supports database and table management operations, such as creating, modifying, and deleting databases and tables. TDengine extends standard SQL by introducing features unique to time-series data processing, such as aggregation queries, downsampling, and interpolation queries, to adapt to the characteristics of time-series data. These extensions enable users to process time-series data more efficiently and perform complex data analysis and processing. For specific supported SQL syntax, please refer to [TDengine SQL](../../tdengine-reference/sql-manual/)

Below, we introduce how to use language connectors to execute SQL for creating databases, tables, writing data, and querying data.

:::note

REST connection: Connectors for various programming languages encapsulate the use of `HTTP` requests for connections, supporting data writing and querying operations, with developers still using the interfaces provided by the connectors to access `TDengine`.  
REST API: Directly call the REST API interface provided by `taosadapter` for data writing and querying operations. Code examples use the `curl` command for demonstration.

:::

## Creating Databases and Tables

Below, using smart meters as an example, we show how to use language connectors to execute SQL commands to create a database named `power`, then use the `power` database as the default database.
Next, create a supertable (STABLE) named `meters`, whose table structure includes columns for timestamp, current, voltage, phase, etc., and labels for group ID and location.

<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">

```java
{{#include docs/examples/java/src/main/java/com/taos/example/JdbcCreatDBDemo.java:create_db_and_table}}
```

</TabItem>
<TabItem label="Python" value="python">

```python title="WebSocket Connection"
{{#include docs/examples/python/create_db_ws.py}}
```

```python title="Native Connection"
{{#include docs/examples/python/create_db_native.py}}
```

```python title="Rest Connection"
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

```c  title="WebSocket Connection"
{{#include docs/examples/c-ws-new/create_db_demo.c:create_db_and_table}}
```

```c  title="Native Connection"
{{#include docs/examples/c/create_db_demo.c:create_db_and_table}}
```

</TabItem>
<TabItem label="REST API" value="rest">

Create Database

```shell
curl --location -uroot:taosdata 'http://127.0.0.1:6041/rest/sql' \
--data 'CREATE DATABASE IF NOT EXISTS power'
```

Create Table, specify the database as `power` in the URL

```shell
curl --location -uroot:taosdata 'http://127.0.0.1:6041/rest/sql/power' \
--data 'CREATE STABLE IF NOT EXISTS meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))'
```

</TabItem>
</Tabs>
> **Note**: It is recommended to construct SQL statements in the format of `<dbName>.<tableName>`. It is not recommended to use `USE DBName` in applications.

## Insert Data

Below, using smart meters as an example, demonstrates how to use connectors to execute SQL to insert data into the `power` database's `meters` supertable. The example uses TDengine's auto table creation SQL syntax, writes 3 records into the d1001 subtable, writes 1 record into the d1002 subtable, and then prints the actual number of records inserted.

<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">
```java
{{#include docs/examples/java/src/main/java/com/taos/example/JdbcInsertDataDemo.java:insert_data}}
```

**Note**
NOW is an internal system function, defaulting to the current time of the client's computer. NOW + 1s represents the client's current time plus 1 second, with the number following representing the time unit: a (millisecond), s (second), m (minute), h (hour), d (day), w (week), n (month), y (year).

</TabItem>
<TabItem label="Python" value="python">

```python title="WebSocket Connection"
{{#include docs/examples/python/insert_ws.py}}
```

```python title="Native Connection"
{{#include docs/examples/python/insert_native.py}}
```

```python title="Rest Connection"
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

```c title="WebSocket Connection"
{{#include docs/examples/c-ws-new/insert_data_demo.c:insert_data}}
```

```c title="Native Connection"
{{#include docs/examples/c/insert_data_demo.c:insert_data}}
```

**Note**
NOW is an internal system function, defaulting to the current time of the client's computer. NOW + 1s represents the client's current time plus 1 second, where the number is followed by a time unit: a (milliseconds), s (seconds), m (minutes), h (hours), d (days), w (weeks), n (months), y (years).
</TabItem>
<TabItem label="REST API" value="rest">

Write data

```shell
curl --location -uroot:taosdata 'http://127.0.0.1:6041/rest/sql' \
--data 'INSERT INTO power.d1001 USING power.meters TAGS(2,'\''California.SanFrancisco'\'') VALUES (NOW + 1a, 10.30000, 219, 0.31000) (NOW + 2a, 12.60000, 218, 0.33000) (NOW + 3a, 12.30000, 221, 0.31000) power.d1002 USING power.meters TAGS(3, '\''California.SanFrancisco'\'') VALUES (NOW + 1a, 10.30000, 218, 0.25000)'
```

</TabItem>
</Tabs>

## Query data

Below, using smart meters as an example, demonstrates how to use connectors in various languages to execute SQL to query data from the `power` database `meters` supertable, querying up to 100 rows of data and printing the results line by line.

<Tabs defaultValue="java" groupId="lang">
<TabItem label="Java" value="java">

```java
{{#include docs/examples/java/src/main/java/com/taos/example/JdbcQueryDemo.java:query_data}}
```

**Note** Querying and operating relational databases are consistent, use indices starting from 1 to get returned field content, and it is recommended to use field names to retrieve.

</TabItem>
<TabItem label="Python" value="python">

```python title="WebSocket Connection"
{{#include docs/examples/python/query_ws.py}}
```

```python title="Native Connection"
{{#include docs/examples/python/query_native.py}}
```

```python title="Rest Connection"
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

Rust connector also supports using **serde** for deserializing to get structured results:

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

```c  title="WebSocket Connection"
{{#include docs/examples/c-ws-new/query_data_demo.c:query_data}}
```

```c  title="Native Connection"
{{#include docs/examples/c/query_data_demo.c:query_data}}
```

</TabItem>
<TabItem label="REST API" value="rest">

Query Data

```shell
curl --location -uroot:taosdata 'http://127.0.0.1:6041/rest/sql' \
--data 'SELECT ts, current, location FROM power.meters limit 100'
```

</TabItem>
</Tabs>

## Execute SQL with reqId

reqId can be used for request link tracing, similar to the role of traceId in distributed systems. A request might need to pass through multiple services or modules to be completed. reqId is used to identify and associate all related operations of this request, allowing us to track and analyze the complete path of the request.

Using reqId has the following benefits:

- Request tracing: By associating the same reqId with all related operations of a request, you can trace the complete path of the request in the system.
- Performance analysis: By analyzing a request's reqId, you can understand the processing time of the request across various services and modules, thereby identifying performance bottlenecks.
- Fault diagnosis: When a request fails, you can identify where the problem occurred by examining the reqId associated with the request.

If the user does not set a reqId, the connector will internally generate one randomly, but it is recommended that users explicitly set it to better associate it with their requests.

Below are code examples of setting reqId to execute SQL in various language connectors.

<Tabs defaultValue="java" groupId="lang">
<TabItem label="Java" value="java">

```java
{{#include docs/examples/java/src/main/java/com/taos/example/JdbcReqIdDemo.java:with_reqid}}
```

</TabItem>
<TabItem label="Python" value="python">

```python title="WebSocket Connection"
{{#include docs/examples/python/reqid_ws.py}}
```

```python title="Native Connection"
{{#include docs/examples/python/reqid_native.py}}
```

```python title="Rest Connection"
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

```c title="WebSocket Connection"
{{#include docs/examples/c-ws-new/with_reqid_demo.c:with_reqid}}
```

```c title="Native Connection"
{{#include docs/examples/c/with_reqid_demo.c:with_reqid}}
```

</TabItem>
<TabItem label="REST API" value="rest">

Query data, specify reqId as 3

```shell
curl --location -uroot:taosdata 'http://127.0.0.1:6041/rest/sql?req_id=3' \
--data 'SELECT ts, current, location FROM power.meters limit 1'
```

</TabItem>
</Tabs>
