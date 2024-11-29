---
title: Running SQL Statements
sidebar_label: Running SQL Statements
slug: /developer-guide/running-sql-statements
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

TDengine provides comprehensive support for SQL, allowing users to perform data queries, inserts, and deletions using familiar SQL syntax. TDengine's SQL also supports database and table management operations, such as creating, modifying, and deleting databases and tables. TDengine extends standard SQL by introducing features specific to time-series data processing, such as aggregation queries, downsampling, and interpolation queries, to accommodate the characteristics of time-series data. These extensions enable users to handle time-series data more efficiently and conduct complex data analysis and processing. For specific supported SQL syntax, please refer to [TDengine SQL](../../tdengine-reference/sql-manual/).

Below is an introduction to how to use various language connectors to execute SQL commands for creating databases, creating tables, inserting data, and querying data.

:::note

REST connection: Each programming language's connector encapsulates the connection using `HTTP` requests, supporting data writing and querying operations. Developers still access `TDengine` through the interfaces provided by the connector.  
REST API: Directly calls the REST API interface provided by `taosadapter` to perform data writing and querying operations. Code examples demonstrate using the `curl` command.

:::

## Create Database and Table

Using a smart meter as an example, below demonstrates how to execute SQL commands using various language connectors to create a database named `power` and then set `power` as the default database. Next, it creates a supertable named `meters`, with columns including timestamp, current, voltage, phase, and tags for group ID and location.

<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">

```java
{{#include docs/examples/java/src/main/java/com/taos/example/JdbcCreatDBDemo.java:create_db_and_table}}
```

</TabItem>

<TabItem label="Python" value="python">

```python title="Websocket Connection"
{{#include docs/examples/python/create_db_ws.py}}
```

```python title="Native Connection"
{{#include docs/examples/python/create_db_native.py}}
```

```python title="REST Connection"
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

```c title="Websocket Connection"
{{#include docs/examples/c-ws/create_db_demo.c:create_db_and_table}}
```

```c title="Native Connection"
{{#include docs/examples/c/create_db_demo.c:create_db_and_table}}
```

</TabItem>

<TabItem label="REST API" value="rest">

Create Database

```bash
curl --location -uroot:taosdata 'http://127.0.0.1:6041/rest/sql' \
--data 'CREATE DATABASE IF NOT EXISTS power'
```

Create Table, specifying the database as `power` in the URL

```bash
curl --location -uroot:taosdata 'http://127.0.0.1:6041/rest/sql/power' \
--data 'CREATE STABLE IF NOT EXISTS meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))'
```

</TabItem>
</Tabs>

:::note

It is recommended to construct SQL statements using the `<dbName>.<tableName>` format; using the `USE DBName` approach in the application is not recommended.

:::

## Insert Data

Using a smart meter as an example, below demonstrates how to execute SQL to insert data into the `meters` supertable in the `power` database. The example uses TDengine's automatic table creation SQL syntax to write 3 data entries into the `d1001` subtable and 1 data entry into the `d1002` subtable, and then prints the actual number of inserted data entries.

<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">

```java
{{#include docs/examples/java/src/main/java/com/taos/example/JdbcInsertDataDemo.java:insert_data}}
```

:::note

NOW is an internal function that defaults to the current time of the client's computer. NOW + 1s means the client's current time plus 1 second; the number after represents the time unit: a (milliseconds), s (seconds), m (minutes), h (hours), d (days), w (weeks), n (months), y (years).

:::

</TabItem>

<TabItem label="Python" value="python">

```python title="Websocket Connection"
{{#include docs/examples/python/insert_ws.py}}
```

```python title="Native Connection"
{{#include docs/examples/python/insert_native.py}}
```

```python title="REST Connection"
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

```c title="Websocket Connection"
{{#include docs/examples/c-ws/insert_data_demo.c:insert_data}}
```

```c title="Native Connection"
{{#include docs/examples/c/insert_data_demo.c:insert_data}}
```

:::note

NOW is an internal function that defaults to the current time of the client's computer. NOW + 1s means the client's current time plus 1 second; the number after represents the time unit: a (milliseconds), s (seconds), m (minutes), h (hours), d (days), w (weeks), n (months), y (years).

:::

</TabItem>

<TabItem label="REST API" value="rest">

Write Data

```bash
curl --location -uroot:taosdata 'http://127.0.0.1:6041/rest/sql' \
--data 'INSERT INTO power.d1001 USING power.meters TAGS(2,'\''California.SanFrancisco'\'') VALUES (NOW + 1a, 10.30000, 219, 0.31000) (NOW + 2a, 12.60000, 218, 0.33000) (NOW + 3a, 12.30000, 221, 0.31000) power.d1002 USING power.meters TAGS(3, '\''California.SanFrancisco'\'') VALUES (NOW + 1a, 10.30000, 218, 0.25000)'
```

</TabItem>
</Tabs>

## Query Data

Using a smart meter as an example, below demonstrates how to execute SQL using various language connectors to query data, retrieving up to 100 rows from the `meters` supertable in the `power` database and printing the results line by line.

<Tabs defaultValue="java" groupId="lang">
<TabItem label="Java" value="java">

```java
{{#include docs/examples/java/src/main/java/com/taos/example/JdbcQueryDemo.java:query_data}}
```

:::note

Query operations are consistent with relational databases. When accessing return field content using indexes, start from 1; it is recommended to use field names for retrieval.

:::

</TabItem>

<TabItem label="Python" value="python">

```python title="Websocket Connection"
{{#include docs/examples/python/query_ws.py}}
```

```python title="Native Connection"
{{#include docs/examples/python/query_native.py}}
```

```python title="REST Connection"
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

The Rust connector also supports using **serde** for deserialization to obtain results as structured data:

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

```c title="Websocket Connection"
{{#include docs/examples/c-ws/query_data_demo.c:query_data}}
```

```c title="Native Connection"
{{#include docs/examples/c/query_data_demo.c:query_data}}
```

</TabItem>

<TabItem label="REST API" value="rest">

Query Data

```bash
curl --location -uroot:taosdata 'http://127.0.0.1:6041/rest/sql' \
--data 'SELECT ts, current, location FROM power.meters limit 100'
```

</TabItem>
</Tabs>

## Execute SQL with reqId

reqId can be used for request tracing. It acts similarly to traceId in distributed systems. A request may need to go through multiple services or modules to complete. reqId is used to identify and associate all related operations for this request, making it easier to trace and analyze the complete path of the request.

Benefits of using reqId include:

- Request tracing: By associating the same reqId with all related operations of a request, you can trace the complete path of the request within the system.
- Performance analysis: Analyzing a request's reqId allows you to understand the processing time across various services and modules, helping to identify performance bottlenecks.
- Fault diagnosis: When a request fails, you can find out where the issue occurred by examining the reqId associated with that request.

If users do not set a reqId, the connector will randomly generate one internally, but it is recommended to set it explicitly for better association with user requests.

Below are code samples for setting reqId while executing SQL with various language connectors.

<Tabs defaultValue="java" groupId="lang">
<TabItem label="Java" value="java">

```java
{{#include docs/examples/java/src/main/java/com/taos/example/JdbcReqIdDemo.java:with_reqid}}
```

</TabItem>

<TabItem label="Python" value="python">

```python title="Websocket Connection"
{{#include docs/examples/python/reqid_ws.py}}
```

```python title="Native Connection"
{{#include docs/examples/python/reqid_native.py}}
```

```python title="REST Connection"
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

```c "Websocket Connection"
{{#include docs/examples/c-ws/with_reqid_demo.c:with_reqid}}
```

```c "Native Connection"
{{#include docs/examples/c/with_reqid_demo.c:with_reqid}}
```

</TabItem>

<TabItem label="REST API" value="rest">

Query Data, specifying reqId as 3

```bash
curl --location -uroot:taosdata 'http://127.0.0.1:6041/rest/sql?req_id=3' \
--data 'SELECT ts, current, location FROM power.meters limit 1'
```

</TabItem>
</Tabs>
