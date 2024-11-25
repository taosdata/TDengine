---
title: Ingesting Data in Parameter Binding Mode
sidebar_label: Parameter Binding
slug: /developer-guide/parameter-binding
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

Using parameter binding for writing data can avoid the resource consumption of SQL syntax parsing, thus significantly improving writing performance. The reasons parameter binding can enhance writing efficiency include:

- **Reduced Parsing Time**: With parameter binding, the structure of the SQL statement is determined upon the first execution. Subsequent executions only need to replace the parameter values, thereby avoiding syntax parsing for each execution, which reduces parsing time.
- **Precompilation**: When using parameter binding, SQL statements can be precompiled and cached. When executing with different parameter values later, the precompiled version can be used directly, improving execution efficiency.
- **Reduced Network Overhead**: Parameter binding can also reduce the amount of data sent to the database since only parameter values need to be sent rather than the full SQL statement. This difference is particularly noticeable when executing a large number of similar insert or update operations.

**Tips: Data writing is recommended to use parameter binding.**

Next, we will continue using smart meters as an example to demonstrate how various language connectors efficiently write data using parameter binding:

1. Prepare a parameterized SQL insert statement for inserting data into the supertable `meters`. This statement allows dynamically specifying the subtable name, tags, and column values.
2. Loop to generate multiple subtables and their corresponding data rows. For each subtable:
    - Set the subtable name and tag values (group ID and location).
    - Generate multiple rows of data, each including a timestamp, randomly generated current, voltage, and phase values.
    - Execute batch insert operations to insert these data rows into the corresponding subtable.
3. Finally, print the actual number of rows inserted into the table.

## Websocket Connection

<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">

```java
{{#include docs/examples/java/src/main/java/com/taos/example/WSParameterBindingBasicDemo.java:para_bind}}
```

Here is a [more detailed parameter binding example](https://github.com/taosdata/TDengine/blob/main/docs/examples/java/src/main/java/com/taos/example/WSParameterBindingFullDemo.java).  

</TabItem>

<TabItem label="Python" value="python">

```python
{{#include docs/examples/python/stmt_ws.py}}
```

</TabItem>

<TabItem label="Go" value="go">

```go
{{#include docs/examples/go/stmt/ws/main.go}}
```

</TabItem>

<TabItem label="Rust" value="rust">

```rust
{{#include docs/examples/rust/restexample/examples/stmt.rs}}
```

</TabItem>

<TabItem label="Node.js" value="node">

```js
{{#include docs/examples/node/websocketexample/stmt_example.js:createConnect}}
```

</TabItem>

<TabItem label="C#" value="csharp">

```csharp
{{#include docs/examples/csharp/wsStmt/Program.cs:main}}
```

</TabItem>

<TabItem label="C" value="c">

```c
{{#include docs/examples/c-ws/stmt_insert_demo.c}}
```

</TabItem>
<TabItem label="REST API" value="rest">

Not supported

</TabItem>
</Tabs>

## Native Connection

<Tabs defaultValue="java" groupId="lang">
<TabItem label="Java" value="java">

```java
{{#include docs/examples/java/src/main/java/com/taos/example/ParameterBindingBasicDemo.java:para_bind}}
```

Here is a [more detailed parameter binding example](https://github.com/taosdata/TDengine/blob/main/docs/examples/java/src/main/java/com/taos/example/ParameterBindingFullDemo.java).  

</TabItem>

<TabItem label="Python" value="python">

```python
{{#include docs/examples/python/stmt_native.py}}
```

</TabItem>

<TabItem label="Go" value="go">

```go
{{#include docs/examples/go/stmt/native/main.go}}
```

</TabItem>

<TabItem label="Rust" value="rust">

```rust
{{#include docs/examples/rust/nativeexample/examples/stmt.rs}}
```

</TabItem>

<TabItem label="Node.js" value="node">

Not supported

</TabItem>

<TabItem label="C#" value="csharp">

```csharp
{{#include docs/examples/csharp/stmtInsert/Program.cs:main}}
```

</TabItem>

<TabItem label="C" value="c">

```c
{{#include docs/examples/c/stmt_insert_demo.c}}
```

</TabItem>

<TabItem label="REST API" value="rest">

Not supported

</TabItem>
</Tabs>
