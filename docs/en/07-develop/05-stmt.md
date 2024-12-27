---
title: Ingesting Data in Parameter Binding Mode
sidebar_label: Parameter Binding
slug: /developer-guide/parameter-binding
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

When inserting data using parameter binding, it can avoid the resource consumption of SQL syntax parsing, thereby significantly improving the write performance. The reasons why parameter binding can improve writing efficiency include:

- Reduced parsing time: With parameter binding, the structure of the SQL statement is determined at the first execution, and subsequent executions only need to replace parameter values, thus avoiding syntax parsing each time and reducing parsing time.  
- Precompilation: When using parameter binding, the SQL statement can be precompiled and cached. When executed later with different parameter values, the precompiled version can be used directly, improving execution efficiency.  
- Reduced network overhead: Parameter binding also reduces the amount of data sent to the database because only parameter values need to be sent, not the complete SQL statement, especially when performing a large number of similar insert or update operations, this difference is particularly noticeable.

**Tips: It is recommended to use parameter binding for data insertion**

Next, we continue to use smart meters as an example to demonstrate the efficient writing functionality of parameter binding with various language connectors:

1. Prepare a parameterized SQL insert statement for inserting data into the supertable `meters`. This statement allows dynamically specifying subtable names, tags, and column values.
2. Loop to generate multiple subtables and their corresponding data rows. For each subtable:
    - Set the subtable's name and tag values (group ID and location).
    - Generate multiple rows of data, each including a timestamp, randomly generated current, voltage, and phase values.
    - Perform batch insertion operations to insert these data rows into the corresponding subtable.
3. Finally, print the actual number of rows inserted into the table.

## WebSocket Connection

<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">
```java
{{#include docs/examples/java/src/main/java/com/taos/example/WSParameterBindingBasicDemo.java:para_bind}}
```

This is a [more detailed parameter binding example](https://github.com/taosdata/TDengine/blob/main/docs/examples/java/src/main/java/com/taos/example/WSParameterBindingFullDemo.java)  

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
The following example are applicable to versions v0.12.4 and above.

```rust
{{#include docs/examples/rust/restexample/examples/stmt2.rs}}
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

<Tabs defaultValue="rust" groupId="lang">
<TabItem label="Rust" value="rust">
<details>
<summary>The following example are applicable to versions v0.12.3 and below</summary> 
```rust
{{#include docs/examples/rust/restexample/examples/stmt.rs}}
```
</details>
</TabItem>
</Tabs>

## Native Connection

<Tabs  defaultValue="java"  groupId="lang">
<TabItem label="Java" value="java">

```java
{{#include docs/examples/java/src/main/java/com/taos/example/ParameterBindingBasicDemo.java:para_bind}}
```

This is a [more detailed parameter binding example](https://github.com/taosdata/TDengine/blob/main/docs/examples/java/src/main/java/com/taos/example/ParameterBindingFullDemo.java)  

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
