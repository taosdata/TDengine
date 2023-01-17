---
sidebar_label: SQL
title: Insert Data Using SQL
description: Insert data using TDengine SQL
---

# Insert Data

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## SQL Examples

Here are some brief examples for `INSET` statement. You can execute these statements manually by TDengine CLI or TDengine Cloud Explorer or programmatically by TDengine connectors. 

### Insert Single Row

The below SQL statement is used to insert one row into table "d1001".

```sql
INSERT INTO d1001 VALUES (1538548685000, 10.3, 219, 0.31);
```

### Insert Multiple Rows

Multiple rows can be inserted in a single SQL statement. The example below inserts 2 rows into table "d1001".

```sql
INSERT INTO d1001 VALUES (1538548684000, 10.2, 220, 0.23) (1538548696650, 10.3, 218, 0.25);
```

### Insert into Multiple Tables

Data can be inserted into multiple tables in the same SQL statement. The example below inserts 2 rows into table "d1001" and 1 row into table "d1002".

```sql
INSERT INTO d1001 VALUES (1538548685000, 10.3, 219, 0.31) (1538548695000, 12.6, 218, 0.33) d1002 VALUES (1538548696800, 12.3, 221, 0.31);
```

For more details about `INSERT` please refer to [INSERT](https://docs.tdengine.com/cloud/taos-sql/insert).


## Connector Examples

:::note
Before executing the sample code in this section, you need to firstly establish connection to TDegnine cloud service, please refer to [Connect to TDengine Cloud Service](../../programming/connect/).

:::

<Tabs>
<TabItem value="python" label="Python">

In this example, we use `execute` method to execute SQL and get affected rows. The variable `conn` is an instance of class  `taosrest.TaosRestConnection` we just created at [Connect Tutorial](../../programming/connect/python#connect).

```python
{{#include docs/examples/python/develop_tutorial.py:insert}}
```
</TabItem>
<TabItem value="java" label="Java">

```java
{{#include docs/examples/java/src/main/java/com/taos/example/CloudTutorial.java:insert}}
```

</TabItem>
<TabItem value="go" label="Go">

```go
{{#include docs/examples/go/tutorial/main.go:insert}}
```

</TabItem>
<TabItem value="rust" label="Rust">

In this example, we use `exec` method to execute SQL. `exec` is designed for some non-query SQL statements, all returned data would be ignored.

```rust
{{#include docs/examples/rust/cloud-example/examples/tutorial.rs:insert}}
```

</TabItem>
<TabItem value="node" label="Node.js">

```javascript
{{#include docs/examples/node/insert.js}}
```

</TabItem>

<TabItem value="C#" label="C#">

``` XML
{{#include docs/examples/csharp/cloud-example/inout/inout.csproj}}
```

```csharp
{{#include docs/examples/csharp/cloud-example/inout/Program.cs:insert}}
```

</TabItem>

</Tabs>

:::note
`Use` statement is not applicable for cloud service since REST API is stateless.
:::