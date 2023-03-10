---
sidebar_label: 写入
title: SQL 写入数据
description: 使用 TDengine SQL 写入数据的开发指南
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## SQL 样例

这里有一些 `INSERT` 语句的基本样例。用户可以通过 TDengine CLI，TDengine Cloud 的数据浏览器或者通过 TDengine 连接器开发等执行这些语句。

### 一次写入一条

下面这条 INSERT 就将一条记录写入到表 d1001 中：

```sql
INSERT INTO d1001 VALUES (1538548685000, 10.3, 219, 0.31);
```

### 一次写入多条

TDengine 支持一次写入多条记录，比如下面这条命令就将两条记录写入到表 d1001 中：

```sql
INSERT INTO test.d1001 VALUES (1538548684000, 10.2, 220, 0.23) (1538548696650, 10.3, 218, 0.25);
```

### 一次写入多表

TDengine 也支持一次向多个表写入数据，比如下面这条命令就向 d1001 写入两条记录，向 d1002 写入一条记录：

```sql
INSERT INTO test.d1001 VALUES (1538548685000, 10.3, 219, 0.31) (1538548695000, 12.6, 218, 0.33) test.d1002 VALUES (1538548696800, 12.3, 221, 0.31);
```

详细的 SQL INSERT 语法规则参考 [TDengine SQL 的数据写入](https://docs.taosdata.com/cloud/taos-sql/insert)。

## 连接器样例

:::note
在执行下面样例代码的之前，您必须首先建立和 TDengine Cloud 的连接，请参考 [连接 云服务](../../programming/connect/).

:::
<Tabs>
<TabItem value="python" label="Python">

这个例子中，我们使用 `execute` 方法来执行 SQL 和得到被影响的行。参数 `conn` 是类`taosrest.TaosRestConnection` 的一个实例，请参考[连接培训](../../programming/connect/python#connect).

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

在这个例子中，我们使用 `exec` 方法来执行 SQL 。`exec` 是为非查询的 SQL 语句设计的，所有返回的数据都会被忽略。

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

由于 RESTful 接口无状态， 不能使用 `USE db;` 语句来切换数据库, 所以在上面示例中使用了`dbName.tbName`指定表名。

:::

#