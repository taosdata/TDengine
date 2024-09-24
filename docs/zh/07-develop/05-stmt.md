---
title: 参数绑定写入
sidebar_label: 参数绑定
toc_max_heading_level: 4
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

通过参数绑定方式写入数据时，能避免SQL语法解析的资源消耗，从而显著提升写入性能。参数绑定能提高写入效率的原因主要有以下几点：

- 减少解析时间：通过参数绑定，SQL 语句的结构在第一次执行时就已经确定，后续的执行只需要替换参数值，这样可以避免每次执行时都进行语法解析，从而减少解析时间。  
- 预编译：当使用参数绑定时，SQL 语句可以被预编译并缓存，后续使用不同的参数值执行时，可以直接使用预编译的版本，提高执行效率。  
- 减少网络开销：参数绑定还可以减少发送到数据库的数据量，因为只需要发送参数值而不是完整的 SQL 语句，特别是在执行大量相似的插入或更新操作时，这种差异尤为明显。 

**Tips: 数据写入推荐使用参数绑定方式**

下面我们继续以智能电表为例，展示各语言连接器使用参数绑定高效写入的功能：
1. 准备一个参数化的 SQL 插入语句，用于向超级表 `meters` 中插入数据。这个语句允许动态地指定子表名、标签和列值。
2. 循环生成多个子表及其对应的数据行。对于每个子表：
    - 设置子表的名称和标签值（分组 ID 和位置）。
    - 生成多行数据，每行包括一个时间戳、随机生成的电流、电压和相位值。
    - 执行批量插入操作，将这些数据行插入到对应的子表中。
3. 最后打印实际插入表中的行数。 

## Websocket 连接
<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">
```java
{{#include docs/examples/java/src/main/java/com/taos/example/WSParameterBindingBasicDemo.java:para_bind}}
```


这是一个[更详细的参数绑定示例](https://github.com/taosdata/TDengine/blob/main/docs/examples/java/src/main/java/com/taos/example/WSParameterBindingFullDemo.java)  

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
不支持
</TabItem>
<TabItem label="REST API" value="rest">
不支持
</TabItem>   
</Tabs>

## 原生连接
<Tabs  defaultValue="java"  groupId="lang">
<TabItem label="Java" value="java">

```java
{{#include docs/examples/java/src/main/java/com/taos/example/ParameterBindingBasicDemo.java:para_bind}}
```

这是一个[更详细的参数绑定示例](https://github.com/taosdata/TDengine/blob/main/docs/examples/java/src/main/java/com/taos/example/ParameterBindingFullDemo.java)  

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
不支持
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
不支持
</TabItem>   
</Tabs>
