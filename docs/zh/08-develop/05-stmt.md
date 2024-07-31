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

下面我们继续以智能电表为例，展示各语言连接器使用参数绑定高效写入的功能。  
## Websocket 连接
<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">
```java
{{#include examples/JDBC/JDBCDemo/src/main/java/com/taosdata/example/WSParameterBindingBasicDemo.java:para_bind}}
```


这是一个[更详细的参数绑定示例](https://github.com/taosdata/TDengine/blob/main/examples/JDBC/JDBCDemo/src/main/java/com/taosdata/example/WSParameterBindingFullDemo.java)  

</TabItem>
<TabItem label="Python" value="python">

    ```python
    {{#include docs/examples/python/connect_websocket_examples.py:connect}}
    ```
</TabItem>
<TabItem label="Go" value="go">

</TabItem>
<TabItem label="Rust" value="rust">

</TabItem>
<TabItem label="Node.js" value="node">

    ```js
        {{#include docs/examples/node/websocketexample/sql_example.js:createConnect}}
    ```
</TabItem>
<TabItem label="C#" value="csharp">

</TabItem>
<TabItem label="R" value="r">

</TabItem>
<TabItem label="C" value="c">

</TabItem>
<TabItem label="PHP" value="php">

</TabItem>
</Tabs>

## 原生连接
<Tabs  defaultValue="java"  groupId="lang">
<TabItem label="Java" value="java">

```java
{{#include examples/JDBC/JDBCDemo/src/main/java/com/taosdata/example/ParameterBindingBasicDemo.java:para_bind}}
```

这是一个[更详细的参数绑定示例](https://github.com/taosdata/TDengine/blob/main/examples/JDBC/JDBCDemo/src/main/java/com/taosdata/example/ParameterBindingFullDemo.java)  

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

</TabItem>
<TabItem label="PHP" value="php">

</TabItem>

</Tabs>
