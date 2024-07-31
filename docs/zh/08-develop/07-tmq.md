---
title: 数据订阅
sidebar_label: 数据订阅
toc_max_heading_level: 4
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

TDengine 提供了类似于消息队列产品的数据订阅和消费接口。在许多场景中，采用TDengine 的时序大数据平台，无须再集成消息队列产品，从而简化应用程序设计并降低运维成本。本章介绍各语言连接器数据订阅的相关API以及使用方法。 数据订阅的基础知识请参考 [数据订阅](../../advanced/subscription/)  

## 创建主题
请用 taos shell 或者 参考 [执行 SQL](../sql/) 章节用程序执行创建主题的 SQL：`CREATE TOPIC IF NOT EXISTS topic_meters AS SELECT ts, current, voltage, phase, groupid, location FROM meters`  

上述 SQL 将创建一个名为 topic_meters 的订阅。使用该订阅所获取的消息中的每条记录都由此查询语句 `SELECT ts, current, voltage, phase, groupid, location FROM meters` 所选择的列组成。

**注意**
在 TDengine 连接器实现中，对于订阅查询，有以下限制。
- 查询语句限制：订阅查询只能使用 select 语句，不支持其他类型的SQL，如 insert、update或delete等。
- 原始始数据查询：订阅查询只能查询原始数据，而不能查询聚合或计算结果。
- 时间顺序限制：订阅查询只能按照时间正序查询数据。

## 创建消费者

TDengine 消费者的概念跟 Kafka 类似，消费者通过订阅主题来接收数据流。消费者可以配置多种参数，如连接方式、服务器地址、自动提交 Offset 等以适应不同的数据处理需求。有的语言连接器的消费者还支持自动重连和数据传输压缩等高级功能，以确保数据的高效和稳定接收。


### 创建参数
<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">
Java 连接器创建消费者的参数为 Properties， 可以设置如下参数：  

- td.connect.type: 连接方式。jni：表示使用动态库连接的方式，ws/WebSocket：表示使用 WebSocket 进行数据通信。默认为 jni 方式。
- bootstrap.servers: TDengine 服务端所在的`ip:port`，如果使用 WebSocket 连接，则为 taosAdapter 所在的`ip:port`。
- enable.auto.commit: 是否允许自动提交。
- group.id: consumer: 所在的 group。
- value.deserializer: 结果集反序列化方法，可以继承 `com.taosdata.jdbc.tmq.ReferenceDeserializer`，并指定结果集 bean，实现反序列化。也可以继承 `com.taosdata.jdbc.tmq.Deserializer`，根据 SQL 的 resultSet 自定义反序列化方式。
- httpConnectTimeout: 创建连接超时参数，单位 ms，默认为 5000 ms。仅在 WebSocket 连接下有效。
- messageWaitTimeout: 数据传输超时参数，单位 ms，默认为 10000 ms。仅在 WebSocket 连接下有效。
- httpPoolSize: 同一个连接下最大并行请求数。仅在 WebSocket 连接下有效。  
- TSDBDriver.PROPERTY_KEY_ENABLE_COMPRESSION: 传输过程是否启用压缩。仅在使用 Websocket 连接时生效。true: 启用，false: 不启用。默认为 false。
- TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT: 是否启用自动重连。仅在使用 Websocket 连接时生效。true: 启用，false: 不启用。默认为 false。
- TSDBDriver.PROPERTY_KEY_RECONNECT_INTERVAL_MS: 自动重连重试间隔，单位毫秒，默认值 2000。仅在 PROPERTY_KEY_ENABLE_AUTO_RECONNECT 为 true 时生效。
- TSDBDriver.PROPERTY_KEY_RECONNECT_RETRY_COUNT: 自动重连重试次数，默认值 3，仅在 PROPERTY_KEY_ENABLE_AUTO_RECONNECT 为 true 时生效。

其他参数请参考：[Consumer 参数列表](../../develop/tmq/#数据订阅相关参数)， 注意TDengine服务端自 3.2.0.0 版本开始消息订阅中的 auto.offset.reset 默认值发生变化。

:::note

- Java 连接器数据订阅 WebSocket 连接方式跟 原生连接方式，除了在创建消费者时参数不同之外，其他接口并无区别。因此我们以 Websocket 连接方式为例介绍数据订阅的其他功能。

:::

</TabItem>
<TabItem label="Python" value="python">

</TabItem>
<TabItem label="Go" value="go">

</TabItem>
<TabItem label="Rust" value="rust">

</TabItem>
<TabItem label="Node.js" value="node">

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

### Websocket 连接 
<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">


```java
{{#include examples/JDBC/JDBCDemo/src/main/java/com/taosdata/example/AbsWsConsumerLoop.java:create_consumer}}
```
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


### 原生连接 
<Tabs groupId="lang">
<TabItem value="java" label="Java">


```java
{{#include examples/JDBC/JDBCDemo/src/main/java/com/taosdata/example/AbsConsumerLoop.java:create_consumer}}
```


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

<TabItem label="C#" value="csharp">
    
</TabItem>

<TabItem label="R" value="r">
    
</TabItem>

<TabItem label="C" value="c">
    
</TabItem>

<TabItem label="PHP" value="php">

</TabItem>
</Tabs>

## 订阅消费数据
消费者订阅主题后，可以开始接收并处理这些主题中的消息。订阅消费数据的示例代码如下：
### Websocket 连接 
<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">

```java
{{#include examples/JDBC/JDBCDemo/src/main/java/com/taosdata/example/AbsConsumerLoop.java:poll_data_code_piece}}
```

- `subscribe` 方法的参数含义为：订阅的主题列表（即名称），支持同时订阅多个主题。 
- `poll` 每次调用获取一个消息，一个消息中可能包含多个记录。
- `ResultBean` 是我们自定义的一个内部类，其字段名和数据类型与列的名称和数据类型一一对应，这样根据 `value.deserializer` 属性对应的反序列化类可以反序列化出 `ResultBean` 类型的对象。

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

### 原生连接 
<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">

同 Websocket 代码样例。

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

<TabItem label="C#" value="csharp">
    
</TabItem>

<TabItem label="R" value="r">
    
</TabItem>

<TabItem label="C" value="c">
    
</TabItem>

<TabItem label="PHP" value="php">

</TabItem>
</Tabs>

## 指定订阅的 Offset
消费者可以指定从特定 Offset 开始读取分区中的消息，这允许消费者重读消息或跳过已处理的消息。下面展示各语言连接器如何指定订阅的 Offset。  

### Websocket 连接 
<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">

```java
// 获取订阅的 topicPartition
Set<TopicPartition assignment() throws SQLException;

// 获取 offset
long position(TopicPartition partition) throws SQLException;
Map<TopicPartition, Long position(String topic) throws SQLException;
Map<TopicPartition, Long beginningOffsets(String topic) throws SQLException;
Map<TopicPartition, Long endOffsets(String topic) throws SQLException;
Map<TopicPartition, OffsetAndMetadata committed(Set<TopicPartition partitions) throws SQLException;

// 指定下一次 poll 中使用的 offset
void seek(TopicPartition partition, long offset) throws SQLException;
void seekToBeginning(Collection<TopicPartition partitions) throws SQLException;
void seekToEnd(Collection<TopicPartition partitions) throws SQLException;
```

示例代码：

```java
{{#include examples/JDBC/JDBCDemo/src/main/java/com/taosdata/example/ConsumerOffsetSeek.java:consumer_seek}}
```

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

### 原生连接 
<Tabs groupId="lang">

<TabItem value="java" label="Java">

同 Websocket 代码样例。

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

<TabItem label="C#" value="csharp">
    
</TabItem>

<TabItem label="R" value="r">
    
</TabItem>

<TabItem label="C" value="c">
    
</TabItem>

<TabItem label="PHP" value="php">

</TabItem>
</Tabs>


## 提交 Offset
当消费者读取并处理完消息后，它可以提交 Offset，这表示消费者已经成功处理到这个 Offset 的消息。Offset 提交可以是自动的（根据配置定期提交）或手动的（应用程序控制何时提交）。   
当创建消费者时，属性 `enable.auto.commit` 为 false 时，可以手动提交 offset。  
### Websocket 连接 
<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">

```java
void commitSync() throws SQLException;
void commitSync(Map<TopicPartition, OffsetAndMetadata offsets) throws SQLException;
// 异步提交仅在 native 连接下有效
void commitAsync(OffsetCommitCallback<V callback) throws SQLException;
void commitAsync(Map<TopicPartition, OffsetAndMetadata offsets, OffsetCommitCallback<V callback) throws SQLException;
```


```java
{{#include examples/JDBC/JDBCDemo/src/main/java/com/taosdata/example/AbsConsumerLoop.java:commit_code_piece}}
```

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

### 原生连接 
<Tabs groupId="lang">

<TabItem value="java" label="Java">

同 Websocket 代码样例。

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


## 取消订阅和关闭消费
消费者可以取消对主题的订阅，停止接收消息。当消费者不再需要时，应该关闭消费者实例，以释放资源和断开与 TDengine 服务器的连接。  

### Websocket 连接 
<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">

```java
{{#include examples/JDBC/JDBCDemo/src/main/java/com/taosdata/example/AbsConsumerLoop.java:unsubscribe_data_code_piece}}
```

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

### 原生连接 
<Tabs groupId="lang">
<TabItem value="java" label="Java">

同 Websocket 代码样例。

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


## 完整示例
### Websocket 连接 
<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">
```java
{{#include examples/JDBC/JDBCDemo/src/main/java/com/taosdata/example/AbsWsConsumerLoop.java:consumer_demo}}
```

**注意**：这里的 value.deserializer 配置参数值应该根据测试环境的包路径做相应的调整。

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

### 原生连接 
<Tabs groupId="lang">
<TabItem value="java" label="Java">
```java
{{#include examples/JDBC/JDBCDemo/src/main/java/com/taosdata/example/AbsConsumerLoopFull.java:consumer_demo}}
```

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
