---
title: Managing Consumers
slug: /developer-guide/manage-consumers
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

TDengine provides data subscription and consumption interfaces similar to those of message queue products. In many scenarios, by adopting TDengine's time-series big data platform, there is no need to integrate additional message queue products, thus simplifying application design and reducing maintenance costs. This chapter introduces the related APIs and usage methods for data subscription with various language connectors. For basic information on data subscription, please refer to [Data Subscription](../../advanced/subscription/)

## Creating Topics

Please use TDengine CLI or refer to the [Execute SQL](../running-sql-statements/) section to execute the SQL for creating topics: `CREATE TOPIC IF NOT EXISTS topic_meters AS SELECT ts, current, voltage, phase, groupid, location FROM meters`

The above SQL will create a subscription named topic_meters. Each record in the messages obtained using this subscription is composed of the columns selected by this query statement `SELECT ts, current, voltage, phase, groupid, location FROM meters`.

**Note**
In the implementation of TDengine connectors, there are the following limitations for subscription queries.
- Only data subscription is supported, and subscription with `with meta` is not supported.
  - Java(WebSocket connection), Go, and Rust connectors support subscribing to databases, super tables, and `SELECT` queries.
  - Java(Native connection), C#, Python, and Node.js connectors only support subscribing to `SELECT` statements and do not support other types of SQL, such as subscribing to databases or super tables.
- Raw data query: Subscription queries can only query raw data, not aggregated or calculated results.
- Time order limitation: Subscription queries can only query data in chronological order.

## Creating Consumers

The concept of TDengine consumers is similar to Kafka, where consumers receive data streams by subscribing to topics. Consumers can be configured with various parameters, such as connection methods, server addresses, automatic Offset submission, etc., to suit different data processing needs. Some language connectors' consumers also support advanced features such as automatic reconnection and data transmission compression to ensure efficient and stable data reception.

### Creation Parameters

There are many parameters for creating consumers, which flexibly support various connection types, Offset submission methods, compression, reconnection, deserialization, and other features. The common basic configuration items applicable to all language connectors are shown in the following table:

|      Parameter Name       |  Type   | Description                                                  | Remarks                                                      |
| :-----------------------: | :-----: | ------------------------------------------------------------ | ------------------------------------------------------------ |
|      `td.connect.ip`      | string  | FQDN of Server                                               | ip or host name                                              |
|     `td.connect.user`     | string  | Username                                                     |                                                              |
|     `td.connect.pass`     | string  | Password                                                     |                                                              |
|     `td.connect.port`     | integer | Server port number                                           |                                                              |
|        `group.id`         | string  | Consumer group ID, the same consumer group shares consumption progress | <br />**Required**. Maximum length: 192,excess length will be cut off. can not contain colon ':'.<br />Each topic can have up to 100 consumer groups |
|        `client.id`        | string  | Client ID                                                    | Maximum length: 255, excess length will be cut off.          |
|    `auto.offset.reset`    |  enum   | Initial position of the consumer group subscription          | <br />`earliest`: default(version < 3.2.0.0); subscribe from the beginning; <br/>`latest`: default(version >= 3.2.0.0); only subscribe from the latest data; <br/>`none`: cannot subscribe without a committed offset |
|   `enable.auto.commit`    | boolean | Whether to enable automatic consumption point submission, true: automatic submission, client application does not need to commit; false: client application needs to commit manually | Default is true                                              |
| `auto.commit.interval.ms` | integer | Time interval for automatically submitting consumption records, in milliseconds | Default is 5000                                              |
|   `msg.with.table.name`   | boolean | Whether to allow parsing the table name from the message, not applicable to column subscription (column subscription can write tbname as a column in the subquery statement) (from version 3.2.0.0 this parameter is deprecated, always true) | Default is off                                               |
|      `enable.replay`      | boolean | Whether to enable data replay function                       | Default is off                                               |
|   `session.timeout.ms`    | integer | Timeout after consumer heartbeat is lost, after which rebalance logic is triggered, and upon success, that consumer will be removed (supported from version 3.3.3.0) | Default is 12000, range [6000, 1800000]                      |
|  `max.poll.interval.ms`   | integer | The longest time interval for consumer poll data fetching, exceeding this time will be considered as the consumer being offline, triggering rebalance logic, and upon success, that consumer will be removed (supported from version 3.3.3.0) | Default is 300000, range [1000, INT32_MAX]                   |
|    `fetch.max.wait.ms`    | integer | The maximum time it takes for the server to return data once (supported from version 3.3.6.0) | Default is 1000, range [1, INT32_MAX]                        |
|      `min.poll.rows`      | integer | The minimum number of data returned by the server once (supported from version 3.3.6.0) | Default is 4096, range [1, INT32_MAX]                        |
|   `msg.consume.rawdata`   | integer | When consuming data, the data type pulled is binary and cannot be parsed. It is an internal parameter and is only used for taosx data migration (supported from version 3.3.6.0) | The default value of 0 indicates that it is not effective, and non-zero indicates that it is effective |

Below are the connection parameters for connectors in various languages:
<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">
The parameters for creating a consumer with the Java connector are Properties. For a list of parameters you can set, please refer to [Consumer Parameters](../../tdengine-reference/client-libraries/java/)  
For other parameters, refer to the common basic configuration items mentioned above.

</TabItem>
<TabItem label="Python" value="python">
The `td.connect.websocket.scheme` parameter is provided to indicate the protocol type, other parameters are the same as the common basic configuration items.
</TabItem>
<TabItem label="Go" value="go">

Supported properties list for creating consumers:

- `ws.url`: WebSocket connection address.
- `ws.message.channelLen`: WebSocket message channel buffer length, default 0.
- `ws.message.timeout`: WebSocket message timeout, default 5m.
- `ws.message.writeWait`: WebSocket message write timeout, default 10s.
- `ws.message.enableCompression`: Whether to enable compression for WebSocket, default false.
- `ws.autoReconnect`: Whether WebSocket should automatically reconnect, default false.
- `ws.reconnectIntervalMs`: WebSocket reconnect interval in milliseconds, default 2000.
- `ws.reconnectRetryCount`: WebSocket reconnect retry count, default 3.
- `timezone`: The timezone used for parsing time-type data in subscription results, using the IANA timezone format, e.g., `Asia/Shanghai`(supported in v3.7.4 and above).

See the table above for other parameters.

</TabItem>
<TabItem label="Rust" value="rust">
The parameters for creating a consumer with the Rust connector are DSN. For a list of parameters you can set, please refer to [DSN](../../tdengine-reference/client-libraries/rust/#dsn)  
For other parameters, refer to the common basic configuration items mentioned above.

</TabItem>
<TabItem label="Node.js" value="node">
The `WS_URL` parameter is provided to indicate the server address to connect to, other parameters are the same as the common basic configuration items.
</TabItem>
<TabItem label="C#" value="csharp">
Supported properties list for creating consumers:

- `useSSL`: Whether to use SSL connection, default false.
- `token`: Token for connecting to TDengine cloud.
- `ws.message.enableCompression`: Whether to enable WebSocket compression, default false.
- `ws.autoReconnect`: Whether to automatically reconnect, default false.
- `ws.reconnect.retry.count`: Reconnect attempts, default 3.
- `ws.reconnect.interval.ms`: Reconnect interval in milliseconds, default 2000.

See the table above for other parameters.

</TabItem>
<TabItem label="C" value="c">
- WebSocket connection: Since it uses dsn, the four configuration items `td.connect.ip`, `td.connect.port`, `td.connect.user`, and `td.connect.pass` are not needed, the rest are the same as the common configuration items.  
- Native connection: Same as the common basic configuration items.

</TabItem>
<TabItem label="REST API" value="rest">
Not supported
</TabItem>
</Tabs>

### WebSocket Connection

Introduces how connectors in various languages use WebSocket connection method to create consumers. Specify the server address to connect, set auto-commit, start consuming from the latest message, specify `group.id` and `client.id`, etc. Some language connectors also support deserialization parameters.

<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">

```java
{{#include docs/examples/java/src/main/java/com/taos/example/WsConsumerLoopFull.java:create_consumer}}
```

</TabItem>

<TabItem label="Python" value="python">

```python
{{#include docs/examples/python/tmq_websocket_example.py:create_consumer}}
```

</TabItem>

<TabItem label="Go" value="go">
```go
{{#include docs/examples/go/tmq/ws/main.go:create_consumer}}
```
</TabItem>

<TabItem label="Rust" value="rust">

```rust
{{#include docs/examples/rust/restexample/examples/tmq.rs:create_consumer_dsn}}
```

```rust
{{#include docs/examples/rust/restexample/examples/tmq.rs:create_consumer_ac}}
```

</TabItem>

<TabItem label="Node.js" value="node">

```js
    {{#include docs/examples/node/websocketexample/tmq_example.js:create_consumer}}
```

</TabItem>

<TabItem label="C#" value="csharp">
```csharp
{{#include docs/examples/csharp/wssubscribe/Program.cs:create_consumer}}
```
</TabItem>
<TabItem label="C" value="c">
```c
{{#include docs/examples/c-ws-new/tmq_demo.c:create_consumer_1}}
```

```c
{{#include docs/examples/c-ws-new/tmq_demo.c:create_consumer_2}}
```

Call the `build_consumer` function to attempt to obtain the consumer instance `tmq`. Print a success log if successful, and a failure log if not.
</TabItem>
<TabItem label="REST API" value="rest">
Not supported
</TabItem>
</Tabs>

### Native Connection

Introduce how connectors in various languages use native connections to create consumers. Specify the server address for the connection, set auto-commit, start consuming from the latest message, and specify information such as `group.id` and `client.id`. Some language connectors also support deserialization parameters.

<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">

```java
{{#include docs/examples/java/src/main/java/com/taos/example/ConsumerLoopFull.java:create_consumer}}
```

</TabItem>

<TabItem label="Python" value="python">

```python
{{#include docs/examples/python/tmq_native.py:create_consumer}}
```

</TabItem>

<TabItem label="Go" value="go">
```go
{{#include docs/examples/go/tmq/native/main.go:create_consumer}}
```
</TabItem>

<TabItem label="Rust" value="rust">
```rust
{{#include docs/examples/rust/nativeexample/examples/tmq.rs:create_consumer_dsn}}
```

```rust
{{#include docs/examples/rust/nativeexample/examples/tmq.rs:create_consumer_ac}}
```

</TabItem>
<TabItem label="Node.js" value="node">
Not supported
</TabItem>
<TabItem label="C#" value="csharp">
```csharp
{{#include docs/examples/csharp/subscribe/Program.cs:create_consumer}}
```
</TabItem>

<TabItem label="C" value="c">

```c
{{#include docs/examples/c/tmq_demo.c:create_consumer_1}}
```

```c
{{#include docs/examples/c/tmq_demo.c:create_consumer_2}}
```

Call the `build_consumer` function to attempt to obtain the consumer instance `tmq`. Print a success log if successful, and a failure log if not.
</TabItem>
<TabItem label="REST API" value="rest">
Not supported
</TabItem>
</Tabs>

## Subscribe to Consume Data

After subscribing to a topic, consumers can start receiving and processing messages from these topics. The example code for subscribing to consume data is as follows:

### WebSocket Connection

<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">

```java
{{#include docs/examples/java/src/main/java/com/taos/example/WsConsumerLoopFull.java:poll_data_code_piece}}
```

- The parameters of the `subscribe` method mean: the list of topics subscribed to (i.e., names), supporting subscription to multiple topics simultaneously.
- `poll` is called each time to fetch a message, which may contain multiple records.
- `ResultBean` is a custom internal class, whose field names and data types correspond one-to-one with the column names and data types, allowing objects of type `ResultBean` to be deserialized using the `value.deserializer` property's corresponding deserialization class.
- When subscribing to a database, you need to set `value.deserializer` to `com.taosdata.jdbc.tmq.MapEnhanceDeserializer` when creating a consumer, and then create a consumer of type `TaosConsumer<TMQEnhMap>`. This way, each row of data can be deserialized into a table name and a `Map`.

</TabItem>

<TabItem label="Python" value="python">

```python
{{#include docs/examples/python/tmq_websocket_example.py:subscribe}}
```

- The parameters of the `subscribe` method mean: the list of topics subscribed to (i.e., names), supporting subscription to multiple topics simultaneously.
- `poll` is called each time to fetch a message, which may contain multiple records.
- `records` contains multiple block segments, each of which may contain multiple records.
</TabItem>

<TabItem label="Go" value="go">
```go
{{#include docs/examples/go/tmq/ws/main.go:subscribe}}
```
</TabItem>

<TabItem label="Rust" value="rust">

```rust
{{#include docs/examples/rust/restexample/examples/tmq.rs:consume}}
```

- Consumers can subscribe to one or more `TOPIC`, generally it is recommended that a consumer subscribes to only one `TOPIC`.
- TMQ message queue is a [futures::Stream](https://docs.rs/futures/latest/futures/stream/index.html) type, which can be used with the corresponding API to consume each message and mark it as consumed through `.commit`.
- `Record` is a custom structure, whose field names and data types correspond one-to-one with the column names and data types, allowing objects of type `Record` to be deserialized using `serde`.

</TabItem>

<TabItem label="Node.js" value="node">

```js
    {{#include docs/examples/node/websocketexample/tmq_seek_example.js:subscribe}}
```

</TabItem>

<TabItem label="C#" value="csharp">
```csharp
{{#include docs/examples/csharp/wssubscribe/Program.cs:subscribe}}
```
</TabItem>
<TabItem label="C" value="c">
```c
{{#include docs/examples/c-ws-new/tmq_demo.c:build_topic_list}}
```

```c
{{#include docs/examples/c-ws-new/tmq_demo.c:basic_consume_loop}}
```

```c
{{#include docs/examples/c-ws-new/tmq_demo.c:msg_process}}
```

```c
{{#include docs/examples/c-ws-new/tmq_demo.c:subscribe_3}}
```

Steps for subscribing and consuming data:

  1. Call the `build_topic_list` function to create a topic list `topic_list`.
  2. If `topic_list` is `NULL`, it means creation failed, and the function returns `-1`.
  3. Use the `tmq_subscribe` function to subscribe to the topic list specified by `tmq`. If the subscription fails, print an error message.
  4. Destroy the topic list `topic_list` to free resources.
  5. Call the `basic_consume_loop` function to start the basic consumption loop, processing the subscribed messages.

</TabItem>
<TabItem label="REST API" value="rest">
Not supported
</TabItem>
</Tabs>

### Native Connection

<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">

```java
{{#include docs/examples/java/src/main/java/com/taos/example/WsConsumerLoopFull.java:poll_data_code_piece}}
```

- The parameters of the `subscribe` method mean: the list of topics (i.e., names) to subscribe to, supporting subscription to multiple topics simultaneously.
- `poll` is called each time to get a message, which may contain multiple records.
- `ResultBean` is a custom internal class, whose field names and data types correspond one-to-one with the column names and data types, allowing objects of type `ResultBean` to be deserialized based on the `value.deserializer` property's corresponding deserialization class.

</TabItem>

<TabItem label="Python" value="python">

```python
{{#include docs/examples/python/tmq_native.py:subscribe}}
```

- The parameters of the `subscribe` method mean: the list of topics (i.e., names) to subscribe to, supporting subscription to multiple topics simultaneously.
- `poll` is called each time to get a message, which may contain multiple records.
- `records` contains multiple blocks, each of which may contain multiple records.
</TabItem>

<TabItem label="Go" value="go">
```go
{{#include docs/examples/go/tmq/native/main.go:subscribe}}
```
</TabItem>

<TabItem label="Rust" value="rust">

```rust
{{#include docs/examples/rust/restexample/examples/tmq.rs:consume}}
```

- Consumers can subscribe to one or more `TOPIC`, generally it is recommended that a consumer subscribes to only one `TOPIC`.
- The TMQ message queue is a [futures::Stream](https://docs.rs/futures/latest/futures/stream/index.html) type, which can be used with the corresponding API to consume each message and mark it as consumed with `.commit`.
- `Record` is a custom structure, whose field names and data types correspond one-to-one with the column names and data types, allowing objects of type `Record` to be deserialized through `serde`.

</TabItem>
<TabItem label="Node.js" value="node">
Not supported
</TabItem>
<TabItem label="C#" value="csharp">
```csharp
{{#include docs/examples/csharp/subscribe/Program.cs:subscribe}}
```
</TabItem>

<TabItem label="C" value="c">
```c
{{#include docs/examples/c/tmq_demo.c:build_topic_list}}
```

```c
{{#include docs/examples/c/tmq_demo.c:basic_consume_loop}}
```

```c
{{#include docs/examples/c/tmq_demo.c:msg_process}}
```

```c
{{#include docs/examples/c/tmq_demo.c:subscribe_3}}
```

Subscription and consumption data steps:

  1. Call the `build_topic_list` function to create a topic list `topic_list`.
  1. If `topic_list` is `NULL`, it means creation failed, and the function returns `-1`.
  1. Use the `tmq_subscribe` function to subscribe to the topic list specified by `tmq`. If the subscription fails, print an error message.
  1. Destroy the topic list `topic_list` to free resources.
  1. Call the `basic_consume_loop` function to start the basic consumption loop, processing the subscribed messages.

</TabItem>
<TabItem label="REST API" value="rest">
Not supported
</TabItem>
</Tabs>

## Specifying the Subscription Offset

Consumers can specify to start reading messages from a specific Offset in the partition, allowing them to reread messages or skip processed messages. Below is how connectors in various languages specify the subscription Offset.  

### WebSocket Connection

<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">

```java
{{#include docs/examples/java/src/main/java/com/taos/example/WsConsumerLoopFull.java:consumer_seek}}
```

1. Use the consumer.poll method to poll data until data is obtained.
1. For the first batch of polled data, print the content of the first message and obtain the current consumer's partition assignment information.
1. Use the consumer.seekToBeginning method to reset the offset of all partitions to the starting position and print the successful reset message.
1. Poll data again using the consumer.poll method and print the content of the first message.

</TabItem>

<TabItem label="Python" value="python">

```python
{{#include docs/examples/python/tmq_websocket_example.py:assignment}}
```

</TabItem>

<TabItem label="Go" value="go">
```go
{{#include docs/examples/go/tmq/ws/main.go:seek}}
```
</TabItem>

<TabItem label="Rust" value="rust">

```rust
{{#include docs/examples/rust/nativeexample/examples/tmq.rs:seek_offset}}
```

1. By calling the consumer.assignments() method, obtain the consumer's current partition assignment information and record the initial assignment status.  
1. Traverse each partition assignment information, for each partition: extract the topic, consumer group ID (vgroup_id), current offset (current), starting offset (begin), and ending offset (end).
Record this information.  
1. Call the consumer.offset_seek method to set the offset to the starting position. If the operation fails, record the error information and current assignment status.  
1. After adjusting the offset for all partitions, obtain and record the consumer's partition assignment information again to confirm the status after the offset adjustment.

</TabItem>

<TabItem label="Node.js" value="node">

```js
    {{#include docs/examples/node/websocketexample/tmq_seek_example.js:offset}}
```

</TabItem>

<TabItem label="C#" value="csharp">
```csharp
{{#include docs/examples/csharp/wssubscribe/Program.cs:seek}}
```
</TabItem>
<TabItem label="C" value="c">
```c
{{#include docs/examples/c-ws-new/tmq_demo.c:consume_repeatly}}
```

1. Use the `tmq_get_topic_assignment` function to obtain the assignment information for a specific topic, including the number of assignments and the details of each assignment.
2. If fetching the assignment information fails, print an error message and return.
3. For each assignment, use the `tmq_offset_seek` function to set the consumer's offset to the earliest offset.
4. If setting the offset fails, print an error message.
5. Release the assignment information array to free resources.
6. Call the `basic_consume_loop` function to start a new consumption loop and process messages.

</TabItem>
<TabItem label="REST API" value="rest">
Not supported
</TabItem>
</Tabs>

### Native Connection

<Tabs defaultValue="java" groupId="lang">

<TabItem value="java" label="Java">

```java
{{#include docs/examples/java/src/main/java/com/taos/example/WsConsumerLoopFull.java:consumer_seek}}
```

1. Use the consumer.poll method to poll data until data is obtained.
1. For the first batch of polled data, print the content of the first data item and obtain the current consumer's partition assignment information.
1. Use the consumer.seekToBeginning method to reset the offset of all partitions to the beginning position and print a message of successful reset.
1. Poll data again using the consumer.poll method and print the content of the first data item.

</TabItem>

<TabItem label="Python" value="python">

```python
{{#include docs/examples/python/tmq_native.py:assignment}}
```

</TabItem>

<TabItem label="Go" value="go">
```go
{{#include docs/examples/go/tmq/native/main.go:seek}}
```
</TabItem>

<TabItem label="Rust" value="rust">

```rust
{{#include docs/examples/rust/nativeexample/examples/tmq.rs:seek_offset}}
```

1. Obtain the consumer's current partition assignment information by calling the consumer.assignments() method and record the initial assignment status.
1. For each partition assignment, extract the topic, consumer group ID (vgroup_id), current offset, beginning offset, and ending offset. Record this information.
1. Use the consumer.offset_seek method to set the offset to the beginning position. If the operation fails, record the error information and the current assignment status.
1. After adjusting the offset for all partitions, obtain and record the consumer's partition assignment information again to confirm the status after the offset adjustment.

</TabItem>
<TabItem label="Node.js" value="node">
Not supported
</TabItem>
<TabItem label="C#" value="csharp">
```csharp
{{#include docs/examples/csharp/subscribe/Program.cs:seek}}
```
</TabItem>

<TabItem label="C" value="c">
```c
{{#include docs/examples/c/tmq_demo.c:consume_repeatly}}
```

1. Use the `tmq_get_topic_assignment` function to obtain the assignment information for a specific topic, including the number of assignments and the details of each assignment.
1. If fetching the assignment information fails, print an error message and return.
1. For each assignment, use the `tmq_offset_seek` function to set the consumer's offset to the earliest offset.
1. If setting the offset fails, print an error message.
1. Release the assignment information array to free resources.
1. Call the `basic_consume_loop` function to start a new consumption loop and process messages.

</TabItem>
<TabItem label="REST API" value="rest">
Not supported
</TabItem>
</Tabs>

## Commit Offset

After a consumer has read and processed messages, it can commit the Offset, indicating that the consumer has successfully processed messages up to this Offset. Offset commits can be automatic (committed periodically based on configuration) or manual (controlled by the application when to commit).
When creating a consumer, if the property `enable.auto.commit` is set to false, the offset can be manually committed.

**Note**: Before manually submitting the consumption progress, ensure that the message has been processed correctly; otherwise, the incorrectly processed message will not be consumed again. Automatic submission may commit the consumption progress of the previous message during the current `poll`, so please ensure that the message processing is completed before the next `poll` or message retrieval.

### WebSocket Connection

<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">

```java
{{#include docs/examples/java/src/main/java/com/taos/example/WsConsumerLoopFull.java:commit_code_piece}}
```

</TabItem>

<TabItem label="Python" value="python">

```python
{{#include docs/examples/python/tmq_websocket_example.py:commit_offset}}
```

</TabItem>

<TabItem label="Go" value="go">
```go
{{#include docs/examples/go/tmq/ws/main.go:commit_offset}}
```
</TabItem>

<TabItem label="Rust" value="rust">
```rust
{{#include docs/examples/rust/restexample/examples/tmq.rs:consumer_commit_manually}}
```

You can manually submit the consumption progress using the `consumer.commit` method.
</TabItem>

<TabItem label="Node.js" value="node">

```js
    {{#include docs/examples/node/websocketexample/tmq_example.js:commit}}
```

</TabItem>

<TabItem label="C#" value="csharp">
```csharp
{{#include docs/examples/csharp/wssubscribe/Program.cs:commit_offset}}
```
</TabItem>
<TabItem label="C" value="c">
```c
{{#include docs/examples/c-ws-new/tmq_demo.c:manual_commit}}
```

You can manually submit the consumption progress using the `tmq_commit_sync` function.

</TabItem>
<TabItem label="REST API" value="rest">
Not supported
</TabItem>
</Tabs>

### Native Connection

<Tabs defaultValue="java" groupId="lang">

<TabItem value="java" label="Java">

```java
{{#include docs/examples/java/src/main/java/com/taos/example/WsConsumerLoopFull.java:commit_code_piece}}
```

</TabItem>

<TabItem label="Python" value="python">

```python
{{#include docs/examples/python/tmq_native.py:commit_offset}}
```

</TabItem>

<TabItem label="Go" value="go">
```go
{{#include docs/examples/go/tmq/native/main.go:commit_offset}}
```
</TabItem>

<TabItem label="Rust" value="rust">
```rust
{{#include docs/examples/rust/restexample/examples/tmq.rs:consumer_commit_manually}}
```

You can manually submit the consumption progress using the `consumer.commit` method.
</TabItem>
<TabItem label="Node.js" value="node">
Not supported
</TabItem>
<TabItem label="C#" value="csharp">

```csharp
{{#include docs/examples/csharp/subscribe/Program.cs:commit_offset}}
```

</TabItem>

<TabItem label="C" value="c">
```c
{{#include docs/examples/c/tmq_demo.c:manual_commit}}
```

You can manually commit the consumption progress using the `tmq_commit_sync` function.

</TabItem>
<TabItem label="REST API" value="rest">
Not supported
</TabItem>
</Tabs>

## Unsubscribe and Close Consumption

Consumers can unsubscribe from topics and stop receiving messages. When a consumer is no longer needed, the consumer instance should be closed to release resources and disconnect from the TDengine server.  

### WebSocket Connection

<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">

```java
{{#include docs/examples/java/src/main/java/com/taos/example/WsConsumerLoopFull.java:unsubscribe_data_code_piece}}
```

</TabItem>

<TabItem label="Python" value="python">

```python
{{#include docs/examples/python/tmq_websocket_example.py:unsubscribe}}
```

</TabItem>

<TabItem label="Go" value="go">
```go
{{#include docs/examples/go/tmq/ws/main.go:close}}
```
</TabItem>

<TabItem label="Rust" value="rust">
```rust
{{#include docs/examples/rust/restexample/examples/tmq.rs:unsubscribe}}
```

**Note**: Once the consumer unsubscribes and is closed, it cannot be reused. If you want to subscribe to a new `topic`, please recreate the consumer.
</TabItem>

<TabItem label="Node.js" value="node">

```js
    {{#include docs/examples/node/websocketexample/tmq_example.js:unsubscribe}}
```

</TabItem>

<TabItem label="C#" value="csharp">
```csharp
{{#include docs/examples/csharp/wssubscribe/Program.cs:close}}
```
</TabItem>
<TabItem label="C" value="c">
```c
{{#include docs/examples/c-ws-new/tmq_demo.c:unsubscribe_and_close}}
```
</TabItem>
<TabItem label="REST API" value="rest">
Not supported
</TabItem>
</Tabs>

### Native Connection

<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">

```java
{{#include docs/examples/java/src/main/java/com/taos/example/WsConsumerLoopFull.java:unsubscribe_data_code_piece}}
```

</TabItem>

<TabItem label="Python" value="python">

```python
{{#include docs/examples/python/tmq_native.py:unsubscribe}}
```

</TabItem>

<TabItem label="Go" value="go">
```go
{{#include docs/examples/go/tmq/native/main.go:close}}
```
</TabItem>

<TabItem label="Rust" value="rust">
```rust
{{#include docs/examples/rust/restexample/examples/tmq.rs:unsubscribe}}
```

**Note**: After the consumer unsubscribes, it is closed and cannot be reused. If you want to subscribe to a new `topic`, please create a new consumer.
</TabItem>
<TabItem label="Node.js" value="node">
Not supported
</TabItem>
<TabItem label="C#" value="csharp">

```csharp
{{#include docs/examples/csharp/subscribe/Program.cs:close}}
```

</TabItem>

<TabItem label="C" value="c">
```c
{{#include docs/examples/c/tmq_demo.c:unsubscribe_and_close}}
```
</TabItem>
<TabItem label="REST API" value="rest">
Not supported
</TabItem>
</Tabs>

## Complete Examples

### WebSocket Connection

<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">
<details>
<summary>Complete code example</summary>
```java
{{#include docs/examples/java/src/main/java/com/taos/example/WsConsumerLoopFull.java:consumer_demo}}
```

**Note**: The value of the `value.deserializer` configuration parameter should be adjusted according to the package path of the test environment.  
</details>

</TabItem>
<TabItem label="Python" value="python">
<details>
<summary>Complete code example</summary>
```python
{{#include docs/examples/python/tmq_websocket_example.py}}
```
</details>
</TabItem>

<TabItem label="Go" value="go">
<details>
<summary>Complete code example</summary>
```go
{{#include docs/examples/go/tmq/ws/main.go}}
```
</details>
</TabItem>

<TabItem label="Rust" value="rust">
<details>
<summary>Complete code example</summary>
```rust
{{#include docs/examples/rust/restexample/examples/tmq.rs}}
```
</details>
</TabItem>

<TabItem label="Node.js" value="node">
<details>
<summary>Complete code example</summary>
```js
    {{#include docs/examples/node/websocketexample/tmq_example.js}}
```
</details>
</TabItem>

<TabItem label="C#" value="csharp">
<details>
<summary>Complete code example</summary>
```csharp
{{#include docs/examples/csharp/wssubscribe/Program.cs}}
```
</details>
</TabItem>
<TabItem label="C" value="c">
<details>
<summary>Complete code example</summary>
```c
{{#include docs/examples/c-ws-new/tmq_demo.c}}
```
</details>

</TabItem>
<TabItem label="REST API" value="rest">
Not supported
</TabItem>
</Tabs>

### Native Connection

<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">
<details>
<summary>Complete code example</summary>
```java
{{#include docs/examples/java/src/main/java/com/taos/example/ConsumerLoopFull.java:consumer_demo}}
```

**Note**: The value of the `value.deserializer` configuration parameter should be adjusted according to the package path in the test environment.  
</details>

</TabItem>

<TabItem label="Python" value="python">
<details>
<summary>Complete code example</summary>
```python
{{#include docs/examples/python/tmq_native.py}}
```
</details>
</TabItem>

<TabItem label="Go" value="go">
<details>
<summary>Complete code example</summary>
```go
{{#include docs/examples/go/tmq/native/main.go}}
```
</details>
</TabItem>

<TabItem label="Rust" value="rust">
<details>
<summary>Complete code example</summary>
```rust
{{#include docs/examples/rust/nativeexample/examples/tmq.rs}}
```
</details>
</TabItem>
<TabItem label="Node.js" value="node">
Not supported
</TabItem>
<TabItem label="C#" value="csharp">
<details>
<summary>Complete code example</summary>
```csharp
{{#include docs/examples/csharp/subscribe/Program.cs}}
```
</details>
</TabItem>

<TabItem label="C" value="c">
<details>
<summary>Complete code example</summary>
```c
{{#include docs/examples/c/tmq_demo.c}}
```
</details>
</TabItem>
<TabItem label="REST API" value="rest">
Not supported
</TabItem>
</Tabs>
