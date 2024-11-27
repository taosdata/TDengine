---
title: Manage Consumers
slug: /developer-guide/manage-consumers
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

TDengine provides data subscription and consumption interfaces similar to those of message queue products. In many scenarios, using TDengine's time-series big data platform eliminates the need to integrate message queue products, thereby simplifying application design and reducing operational costs. This chapter introduces the relevant API for data subscription for various language connectors and how to use them. For basic knowledge of data subscription, please refer to [Data Subscription](../../advanced-features/data-subscription/).  

## Creating a Topic

You can use taos shell or refer to the [Execute SQL](../running-sql-statements/) chapter to execute the SQL for creating a topic: `CREATE TOPIC IF NOT EXISTS topic_meters AS SELECT ts, current, voltage, phase, groupid, location FROM meters`  

The above SQL will create a subscription named topic_meters. Each record in the messages retrieved using this subscription consists of the columns selected by this query statement `SELECT ts, current, voltage, phase, groupid, location FROM meters`.

:::note

In the TDengine connector implementation, there are the following limitations for subscription queries:

- **Query Limitations**: Subscription queries can only use select statements and do not support other types of SQL, such as insert, update, or delete.
- **Raw Data Query**: Subscription queries can only query raw data and cannot query aggregated or computed results.
- **Time Order Limit**: Subscription queries can only query data in chronological order.

:::

## Creating a Consumer

The concept of a TDengine consumer is similar to that of Kafka. Consumers receive data streams by subscribing to topics. Consumers can configure various parameters, such as connection methods, server addresses, and automatic offset commits, to suit different data processing needs. Some language connectors' consumers also support advanced features such as automatic reconnection and data transmission compression to ensure efficient and stable data reception.

### Creating Parameters

The parameters for creating consumers are numerous and flexible, supporting various connection types, offset commit methods, compression, reconnection, deserialization, and other features. The common basic configuration items applicable to all language connectors are shown in the following table:

|         Parameter Name         |  Type   | Parameter Description                                                                                                                                          | Remarks                                                                                                                                                                |
| :----------------------------: | :-----: | -------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
|        `td.connect.ip`        | string  | Server's IP address                                                                                                                                          |                                                                                                                                                                     |
|       `td.connect.user`       | string  | Username                                                                                                                                                       |                                                                                                                                                                     |
|       `td.connect.pass`       | string  | Password                                                                                                                                                       |                                                                                                                                                                     |
|       `td.connect.port`       | integer | Server's port number                                                                                                                                         |                                                                                                                                                                     |
|         `group.id`            | string  | Consumer group ID, shared consumption progress among the same group                                                                                         | <br />**Required**. Maximum length: 192.<br />A maximum of 100 consumer groups can be established for each topic.                                                                                     |
|         `client.id`           | string  | Client ID                                                                                                                                                    | Maximum length: 192                                                                                                                                                   |
|     `auto.offset.reset`       |  enum   | Initial position for the consumer group subscription                                                                                                          | <br />`earliest`: default (version < 3.2.0.0); subscribe from the beginning;<br/>`latest`: default (version >= 3.2.0.0); only start from the latest data;<br/>`none`: cannot subscribe without a committed offset. |
|    `enable.auto.commit`       | boolean | Whether to enable automatic offset submission, true: automatically submit, the client application does not need to commit; false: the client application needs to commit itself | Default value is true                                                                                                                                                 |
|  `auto.commit.interval.ms`    | integer | Interval for automatically committing consumed offsets, in milliseconds                                                                                        | Default value is 5000                                                                                                                                                 |
|    `msg.with.table.name`      | boolean | Whether to allow parsing the table name from the message; not applicable for column subscriptions (for column subscriptions, tbname can be written as a column in the subquery) (this parameter is deprecated from version 3.2.0.0 and is always true) | Default is off                                                                                                                                                        |
|       `enable.replay`         | boolean | Whether to enable data replay functionality                                                                                                                  | Default is off                                                                                                                                                        |
|    `session.timeout.ms`       | integer | Timeout period after the consumer's heartbeat is lost; after timeout, the rebalance logic will be triggered, and if successful, the consumer will be deleted (supported from version 3.3.3.0) | Default value is 12000, value range [6000, 1800000]                                                                                                                 |
|    `max.poll.interval.ms`     | integer | Maximum time interval for the consumer to poll and fetch data; exceeding this time will be considered as the consumer being offline, triggering rebalance logic, and if successful, the consumer will be deleted (supported from version 3.3.3.0) | Default value is 300000, [1000, INT32_MAX]                                                                                                                           |

Here are the creation parameters for various language connectors:

<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">

The parameters for creating a consumer in the Java connector are Properties. For the list of configurable parameters, please refer to [Consumer](../../tdengine-reference/client-libraries/java/#consumer). Other parameters can be referenced in the common basic configuration items above.

</TabItem>

<TabItem label="Python" value="python">

Provides the `td.connect.websocket.scheme` parameter to indicate the protocol type, with other parameters being the same as the common basic configuration items.

</TabItem>

<TabItem label="Go" value="go">

Supported property list for creating a consumer:

- `ws.url`: WebSocket connection address.
- `ws.message.channelLen`: WebSocket message channel cache length, default 0.
- `ws.message.timeout`: WebSocket message timeout, default 5m.
- `ws.message.writeWait`: WebSocket message write timeout, default 10s.
- `ws.message.enableCompression`: Whether to enable WebSocket compression, default false.
- `ws.autoReconnect`: Whether to automatically reconnect WebSocket, default false.
- `ws.reconnectIntervalMs`: WebSocket reconnection interval time in milliseconds, default 2000.
- `ws.reconnectRetryCount`: WebSocket reconnection retry count, default 3.

Other parameters are referenced in the table above.

</TabItem>

<TabItem label="Rust" value="rust">

The parameters for creating a consumer in the Rust connector are DSN. For the list of configurable parameters, please refer to [DSN](../../tdengine-reference/client-libraries/rust/#dsn). Other parameters can be referenced in the common basic configuration items above.

</TabItem>

<TabItem label="Node.js" value="node">

Provides the `WS_URL` parameter to indicate the server address to connect to, with other parameters being the same as the common basic configuration items.

</TabItem>

<TabItem label="C#" value="csharp">

Supported property list for creating a consumer:

- `useSSL`: Whether to use SSL for the connection, default false.
- `token`: Token to connect to TDengine cloud.
- `ws.message.enableCompression`: Whether to enable WebSocket compression, default false.
- `ws.autoReconnect`: Whether to automatically reconnect, default false.
- `ws.reconnect.retry.count`: Reconnection attempts, default 3.
- `ws.reconnect.interval.ms`: Reconnection interval in milliseconds, default 2000.

Other parameters are referenced in the table above.

</TabItem>

<TabItem label="C" value="c">

- **WebSocket Connection**: Since DSN is used, there is no need for the configurations `td.connect.ip`, `td.connect.port`, `td.connect.user`, and `td.connect.pass`, with the rest being the same as the common configuration items.  
- **Native Connection**: Same as the common basic configuration items.

</TabItem>

<TabItem label="REST API" value="rest">

Not supported

</TabItem>
</Tabs>

### Websocket Connection

This section introduces how to create a consumer using WebSocket connection in various language connectors. Specify the server address to connect, set automatic commits, start consuming from the latest messages, and specify `group.id` and `client.id` information. Some language connectors also support deserialization parameters.

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
{{#include docs/examples/c-ws/tmq_demo.c:create_consumer_1}}
```

```c
{{#include docs/examples/c-ws/tmq_demo.c:create_consumer_2}}
```

Call the `build_consumer` function to attempt to get a consumer instance `tmq`. If successful, print a success log; otherwise, print a failure log.

</TabItem>

<TabItem label="REST API" value="rest">

Not supported

</TabItem>
</Tabs>

### Native Connection

This section introduces how to create a consumer using native connection in various language connectors. Specify the server address to connect, set automatic commits, start consuming from the latest messages, and specify `group.id` and `client.id` information. Some language connectors also support deserialization parameters.

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

Call the `build_consumer` function to attempt to get a consumer instance `tmq`. If successful, print a success log; otherwise, print a failure log.

</TabItem>

<TabItem label="REST API" value="rest">

Not supported

</TabItem>
</Tabs>

## Subscribing to Consume Data

After the consumer subscribes to a topic, it can start receiving and processing messages from that topic. Here are example codes for subscribing to consume data:

### Websocket Connection

<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">

```java
{{#include docs/examples/java/src/main/java/com/taos/example/WsConsumerLoopFull.java:poll_data_code_piece}}
```

- The parameter of the `subscribe` method represents the list of topics to subscribe to (i.e., names), supporting multiple topics simultaneously.
- The `poll` method fetches a message each time it is called, and a single message may contain multiple records.
- `ResultBean` is a custom internal class whose field names and data types correspond to the names and data types of the columns, allowing deserialization into objects of type `ResultBean` based on the deserialization class specified by the `value.deserializer` property.

</TabItem>

<TabItem label="Python" value="python">

```python
{{#include docs/examples/python/tmq_websocket_example.py:subscribe}}
```

- The parameter of the `subscribe` method represents the list of topics to subscribe to (i.e., names), supporting multiple topics simultaneously.
- The `poll` method fetches a message each time it is called, and a single message may contain multiple records.
- `records` contains multiple block chunks, with each chunk possibly containing multiple records.
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

- Consumers can subscribe to one or more `TOPIC`. It is generally recommended for a consumer to subscribe to only one `TOPIC`.  
- TMQ message queue is a type of [futures::Stream](https://docs.rs/futures/latest/futures/stream/index.html), which can use the corresponding API to consume each message and mark it as consumed via `.commit`.  
- `Record` is a custom structure whose field names and data types correspond to the names and data types of the columns, allowing deserialization into objects of type `Record` using `serde`.  

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
{{#include docs/examples/c-ws/tmq_demo.c:build_topic_list}}
```

```c
{{#include docs/examples/c-ws/tmq_demo.c:basic_consume_loop}}
```

```c
{{#include docs/examples/c-ws/tmq_demo.c:msg_process}}
```

```c
{{#include docs/examples/c-ws/tmq_demo.c:subscribe_3}}
```

Steps for subscribing to consume data:

1. Call the `ws_build_topic_list` function to create a list of topics `topic_list`.
2. If `topic_list` is `NULL`, it indicates creation failed; the function returns `-1`.
3. Use the `ws_tmq_subscribe` function to subscribe to the specified topic list for `tmq`. If subscription fails, print the error message.
4. Destroy the topic list `topic_list` to release resources.
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

- The parameter of the `subscribe` method represents the list of topics to subscribe to (i.e., names), supporting multiple topics simultaneously.
- The `poll` method fetches a message each time it is called, and a single message may contain multiple records.
- `ResultBean` is a custom internal class whose field names and data types correspond to the names and data types of the columns, allowing deserialization into objects of type `ResultBean` based on the deserialization class specified by the `value.deserializer` property.

</TabItem>

<TabItem label="Python" value="python">

```python
{{#include docs/examples/python/tmq_native.py:subscribe}}
```

- The parameter of the `subscribe` method represents the list of topics to subscribe to (i.e., names), supporting multiple topics simultaneously.
- The `poll` method fetches a message each time it is called, and a single message may contain multiple records.
- `records` contains multiple block chunks, with each chunk possibly containing multiple records.

</TabItem>

<TabItem label="Go" value="go">

```go
{{#include docs/examples/go/tmq/native/main.go:subscribe}}
```

</TabItem>

<TabItem label="Rust" value="rust">

```rust
{{#include docs/examples/rust/nativeexample/examples/tmq.rs:consume}}
```

- Consumers can subscribe to one or more `TOPIC`. It is generally recommended for a consumer to subscribe to only one `TOPIC`.  
- TMQ message queue is a type of [futures::Stream](https://docs.rs/futures/latest/futures/stream/index.html), which can use the corresponding API to consume each message and mark it as consumed via `.commit`.  
- `Record` is a custom structure whose field names and data types correspond to the names and data types of the columns, allowing deserialization into objects of type `Record` using `serde`.  

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

Steps for subscribing to consume data:

1. Call the `build_topic_list` function to create a list of topics `topic_list`.
2. If `topic_list` is `NULL`, it indicates creation failed; the function returns `-1`.
3. Use the `tmq_subscribe` function to subscribe to the specified topic list for `tmq`. If subscription fails, print the error message.
4. Destroy the topic list `topic_list` to release resources.
5. Call the `basic_consume_loop` function to start the basic consumption loop, processing the subscribed messages.

</TabItem>

<TabItem label="REST API" value="rest">

Not supported

</TabItem>
</Tabs>

## Specifying the Subscription Offset

Consumers can specify the offset from which to start reading messages in a partition. This allows consumers to re-read messages or skip processed messages. The following shows how to specify the subscription offset in various language connectors.  

### Websocket Connection

<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">

```java
{{#include docs/examples/java/src/main/java/com/taos/example/WsConsumerLoopFull.java:consumer_seek}}
```

1. Use the `consumer.poll` method to poll data until data is retrieved.
2. For the first batch of data polled, print the content of the first message and retrieve the current consumer's partition assignment information.
3. Use the `consumer.seekToBeginning` method to reset the offsets of all partitions to the starting position and print a message indicating successful reset.
4. Call the `consumer.poll` method again to poll data and print the content of the first message.

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

1. Retrieve the current partition assignment information for the consumer by calling the `consumer.assignments()` method and record the initial assignment status.  
2. Iterate through each partition assignment information, extracting the topic, consumer group ID (vgroup_id), current offset, starting offset, and ending offset for each partition. Record this information.  
3. Call the `consumer.offset_seek` method to set the offset to the starting position. If the operation fails, record the error message and current assignment status.  
4. After adjusting the offsets for all partitions, retrieve and record the consumer's partition assignment information again to confirm the status after the offset adjustments.  

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
{{#include docs/examples/c-ws/tmq_demo.c:consume_repeatly}}
```

1. Retrieve specific topic assignment information using the `ws_tmq_get_topic_assignment` function, including the number of assignments and specific assignment details.
2. If retrieving assignment information fails, print the error message and return.
3. For each assignment, use the `ws_tmq_offset_seek` function to set the consumer's offset to the earliest offset.
4. If setting the offset fails, print the error message.
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

1. Use the `consumer.poll` method to poll data until data is retrieved.
2. For the first batch of data polled, print the content of the first message and retrieve the current consumer's partition assignment information.
3. Use the `consumer.seekToBeginning` method to reset the offsets of all partitions to the starting position and print a message indicating successful reset.
4. Call the `consumer.poll` method again to poll data and print the content of the first message.

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

1. Retrieve the current partition assignment information for the consumer by calling the `consumer.assignments()` method and record the initial assignment status.  
2. Iterate through each partition assignment information, extracting the topic, consumer group ID (vgroup_id), current offset, starting offset, and ending offset for each partition. Record this information.  
3. Call the `consumer.offset_seek` method to set the offset to the starting position. If the operation fails, record the error message and current assignment status.  
4. After adjusting the offsets for all partitions, retrieve and record the consumer's partition assignment information again to confirm the status after the offset adjustments.  

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

1. Retrieve specific topic assignment information using the `tmq_get_topic_assignment` function, including the number of assignments and specific assignment details.
2. If retrieving assignment information fails, print the error message and return.
3. For each assignment, use the `tmq_offset_seek` function to set the consumer's offset to the earliest offset.
4. If setting the offset fails, print the error message.
5. Release the assignment information array to free resources.
6. Call the `basic_consume_loop` function to start a new consumption loop and process messages.

</TabItem>

<TabItem label="REST API" value="rest">

Not supported

</TabItem>
</Tabs>

## Committing Offset

After the consumer reads and processes messages, it can commit the offset, indicating that the consumer has successfully processed messages up to this offset. Offset commits can be automatic (periodically submitted according to configuration) or manual (controlled by the application).

When creating a consumer, if the `enable.auto.commit` attribute is set to false, the offset can be committed manually.  

:::note

Before manually committing the consumption progress, ensure that the message has been processed successfully; otherwise, incorrectly processed messages will not be consumed again. Automatic commits may submit the consumption progress of the previous message during the current `poll`, so ensure that messages are processed before performing the next `poll` or message retrieval.

:::

### Websocket Connection

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

You can manually commit the consumption progress using the `consumer.commit` method.

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
{{#include docs/examples/c-ws/tmq_demo.c:manual_commit}}
```

You can manually commit the consumption progress using the `ws_tmq_commit_sync` function.

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

You can manually commit the consumption progress using the `consumer.commit` method.

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

## Unsubscribing and Closing Consumption

Consumers can unsubscribe from topics to stop receiving messages. When a consumer is no longer needed, it should close the consumer instance to free resources and disconnect from the TDengine server.  

### Websocket Connection

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

:::note

Once the consumer has unsubscribed and closed, it cannot be reused. If you want to subscribe to a new topic, please recreate the consumer.

:::

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
{{#include docs/examples/c-ws/tmq_demo.c:unsubscribe_and_close}}
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

:::note

Once the consumer has unsubscribed and closed, it cannot be reused. If you want to subscribe to a new topic, please recreate the consumer.

:::

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

## Complete Example

### Websocket Connection

<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">
<details>
<summary>Complete Code Example</summary>

```java
{{#include docs/examples/java/src/main/java/com/taos/example/WsConsumerLoopFull.java:consumer_demo}}
```

:::note

The value of the `value.deserializer` configuration parameter should be adjusted according to the package path of the testing environment.  

:::

</details>
</TabItem>

<TabItem label="Python" value="python">
<details>
<summary>Complete Code Example</summary>

```python
{{#include docs/examples/python/tmq_websocket_example.py}}
```

</details>
</TabItem>

<TabItem label="Go" value="go">
<details>
<summary>Complete Code Example</summary>

```go
{{#include docs/examples/go/tmq/ws/main.go}}
```

</details>
</TabItem>

<TabItem label="Rust" value="rust">
<details>
<summary>Complete Code Example</summary>

```rust
{{#include docs/examples/rust/restexample/examples/tmq.rs}}
```

</details>
</TabItem>

<TabItem label="Node.js" value="node">
<details>
<summary>Complete Code Example</summary>

```js
{{#include docs/examples/node/websocketexample/tmq_example.js}}
```

</details>
</TabItem>

<TabItem label="C#" value="csharp">
<details>
<summary>Complete Code Example</summary>

```csharp
{{#include docs/examples/csharp/wssubscribe/Program.cs}}
```

</details>
</TabItem>

<TabItem label="C" value="c">
<details>
<summary>Complete Code Example</summary>

```c
{{#include docs/examples/c-ws/tmq_demo.c}}
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
<summary>Complete Code Example</summary>

```java
{{#include docs/examples/java/src/main/java/com/taos/example/ConsumerLoopFull.java:consumer_demo}}
```

:::note

The value of the `value.deserializer` configuration parameter should be adjusted according to the package path of the testing environment.  

:::

</details>
</TabItem>

<TabItem label="Python" value="python">
<details>
<summary>Complete Code Example</summary>

```python
{{#include docs/examples/python/tmq_native.py}}
```

</details>
</TabItem>

<TabItem label="Go" value="go">
<details>
<summary>Complete Code Example</summary>

```go
{{#include docs/examples/go/tmq/native/main.go}}
```

</details>
</TabItem>

<TabItem label="Rust" value="rust">
<details>
<summary>Complete Code Example</summary>

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
<summary>Complete Code Example</summary>

```csharp
{{#include docs/examples/csharp/subscribe/Program.cs}}
```

</details>
</TabItem>

<TabItem label="C" value="c">
<details>
<summary>Complete Code Example</summary>

```c
{{#include docs/examples/c/tmq_demo.c}}
```

</details>
</TabItem>

<TabItem label="REST API" value="rest">

Not supported

</TabItem>
</Tabs>
