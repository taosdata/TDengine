---
sidebar_label: Subscription
title: Data Subscritpion
description: Use data subscription to get data from TDengine.
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import Java from "./_sub_java.mdx";
import Python from "./_sub_python.mdx";
import Go from "./_sub_go.mdx";
import Node from "./_sub_node.mdx";
import CSharp from "./_sub_cs.mdx";
import CDemo from "./_sub_c.mdx";

This topic introduces how to read out data from TDengine using data subscription, which is an advanced feature in TDengine. To access the data in TDengine in data subscription way, you need to create topic, create consumer, subscribe to a topic, and consume data. In this document we will briefly explain these main steps of data subscription.

## Create Topic

A topic can be created on a database, on some selected columns,or on a supertable. 

### Topic on Columns

The most common way to create a topic is to create a topic on some specifically selected columns. The Syntax is like below:

```sql
CREATE TOPIC topic_name as subquery;
```

You can subscribe to a topic through a SELECT statement. Statements that specify columns, such as `SELECT *` and `SELECT ts, cl` are supported, as are filtering conditions and scalar functions. Aggregate functions and time window aggregation are not supported. Note:

- The schema of topics created in this manner is determined by the subscribed data.
- You cannot modify (`ALTER <table> MODIFY`) or delete (`ALTER <table> DROP`) columns or tags that are used in a subscription or calculation.
- Columns added to a table after the subscription is created are not displayed in the results. Deleting columns will cause an error.

For example:

```sql
CREATE TOPIC topic_name AS SELECT ts, c1, c2, c3 FROM tmqdb.stb WHERE c1 > 1;
```

### Topic on SuperTable

Syntax:

```sql
CREATE TOPIC topic_name AS STABLE stb_name;
```

Creating a topic in this manner differs from a `SELECT * from stbName` statement as follows:

- The table schema can be modified.
- Unstructured data is returned. The format of the data returned changes based on the supertable schema.
- A different table schema may exist for every data block to be processed.
- The data returned does not include tags.

### Topic on Database

Syntax:

```sql
CREATE TOPIC topic_name [WITH META] AS DATABASE db_name;
```

This SQL statement creates a subscription to all tables in the database. You can add the `WITH META` parameter to include schema changes in the subscription, including creating and deleting supertables; adding, deleting, and modifying columns; and creating, deleting, and modifying the tags of subtables. Consumers can determine the message type from the API. Note that this differs from Kafka.

## Create Consumer

To create a consumer, you must use the APIs provided by TDengine connectors. Below is the sample code of using connectors of different languages.


You configure the following parameters when creating a consumer:

|            Parameter            |  Type   | Description                                                 | Remarks                                        |
| :----------------------------: | :-----: | -------------------------------------------------------- | ------------------------------------------- |
|        `td.connect.ip`         | string  | Used in establishing a connection; same as `taos_connect`                          |                                             |
|        `td.connect.user`         | string  | Used in establishing a connection; same as `taos_connect`                          |                                             |
|        `td.connect.pass`         | string  | Used in establishing a connection; same as `taos_connect`                          |                                             |
|        `td.connect.port`         | string  | Used in establishing a connection; same as `taos_connect`                          |                                             |
|           `group.id`           | string  | Consumer group ID; consumers with the same ID are in the same group                        | **Required**. Maximum length: 192.                 |
|          `client.id`           | string  | Client ID                                                | Maximum length: 192.                             |
|      `auto.offset.reset`       |  enum   | Initial offset for the consumer group                                     | Specify `earliest`, `latest`, or `none`(default) |
|      `enable.auto.commit`      | boolean | Commit automatically                                             | Specify `true` or `false`.                   |
|   `auto.commit.interval.ms`    | integer | Interval for automatic commits, in milliseconds                           |
| `enable.heartbeat.background`  | boolean | Backend heartbeat; if enabled, the consumer does not go offline even if it has not polled for a long time |                                             |
| `experimental.snapshot.enable` | boolean | Specify whether to consume messages from the WAL or from TSBS                    |                                             |
|     `msg.with.table.name`      | boolean | Specify whether to deserialize table names from messages                                 |

The method of specifying these parameters depends on the language used:

<Tabs defaultValue="java" groupId="lang">
<TabItem value="c" label="C">

Will be available soon
<!-- temporarily comment off
```c
/* Create consumer groups on demand (group.id) and enable automatic commits (enable.auto.commit),
   an automatic commit interval (auto.commit.interval.ms), and a username (td.connect.user) and password (td.connect.pass) */
tmq_conf_t* conf = tmq_conf_new();
tmq_conf_set(conf, "enable.auto.commit", "true");
tmq_conf_set(conf, "auto.commit.interval.ms", "1000");
tmq_conf_set(conf, "group.id", "cgrpName");
tmq_conf_set(conf, "td.connect.user", "root");
tmq_conf_set(conf, "td.connect.pass", "taosdata");
tmq_conf_set(conf, "auto.offset.reset", "earliest");
tmq_conf_set(conf, "experimental.snapshot.enable", "true");
tmq_conf_set(conf, "msg.with.table.name", "true");
tmq_conf_set_auto_commit_cb(conf, tmq_commit_cb_print, NULL);

tmq_t* tmq = tmq_consumer_new(conf, NULL, 0);
tmq_conf_destroy(conf);
```
-->

</TabItem>
<TabItem value="java" label="Java">

Will be available soon
<!-- temporarily comment off
Java programs use the following parameters:

|            Parameter            |  Type   | Description                                                 | Remarks                                        |
| ----------------------------- | ------ | ----------------------------------------------------------------------------------------------------------------------------- |
| `bootstrap.servers`           | string |Connection address, such as `localhost:6030`                                                                                                 |
| `value.deserializer`          | string | Value deserializer; to use this method, implement the `com.taosdata.jdbc.tmq.Deserializer` interface or inherit the `com.taosdata.jdbc.tmq.ReferenceDeserializer` type |
| `value.deserializer.encoding` | string | Specify the encoding for string deserialization                                                                                                        |  |

Note: The `bootstrap.servers` parameter is used instead of `td.connect.ip` and `td.connect.port` to provide an interface that is consistent with Kafka.

```java
Properties properties = new Properties();
properties.setProperty("enable.auto.commit", "true");
properties.setProperty("auto.commit.interval.ms", "1000");
properties.setProperty("group.id", "cgrpName");
properties.setProperty("bootstrap.servers", "127.0.0.1:6030");
properties.setProperty("td.connect.user", "root");
properties.setProperty("td.connect.pass", "taosdata");
properties.setProperty("auto.offset.reset", "earliest");
properties.setProperty("msg.with.table.name", "true");
properties.setProperty("value.deserializer", "com.taos.example.MetersDeserializer");

TaosConsumer<Meters> consumer = new TaosConsumer<>(properties);

/* value deserializer definition. */
import com.taosdata.jdbc.tmq.ReferenceDeserializer;

public class MetersDeserializer extends ReferenceDeserializer<Meters> {
}
```
-->

</TabItem>

<TabItem label="Go" value="Go">

Will be available soon
<!-- temporarily comment off
```go
config := tmq.NewConfig()
defer config.Destroy()
err = config.SetGroupID("test")
if err != nil {
  panic(err)
}
err = config.SetAutoOffsetReset("earliest")
if err != nil {
  panic(err)
}
err = config.SetConnectIP("127.0.0.1")
if err != nil {
  panic(err)
}
err = config.SetConnectUser("root")
if err != nil {
  panic(err)
}
err = config.SetConnectPass("taosdata")
if err != nil {
  panic(err)
}
err = config.SetConnectPort("6030")
if err != nil {
  panic(err)
}
err = config.SetMsgWithTableName(true)
if err != nil {
  panic(err)
}
err = config.EnableHeartBeat()
if err != nil {
  panic(err)
}
err = config.EnableAutoCommit(func(result *wrapper.TMQCommitCallbackResult) {
  if result.ErrCode != 0 {
    errStr := wrapper.TMQErr2Str(result.ErrCode)
    err := errors.NewError(int(result.ErrCode), errStr)
    panic(err)
  }
})
if err != nil {
  panic(err)
}
```
-->

</TabItem>

<TabItem label="Rust" value="Rust">

```rust
let mut dsn = std::env::var("TDENGINE_CLOUD_DSN").parse()?;
dsn.set("group.id", "group1");
dsn.set("client.id", "test");
dsn.set("auto.offset.reset", "earliest");

let tmq = TmqBuilder::from_dsn(dsn)?;

let mut consumer = tmq.build()?;
```

</TabItem>

<TabItem value="Python" label="Python">

Will be available soon
<!-- temporarily comment off
Python programs use the following parameters:

|            Parameter            |  Type   | Description                                                 | Remarks                                        |
| :----------------------------: | :----: | -------------------------------------------------------- | ------------------------------------------- |
|        `td_connect_ip`         | string  | Used in establishing a connection; same as `taos_connect`                          |                                             |
|        `td_connect_user`         | string  | Used in establishing a connection; same as `taos_connect`                          |                                             |
|        `td_connect_pass`         | string  | Used in establishing a connection; same as `taos_connect`                          |                                             |
|        `td_connect_port`         | string  | Used in establishing a connection; same as `taos_connect`                          |                                             |
|           `group_id`           | string  | Consumer group ID; consumers with the same ID are in the same group                        | **Required**. Maximum length: 192.                 |
|          `client_id`           | string  | Client ID                                                | Maximum length: 192.                             |
|      `auto_offset_reset`       |  string   | Initial offset for the consumer group                                     | Specify `earliest`, `latest`, or `none`(default) |
|      `enable_auto_commit`      | string | Commit automatically                                             | Specify `true` or `false`.                   |
|   `auto_commit_interval_ms`    | string | Interval for automatic commits, in milliseconds                           |
| `enable_heartbeat_background`  | string | Backend heartbeat; if enabled, the consumer does not go offline even if it has not polled for a long time |  Specify `true` or `false`.                                           |
| `experimental_snapshot_enable` | string | Specify whether to consume messages from the WAL or from TSBS                    | Specify `true` or `false`.                                            |
|     `msg_with_table_name`      | string | Specify whether to deserialize table names from messages                                 | Specify `true` or `false`.
|           `timeout`            |  int   | Consumer pull timeout                                     |                                             |

</TabItem>
-->

<TabItem label="Node.JS" value="Node.JS">

Will be available soon
<!-- temporarily comment off
```js
// Create consumer groups on demand (group.id) and enable automatic commits (enable.auto.commit),
// an automatic commit interval (auto.commit.interval.ms), and a username (td.connect.user) and password (td.connect.pass) 

let consumer = taos.consumer({
  'enable.auto.commit': 'true',
  'auto.commit.interval.ms','1000',
  'group.id': 'tg2',
  'td.connect.user': 'root',
  'td.connect.pass': 'taosdata',
  'auto.offset.reset','earliest',
  'msg.with.table.name': 'true',
  'td.connect.ip','127.0.0.1',
  'td.connect.port','6030'  
  });
```
-->

</TabItem>

<TabItem value="C#" label="C#">

Will be available soon
<!-- temporarily comment off
```csharp
using TDengineTMQ;

// Create consumer groups on demand (GourpID) and enable automatic commits (EnableAutoCommit),
// an automatic commit interval (AutoCommitIntervalMs), and a username (TDConnectUser) and password (TDConnectPasswd)
var cfg = new ConsumerConfig
 {
    EnableAutoCommit = "true"
    AutoCommitIntervalMs = "1000"
    GourpId = "TDengine-TMQ-C#",
    TDConnectUser = "root",
    TDConnectPasswd = "taosdata",
    AutoOffsetReset = "earliest"
    MsgWithTableName = "true",
    TDConnectIp = "127.0.0.1",
    TDConnectPort = "6030"
 };

var consumer = new ConsumerBuilder(cfg).Build();

```
-->

</TabItem>

</Tabs>

A consumer group is automatically created when multiple consumers are configured with the same consumer group ID.

## Subscribe to a Topic

A single consumer can subscribe to multiple topics.

<Tabs defaultValue="java" groupId="lang">
<TabItem value="c" label="C">

Will be available soon
<!-- temporarily comment off
```c
// Create a list of subscribed topics
tmq_list_t* topicList = tmq_list_new();
tmq_list_append(topicList, "topicName");
// Enable subscription
tmq_subscribe(tmq, topicList);
tmq_list_destroy(topicList);
  
```
-->

</TabItem>
<TabItem value="java" label="Java">

Will be available soon
<!-- temporarily comment off
```java
List<String> topics = new ArrayList<>();
topics.add("tmq_topic");
consumer.subscribe(topics);
```
-->

</TabItem>
<TabItem value="Go" label="Go">

Will be available soon
<!-- temporarily comment off
```go
consumer, err := tmq.NewConsumer(config)
if err != nil {
 panic(err)
}
err = consumer.Subscribe([]string{"example_tmq_topic"})
if err != nil {
 panic(err)
}
```
-->

</TabItem>
<TabItem value="Rust" label="Rust">

```rust
consumer.subscribe(["tmq_meters"]).await?;
```

</TabItem>

<TabItem value="Python" label="Python">

Will be available soon
<!-- temporarily comment off
```python
consumer = TaosConsumer('topic_ctb_column', group_id='vg2')
```
-->

</TabItem>

<TabItem label="Node.JS" value="Node.JS">

Will be available soon
<!-- temporarily comment off
```js
// Create a list of subscribed topics
let topics = ['topic_test']

// Enable subscription
consumer.subscribe(topics);
```
-->

</TabItem>

<TabItem value="C#" label="C#">

Will be available soon
<!-- temporarily comment off
```csharp
// Create a list of subscribed topics
List<String> topics = new List<string>();
topics.add("tmq_topic");
// Enable subscription
consumer.Subscribe(topics);
```
-->

</TabItem>

</Tabs>

## Consume messages

The following code demonstrates how to consume the messages in a queue.

<Tabs defaultValue="java" groupId="lang">
<TabItem value="c" label="C">

Will be available soon
<!-- temporarily comment off
```c
## Consume data
while (running) {
  TAOS_RES* msg = tmq_consumer_poll(tmq, timeOut);
  msg_process(msg);
}  
```

The `while` loop obtains a message each time it calls `tmq_consumer_poll()`. This message is exactly the same as the result returned by a query, and the same deserialization API can be used on it.
-->

</TabItem>
<TabItem value="java" label="Java">

Will be available soon
<!-- temporarily comment off
```java
while(running){
  ConsumerRecords<Meters> meters = consumer.poll(Duration.ofMillis(100));
    for (Meters meter : meters) {
      processMsg(meter);
    }    
}
```
-->

</TabItem>

<TabItem value="Go" label="Go">

Will be available soon
<!-- temporarily comment off
```go
for {
 result, err := consumer.Poll(time.Second)
 if err != nil {
  panic(err)
 }
 fmt.Println(result)
 consumer.Commit(context.Background(), result.Message)
 consumer.FreeMessage(result.Message)
}
```
-->

</TabItem>

<TabItem value="Rust" label="Rust">

```rust
{
    let mut stream = consumer.stream();

    while let Some((offset, message)) = stream.try_next().await? {
        // get information from offset

        // the topic
        let topic = offset.topic();
        // the vgroup id, like partition id in kafka.
        let vgroup_id = offset.vgroup_id();
        println!("* in vgroup id {vgroup_id} of topic {topic}\n");

        if let Some(data) = message.into_data() {
            while let Some(block) = data.fetch_raw_block().await? {
                // one block for one table, get table name if needed
                let name = block.table_name();
                let records: Vec<Record> = block.deserialize().try_collect()?;
                println!(
                    "** table: {}, got {} records: {:#?}\n",
                    name.unwrap(),
                    records.len(),
                    records
                );
            }
        }
        consumer.commit(offset).await?;
    }
}
```

</TabItem>
<TabItem value="Python" label="Python">
Will be available soon
<!-- temporarily comment off

```python
for msg in consumer:
    for row in msg:
        print(row)
```
-->

</TabItem>

<TabItem label="Node.JS" value="Node.JS">

Will be available soon
<!-- temporarily comment off
```js
while(true){
  msg = consumer.consume(200);
  // process message(consumeResult)
  console.log(msg.topicPartition);
  console.log(msg.block);
  console.log(msg.fields)
}
```
-->

</TabItem>

<TabItem value="C#" label="C#">

Will be available soon
<!-- temporarily comment off
```csharp
## Consume data
while (true)
{
    var consumerRes = consumer.Consume(100);
    // process ConsumeResult
    ProcessMsg(consumerRes);
    consumer.Commit(consumerRes);
}
```
-->

</TabItem>

</Tabs>

## Subscribe to a Topic

A single consumer can subscribe to multiple topics.

<Tabs defaultValue="java" groupId="lang">
<TabItem value="c" label="C">
Will be available soon
<!-- temporarily comment off

```c
// Create a list of subscribed topics
tmq_list_t* topicList = tmq_list_new();
tmq_list_append(topicList, "topicName");
// Enable subscription
tmq_subscribe(tmq, topicList);
tmq_list_destroy(topicList);
  
```
-->

</TabItem>
<TabItem value="java" label="Java">

Will be available soon
<!-- temporarily comment off
```java
List<String> topics = new ArrayList<>();
topics.add("tmq_topic");
consumer.subscribe(topics);
```
-->

</TabItem>
<TabItem value="Go" label="Go">

Will be available soon
<!-- temporarily comment off
```go
consumer, err := tmq.NewConsumer(config)
if err != nil {
 panic(err)
}
err = consumer.Subscribe([]string{"example_tmq_topic"})
if err != nil {
 panic(err)
}
```
-->

</TabItem>
<TabItem value="Rust" label="Rust">

```rust
consumer.subscribe(["tmq_meters"]).await?;
```

</TabItem>

<TabItem value="Python" label="Python">
Will be available soon
<!-- temporarily comment off

```python
consumer = TaosConsumer('topic_ctb_column', group_id='vg2')
```
-->

</TabItem>

Will be available soon
<!-- temporarily comment off
<TabItem label="Node.JS" value="Node.JS">

```js
// Create a list of subscribed topics
let topics = ['topic_test']

// Enable subscription
consumer.subscribe(topics);
```
-->

</TabItem>

<TabItem value="C#" label="C#">
Will be available soon
<!-- temporarily comment off

```csharp
// Create a list of subscribed topics
List<String> topics = new List<string>();
topics.add("tmq_topic");
// Enable subscription
consumer.Subscribe(topics);
```
-->

</TabItem>

</Tabs>


## Consume Data

The following code demonstrates how to consume the messages in a queue.

<Tabs defaultValue="java" groupId="lang">
<TabItem value="c" label="C">

Will be available soon
<!-- temporarily comment off
```c
## Consume data
while (running) {
  TAOS_RES* msg = tmq_consumer_poll(tmq, timeOut);
  msg_process(msg);
}  
```
-->

The `while` loop obtains a message each time it calls `tmq_consumer_poll()`. This message is exactly the same as the result returned by a query, and the same deserialization API can be used on it.

</TabItem>
<TabItem value="java" label="Java">

Will be available soon
<!-- temporarily comment off
```java
while(running){
  ConsumerRecords<Meters> meters = consumer.poll(Duration.ofMillis(100));
    for (Meters meter : meters) {
      processMsg(meter);
    }    
}
```
-->

</TabItem>

<TabItem value="Go" label="Go">

Will be available soon
<!-- temporarily comment off
```go
for {
 result, err := consumer.Poll(time.Second)
 if err != nil {
  panic(err)
 }
 fmt.Println(result)
 consumer.Commit(context.Background(), result.Message)
 consumer.FreeMessage(result.Message)
}
```
-->

</TabItem>

<TabItem value="Rust" label="Rust">

```rust
{
    let mut stream = consumer.stream();

    while let Some((offset, message)) = stream.try_next().await? {
        // get information from offset

        // the topic
        let topic = offset.topic();
        // the vgroup id, like partition id in kafka.
        let vgroup_id = offset.vgroup_id();
        println!("* in vgroup id {vgroup_id} of topic {topic}\n");

        if let Some(data) = message.into_data() {
            while let Some(block) = data.fetch_raw_block().await? {
                // one block for one table, get table name if needed
                let name = block.table_name();
                let records: Vec<Record> = block.deserialize().try_collect()?;
                println!(
                    "** table: {}, got {} records: {:#?}\n",
                    name.unwrap(),
                    records.len(),
                    records
                );
            }
        }
        consumer.commit(offset).await?;
    }
}
```

</TabItem>
<TabItem value="Python" label="Python">
Will be available soon
<!-- temporarily comment off

```python
for msg in consumer:
    for row in msg:
        print(row)
```
-->

</TabItem>

<TabItem label="Node.JS" value="Node.JS">

Will be available soon
<!-- temporarily comment off
```js
while(true){
  msg = consumer.consume(200);
  // process message(consumeResult)
  console.log(msg.topicPartition);
  console.log(msg.block);
  console.log(msg.fields)
}
```
-->

</TabItem>

<TabItem value="C#" label="C#">

Will be available soon
<!-- temporarily comment off
```csharp
## Consume data
while (true)
{
    var consumerRes = consumer.Consume(100);
    // process ConsumeResult
    ProcessMsg(consumerRes);
    consumer.Commit(consumerRes);
}
```

-->
</TabItem>

</Tabs>

## Close the consumer

After message consumption is finished, the consumer is unsubscribed.

<Tabs defaultValue="java" groupId="lang">
<TabItem value="c" label="C">

Will be available soon
<!-- temporarily comment off
```c
/* Unsubscribe */
tmq_unsubscribe(tmq);

/* Close consumer object */
tmq_consumer_close(tmq);
```

-->
</TabItem>
<TabItem value="java" label="Java">

Will be available soon
<!-- temporarily comment off
```java
/* Unsubscribe */
consumer.unsubscribe();

/* Close consumer */
consumer.close();
```
-->

</TabItem>

<TabItem value="Go" label="Go">

Will be available soon
<!-- temporarily comment off
```go
consumer.Close()
```
-->

</TabItem>

<TabItem value="Rust" label="Rust">

```rust
consumer.unsubscribe().await;
```

</TabItem>

<TabItem value="Python" label="Python">

Will be available soon
<!-- temporarily comment off
```py
# Unsubscribe
consumer.unsubscribe()
# Close consumer
consumer.close()
```
-->

</TabItem>
<TabItem label="Node.JS" value="Node.JS">

Will be available soon
<!-- temporarily comment off
```js
consumer.unsubscribe();
consumer.close();
```
-->

</TabItem>

<TabItem value="C#" label="C#">
Will be available soon
<!-- temporarily comment off

```csharp
// Unsubscribe
consumer.Unsubscribe();

// Close consumer
consumer.Close();
```
-->

</TabItem>

</Tabs>


## Close Consumer

After message consumption is finished, the consumer is unsubscribed.

<Tabs defaultValue="java" groupId="lang">
<TabItem value="c" label="C">
Will be available soon
<!-- temporarily comment off

```c
/* Unsubscribe */
tmq_unsubscribe(tmq);

/* Close consumer object */
tmq_consumer_close(tmq);
```
-->

</TabItem>
<TabItem value="java" label="Java">

Will be available soon
<!-- temporarily comment off
```java
/* Unsubscribe */
consumer.unsubscribe();

/* Close consumer */
consumer.close();
```
-->

</TabItem>

<TabItem value="Go" label="Go">

Will be available soon
<!-- temporarily comment off
```go
consumer.Close()
```
-->

</TabItem>

<TabItem value="Rust" label="Rust">

```rust
consumer.unsubscribe().await;
```

</TabItem>

<TabItem value="Python" label="Python">
Will be available soon
<!-- temporarily comment off

```py
# Unsubscribe
consumer.unsubscribe()
# Close consumer
consumer.close()
```
-->

</TabItem>
<TabItem label="Node.JS" value="Node.JS">

Will be available soon
<!-- temporarily comment off
```js
consumer.unsubscribe();
consumer.close();
```
-->

</TabItem>

<TabItem value="C#" label="C#">
Will be available soon
<!-- temporarily comment off

```csharp
// Unsubscribe
consumer.Unsubscribe();

// Close consumer
consumer.Close();
```
-->

</TabItem>

</Tabs>

## Delete Topic

Once a topic becomes useless, it can be deleted.

You can delete topics that are no longer useful. Note that you must unsubscribe all consumers from a topic before deleting it.

```sql
/* Delete topic/
DROP TOPIC topic_name;
```

## Check Status

At any time, you can check the status of existing topics and consumers.

1. Query all existing topics.

```sql
SHOW TOPICS;
```

2. Query the status and subscribed topics of all consumers.

```sql
SHOW CONSUMERS;
```
