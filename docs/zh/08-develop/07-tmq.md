---
title: 数据订阅
sidebar_label: 数据订阅
toc_max_heading_level: 4
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

TDengine提供了类似Kafka的数据订阅功能。本章以 WebSocket 连接方式为例，介绍数据订阅的相关API以及使用方法。

## 创建主题

创建主题的示例代码如下。

<Tabs defaultValue="java" groupId="createTopic">
<TabItem value="java" label="Java">

```java
Connection connection = DriverManager.getConnection(url, properties);
Statement statement = connection.createStatement();
statement.executeUpdate("CREATE TOPIC IF NOT EXISTS topic_meters AS SELECT ts, current, voltage, phase, groupid, location FROM meters");
```
</TabItem>
</Tabs>

上述代码将使用SQL“select ts, current, voltage, phase, groupId, location from meters”创建一个名为topic_meters的订阅。使用该订阅所获取的消息中的每条记录都由该查询语句所选择的列组成。

**注意**
在TDengine中，对于订阅查询，有以下限制。
- 查询语句限制：订阅查询只能使用 select 语句，不支持其他类型的SQL，如 insert、update或delete等。
- 始数据查询：订阅查询只能查询原始数据，而不能查询聚合或计算结果。
- 时间顺序限制：订阅查询只能按照时间正序查询数据。

## 创建消费者

<Tabs defaultValue="java" groupId="createConsumer">
<TabItem value="java" label="Java">

```java
Properties config = new Properties();
config.setProperty("td.connect.type", "ws");
config.setProperty("bootstrap.servers", "localhost:6041");
config.setProperty("auto.offset.reset", "latest");
config.setProperty("msg.with.table.name", "true");
config.setProperty("enable.auto.commit", "true");
config.setProperty("auto.commit.interval.ms", "1000");
config.setProperty("group.id", "group1");
config.setProperty("client.id", "1");
config.setProperty("value.deserializer", "com.taosdata.example.AbsConsumerLoop$ResultDeserializer");
config.setProperty("value.deserializer.encoding", "UTF-8");

this.consumer = new TaosConsumer<(config);
```
</TabItem>
</Tabs>

相关参数说明如下：
1. td.connect.type： 连接方式。jni：表示使用动态库连接的方式，ws/WebSocket：表示使用 WebSocket 进行数据通信。默认为 jni 方式。
2. bootstrap.servers： TDengine 服务端所在的ip:port，如果使用 WebSocket 连接，则为 taosAdapter 所在的ip:port。
3. auto.offset.reset：消费组订阅的初始位置，earliest： 从头开始订阅； latest： 仅从最新数据开始订阅。
4. enable.auto.commit： 是否允许自动提交。
5. group.id： consumer: 所在的 group。
6. value.deserializer： 结果集反序列化方法，可以继承 com.taosdata.jdbc.tmq.ReferenceDeserializer，并指定结果集 bean，实现反序列化。也可以继承 com.taosdata.jdbc.tmq.Deserializer，根据 SQL 的 resultSet 自定义反序列化方式。

## 订阅消费数据

订阅消费数据的示例代码如下

<Tabs defaultValue="java" groupId="poll">
<TabItem value="java" label="Java">

```java
while (!shutdown.get()) {
    ConsumerRecords<ResultBean records = consumer.poll(Duration.ofMillis(100));
    for (ConsumerRecord<ResultBean record : records) {
        ResultBean bean = record.value();
        process(bean);
    }
}
```
</TabItem>
</Tabs>

poll 每次调用获取一个消息，一个消息中可能有多个记录，需要循环处理。

## 指定订阅的 Offset

<Tabs defaultValue="java" groupId="seek">
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
</TabItem>
</Tabs>

## 提交 Offset

当 `enable.auto.commit` 为 false 时，可以手动提交 offset。

<Tabs defaultValue="java" groupId="commit">
<TabItem value="java" label="Java">

```java
void commitSync() throws SQLException;
void commitSync(Map<TopicPartition, OffsetAndMetadata offsets) throws SQLException;
// 异步提交仅在 native 连接下有效
void commitAsync(OffsetCommitCallback<V callback) throws SQLException;
void commitAsync(Map<TopicPartition, OffsetAndMetadata offsets, OffsetCommitCallback<V callback) throws SQLException;
```
</TabItem>
</Tabs>

## 取消订阅和关闭消费

<Tabs defaultValue="java" groupId="close">
<TabItem value="java" label="Java">

```java
// 取消订阅
consumer.unsubscribe();
// 关闭消费
consumer.close()
```
</TabItem>
</Tabs>