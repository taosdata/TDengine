---
sidebar_label: Native 数据订阅
title: Native 数据订阅
toc_max_heading_level: 4
---

TDengine TSDB 提供了类似于消息队列产品的数据订阅和消费接口。在许多场景中，采用 TDengine TSDB 的时序大数据平台，无须再集成消息队列产品，从而简化应用程序设计并降低运维成本。数据订阅的主题管理等基础知识参考 [文档](../topic/) ，详细 API 可参考 [开发指南](../../../develop/tmq/)。

## 创建主题

如下 SQL 将创建一个名为 topic_meters 的订阅。使用该订阅所获取的消息中的每条记录都由此查询语句 `SELECT ts, current, voltage, phase, groupid, location FROM meters` 所选择的列组成。

```SQL
CREATE TOPIC IF NOT EXISTS topic_meters AS SELECT ts, current, voltage, phase, groupid, location FROM meters
```

## 创建消费者

TDengine TSDB 消费者的概念跟 Kafka 类似，消费者通过订阅主题来接收数据流。消费者可以配置多种参数，如连接方式、服务器地址、自动提交 Offset、自动重连、数据传输压缩等，以适应不同的数据处理需求。创建消费者的参数包括：

- td.connect.ip：服务端的 FQDN。
- td.connect.user：用户名。
- td.connect.pass：密码。
- td.connect.token：token。
- td.connect.port：服务端的端口号。
- group.id：消费组 ID，同一消费组共享消费进度。
- client.id：客户端 ID。
- auto.offset.reset：消费组订阅的初始位置。
- enable.auto.commit：是否启用消费位点自动提交。
- auto.commit.interval.ms：消费记录自动提交消费位点时间间隔。
- msg.with.table.name：是否允许从消息中解析表名。
- enable.replay：是否开启数据回放功能。
- session.timeout.ms：消费者心跳丢失后超时时间。
- max.poll.interval.ms：消费者拉取数据间隔的最长时间。
- fetch.max.wait.ms：服务端单次返回数据的最大耗时。
- min.poll.rows：服务端单次返回数据的最小条数。

## 订阅消费数据

消费者订阅主题后，可以开始接收并处理这些主题中的消息。典型的流程如下：

- 订阅数据：调用 subscribe 函数，指定订阅的主题列表（即名称），支持同时订阅多个主题。
- 拉取数据：调用 poll 函数，每次调用获取一个消息，一个消息中可能包含多个记录。
- 获取结果集：解析返回的 ResultBean 对象，其字段名和数据类型与列的名称和数据类型一一对应。

## 指定订阅的 Offset

消费者可以指定从特定 Offset 开始读取分区中的消息，这允许消费者重读消息或跳过已处理的消息。

## 提交 Offset

当消费者读取并处理完消息后，它可以提交 Offset，这表示消费者已经成功处理到这个 Offset 的消息。Offset 提交可以是自动的（根据配置定期提交）或手动的（应用程序控制何时提交）。

## 取消订阅和关闭消费

消费者可以取消对主题的订阅，停止接收消息。当消费者不再需要时，应该关闭消费者实例，以释放资源和断开与 TDengine TSDB 服务器的连接。  
