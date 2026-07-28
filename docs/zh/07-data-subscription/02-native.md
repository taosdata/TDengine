---
sidebar_label: Native 订阅
title: Native 订阅
description: 通过连接器 API 创建消费者并订阅主题
toc_max_heading_level: 4
---

TDengine 提供了类似于消息队列产品的数据订阅和消费接口。在许多场景中，采用 TDengine 的时序大数据平台，无须再集成消息队列产品，从而简化应用程序设计并降低运维成本。主题管理等基础知识请参阅 [数据订阅](./index.md)；详细 API 请参阅 [数据订阅编程接口](../10-developer-guide/07-subscription-api.md)。

## 创建主题

如下 SQL 将创建一个名为 `topic_meters` 的订阅。使用该订阅所获取的消息中的每条记录都由查询语句 `SELECT ts, current, voltage, phase, groupid, location FROM meters` 所选择的列组成。

```sql
CREATE TOPIC IF NOT EXISTS topic_meters AS SELECT ts, current, voltage, phase, groupid, location FROM meters;
```

## 创建消费者

TDengine 消费者的概念与 Kafka 类似，消费者通过订阅主题来接收数据流。消费者可以配置多种参数，如连接方式、服务器地址、自动提交 Offset、自动重连、数据传输压缩等，以适应不同的数据处理需求。创建消费者的常用参数包括：

- `td.connect.ip`：服务端的 FQDN。
- `td.connect.user`：用户名。
- `td.connect.pass`：密码。
- `td.connect.token`：Token。
- `td.connect.port`：服务端的端口号。
- `group.id`：消费组 ID，同一消费组共享消费进度。
- `client.id`：客户端 ID。
- `auto.offset.reset`：消费组订阅的初始位置（默认 `latest`）。
- `enable.auto.commit`：是否启用消费位点自动提交（默认开启）。
- `auto.commit.interval.ms`：自动提交消费位点的时间间隔（默认 `5000`）。
- `msg.with.table.name`：是否允许从消息中解析表名。
- `enable.replay`：是否开启数据回放功能。
- `session.timeout.ms`：消费者心跳丢失后的超时时间（默认 `12000`）。
- `max.poll.interval.ms`：消费者拉取数据间隔的最长时间（默认 `300000`）。
- `fetch.max.wait.ms`：服务端单次返回数据的最大耗时（默认 `1000`）。
- `min.poll.rows`：服务端单次返回数据的最小条数（默认 `4096`）。

高级参数（`tmq_conf_new` 默认均为关闭，详见 [数据订阅编程接口](../10-developer-guide/07-subscription-api.md)）：

- `enable.wal.marker`：提交位点时是否向 mnode 发送 WAL marker（boolean，默认 `false`）。
- `msg.enable.batchmeta`：是否启用批量元数据返回（非 `0` 开启；默认关闭）。Java WebSocket 侧属性名为 `enable_batch_meta`。

完整参数与各语言示例见 [数据订阅编程接口](../10-developer-guide/07-subscription-api.md)。

## 订阅消费数据

消费者订阅主题后，可以开始接收并处理这些主题中的消息。典型流程如下：

- 订阅数据：调用订阅接口，指定主题列表（名称），支持同时订阅多个主题。
- 拉取数据：调用 poll 类接口，每次调用获取一条消息；一条消息中可能包含多条记录。
- 解析结果：按各语言连接器约定解析消息字段；字段名和数据类型与主题定义的列一一对应。

## 指定订阅的 Offset

消费者可以指定从特定 Offset 开始读取分区中的消息，从而重读消息或跳过已处理的消息。

## 提交 Offset

当消费者读取并处理完消息后，可以提交 Offset，表示已经成功处理到该 Offset。Offset 提交可以是自动的（根据配置定期提交）或手动的（由应用程序控制何时提交）。

## 取消订阅和关闭消费

消费者可以取消对主题的订阅，停止接收消息。当消费者不再需要时，应关闭消费者实例，以释放资源并断开与 TDengine 服务器的连接。
