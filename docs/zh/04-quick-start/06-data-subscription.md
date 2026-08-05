---
sidebar_label: 数据订阅
title: 数据订阅
description: 体验消息队列提供的数据订阅功能
toc_max_heading_level: 4
---

在监控、告警、实时分析和数据同步等场景中，下游程序通常需要第一时间获取新写入的数据。如果通过定时查询拉取数据，不仅延迟更高，也会增加数据库查询压力。TDengine 提供内置数据订阅能力，可以把持续写入的数据按主题推送给下游程序，减少轮询逻辑和额外消息队列组件带来的复杂度。

本章继续使用前几章的智能电表模型，通过两个 `taos` shell 快速体验一次完整流程：先创建主题，再打开一个 shell 订阅主题，最后回到另一个 shell 写入数据并观察订阅结果。下面先给出订阅相关能力全景；完整语法与进阶说明请按下列链接深入阅读，或参见文末“继续阅读”。

## 订阅能力一览

与 Kafka 类似，你需要在 TDengine 中定义主题（topic）。主题可以是数据库、超级表，或基于现有表的查询语句；过滤与预处理由 TDengine 完成。消费者可加入消费组共享进度，数据从 WAL 推送，并提供至少一次（at least once）消费语义。

- **主题管理**
  创建 / 查看 / 删除主题；支持查询主题、超级表主题、数据库主题，以及 `RELOAD TOPIC` 重新加载查询主题定义。详见 [主题语法](../06-data-subscription/01-topic.md)。

- **消费组与进度**
  多个消费者组成消费组共享消费进度；不同消费组互不影响。可用 `SHOW CONSUMERS` / `SHOW SUBSCRIPTIONS` 查看状态，并用 `DROP CONSUMER GROUP` 清理。详见 [主题语法](../06-data-subscription/01-topic.md)。

- **Native 订阅**
  通过各语言连接器 API 创建消费者、订阅主题、拉取与解析消息、提交 Offset。详见 [Native 订阅](../06-data-subscription/02-native.md)、[数据订阅编程接口](../10-developer-guide/07-subscription-api.md)。

- **MQTT 订阅**
  从 `v3.3.7.0` 起，可通过 MQTT 客户端连接 Bnode（`taosmqtt`）订阅已创建主题。详见 [MQTT 订阅](../06-data-subscription/03-mqtt.md)。

下文从创建查询主题并用 shell 实时消费开始上手。

## 前提条件

请先确认已经完成前几章的操作：

1. TDengine 服务已经启动，可以通过 shell 连接。
2. 已经了解 `power` 数据库、`meters` 超级表和 `d1001`、`d1002` 等子表的基本模型。

如果你还没有创建这些对象，可以直接在第一个 shell 中执行下面的 SQL。

```sql
CREATE DATABASE IF NOT EXISTS power PRECISION 'ms' KEEP 3650 DURATION 10 BUFFER 16;

USE power;

CREATE STABLE IF NOT EXISTS meters (
    ts timestamp,
    current float,
    voltage int,
    phase float
) TAGS (
    location varchar(64),
    group_id int
);

CREATE TABLE IF NOT EXISTS d1001
USING meters TAGS ("California.SanFrancisco", 2);

CREATE TABLE IF NOT EXISTS d1002
USING meters TAGS ("California.SanFrancisco", 3);
```

## 创建订阅主题

在第一个 shell 中创建一个名为 `topic_meters` 的主题。主题定义了订阅者可以收到哪些数据。下面的主题订阅 `meters` 超级表中新写入的数据，并额外输出 `tbname`，方便你看到数据来自哪张子表。

```sql
CREATE TOPIC IF NOT EXISTS topic_meters AS
SELECT tbname, ts, current, voltage, phase FROM meters;
```

执行下面的命令可以查看主题是否创建成功。

```sql
SHOW TOPICS;
```

## 打开第二个 shell 订阅主题

新开一个终端窗口，进入 shell，然后执行订阅命令。

```sql
subscribe topic_meters -g quickstart_cg;
```

其中：

- `topic_meters` 是要订阅的主题名称。
- `-g quickstart_cg` 指定消费组。消费组会保存消费进度，同一个消费组再次订阅时会从已提交的位置继续消费。

执行后，shell 会进入等待状态，看到类似下面的提示：

```text
Subscribing to topic [topic_meters], group [quickstart_cg], offset [latest] ...
Press Ctrl+C to stop.
```

默认情况下，订阅从最新位置开始读取。因此请保持这个 shell 不要关闭，然后回到第一个 shell 写入新数据。

## 写入数据并查看订阅结果

在第一个 shell 中写入两条新的电表数据。

```sql
INSERT INTO d1001 VALUES (NOW, 10.3, 219, 0.31);
INSERT INTO d1002 VALUES (NOW, 10.2, 220, 0.23);
```

回到第二个 shell，可以看到订阅命令实时输出了刚写入的数据。输出格式会随终端宽度略有变化，内容类似如下：

```text
tbname |           ts            | current | voltage | phase |
================================================================
d1001  | 2026-07-24 18:20:01.000 | 10.3000 |     219 | 0.310 |
d1002  | 2026-07-24 18:20:02.000 | 10.2000 |     220 | 0.230 |
```

按 `Ctrl+C` 可以停止订阅。停止后，shell 会输出本次收到的总行数。

```text
Unsubscribed. Total rows received: 2
```

## 常用订阅选项

shell 的订阅命令格式如下：

```sql
subscribe <topic> -g <group_id> [options];
```

常用选项包括：

- `-o earliest`：从最早可消费的位置开始读取。适合希望读取主题中已有数据的场景。
- `-o latest`：从最新位置开始读取。这是默认值，适合实时等待新数据。
- `-n <count>`：收到指定行数后自动退出。演示和测试时很方便。
- `-t <timeout_ms>`：设置每次轮询的超时时间，单位为毫秒。

例如，下面的命令会从最早位置读取，收到 5 行后自动退出。

```sql
subscribe topic_meters -g quickstart_cg_earliest -o earliest -n 5;
```

如果想查看帮助，可以执行：

```sql
subscribe -h;
```

## 查看和清理订阅资源

在 shell 中可以查看当前 topic、消费者和订阅分配信息。

```sql
SHOW TOPICS;
SHOW CONSUMERS;
SHOW SUBSCRIPTIONS;
```

如果不再需要这个快速上手示例，可以先停止订阅 shell，再执行下面的 SQL 清理资源。

```sql
DROP CONSUMER GROUP IF EXISTS FORCE quickstart_cg ON topic_meters;
DROP CONSUMER GROUP IF EXISTS FORCE quickstart_cg_earliest ON topic_meters;
DROP TOPIC IF EXISTS topic_meters;
```

## 继续阅读

本章只覆盖快速上手阶段用 shell 验证查询主题订阅的常用流程。更完整的订阅能力，请继续阅读以下文档：

- [数据订阅](../06-data-subscription/index.md)：数据订阅概述、主题与消费组、WAL 与消费方式
- [主题语法](../06-data-subscription/01-topic.md)：`CREATE` / `DROP` / `SHOW TOPIC`、三种主题类型、消费组与回放说明
- [Native 订阅](../06-data-subscription/02-native.md)：通过连接器 API 创建消费者并订阅主题
- [MQTT 订阅](../06-data-subscription/03-mqtt.md)：通过 MQTT 客户端连接 Bnode 订阅主题数据
- [数据订阅编程接口](../10-developer-guide/07-subscription-api.md)：各语言连接器订阅 API 与示例
