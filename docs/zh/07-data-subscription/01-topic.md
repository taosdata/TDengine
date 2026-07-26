---
sidebar_label: 语法定义
title: 语法定义
toc_max_heading_level: 4
---

TDengine TSDB 3.0.0.0 开始对消息队列做了大幅优化和增强，以简化用户的数据订阅方案。用户可以通过 SQL 创建订阅主题，然后使用连接器 API、`taos shell` 或 MQTT 客户端消费主题中的数据。

## 主题类型

TDengine TSDB 使用 SQL 创建的主题共有 3 种类型，下面分别介绍。一个 TDengine TSDB 实例可创建的 topic 个数上限由 `tmqMaxTopicNum` 参数控制，默认值为 20。

### 查询主题

订阅一条 SQL 查询定义的数据流，创建语法如下：

```sql
CREATE TOPIC [IF NOT EXISTS] topic_name AS subquery
```

该 SQL 通过 `SELECT` 语句订阅数据，包括 `SELECT *`，或 `SELECT ts, c1` 等指定列查询。查询主题可以带条件过滤和标量函数计算，但不支持聚合函数，不支持时间窗口聚合。需要注意的是：

1. 该类型 topic 一旦创建，订阅数据的结构即确定。
2. 被订阅或用于计算的列或标签不可被删除（`ALTER TABLE DROP`）或修改（`ALTER TABLE MODIFY`）。从 3.4.0.0 开始，可以修改、删除、增加这些列或标签，但需要执行 `RELOAD TOPIC` 使变更生效。
3. 对于 `SELECT *`，订阅会展开为创建 topic 时的所有列：子表、普通表为数据列，超级表为数据列加标签列。
4. 不支持虚拟表的查询订阅。
5. `subquery` 中的超级表、子表、普通表可以被删除。删除后，订阅数据为空；如果删除后重新创建同名表，订阅数据仍然为空，因为表 ID 已变化。如需订阅新建表的数据，可以通过 `RELOAD TOPIC` 重新加载 topic。

假设需要订阅所有智能电表中电压值大于 200 的数据，且仅仅返回时间戳、电流、电压 3 个采集量（不返回相位），那么可以通过下面的 SQL 创建 power_topic 这个主题。

```sql
CREATE TOPIC power_topic AS SELECT ts, current, voltage FROM power.meters WHERE voltage > 200;
```

### 超级表主题

订阅一个超级表中的所有数据，语法如下：

```sql
CREATE TOPIC [IF NOT EXISTS] topic_name [WITH META] AS STABLE stb_name [where_condition]
```

与使用 `SELECT * from stbName` 订阅的区别是：

1. 不会限制用户的表结构变更，即表结构变更以及变更后的新数据都能够订阅到。
2. 返回的是非结构化的数据，返回数据的结构会随着超级表的表结构变化而变化。
3. `WITH META` 参数可选，指定后将返回创建超级表、子表等语句，主要用于 taosX 做超级表迁移。
4. `where_condition` 参数可选，用于过滤符合条件的子表并订阅这些子表。`WHERE` 条件里不能有普通列，只能是 tag 或 `tbname`；可以使用函数过滤 tag，但不能使用聚合函数，因为子表 tag 值无法做聚合；也可以是常量表达式，比如 `2 > 1`（订阅全部子表），或者 `false`（订阅 0 个子表）。
5. 返回数据不包含标签。
6. 支持虚拟超级表的订阅，仅能订阅出虚拟超级表的 meta 信息，所以虚拟超级表订阅需要带上 `WITH META` 参数，否则订阅不到内容。

### 数据库主题

订阅一个数据库里所有数据，其语法如下：

```sql
CREATE TOPIC [IF NOT EXISTS] topic_name [WITH META] AS DATABASE db_name;
```

通过该语句可创建一个包含数据库所有表数据的订阅：

1. `WITH META` 参数可选，指定后将返回数据库里所有超级表、子表、普通表的元数据创建、删除、修改语句，主要用于 taosX 做数据库迁移。
2. `WITH META` 的情况下可订阅出虚拟表的信息，并且仅能订阅出虚拟表的 meta 信息。

说明：超级表订阅和库订阅属于高级订阅模式，容易出错，如确实要使用，请咨询技术支持人员。

## 删除主题

如果不再需要订阅数据，可以删除 topic，如果当前 topic 被消费者订阅，通过 FORCE 语法可强制删除，强制删除后订阅的消费者会消费数据会出错（FORCE 语法从 v3.3.6.0 开始支持）。

```sql
DROP TOPIC [IF EXISTS] [FORCE] topic_name;
```

## 查看主题

```sql
SHOW TOPICS;
```

显示当前数据库下的所有主题信息。

## 加载主题

```sql
RELOAD TOPIC [IF EXISTS] topic_name AS subquery;
```

1. 该语法从 3.4.0.0 版本开始支持，仅适用于查询主题，用于重新加载主题定义。它主要解决查询主题里变更列或 tag 长度，以及 `SELECT *` 查询订阅时，删除或增加列、tag 后输出结果不生效的问题。
2. 需要变更订阅表结构的 schema 时，建议先停止消费，再变更表结构，然后执行 `RELOAD TOPIC`，接着重新开始订阅。

## 消费者

### 创建消费者

消费者通常通过 TDengine TSDB 客户端驱动或者连接器所提供的 API 创建，详情可以参考开发指南或参考手册。为了快速验证订阅功能，也可以在 `taos` shell 中执行 `subscribe <topic> -g <group_id>` 创建消费者并开始消费，具体用法请参考 [`taos` CLI 数据订阅](../12-operations-and-tooling/04-tools/01-taos-cli.md#数据订阅)。

### 查看消费者

```sql
SHOW CONSUMERS;
```

显示当前数据库下所有消费者的信息，会显示消费者的状态，创建时间等信息。

### 删除消费组

消费者创建的时候，会给消费者指定一个消费者组，消费者不能显式的删除，但是可以删除消费者组。如果当前消费者组里有消费者在消费，通过 FORCE 语法可强制删除，强制删除后订阅的消费者会消费数据会出错（FORCE 语法从 v3.3.6.0 开始支持）。

```sql
DROP CONSUMER GROUP [IF EXISTS] [FORCE] cgroup_name ON topic_name;
```

## 数据订阅

### 查看订阅信息

```sql
SHOW SUBSCRIPTIONS;
```

显示 topic 在不同 vgroup 上的消费信息，可用于查看消费进度。

### 订阅数据

TDengine TSDB 提供了全面且丰富的数据订阅 API，旨在满足不同编程语言和框架下的数据订阅需求。这些接口包括但不限于创建消费者、订阅主题、取消订阅、获取实时数据、提交消费进度以及获取和设置消费进度等功能。目前，TDengine TSDB 支持多种主流编程语言，包括 C、Java、Go、Rust、Python 和 C# 等，使得开发者能够轻松地在各种应用场景中使用 TDengine TSDB 的数据订阅功能。

值得一提的是，TDengine TSDB 的数据订阅 API 与业界流行的 Kafka 订阅 API 保持了高度的一致性，以便于开发者能够快速上手并利用现有的知识经验。为了方便用户了解和参考，TDengine TSDB 的官方文档详细介绍了各种 API 的使用方法和示例代码，具体内容可访问 TDengine TSDB 官方网站的连接器部分。通过这些 API，开发者可以高效地实现数据的实时订阅和处理，从而满足各种复杂场景下的数据处理需求。

TDengine TSDB v3.3.7.0 版本提供了 MQTT 订阅功能，可以通过 MQTT 客户端直接订阅数据，具体内容请参考 MQTT 数据订阅部分。

### 回放功能

TDengine TSDB 的数据订阅功能支持回放（replay）功能，允许用户按照数据的实际写入时间顺序重新播放数据流。这一功能基于 TDengine TSDB 的高效 WAL 机制实现，确保了数据的一致性和可靠性。

要使用数据订阅的回放功能，用户可以在查询语句中指定时间范围，从而精确控制回放的起始时间和结束时间。这使得用户能够轻松地重放特定时间段内的数据，无论是为了故障排查、数据分析还是其他目的。

如果写入了如下 3 条数据，那么回放时则先返回第 1 条数据，5s 后返回第 2 条数据，在获取第 2 条数据 3s 后返回第 3 条数据。

```text
2023/09/22 00:00:00.000
2023/09/22 00:00:05.000
2023/09/22 00:00:08.000
```

使用数据订阅的回放功能时需要注意如下几项：

- 通过配置消费参数 enable.replay 为 true 开启回放功能。
- 数据订阅的回放功能仅查询订阅支持数据回放，超级表和库订阅不支持回放。
- 回放不支持进度保存。
- 因为数据回放本身需要处理时间，所以回放的精度存在几十毫秒的误差。
