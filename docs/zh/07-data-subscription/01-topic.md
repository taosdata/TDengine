---
sidebar_label: 主题语法
title: 主题语法
description: 数据订阅 CREATE/DROP/SHOW TOPIC、消费组与回放说明
toc_max_heading_level: 4
---

TDengine 从 `v3.0.0.0` 开始对消息队列做了大幅优化和增强，以简化用户的数据订阅方案。用户可以通过 SQL 创建订阅主题，然后使用连接器 API、`taos` shell 或 MQTT 客户端消费主题中的数据。

## 主题类型

TDengine 使用 SQL 创建的主题共有 3 种类型，下面分别介绍。一个 TDengine 实例可创建的 topic 个数上限由 `tmqMaxTopicNum` 参数控制，默认值为 20，详见 [taosd 配置参数](../12-operations-and-tooling/03-components/01-taosd.md#tmqmaxtopicnum)。

### 查询主题

订阅一条 SQL 查询定义的数据流，创建语法如下：

```sql
CREATE TOPIC [IF NOT EXISTS] topic_name AS subquery
```

该 SQL 通过 `SELECT` 语句订阅数据，包括 `SELECT *`，或 `SELECT ts, c1` 等指定列查询。查询主题可以带条件过滤和标量函数计算，但不支持聚合函数、时间窗口聚合，也不支持 `DISTINCT`、`GROUP BY`、`ORDER BY`、`PARTITION BY`、`LIMIT`/`SLIMIT` 等。需要注意的是：

1. 该类型 topic 一旦创建，订阅数据的结构即确定。
2. 被订阅或用于计算的列或标签不可被删除（`ALTER TABLE DROP`）或修改（`ALTER TABLE MODIFY`）。从 `v3.4.0.0` 开始，可以修改、删除、增加这些列或标签，但需要执行 `RELOAD TOPIC` 使变更生效。
3. 对于 `SELECT *`，订阅会展开为创建 topic 时的所有列：子表、普通表为数据列，超级表为数据列加标签列。
4. 不支持虚拟表的查询订阅。
5. `subquery` 中的超级表、子表、普通表可以被删除。删除后，订阅数据为空；如果删除后重新创建同名表，订阅数据仍然为空，因为表 ID 已变化。如需订阅新建表的数据，可以通过 `RELOAD TOPIC` 重新加载 topic。

假设需要订阅所有智能电表中电压值大于 200 的数据，且仅仅返回时间戳、电流、电压 3 个采集量（不返回相位），那么可以通过下面的 SQL 创建 `power_topic` 这个主题。

```sql
CREATE TOPIC power_topic AS SELECT ts, current, voltage FROM power.meters WHERE voltage > 200;
```

### 超级表主题

订阅一个超级表中的所有数据，语法如下：

```sql
CREATE TOPIC [IF NOT EXISTS] topic_name [WITH META | ONLY META] AS STABLE stb_name [where_condition]
```

与使用 `SELECT * FROM stbName` 订阅的区别是：

1. 不会限制用户的表结构变更，即表结构变更以及变更后的新数据都能够订阅到。
2. 返回的是非结构化的数据，返回数据的结构会随着超级表的表结构变化而变化。
3. `WITH META` 参数可选，指定后将返回创建超级表、子表等语句，主要用于 taosX 做超级表迁移。
4. `ONLY META` 参数可选，指定后仅订阅元数据变更，不再传输时序数据。
5. `where_condition` 参数可选，用于过滤符合条件的子表并订阅这些子表。`WHERE` 条件里不能有普通列，只能是 tag 或 `tbname`；可以使用函数过滤 tag，但不能使用聚合函数，因为子表 tag 值无法做聚合；也可以是常量表达式，比如 `2 > 1`（订阅全部子表），或者 `false`（订阅 0 个子表）。
6. 返回数据不包含标签。
7. 支持虚拟超级表的订阅，仅能订阅出虚拟超级表的 meta 信息，所以虚拟超级表订阅需要带上 `WITH META` 或 `ONLY META` 参数，否则订阅不到内容。

### 数据库主题

订阅一个数据库里所有数据，其语法如下：

```sql
CREATE TOPIC [IF NOT EXISTS] topic_name [WITH META | ONLY META] AS DATABASE db_name;
```

通过该语句可创建一个包含数据库所有表数据的订阅：

1. `WITH META` 参数可选，指定后将返回数据库里所有超级表、子表、普通表的元数据创建、删除、修改语句，主要用于 taosX 做数据库迁移。
2. `ONLY META` 参数可选，指定后仅订阅元数据变更，不再传输时序数据。
3. `WITH META` 或 `ONLY META` 的情况下可订阅出虚拟表的信息，并且仅能订阅出虚拟表的 meta 信息。

**说明**：超级表订阅和库订阅属于高级订阅模式，容易出错，如确实要使用，请咨询技术支持人员。

## 删除主题

如果不再需要订阅数据，可以删除 topic。如果当前 topic 被消费者订阅，通过 `FORCE` 语法可强制删除；强制删除后，订阅的消费者在消费数据时会出错（`FORCE` 语法从 `v3.3.6.0` 开始支持）。

```sql
DROP TOPIC [IF EXISTS] [FORCE] topic_name;
```

## 查看主题

```sql
SHOW TOPICS;
```

显示当前数据库下的所有主题信息。更完整字段见元数据表 [`INS_TOPICS`](../05-tdengine-sql/09-system-info/01-meta.md#ins_topics)。

## 加载主题

```sql
RELOAD TOPIC [IF EXISTS] topic_name AS subquery;
```

1. 该语法从 `v3.4.0.0` 开始支持，仅适用于查询主题，用于重新加载主题定义。它主要解决查询主题里变更列或 tag，以及 `SELECT *` 查询订阅时删除或增加列、tag 后输出结果不生效的问题。
2. 需要变更订阅表结构的 schema 时，建议先停止消费，再变更表结构，然后执行 `RELOAD TOPIC`，接着重新开始订阅。

## 消费者

### 创建消费者

消费者通常通过 TDengine 客户端驱动或者连接器所提供的 API 创建，详情可以参考 [开发指南 · 数据订阅](../10-developer-guide/07-subscription-api.md)。为了快速验证订阅功能，也可以在 `taos` shell 中执行 `subscribe <topic> -g <group_id>` 创建消费者并开始消费，具体用法请参考 [`taos` CLI 数据订阅](../12-operations-and-tooling/04-tools/01-taos-cli.md#数据订阅)。

### 查看消费者

```sql
SHOW CONSUMERS;
```

显示当前数据库下所有消费者的信息，包括消费者的状态、创建时间等。更完整字段见性能数据表 [`PERF_CONSUMERS`](../05-tdengine-sql/09-system-info/02-perf.md#perf_consumers)。

### 删除消费组

创建消费者时会为其指定一个消费者组。消费者不能显式地删除，但可以删除消费者组。如果当前消费者组里有消费者在消费，通过 `FORCE` 语法可强制删除；强制删除后，订阅的消费者在消费数据时会出错（`FORCE` 语法从 `v3.3.6.0` 开始支持）。

```sql
DROP CONSUMER GROUP [IF EXISTS] [FORCE] cgroup_name ON topic_name;
```

## 数据订阅

### 查看订阅信息

```sql
SHOW SUBSCRIPTIONS;
```

显示 topic 在不同 vgroup 上的消费信息，可用于查看消费进度。更完整字段见元数据表 [`INS_SUBSCRIPTIONS`](../05-tdengine-sql/09-system-info/01-meta.md#ins_subscriptions)。

### 订阅数据

TDengine 提供了多语言数据订阅 API（包括但不限于创建消费者、订阅主题、取消订阅、拉取数据、提交与设置消费进度等），与 Kafka 订阅 API 保持高度一致，便于复用既有开发经验。目前支持 C、Java、Go、Rust、Python 和 C# 等。用法与示例见 [开发指南 · 数据订阅](../10-developer-guide/07-subscription-api.md) 及各语言连接器文档。

从 `v3.3.7.0` 开始还提供 MQTT 订阅功能，可通过 MQTT 客户端直接订阅数据，详见 [MQTT 订阅](./03-mqtt.md)。Native 连接器消费流程与常用参数见 [Native 订阅](./02-native.md)。

### 回放功能

TDengine 的数据订阅支持回放（replay）：按数据实际写入时间间隔重新推送消息，便于按原有节奏重放数据流。该能力基于 WAL 实现。

例如写入如下 3 条数据时，回放会先返回第 1 条，约 5s 后返回第 2 条，再约 3s 后返回第 3 条：

```text
2023/09/22 00:00:00.000
2023/09/22 00:00:05.000
2023/09/22 00:00:08.000
```

使用回放功能时需要注意：

- 通过消费参数 `enable.replay` 为 `true` 开启回放。
- 仅查询主题支持回放，超级表主题和数据库主题不支持。
- 回放不支持进度保存。
- 回放本身需要处理时间，精度存在约数十毫秒的误差。
- 主题 SQL 中可用 `WHERE` 限定时间范围或过滤条件，这属于主题定义本身，与是否开启回放无关。
