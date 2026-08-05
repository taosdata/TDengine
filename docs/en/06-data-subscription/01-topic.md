---
sidebar_label: Topic Syntax
title: Topic Syntax
description: CREATE/DROP/SHOW TOPIC, consumer groups, and replay for data subscription
toc_max_heading_level: 4
---

Starting from TDengine `v3.0.0.0`, the message queue has been significantly optimized and enhanced to simplify data subscription. Users can create topics with SQL and consume topic data through connector APIs, the `taos` shell, or MQTT clients.

## Topic Types

TDengine TSDB supports three types of topics created with SQL. The following sections describe each type. The maximum number of topics in a TDengine instance is controlled by `tmqMaxTopicNum` (default 20). See [taosd configuration parameters](../12-operations-and-tooling/03-components/01-taosd.md).

### Query Topics

Subscribe to a data stream defined by an SQL query. The creation syntax is as follows:

```sql
CREATE TOPIC [IF NOT EXISTS] topic_name AS subquery
```

This SQL subscribes to data with a `SELECT` statement (for example `SELECT *`, or column projections such as `SELECT ts, c1`). Query topics may include filter conditions and scalar functions, but do not support aggregate functions, time-window aggregation, or `DISTINCT`, `GROUP BY`, `ORDER BY`, `PARTITION BY`, `LIMIT`/`SLIMIT`, and similar clauses. Note that:

1. Once this type of topic is created, the structure of the subscribed data is fixed.
2. Columns or tags that are subscribed to or referenced in calculations cannot be deleted (`ALTER TABLE DROP`) or modified (`ALTER TABLE MODIFY`). From `v3.4.0.0`, you can modify, delete, or add these columns or tags, but you must run `RELOAD TOPIC` for the change to take effect.
3. For `SELECT *`, the subscription expands to all columns present at creation time: data columns for subtables and normal tables; data columns plus tag columns for supertables.
4. Query subscription on virtual tables is not supported.
5. Supertables, subtables, and normal tables in the subquery can be deleted. After deletion, subscribed data is empty. If a table with the same name is recreated, subscribed data remains empty because the table ID has changed. To subscribe to the new table, reload the topic with `RELOAD TOPIC`.

For example, to subscribe to all smart-meter rows where voltage is greater than 200 and return only the timestamp, current, and voltage (not phase), create topic `power_topic` as follows:

```sql
CREATE TOPIC power_topic AS SELECT ts, current, voltage FROM power.meters WHERE voltage > 200;
```

### Supertable Topics

Subscribe to all data in a specified supertable. The syntax is as follows:

```sql
CREATE TOPIC [IF NOT EXISTS] topic_name [WITH META | ONLY META] AS STABLE stb_name [where_condition]
```

Differences from subscribing with `SELECT * FROM stbName`:

1. Schema changes are not restricted. Structural changes and new data after those changes remain in the subscription.
2. Returned data is unstructured; its schema follows changes to the supertable definition.
3. Optional `WITH META` returns statements for creating the supertable and its subtables—mainly used by taosX for supertable migration.
4. Optional `ONLY META` subscribes only to metadata changes and does not transfer time-series data.
5. Optional `where_condition` filters which subtables to subscribe to. The `WHERE` clause cannot use regular columns—only tags or `tbname`. Functions may filter tags, but aggregate functions are not allowed because subtable tag values cannot be aggregated. Constant expressions such as `2 > 1` (all subtables) or `false` (no subtables) are also valid.
6. Returned data does not include tag values.
7. Subscription to virtual supertables is supported, but only metadata of virtual supertables can be subscribed. Specify `WITH META` or `ONLY META` when subscribing to virtual supertables; otherwise no content is received.

### Database Topics

Subscribe to all data in a specified database. The syntax is as follows:

```sql
CREATE TOPIC [IF NOT EXISTS] topic_name [WITH META | ONLY META] AS DATABASE db_name;
```

This statement creates a subscription that includes data from all tables in the database:

1. Optional `WITH META` returns metadata create/drop/alter statements for all supertables, subtables, and normal tables in the database—mainly used by taosX for database migration.
2. Optional `ONLY META` subscribes only to metadata changes and does not transfer time-series data.
3. With `WITH META` or `ONLY META`, virtual table information can be subscribed, and only virtual-table metadata is available.

**Note:** Supertable and database subscriptions are advanced modes and are more error-prone. If you need them, consult technical support.

## Deleting a Topic

If a topic is no longer needed, you can delete it. If consumers are subscribed to the topic, use `FORCE` to delete it forcibly. After a forced deletion, those consumers encounter errors when consuming (`FORCE` is supported from `v3.3.6.0`).

```sql
DROP TOPIC [IF EXISTS] [FORCE] topic_name;
```

## View Topics

```sql
SHOW TOPICS;
```

Displays information about all topics in the current database. For the full field list, see the metadata table [`INS_TOPICS`](../05-tdengine-sql/09-system-info/01-meta.md#ins_topics).

## Reload Topic

```sql
RELOAD TOPIC [IF EXISTS] topic_name AS subquery;
```

1. Supported from `v3.4.0.0`, for query topics only. It reloads the topic definition—mainly when changing columns or tags in a query topic, or when `SELECT *` subscriptions do not pick up added/removed columns or tags.
2. When you need to change the subscribed table schema, stop consumption first, change the schema, run `RELOAD TOPIC`, then resume subscription.

## Consumers

### Create a Consumer

Consumers are usually created through TDengine TSDB client drivers or connector APIs. See [Developer Guide · Data Subscription](../10-developer-guide/07-subscription-api.md). For a quick check, you can also run `subscribe <topic> -g <group_id>` in the `taos` shell; see [`taos` CLI data subscription](../12-operations-and-tooling/04-tools/01-taos-cli.md#data-subscription).

### View Consumers

```sql
SHOW CONSUMERS;
```

Displays information about all consumers in the current database, including status and creation time. For the full field list, see the performance table [`PERF_CONSUMERS`](../05-tdengine-sql/09-system-info/02-perf.md#perf_consumers).

### Delete a Consumer Group

When creating a consumer, you assign it to a consumer group. Individual consumers cannot be deleted explicitly, but you can delete the consumer group. If the group has active consumers, use `FORCE` to delete it forcibly. After forced deletion, those consumers encounter errors when consuming (`FORCE` is supported from `v3.3.6.0`).

```sql
DROP CONSUMER GROUP [IF EXISTS] [FORCE] cgroup_name ON topic_name;
```

## Data Subscription

### View Subscriptions

```sql
SHOW SUBSCRIPTIONS;
```

Displays consumption of a topic across vgroups, useful for monitoring progress. For the full field list, see the metadata table [`INS_SUBSCRIPTIONS`](../05-tdengine-sql/09-system-info/01-meta.md#ins_subscriptions).

### Subscribe to Data

TDengine TSDB provides multi-language data subscription APIs (create consumers, subscribe/unsubscribe, poll data, commit and set offsets, and more) that stay highly compatible with the Kafka subscription API so existing experience can be reused. Supported languages include C, Java, Go, Rust, Python, and C#. Usage and examples are in [Developer Guide · Data Subscription](../10-developer-guide/07-subscription-api.md) and the connector docs for each language.

Starting from `v3.3.7.0`, MQTT subscription is also available so MQTT clients can subscribe to data directly. See [MQTT Data Subscription](./03-mqtt.md). For native connector consumption flow and common parameters, see [Native Subscription](./02-native.md).

### Replay

TDengine TSDB data subscription supports replay: messages are pushed again at the original write-time intervals so you can re-run a data stream at its original pace. This capability is built on the WAL.

For example, if the following three rows were written, replay returns the first immediately, the second about 5 seconds later, and the third about 3 seconds after that:

```text
2023/09/22 00:00:00.000
2023/09/22 00:00:05.000
2023/09/22 00:00:08.000
```

When using replay, note that:

- Enable replay by setting the consumer parameter `enable.replay` to `true`.
- Only query topics support replay. Supertable and database topics do not.
- Replay progress is not saved.
- Replay needs processing time; timing error is typically on the order of tens of milliseconds.
- A `WHERE` clause in the topic SQL can limit the time range or filters; that is part of the topic definition and is independent of whether replay is enabled.
