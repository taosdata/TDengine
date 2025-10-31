---
title: Introduction
---

Similar to Kafka, users must define topics in TDengine TSDB. A topic in TDengine TSDB can represent a database, a supertable, or a query based on existing supertables, subtables, or basic tables.

Users can define topics using SQL statements with filters on tags, table names, columns, or expressions, and can apply scalar functions or user-defined functions (UDFs), excluding aggregation functions.

Compared with other message queue systems, this is the key advantage of TDengine TSDB’s data subscription feature. It provides greater flexibility: the granularity of the data is determined by the SQL that defines the topic, and data filtering and preprocessing are automatically handled by TDengine TSDB. This reduces data transmission volume and simplifies application design.

Once a consumer subscribes to a topic, it can receive data updates in real time. Multiple consumers can form a consumer group, sharing consumption progress to enable multithreaded, distributed data consumption and improve throughput. Different consumer groups consuming the same topic maintain independent offsets. A single consumer can subscribe to multiple topics.

When a topic corresponds to a supertable or database, its data may be distributed across multiple nodes or shards. Having multiple consumers in a group allows for higher consumption efficiency. TDengine TSDB’s message queue supports an ACK (Acknowledgment) mechanism, ensuring at least-once delivery even in complex conditions such as crashes or restarts.

To achieve this, TDengine TSDB automatically creates indexes for Write-Ahead Logging (WAL) files to support fast random access, and provides configurable file rotation and retention policies. Users can specify the retention period and size limits for WAL files. Through these mechanisms, the WAL is transformed into a persistent storage engine that preserves event arrival order. For query-based topics, TDengine TSDB reads data directly from the WAL. During consumption, TDengine TSDB retrieves data according to the current offset, applies filtering and transformation using the unified query engine, and pushes the processed data to consumers.

Starting from version 3.2.0.0, data subscription supports vnode migration and splitting. Since data subscription relies on WAL files, and these files are not synchronized during vnode migration or splitting, it is important to fully consume all WAL data before performing these operations. Otherwise, any unconsumed WAL data will become unavailable after the operation completes.

## Topic Types

TDengine TSDB supports three types of topics that can be created using SQL statements. The following sections describe each type.

### Query Topics

A query topic subscribes to the result of an SQL query. Essentially, it functions as a continuous query, where each execution returns only the latest data. The creation syntax is as follows:

```sql
CREATE TOPIC [IF NOT EXISTS] topic_name as subquery;
```

This SQL query subscribes to data using a SELECT statement (for example, SELECT * or SELECT ts, c1), and may include filter conditions and scalar function calculations. However, aggregate functions and time-window aggregations are not supported.

1. Once this type of topic is created, the structure of the subscribed data is fixed.
1. Columns or tags that are subscribed to or referenced in calculations cannot be deleted (`ALTER TABLE DROP`) or modified (`ALTER TABLE MODIFY`).
1. If the table schema changes, any newly added columns will not appear in the subscription result.
1. For SELECT \*, the subscription expands to include all columns present at creation time. For subtables and normal tables, these are data columns; for supertables, they include both data and tag columns.

For example, if you need to subscribe to all smart meter records where the voltage is greater than 200, and only return the timestamp, current, and voltage (excluding the phase), you can create a topic named `power_topic` with the following SQL statement:

```sql
CREATE TOPIC power_topic AS SELECT ts, current, voltage FROM power.meters WHERE voltage > 200;
```

### Supertable Topics

A supertable topic subscribes to all data within a specified supertable. The syntax is as follows:

```sql
CREATE TOPIC [IF NOT EXISTS] topic_name [with meta] AS STABLE stb_name [where_condition];
```

The differences between this and subscribing with `SELECT * FROM stbName` are as follows:

1. Schema changes are not restricted. Any structural changes to the supertable, as well as new data inserted afterward, will continue to be included in the subscription.
1. The returned data is unstructured, and its schema dynamically adapts to changes in the supertable definition.
1. The optional `WITH META` parameter allows the subscription to return statements for creating the supertable and its subtables — primarily used by taosX during supertable migration.
1. The optional `WHERE` condition parameter filters the subtables to subscribe to.
   - Only tags or tbname can be used in the `WHERE` clause; regular columns are not allowed.
   - Functions can be used to filter tags, but aggregate functions are not supported, since subtable tag values cannot be aggregated.
   - Constant expressions such as 2 > 1 (subscribe to all subtables) or FALSE (subscribe to none) are also valid.
1. The returned data does not include tag values.

### Database Topics

A database topic subscribes to all data within a specified database. The syntax is as follows:

```sql
CREATE TOPIC [IF NOT EXISTS] topic_name [with meta] AS DATABASE db_name;
```

This statement creates a subscription that includes data from all tables within the specified database:

1. The optional `WITH META` parameter allows the subscription to return metadata statements for creating, deleting, or modifying all supertables, subtables, and regular tables in the database. This is primarily used by taosX for database migration.
1. Supertable and database-level subscriptions are considered advanced subscription modes and are more prone to errors. If you need to use them, please consult technical support in advance.

## Deleting a Topic

If a topic is no longer needed, you can delete it. If the topic is currently being consumed by active subscribers, you can use the FORCE option to delete it forcibly. After a forced deletion, any consumers still subscribed to the topic will encounter errors when attempting to consume data.

```sql
DROP TOPIC [IF EXISTS] [FORCE] topic_name;
```

## View Topics

```sql
SHOW TOPICS;
```

The above SQL statement displays information about all topics in the current database.

## Consumers

### Create a Consumer

Consumers can only be created using the APIs provided by TDengine TSDB client drivers or connectors. For detailed instructions, refer to the Developer Guide or the Reference Manual.

### View Consumers

```sql
SHOW CONSUMERS;
```

The above SQL statement displays information about all consumers in the current database.

### Delete a Consumer Group

When creating a consumer, you must assign it to a consumer group. Individual consumers cannot be explicitly deleted, but you can delete the entire consumer group. If there are active consumers within the group, you can use the FORCE option to forcibly delete it. After a forced deletion, any consumers still consuming data will encounter errors.

```sql
DROP CONSUMER GROUP [IF EXISTS] [FORCE] cgroup_name ON topic_name;
```

## Data Subscription

### View Subscriptions

```sql
SHOW SUBSCRIPTIONS;
```

The above SQL statement displays information about the consumption of a topic across different vgroups, which can be used to monitor consumption progress.

### Data Subscription

TDengine TSDB provides a comprehensive and feature-rich data subscription API designed to meet real-time data streaming needs across different programming languages and frameworks. These APIs include functions for creating consumers, subscribing to topics, unsubscribing, retrieving real-time data, committing consumption progress, and getting or setting offsets.

TDengine TSDB currently supports multiple programming languages, including C, Java, Go, Rust, Python, and C#, enabling developers to easily integrate TDengine’s subscription capabilities into various application scenarios.

Notably, TDengine TSDB’s data subscription API maintains a high degree of compatibility with Kafka’s subscription API, allowing developers to get started quickly and leverage their existing experience. The official documentation provides detailed usage guides and example code for all APIs. See the Connectors section on the TDengine TSDB official website for more information.

TDengine TSDB also supports MQTT subscriptions, allowing data to be subscribed directly through MQTT clients. For details, refer to the MQTT Data Subscription section.

### Replay

TDengine TSDB data subscription supports replay, with which you can replay data streams in the order they were originally written. This functionality is built on TDengine TSDB's write-ahead logging (WAL) mechanism, ensuring data consistency and reliability.

To use the replay feature, you specify a time range in your query statement that defines the start and end times of the replay. This makes it easy to replay data from a specific time window, whether for troubleshooting, data analysis, or other purposes.

For example, if the following three records were written:

```text
2023/09/22 00:00:00.000
2023/09/22 00:00:05.000
2023/09/22 00:00:08.000
```

1. The first record will be replayed immediately.
1. The second record will be returned 5 seconds later.
1. The third record will be returned 3 seconds after the second one.

:::note

- To enable replay, set the consumer parameter `enable.replay` to `true`.
- Only query topics support replay. Supertable and database topics do not support replay.
- Replay progress is not saved. Once replay is stopped, you cannot resume it from the previous position.
- Because replay requires processing time, a timing deviation in the tens of milliseconds may occur during playback.

:::
