---
title: Data Subscription
slug: /advanced-features/data-subscription
---

TDengine provides Kafka-like publish/subscribe data subscription as a built-in component. You create topics in TDengine using SQL statements, and your applications can subscribe to your topics as consumers.

TDengine's message queue provides an ACK (Acknowledgment) mechanism to ensure at least once consumption in complex environments such as crashes and restarts.

To achieve the above functionality, TDengine automatically creates indexes for the Write-Ahead Logging (WAL) files to support fast random access and provides a flexible and configurable file switching and retention mechanism. Users can specify the retention time and size of the WAL files according to their needs. Through these methods, the WAL is transformed into a persistent storage engine that preserves the order of event arrival. For queries created in the form of topics, TDengine will read data from the WAL. During consumption, TDengine reads data directly from the WAL based on the current consumption progress and uses a unified query engine to perform filtering, transformation, and other operations before pushing the data to consumers.

## Topics

A topic can be a query, a supertable, or a database. You can filter by tag, table name, column, or expression and perform scalar operations. Note that data aggregation and time windows are not supported. The data granularity is determined by the SQL statement that defines the topic, and data filtering and preprocessing are automatically handled by TDengine.

For more information about topics, see [Create a Topic](../../tdengine-reference/sql-manual/manage-topics-and-consumer-groups/#create-a-topic).

## Consumers and Consumer Groups

Consumers that subscribe to a topic receive the latest data in real time. A single consumer can subscribe to multiple topics. If the topic corresponds to a supertable or database, the data may be distributed across multiple different nodes or data shards.

You can also create consumer groups to enable multithreaded, distributed data consumption. The consumers in a consumer group share consumption progress, while consumers in different consumer groups do not share consumption progress even if they consume the same topic.

Consumers and consumer groups are created in your applications, not in TDengine. For more information, see [Manage Consumers](../../developer-guide/manage-consumers/). To delete consumer groups from TDengine or view existing consumer groups, see [Manage Consumer Groups](../../tdengine-reference/sql-manual/manage-topics-and-consumer-groups/#manage-consumer-groups).

## Replay

You can replay data streams in the order of their actual write times. To replay a data stream, specify a time range in the query statement to control the start and end times for the replay.

For example, assume the following three records have been written to the database. During replay, the first record is returned immediately, the second record is returned 5 seconds later, and the third record is returned 3 seconds after the second record.

```text
2023/09/22 00:00:00.000
2023/09/22 00:00:05.000
2023/09/22 00:00:08.000
```

The following conditions apply to replay:

- Replay supports query topics only. You cannot use replay with supertable or database topics.
- Replay does not support progress saving.
- Replay precision may be delayed by several dozen milliseconds due to data processing.
