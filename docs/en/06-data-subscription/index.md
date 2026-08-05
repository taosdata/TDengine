---
sidebar_label: Data Subscription
title: Data Subscription
description: Overview of data subscription topics, consumer groups, WAL, and consumption methods
---

In monitoring, alerting, real-time analytics, and data synchronization scenarios, downstream applications often need newly written data as soon as it arrives. Polling with periodic queries increases latency and puts extra load on the database. TDengine TSDB provides a data subscription and consumption interface similar to that of a message queue product. In many scenarios, using TDengine TSDB as the time-series data platform removes the need to integrate a separate message queue, simplifying application design and lowering operational cost.

Similar to Kafka, users define topics in TDengine TSDB. A topic can be a database, a supertable, or a query over existing supertables, subtables, or basic tables—that is, a query statement. Users can filter by tags, table names, columns, or expressions in SQL, and apply scalar functions and UDFs (but not aggregation). Compared with other message queue tools, this is the main advantage of TDengine TSDB data subscription: the data granularity is determined by the topic SQL, and filtering and preprocessing are done automatically by TDengine TSDB, which reduces transferred data volume and application complexity.

After a consumer subscribes to a topic, it can receive the latest data in real time. Multiple consumers can form a consumer group that shares consumption progress for multithreaded or distributed consumption and higher throughput. Consumers in different groups do not share progress even when they subscribe to the same topic. One consumer can subscribe to multiple topics. If a topic maps to a supertable or database, data may span multiple nodes or shards; multiple consumers in a group improve efficiency. TDengine TSDB’s message queue provides an ACK (acknowledgment) mechanism for at-least-once delivery across failures such as crashes and restarts.

To support this, TDengine TSDB automatically indexes write-ahead log (WAL) files for fast random access and offers configurable file rotation and retention. Users can set WAL retention time and size. These mechanisms turn the WAL into a durable, arrival-order-preserving storage engine. For query topics, TDengine TSDB reads from the WAL, applies filtering and transformation with the unified query engine according to the current offset, and pushes data to consumers.

Starting from `v3.2.0.0`, data subscription supports vnode migration and splitting. Because subscription depends on WAL files, and WAL is not synchronized during vnode migration or splitting, you cannot continue consuming WAL data that was not finished before those operations. Fully consume all WAL data before migrating or splitting vnodes.

Starting from `v3.3.7.0`, you can also subscribe to existing topics with an MQTT client. See [MQTT Data Subscription](./03-mqtt.md).

This chapter next covers [Topic Syntax](./01-topic.md), [Native Subscription](./02-native.md), and [MQTT Data Subscription](./03-mqtt.md).
