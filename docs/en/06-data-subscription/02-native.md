---
sidebar_label: Native Subscription
title: Native Subscription
description: Create consumers and subscribe to topics through connector APIs
toc_max_heading_level: 4
---

TDengine TSDB provides data subscription and consumption interfaces similar to those of message queue systems. In many scenarios, using TDengine TSDB as the time-series data platform eliminates the need to integrate an additional message queue, thereby simplifying application design and reducing operational costs.

For fundamental concepts such as topic management, see [Data Subscription](./index.md). For detailed API usage, see [Data Subscription API](../10-developer-guide/07-subscription-api.md).

## Create a Topic

The following SQL statement creates a subscription named `topic_meters`. Each record retrieved from this subscription will contain the columns selected by the query `SELECT ts, current, voltage, phase, groupid, location FROM meters`.

```sql
CREATE TOPIC IF NOT EXISTS topic_meters AS SELECT ts, current, voltage, phase, groupid, location FROM meters; 
```

## Create a Consumer

The concept of a consumer in TDengine TSDB is similar to Kafka: consumers receive data streams by subscribing to topics. Consumers can be configured with various parameters, such as connection method, server address, auto-commit offsets, auto-reconnect, and data compression.

The key parameters for creating a consumer include:

- `td.connect.ip`: FQDN of the server.
- `td.connect.user`: Username.
- `td.connect.pass`: Password.
- `td.connect.token`: Token.
- `td.connect.port`: Server port.
- `group.id`: Consumer group ID; members of the same group share consumption progress.
- `client.id`: Client ID.
- `auto.offset.reset`: Initial position for the group’s subscription (default: `latest`).
- `enable.auto.commit`: Whether to enable automatic offset commits (default: enabled).
- `auto.commit.interval.ms`: Interval for automatically committing offsets (default: `5000`).
- `msg.with.table.name`: Whether to parse the table name from messages.
- `enable.replay`: Whether to enable data replay.
- `session.timeout.ms`: Timeout after missed consumer heartbeats (default: `12000`).
- `max.poll.interval.ms`: Maximum interval between consumer polls (default: `300000`).
- `fetch.max.wait.ms`: Maximum server wait time for a single fetch response (default: `1000`).
- `min.poll.rows`: Minimum number of rows returned per server fetch (default: `4096`).

Advanced parameters (disabled by default in `tmq_conf_new`; see [Data Subscription API](../10-developer-guide/07-subscription-api.md)):

- `enable.wal.marker`: Whether to send a WAL marker to the mnode when committing an offset (Boolean; default: `false`).
- `msg.enable.batchmeta`: Whether to return metadata in batches (enabled by a nonzero value; disabled by default). The Java WebSocket property is named `enable_batch_meta`.

For the complete parameter list and language-specific examples, see [Data Subscription API](../10-developer-guide/07-subscription-api.md).

## Subscribe and Consume Data

After a consumer subscribes to one or more topics, it can begin receiving and processing messages from those topics.

The typical workflow is as follows:

- Subscribe to data: Call the subscribe function and specify the list of topic names to subscribe to. Multiple topics can be subscribed to simultaneously.
- Pull data: Call the poll function. Each call retrieves one message, which may contain multiple records.
- Process results: Parse message fields according to the conventions of the connector in use. Field names and data types correspond one-to-one with the columns defined by the topic query.

## Specify a Subscription Offset

A consumer can specify an offset to start reading messages from a particular position within a partition. This allows the consumer to re-read previously processed messages or skip those that have already been handled.

## Commit an Offset

After a consumer has successfully read and processed messages, it can commit the offset to record its progress. A committed offset indicates that all messages up to that position have been processed successfully. Offset commits can be performed either automatically (at regular intervals based on configuration) or manually (controlled by the application).

## Unsubscribe and Close the Consumer

A consumer can unsubscribe from topics to stop receiving messages. When a consumer is no longer needed, it should be closed to release resources and disconnect from the TDengine TSDB server.
