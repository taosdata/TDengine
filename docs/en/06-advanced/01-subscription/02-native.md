---
title: Native Subscription
---

TDengine TSDB provides data subscription and consumption interfaces similar to those of message queue systems. In many scenarios, using TDengine TSDB as the time-series data platform eliminates the need to integrate an additional message queue, thereby simplifying application design and reducing operational costs.

For fundamental concepts such as topic management, refer to the [documentation](../topic/). For detailed API usage, see the Developer Guide.

## Create a Topic

The following SQL statement creates a subscription named `topic_meters`. Each record retrieved from this subscription will contain the columns selected by the query `SELECT ts, current, voltage, phase, groupid, location FROM meters`.

```sql
CREATE TOPIC IF NOT EXISTS topic_meters AS SELECT ts, current, voltage, phase, groupid, location FROM meters; 
```

## Create a Consumer

The concept of a consumer in TDengine TSDB is similar to Kafka: consumers receive data streams by subscribing to topics. Consumers can be configured with various parameters, such as connection method, server address, auto-commit offsets, auto-reconnect, and data compression.

The key parameters for creating a consumer include:

- td.connect.ip: FQDN of the server.
- td.connect.user: Username.
- td.connect.pass: Password.
- td.connect.port: Server port.
- group.id: Consumer group ID; members of the same group share consumption progress.
- client.id: Client ID.
- auto.offset.reset: Initial position for the group’s subscription.
- enable.auto.commit: Whether to enable automatic offset commits.
- auto.commit.interval.ms: Interval for automatically committing offsets.
- msg.with.table.name: Whether to parse the table name from messages.
- enable.replay: Whether to enable data replay.
- session.timeout.ms: Timeout after missed consumer heartbeats.
- max.poll.interval.ms: Maximum interval between consumer polls.
- fetch.max.wait.ms: Maximum server wait time for a single fetch response.
- min.poll.rows: Minimum number of rows returned per server fetch.

## Subscribe and Consume Data

After a consumer subscribes to one or more topics, it can begin receiving and processing messages from those topics.

The typical workflow is as follows:

- Subscribe to data: Call the subscribe function and specify the list of topic names to subscribe to. Multiple topics can be subscribed to simultaneously.
- Pull data: Call the poll function. Each call retrieves one message, which may contain multiple records.
- Process results: Parse the returned ResultBean object. The field names and data types in the object correspond one-to-one with the column names and data types defined in the topic’s query.

## Specify a Subscription Offset

A consumer can specify an offset to start reading messages from a particular position within a partition. This allows the consumer to re-read previously processed messages or skip those that have already been handled.

## Commit an Offset

After a consumer has successfully read and processed messages, it can commit the offset to record its progress. A committed offset indicates that all messages up to that position have been processed successfully. Offset commits can be performed either automatically (at regular intervals based on configuration) or manually (controlled by the application).

## Unsubscribe and Close the Consumer

A consumer can unsubscribe from topics to stop receiving messages. When a consumer is no longer needed, it should be closed to release resources and disconnect from the TDengine TSDB server.