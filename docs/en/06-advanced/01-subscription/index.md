---
title: Data Subscription
slug: /advanced-features/data-subscription
---

To meet the needs of applications to obtain data written to TDengine in real-time, or to process data in the order of event arrival, TDengine provides data subscription and consumption interfaces similar to those of message queue products. In many scenarios, by adopting TDengine's time-series big data platform, there is no need to integrate additional message queue products, thus simplifying application design and reducing maintenance costs.

Similar to Kafka, users need to define topics in TDengine. However, a topic in TDengine can be a database, a supertable, or based on existing supertables, subtables, or basic tables with specific query conditions, i.e., a query statement. Users can use SQL to filter by tags, table names, columns, expressions, etc., and perform scalar function and UDF computations (excluding data aggregation). Compared to other message queue tools, this is the biggest advantage of TDengine's data subscription feature. It offers greater flexibility; the granularity of the data is determined by the SQL defining the topic, and the filtering and preprocessing of data are automatically handled by TDengine, reducing the amount of data transmitted and simplifying application complexity.

After subscribing to a topic, consumers can receive the latest data in real-time. Multiple consumers can form a consumption group to share consumption progress, enabling multi-threaded, distributed data consumption to increase consumption speed. Consumers in different consumption groups do not share consumption progress even if they consume the same topic. A consumer can subscribe to multiple topics. If the topic corresponds to a supertable or database, the data may be distributed across multiple different nodes or data shards. When there are multiple consumers in a consumption group, consumption efficiency can be improved. TDengine's message queue provides an ACK (Acknowledgment) mechanism to ensure at least once consumption in complex environments such as crashes and restarts.

To implement the above functions, TDengine automatically creates indexes for Write-Ahead Logging (WAL) files to support fast random access and provides flexible and configurable file switching and retention mechanisms. Users can specify the retention time and size of WAL files according to their needs. Through these methods, WAL is transformed into a persistent storage engine that retains the order of event arrival. For queries created in the form of topics, TDengine reads data from WAL. During consumption, TDengine reads data directly from WAL based on the current consumption progress, performs filtering, transformation, and other operations using a unified query engine, and then pushes the data to consumers.

Starting from version 3.2.0.0, data subscription supports vnode migration and splitting. Due to the dependence of data subscription on wal files, wal does not synchronize during vnode migration and splitting. Therefore, after migration or splitting, wal data that has not been consumed before cannot be consumed. So please ensure that all data has been consumed before proceeding with vnode migration or splitting, otherwise data loss may occur during consumption.

## Topics

TDengine uses SQL to create three types of topics, which are introduced below.

### Query Topic

Subscribe to the results of an SQL query, essentially a continuous query, returning only the latest values each time, with the following creation syntax:

```sql
CREATE TOPIC [IF NOT EXISTS] topic_name AS subquery;
```

This SQL subscribes through a SELECT statement (including SELECT \*, or specific query subscriptions like SELECT ts, c1, with condition filtering, scalar function computations, but does not support aggregate functions or time window aggregation). Note that:

1. Once this type of TOPIC is created, the structure of the subscribed data is fixed.
1. Columns or tags that are subscribed to or used for calculations cannot be deleted (ALTER table DROP) or modified (ALTER table MODIFY).
1. If table structure changes occur, newly added columns will not appear in the results.
1. For select \*, it subscribes to all columns at the time of creation (data columns for subtables and basic tables, data columns plus tag columns for supertables).

Suppose you need to subscribe to data where the voltage value in all smart meters is greater than 200, and only return the timestamp, current, and voltage (not phase), then you can create the topic power_topic with the following SQL.

```sql
CREATE TOPIC power_topic AS SELECT ts, current, voltage FROM power.meters WHERE voltage > 200;
```

### Supertable Topic

Subscribe to all data in a supertable, with the following syntax:

```sql
CREATE TOPIC [IF NOT EXISTS] topic_name [WITH META] AS STABLE stb_name [where_condition];
```

The difference from subscribing using `SELECT * from stbName` is:

1. It does not restrict user table structure changes, i.e., both structure changes and new data after changes can be subscribed to.
1. It returns unstructured data, and the structure of the returned data will change with the structure of the supertable.
1. The with meta parameter is optional; when selected, it returns statements for creating supertables, subtables, etc., mainly used for supertable migration in taosx.
1. The where_condition parameter is optional; when selected, it will be used to filter subtables that meet the conditions, subscribing to these subtables. The where condition cannot include ordinary columns, only tags or tbname, and functions can be used to filter tags, but not aggregate functions, as subtable tag values cannot be aggregated. It can also be a constant expression, such as 2 > 1 (subscribe to all subtables), or false (subscribe to 0 subtables).
1. Returned data does not include tags.

### Database Topics

Subscribe to all data in a database, with the syntax as follows:

```sql
CREATE TOPIC [IF NOT EXISTS] topic_name [WITH META] AS DATABASE db_name;
```

This statement creates a subscription that includes all table data in the database:

1. The `with meta` parameter is optional. When selected, it will return the creation, deletion, and modification statements of all supertables, subtables, and basic tables' metadata in the database, mainly used for database migration in taosx.
2. Subscriptions to supertables and databases are advanced subscription modes and are prone to errors. If you really need to use them, please consult technical support personnel.

## Delete Topic

If you no longer need to subscribe to the data, you can delete the topic. If the current topic is subscribed to by a consumer, it can be forcibly deleted using the FORCE syntax. After the forced deletion, the subscribed consumer will consume data with errors (FORCE syntax supported from version 3.3.6.0).

```sql
DROP TOPIC [IF EXISTS] [FORCE] topic_name;
```

## View Topics

```sql
SHOW TOPICS;
```

The above SQL will display information about all topics under the current database.

## Consumers

### Creating Consumers

Consumers can only be created through the TDengine client driver or APIs provided by connectors. For details, refer to the development guide or reference manual.

### View Consumers

```sql
SHOW CONSUMERS;
```

Displays information about all consumers in the current database, including the consumer's status, creation time, etc.

### Delete Consumer Group

When creating a consumer, a consumer group is assigned to the consumer. Consumers cannot be explicitly deleted, but the consumer group can be deleted. If there are consumers in the current consumer group who are consuming, the FORCE syntax can be used to force deletion. After forced deletion, subscribed consumers will consume data with errors (FORCE syntax supported from version 3.3.6.0).

```sql
DROP CONSUMER GROUP [IF EXISTS] [FORCE] cgroup_name ON topic_name;
```

## Data Subscription

### View Subscription Information

```sql
SHOW SUBSCRIPTIONS;
```

Displays consumption information of the topic on different vgroups, useful for viewing consumption progress.

### Subscribe to Data

TDengine provides comprehensive and rich data subscription APIs, aimed at meeting data subscription needs under different programming languages and frameworks. These interfaces include but are not limited to creating consumers, subscribing to topics, unsubscribing, obtaining real-time data, submitting consumption progress, and getting and setting consumption progress. Currently, TDengine supports a variety of mainstream programming languages, including C, Java, Go, Rust, Python, and C#, enabling developers to easily use TDengine's data subscription features in various application scenarios.

It is worth mentioning that TDengine's data subscription APIs are highly consistent with the popular Kafka subscription APIs in the industry, making it easy for developers to get started and leverage their existing knowledge and experience. To facilitate user understanding and reference, TDengine's official documentation provides detailed descriptions and example codes of various APIs, which can be accessed in the connectors section of the TDengine official website. Through these APIs, developers can efficiently implement real-time data subscription and processing to meet data handling needs in various complex scenarios.

TDengine 3.3.7.0 includes MQTT-based data subscription, allowing data to be subscribed to directly via an MQTT client. For details, refer to the MQTT Data Subscription section.

### Replay Feature

TDengine's data subscription feature supports a replay function, allowing users to replay the data stream in the actual order of data writing. This feature is based on TDengine's efficient WAL mechanism, ensuring data consistency and reliability.

To use the data subscription's replay feature, users can specify the time range in the query statement to precisely control the start and end times of the replay. This allows users to easily replay data within a specific time period, whether for troubleshooting, data analysis, or other purposes.

If the following 3 data entries were written, then during replay, the first entry is returned first, followed by the second entry after 5 seconds, and the third entry 3 seconds after obtaining the second entry.

```text
2023-09-22 00:00:00.000
2023-09-22 00:00:05.000
2023-09-22 00:00:08.000
```

When using the data subscription's replay feature, note the following:

- Enable replay function by configuring the consumption parameter enable.replay to true
- The replay function of data subscription only supports data playback for query subscriptions; supertable and database subscriptions do not support playback.
- Replay does not support progress saving.
- Because data playback itself requires processing time, there is a precision error of several tens of milliseconds in playback.
- MQTT data subscription does not support replay.


```mdx-code-block
import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

<DocCardList items={useCurrentSidebarCategory().items}/>
```
