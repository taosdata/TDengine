---
sidebar_label: Subscription
title: Data Subscritpion
description: Use data subscription to get data from TDengine.
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import Java from "./_sub_java.mdx";
import Python from "./_sub_python.mdx";
import Go from "./_sub_go.mdx";
import Rust from "./_sub_rust.mdx";
import Node from "./_sub_node.mdx";
import CSharp from "./_sub_cs.mdx";
import CDemo from "./_sub_c.mdx";

This topic introduces how to read out data from TDengine using data subscription, which is an advanced feature in TDengine. To access the data in TDengine in data subscription way, you need to create topic, create consumer, subscribe to a topic, and consume data. In this document we will briefly explain these main steps of data subscription.

## Create Topic

A topic can be created on a database, on some selected columns,or on a supertable. 

### Topic on Columns

The most common way to create a topic is to create a topic on some specifically selected columns. The Syntax is like below:

```sql
CREATE TOPIC topic_name as subquery;
```

You can subscribe to a topic through a SELECT statement. Statements that specify columns, such as `SELECT *` and `SELECT ts, cl` are supported, as are filtering conditions and scalar functions. Aggregate functions and time window aggregation are not supported. Note:

- The schema of topics created in this manner is determined by the subscribed data.
- You cannot modify (`ALTER <table> MODIFY`) or delete (`ALTER <table> DROP`) columns or tags that are used in a subscription or calculation.
- Columns added to a table after the subscription is created are not displayed in the results. Deleting columns will cause an error.

For example:

```sql
CREATE TOPIC topic_name AS SELECT ts, c1, c2, c3 FROM tmqdb.stb WHERE c1 > 1;
```

### Topic on SuperTable

Syntax:

```sql
CREATE TOPIC topic_name AS STABLE stb_name;
```

Creating a topic in this manner differs from a `SELECT * from stbName` statement as follows:

- The table schema can be modified.
- Unstructured data is returned. The format of the data returned changes based on the supertable schema.
- A different table schema may exist for every data block to be processed.
- The data returned does not include tags.

### Topic on Database

Syntax:

```sql
CREATE TOPIC topic_name [WITH META] AS DATABASE db_name;
```

This SQL statement creates a subscription to all tables in the database. You can add the `WITH META` parameter to include schema changes in the subscription, including creating and deleting supertables; adding, deleting, and modifying columns; and creating, deleting, and modifying the tags of subtables. Consumers can determine the message type from the API. Note that this differs from Kafka.

## Program Model

1. Create Consumer

To create a consumer, you must use the APIs provided by TDengine connectors. Below is the sample code of using connectors of different languages.


You configure the following parameters when creating a consumer:

|            Parameter            |  Type   | Description                                                 | Remarks                                        |
| :----------------------------: | :-----: | -------------------------------------------------------- | ------------------------------------------- |
|        `td.connect.ip`         | string  | Used in establishing a connection; same as `taos_connect`                          |                                             |
|        `td.connect.user`         | string  | Used in establishing a connection; same as `taos_connect`                          |                                             |
|        `td.connect.pass`         | string  | Used in establishing a connection; same as `taos_connect`                          |                                             |
|        `td.connect.port`         | string  | Used in establishing a connection; same as `taos_connect`                          |                                             |
|           `group.id`           | string  | Consumer group ID; consumers with the same ID are in the same group                        | **Required**. Maximum length: 192.                 |
|          `client.id`           | string  | Client ID                                                | Maximum length: 192.                             |
|      `auto.offset.reset`       |  enum   | Initial offset for the consumer group                                     | Specify `earliest`, `latest`, or `none`(default) |
|      `enable.auto.commit`      | boolean | Commit automatically                                             | Specify `true` or `false`.                   |
|   `auto.commit.interval.ms`    | integer | Interval for automatic commits, in milliseconds                           |
| `enable.heartbeat.background`  | boolean | Backend heartbeat; if enabled, the consumer does not go offline even if it has not polled for a long time |                                             |
| `experimental.snapshot.enable` | boolean | Specify whether to consume messages from the WAL or from TSBS                    |                                             |
|     `msg.with.table.name`      | boolean | Specify whether to deserialize table names from messages                                 |

2. Subscribe to a Topic

A single consumer can subscribe to multiple topics.

3. Consume messages

4. Subscribe to a Topic

A single consumer can subscribe to multiple topics.

5. Consume Data

6. Close the consumer

After message consumption is finished, the consumer is unsubscribed.

## Sample Code

<Tabs defaultValue="rust" groupId="lang">

<TabItem value="c" label="C">

Will be available soon

</TabItem>
<TabItem value="java" label="Java">

Will be available soon

</TabItem>

<TabItem label="Go" value="Go">

Will be available soon

</TabItem>

<TabItem label="Rust" value="Rust">

<Rust />

</TabItem>

<TabItem value="Python" label="Python">

Will be available soon

<TabItem label="Node.JS" value="Node.JS">

Will be available soon

</TabItem>

<TabItem value="C#" label="C#">

Will be available soon

</TabItem>

</Tabs>

## Delete Topic

Once a topic becomes useless, it can be deleted.

You can delete topics that are no longer useful. Note that you must unsubscribe all consumers from a topic before deleting it.

```sql
/* Delete topic/
DROP TOPIC topic_name;
```

## Check Status

At any time, you can check the status of existing topics and consumers.

1. Query all existing topics.

```sql
SHOW TOPICS;
```

2. Query the status and subscribed topics of all consumers.

```sql
SHOW CONSUMERS;
```
