---
title: Manage Topics and Consumer Groups
description: The data subscription functionality provided by TDengine's message queue
slug: /tdengine-reference/sql-manual/manage-topics-and-consumer-groups
---

You create topics in TDengine to which your consumers can subscribe. You can also view a list of topics or delete topics as needed.

## Create a Topic

You can create a topic on a query, a supertable, or a database.

:::note

Exercise caution when creating supertable and database topics. Query topics are suitable for most use cases.

:::

### Query Topic

A query topic is a continuous query that returns only the latest value. The syntax is described as follows:

```sql
CREATE TOPIC [IF NOT EXISTS] <topic_name> AS <subquery>;
```

The subquery is a `SELECT` statement in which you can filter by tag, table name, column, or expression and perform scalar operations. Note that data aggregation and time windows are not supported.

The following conditions apply to query topics:

- You cannot modify or delete columns, including tag columns, that have an active subscription.
- If the table structure changes, newly added columns will not appear in the results of the topic. The structure of the results is determined when the topic is created, and it cannot be modified.
- You can specify `SELECT *` as a query topic to include all data and tag columns that exist on the target supertable or table when the topic is created.

The following example subscribes to all smart meters whose voltage is greater than 200 and returns the timestamp, current, and phase.

```sql
CREATE TOPIC power_topic AS SELECT ts, current, voltage FROM power.meters WHERE voltage > 200;
```

### Supertable Topic

A supertable topic allows consumers to subscribe to all data in a supertable. The syntax is described as follows:

```sql
CREATE TOPIC [IF NOT EXISTS] <topic_name> [WITH META] AS STABLE <stb_name> [WHERE <condition>];
```

The following conditions apply to supertable topics:

- The results of supertable topics do not include tag columns.
- You can modify the supertable after the topic has been created. The structure of the results reflects the latest structure of the supertable when the results are generated.
- You can include an optional `WITH META` clause to return the SQL statements used to create the supertable and subtables in the topic.
- You can include an optional `WHERE` clause to specify filtering conditions. Note the following with regard to filtering conditions:
  - You cannot filter by data columns. You can filter only by tag columns or by the `tbname` pseudocolumn.
  - You can use functions to filter tags, but aggregation and selection functions are not supported.
  - You can specify a constant expression, such as `2 > 1` (subscribing to all subtables) or `false` (subscribing to no subtables).


### Database Topic

A database topic allows consumers to subscribe to all data in a database. The syntax is described as follows:

```sql
CREATE TOPIC [IF NOT EXISTS] <topic_name> [WITH META] AS DATABASE <db_name>;
```

The optional `WITH META` clause returns the SQL statements used to create the supertables and subtables in the database.

## View Topics

You can display information about all topics in the current database. The syntax is described as follows:

```sql
SHOW TOPICS;
```

## Delete a Topic

You can delete topics that are no longer needed.

:::note

You must cancel subscriptions to a topic before deleting it. Any consumers subscribed to a topic will receive an error if the topic is deleted.

:::

The syntax for deleting a topic is described as follows:

```sql
DROP TOPIC [IF EXISTS] <topic_name>;
```

## Parameters

The `tmqMaxTopicNum` parameter defines the maximum number of topics that can be created in a TDengine deployment. The default value is 20.

## Manage Consumer Groups

You create consumer groups in your application through the TDengine client driver or APIs provided by client libraries. For more information, see [Manage Consumers](../../../developer-guide/manage-consumers/).

### View a Consumer Group

You can display information about all active consumers in the current database. The syntax is described as follows:

```sql
SHOW CONSUMERS;
```

### Delete a Consumer Group

You can delete consumer groups that are no longer needed. The syntax is described as follows:

```sql
DROP CONSUMER GROUP [IF EXISTS] <cgroup_name> ON <topic_name>;
```

## View Subscriptions

You can display information about all subscriptions in the current database. The syntax is described as follows:

```sql
SHOW SUBSCRIPTIONS;
```
