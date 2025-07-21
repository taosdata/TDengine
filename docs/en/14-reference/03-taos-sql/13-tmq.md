---
title: Data Subscription
slug: /tdengine-reference/sql-manual/manage-topics-and-consumer-groups
---

Starting from TDengine 3.0.0.0, significant optimizations and enhancements have been made to the message queue to simplify user solutions.

## Create topic

The maximum number of topics that can be created in TDengine is controlled by the parameter `tmqMaxTopicNum`, with a default of 20.

TDengine uses SQL to create a topic, with three types of topics available:

### Query topic

Syntax:

```sql
CREATE TOPIC [IF NOT EXISTS] topic_name as subquery
```

Subscribe through a `SELECT` statement (including `SELECT *`, or specific query subscriptions like `SELECT ts, c1`, which can include conditional filtering and scalar function calculations, but do not support aggregate functions or time window aggregation). It is important to note:

- Once this type of TOPIC is created, the structure of the subscribed data is fixed.
- Columns or tags that are subscribed to or used for calculations cannot be deleted (`ALTER table DROP`) or modified (`ALTER table MODIFY`).
- If there are changes to the table structure, new columns will not appear in the results.
- For `select *`, the subscription expands to include all columns at the time of creation (data columns for subtables and basic tables, data columns plus tag columns for supertables).

### Supertable topic

Syntax:

```sql
CREATE TOPIC [IF NOT EXISTS] topic_name [with meta] AS STABLE stb_name [where_condition]
```

The difference from subscribing with `SELECT * from stbName` is:

- It does not restrict user's table structure changes.
- Returns unstructured data: the structure of the returned data will change along with the supertable's structure.
- The `with meta` parameter is optional, when selected it will return statements for creating supertables, subtables, etc., mainly used for supertable migration in taosx.
- The `where_condition` parameter is optional, when selected it will be used to filter subtables that meet the conditions, subscribing to these subtables. The where condition cannot include ordinary columns, only tags or tbname, and can use functions to filter tags, but not aggregate functions, as subtable tag values cannot be aggregated. It can also be a constant expression, such as 2 > 1 (subscribe to all subtables), or false (subscribe to 0 subtables).
- Returned data does not include tags.

### Database topic

Syntax:

```sql
CREATE TOPIC [IF NOT EXISTS] topic_name [with meta] AS DATABASE db_name;
```

This statement can create a subscription that includes all table data in the database.

- The `with meta` parameter is optional, when selected it will return statements for creating all supertables and subtables in the database, mainly used for database migration in taosx.

Note: Subscriptions to supertables and databases are advanced subscription modes and are prone to errors. If you really need to use them, please consult a professional.

## Delete topic

If you no longer need to subscribe to the data, you can delete the topic. If the current topic is subscribed to by a consumer, it can be forcibly deleted using the FORCE syntax. After the forced deletion, the subscribed consumer will consume data with errors (FORCE syntax supported from version 3.3.6.0)

```sql
/* Delete topic */
DROP TOPIC [IF EXISTS] [FORCE] topic_name;
```

At this point, if there are consumers on this subscription topic, they will receive an error.

## View topics

```sql
SHOW TOPICS;
```

Displays information about all topics in the current database.

## Create consumer group

Consumer groups can only be created through the TDengine client driver or APIs provided by connectors.

## Delete consumer group

When creating a consumer, a consumer group is assigned to the consumer. Consumers cannot be explicitly deleted, but the consumer group can be deleted. If there are consumers in the current consumer group who are consuming, the FORCE syntax can be used to force deletion. After forced deletion, subscribed consumers will consume data with errors (FORCE syntax supported from version 3.3.6.0).

```sql
DROP CONSUMER GROUP [IF EXISTS] [FORCE] cgroup_name ON topic_name;
```

Deletes the consumer group `cgroup_name` on the topic `topic_name`.

## View consumer groups

```sql
SHOW CONSUMERS;
```

Displays information about all active consumers in the current database.

## View subscription information

```sql
SHOW SUBSCRIPTIONS;
```

Displays the allocation relationship and consumption information between consumers and vgroups.
