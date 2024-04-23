---
title: Data Subscription
sidebar_label: Data Subscription
description: This document describes the SQL statements related to the data subscription component of TDengine.
---

The information in this document is related to the TDengine data subscription feature.

## Create a Topic

```sql
CREATE TOPIC [IF NOT EXISTS] topic_name AS subquery;
```


You can use filtering, scalar functions, and user-defined scalar functions with a topic. JOIN, GROUP BY, windows, aggregate functions, and user-defined aggregate functions are not supported. The following rules apply to subscribing to a column:

1. The returned field is determined when the topic is created.
2. Columns to which a consumer is subscribed or that are involved in calculations cannot be deleted or modified.
3. If you add a column, the new column will not appear in the results for the subscription.
4. If you run `SELECT \*`, all columns in the subscription at the time of its creation are displayed. This includes columns in supertables, standard tables, and subtables. Supertables are shown as data columns plus tag columns.


## Delete a Topic

```sql
DROP TOPIC [IF EXISTS] topic_name;
```

If a consumer is subscribed to the topic that you delete, the consumer will receive an error.

## View Topics

## SHOW TOPICS

```sql
SHOW TOPICS;
```

The preceding command displays all topics in the current database.

## Create Consumer Group

You can create consumer groups only through the TDengine Client driver or the API provided by a client library.

## Delete Consumer Group

```sql
DROP CONSUMER GROUP [IF EXISTS] cgroup_name ON topic_name;
```

This deletes the cgroup_name in the topic_name.

## View Consumer Groups

```sql
SHOW CONSUMERS;
```

The preceding command displays all consumer groups in the current database.
