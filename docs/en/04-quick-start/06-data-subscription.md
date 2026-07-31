---
sidebar_label: Data Subscription
title: Data Subscription
description: Try the data subscription features provided by the message queue
---

In monitoring, alerting, real-time analytics, and data synchronization scenarios, downstream programs often need newly written data as soon as it arrives. Polling with periodic queries increases latency and puts extra load on the database. TDengine provides built-in data subscription so continuously written data can be pushed to downstream programs by topic, reducing polling logic and the complexity of adding a separate message queue.

This chapter continues with the smart meter model from previous chapters. You will use two `taos` shells for an end-to-end walkthrough: create a topic, open a second shell to subscribe, then write data in the first shell and watch the subscription output.

## Prerequisites

Confirm that you have completed the earlier chapters:

1. The TDengine service is running and you can connect with the shell.
2. You understand the basic model of the `power` database, the `meters` supertable, and subtables such as `d1001` and `d1002`.

If you have not created these objects yet, run the following SQL in the first shell.

```sql
CREATE DATABASE IF NOT EXISTS power PRECISION 'ms' KEEP 3650 DURATION 10 BUFFER 16;

USE power;

CREATE STABLE IF NOT EXISTS meters (
    ts timestamp,
    current float,
    voltage int,
    phase float
) TAGS (
    location varchar(64),
    group_id int
);

CREATE TABLE IF NOT EXISTS d1001
USING meters TAGS ("California.SanFrancisco", 2);

CREATE TABLE IF NOT EXISTS d1002
USING meters TAGS ("California.SanFrancisco", 3);
```

## Create a Topic

In the first shell, create a topic named `topic_meters`. A topic defines which data subscribers can receive. The following topic subscribes to newly written data on the `meters` supertable and also outputs `tbname` so you can see which subtable each row comes from.

```sql
CREATE TOPIC IF NOT EXISTS topic_meters AS
SELECT tbname, ts, current, voltage, phase FROM meters;
```

Run the following command to verify that the topic was created.

```sql
SHOW TOPICS;
```

## Open a Second Shell and Subscribe

Open another terminal, enter the shell, and run the subscribe command.

```sql
subscribe topic_meters -g quickstart_cg;
```

Where:

- `topic_meters` is the topic name to subscribe to.
- `-g quickstart_cg` specifies the consumer group. The group stores consumption progress; when the same group subscribes again, it continues from the committed position.

After you run the command, the shell waits and shows a prompt similar to:

```text
Subscribing to topic [topic_meters], group [quickstart_cg], offset [latest] ...
Press Ctrl+C to stop.
```

By default, subscription starts from the latest position. Keep this shell open, then return to the first shell to write new data.

## Write Data and View Subscription Results

In the first shell, insert two new meter rows.

```sql
INSERT INTO d1001 VALUES (NOW, 10.3, 219, 0.31);
INSERT INTO d1002 VALUES (NOW, 10.2, 220, 0.23);
```

In the second shell, the subscribe command outputs the newly written data in real time. The layout may vary slightly with terminal width; the content looks similar to:

```text
tbname |           ts            | current | voltage | phase |
================================================================
d1001  | 2026-07-24 18:20:01.000 | 10.3000 |     219 | 0.310 |
d1002  | 2026-07-24 18:20:02.000 | 10.2000 |     220 | 0.230 |
```

Press `Ctrl+C` to stop the subscription. After stopping, the shell prints the total number of rows received.

```text
Unsubscribed. Total rows received: 2
```

## Common Subscribe Options

The shell subscribe command format is:

```sql
subscribe <topic> -g <group_id> [options];
```

Common options include:

- `-o earliest`: Start from the earliest consumable position. Useful when you want existing data in the topic.
- `-o latest`: Start from the latest position. This is the default and is suited to waiting for new data in real time.
- `-n <count>`: Exit automatically after receiving the specified number of rows. Convenient for demos and tests.
- `-t <timeout_ms>`: Poll timeout in milliseconds.

For example, the following command reads from the earliest position and exits after 5 rows.

```sql
subscribe topic_meters -g quickstart_cg_earliest -o earliest -n 5;
```

For help, run:

```sql
subscribe -h;
```

## View and Clean Up Subscription Resources

In the shell you can list topics, consumers, and subscription assignments.

```sql
SHOW TOPICS;
SHOW CONSUMERS;
SHOW SUBSCRIPTIONS;
```

When you no longer need this quick-start example, stop the subscribe shell first, then run the following SQL to clean up.

```sql
DROP CONSUMER GROUP IF EXISTS FORCE quickstart_cg ON topic_meters;
DROP CONSUMER GROUP IF EXISTS FORCE quickstart_cg_earliest ON topic_meters;
DROP TOPIC IF EXISTS topic_meters;
```

For more topic types, consumer group management, and programming APIs, continue with [Data Subscription](../07-data-subscription/index.md) and [Topic Syntax](../07-data-subscription/01-topic.md).
