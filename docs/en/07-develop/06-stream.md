---
title: Stream Processing
sidebar_label: Stream Processing
description: This document describes the stream processing component of TDengine.
---

Raw time-series data is often cleaned and preprocessed before being permanently stored in a database. In a traditional time-series solution, this generally requires the deployment of stream processing systems such as Kafka or Flink. However, the complexity of such systems increases the cost of development and maintenance.

With the stream processing engine built into TDengine, you can process incoming data streams in real time and define stream transformations in SQL. Incoming data is automatically processed, and the results are pushed to specified tables based on triggering rules that you define. This is a lightweight alternative to complex processing engines that returns computation results in milliseconds even in high throughput scenarios.

The stream processing engine includes data filtering, scalar function computation (including user-defined functions), and window aggregation, with support for sliding windows, session windows, and event windows. Stream processing can write data to supertables from other supertables, standard tables, or subtables. When you create a stream, the target supertable is automatically created. New data is then processed and written to that supertable according to the rules defined for the stream. You can use PARTITION BY statements to partition the data by table name or tag. Separate partitions are then written to different subtables within the target supertable.

TDengine stream processing supports the aggregation of supertables that are deployed across multiple vnodes. It can also handle out-of-order writes and includes a watermark mechanism that determines the extent to which out-of-order data is accepted by the system. You can configure whether to drop or reprocess out-of-order data through the **ignore expired** parameter.

For more information, see [Stream Processing](../../taos-sql/stream).


## Create a Stream

```sql
CREATE STREAM [IF NOT EXISTS] stream_name [stream_options] INTO stb_name AS subquery
stream_options: {
 TRIGGER    [AT_ONCE | WINDOW_CLOSE | MAX_DELAY time]
 WATERMARK   time
 IGNORE EXPIRED [0 | 1]
}
```

For more information, see [Stream Processing](../../taos-sql/stream).

## Usage Scenario 1

It is common that smart electrical meter systems for businesses generate millions of data points that are widely dispersed and not ordered. The time required to clean and convert this data makes efficient, real-time processing impossible for traditional solutions. This scenario shows how you can configure TDengine stream processing to drop data points over 220 V, find the maximum voltage for 5 second windows, and output this data to a table.

### Create a Database for Raw Data

A database including one supertable and four subtables is created as follows:

```sql
DROP DATABASE IF EXISTS power;
CREATE DATABASE power;
USE power;

CREATE STABLE meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);

CREATE TABLE d1001 USING meters TAGS ("California.SanFrancisco", 2);
CREATE TABLE d1002 USING meters TAGS ("California.SanFrancisco", 3);
CREATE TABLE d1003 USING meters TAGS ("California.LosAngeles", 2);
CREATE TABLE d1004 USING meters TAGS ("California.LosAngeles", 3);
```

### Create a Stream

```sql
create stream current_stream trigger at_once into current_stream_output_stb as select _wstart as wstart, _wend as wend, max(current) as max_current from meters where voltage <= 220 interval (5s);
```

### Write Data
```sql
insert into d1001 values("2018-10-03 14:38:05.000", 10.30000, 219, 0.31000);
insert into d1001 values("2018-10-03 14:38:15.000", 12.60000, 218, 0.33000);
insert into d1001 values("2018-10-03 14:38:16.800", 12.30000, 221, 0.31000);
insert into d1002 values("2018-10-03 14:38:16.650", 10.30000, 218, 0.25000);
insert into d1003 values("2018-10-03 14:38:05.500", 11.80000, 221, 0.28000);
insert into d1003 values("2018-10-03 14:38:16.600", 13.40000, 223, 0.29000);
insert into d1004 values("2018-10-03 14:38:05.000", 10.80000, 223, 0.29000);
insert into d1004 values("2018-10-03 14:38:06.500", 11.50000, 221, 0.35000);
```

### Query the Results

```sql
taos> select start, end, max_current from current_stream_output_stb;
          start          |           end           |     max_current      |
===========================================================================
 2018-10-03 14:38:05.000 | 2018-10-03 14:38:10.000 |             10.30000 |
 2018-10-03 14:38:15.000 | 2018-10-03 14:38:20.000 |             12.60000 |
Query OK, 2 rows in database (0.018762s)
```

## Usage Scenario 2

In this scenario, the active power and reactive power are determined from the data gathered in the previous scenario. The location and name of each meter are concatenated with a period (.) between them, and the data set is partitioned by meter name and written to a new database.

### Create a Database for Raw Data

The procedure from the previous scenario is used to create the database.

### Create a Stream

```sql
create stream power_stream into power_stream_output_stb as select ts, concat_ws(".", location, tbname) as meter_location, current*voltage*cos(phase) as active_power, current*voltage*sin(phase) as reactive_power from meters partition by tbname;
```

### Write data

The procedure from the previous scenario is used to write the data.

### Query the Results
```sql
taos> select ts, meter_location, active_power, reactive_power from power_stream_output_stb;
           ts            |         meter_location         |       active_power        |      reactive_power       |
===================================================================================================================
 2018-10-03 14:38:05.000 | California.LosAngeles.d1004    |            2307.834596289 |             688.687331847 |
 2018-10-03 14:38:06.500 | California.LosAngeles.d1004    |            2387.415754896 |             871.474763418 |
 2018-10-03 14:38:05.500 | California.LosAngeles.d1003    |            2506.240411679 |             720.680274962 |
 2018-10-03 14:38:16.600 | California.LosAngeles.d1003    |            2863.424274422 |             854.482390839 |
 2018-10-03 14:38:05.000 | California.SanFrancisco.d1001  |            2148.178871730 |             688.120784090 |
 2018-10-03 14:38:15.000 | California.SanFrancisco.d1001  |            2598.589176205 |             890.081451418 |
 2018-10-03 14:38:16.800 | California.SanFrancisco.d1001  |            2588.728381186 |             829.240910475 |
 2018-10-03 14:38:16.650 | California.SanFrancisco.d1002  |            2175.595991997 |             555.520860397 |
Query OK, 8 rows in database (0.014753s)
```
