---
sidebar_label: Data Querying
title: Query Data
slug: /basic-features/data-querying
---

import Image from '@theme/IdealImage';
import windowModel from '../assets/data-querying-01.png';
import slidingWindow from '../assets/data-querying-02.png';
import sessionWindow from '../assets/data-querying-03.png';
import eventWindow from '../assets/data-querying-04.png';

This document describes how to query data that is stored in TDengine. You can use the following taosBenchmark command to generate the sample data used in this document.

```shell
taosBenchmark --start-timestamp=1600000000000 --tables=100 --records=10000000 --time-step=10000
```

This command creates the `test` database in TDengine and then creates the `meters` supertable containing a timestamp column; current, voltage, and phase metrics; and group ID and location tags. The supertable contains 100 subtables, each having 10 million data records. The timestamps of these records start from September 13, 2020, 12:26:40 UTC (1600000000000 Unix time) and increase by 10 seconds per record.

## Basic Queries

You use the `SELECT` statement to query data in TDengine. To specify filtering conditions, you use the `WHERE` clause.

```sql
SELECT * FROM meters 
WHERE voltage > 10 
ORDER BY ts DESC
LIMIT 5;
```

This SQL statement queries all records in the meters supertable whose voltage is greater than 10. It then orders them by timestamp in descending order (latest timestamp first) and limits the output to the first five records.

The results of the query are similar to the following:

```text
           ts            |       current        |   voltage   |        phase         |   groupid   |          location          |
=================================================================================================================================
 2023-11-15 06:13:10.000 |           11.2467804 |         245 |          149.5000000 |          10 | California.MountainView    |
 2023-11-15 06:13:10.000 |           11.2467804 |         245 |          149.5000000 |           5 | California.Sunnyvale       |
 2023-11-15 06:13:10.000 |           11.2467804 |         245 |          149.5000000 |           4 | California.Cupertino       |
 2023-11-15 06:13:10.000 |           11.2467804 |         245 |          149.5000000 |           3 | California.Sunnyvale       |
 2023-11-15 06:13:10.000 |           11.2467804 |         245 |          149.5000000 |           8 | California.SanDiego        |
```

## Aggregation Queries

You use the `GROUP BY` clause to perform aggregation queries. This clause groups data and returns a summary row for each group. You can group data by any column in the target table or view. It is not necessary that the columns in the `GROUP BY` clause be included in the `SELECT` list.

```sql
SELECT groupid, AVG(voltage) 
FROM meters 
WHERE ts >= "2022-01-01T00:00:00+00:00" 
AND ts < "2023-01-01T00:00:00+00:00" 
GROUP BY groupid;
```

This SQL statement queries the `meters` supertable for records whose timestamp is between January 1, 2022 at midnight UTC inclusive and January 1, 2023 at midnight UTC exclusive. It groups the results by the value of the `groupid` tag and calculates the average of the `voltage` metric for each group.

The results of the query are similar to the following:

```text
   groupid   |       avg(voltage)        |
==========================================
           8 |       244.093189053779810 |
           5 |       244.093189053779810 |
           1 |       244.093189053779810 |
           7 |       244.093189053779810 |
           9 |       244.093189053779810 |
           6 |       244.093189053779810 |
           4 |       244.093189053779810 |
          10 |       244.093189053779810 |
           2 |       244.093189053779810 |
           3 |       244.093189053779810 |
```

:::note

In a query with the `GROUP BY` clause, the `SELECT` list can include only the following expressions:

- Constants
- Aggregate functions
- The same expressions as those in the `GROUP BY` clause
- Expressions that include the preceding expressions

The `GROUP BY` clause does not order the result set in any specific way when aggregating data. To obtain an ordered result set, use the `ORDER BY` clause after the `GROUP BY` clause.

:::

For information about the aggregation functions that TDengine supports, see [Aggregation Functions](../../tdengine-reference/sql-manual/functions/#aggregate-functions).

## Partitioned Queries

You use the `PARTITION BY` clause to  partition data on a certain dimension and then perform calculations within each partition. You can partition data by any scalar expression, including columns, constants, scalar functions, or combinations of these.

```sql
SELECT location, AVG(voltage) 
FROM meters 
PARTITION BY location;
```

This SQL statement queries the `meters` supertable for all records. It partitions the results by the value of the `location` tag and calculates the average of the `voltage` metric for each partition.

The results of the query are similar to the following:

```text
          location          |       avg(voltage)        |
=========================================================
 California.SantaClara      |       244.093199999999996 |
 California.SanFrancisco    |       244.093199999999996 |
 California.SanJose         |       244.093199999999996 |
 California.LosAngles       |       244.093199999999996 |
 California.SanDiego        |       244.093199999999996 |
 California.Sunnyvale       |       244.093199999999996 |
 California.PaloAlto        |       244.093199999999996 |
 California.Cupertino       |       244.093199999999996 |
 California.MountainView    |       244.093199999999996 |
 California.Campbell        |       244.093199999999996 |
```

:::note

The `PARTITION BY` clause can be used before a `GROUP BY` clause or `WINDOW` clause. In this case, the subsequent clauses take effect on each partition.

:::

## Windowed Queries

Windowed queries partition the dataset by a window and perform aggregations on the data within each window. The following windows are supported:

- Time window
- State window
- Session window
- Event window
- Count window

The logic for windowing is shown in the following figure.

<figure>
<Image img={windowModel} alt="Windowing description"/>
<figcaption>Figure 1. Windowing logic</figcaption>
</figure>

:::note

The following conditions apply to all windowed queries:

1. You cannot use a `GROUP BY` clause with a window clause.
2. If you use a `PARTITION BY` clause in a windowed query, the `PARTITION BY` clause must occur before the window clause.
3. The expressions in the `SELECT` list for a windowed query can include only the following:
   - Constants
   - Pseudocolumns (`_wstart`, `_wend`, and `_wduration`)
   - Aggregate functions, including selection functions and time-series-specific functions that determine the number of rows output.

This means that windowed queries cannot include the timestamp column in the `SELECT` list. Instead, use the `_wstart` and `_wend` pseudocolumns to indicate the start and end time of the window, the `_wduration` pseudocolumn to indicate the duration of the window, and the `_qstart` and `_qend` pseudocolumns to indicate the start and end time of the query.

When using these pseudocolumns, note the following:

- The start and end time of the window are inclusive.
- The window duration is expressed in the time precision configured for the database.

:::

### Time Windows

You use the `INTERVAL` clause to create a time window. In this clause, you specify the size of the time window and an optional offset.

```sql
SELECT tbname, _wstart, _wend, AVG(voltage) 
FROM meters 
WHERE ts >= "2022-01-01T00:00:00+00:00" 
AND ts < "2022-01-01T00:05:00+00:00" 
PARTITION BY tbname 
INTERVAL(1m, 5s) 
SLIMIT 2;
```

This SQL statement queries the `meters` supertable for records whose timestamp is between January 1, 2022 at midnight UTC inclusive and 00:05 UTC exclusive. It partitions the results by table name in 1 minute windows with a 5 second offset and calculates the average of the `voltage` metric for each window. The `SLIMIT` clause then limits the results to only two partitions.

The results of this query are similar to the following:

```text
             tbname             |         _wstart         |          _wend          |       avg(voltage)        |
=================================================================================================================
 d26                            | 2022-01-01 07:59:05.000 | 2022-01-01 08:00:05.000 |       244.000000000000000 |
 d26                            | 2022-01-01 08:00:05.000 | 2022-01-01 08:01:05.000 |       244.166666666666657 |
 d26                            | 2022-01-01 08:01:05.000 | 2022-01-01 08:02:05.000 |       241.333333333333343 |
 d26                            | 2022-01-01 08:02:05.000 | 2022-01-01 08:03:05.000 |       245.166666666666657 |
 d26                            | 2022-01-01 08:03:05.000 | 2022-01-01 08:04:05.000 |       237.500000000000000 |
 d26                            | 2022-01-01 08:04:05.000 | 2022-01-01 08:05:05.000 |       240.800000000000011 |
 d2                             | 2022-01-01 07:59:05.000 | 2022-01-01 08:00:05.000 |       244.000000000000000 |
 d2                             | 2022-01-01 08:00:05.000 | 2022-01-01 08:01:05.000 |       244.166666666666657 |
 d2                             | 2022-01-01 08:01:05.000 | 2022-01-01 08:02:05.000 |       241.333333333333343 |
 d2                             | 2022-01-01 08:02:05.000 | 2022-01-01 08:03:05.000 |       245.166666666666657 |
 d2                             | 2022-01-01 08:03:05.000 | 2022-01-01 08:04:05.000 |       237.500000000000000 |
 d2                             | 2022-01-01 08:04:05.000 | 2022-01-01 08:05:05.000 |       240.800000000000011 |
```

By default, the `INTERVAL` clause creates a tumbling window in which time intervals do not overlap. You can add the `SLIDING` clause after the `INTERVAL` clause to create a sliding window.

```sql
SELECT tbname, _wstart, AVG(voltage)
FROM meters
WHERE ts >= "2022-01-01T00:00:00+00:00" 
AND ts < "2022-01-01T00:05:00+00:00" 
PARTITION BY tbname
INTERVAL(1m) SLIDING(30s)
SLIMIT 1;
```

This SQL statement queries the `meters` supertable for records whose timestamp is between January 1, 2022 at midnight UTC inclusive and 00:05 UTC exclusive. It partitions the results by table name in 1 minute windows with a sliding time of 30 seconds and calculates the average of the `voltage` metric for each window. The `SLIMIT` clause then limits the results to only one partition.

The results of this query are similar to the following:

```text
             tbname             |         _wstart         |       avg(voltage)        |
=======================================================================================
 d0                             | 2022-01-01 07:59:30.000 |       245.666666666666657 |
 d0                             | 2022-01-01 08:00:00.000 |       242.500000000000000 |
 d0                             | 2022-01-01 08:00:30.000 |       243.833333333333343 |
 d0                             | 2022-01-01 08:01:00.000 |       243.666666666666657 |
 d0                             | 2022-01-01 08:01:30.000 |       240.166666666666657 |
 d0                             | 2022-01-01 08:02:00.000 |       242.166666666666657 |
 d0                             | 2022-01-01 08:02:30.000 |       244.500000000000000 |
 d0                             | 2022-01-01 08:03:00.000 |       240.500000000000000 |
 d0                             | 2022-01-01 08:03:30.000 |       239.333333333333343 |
 d0                             | 2022-01-01 08:04:00.000 |       240.666666666666657 |
 d0                             | 2022-01-01 08:04:30.000 |       237.666666666666657 |
```

The relationship between the time window and sliding time in a windowed query is described in the following figure.

<figure>
<Image img={slidingWindow} alt="Sliding window logic"/>
<figcaption>Figure 2. Sliding window logic</figcaption>
</figure>

:::note

The following conditions apply to time windows:

- The offset, if used, cannot be larger than the time window.
- The sliding time, if used, cannot be larger than the time window. If the sliding time is equal to the time window, a tumbling window is created instead of a sliding window.
- The minimum size of the time window is 10 milliseconds.
- The following units of time are supported:
  - `b` (nanoseconds)
  - `u` (microseconds)
  - `a` (milliseconds)
  - `s` (seconds)
  - `m` (minutes)
  - `h` (hours)
  - `d` (days)
  - `w` (weeks)
  - `n` (months)
  - `y` (years).

:::

:::tip

For optimal performance, ensure that the local time zone is the same on your TDengine client and server when you use time windows. Time zone conversion may cause performance deterioration.

:::

You can use the `FILL` clause to specify how to handle missing data within a time window. The following fill modes are supported:

- `FILL(NONE)`: makes no changes. This mode is used by default.
- `FILL(NULL)`: replaces missing data with null values.
- `FILL(VALUE, <val>)`: replaces missing data with a specified value. Note that the actual value is determined by the data type of the column. For example, the `FILL(VALUE, 1.23)` clause on a column of type `INT` will replace missing data with the value `1`.
- `FILL(PREV)`: replaces missing data with the previous non-null value.
- `FILL(NEXT)`: replaces missing data with the next non-null value.
- `FILL(LINEAR)`: replaces missing data by interpolating between the nearest non-null values.

Note that if there is no data within the entire query time range, the specified fill mode does not take effect and no changes are made to the data. You can use the following fill modes to forcibly replace data: 

- `FILL(NULL_F)`: forcibly replaces missing data with null values.
- `FILL(VALUE_F, <val>)`: forcibly replaces missing data with the specified value.

:::note

- In a stream with the `INTERVAL` clause, `FILL(NULL_F)` and `FILL(VALUE_F)` do not take effect. It is not possible to forcibly replace missing data.
- In an `INTERP` clause, `FILL(NULL)` and `FILL(VALUE)` always forcibly replace missing data. It is not necessary to use `FILL(NULL_F)` and `FILL(VALUE_F)`.

:::

An example of the `FILL` clause is shown as follows:

```sql
SELECT tbname, _wstart, _wend, AVG(voltage)
FROM meters
WHERE ts >= "2022-01-01T00:00:00+00:00" 
AND ts < "2022-01-01T00:05:00+00:00" 
PARTITION BY tbname
INTERVAL(1m) FILL(prev)
SLIMIT 2;
```

This SQL statement queries the `meters` supertable for records whose timestamp is between January 1, 2022 at midnight UTC inclusive and 00:05 UTC exclusive. It partitions the results by table name in 1 minute windows, replaces any missing data with the previous non-null value, and calculates the average of the `voltage` metric for each window. The `SLIMIT` clause then limits the results to two partitions.

The results of this query are similar to the following:

```text
             tbname             |         _wstart         |          _wend          |       avg(voltage)        |
=================================================================================================================
 d0                             | 2022-01-01 08:00:00.000 | 2022-01-01 08:01:00.000 |       242.500000000000000 |
 d0                             | 2022-01-01 08:01:00.000 | 2022-01-01 08:02:00.000 |       243.666666666666657 |
 d0                             | 2022-01-01 08:02:00.000 | 2022-01-01 08:03:00.000 |       242.166666666666657 |
 d0                             | 2022-01-01 08:03:00.000 | 2022-01-01 08:04:00.000 |       240.500000000000000 |
 d0                             | 2022-01-01 08:04:00.000 | 2022-01-01 08:05:00.000 |       240.666666666666657 |
 d13                            | 2022-01-01 08:00:00.000 | 2022-01-01 08:01:00.000 |       242.500000000000000 |
 d13                            | 2022-01-01 08:01:00.000 | 2022-01-01 08:02:00.000 |       243.666666666666657 |
 d13                            | 2022-01-01 08:02:00.000 | 2022-01-01 08:03:00.000 |       242.166666666666657 |
 d13                            | 2022-01-01 08:03:00.000 | 2022-01-01 08:04:00.000 |       240.500000000000000 |
 d13                            | 2022-01-01 08:04:00.000 | 2022-01-01 08:05:00.000 |       240.666666666666657 |
```

:::tip

1. Ensure that you specify a time range when using a `FILL` clause. Otherwise, a large amount of data may be filled.
2. TDengine does not return more than 10 million rows with interpolated data in a single query.

:::

### State Windows

You use the `STATE_WINDOW` clause to create a state window, with the `CASE` expression defining a condition that triggers the start of a state and another condition that triggers its end. You can use integers or strings to represent this state. The state window opens when the start condition is met by a record and closes when the end condition is met by a subsequent record.

```sql
SELECT tbname, _wstart, _wend, _wduration, CASE WHEN voltage >= 205 AND voltage <= 235 THEN 1 ELSE 0 END status 
FROM meters 
WHERE ts >= "2022-01-01T00:00:00+00:00" 
AND ts < "2022-01-01T00:05:00+00:00" 
PARTITION BY tbname 
STATE_WINDOW(
    CASE WHEN voltage >= 205 AND voltage <= 235 THEN 1 ELSE 0 END
)
SLIMIT 10;
```

This SQL statement queries the `meters` supertable for records whose timestamp is between January 1, 2022 at midnight UTC inclusive and 00:05 UTC exclusive. It partitions the results by table name and sets the state value based on the voltage. Voltage between 205 and 235 inclusive returns a state value of 1 (normal), and voltage outside that range returns a state value of 0 (abnormal). The `SLIMIT` clause then limits the results to ten partitions.

The results of this query are similar to the following:

```text
             tbname             |         _wstart         |          _wend          |      _wduration       |        status         |
=====================================================================================================================================
 d26                            | 2022-01-01 08:00:00.000 | 2022-01-01 08:00:20.000 |                 20000 |                     0 |
 d26                            | 2022-01-01 08:00:30.000 | 2022-01-01 08:00:30.000 |                     0 |                     1 |
 d26                            | 2022-01-01 08:00:40.000 | 2022-01-01 08:01:40.000 |                 60000 |                     0 |
 d26                            | 2022-01-01 08:01:50.000 | 2022-01-01 08:01:50.000 |                     0 |                     1 |
 d26                            | 2022-01-01 08:02:00.000 | 2022-01-01 08:02:00.000 |                     0 |                     0 |
 d26                            | 2022-01-01 08:02:10.000 | 2022-01-01 08:02:10.000 |                     0 |                     1 |
 d26                            | 2022-01-01 08:02:20.000 | 2022-01-01 08:03:00.000 |                 40000 |                     0 |
 d26                            | 2022-01-01 08:03:10.000 | 2022-01-01 08:03:10.000 |                     0 |                     1 |
 d26                            | 2022-01-01 08:03:20.000 | 2022-01-01 08:03:20.000 |                     0 |                     0 |
 d26                            | 2022-01-01 08:03:30.000 | 2022-01-01 08:03:30.000 |                     0 |                     1 |
```

### Session Windows

You use the `SESSION` clause to create a session window. Sessions are based on the value of the primary timestamp column. The session is considered to be closed when a new record's timestamp exceeds a specified interval from the previous record.

```sql
SELECT tbname, _wstart, _wend, _wduration, COUNT(*)
FROM meters 
WHERE ts >= "2022-01-01T00:00:00+00:00" 
AND ts < "2022-01-01T00:10:00+00:00" 
PARTITION BY tbname
SESSION(ts, 10m)
SLIMIT 10;
```

This SQL statement queries the `meters` supertable for records whose timestamp is between January 1, 2022 at midnight UTC inclusive and 00:10 UTC exclusive. It partitions the results by table name in 10-minute sessions. The `SLIMIT` clause then limits the results to ten partitions.

The results of this query are similar to the following:

```text
             tbname             |         _wstart         |          _wend          |      _wduration       |       count(*)        |
=====================================================================================================================================
 d76                            | 2021-12-31 16:00:00.000 | 2021-12-31 16:09:50.000 |                590000 |                    60 |
 d47                            | 2021-12-31 16:00:00.000 | 2021-12-31 16:09:50.000 |                590000 |                    60 |
 d37                            | 2021-12-31 16:00:00.000 | 2021-12-31 16:09:50.000 |                590000 |                    60 |
 d87                            | 2021-12-31 16:00:00.000 | 2021-12-31 16:09:50.000 |                590000 |                    60 |
 d64                            | 2021-12-31 16:00:00.000 | 2021-12-31 16:09:50.000 |                590000 |                    60 |
 d35                            | 2021-12-31 16:00:00.000 | 2021-12-31 16:09:50.000 |                590000 |                    60 |
 d83                            | 2021-12-31 16:00:00.000 | 2021-12-31 16:09:50.000 |                590000 |                    60 |
 d51                            | 2021-12-31 16:00:00.000 | 2021-12-31 16:09:50.000 |                590000 |                    60 |
 d63                            | 2021-12-31 16:00:00.000 | 2021-12-31 16:09:50.000 |                590000 |                    60 |
 d0                             | 2021-12-31 16:00:00.000 | 2021-12-31 16:09:50.000 |                590000 |                    60 |
Query OK, 10 row(s) in set (0.043489s)
```

In the following figure, a 12-second session window has been configured. The first three records are in the first session, and the second three records are in the second session.

<figure>
<Image img={sessionWindow} alt="Session window example"/>
<figcaption>Figure 3. Session window example</figcaption>
</figure>

The difference between the timestamps of the first and second records is 10 seconds, less than the 12-second window defined in this example. Therefore the session remains open. However, the difference between the timestamps of the third and fourth records is 40 seconds. Therefore the session is closed after the third record, and a new session is opened when the fourth record is ingested.

### Event Windows

You use the `EVENT_WINDOW` clause to create an event window, with the `START WITH` expression defining a condition that opens the window and the `END WITH` expression defining a condition that closes the window. Both conditions can be any expression supported by TDengine, and they can involve different columns.

```sql
SELECT tbname, _wstart, _wend, _wduration, COUNT(*)
FROM meters 
WHERE ts >= "2022-01-01T00:00:00+00:00" 
AND ts < "2022-01-01T00:10:00+00:00" 
PARTITION BY tbname
EVENT_WINDOW START WITH voltage >= 10 END WITH voltage < 20
LIMIT 10;
```

This SQL statement queries the `meters` supertable for records whose timestamp is between January 1, 2022 at midnight UTC inclusive and 00:10 UTC exclusive. It partitions the results by table name and creates events based on the voltage. An event is triggered when voltage is greater than or equal to 10 and ends when voltage is less than 20. The `SLIMIT` clause then limits the results to ten partitions.

The results of this query are similar to the following:

```text
             tbname             |         _wstart         |          _wend          |      _wduration       |       count(*)        |
=====================================================================================================================================
 d0                             | 2021-12-31 16:00:00.000 | 2021-12-31 16:00:00.000 |                     0 |                     1 |
 d0                             | 2021-12-31 16:00:30.000 | 2021-12-31 16:00:30.000 |                     0 |                     1 |
 d0                             | 2021-12-31 16:00:40.000 | 2021-12-31 16:00:40.000 |                     0 |                     1 |
 d0                             | 2021-12-31 16:01:20.000 | 2021-12-31 16:01:20.000 |                     0 |                     1 |
 d0                             | 2021-12-31 16:02:20.000 | 2021-12-31 16:02:20.000 |                     0 |                     1 |
 d0                             | 2021-12-31 16:02:30.000 | 2021-12-31 16:02:30.000 |                     0 |                     1 |
 d0                             | 2021-12-31 16:03:10.000 | 2021-12-31 16:03:10.000 |                     0 |                     1 |
 d0                             | 2021-12-31 16:03:30.000 | 2021-12-31 16:03:30.000 |                     0 |                     1 |
 d0                             | 2021-12-31 16:03:40.000 | 2021-12-31 16:03:40.000 |                     0 |                     1 |
 d0                             | 2021-12-31 16:03:50.000 | 2021-12-31 16:03:50.000 |                     0 |                     1 |
Query OK, 10 row(s) in set (0.034127s)
```

The following figure describes how records trigger event windows.

<figure>
<Image img={eventWindow} alt="Event window example"/>
<figcaption>Figure 4. Event window example</figcaption>
</figure>

:::note

- An event window can consist of a single record. If a record satisfies the start and end conditions for the window while no other window is open, the record forms a window.
- Results are generated only when the event window closes. If the conditions that close the window are never met, the window is not created.
- If an event window query is performed on a supertable, all data from the supertable is consolidated into a single timeline. Event windows are opened and closed based on whether the consolidated data meets the specified conditions for the window.
- If an event window query is performed on the results of a subquery, the subquery results must be output in a valid timestamp order and contain valid timestamp columns.

:::

### Count Windows

You use the `COUNT_WINDOW` clause to create a count window. This window is defined as a fixed number of records. Data records are sorted by timestamp and divided into windows. If the total number of rows is not divisible by the specified number of records, the last window will have fewer records.

```sql
SELECT _wstart, _wend, COUNT(*)
FROM meters
WHERE ts >= "2022-01-01T00:00:00+00:00" AND ts < "2022-01-01T00:30:00+00:00"
COUNT_WINDOW(10);
```

This SQL statement queries the `meters` supertable for records whose timestamp is between January 1, 2022 at midnight UTC inclusive and 00:30 UTC exclusive. The data is then grouped into windows of 10 records each.

The results of this query are similar to the following:

```text
         _wstart         |          _wend          |       count(*)        |
============================================================================
 2022-01-01 08:00:00.000 | 2022-01-01 08:00:00.000 |                    10 |
 2022-01-01 08:00:00.000 | 2022-01-01 08:00:00.000 |                    10 |
 2022-01-01 08:00:00.000 | 2022-01-01 08:00:00.000 |                    10 |
 2022-01-01 08:00:00.000 | 2022-01-01 08:00:00.000 |                    10 |
 2022-01-01 08:00:00.000 | 2022-01-01 08:00:00.000 |                    10 |
 2022-01-01 08:00:00.000 | 2022-01-01 08:00:00.000 |                    10 |
 2022-01-01 08:00:00.000 | 2022-01-01 08:00:00.000 |                    10 |
 2022-01-01 08:00:00.000 | 2022-01-01 08:00:00.000 |                    10 |
 2022-01-01 08:00:00.000 | 2022-01-01 08:00:00.000 |                    10 |
 2022-01-01 08:00:00.000 | 2022-01-01 08:00:00.000 |                    10 |
```

You can create a sliding count window by specifying two arguments in the `COUNT_WINDOW` clause. For example, `COUNT_WINDOW(10, 2)` creates count windows of 10 records with an overlap of 2 records.

## Nested Queries

A nested query is a query whose results depend on the results of another query, known as an inner query. The inner query does not depend on the parameters of the outer query.

```sql
SELECT MAX(voltage), * 
FROM (
    SELECT tbname, LAST_ROW(ts), voltage, current, phase, groupid, location 
    FROM meters 
    PARTITION BY tbname
) 
GROUP BY groupid;
```

In this SQL statement, the inner query retrieves the last row of each subtable in the `meters` supertable and partitions the results by table name. The outer query then groups the results by the `groupid` tag and returns the maximum voltage for each group.

:::note

- The inner query results are treated as a virtual table for the outer query. You can assign an alias to this virtual table for easier referencing.
- The outer query can reference columns and pseudocolumns from the inner query using column names or expressions.
- Both the inner and outer queries support table joins. The results of the inner query can also participate in table joins.
- The inner query supports the same features as non-nested queries. However, the `ORDER BY` clause typically does not take effect in the inner query.
- The following conditions apply to outer queries:
  - If the inner query does not return a timestamp, the outer query cannot include functions that implicitly rely on the timestamp, such as `INTERP`, `DERIVATIVE`, `IRATE`, `LAST_ROW`, `FIRST`, `LAST`, `TWA`, `STATEDURATION`, `TAIL`, and `UNIQUE`.
  - If the inner query result is not ordered by timestamp, the outer query cannot include functions that require ordered data, such as `LEASTSQUARES`, `ELAPSED`, `INTERP`, `DERIVATIVE`, `IRATE`, `TWA`, `DIFF`, `STATECOUNT`, `STATEDURATION`, `CSUM`, `MAVG`, `TAIL`, and `UNIQUE`.
  - The outer query cannot include functions that require two-pass scanning, such as `PERCENTILE`.

:::

## UNION Clause

You use the `UNION` clause to combine the results of multiple queries whose results have the same structure. The column names, types, counts, and order must be identical among queries combined with a `UNION` clause.

```sql
(SELECT tbname, * FROM d1 LIMIT 1) 
UNION ALL 
(SELECT tbname, * FROM d11 LIMIT 2) 
UNION ALL 
(SELECT tbname, * FROM d21 LIMIT 3);
```

In this SQL statement, one record is queried from subtable `d`, two from `d11`, and three from `d21`. These records are combined into a single result set.

The results of this query are similar to the following:

```text
             tbname             |           ts            |       current        |   voltage   |        phase         |
=======================================================================================================================
 d21                            | 2020-09-13 20:26:40.000 |           11.7680807 |         255 |          146.0000000 |
 d21                            | 2020-09-13 20:26:50.000 |           14.2392311 |         244 |          148.0000000 |
 d21                            | 2020-09-13 20:27:00.000 |           10.3999424 |         239 |          149.5000000 |
 d11                            | 2020-09-13 20:26:40.000 |           11.7680807 |         255 |          146.0000000 |
 d11                            | 2020-09-13 20:26:50.000 |           14.2392311 |         244 |          148.0000000 |
 d1                             | 2020-09-13 20:26:40.000 |           11.7680807 |         255 |          146.0000000 |
```

:::note

A SQL statement can contain a maximum of 100 `UNION` clauses.

:::

## Join Queries

### Join Concepts

1. Driving Table

   In a join query, the role of the driving table depends on the type of join used: in Left Join queries, the left table is the driving table, while in Right Join queries, the right table is the driving table.

2. Join Condition

   In TDengine, the join condition is the specified criteria used to perform the join. For all join queries (except As Of Join and Window Join), you must specify a join condition, typically following the `ON` clause. In As Of Join, conditions in the `WHERE` clause can also be considered join conditions, while Window Join uses the `window_offset` as the join condition.

   For all joins except As Of Join, you must explicitly define a join condition. As Of Join defines an implicit condition by default, so if the default condition meets your needs, you can omit an explicit join condition.

   In joins other than As Of Join and Window Join, you can include multiple join conditions beyond the primary one. These secondary conditions must have an `AND` relationship with the primary condition, but they don’t need to have an `AND` relationship with each other. They can include primary key columns, tag columns, normal columns, constants, scalar functions, or any combination of logical expressions.

   The following SQL queries for smart meters contain valid join conditions:

   ```sql
   SELECT a.* FROM meters a LEFT JOIN meters b ON a.ts = b.ts AND a.ts > '2023-10-18 10:00:00.000';
   SELECT a.* FROM meters a LEFT JOIN meters b ON a.ts = b.ts AND (a.ts > '2023-10-18 10:00:00.000' OR a.ts < '2023-10-17 10:00:00.000');
   SELECT a.* FROM meters a LEFT JOIN meters b ON TIMETRUNCATE(a.ts, 1s) = TIMETRUNCATE(b.ts, 1s) AND (a.ts + 1s > '2023-10-18 10:00:00.000' OR a.groupId > 0);
   SELECT a.* FROM meters a LEFT ASOF JOIN meters b ON TIMETRUNCATE(a.ts, 1s) < TIMETRUNCATE(b.ts, 1s) AND a.groupId = b.groupId;
   ```

3. Primary Join Condition

   As a time-series database, TDengine join queries revolve around the primary key column. Therefore, for all join queries except As Of Join and Window Join, the join condition must include the primary key column as an equality condition. The first appearance of the primary key column in an equality join condition is considered the primary join condition. As Of Join allows non-equality conditions in the primary join condition, while Window Join specifies the primary condition via the `window_offset`.

   Except for Window Join, TDengine supports the usage of the `TIMETRUNCATE` function on the primary join condition, such as `ON TIMETRUNCATE(a.ts, 1s) = TIMETRUNCATE(b.ts, 1s)`. Beyond that, other functions and scalar operations are not supported for the primary join condition.

4. Grouping Conditions

   As Of Join and Window Join support grouping input data before applying the join. Each group is then joined separately, and the output does not include the group information. In As Of Join and Window Join, any equality conditions that appear after the `ON` clause (except for the As Of Join primary join condition) are considered grouping conditions.

5. Primary Key Timeline

   In TDengine, every table must have a primary key timestamp column, which is the primary key timeline used for time-related calculations. The `subquery` or `join` result must also clearly identify which column is the primary key timeline for further calculations. In a `subquery`, the first ordered primary key column (or a related pseudo-column, such as `_wstart`, `_wend`) is considered the primary key timeline of the output table. For join results, the primary key timeline follows these rules:

   - In Left/Right Join, the primary key column from the driving table (subquery) becomes the primary key timeline for subsequent queries. In Window Join, both left and right tables are ordered, so the primary key timeline can be selected from either table, with priority given to the table's own primary key column.
   - In Inner Join, either table’s primary key column can be selected as the primary key timeline. If there is a grouping condition (an equality condition on a tag column that is `AND`-related to the primary join condition), the primary key timeline cannot be generated.
   - Full Join does not generate a valid primary key timeline, meaning time-dependent operations cannot be performed.

### Syntax Explanation

In the following sections, we will explain Left Join and Right Join series using a unified approach. Therefore, in the explanations for Outer, Semi, Anti-Semi, As Of, and Window join types, we use "Left/Right" to cover both Left Join and Right Join. In these descriptions:

- "Left/Right" table refers to the left table for Left Join, and the right table for Right Join.
- "Right/Left" table refers to the right table for Left Join, and the left table for Right Join.

### Join Functions

The table below lists the different types of joins supported in TDengine, along with their definitions.

|         Join Type         |                          Definition                          |
| :-----------------------: | :----------------------------------------------------------: |
|        Inner Join         | An inner join returns only the data where both the left and right tables match the join condition, essentially returning the intersection of the two tables that meet the condition. |
|   Left/Right Outer Join   | A left/right outer join includes both the data where the left/right tables match the join condition, as well as the data from the left/right table that does not match the join condition. |
|   Left/Right Semi Join    | A left/right semi-join typically expresses the meaning of `IN` or `EXISTS`. Data is returned from the left/right table only when at least one match is found in the right/left table based on the join condition. |
| Left/Right Anti-Semi Join | A left/right anti-semi join is the inverse of a semi-join. It typically expresses the meaning of `NOT IN` or `NOT EXISTS`. Data is returned from the left/right table only when no match is found in the right/left table. |
|   Left/Right As Of Join   | A left/right approximate join, where the join does not require an exact match. As Of Join matches based on the nearest timestamp within the specified criteria. |
|  Left/Right Window Join   | A left/right window join matches data based on a sliding window. Each row in the left/right table constructs a window based on its timestamp and a specified window size, and matches with the right/left table. |
|      Full Outer Join      | A full outer join includes data from both tables, where matches exist or do not exist, returning the union of both tables' datasets. |

### Constraints and Limitations

1. Input Timeline Limitation

   In TDengine, all join operations require the input data to contain a valid primary key timeline. For table queries, this requirement is typically met. However, for subqueries, the output data must include a valid primary key timeline.

2. Join Condition Limitation

   The join condition limitations include the following:

   - Except for As Of Join and Window Join, all other join operations must have a primary key column as the primary join condition.
   - The primary join condition must have an `AND` relationship with other join conditions.
   - The primary key column in the primary join condition only supports the `timetruncate` function, but no other functions or scalar operations. However, as secondary join conditions, there are no such restrictions.

3. Grouping Condition Limitation

   Grouping condition limitations include:

   - Grouping conditions are restricted to tag columns or normal columns, excluding the primary key column.
   - Only equality conditions are supported for grouping.
   - Multiple grouping conditions are allowed, and they must have an `AND` relationship.

4. Query Result Order Limitation

   The limitations on query result ordering include:

   - For basic table queries, subqueries without grouping or ordering, results will be ordered by the primary key column from the driving table.
   - For supertable queries, Full Join, or queries with grouping conditions but no ordering, the results will not have a fixed order. If ordering is required, an `ORDER BY` clause should be used. In cases where a function depends on an ordered timeline, missing a valid timeline will prevent the function from working properly.
