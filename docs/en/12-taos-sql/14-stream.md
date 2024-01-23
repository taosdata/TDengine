---
title: Stream Processing
sidebar_label: Stream Processing
description: This document describes the SQL statements related to the stream processing component of TDengine.
---

Raw time-series data is often cleaned and preprocessed before being permanently stored in a database. Stream processing components like Kafka, Flink, and Spark are often deployed alongside a time-series database to handle these operations, increasing system complexity and maintenance costs.

Because stream processing is built in to TDengine, you are no longer reliant on middleware. TDengine offers a unified platform for writing, preprocessing, permanent storage, complex analysis, and real-time computation and alerting. Additionally, you can use SQL to perform all these tasks.

## Create a Stream

```sql
CREATE STREAM [IF NOT EXISTS] stream_name [stream_options] INTO stb_name SUBTABLE(expression) AS subquery
stream_options: {
 TRIGGER        [AT_ONCE | WINDOW_CLOSE | MAX_DELAY time]
 WATERMARK      time
 IGNORE EXPIRED [0|1]
 DELETE_MARK    time
 FILL_HISTORY   [0|1]
}

```

The subquery is a subset of standard SELECT query syntax:

```sql
subquery: SELECT [DISTINCT] select_list
    from_clause
    [WHERE condition]
    [PARTITION BY tag_list]
    [window_clause]
```

Session windows, state windows, and sliding windows are supported. When you configure a session or state window for a supertable, you must use PARTITION BY TBNAME.

Subtable Clause defines the naming rules of auto-created subtable, you can see more details in below part: Partitions of Stream.

```sql
window_clause: {
    SESSION(ts_col, tol_val)
  | STATE_WINDOW(col)
  | INTERVAL(interval_val [, interval_offset]) [SLIDING (sliding_val)]
}
```

`SESSION` indicates a session window, and `tol_val` indicates the maximum range of the time interval. If the time interval between two continuous rows are within the time interval specified by `tol_val` they belong to the same session window; otherwise a new session window is started automatically.

For example, the following SQL statement creates a stream and automatically creates a supertable named `avg_vol`. The stream has a 1 minute time window that slides forward in 30 second intervals to calculate the average voltage of the meters supertable.

```sql
CREATE STREAM avg_vol_s INTO avg_vol AS
SELECT _wstart, count(*), avg(voltage) FROM meters PARTITION BY tbname INTERVAL(1m) SLIDING(30s);
```

## Partitions of Stream

A Stream can process data in multiple partitions. Partition rules can be defined by PARTITION BY clause in stream processing. Each partition will have different timelines and windows, and will be processed separately and be written into different subtables of target supertable.

If a stream is created without PARTITION BY clause, all data will be written into one subtable.

If a stream is created with PARTITION BY clause without SUBTABLE clause, each partition will be given a random name. 

If a stream is created with PARTITION BY clause and SUBTABLE clause, the name of each partition will be calculated according to SUBTABLE clause. For example:

```sql
CREATE STREAM avg_vol_s INTO avg_vol SUBTABLE(CONCAT('new-', tname)) AS SELECT _wstart, count(*), avg(voltage) FROM meters PARTITION BY tbname tname INTERVAL(1m);
```

IN PARTITION clause, 'tbname', representing each subtable name of source supertable, is given alias 'tname'. And 'tname' is used in SUBTABLE clause. In SUBTABLE clause, each auto created subtable will concat 'new-' and source subtable name as their name(Starting from 3.2.3.0, in order to avoid the expression in subtable being unable to distinguish between different subtables, add '_groupId' to the end of subtable name).

If the output length exceeds the limitation of TDengine(192), the name will be truncated. If the generated name is occupied by some other table, the creation and writing of the new subtable will be failed.

## Filling history data

Normally a stream does not process data already or being written into source table when it's being creating. But adding FILL_HISTORY 1 as a stream option when creating the stream will allow it to process data written before and while creating the stream. For example:

```sql
create stream if not exists s1 fill_history 1 into st1  as select count(*) from t1 interval(10s)
```

Combining fill_history option and where clause, stream can processing data of specific time range. For example, only process data after a past time. (In this case, 2020-01-30)

```sql
create stream if not exists s1 fill_history 1 into st1  as select count(*) from t1 where ts > '2020-01-30' interval(10s)
```

As another example, only processing data starting from some past time, and ending at some future time.

```sql
create stream if not exists s1 fill_history 1 into st1  as select count(*) from t1 where ts > '2020-01-30' and ts < '2023-01-01' interval(10s)
```

If some streams are totally outdated, and you do not want it to monitor or process anymore, those streams can be manually dropped and output data will be still kept.


## Delete a Stream

```sql
DROP STREAM [IF EXISTS] stream_name
```

This statement deletes the stream processing service only. The data generated by the stream is retained.

## View Streams

```sql
SHOW STREAMS;
```

## Trigger Stream Processing

When you create a stream, you can use the TRIGGER parameter to specify triggering conditions for it.

For non-windowed processing, triggering occurs in real time. For windowed processing, there are three methods of triggering, the default value is AT_ONCE:

1. AT_ONCE: triggers on write

2. WINDOW_CLOSE: triggers when the window closes. This is determined by the event time. You can use WINDOW_CLOSE together with `watermark`. For more information, see Stream Processing Strategy for Out-of-Order Data.

3. MAX_DELAY: triggers when the window closes. If the window has not closed but the time elapsed exceeds MAX_DELAY, stream processing is also triggered.

Because the window closing is determined by the event time, a delay or termination of an event stream will prevent the event time from being updated. This may result in an inability to obtain the latest results.

For this reason, MAX_DELAY is provided as a way to ensure that processing occurs even if the window does not close.

MAX_DELAY also triggers when the window closes. Additionally, if a write occurs but the processing is not triggered before MAX_DELAY expires, processing is also triggered. 

## Stream Processing Strategy for Out-of-Order Data

When you create a stream, you can specify a watermark in the `stream_option` parameter.

The watermark is used to specify the tolerance for out-of-order data. The default value is 0.

T = latest event time - watermark

The window closing time for each batch of data that arrives at the system is updated using the preceding formula, and all windows are closed whose closing time is less than T. If the triggering method is WINDOW_CLOSE or MAX_DELAY, the aggregate result for the window is pushed.

Stream processing strategy for expired data
The data in expired windows is tagged as expired. TDengine stream processing provides two methods for handling such data:

1. Drop the data. This is the default and often only handling method for most stream processing engines.

2. Recalculate the data. In this method, all data in the window is reobtained from the database and recalculated. The latest results are then returned.

In both of these methods, configuring the watermark is essential for obtaining accurate results (if expired data is dropped) and avoiding repeated triggers that affect system performance (if expired data is recalculated).

## Supported functions

All [scalar functions](../function/#scalar-functions) are available in stream processing. All [Aggregate functions](../function/#aggregate-functions)  and  [Selection functions](../function/#selection-functions) are available in stream processing, except the followings:
  - [leastsquares](../function/#leastsquares)
  - [percentile](../function/#percentile)
  - [top](../function/#top)
  - [bottom](../function/#bottom)
  - [elapsed](../function/#elapsed)
  - [interp](../function/#interp)
  - [derivative](../function/#derivative)
  - [irate](../function/#irate)
  - [twa](../function/#twa)
  - [histogram](../function/#histogram)
  - [diff](../function/#diff)
  - [statecount](../function/#statecount)
  - [stateduration](../function/#stateduration)
  - [csum](../function/#csum)
  - [mavg](../function/#mavg)
  - [sample](../function/#sample)
  - [tail](../function/#tail)
  - [unique](../function/#unique)
  - [mode](../function/#mode)

## Pause\Resume stream
1.pause stream
PAUSE STREAM [IF EXISTS] stream_name;
If "IF EXISTS" is not specified and the stream does not exist, an error will be reported; If "IF EXISTS" is specified and the stream does not exist, success is returned; If the stream exists, paused all stream tasks.

2.resume stream
RESUME STREAM [IF EXISTS] [IGNORE UNTREATED] stream_name;
If "IF EXISTS" is not specified and the stream does not exist, an error will be reported. If "IF EXISTS" is specified and the stream does not exist, success is returned; If the stream exists, all of the stream tasks will be resumed. If "IGNORE UntREATED" is specified, data written during the pause period of stream is ignored when resuming stream.
