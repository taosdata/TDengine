---
title: Manage Streams
description: Detailed syntax for SQL related to stream processing
slug: /tdengine-reference/sql-manual/manage-streams
---

You create streams to perform real-time computations on your data. You can also view a list of streams and delete streams as needed. For more information, see [Stream Processing](../../../advanced-features/stream-processing/).

## Create a Stream

```sql
CREATE STREAM [IF NOT EXISTS] stream_name [stream_options] INTO stb_name[(field1_name, field2_name [PRIMARY KEY], ...)] [TAGS (create_definition [, create_definition] ...)] SUBTABLE(expression) AS subquery
stream_options: {
 TRIGGER        [AT_ONCE | WINDOW_CLOSE | MAX_DELAY time]
 WATERMARK      time
 IGNORE EXPIRED [0|1]
 DELETE_MARK    time
 FILL_HISTORY   [0|1]
 IGNORE UPDATE  [0|1]
}
```

Where `subquery` is a subset of the standard SQL select query syntax:

```sql
subquery: SELECT select_list
    from_clause
    [WHERE condition]
    [PARTITION BY tag_list]
    window_clause
```

It supports session windows, state windows, sliding windows, event windows, and count windows. Note that state windows, event windows, and count windows must be used with `PARTITION BY tbname` when combined with super tables. If the data source table has a composite primary key, calculations for state windows, event windows, and count windows are not supported.

`stb_name` is the name of the super table that saves the calculation results. If this super table does not exist, it will be automatically created; if it does exist, it will check the schema information of the columns. For more details, see **Writing to Existing Super Tables**.

The `TAGS` clause defines the rules for creating TAGs in stream processing, allowing the generation of custom TAG values for each partition. For more details, see **Custom TAG**.

```sql
create_definition:
    col_name column_definition
column_definition:
    type_name [COMMENT 'string_value']
```

The `subtable` clause defines the naming rules for the sub-tables created in stream processing. For more details, see the **partition** section of stream processing.

```sql
window_clause: {
    SESSION(ts_col, tol_val)
  | STATE_WINDOW(col)
  | INTERVAL(interval_val [, interval_offset]) [SLIDING (sliding_val)]
  | EVENT_WINDOW START WITH start_trigger_condition END WITH end_trigger_condition
  | COUNT_WINDOW(count_val[, sliding_val])
}
```

Here, `SESSION` is the session window, where `tol_val` is the maximum range of the time interval. Data within the `tol_val` time interval belongs to the same window. If the time between two consecutive pieces of data exceeds `tol_val`, the next window will be automatically opened. The `_wend` of this window is equal to the time of the last piece of data plus `tol_val`.

`EVENT_WINDOW` is an event window, which defines a window based on start and end conditions. The window starts when `start_trigger_condition` is met and closes when `end_trigger_condition` is satisfied. Both `start_trigger_condition` and `end_trigger_condition` can be any valid TDengine condition expressions and may involve different columns.

`COUNT_WINDOW` is a counting window, which divides the window based on a fixed number of data rows. `count_val` is a constant, a positive integer that must be greater than or equal to 2 and less than 2147483648. `count_val` indicates the maximum number of data rows contained in each COUNT_WINDOW. If the total number of data rows is not evenly divisible by `count_val`, the last window may contain fewer than `count_val` rows. `sliding_val` is a constant that indicates the number of sliding windows, similar to INTERVAL's SLIDING.

The definition of the windows is identical to that in the distinguished query for time series data. For more details, see [TDengine Distinguished Queries](../time-series-extensions/).

For example, the following statements create stream processing tasks. The first stream processing task automatically creates a super table named `avg_vol`, calculates the average voltage of these meters with a one-minute time window and a 30-second forward increment, and writes the results from the `meters` table into the `avg_vol` table. Data from different partitions will create sub-tables and write to different sub-tables.

The second stream processing task automatically creates a super table named `streamt0`, aggregates data in order of timestamp with a window starting condition of `voltage < 0` and an ending condition of `voltage > 9`, writing the results from the `meters` table into the `streamt0` table, creating separate sub-tables for different partitions.

The third stream processing task automatically creates a super table named `streamt1`, aggregates data in order of timestamp with 10 data rows as a group, and writes the results from the `meters` table into the `streamt1` table, creating separate sub-tables for different partitions.

```sql
CREATE STREAM avg_vol_s INTO avg_vol AS
SELECT _wstart, COUNT(*), AVG(voltage) FROM meters PARTITION BY tbname INTERVAL(1m) SLIDING(30s);

CREATE STREAM streams0 INTO streamt0 AS
SELECT _wstart, count(*), AVG(voltage) FROM meters PARTITION BY tbname EVENT_WINDOW START WITH voltage < 0 END WITH voltage > 9;

CREATE STREAM streams1 IGNORE EXPIRED 1 WATERMARK 100s INTO streamt1 AS
SELECT _wstart, COUNT(*), AVG(voltage) FROM meters PARTITION BY tbname COUNT_WINDOW(10);
```

## View Streams

You can display information about all streams in the current database. The syntax is described as follows:

```sql
SHOW STREAMS;
```

For more detailed information, use the following syntax:

```sql
SELECT * FROM information_schema.`ins_streams`;
```

## Delete a Stream

You can delete streams that are no longer needed. The syntax is described as follows:

```sql
DROP STREAM [IF EXISTS] stream_name;
```

:::note

When you delete a stream, the data written by the stream is retained.

:::

## Supported Functions for Stream Processing

1. All [single-row functions](../functions/#single-row-functions) can be used for stream processing.
2. The following 19 aggregate/select functions cannot be applied in the SQL statements for creating stream processing. Other types of functions can be used in stream processing.
   - [leastsquares](../functions/#leastsquares)
   - [percentile](../functions/#percentile)
   - [top](../functions/#top)
   - [bottom](../functions/#bottom)
   - [elapsed](../functions/#elapsed)
   - [interp](../functions/#interp)
   - [derivative](../functions/#derivative)
   - [irate](../functions/#irate)
   - [twa](../functions/#twa)
   - [histogram](../functions/#histogram)
   - [diff](../functions/#diff)
   - [statecount](../functions/#statecount)
   - [stateduration](../functions/#stateduration)
   - [csum](../functions/#csum)
   - [mavg](../functions/#mavg)
   - [sample](../functions/#sample)
   - [tail](../functions/#tail)
   - [unique](../functions/#unique)
   - [mode](../functions/#mode)

## Pause a Stream

You can pause a stream to stop computing results temporarily. The syntax is described as follows:

```sql
PAUSE STREAM [IF EXISTS] stream_name;
```

## Resume a Stream

You can resume a paused stream to continue computing results. The syntax is described as follows:

```sql
RESUME STREAM [IF EXISTS] [IGNORE UNTREATED] stream_name;
```

If you specify the `IGNORE UNTREATED` parameter, any data ingested while the stream was paused is ignored after the stream has been resumed.

## Back Up and Synchronize State Data

:::info[Version Info]

This section applies to TDengine 3.3.2.1 and later versions.

:::

The intermediate results of stream processing become state data that needs to be persistently saved throughout the lifecycle of the stream processing. To ensure that intermediate states can be reliably synchronized and migrated across different nodes in a cluster environment, perform the following steps:

1. In the TDengine configuration file, configure the IP address and port of the snode. The backup directory is located on the physical node where the snode is deployed.

   ```text
   snodeAddress           127.0.0.1:873

   checkpointBackupDir    /home/user/stream/backup/checkpoint/
   ```

2. Create an snode in TDengine.

```sql
CREATE SNODE ON DNODE <dnode-id>;
```

Only after completing these two steps can streams be created. If an snode is not created and its address is not properly configured, checkpoints cannot be generated during the stream processing, which may lead to errors in subsequent calculation results.

## Delete Intermediate Results

You can specify the `DELETE_MARK` parameter to delete cached window states, removing intermediate results of stream processing. The default value is 10 years.
T = Latest event time - DELETE_MARK
