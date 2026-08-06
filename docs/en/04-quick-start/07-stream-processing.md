---
sidebar_label: Stream Processing
title: Stream Processing
description: Try stream processing quickly with SQL
toc_max_heading_level: 4
---

In time-series processing, real-time aggregation, downsampling, and early alerting usually require continuously processing newly written data. Traditional approaches often deploy Kafka, Flink, and other stream systems, which lengthens development and operations. TDengine includes stream processing so you can define real-time logic in SQL: computation runs automatically after data is written, and results are written to target tables or sent as notifications. Typical scenarios include:

- Real-time aggregation and downsampling: Continuously compute minute- or hour-level metrics to reduce data scanned by later queries.
- Precomputation for reports and dashboards: Generate common statistics ahead of time to cut latency from wide-range queries.
- Anomaly detection and early alerting: Compute key metrics soon after writes so downstream alerting or detection can use them.

This chapter continues with the `test` database meter data written by `taosBenchmark -y` in the quick start, and creates a stream that computes each meter’s average current every 1 minute. You will walk through confirming data, creating the stream, checking the output table, writing new data, and watching results update. The following overview summarizes stream-processing capabilities; for full syntax and advanced topics, follow the links below or see “Further Reading” at the end of this page.

## Stream Processing Capabilities at a Glance

Compared with traditional stream processing, TDengine separates **trigger** from **compute**: the trigger decides when to compute; compute decides which data to use and where results go. They can share the same table or be separated by business need. The new stream processing capability is available from `v3.3.7.0`.

- **Trigger modes**
  Supports periodic (`PERIOD`), sliding (`SLIDING`), time-window (`INTERVAL`), session / state / event / count windows, and more. Triggers can be partitioned, and trigger data can be pre-filtered. See [Stream Syntax](../07-stream-processing/01-syntax.md).

- **Compute and result output**
  Compute can be any query. Results can be written to an output table (`INTO`), sent as notifications (`NOTIFY`), or both. See [Stream Syntax](../07-stream-processing/01-syntax.md).

- **Control options**
  Use `STREAM_OPTIONS` for history replay, out-of-order watermarks, max delay, low-latency compute, and more, balancing result freshness against resource load. See [Stream Syntax](../07-stream-processing/01-syntax.md).

- **Operations and limits**
  Stream tasks run on snodes. Covers high availability, permissions, manual recomputation, and atypical writes such as out-of-order / update / delete. See [Operations and Limits](../07-stream-processing/02-instructions.md).

- **Deployment and design**
  Deployment, configuration, design before creating streams, and typical examples. See [Deployment and Design](../07-stream-processing/03-best-practices.md).

The rest of this chapter starts by creating an `INTERVAL` window stream.

## Prerequisites

Confirm the following:

1. The TDengine service is running and you can connect with the shell.
2. An snode is deployed in the cluster. Stream tasks run on snodes.
3. You have run `taosBenchmark -y` in the Download and Install quick start so the `test` database and `meters` supertable exist. If not, run `taosBenchmark -y` in a terminal first.

You can list snodes in the shell:

```sql
SHOW SNODES;
```

If there is no snode, list dnodes and create an snode on one of them. Replace `1` in the example with the actual dnode ID.

```sql
SHOW DNODES;
CREATE SNODE ON DNODE 1;
```

For more snode deployment guidance, see [Operations and Limits](../07-stream-processing/02-instructions.md#deploy-an-snode).

## Prepare Sample Data

After entering the shell, switch to the `test` database.

```sql
USE test;
```

Confirm that the `meters` supertable already has data.

```sql
SELECT tbname, ts, current, voltage
FROM meters
WHERE voltage > 250 and tbname = 'd1'
ORDER BY ts DESC
LIMIT 5;
```

The result looks similar to the following. Subtable names and values may vary slightly by `taosBenchmark` version or random data.

```text
 tbname |           ts            | current  | voltage |
========================================================
 d1     | 2017-07-14 10:40:09.998 |  11.7984 |     253 |
 d1     | 2017-07-14 10:40:09.998 |  11.7984 |     253 |
 d1     | 2017-07-14 10:40:09.998 |  11.7984 |     253 |
 d1     | 2017-07-14 10:40:09.998 |  11.7984 |     253 |
 d1     | 2017-07-14 10:40:09.998 |  11.7984 |     253 |

Query OK, 5 row(s) in set
```

Timestamps in `test.meters` fall between `2017-07-14 10:40:00.000` and `2017-07-14 10:40:09.999`. With a 1-minute window below, historical data lands in one window; the write examples insert data in the next minute so you can observe a new window.

## Create a Stream

To make the example easy to re-run, clean up any stream and output table with the same names first.

```sql
DROP STREAM IF EXISTS avg_current_stream;
DROP STABLE IF EXISTS avg_current_stb;
```

Run the following SQL to create a stream: one window every 1 minute, average current per subtable, results written to the output supertable `avg_current_stb`. Output subtable names match the source (for example `d0` → `avg_d0`), and the source `groupId` tag is retained.

```sql
CREATE STREAM avg_current_stream
  INTERVAL(1m) SLIDING(1m)
  FROM meters PARTITION BY tbname, groupId
  STREAM_OPTIONS(FILL_HISTORY_FIRST | MAX_DELAY(3s))
  INTO avg_current_stb
  OUTPUT_SUBTABLE(CONCAT("avg_", tbname))
  TAGS (
      groupId INT AS groupId
  )
  AS
    SELECT _twstart AS ts,
           _twend AS window_end,
           AVG(current) AS avg_current
    FROM %%trows;
```

Where:

- `INTERVAL(1m) SLIDING(1m)` triggers computation on 1-minute windows.
- `FROM meters PARTITION BY tbname, groupId` triggers per subtable; including `groupId` in the partition list also writes it as an output tag.
- `STREAM_OPTIONS(FILL_HISTORY_FIRST | MAX_DELAY(3s))` computes already written history first; an open window also triggers about 3 seconds after it opens, which is convenient for a quick demo.
- `INTO avg_current_stb` writes results to the output supertable.
- `OUTPUT_SUBTABLE(CONCAT("avg_", tbname))` builds output subtable names from the source, for example `d0` → `avg_d0`.
- `TAGS (groupId INT AS groupId)` copies the source grouping tag to the output supertable for grouped queries.
- `%%trows` is the set of rows in the current trigger window.

## View the Stream

After creation, list streams in the current database.

```sql
SHOW STREAMS;
```

The result looks similar to:

```text
      stream_name     | status |         message         | db_name |
====================================================================
 avg_current_stream   | Idle   | Current deploy times: 0 | test    |

Query OK, 1 row(s) in set
```

For more detail, query the system table:

```sql
SELECT *
FROM information_schema.ins_streams
WHERE stream_name = 'avg_current_stream';
```

Streams are scheduled asynchronously. Wait a few seconds after creation before querying the output table. With `FILL_HISTORY_FIRST`, history windows are replayed for about 10,000 subtables, so the first computation may take a little longer.

## View Computation Results

Query the output supertable `avg_current_stb`.

```sql
SELECT tbname, groupId, ts, window_end, avg_current
FROM avg_current_stb
ORDER BY ts, tbname
LIMIT 5;
```

Each row is one meter’s average current in a 1-minute window. `tbname` is the output subtable (for example `avg_d0`). Historical data falls in the `10:40:00`–`10:41:00` window, as shown below.

```text
 tbname | groupId |           ts            |       window_end        | avg_current |
=======================================================================================
 avg_d0 |       1 | 2017-07-14 10:40:00.000 | 2017-07-14 10:41:00.000 |   10.208475 |
 avg_d1 |       7 | 2017-07-14 10:40:00.000 | 2017-07-14 10:41:00.000 |   10.208475 |
 avg_d2 |       2 | 2017-07-14 10:40:00.000 | 2017-07-14 10:41:00.000 |   10.208475 |
 avg_d3 |       4 | 2017-07-14 10:40:00.000 | 2017-07-14 10:41:00.000 |   10.208475 |
 avg_d4 |       3 | 2017-07-14 10:40:00.000 | 2017-07-14 10:41:00.000 |   10.208475 |
```

Exact `groupId` values depend on tags assigned by `taosBenchmark` and may differ slightly from the table above.

## Write New Data and Watch Updates

Windows trigger by default when they close. If you only write data in the `10:41:00`–`10:42:00` window, the window is not closed yet; this example sets `MAX_DELAY(3s)`, so a result is still produced about 3 seconds after the window opens.

Insert one row for `d0` inside that window.

```sql
INSERT INTO d0 VALUES ("2017-07-14 10:41:30", 12.4, 221, 147);
```

You can also write a row at the next window start so event time closes the window (this triggers even without `MAX_DELAY`):

```sql
INSERT INTO d0 VALUES ("2017-07-14 10:42:00", 12.5, 220, 147);
```

After a few seconds, query recent stream results for `d0`’s output subtable `avg_d0`.

```sql
SELECT tbname, groupId, ts, window_end, avg_current
FROM avg_current_stb
WHERE tbname = "avg_d0"
ORDER BY ts DESC
LIMIT 3;
```

The result looks similar to the following. The `10:41:00` window now appears in the output table.

```text
 tbname | groupId |           ts            |       window_end        | avg_current |
=======================================================================================
 avg_d0 |       1 | 2017-07-14 10:41:00.000 | 2017-07-14 10:42:00.000 |   12.400000 |
 avg_d0 |       1 | 2017-07-14 10:40:00.000 | 2017-07-14 10:41:00.000 |   10.208475 |
```

The stream keeps running. As long as new data is written to `meters`, windows that meet the trigger conditions continue to compute and write to the output table.

## Clean Up the Example

If you no longer need the stream and output table from this chapter, run:

```sql
DROP STREAM IF EXISTS avg_current_stream;
DROP STABLE IF EXISTS avg_current_stb;
```

## Common Adjustments

At the quick-start stage, keep these common adjustments in mind:

- To process only newly written data, remove `FILL_HISTORY_FIRST` from `STREAM_OPTIONS`.
- To get results sooner before a window closes, keep or tune `MAX_DELAY`; you can also write data in the next window so event time closes the current one.
- For out-of-order writes, updates, or deletes, design streams with `WATERMARK`, recomputation, and best practices.
- To send results to external applications, use `NOTIFY` to create a notification stream.

## Further Reading

This chapter covers only a minimal runnable stream example. For the full stream-processing capabilities, continue with:

- [Stream Processing](../07-stream-processing/index.md): overview of stream processing, scenarios, trigger/compute separation, and capability extensions
- [Stream Syntax](../07-stream-processing/01-syntax.md): `CREATE STREAM`, triggers, result output, control options, and notification syntax
- [Operations and Limits](../07-stream-processing/02-instructions.md): snode, permissions, recomputation, out-of-order writes, and configuration
- [Deployment and Design](../07-stream-processing/03-best-practices.md): deployment, configuration, design before creating streams, and typical examples
- [Data Querying](./05-query-and-aggregate.md): window queries, aggregations, and interpreting results
