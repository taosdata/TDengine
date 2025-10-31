---
title: Stream Processing
slug: /advanced-features/stream-processing
---

In time-series data processing, there are many common stream processing requirements, such as:

- Tiered storage and intelligent downsampling: Industrial equipment may generate tens of thousands of raw data points per second. Storing everything in full causes storage costs to surge, query efficiency to drop, and historical trend analysis to respond slowly.
- Precomputation to accelerate real-time decisions: When users query full datasets, the system may need to scan tens of billions of records, making it nearly impossible to return results in real time. This leads to lag in dashboards and reports.
- Anomaly detection and low-latency alerts: Monitoring and alerting require retrieving specific data with very low latency based on predefined rules. Traditional batch processing often has delays on the order of minutes.

In traditional time-series solutions, Kafka, Flink, and other stream processing systems are often deployed. However, the complexity of these systems brings high development and operations costs. The stream processing engine in TDengine TSDB provides the capability to process incoming data streams in real time. Using SQL, users can define real-time transformations. Once data is written into the source table of a stream, it is automatically processed as defined, and results are pushed to target tables according to the trigger mode. This offers a lightweight alternative to complex stream processing systems, while still delivering millisecond-level result latency even under high-throughput data ingestion. Unlike traditional stream processing, TDengine TSDB adopts a trigger–compute decoupling strategy, still operating on continuous unbounded data streams, but with the following enhancements:

- **Extended processing targets:** In traditional stream processing, the event trigger source and the computation target are usually the same — events and computations are both generated from the same dataset. TDengine TSDB's stream processing allows the trigger source (event driver) and the computation source to be separated. The trigger table and the computation source table can be different, and a trigger table may not be required at all. The processed dataset can vary in terms of columns and time ranges.
- **Extended triggering mechanisms: In addition to the standard “data write” trigger, TDengine TSDB's stream processing supports more trigger modes. With window-based triggers, users can flexibly define and use various windowing strategies to generate trigger events, choosing to trigger on window open, window close, or both. Beyond event-time-driven triggers linked to a trigger table, time-independent triggers are also supported, such as scheduled triggers. Before an event is triggered, TDengine can pre-filter trigger data so that only data meeting certain conditions proceeds to trigger evaluation.
- **Extended computation scope:** Computations can be performed on the trigger table or on other databases and tables. The computation type is unrestricted — any query statement is supported. The application of computation results is flexible: results can be sent as notifications, written to output tables, or both.

TDengine TSDB’s stream processing engine also offers additional usability benefits. For varying requirements on result latency, it allows users to balance between result timeliness and resource load. For different needs in out-of-order write scenarios, it enables users to flexibly choose appropriate handling methods and strategies. This offers a lightweight alternative to complex stream processing systems, while still delivering millisecond-level result latency even under high-throughput data ingestion.

For detailed usage instructions, see [SQL Manual](../14-reference/03-taos-sql/41-stream.md).

## Create a Stream

```sql
CREATE STREAM [IF NOT EXISTS] [db_name.]stream_name options [INTO [db_name.]table_name] [OUTPUT_SUBTABLE(tbname_expr)] [(column_name1, column_name2 [COMPOSITE KEY][, ...])] [TAGS (tag_definition [, ...])] [AS subquery]

options: {
    trigger_type [FROM [db_name.]table_name] [PARTITION BY col1 [, ...]] [STREAM_OPTIONS(stream_option [|...])] [notification_definition]
}
    
trigger_type: {
    PERIOD(period_time[, offset_time])
  | [INTERVAL(interval_val[, interval_offset])] SLIDING(sliding_val[, offset_time]) 
  | SESSION(ts_col, session_val)
  | STATE_WINDOW(col[, extend[, zeroth_state]]) [TRUE_FOR(duration_time)] 
  | EVENT_WINDOW(START WITH start_condition END WITH end_condition) [TRUE_FOR(duration_time)]
  | COUNT_WINDOW(count_val[, sliding_val][, col1[, ...]]) 
}

stream_option: {WATERMARK(duration_time) | EXPIRED_TIME(exp_time) | IGNORE_DISORDER | DELETE_RECALC | DELETE_OUTPUT_TABLE | FILL_HISTORY[(start_time)] | FILL_HISTORY_FIRST[(start_time)] | CALC_NOTIFY_ONLY | LOW_LATENCY_CALC | PRE_FILTER(expr) | FORCE_OUTPUT | MAX_DELAY(delay_time) | EVENT_TYPE(event_types)}

notification_definition:
    NOTIFY(url [, ...]) [ON (event_types)] [WHERE condition] [NOTIFY_OPTIONS(notify_option[|notify_option])]

notify_option: [NOTIFY_HISTORY | ON_FAILURE_PAUSE]
    
event_types:
    event_type [|event_type]    
    
event_type: {WINDOW_OPEN | WINDOW_CLOSE}    

tag_definition:
    tag_name type_name [COMMENT 'string_value'] AS expr
```

### Trigger Methods

- Periodic trigger: drives execution by fixed intervals of system time. The baseline is midnight of the day the stream is created, and subsequent trigger times are determined by the specified interval. A time offset can be applied to adjust the baseline.
- Sliding trigger: drives execution based on a fixed interval of event time for data written to the trigger table. An INTERVAL window can be specified if desired.
- Session window trigger: divides the incoming data written to the trigger table into windows based on session boundaries, and triggers when a window starts and/or closes.
- State window trigger: divides the written data of the trigger table into windows based on the values in a state column. A trigger occurs when a window is opened and/or closed.
- Event window trigger: partitions the incoming data of the trigger table into windows based on defined event start and end conditions, and triggers when the window opens and/or closes.
- Count window trigger: partitions the written data from the trigger table based on a counting window, and triggers when the window starts and/or closes. It supports column-based triggering, where the trigger occurs when the specified columns receive data writes.

### Trigger Actions

After a trigger occurs, different actions can be performed as needed, for example, sending an event notification, executing a computation, or both.

- Notify only, no computation: Send an event notification to an external application via WebSocket.
- Compute only, no notification: Execute any query and store the results in the stream computing output table.
- Both notify and compute: Execute any query and send the computation results or event notifications to an external application at the same time.

### Trigger Table and Grouping

In general, one stream computing task corresponds to a single computation — for example, triggering a computation based on one subtable and storing the result in one output table. Following TDengine’s “one device, one table” design philosophy, if you need to compute results separately for all devices, you would traditionally need to create a separate stream computing task for each subtable. This can be inconvenient to manage and inefficient to process. To address this, TDengine TSDB's stream computing supports trigger grouping. A group is the smallest execution unit in stream computing. Logically, you can think of each group as an independent stream computing task, with its own output table and its own event notifications.

In summary, the number of output tables (subtables or regular tables) produced by a stream computing task equals the number of groups in the trigger table. If no grouping is specified, only one output table (a regular table) is created.

### Stream Tasks

A computation task is the calculation executed by the stream after an event is triggered. It can be any type of query statement, and can operate on the trigger table or on other databases and tables. When performing calculations, you may need to use contextual information from the trigger event. In SQL statements, these are represented as placeholders, which are replaced with constant values at execution time for each calculation. The available options include:

- `_tprev_ts`: event time of previous trigger
- `_tcurrent_ts`: event time of current trigger
- `_tnext_ts`: event time of next trigger
- `_twstart`: start timestamp of current window
- `_twend`: end timestamp of current window
- `_twduration`: duration of current window
- `_twrownum`: number of rows in current window
- `_tprev_localtime`: system time of previous trigger
- `_tnext_localtime`: system time of next trigger
- `_tgrpid`: ID of trigger group
- `_tlocaltime`: system time of current trigger
- `%%n`: reference to the trigger grouping column, where n is the index of the grouping column
- `%%tbname`: reference to trigger table; can be used in queries as `FROM %%tbname`.
- `%%trows`: reference to the trigger dataset for each group in the trigger table (i.e., the set of rows that meet the current trigger condition)

### Control Options

Control options are used to manage trigger and computation behavior. Multiple options can be specified, but the same option cannot be specified more than once. The available options include:

- WATERMARK(duration_time) specifies the tolerance duration for out-of-order data.
- EXPIRED_TIME(exp_time) specifies an expiration interval after which data is ignored.
- IGNORE_DISORDER ignores out-of-order data in the trigger table.
- DELETE_OUTPUT_TABLE ensures that when a subtable in the trigger table is deleted, its corresponding output subtable is also deleted.
- FILL_HISTORY[(start_time)] triggers historical data computation starting from the earliest record.
- FILL_HISTORY_FIRST[(start_time)] triggers historical data computation with priority, starting from start_time (event time).
- CALC_NOTIFY_ONLY sends computation results as notifications only, without saving them to the output table.
- LOW_LATENCY_CALC ensures low-latency computation or notification after each trigger. Processing starts immediately.
- PRE_FILTER(expr) specifies data filtering on the trigger table before evaluation. Only rows that meet the condition will be considered for triggering.
- FORCE_OUTPUT forces an output row even when a trigger produces no computation result.
- MAX_DELAY(delay_time) defines the maximum waiting time (processing time) before a window is forcibly triggered if it has not yet closed.
- EVENT_TYPE(event_types) specifies the types of events that can trigger (open or close) a window.
- IGNORE_NODATA_TRIGGER ignores triggers when the trigger table has no input data.

### Notification Mechanism

Event notifications are optional actions executed after a stream is triggered. Notifications can be sent to applications over the WebSocket protocol. Users specify the events to be notified and the target address for receiving messages. The notification content may include the computation results, or, when no result is produced, only the event-related information.

## Examples

### Count Window Trigger

- Each time one row is written into table tb1, compute the average value of column col1 in table tb2 over the past 5 minutes up to that moment, and write the result into table tb3.

```SQL
CREATE STREAM sm1 COUNT_WINDOW(1) FROM tb1 
  INTO tb3 AS
    SELECT _twstart, avg(col1) FROM tb2 
    WHERE _c0 >= _twend - 5m AND _c0 <= _twend;
```

- Each time 10 rows are written into table tb1 where column col1 is greater than 0, compute the average of column col1 for those 10 rows. The result does not need to be saved but must be sent as a notification to ws://localhost:8080/notify.

```SQL
CREATE STREAM sm2 COUNT_WINDOW(10, 1, col1) FROM tb1 
  STREAM_OPTIONS(CALC_ONTIFY_ONLY | PRE_FILTER(col1 > 0)) 
  NOTIFY("ws://localhost:8080/notify") ON (WINDOW_CLOSE) 
  AS 
    SELECT avg(col1) FROM %%trows;
```

### Event Window Trigger

- When the ambient temperature exceeds 80° and remains above that threshold for more than 10 minutes, compute the average ambient temperature.

```SQL
CREATE STREAM `idmp`.`ana_temp` EVENT_WINDOW(start with `temp` > 80 end with `temp` <= 80 ) TRUE_FOR(10m) FROM `idmp`.`vt_envsens02_471544` 
  STREAM_OPTIONS( IGNORE_DISORDER)
  INTO `idmp`.`ana_temp` 
  AS 
    SELECT _twstart+0s as output_timestamp, avg(`temp`) as `avgtemp` FROM idmp.`vt_engsens02_471544` where ts >= _twstart and ts <= _twend;
```

### Sliding Trigger

- For each subtable of supertable stb1, at the end of every 5-minute time window, compute the average of column col1 over that interval. The results for each subtable are written separately into different subtables of supertable stb2.

```SQL
CREATE STREAM sm1 INTERVAL(5m) SLIDING(5m) FROM stb1 PARTITION BY tbname 
  INTO stb2 
  AS 
    SELECT _twstart, avg(col1) FROM %%tbname 
    WHERE _c0 >= _twstart AND _c0 <= _twend;
```

In the SQL above, `FROM %%tbname WHERE _c0 >= _twstart AND _c0 <= _twend` and `FROM %%trows` are not equivalent. The former means the computation uses data from the trigger group’s corresponding table within the window’s time range; those in-window rows may differ from what `%%trows` saw at trigger time. The latter means the computation uses only the window data captured at the moment of triggering.

- For each subtable of supertable stb1, starting from the earliest data, compute the average of col1 for each 5-minute time window either when the window closes or when 1 minute has elapsed since the window opened and it is still not closed. Write each subtable’s result to a separate subtable under supertable stb2.

```SQL
CREATE STREAM sm2 INTERVAL(5m) SLIDING(5m) FROM stb1 PARTITION BY tbname 
  STREAM_OPTIONS(MAX_DELAY(1m) | FILL_HISTORY_FIRST) 
  INTO stb2 
  AS 
    SELECT _twstart, avg(col1) FROM %%tbname WHERE _c0 >= _twstart AND _c0 <= _twend;
```

- Compute the per-minute average of the meter current, and send notifications to two target addresses when the window opens and closes. Do not send notifications during historical computation, and do not allow notifications to be dropped on delivery failure (pause and retry until delivered).

```sql
CREATE STREAM avg_stream INTERVAL(1m) SLIDING(1m) FROM meters 
  NOTIFY ('ws://localhost:8080/notify', 'wss://192.168.1.1:8080/notify?key=foo') ON ('WINDOW_OPEN', 'WINDOW_CLOSE') NOTIFY_OPTIONS(NOTIFY_HISTORY | ON_FAILURE_PAUSE)
  INTO avg_stb
  AS 
    SELECT _twstart, _twend, AVG(current) FROM %%trows;
```

### Scheduled Trigger

- Every hour, compute the total number of rows in table tb1 and write the result to table tb2 (in a millisecond-precision database).

```SQL
CREATE STREAM sm1 PERIOD(1h) 
  INTO tb2 
  AS
    SELECT cast(_tlocaltime/1000000 AS TIMESTAMP), count(*) FROM tb1;
```

- Every hour, send a notification with the current system time to `ws://localhost:8080/notify`.

```SQL
CREATE STREAM sm1 PERIOD(1h) 
  NOTIFY("ws://localhost:8080/notify");
```

## Other Features

### High Availability

Stream processing in TDengine is architected with a separation of compute and storage, which requires that at least one snode be deployed in the system. Except for data reads, all stream processing functions run exclusively on snodes.

- snode: The node responsible for executing stream processing tasks. A cluster can have one or more snodes.
- Deploying snodes on dedicated dnodes ensures resource separation so that stream processing does not interfere significantly with writes, queries, or other operations.
- To ensure high availability for stream processing, deploy multiple snodes across different physical nodes in the cluster:
  - Stream tasks are load-balanced across multiple snodes.
  - Each pair of snodes acts as replicas, storing stream state and progress information.

### Recomputation

TDengine supports the use of WATERMARK to mitigate issues caused by out-of-order data, updates, and deletions. A WATERMARK is a user-defined duration based on event time that represents the system’s progress in stream processing, reflecting the user’s tolerance for out-of-order data. The current watermark is defined as `latest processed event time – WATERMARK interval`. Only data with event times earlier than the current watermark are eligible for trigger evaluation. Likewise, only windows or other trigger conditions whose time boundaries are earlier than the current watermark will be triggered.

For out-of-order, update, or delete scenarios that exceed the WATERMARK, recalculation is used to ensure the correctness of results. Recalculation means re-triggering and re-executing computations for the data range affected by out-of-order, updated, or deleted records. To make this approach effective, users must ensure that their computation statements and source tables are independent of processing time—that is, the same trigger should produce valid results even if executed multiple times.

Recalculation can be either automatic or manual. If automatic recalculation is not needed, it can be disabled via configuration options.
