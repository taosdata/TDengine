---
sidebar_label: Stream Processing
title: Stream Processing
description: This article describes the SQL statements and syntax related to stream processing.
toc_max_heading_level: 4
slug: /tdengine-reference/sql-manual/manage-streams
---

Compared with traditional stream processing, TDengine TSDB’s stream processing extends both functionality and boundaries. Traditionally, stream processing is defined as a real-time computing paradigm focused on low latency, continuity, and event-time-driven processing of unbounded data streams. TDengine TSDB’s stream processing adopts a trigger–compute decoupling strategy, still operating on continuous unbounded data streams, but with the following enhancements:

- **Extended processing targets:** In traditional stream processing, the event trigger source and the computation target are usually the same — events and computations are both generated from the same dataset. TDengine TSDB's stream processing allows the trigger source (event driver) and the computation source to be separated. The trigger table and the computation source table can be different, and a trigger table may not be required at all. The processed dataset can vary in terms of columns and time ranges.
- **Extended triggering mechanisms: In addition to the standard “data write” trigger, TDengine TSDB's stream processing supports more trigger modes. With window-based triggers, users can flexibly define and use various windowing strategies to generate trigger events, choosing to trigger on window open, window close, or both. Beyond event-time-driven triggers linked to a trigger table, time-independent triggers are also supported, such as scheduled triggers. Before an event is triggered, TDengine can pre-filter trigger data so that only data meeting certain conditions proceeds to trigger evaluation.
- **Extended computation scope:** Computations can be performed on the trigger table or on other databases and tables. The computation type is unrestricted — any query statement is supported. The application of computation results is flexible: results can be sent as notifications, written to output tables, or both.

TDengine TSDB’s stream processing engine also offers additional usability benefits. For varying requirements on result latency, it allows users to balance between result timeliness and resource load. For different needs in out-of-order write scenarios, it enables users to flexibly choose appropriate handling methods and strategies.

**Note:** The new stream processing feature is supported starting from v3.3.7.0.

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
  | STATE_WINDOW(col[, extend]) [TRUE_FOR(duration_time)] 
  | EVENT_WINDOW(START WITH start_condition END WITH end_condition) [TRUE_FOR(duration_time)]
  | COUNT_WINDOW(count_val[, sliding_val][, col1[, ...]]) 
}

stream_option: {WATERMARK(duration_time) | EXPIRED_TIME(exp_time) | IGNORE_DISORDER | DELETE_RECALC | DELETE_OUTPUT_TABLE | FILL_HISTORY[(start_time)] | FILL_HISTORY_FIRST[(start_time)] | CALC_NOTIFY_ONLY | LOW_LATENCY_CALC | PRE_FILTER(expr) | FORCE_OUTPUT | MAX_DELAY(delay_time) | EVENT_TYPE(event_types) | IGNORE_NODATA_TRIGGER}

notification_definition:
    NOTIFY(url [, ...]) [ON (event_types)] [WHERE condition] [NOTIFY_OPTIONS(notify_option[|notify_option])]

notify_option: [NOTIFY_HISTORY | ON_FAILURE_PAUSE]
    
event_types:
    event_type [|event_type]    
    
event_type: {WINDOW_OPEN | WINDOW_CLOSE}    

tag_definition:
    tag_name type_name [COMMENT 'string_value'] AS expr
```

### Trigger Methods in Stream Processing

Event triggers are the driving mechanism for stream processing. The source of an event trigger can vary—it may come from data being written to a table, from the analytical results of computations on a table, or even from no table at all. When the stream processing engine detects that the user-defined trigger conditions are met, it initiates the computation. The number of times the condition is met corresponds exactly to the number of times computation is triggered. The trigger object and the computation object are independent of each other. Users can flexibly define and use various types of windows to generate trigger events, with support for triggering on window open, window close, or both. Group-based triggering is supported, as well as pre-filtering of trigger data so that only data meeting the criteria will be considered for triggering.

#### Trigger Types

The trigger type is specified using trigger_type and includes: scheduled trigger, sliding trigger, sliding window trigger, session window trigger, state window trigger, event window trigger, and count window trigger. When using state windows, event windows, or count windows with a supertable, they must be used together with `partition by tbname`.

##### Scheduled Trigger

```sql
PERIOD(period_time[, offset_time])
```

A scheduled trigger is driven by a fixed interval based on the system time, essentially functioning as a scheduled task. It does not belong to the category of window triggers. Parameter definitions are as follows:

- period_time: The scheduling interval. Supported time units include milliseconds (a), seconds (s), minutes (m), hours (h), and days (d). The supported range is [10a, 3650d].
- offset_time: (Optional) The scheduling offset. Supported units include milliseconds (a), seconds (s), minutes (m), and hours (h). The offset value must be less than 1 day.

Usage Notes:

- When the scheduling interval is less than one day, the base time is calculated as midnight (00:00) plus the scheduling offset. The next trigger time is determined based on this base time and the specified interval. The base time resets to midnight each day. The time between the last trigger of one day and the base time of the next day may be shorter than the scheduling interval. For example:
  - If the scheduling interval is 5 hours 30 minutes, the trigger times for the day will be [00:00, 05:30, 11:00, 16:30, 22:00]. The trigger times for subsequent days will be the same.
  - With the same interval but an offset of 1 minute, the trigger times will be [00:01, 05:31, 11:01, 16:31, 22:01] each day.
  - Under the same conditions, if the stream is created when the system time is 12:00, the trigger times for the current day will be [16:31, 22:01]. From the next day onwards, the trigger times will be [00:01, 05:31, 11:01, 16:31, 22:01].
- When the scheduling interval is greater than or equal to 1 day, the base time is calculated as midnight (00:00) of the current day plus the scheduling offset, and it will not reset on subsequent days. For example:
  - If the scheduling interval is 1 day 1 hour and the stream is created when the system time is 05-01 12:00, the trigger times will be [05-02 01:00, 05-03 02:00, 05-04 03:00, 05-05 04:00, …].
  - Under the same conditions, if the time offset is 1 minute, the trigger times will be [05-02 01:01, 05-03 02:02, 05-04 03:03, 05-05 04:04, …].

Applicable scenarios: Situations requiring scheduled computation driven continuously by system time, such as generating daily statistics every hour, or sending scheduled statistical reports once a day.

##### Sliding Trigger

```sql
SLIDING(sliding_val[, offset_time]) 
```

A sliding trigger drives execution based on a fixed interval of event time for data written to the trigger table. It cannot specify an INTERVAL window and is not considered a window trigger. A trigger table must be specified. The trigger times and time offset rules are the same as for scheduled triggers, with the only difference being that the system time is replaced by event time.

Parameter definitions are as follows:

- sliding_val: Required. The sliding duration based on event time.
- offset_time: Optional. The time offset for the sliding trigger. Supported time units include milliseconds (a), seconds (s), minutes (m), and hours (h).

Usage Notes:

- A trigger table must be specified. When the trigger table is a supertable, grouping by tags or subtables is supported, as well as no grouping.
- Supports sliding triggers after processing and filtering the incoming data (conditional triggers).

Applicable scenarios: Situations where calculations need to be driven continuously and periodically based on event time, such as generating daily statistical data every hour or sending scheduled reports each day.

##### Sliding Window Trigger

```sql
[INTERVAL(interval_val[, interval_offset])] SLIDING(sliding_val) 
```

A sliding window trigger refers to triggering based on incoming data written to the trigger table, using event time and a fixed window size that slides over time. The INTERVAL window must be specified. This is a type of window trigger, and a trigger table must be specified.

The starting point for a sliding window trigger is the beginning of the window. By default, windows are divided starting from Unix time 0 (1970-01-01 00:00:00 UTC). You can change the starting point of the window division by specifying a window time offset. Parameter definitions are as follows:

- interval_val: Optional. The duration of the sliding window.
- interval_offset: Optional. The time offset for the sliding window.
- sliding_val: Required. The sliding duration based on event time.

Usage Notes:

- A trigger table must be specified. When the trigger table is a supertable, grouping by tags or subtables is supported, as well as no grouping.
- Supports conditional sliding window triggers after processing and filtering the incoming data.

Applicable Scenarios: Suitable for event-time-based scheduled window calculations, such as generating hourly statistics for that hour, or calculating data within the last 5-minute window every hour.

##### Session Window Trigger

```sql
SESSION(ts_col, session_val)
```

A session window trigger divides the incoming data written to the trigger table into windows based on session boundaries, and triggers when a window starts and/or closes. Parameter definitions are as follows:

- ts_col: The name of the primary key column.
- session_val: The maximum time gap for records to belong to the same session. Records with a time gap less than or equal to session_val are considered part of the same session.

Usage Notes:

- A trigger table must be specified. When the trigger table is a supertable, grouping by tags or subtables is supported, as well as no grouping.
- Supports conditional window triggering after filtering the written data.

Applicable Scenarios: Suitable for use cases where computations and/or notifications need to be driven by session windows.

##### State Window Trigger

```sql
STATE_WINDOW(col[, extend]) [TRUE_FOR(duration_time)] 
```

A state window trigger divides the written data of the trigger table into windows based on the values in a state column. A trigger occurs when a window is opened and/or closed. Parameter definitions are as follows:

- col: The name of the state column.
- extend (optional): Specifies the extension strategy for the start and end of a window. The optional values are 0 (default), 1, and 2, representing no extension, backward extension, and forward extension respectively.
- duration_time (optional): Specifies the minimum duration of a window. If the duration of a window is shorter than this value, the window will be discarded and no trigger will be generated.

Usage Notes:

- A trigger table must be specified. When the trigger table is a supertable, grouping by tags or subtables is supported, as well as no grouping.
- When used with a supertable, it must be combined with PARTITION BY tbname.
- Supports conditional window triggering after filtering the written data.

Applicable Scenarios: Suitable for use cases where computations and/or notifications need to be driven by state windows.

##### Event Window Trigger

```sql
EVENT_WINDOW(START WITH start_condition END WITH end_condition) [TRUE_FOR(duration_time)]
```

An event window trigger partitions the incoming data of the trigger table into windows based on defined event start and end conditions, and triggers when the window opens and/or closes. Parameter definitions are as follows:

- start_condition: Definition of the event start condition.
- end_condition: Definition of the event end condition.
- duration_time (optional): Specifies the minimum duration of a window. If the duration of a window is shorter than this value, the window will be discarded and no trigger will be generated.

Usage Notes:

- A trigger table must be specified. When the trigger table is a supertable, grouping by tags or subtables is supported, as well as no grouping.
- When used with a supertable, it must be combined with PARTITION BY tbname.
- Supports conditional window triggering after filtering the written data.

Applicable Scenarios: Suitable for use cases where computations and/or notifications need to be driven by event windows.

##### Count Window Trigger

```sql
COUNT_WINDOW(count_val[, sliding_val][, col1[, ...]]) 
```

A count window trigger partitions the written data from the trigger table based on a counting window, and triggers when the window starts and/or closes. It supports column-based triggering, where the trigger occurs only when the specified columns receive data writes. Parameter definitions are as follows:

- count_val: The number of rows in the window. The trigger fires when the number of written rows reaches count_val. The minimum value is 1.
- sliding_val (optional): The number of rows by which the window slides.
- col1 [, ...] (optional): The list of trigger columns for column-based triggering. Only regular columns are supported. A row is considered valid if any column in the list has a non-null value. NULL values are treated as invalid.

Usage Notes:

- A trigger table must be specified. When the trigger table is a supertable, grouping by tags or subtables is supported, as well as no grouping.
- When used with a supertable, it must be combined with PARTITION BY tbname.
- Supports conditional window triggering after filtering the written data.

Applicable Scenarios:

- When each individual data entry needs to be processed, such as fault data writes or sampling data writes.
- When processing is required based on specific values in certain columns, such as abnormal value writes
- When data needs to be processed in batches, for example, calculating the average voltage for every 1,000 rows of voltage data.

#### Trigger Actions

After a trigger is activated, different actions can be performed as needed, such as sending an event notification, executing a computation task, or performing both simultaneously.

- Notify only, no computation: Send an event notification to an external application via WebSocket.
- Compute only, no notification: Execute any query and store the results in the stream computing output table.
- Both notify and compute: Execute any query and send the computation results or event notifications to an external application at the same time.

#### Trigger Table and Grouping

In general, one stream computing task corresponds to a single computation — for example, triggering a computation based on one subtable and storing the result in one output table. Following TDengine’s “one device, one table” design philosophy, if you need to compute results separately for all devices, you would traditionally need to create a separate stream computing task for each subtable. This can be inconvenient to manage and inefficient to process. To address this, TDengine TSDB's stream computing supports trigger grouping. A group is the smallest execution unit in stream computing. Logically, you can think of each group as an independent stream computing task, with its own output table and its own event notifications. If no group is specified, or if no trigger table is specified (allowed in the case of scheduled triggers), the entire stream computing task will produce only a single computation — effectively meaning there is only one group, which corresponds to a single output table and a single notification. Since each group operates as an independent stream computing task, their computation progress, output frequency, and other behaviors can differ from one another.

In summary, the number of output tables (subtables or regular tables) produced by a stream computing task equals the number of groups in the trigger table. If no grouping is specified, only one output table (a regular table) is created. The currently supported combinations of trigger types and grouping are as follows:

| Trigger Mechanism                      | Supported Grouping      |
| -------------------------------------- | ----------------------- |
| PERIOD, SLIDING, INTERVAL, and SESSION | Subtable, tag, and none |
| Other                                  | Subtable                |

##### Trigger Tables

A trigger table can be a regular table, supertable, subtable, or virtual table. System tables, views, and queries are not supported. Except for periodic triggers, which can omit specifying a trigger table, all other trigger types must specify one.

```sql
[FROM [db_name.]table_name]
```

##### Trigger Grouping

Specifies the columns used for trigger grouping. Multiple columns are supported, but currently only grouping by subtables and tags is supported.

```sql
[PARTITION BY col1 [, ...]]
```

### Stream Processing Output

By default, the results of a stream are stored in an output table. Each output table contains only the results that have been triggered and computed up to the current time. You can define the structure of the output table, and if grouping is used, you can also specify the tag values for each subtable.

```sql
[INTO [db_name.]table_name] [OUTPUT_SUBTABLE(tbname_expr)] [(column_name1, column_name2 [COMPOSITE KEY][, ...])] [TAGS (tag_definition [, ...])] 

tag_definition:
    tag_name type_name [COMMENT 'string_value'] AS expr
```

Details are as follows:

- INTO [db_name.]table_name: Optional. Specifies the output table name as table_name and the database name as db_name.
  - If trigger grouping is used, this table will be a supertable.
  - If no trigger grouping is used, this table will be a regular table.
  - If the trigger only sends notifications without computation, or if computation results are only sent as notifications without being stored, this option does not need to be specified.
- [OUTPUT_SUBTABLE(tbname_expr)]: Optional. Specifies the name of the calculation output table (subtable) for each trigger group. This cannot be specified if there is no trigger grouping. If not specified, a unique output table (subtable) name will be automatically generated for each group. tbname_expr can be any output string expression, and may include trigger group partition columns (from [PARTITION BY col1[, ...]]). The output length must not exceed the maximum table name length; if it does, it will be truncated. If you do not want different groups to output to the same subtable, you must ensure each group's output table name is unique.
- [(column_name1, column_name2 [COMPOSITE KEY][, ...])]: Optional. Specifies the column names for each column in the output table. If not specified, each column name will be the same as the corresponding column name in the calculation result. You can use [COMPOSITE KEY] to indicate that the second column is a primary key column, forming a composite primary key together with the first column.
- [TAGS (tag_definition [, ...])]: Optional. Specifies the list of tag column definitions and values for the output supertable. This can only be specified if trigger grouping is present. If not specified, the tag column definitions and values are derived from all grouping columns, and in this case, grouping columns cannot have duplicate names. When grouping by subtable, the default generated tag column name is tag_tbname, with the type VARCHAR(270). The tag_definition parameters are as follows:
  - tag_name: Name of the tag column.
  - type_name: Data type of the tag column.
  - string_value: Description of the tag column.
  - expr: Tag value calculation expression, which can use any trigger table grouping columns (from [PARTITION BY col1[, ...]]).

### Stream Processing Computation Tasks

```sql
[AS subquery]
```

A computation task is the calculation executed by the stream after an event is triggered. It can be any type of query statement, and can operate on the trigger table or on other databases and tables. Computation tasks are highly flexible and should be carefully designed before creating the stream. Notes:

- The first column in the query output will serve as the primary key column of the output table: The first column in the query output must be a valid primary key value (TIMESTAMP). If the column type does not match, an error will occur when creating the stream. If a NULL value appears during execution, the corresponding computation result will be discarded.
- Each trigger group’s computation results are written into the same output table (subtable or regular table) for that group: If the query also contains a GROUP BY clause, records with the same primary key in the grouped results will overwrite each other. If grouping is required, it is recommended to define a composite primary key for the output table.

##### Placeholders

When performing calculations, you may need to use contextual information from the trigger event. In SQL statements, these are represented as placeholders, which are replaced with constant values at execution time for each calculation. Placeholders include:

| Trigger Type      | Placeholder      | Description                                                  |
| ----------------- | ---------------- | ------------------------------------------------------------ |
| Scheduled Trigger | _tprev_localtime | System time of previous trigger (nanosecond precision)       |
| Scheduled Trigger | _tnext_localtime | System time of next trigger (nanosecond precision)           |
| Sliding Trigger   | _tprev_ts        | Event time of previous trigger (same precision as record)    |
| Sliding Trigger   | _tcurrent_ts     | Event time of current trigger (same precision as record)     |
| Sliding Trigger   | _tnext_ts        | Event time of next trigger (same precision as record)        |
| Window Trigger    | _twstart         | Start timestamp of current window                            |
| Window Trigger    | _twend           | End timestamp of currently open window. Used only with WINDOW_CLOSE trigger. |
| Window Trigger    | _twduration      | Duration of currently open window. Used only with WINDOW_CLOSE trigger. |
| Window Trigger    | _twrownum        | Number of rows in currently open window. Used only with WINDOW_CLOSE trigger. |
| All               | _tgrpid          | ID of trigger group (data type BIGINT)                       |
| All               | _tlocaltime      | System time of current trigger (nanosecond precision)        |
| All               | %%n              | Reference to trigger group column<br/>n is the column number in `[PARTITION BY col1[, ...]]`, starting with 1 |
| All               | %%tbname         | Reference to trigger table<br/>Only used with the trigger group contains tbname.<br/>Can be used in queries as `FROM %%tbname` |
| All               | %%trows          | Reference to the trigger dataset of each group in the trigger table (the dataset that satisfies the current trigger).<br/>For scheduled triggers, this refers to the data written to the trigger table between the last and current trigger.<br/>Can only be used as a query table name (FROM %%trows).<br/>Applicable only for WINDOW_CLOSE triggers.<br/>Recommended for use in small data volume scenarios. |

Usage Restrictions:

- %%trows: Can only be used in the FROM clause. Queries that use %%trows do not support WHERE condition filtering or join operations on %%trows.
- %%tbname: Can be used in the FROM, SELECT, and WHERE clauses.
- Other placeholders: Can only be used in the SELECT and WHERE clauses.

### Stream Processing Control Options

```sql
[STREAM_OPTIONS(stream_option [|...])]

stream_option: {WATERMARK(duration_time) | EXPIRED_TIME(exp_time) | IGNORE_DISORDER | DELETE_RECALC | DELETE_OUTPUT_TABLE | FILL_HISTORY[(start_time)] | FILL_HISTORY_FIRST[(start_time)] | CALC_NOTIFY_ONLY | LOW_LATENCY_CALC | PRE_FILTER(expr) | FORCE_OUTPUT | MAX_DELAY(delay_time) | EVENT_TYPE(event_types) | IGNORE_NODATA_TRIGGER}
```

Control options are used to manage trigger and computation behavior. Multiple options can be specified, but the same option cannot be specified more than once. The available options include:

- WATERMARK(duration_time) specifies the tolerance duration for out-of-order data. Data arriving later than this duration is treated as out-of-order and processed according to the out-of-order handling strategy of the trigger type and user configuration. Default: duration_time = 0 (no tolerance).
- EXPIRED_TIME(exp_time) specifies an expiration interval after which data is ignored. If not set, no data is considered expired. This option can be used when data writes or updates older than a certain time range are irrelevant. exp_time defines the expiration interval. Supported time units: milliseconds (a), seconds (s), minutes (m), hours (h), days (d).
- IGNORE_DISORDER ignores out-of-order data in the trigger table. By default, out-of-order data is not ignored. This option is useful in scenarios where timeliness of computation or notification is more important, and where out-of-order data does not affect the result. Out-of-order data includes both newly written late data and updates to previously written data.
- DELETE_RECALC specifies that data deletions in the trigger table (including when a child table is dropped) should trigger automatic recomputation. This can only be set if the trigger type supports automatic recomputation for deletions. By default, deletions are ignored. This is only needed when data deletions in the trigger table may affect computation results.
- DELETE_OUTPUT_TABLE ensures that when a subtable in the trigger table is deleted, its corresponding output subtable is also deleted. Applies only to scenarios grouped by subtables. Default: If not specified, deleting a subtable does not delete its output subtable.
- FILL_HISTORY[(start_time)] triggers historical data computation starting from start_time (event time). Default: If not specified, computation starts from the earliest record. If neither FILL_HISTORY nor FILL_HISTORY_FIRST is specified, historical computation is disabled. Cannot be used together with FILL_HISTORY_FIRST. Not supported in PERIOD (scheduled trigger) mode.
- FILL_HISTORY_FIRST[(start_time)] triggers historical data computation with priority, starting from start_time (event time). Default: If not specified, computation starts from the earliest record. Suitable when historical data must be processed strictly in time order, and real-time computation should not begin until historical processing is complete. Cannot be used together with FILL_HISTORY. Not supported in PERIOD (scheduled trigger) mode.
- CALC_NOTIFY_ONLY sends computation results as notifications only, without saving them to the output table. Default: If not specified, results are saved to the output table.
- LOW_LATENCY_CALC ensures low-latency computation or notification after each trigger. Processing starts immediately. This option guarantees the timeliness of real-time stream processing, but at the cost of lower efficiency and potentially higher resource usage. It is recommended only for workloads with strict real-time requirements. By default, if not specified, computation may be deferred and performed in batches to achieve better resource efficiency.
- PRE_FILTER(expr) specifies data filtering on the trigger table before evaluation. Only rows that meet the condition will be considered for triggering. The expr can include columns, tags, constants, and scalar or logical operations. Example: `col1 > 0` ensures that only rows where col1 is positive will be evaluated. Default: If not specified, no pre-filtering is applied.
- FORCE_OUTPUT forces an output row even when a trigger produces no computation result. In this case, all columns except constants (including constant-treated columns) will be set to NULL. More fill strategies will be added in future releases.
- MAX_DELAY(delay_time) defines the maximum waiting time (processing time) before a window is forcibly triggered if it has not yet closed. Starting from the time a window opens, a trigger will be generated at every interval of delay_time if the window remains open. Non-window triggers automatically ignore this option. If a TRUE_FOR condition is specified and its duration is greater than MAX_DELAY, the MAX_DELAY setting still applies even if the window ultimately does not satisfy the TRUE_FOR condition. delay_time supports the following units: seconds (s), minutes (m), hours (h), days (d). Minimum value: 3 seconds, with an accuracy tolerance of about 1 second. If computation time exceeds delay_time, intermediate MAX_DELAY triggers are skipped. Note: WATERMARK is evaluated before window determination. This may cause cases where MAX_DELAY is set but no trigger occurs, because the window never actually opened.
- EVENT_TYPE(event_types) specifies the types of events that can trigger a window. Multiple types may be selected. Default: WINDOW_CLOSE. Not applicable for sliding triggers without INTERVAL or for periodic (PERIOD) triggers (automatically ignored). Options:
  - WINDOW_OPEN: Window start event.
  - WINDOW_CLOSE: Window close event.
- IGNORE_NODATA_TRIGGER ignores triggers when the trigger table has no input data. Applicable for sliding (SLIDING), sliding window (INTERVAL), and periodic (PERIOD) triggers.
  - Sliding and periodic triggers: If there is no data between two trigger times, the trigger is ignored.
  - Sliding window triggers: If no data exists in the window, the trigger is ignored.
  - Default: If not specified, triggers will occur even when no input data is present.

### Notification Mechanism in Stream Processing

Event notifications are optional actions executed after a stream is triggered. Notifications can be sent to applications over the WebSocket protocol. Users define notifications through a notification_definition, which specifies the events to be notified and the target address for receiving messages. The notification content may include the computation results, or, when no result is produced, only the event-related information.

```sql
[notification_definition]

notification_definition:
    NOTIFY(url [, ...]) [ON (event_types)] [WHERE condition] [NOTIFY_OPTIONS(notify_option[|notify_option])]

event_types:
    event_type [|event_type]    
    
event_type: {WINDOW_OPEN | WINDOW_CLOSE | ON_TIME}   
```

Details:

- url [, ...]: Specifies the target address(es) for notifications. Each URL must include the protocol, IP or domain, and port; it may also include a path and query parameters. Enclose the entire URL in quotes. Currently, only the WebSocket protocol is supported. Examples: `ws://localhost:8080`, `ws://localhost:8080/notify`, `ws://localhost:8080/notify?key=foo`.
- [ON (event_types)]: Specifies the event types to notify; multiple values are allowed. For SLIDING (without INTERVAL) and PERIOD triggers, this clause is not required; for other trigger types, it is mandatory. Supported event types:
  - WINDOW_OPEN: Window open event; sent when a group window in the trigger table opens.
  - WINDOW_CLOSE: Window close event; sent when a group window in the trigger table closes.
  - ON_TIME: Scheduled trigger event; sent at the trigger time.
- [WHERE condition]: Specifies a condition that must be met for a notification to be sent. The condition may reference only columns from the computation result and/or constants.
- [NOTIFY_OPTIONS(notify_option[|notify_option])]: Optional. Specifies one or more options to control notification behavior (use | to combine). Supported options:
  - NOTIFY_HISTORY: Send notifications during historical computation. Default: not sent.
  - ON_FAILURE_PAUSE: If sending to the target address fails, pause the stream task and retry in a loop until the notification is successfully delivered; the task then resumes. Default (when not specified): drop the notification (stream computation continues).

When a specified event is triggered, taosd sends a POST request to the configured URL. The message body is in JSON format. A single request may contain events from multiple streams, and the event types may vary.
The event information included depends on the window type:

- Time window: on open: start time; on close: start time, end time, computation result.
- State window: on open: start time, previous state value, current state value; on close: start time, end time, computation result, current state value, next state value.
- Session window: on open: start time; on close: start time, end time, computation result.
- Event window: on open: start time, triggering data value(s), and condition ID(s); on close: start time, end time, computation result, closing data value(s), and condition ID(s).
- Count window: on open: start time; on close: start time, end time, computation result.

An example structure of a notification message is shown below:

```json
{
  "messageId": "unique-message-id-12345",
  "timestamp": 1733284887203,
  "streams": [
    {
      "streamName": "avg_current_stream",
      "events": [
        {
          "tableName": "t_a667a16127d3b5a18988e32f3e76cd30",
          "eventType": "WINDOW_OPEN",
          "eventTime": 1733284887097,
          "triggerId": "window-id-67890",
          "triggerType": "Interval",
          "groupId": "2650968222368530754",
          "windowStart": 1733284800000
        },
        {
          "tableName": "t_a667a16127d3b5a18988e32f3e76cd30",
          "eventType": "WINDOW_CLOSE",
          "eventTime": 1733284887197,
          "triggerId": "window-id-67890",
          "triggerType": "Interval",
          "groupId": "2650968222368530754",
          "windowStart": 1733284800000,
          "windowEnd": 1733284860000,
          "result": {
            "_wstart": 1733284800000,
            "avg(current)": 1.3
          }
        }
      ]
    },
    {
      "streamName": "max_voltage_stream",
      "events": [
        {
          "tableName": "t_96f62b752f36e9b16dc969fe45363748",
          "eventType": "WINDOW_OPEN",
          "eventTime": 1733284887231,
          "triggerId": "window-id-13579",
          "triggerType": "Event",
          "groupId": "7533998559487590581",
          "windowStart": 1733284800000,
          "triggerCondition": {
            "conditionIndex": 0,
            "fieldValue": {
              "c1": 10,
              "c2": 15
            }
          },
        },
        {
          "tableName": "t_96f62b752f36e9b16dc969fe45363748",
          "eventType": "WINDOW_CLOSE",
          "eventTime": 1733284887231,
          "triggerId": "window-id-13579",
          "triggerType": "Event",
          "groupId": "7533998559487590581",
          "windowStart": 1733284800000,
          "windowEnd": 1733284810000,
          "triggerCondition": {
            "conditionIndex": 1,
            "fieldValue": {
              "c1": 20,
              "c2": 3
            }
          },
          "result": {
            "_wstart": 1733284800000,
            "max(voltage)": 220
          }
        }
      ]
    }
  ]
}
```

The following sections describe each field in the notification message.

##### Root-Level Fields

- messageId: String. A unique identifier for the notification message, used to ensure the message can be tracked and de-duplicated.
- timestamp: Long integer. The time the notification message was generated, in milliseconds since 00:00, Jan 1 1970 UTC.
- streams: Array of objects. Contains event information for one or more stream tasks. (See the next section for details.)

##### Fields of the stream Object

- streamName: String. The name of the stream task, used to identify which stream the event belongs to.
- events: Array of objects. The list of events under this stream task, containing one or more event objects. (See the next section for details.)

##### Fields of the event Object

###### Common Fields

These fields are shared by all event objects:

- tableName: String. The name of the target child table associated with the event. When there is no output, this field does not exist. 
- eventType: String. The type of event. Supported values are WINDOW_OPEN, WINDOW_CLOSE, and WINDOW_INVALIDATION.
- eventTime: Long integer. The time the event was generated, in milliseconds since 00:00, Jan 1 1970 UTC.
- triggerId: String. A unique identifier for the trigger event. Ensures that open and close events (if both exist) share the same ID, allowing external systems to correlate them. If taosd crashes and restarts, some events may be resent, but the same event will always retain the same triggerId.
- triggerType: String. The type of trigger. Supported values include the two non-window types Period and SLIDING, as well as the five window types INTERVAL, State, Session, Event, and Count.
- groupId: String. The unique identifier of the group to which the event belongs. If the grouping is by child table, this matches the UID of the corresponding table. When there is no grouping, this field is 0.

###### Fields for Scheduled Triggers

These fields apply when triggerType is Period.

- eventType: Always ON_TIME.
  - result: The computation result, expressed as key–value pairs containing the names of the result columns and their corresponding values.

###### Fields for Sliding Triggers

These fields apply when triggerType is Sliding.

- eventType: Always ON_TIME.
  - result: The computation result, expressed as key–value pairs containing the names of the result columns and their corresponding values.

###### Fields for Time Windows (Interval)

These fields apply when triggerType is Interval.

- If eventType = WINDOW_OPEN, the event object includes:
  - windowStart: Long integer timestamp indicating the window’s start time. Precision matches the time precision of the result table.
- If eventType = WINDOW_CLOSE, the event object includes:
  - windowStart: Long integer timestamp indicating the window’s start time. Precision matches the time precision of the result table.
  - windowEnd: Long integer timestamp indicating the window’s end time. Precision matches the time precision of the result table.

###### Fields for State Windows

These fields apply only when triggerType is State.

- If eventType = WINDOW_OPEN, the event object includes:
  - windowStart: Long integer timestamp indicating the window’s start time. Precision matches the time precision of the result table.
  - prevState: Same type as the state column. Represents the state value of the previous window, or NULL if there is no previous window (i.e., this is the first window).
  - curState: Same type as the state column. Represents the state value of the current window.
- If eventType = WINDOW_CLOSE, the event object includes:
  - windowStart: Long integer timestamp indicating the window’s start time. Precision matches the time precision of the result table.
  - windowEnd: Long integer timestamp indicating the window’s end time. Precision matches the time precision of the result table.
  - curState: Same type as the state column. Represents the state value of the current window.
  - nextState: Same type as the state column. Represents the state value of the next window.
  - result: The computation result, expressed as key–value pairs containing the names of the result columns and their corresponding values.

###### Fields for Session Windows

These fields apply only when triggerType is Session.

- If eventType = WINDOW_OPEN, the event object includes:
  - windowStart: Long integer timestamp indicating the window’s start time. Precision matches the time precision of the result table.
- If eventType = WINDOW_CLOSE, the event object includes:
  - windowStart: Long integer timestamp indicating the window’s start time. Precision matches the time precision of the result table.
  - windowEnd: Long integer timestamp indicating the window’s end time. Precision matches the time precision of the result table.
  - result: The computation result, expressed as key–value pairs containing the names of the result columns and their corresponding values.

###### Fields for Event Windows

These fields apply only when triggerType is Event.

- If eventType = WINDOW_OPEN, the event object includes:
- windowStart: Long integer timestamp indicating the window’s start time. Precision matches the time precision of the result table.
- triggerCondition: Information about the condition that opened the window, including:
  - conditionIndex: Integer. The index of the condition that triggered the window open, starting from 0.
  - fieldValue: Key–value pairs containing the condition column names and their corresponding values.
- If eventType = WINDOW_CLOSE, the event object includes:
  - windowStart: Long integer timestamp indicating the window’s start time. Precision matches the time precision of the result table.
  - windowEnd: Long integer timestamp indicating the window’s end time. Precision matches the time precision of the result table.
  - triggerCondition: Information about the condition that closed the window, including:
    - conditionIndex: Integer. The index of the condition that triggered the window close, starting from 0.
    - fieldValue: Key–value pairs containing the condition column names and their corresponding values.
  - result: The computation result, expressed as key–value pairs containing the names of the result columns and their corresponding values.

###### Fields for Count Windows

These fields apply only when triggerType is Count.

- If eventType = WINDOW_OPEN, the event object includes:
  - windowStart: Long integer timestamp indicating the window’s start time. Precision matches the time precision of the result table.
- If eventType = WINDOW_CLOSE, the event object includes:
  - windowStart: Long integer timestamp indicating the window’s start time. Precision matches the time precision of the result table.
  - result: The computation result, expressed as key–value pairs containing the names of the result columns and their corresponding values.

###### Fields for Window Invalidation

During stream processing, out-of-order data, updates, or deletions may cause an already generated window to be removed or require its results to be recalculated. In such cases, a WINDOW_INVALIDATION notification is sent to the target address to indicate which windows have been deleted.

These fields apply only when eventType is WINDOW_INVALIDATION.

- windowStart: Long integer timestamp indicating the window’s start time. Precision matches the time precision of the result table.
- windowEnd: Long integer timestamp indicating the window’s end time. Precision matches the time precision of the result table.

## Delete a Stream

This operation deletes only the stream processing task. Data written by the stream processing task will not be deleted.

```sql
DROP STREAM [IF EXISTS] [db_name.]stream_name;
```

## View Streams

##### View Stream Information

Displays the stream processing tasks in the current database or in a specified database.

```sql
SHOW [db_name.]STREAMS;
```

For more detailed information, query the system table `information_schema.ins_streams`:

```sql
SELECT * from information_schema.`ins_streams`;
```

##### View Stream Tasks

When a stream is running, it is executed as multiple tasks. Detailed task information can be obtained from the system table `information_schema.ins_stream_tasks`:

```sql
SELECT * from information_schema.`ins_stream_tasks`;
```

## Start or Stop a Stream

##### Start a Stream

```sql
START STREAM [IF EXISTS] [IGNORE UNTREATED] [db_name.]stream_name; 
```

Notes:

- If IF EXISTS is not specified and the stream does not exist, an error is returned; if the stream exists, the stream processing is started.
- If IF EXISTS is specified and the stream does not exist, the operation returns success; if the stream exists, the stream processing is started.
- After a stream is created, it starts automatically. Manual start is only required if the stream has been stopped and needs to be resumed.
- When a stream is started, any data written during the time the stream was stopped is processed as historical data.

##### Stop a Stream

```sql
STOP STREAM [IF EXISTS] [db_name.]stream_name; 
```

Notes:

- If IF EXISTS is not specified and the stream does not exist, an error is returned; if the stream exists, the stream processing is stopped.
- If IF EXISTS is specified and the stream does not exist, the operation returns success; if the stream exists, the stream processing is stopped.
- The stop operation is persistent. The stream will not resume until the user explicitly restarts it.

## Other Features and Notes

### High Availability

Stream processing in TDengine is architected with a separation of compute and storage, which requires that at least one snode be deployed in the system. Except for data reads, all stream processing functions run exclusively on snodes.

- snode: The node responsible for executing stream processing tasks. A cluster can have one or more snodes (at least one is required). Each dnode can host at most one snode, and each snode has multiple execution threads.
- An snode can be deployed on the same dnode as other node types (vnode/mnode). However, for better resource isolation, it is strongly recommended to deploy snodes on dedicated dnodes. This ensures resource separation so that stream processing does not interfere significantly with writes, queries, or other operations.
- To ensure high availability for stream processing, it is recommended to deploy multiple snodes across different physical nodes in the cluster:
  - Stream tasks are load-balanced across multiple snodes.
  - Each pair of snodes acts as replicas, storing stream state and progress information.
  - If only a single snode is deployed in the cluster, the system cannot guarantee high availability.

##### Deploy an Snode

Before creating a stream processing task, an snode must be deployed. The syntax is as follows:

```sql
CREATE SNODE ON DNODE dnode_id;
```

##### View Snodes

You can view information about snodes with the following command:

```sql
SHOW SNODES;
```

For more detailed information, use:

```sql
SELECT * FROM information_schema.`ins_snodes`;
```

##### Delete an Snode

When you delete an snode, both the snode and its replica must be online to synchronize stream state information. If either the snode or its replica is offline, the deletion will fail.

```sql
DROP SNODE ON DNODE dnode_id;
```

### Permission Control

Permission control for stream processing is tied only to database-level permissions. Since each stream may be associated with multiple databases, the requirements are as follows:

| Associated Database                  | Count     | Auth Action                                       | Required Permission |
| ------------------------------------ | --------- | ------------------------------------------------- | ------------------- |
| Database where the stream is defined | 1         | Create, delete, stop, start, manual recomputation | Write               |
| Database of the trigger table        | 1         | Create                                            | Read                |
| Database of the output table         | 1         | Create                                            | Write               |
| Databases of the computation sources | 1 or more | Create                                            | Read                |

### Recomputation

Most TDengine TSDB window types are associated with primary key columns. For example, event windows rely on data ordered by primary key to determine when to open and close a window. When using window-based triggers, it is important that trigger table data be written in an orderly fashion, as this ensures the highest efficiency in stream processing. If out-of-order data is written, it may affect the correctness of results for windows that have already been triggered. Similarly, updates and deletions can also compromise result correctness.

TDengine supports the use of WATERMARK to mitigate issues caused by out-of-order data, updates, and deletions. A WATERMARK is a user-defined duration based on event time that represents the system’s progress in stream processing, reflecting the user’s tolerance for out-of-order data. The current watermark is defined as `latest processed event time – WATERMARK interval`. Only data with event times earlier than the current watermark are eligible for trigger evaluation. Likewise, only windows or other trigger conditions whose time boundaries are earlier than the current watermark will be triggered. Note: WATERMARK does not apply to PERIOD (scheduled) triggers. In PERIOD mode, no recalculation is performed.

For out-of-order, update, or delete scenarios that exceed the WATERMARK, recalculation is used to ensure the correctness of results. Recalculation means re-triggering and re-executing computations for the data range affected by out-of-order, updated, or deleted records. The results already written to the output table are not deleted; instead, new results are written again. To make this approach effective, users must ensure that their computation statements and source tables are independent of processing time—that is, the same trigger should produce valid results even if executed multiple times.

Recalculation can be either automatic or manual. If automatic recalculation is not needed, it can be disabled via configuration options.

##### Manual Recalculation

Manual recalculation must be explicitly initiated by the user and can be started with an SQL command when needed.

```sql
RECALCULATE STREAM [db_name.]stream_name FROM start_time [TO end_time];
```

Notes:

- You can specify a time range (based on event time) for which the stream should be recalculated. If no end time (end_time) is specified, the recalculation range extends from the given start time (start_time) up to the stream’s current processing progress at the moment the manual recalculation is initiated.
- Manual recalculation is not supported for scheduled triggers (PERIOD) but is supported for all other trigger types.
- For count window triggers, both the start time and end time must be specified. Recalculation applies only to intervals that the stream has already processed. If the specified range includes intervals that the stream has not yet begun processing, those portions are automatically ignored. During recalculation, trigger windows are re-partitioned within the specified interval. This may cause misalignment between the new windows and those computed previously. As a result, users may need to manually delete the existing results for that interval from the output table to avoid duplicate results. Similarly, if no end time is specified, the recalculation request will be ignored. For scenarios that require recalculation from a certain start time with no defined end, the recommended approach is to drop the stream, recreate it, and specify FILL_HISTORY_FIRST.

### Atypical Data Ingestion Scenarios

#### Out-of-Order Data

Out-of-order data refers to records written to the trigger table in a non-sequential order. While the computation itself does not depend on whether the source table is ordered, users must ensure—based on business requirements—that the source table’s data is fully written before a trigger occurs. The impact of out-of-order data and how it is handled vary depending on the trigger type.

| Trigger Type                                                 | Impact and Handling                                          |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| Periodic trigger<br/>Sliding trigger<br/>Count window trigger | Ignored; no processing performed.                            |
| Other window triggers                                        | Default: Handled through recalculation.<br/>Optional: Ignored; no processing performed. |

#### Data Updates

Data updates refer to multiple writes of records with the same timestamp, where other column values may or may not change. Update operations affect only the trigger table and the triggering behavior—they do not directly affect the computation process itself. The impact of data updates and how they are handled vary depending on the trigger type.

| Trigger Type                                                 | Impact and Handling                                          |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| Periodic trigger<br/>Sliding trigger<br/>Count window trigger | Ignored; no processing performed.                            |
| Other window triggers                                        | Treated as out-of-order data and handled through recalculation. |

#### Data Deletions

Data deletions affect only the trigger table and the triggering behavior—they do not directly impact the computation process itself. The impact of data deletions and how they are handled vary depending on the trigger type.

| Trigger Type                                                 | Impact and Handling                                          |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| Periodic trigger<br/>Sliding trigger<br/>Count window trigger | Ignored; no processing performed.                            |
| Other window triggers                                        | Default: Ignored; no processing performed.<br/>Optional: Treated as out-of-order data and handled through recalculation. |

#### Expired Data

The expired_time setting defines a data expiration interval. For each group generated by a stream trigger, the system determines whether new data is expired by comparing the event time of the latest data against the expiration threshold. The threshold is calculated as:`latest event time – expired_time`. All data earlier than this threshold is treated as expired.

- Expired data applies only to real-time data in the trigger table. Historical data and data from other tables do not have the concept of expiration. Expiration is evaluated at the time of stream triggering, and whether data is expired depends on when it is written. Data written in event-time order will never be expired; only out-of-order data may be considered expired.
- Expired data does not automatically trigger new computation or recalculation. This means that under all trigger types, expired data is ignored (not computed or recalculated). If no time ranges need to be excluded from computation or recalculation, you do not need to specify expired_time. If expired data is defined but you still want to compute or recalculate over part of it, you can use manual recalculation.
- Expired data affects only whether automatic triggers occur. It does not affect the computation range itself. Therefore, if a trigger’s computation range includes expired data in the trigger table, that data will still be used in the calculation.

### Database and Table Operations

After a stream is created, users may perform operations on the databases and tables associated with the stream. The effects of these operations on the stream and how the stream handles them are summarized as follows:

| Operation                                                    | Operation Impact and Stream Handling                         |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| User creates a new child table under a trigger supertable (non-virtual) and writes data | The new child table is automatically included in the current stream processing, either joining an existing group or creating a new one. |
| User creates a new child table under a virtual trigger supertable and writes data | Ignored; no additional handling.                             |
| User deletes a child table of the trigger supertable         | Default: Ignored.<br/>Optional: Certain trigger types can be configured to automatically recalculate, or to delete the corresponding result table (only applies to streams grouped by child table). |
| User deletes the trigger table                               | Ignored; no additional handling.                             |
| User adds a column to the trigger table                      | Ignored; no additional handling.                             |
| User deletes a column from the trigger table                 | Ignored; no additional handling.                             |
| User modifies the tag value of a child table under the trigger supertable | If the tag column is used by the stream as a grouping key, the operation is not allowed and results in an error.<br/>Otherwise, ignored. |
| User modifies the schema of the trigger table columns        | Ignored; no additional handling. (An error will be raised when a schema mismatch is detected at read time.) |
| User modifies or deletes a source table                      | Ignored; no additional handling.                             |
| User modifies or deletes an output table                     | Ignored; no additional handling. (If a schema mismatch is detected at write time, an error is raised. If the table does not exist, it will be recreated.)） |
| User splits a vnode                                          | Not allowed if the database containing the vnode is a source database or trigger table database.<br/>Not allowed if virtual tables are used for triggers or computations.<br/>The user may force execution after confirming no impact with SPLIT VGROUP N FORCE. |
| User deletes a database                                      | Not allowed if the deleted database is a source database of a stream, or a trigger table database that is not the same as the stream’s own database.<br/>Not allowed if the stream involves triggers or computations on virtual tables from non-target databases.<br/>The user may force execution after confirming no impact with `DROP DATABASE name FORCE`. |

Apart from the operations explicitly restricted or specially handled in the table above, all other operations—as well as those marked as ignored; no additional handling—are unrestricted. However, if such operations may affect stream computation, it is the user’s responsibility to decide how to proceed: either ignore the impact or perform a manual recalculation to restore correctness.

### Configuration Parameters

Stream processing–related configuration parameters are listed below. For full details, see [taosd](https://docs.tdengine.com/tdengine-reference/components/taosd/).

- numOfMnodeStreamMgmtThreads: Number of stream management threads on mnodes.
- numOfStreamMgmtThreads: Number of stream management threads on vnodes/snodes.
- numOfVnodeStreamReaderThreads: Number of stream reader threads on vnodes.
- numOfStreamTriggerThreads: Number of stream trigger threads.
- numOfStreamRunnerThreads: Number of stream execution threads
- streamBufferSize: Maximum buffer size available for stream processing, used only for caching results of %%trows (unit: MB).
- streamNotifyMessageSize: Controls the size of event notification messages.
- streamNotifyFrameSize: Controls the underlying frame size used when sending event notification messages.

### Rules and Limitations

The following rules and limitations apply to stream processing:

- Before creating a stream, the cluster must have at least one snode deployed, and there must be an available (running) snode at the time of creation.
- Each stream belongs to a specific database. Therefore, the database must already exist before creating a stream, and streams within the same database cannot share the same name.
- The trigger table and the source table for a stream may be the same or different, and they can belong to different databases.
- The output table of a stream can be in a different database from the stream, trigger table, or source table, but it cannot be the same as the trigger table or the source table.
- Output tables (whether supertables or regular tables) are created automatically when the stream is created. If you want to write to an existing table, its schema must match exactly.
- Output child tables for each group do not need to be created in advance; they are created automatically when results are written during computation.
- The computation results of each trigger group are written to the same child table. If no trigger group is specified, all results are written to a single regular table.
- If different groups are configured to generate child tables with the same name, their results will be written into the same child table. Users must confirm this is the intended behavior; otherwise, ensure each group generates a uniquely named child table.
- In addition to specifying child table names, users can also define the tag columns of the output supertable and the tag values for each child table.
- Stream processing supports nesting, meaning a new stream can be created based on the output table of an existing stream.
- Count window triggers do not support automatic handling of out-of-order, update, or delete scenarios (they are ignored). In non-FILL_HISTORY_FIRST mode, historical and real-time windows may not align.
- For supertable window triggers, only interval and session windows support grouping by tag, by child table, or no grouping. Other window types only support grouping by child table.
- Pseudo-columns qstart, qend, and qduration are not supported in queries.

Temporary Restrictions:

- Grouping by regular data columns is not yet supported.
- The Geometry data type is not yet supported.
- The functions Interp, Percentile, Forecast, and UDFs are not yet supported.
- The DELETE_OUTPUT_TABLE option is not yet supported.
- The ON_FAILURE_PAUSE option in NOTIFY_OPTIONS is not yet supported.
- The Cast function is not yet supported in state window triggers.
- The Windows platform is not yet supported.

### Compatibility Notes

Compared with version 3.3.6.0, stream processing has been completely redesigned. Before upgrading from the old version, the following steps must be performed, after which streams should be recreated under the new stream processing version:

- Delete all existing stream processing tasks.
- Delete all TSMA.
- Delete all snodes.
- Remove snode-related directories:
  - The snode directory under the dataDir configuration path (default: /var/lib/taos/snode).
  - The directory specified by the former checkpointBackupDir configuration option (default: /var/lib/taos/backup/checkpoint/).
- Delete all result tables.

Note: If the above steps are not performed, taosd will fail to start.

## Best Practices

The redesigned stream processing engine offers greater flexibility and removes many previous limitations. While availability has been improved, it also introduces higher requirements for proper use.

### Deployment

- Deploy snodes on dedicated dnodes to minimize the impact of stream processing on database reads and writes. These dnodes should not host vnodes, mnodes, or qnodes.
- Deploy multiple snodes within the cluster to ensure high availability.
- Create multiple snodes in advance before defining streams to achieve better load balancing.
- When stream processing workloads are heavy, scale out by adding additional snodes to balance the load.

### Configuration

- Configure the number of stream-related threads according to the deployment method and workload. A higher thread count increases CPU resource consumption, while a lower thread count reduces it.
- Configure the maximum stream buffer size based on the deployment method. For heavier workloads or when running many concurrent streams, increase the buffer size—especially when snodes are deployed on dedicated nodes.

### Designing Streams

Before creating a stream, users should carefully review the following key checkpoints. Once clarified, streams can be designed and used accordingly:

- Choose a trigger type based on business characteristics: If you need to process after each one or more data records are written, choose a count trigger. If you need to process when window conditions are met, choose a window trigger. If you need periodic computation based on event time, choose a sliding trigger. If you need periodic computation based on processing time, choose a periodic trigger.
- Choose window trigger options based on timeliness requirements: In addition to selecting the window type, decide whether to trigger on window open, window close, or both. You may also choose whether to compute promptly before the window closes using MAX_DELAY(delay_time).
- Select the trigger table based on event sources: The event source table is used as the trigger table. For periodic triggers, no trigger table is required. However, if grouped outputs or the trigger table dataset (%%trows) during the scheduled interval are needed, a trigger table must be specified.
- Ensure time sequence consistency between source and trigger tables: If the source table and the trigger table are different, ensure that the source table contains valid data when the trigger table event is triggered. Otherwise, computation accuracy may be affected.
- Decide grouping based on final business requirements: For computations on a supertable, no grouping is required for global aggregation. Use tag-based grouping if aggregation by certain tags is needed. Use child-table grouping if results are required at the individual table level.
- Choose grouping and output subtables based on how stream results will be used:
  - Each group can have its own independent output subtable. However, if there are too many groups this may result in too many result tables. Depending on how the results will be used and on system resource limits, you can decide whether each group really needs its own output subtable.
  - If multiple groups can be merged into a single subtable, you can configure those groups to use the same output table name ([OUTPUT_SUBTABLE(tbname_expr)]). Combined with a composite primary key design in the output table, this allows group results to be merged.
- Use the optimal data writing method: The best ingestion pattern for stream processing is sequential writes per group. If there are a large number of out-of-order writes, update writes, or data deletions, this can trigger extensive recalculation. If sequential writing can be guaranteed, computation efficiency will improve significantly.
- Check the degree of out-of-order writes: Based on the out-of-order behavior of each child table, determine whether a WATERMARK is needed and what an appropriate WATERMARK duration should be.
- Assess the impact of out-of-order data on stream processing:
  - If out-of-order data is present, confirm whether it affects the correctness of results. In scenarios where timeliness is more important, or where trigger table disorder does not affect computation results, you can use STREAM_OPTIONS(IGNORE_DISORDER) to ignore the out-of-order data.
  - If there are severe out-of-order records from far in the past (where the event time is much earlier than the current processed event time) and these records do not affect correctness, or their timeliness has already been lost, you can mark them as expired and ignore them with STREAM_OPTIONS(EXPIRED_TIME(exp_time)).
- Verify the validity of recalculation for stream results: Out-of-order, update, and delete scenarios are mainly addressed through recalculation. If recalculation is not idempotent or produces invalid results, correctness may be affected. This should be judged based on business requirements.
- Check the impact of deletions on stream processing: If deletions occur and results need to be recomputed based on deleted data, use STREAM_OPTIONS(DELETE_RECALC).
- Confirm whether historical data needs to be computed and how:
  - If data already exists in the database before the stream is created, it may need to be computed. Depending on business requirements and logic, confirm whether historical data should be prioritized over real-time data. For example, COUNT_WINDOW triggers should prioritize historical data, otherwise windows may not align properly.
  - To prioritize historical data, specify STREAM_OPTIONS(FILL_HISTORY_FIRST). Otherwise, specify STREAM_OPTIONS(FILL_HISTORY).
- Confirm the level of real-time requirements for stream processing: If your business requires very high timeliness for notifications or computations, you can specify STREAM_OPTIONS(LOW_LATENCY_CALC). This mode consumes more computing resources.
- Clarify the purpose of stream processing: If you only need an event trigger notification without computation, you can use the notification-only mode (i.e., no computation statement specified).
- Confirm how computed results will be used: If only result notifications are required and results do not need to be saved, use the notify-only option (STREAM_OPTIONS(CALC_NOTIFY_ONLY)).
- Verify the reliability of result writes: If the primary key of a computed result is NULL, that result will be discarded.
  - If the query statement includes a grouping clause and results from different groups are written into the same subtable, records with identical timestamps may overwrite each other.
  - If multiple triggers from the same group produce results with the same primary key timestamp, they will overwrite each other.
  - If multiple triggers (including recalculations) from the same group produce results with different primary key timestamps, they will not overwrite each other.

### Stream Maintenance

In the stream status display (query the table information_schema.ins_streams), several detailed status indicators are listed. Examples include whether real-time computations are keeping up with progress, how many recalculations (and what ratio) have occurred, and any error messages. Users and administrators should monitor this information to determine whether stream processing is functioning normally, and use it as a basis for analysis and optimization.

### Stream Creation Example

##### Count Window Trigger

- Each time one row is written into table tb1, compute the average value of column col1 in table tb2 over the past 5 minutes up to that moment, and write the result into table tb3.

```SQL
CREATE stream sm1 count_window(1) FROM tb1 
  INTO tb3 AS
    SELECT _twstart, avg(col1) FROM tb2 
    WHERE _c0 >= _twend - 5m AND _c0 <= _twend;
```

- Each time 10 rows are written into table tb1 where column col1 is greater than 0, compute the average of column col1 for those 10 rows. The result does not need to be saved but must be sent as a notification to ws://localhost:8080/notify.

```SQL
CREATE stream sm2 count_window(10, 1, col1) FROM tb1 
  STREAM_OPTIONS(CALC_ONTIFY_ONLY | PRE_FILTER(col1 > 0)) 
  NOTIFY("ws://localhost:8080/notify") ON (WINDOW_CLOSE) 
  AS 
    SELECT avg(col1) FROM %%trows;
```

##### Event Window Trigger

- When the ambient temperature exceeds 80° and remains above that threshold for more than 10 minutes, compute the average ambient temperature.

```SQL
CREATE STREAM `idmp`.`ana_temp` EVENT_WINDOW(start with `temp` > 80 end with `temp` <= 80 ) TRUE_FOR(10m) FROM `idmp`.`vt_envsens02_471544` 
  STREAM_OPTIONS( IGNORE_DISORDER)
  INTO `idmp`.`ana_temp` 
  AS 
    SELECT _twstart+0s as output_timestamp, avg(`temp`) as `avgtemp` FROM idmp.`vt_engsens02_471544` where ts >= _twstart and ts <= _twend;
```

##### Sliding Trigger

- For each subtable of supertable stb1, at the end of every 5-minute time window, compute the average of column col1 over that interval. The results for each subtable are written separately into different subtables of supertable stb2.

```SQL
CREATE stream sm1 INTERVAL(5m) SLIDING(5m) FROM stb1 PARTITION BY tbname 
  INTO stb2 
  AS 
    SELECT _twstart, avg(col1) FROM %%tbname 
    WHERE _c0 >= _twstart AND _c0 <= _twend;
```

In the SQL above, `FROM %%tbname WHERE _c0 >= _twstart AND _c0 <= _twend` and `FROM %%trows` are not equivalent. The former means the computation uses data from the trigger group’s corresponding table within the window’s time range; those in-window rows may differ from what `%%trows` saw at trigger time. The latter means the computation uses only the window data captured at the moment of triggering.

- For each subtable of supertable stb1, starting from the earliest data, compute the average of col1 for each 5-minute time window either when the window closes or when 1 minute has elapsed since the window opened and it is still not closed. Write each subtable’s result to a separate subtable under supertable stb2.

```SQL
CREATE stream sm2 INTERVAL(5m) SLIDING(5m) FROM stb1 PARTITION BY tbname 
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

##### Scheduled Trigger

- Every hour, compute the total number of rows in table tb1 and write the result to table tb2 (in a millisecond-precision database).

```SQL
CREATE stream sm1 PERIOD(1h) 
  INTO tb2 
  AS
    SELECT cast(_tlocaltime/1000000 AS TIMESTAMP), count(*) FROM tb1;
```

- Every hour, send a notification with the current system time to `ws://localhost:8080/notify`.

```SQL
CREATE stream sm1 PERIOD(1h) 
  NOTIFY("ws://localhost:8080/notify");
```

- Calculate the sum of power consumption for each subtable in the smart meter supertable meters every day, write the calculation results into the downsampled supertable meters_1d, and carry over the TAG values from each subtable.

```SQL
CREATE stream stream_consumer_energy 
  PERIOD(1d) 
  FROM meters PARTITION BY tbname, groupid, location
  INTO meters_1d (ts, sum_power)
     TAGS (groupid INT AS groupid , location VARCHAR(24) AS location)
  AS 
     SELECT cast(_tlocaltime/1000000 AS timestamp) ,sum(current*voltage) AS sum_power
          FROM meters
          WHERE ts >= cast(_tprev_localtime/1000000 AS timestamp) AND ts <= cast(_tlocaltime/1000000 AS timestamp);
```