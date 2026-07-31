---
sidebar_label: Stream Syntax
title: Stream Syntax
description: CREATE/SHOW/start/stop stream syntax and parameter descriptions
toc_max_heading_level: 4
---

## Create a Stream

```sql
CREATE STREAM [IF NOT EXISTS] [db_name.]stream_name options [INTO [db_name.]table_name] [NODELAY_CREATE_SUBTABLE] [OUTPUT_SUBTABLE(tbname_expr)] [(column_name1, column_name2 [COMPOSITE KEY][, ...])] [TAGS (tag_definition [, ...])] [AS subquery]

options: {
    trigger_type [FROM [db_name.]table_name] [{PARTITION BY col1 [, ...] | ROLLUP BY tag_name}] [STREAM_OPTIONS(stream_option [|...])] [notification_definition]
}
    
trigger_type: {
    PERIOD(period_time[, offset_time])
  | SLIDING(sliding_val[, offset_time]) 
  | INTERVAL(interval_val[, interval_offset]) SLIDING(sliding_val[, offset_time]) 
  | SESSION(ts_col, session_val)
  | STATE_WINDOW(state_expr [, state_expr ...]) [EXTEND(extend_val)] [ZEROTH_STATE(zeroth_val [, zeroth_val ...])] [TRUE_FOR(true_for_expr)]
  | EVENT_WINDOW(START WITH start_condition END WITH end_condition) [TRUE_FOR(true_for_expr)]
  | EVENT_WINDOW(START WITH (start_condition_1, start_condition_2 [,...]) [END WITH end_condition]) [TRUE_FOR(true_for_expr)]
  | COUNT_WINDOW(count_val[, sliding_val][, col1[, ...]]) 
}

true_for_expr:
    true_for_arg [, true_for_arg [, true_for_arg]]

true_for_arg: {
    limit_expr
  | start(limit_expr)
  | end(limit_expr)
}

limit_expr: {
    duration_time
  | COUNT count_val
  | duration_time AND COUNT count_val
  | duration_time OR COUNT count_val
}

stream_option: {WATERMARK(duration_time) | EXPIRED_TIME(exp_time) | IGNORE_DISORDER | DELETE_RECALC | DELETE_OUTPUT_TABLE | FILL_HISTORY[(start_time)] | FILL_HISTORY_FIRST[(start_time)] | CALC_NOTIFY_ONLY | LOW_LATENCY_CALC | PRE_FILTER(expr) | FORCE_OUTPUT | MAX_DELAY(delay_time) | EVENT_TYPE(event_types) | IGNORE_NODATA_TRIGGER | IDLE_TIMEOUT(duration_time)}

notification_definition:
    NOTIFY(url [, ...]) [ON (event_types)] [WHERE condition] [NOTIFY_OPTIONS(notify_option[|notify_option])]

notify_option: NOTIFY_HISTORY

event_types:
    event_type [|event_type]

event_type: {WINDOW_OPEN | WINDOW_CLOSE | IDLE | RESUME}

tag_definition:
    tag_name type_name AS expr
```

For version-specific support of stream timezones and natural time units (such as weeks, months, quarters, and years in `PERIOD`, `SLIDING`, and `INTERVAL`), see [Stream Timezone](../05-tdengine-sql/10-time/01-timezone.md#stream-timezone).

### Trigger Methods in Stream Processing

Event triggers are the driving mechanism for stream processing. The source of an event trigger can vary—it may come from data being written to a table, from the analytical results of computations on a table, or even from no table at all. When the stream processing engine detects that the user-defined trigger conditions are met, it initiates the computation. The number of times the condition is met corresponds exactly to the number of times computation is triggered. The trigger object and the computation object are independent of each other. Users can flexibly define and use various types of windows to generate trigger events, with support for triggering on window open, window close, or both. Group-based triggering is supported, as well as pre-filtering of trigger data so that only data meeting the criteria will be considered for triggering.

#### Trigger Types

The trigger type is specified using trigger_type and includes: scheduled trigger, sliding trigger, time window trigger, session window trigger, state window trigger, event window trigger, and count window trigger. When using state windows, event windows, or count windows with a supertable, they must be used together with `partition by tbname`.

##### Scheduled Trigger

```sql
PERIOD(period_time[, offset_time])
```

A scheduled trigger is driven by a fixed interval based on the system time, essentially functioning as a scheduled task. It does not belong to the category of window triggers. Parameter definitions are as follows:

- period_time: The scheduling interval. Supported time units are listed in [Time Units](../05-tdengine-sql/01-datatype.md#time-units) (supports milliseconds through years). The supported range is [10a, 3650d].
- offset_time: (Optional) The scheduling offset. Supported units include milliseconds (a), seconds (s), minutes (m), hours (h), and days (d). For week/month/year units, the offset must be strictly less than the trigger period; for month units, validation is based on 28 days/month (e.g., `PERIOD(1n, 28d)` is invalid).

Usage Notes:

- When the scheduling interval is less than one day, the base time is calculated as midnight (00:00) plus the scheduling offset. The next trigger time is determined based on this base time and the specified interval. The base time resets to midnight each day. The time between the last trigger of one day and the base time of the next day may be shorter than the scheduling interval. For example:
  - If the scheduling interval is 5 hours 30 minutes, the trigger times for the day will be [00:00, 05:30, 11:00, 16:30, 22:00]. The trigger times for subsequent days will be the same.
  - With the same interval but an offset of 1 minute, the trigger times will be [00:01, 05:31, 11:01, 16:31, 22:01] each day.
  - Under the same conditions, if the stream is created when the system time is 12:00, the trigger times for the current day will be [16:31, 22:01]. From the next day onwards, the trigger times will be [00:01, 05:31, 11:01, 16:31, 22:01].
- When the scheduling interval is greater than or equal to 1 day, the base time is calculated from the server timezone's Unix epoch (1970-01-01 00:00:00) plus the scheduling offset, aligned by integer multiples of the trigger interval to ensure global consistency across all tasks. For example:
  - With a scheduling interval of 2 days, all tasks using this interval will trigger at times that are integer multiples of 2 days from the epoch (e.g., 1970-01-03 00:00:00, 1970-01-05 00:00:00, ...), ensuring global alignment.
  - With a scheduling interval of 1 week (`PERIOD(1w)`), triggers align to every Monday at 00:00:00; `PERIOD(1w, 1d)` triggers every Tuesday at 00:00:00.
  - With a scheduling interval of 1 month (`PERIOD(1n)`), triggers align to the 1st of each month at 00:00:00; `PERIOD(1n, 14d)` triggers on the 15th of each month at 00:00:00.
  - With a scheduling interval of 1 year (`PERIOD(1y)`), triggers align to January 1st at 00:00:00 each year; `PERIOD(1y, 31d)` triggers on February 1st at 00:00:00 each year.

Applicable scenarios: Situations requiring scheduled computation driven continuously by system time, such as generating daily statistics every hour, or sending scheduled statistical reports once a day.

##### Sliding Trigger

```sql
SLIDING(sliding_val[, offset_time]) 
```

A sliding trigger drives execution based on a fixed interval of event time for data written to the trigger table. It is not considered a window trigger. A trigger table must be specified. The trigger times and time offset rules are the same as for scheduled triggers, with the only difference being that the system time is replaced by event time.

Parameter definitions are as follows:

- sliding_val: Required. The sliding duration based on event time.
- offset_time: Optional. The time offset for the sliding trigger. Supported time units include milliseconds (a), seconds (s), minutes (m), and hours (h).

Usage Notes:

- A trigger table must be specified. When the trigger table is a supertable, grouping by tags or subtables is supported, as well as no grouping.
- Supports sliding triggers after processing and filtering the incoming data (conditional triggers).

Applicable scenarios: Situations where calculations need to be driven continuously and periodically based on event time, such as generating daily statistical data every hour or sending scheduled reports each day.

##### Time Window Trigger

```sql
INTERVAL(interval_val[, interval_offset]) SLIDING(sliding_val[, offset_time])
```

A time window trigger refers to triggering based on incoming data written to the trigger table, using event time and a fixed window size that slides over time. The INTERVAL window must be specified. This is a type of window trigger, and a trigger table must be specified.

The starting point for a time window trigger is the beginning of the window. By default, windows are divided starting from Unix time 0 (1970-01-01 00:00:00 UTC). You can change the starting point of the window division by specifying a window time offset. Parameter definitions are as follows:

- interval_val: Required. The duration of the interval window.
- interval_offset: Optional. The time offset for the interval window.
- sliding_val: Required. The sliding duration based on event time.
- offset_time: Optional. The time offset for the sliding trigger, with the same meaning as for a sliding trigger.

Usage Notes:

- A trigger table must be specified. When the trigger table is a supertable, grouping by tags or subtables is supported, as well as no grouping.
- Supports conditional time window triggers after processing and filtering the incoming data.

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
STATE_WINDOW(state_expr [, state_expr ...]) [EXTEND(extend_val)] [ZEROTH_STATE(zeroth_val [, zeroth_val ...])] [TRUE_FOR(true_for_expr)]
```

A state window trigger divides the written data of the trigger table into windows based on one or more state keys. A trigger occurs when a window is opened and/or closed. Parameter definitions are as follows:

- state_expr: One or more state keys. Each state key can be a column reference or a tag column, or an expression such as `CASE WHEN`, `IF`, or `CAST`. The result type must be integer, boolean, or `VARCHAR`.
- extend_val (optional): Specifies the extension strategy for the start and end of a window. `EXTEND(0)` is the default behavior. `EXTEND(1)` keeps the window start unchanged and extends the window end forward to just before the next window starts. `EXTEND(2)` keeps the window end unchanged and extends the window start backward to just after the previous window ends.
- zeroth_val (optional): Specifies the zero state. The number of arguments must match the number of state keys. Any argument other than `NO_ZEROTH` must be a constant and convertible to the corresponding state-key type. `NO_ZEROTH` means the corresponding position does not participate in zero-state matching. A window is filtered only when all constrained positions match their zero-state values.
- true_for_expr (optional): Specifies the filtering condition for windows. Only windows that meet the condition will generate a trigger. Supports the following four modes:
  - `TRUE_FOR(duration_time)`: Filters based on duration only. The window duration must be greater than or equal to `duration_time`.
  - `TRUE_FOR(COUNT n)`: Filters based on row count only. The window row count must be greater than or equal to `n`.
  - `TRUE_FOR(duration_time AND COUNT n)`: Both duration and row count conditions must be satisfied.
  - `TRUE_FOR(duration_time OR COUNT n)`: Either duration or row count condition must be satisfied.

  Where `duration_time` is a positive time value. Supported time units are listed in [Time Units](../05-tdengine-sql/01-datatype.md#time-units) (milliseconds through weeks only). Examples: `TRUE_FOR(10m)`, `TRUE_FOR(COUNT 100)`, `TRUE_FOR(10m AND COUNT 100)`, `TRUE_FOR(10m OR COUNT 100)`.

Usage Notes:

- A trigger table must be specified. When the trigger table is a supertable, grouping by tags or subtables is supported, as well as no grouping.
- State windows support single-key and multi-key definitions. The current window closes when any state key changes.
- When used with a supertable, it must be combined with PARTITION BY tbname.
- Supports conditional window triggering after filtering the written data.
- If all state-key columns are `NULL`, the row follows the existing `NULL` behavior of state windows. If only some state-key columns are `NULL`, consecutive partial-`NULL` rows are handled as a whole and may merge into the previous window, merge into the next window, or become an independent window.
- The table below shows the most common merge outcomes for state-window triggers. In each row, “merge into previous”, “merge into next”, and “independent window” all refer to the consecutive partial-`NULL` rows in the middle:

| Input sequence (state keys) | `EXTEND(0)` | `EXTEND(1)` | `EXTEND(2)` |
| --- | --- | --- | --- |
| `(1, 10) -> (1, NULL) -> (1, 20)` | Merge into previous | Merge into previous | Merge into next |
| `(1, 'a') -> (1, NULL) -> (2, 'a')` | Merge into previous | Merge into previous | Independent window |
| `(1, 'a') -> (NULL, 'b') -> (1, 'b')` | Merge into next | Independent window | Merge into next |
| `(1, 'a') -> (NULL, 'b') -> (2, 'a')` | Independent window | Independent window | Independent window |

- If a consecutive partial-`NULL` run contains all-`NULL` rows in the middle, those all-`NULL` rows are handled together with that run. For example, in `(1, 'a') -> (1, NULL) -> (NULL, NULL) -> (1, NULL) -> (2, 'a')`, the three middle rows are handled together: `EXTEND(0)` and `EXTEND(1)` merge them into the previous window, while `EXTEND(2)` keeps them as an independent window.
- `ZEROTH_STATE(...)` works position by position. A window is filtered only when every participating position equals its configured zero-state value. If a position uses `NO_ZEROTH`, that position is excluded from zero-state matching.
- The state expression can reference tag columns visible in the trigger-table context. For example:

```sql
CREATE STREAM s_tag_state
  STATE_WINDOW(voltage >= 220 + groupId)
  FROM meters
  PARTITION BY tbname
  INTO meters_state_out
  AS SELECT _twstart AS ts, _twend AS te, COUNT(*) AS cnt FROM %%trows;
```

Multi-key state-window example:

```sql
CREATE STREAM s_multi_state
  STATE_WINDOW(s1, s2) EXTEND(0) ZEROTH_STATE(1, NO_ZEROTH)
  FROM ntb
  PARTITION BY tbname
  INTO result_table
  AS
    SELECT _twstart AS ts, _twend AS te, COUNT(*) AS cnt FROM %%trows;
```

The stream above cuts a new window whenever either `s1` or `s2` changes. Zero-state filtering is applied only to `s1 = 1`; `s2` does not participate in zero-state matching.

Applicable Scenarios: Suitable for use cases where computations and/or notifications need to be driven by state windows.

##### Event Window Trigger

```sql
EVENT_WINDOW(START WITH start_condition END WITH end_condition) [TRUE_FOR(true_for_expr)]
```

An event window trigger partitions the incoming data of the trigger table into windows based on defined event start and end conditions, and triggers when the window opens and/or closes. Parameter definitions are as follows:

- start_condition: Definition of the event start condition. It can be any valid conditional expression.
- end_condition: Definition of the event end condition. It can be any valid conditional expression.
- true_for_expr (optional): Specifies window-level filtering conditions and open/close streak thresholds. All three sub-parameters are optional and may appear in any order, at most once each:
  - **Window-level filter (`limit_expr`)**: Only windows meeting the condition will generate a trigger:
    - `TRUE_FOR(duration_time)`: The window duration must be greater than or equal to `duration_time`.
    - `TRUE_FOR(COUNT n)`: The window row count must be greater than or equal to `n`.
    - `TRUE_FOR(duration_time AND COUNT n)`: Both conditions must be satisfied.
    - `TRUE_FOR(duration_time OR COUNT n)`: Either condition must be satisfied.
  - **Open-condition streak threshold (`start(limit_expr)`)**: The `START WITH` expression must be continuously satisfied for `limit_expr` before the window actually opens. `_wstart` is set to the first row of the streak. Streak interruption resets the counter.
  - **Close-condition streak threshold (`end(limit_expr)`)**: The `END WITH` expression must be continuously satisfied for `limit_expr` before the window actually closes. `_wend` is set to the first row of the close streak. Streak interruption resets the counter; the window stays open.

  Where `duration_time` is a positive time value. Supported time units are listed in [Time Units](../05-tdengine-sql/01-datatype.md#time-units) (milliseconds through weeks only). Examples: `TRUE_FOR(10m)`, `TRUE_FOR(COUNT 100)`, `TRUE_FOR(start(COUNT 2))`, `TRUE_FOR(end(3s))`, `TRUE_FOR(5s, start(COUNT 2), end(COUNT 3))`. `start(...)` and `end(...)` are only supported for single-condition `EVENT_WINDOW`.

```sql
CREATE STREAM s_tag_event
  EVENT_WINDOW(START WITH voltage >= 220 + groupId END WITH voltage < 220 + groupId)
  FROM meters
  PARTITION BY tbname
  INTO meters_event_out
  AS SELECT _twstart AS ts, _twend AS te, COUNT(*) AS cnt FROM %%trows;
```

Usage Notes:

- A trigger table must be specified. When the trigger table is a supertable, grouping by tags or subtables is supported, as well as no grouping.
- When used with a supertable, it must be combined with PARTITION BY tbname.
- Supports conditional window triggering after filtering the written data.
- The start/end condition expressions can reference tag columns visible in the trigger-table context. For example:

Applicable Scenarios: Suitable for use cases where computations and/or notifications need to be driven by event windows.

##### Event Window Trigger (with Sub-Event Window Support)

```sql
EVENT_WINDOW(START WITH (start_condition_1, start_condition_2 [,...]) [END WITH end_condition]) [TRUE_FOR(true_for_expr)]
```

An event window trigger partitions the incoming data of the trigger table into windows based on event windows. It now supports specifying multiple start conditions and can further subdivide and manage sub-event windows within the original event window based on changes in the effective trigger condition, while introducing the concept of a parent event window to aggregate related sub-event windows. Parameter definitions are as follows:

- start_condition_1, start_condition_2 [, ...]: Defines multiple event start conditions. The event window opens when any one of these conditions is satisfied. The system evaluates these conditions in order from first to last, and the first satisfied condition becomes the "effective trigger condition". When all start_conditions are not satisfied, both the parent window and the last sub-window close.
- end_condition: Definition of the event end condition. When this condition is satisfied, both the current parent window and the last sub-window close. This parameter is now optional.
- true_for_expr (optional): Specifies the filtering condition for windows. Only windows that meet the condition will generate a trigger. Supports the following four modes:
  - `TRUE_FOR(duration_time)`: Filters based on duration only. The window duration must be greater than or equal to `duration_time`.
  - `TRUE_FOR(COUNT n)`: Filters based on row count only. The window row count must be greater than or equal to `n`.
  - `TRUE_FOR(duration_time AND COUNT n)`: Both duration and row count conditions must be satisfied.
  - `TRUE_FOR(duration_time OR COUNT n)`: Either duration or row count condition must be satisfied.

  Where `duration_time` is a positive time value. Supported time units are listed in [Time Units](../05-tdengine-sql/01-datatype.md#time-units) (milliseconds through weeks only). Examples: `TRUE_FOR(10m)`, `TRUE_FOR(COUNT 100)`, `TRUE_FOR(10m AND COUNT 100)`, `TRUE_FOR(10m OR COUNT 100)`.

Usage Notes:

- A trigger table must be specified. When the trigger table is a supertable, grouping by tags or subtables is supported, as well as no grouping.
- When used with a supertable, it must be combined with PARTITION BY tbname.
- Supports conditional window triggering after filtering the written data.
- The multiple `start_condition` expressions and the optional `end_condition` can also reference tag columns visible in the trigger-table context.
- Parent and sub-window behavior:
  - No parent/sub-windows: During the event window opening period, if the effective trigger condition does not change, only one window is produced. The system treats it as a regular event window, without generating the concept of parent/sub-windows.
  - Sub-windows: When a specific start_condition becomes the effective trigger condition, a sub-window opens. If the effective trigger condition changes, or when the end_condition is satisfied, the current sub-window closes. Sub-windows do not overlap with each other.
  - Parent window: A parent window only opens when the second sub-window opens. The parent window's start time is the start time of the first sub-window, and its end time is the end time of the last sub-window. It closes when all start_conditions are not satisfied, or when the end_condition is satisfied.
- Notification message extensions: In the window open (WINDOW_OPEN) notification message, two new fields are added:
  - conditionIndex: The index number of the start condition that triggered the current window opening, counting from 0. For a parent window, its value is the same as the first sub-window's value.
  - windowIndex: The index number of the sub-event window within the parent window, counting from 0. If it is not a sub-window (i.e., a regular event window or parent window), this field value is -1.
- The TRUE_FOR option applies to both sub-windows and parent windows, meaning windows (whether sub-windows or parent windows) shorter than the duration limit will be directly ignored. When some sub-windows under a parent window do not meet the TRUE_FOR condition, the valid sub-windows may not be consecutive. If only 1 sub-window under a parent window meets the TRUE_FOR condition, the parent/sub-window structure is still retained and triggers notifications and computations.

Applicable Scenarios: Suitable for use cases where computations and/or notifications need to be driven by event windows, especially in IoT and industrial data management fields where fine-grained monitoring and analysis of events based on multiple dynamically changing conditions is required. For example, in equipment fault alarms, multiple alarm level conditions (such as "load above 90" and "load above 60") can be defined, and when alarm levels change, the escalation or de-escalation of alarm states can be clearly tracked.

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
| PERIOD, SLIDING, INTERVAL, and SESSION | Subtable, tag, rollup tag, and none |
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

You can also specify a hierarchical tag rollup grouping column. `ROLLUP BY` is mutually exclusive with `PARTITION BY` and supports only one tag column.

```sql
[ROLLUP BY tag_name]
```

`ROLLUP BY` is intended for tag values that encode a hierarchy, for example `factory.workshop.line`. TDengine uses the fixed separator `.` to expand the string value of `tag_name` into all path prefixes from the root to the current node. Each prefix is an independent trigger group. For example, a tag value `A.B.C` is expanded into `A`, `A.B`, and `A.B.C`; data from that child table participates in all three groups. A parent rollup group includes data from child tables whose full path is the parent path itself or any descendant path.

Usage notes:

- `tag_name` must be a `VARCHAR` or `NCHAR` tag column on the trigger supertable or virtual supertable.
- `ROLLUP BY` supports `PERIOD`, `SLIDING`, `INTERVAL`, and `SESSION` triggers. It does not support state windows, event windows, or count windows.
- `FROM <table_name>` must be specified when using `ROLLUP BY`, even for `PERIOD` triggers.
- If the tag value is `NULL` or an empty string, no rollup group is generated and no trigger or computation occurs for that value.
- Tag values must not contain leading separators, trailing separators, repeated separators, empty path segments, control characters, or leading/trailing whitespace in a path segment. If an invalid path is detected, the stream enters the error state.
- The tag column referenced by `ROLLUP BY` cannot be modified, dropped, or renamed. Dropping a source child table does not drop output subtables that have already been generated.

### Stream Processing Output

By default, the results of a stream are stored in an output table. Each output table contains only the results that have been triggered and computed up to the current time. You can define the structure of the output table, and if grouping is used, you can also specify the tag values for each subtable.

```sql
[INTO [db_name.]table_name] [NODELAY_CREATE_SUBTABLE] [OUTPUT_SUBTABLE(tbname_expr)] [(column_name1, column_name2 [COMPOSITE KEY][, ...])] [TAGS (tag_definition [, ...])] 

tag_definition:
    tag_name type_name AS expr
```

Details are as follows:

- `INTO [db_name.]table_name`: Optional. Specifies the output table name as table_name and the database name as db_name.
  - If trigger grouping is used, this table will be a supertable.
  - If no trigger grouping is used, this table will be a regular table.
  - If the trigger only sends notifications without computation, or if computation results are only sent as notifications without being stored, this option does not need to be specified.
- `[NODELAY_CREATE_SUBTABLE]`: Optional. Specifies that the calculation output subtables/normal-table for each group are created immediately when the stream is created. By default, output subtables/normal-table are created only when the first calculated data is written. If this option is added, subtables are created asynchronously after the stream is created. If not all subtables are created successfully, the stream status remains `Idle`; if creation succeeds, the status changes to `Running`. For regular tables and supertables as output tables, they are created automatically when the stream is created by default, and no configuration is needed.
- `[OUTPUT_SUBTABLE(tbname_expr)]`: Optional. Specifies the name of the calculation output table (subtable) for each trigger group. This cannot be specified if there is no trigger grouping. If not specified, a unique output table (subtable) name will be automatically generated for each group. tbname_expr can be any output string expression, and may include trigger group partition columns (from [PARTITION BY col1[, ...]]). When `ROLLUP BY` is used, `%%1` references the full path of the current rollup node and `%%rollup_tag` references the local tag value of the current rollup node; `_trollup_tbcount` cannot be used here. The output length must not exceed the maximum table name length; if it does, it will be truncated. If you do not want different groups to output to the same subtable, you must ensure each group's output table name is unique.
- `[(column_name1, column_name2 [COMPOSITE KEY][, ...])]`: Optional. Specifies the column names for each column in the output table. If not specified, each column name will be the same as the corresponding column name in the calculation result. You can use [COMPOSITE KEY] to indicate that the second column is a primary key column, forming a composite primary key together with the first column.
- `[TAGS (tag_definition [, ...])]`: Optional. Specifies the list of tag column definitions and values for the output supertable. This can only be specified if trigger grouping is present. If not specified, the tag column definitions and values are derived from all grouping columns, and in this case, grouping columns cannot have duplicate names. When grouping by subtable, the default generated tag column name is tag_tbname, with the type VARCHAR(270). When `ROLLUP BY` is used, the default tag value is the full path of the current rollup node. The tag_definition parameters are as follows:
  - `tag_name`: Name of the tag column.
  - `type_name`: Data type of the tag column.
  - `expr`: Tag value calculation expression, which can use any trigger table grouping columns (`from [PARTITION BY col1[, ...]]`). When `ROLLUP BY` is used, `%%1` and `%%rollup_tag` can be used; `_trollup_tbcount` cannot be used.

### Stream Processing Computation Tasks

```sql
[AS subquery]
```

A computation task is the calculation executed by the stream after an event is triggered. It can be any type of query statement, and can operate on the trigger table or on other databases and tables. Computation tasks are highly flexible and should be carefully designed before creating the stream. Notes:

- The first column in the query output will serve as the primary key column of the output table: The first column in the query output must be a valid primary key value (TIMESTAMP). If the column type does not match, an error will occur when creating the stream. If a NULL value appears during execution, the corresponding computation result will be discarded.
- Each trigger group’s computation results are written into the same output table (subtable or regular table) for that group: If the query also contains a GROUP BY clause, records with the same primary key in the grouped results will overwrite each other. If grouping is required, it is recommended to define a composite primary key for the output table.

#### Placeholders

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
| Idle Trigger      | _tidlestart      | The time (processing time) of the last data received by the group before it entered idle state. Nanosecond precision Unix epoch. Applicable only for IDLE/RESUME triggers. Cannot be mixed with `_twstart/_twend`. Since output tables are usually millisecond-precision, use `cast(_tidlestart/1000000 as timestamp)` to convert. |
| Idle Trigger      | _tidleend        | The trigger time of the IDLE or RESUME event. Nanosecond precision Unix epoch. Applicable only for IDLE/RESUME triggers. Cannot be mixed with `_twstart/_twend`. Since output tables are usually millisecond-precision, use `cast(_tidleend/1000000 as timestamp)` to convert.|
| All               | _tgrpid          | ID of trigger group (data type BIGINT)                       |
| All               | _tlocaltime      | System time of current trigger (nanosecond precision)        |
| All               | %%n              | Reference to trigger group column<br/>n is the column number in `[PARTITION BY col1[, ...]]`, starting with 1<br/>When `ROLLUP BY` is used, `%%1` is the full path of the current rollup node |
| All               | %%tbname         | Reference to trigger table<br/>Only used with the trigger group contains tbname.<br/>Can be used in queries as `FROM %%tbname` |
| All               | %%trows          | Reference to the trigger dataset of each group in the trigger table (the dataset that satisfies the current trigger).<br/>For scheduled triggers, this refers to the data written to the trigger table between the last and current trigger.<br/>When `ROLLUP BY` is used, it refers to the trigger dataset from child tables associated with the current rollup node path and all descendant paths.<br/>Can only be used as a query table name (FROM %%trows).<br/>Applicable only for WINDOW_CLOSE triggers.<br/>Recommended for use in small data volume scenarios. |
| ROLLUP BY         | %%rollup_tag     | Local tag value of the current rollup node, that is, the last segment of the path. If the path does not contain `.`, it is the full path. |
| ROLLUP BY         | _trollup_tbcount | Number of source child tables associated with the current rollup node at this trigger. |

Usage Restrictions:

- %%trows: Can only be used in the FROM clause. Queries that use %%trows do not support WHERE condition filtering or join operations on %%trows.
- %%tbname: Can be used in the FROM, SELECT, and WHERE clauses.
- %%rollup_tag: Available only with `ROLLUP BY`. It can be used in `OUTPUT_SUBTABLE`, `TAGS`, and positions in `AS subquery` where existing trigger placeholders are allowed.
- _trollup_tbcount: Available only with `ROLLUP BY`. It can be used only in `AS subquery`; it cannot be used in `OUTPUT_SUBTABLE` or `TAGS`.
- Other placeholders: Can only be used in the SELECT and WHERE clauses.

### Stream Processing Control Options

```sql
[STREAM_OPTIONS(stream_option [|...])]

stream_option: {WATERMARK(duration_time) | EXPIRED_TIME(exp_time) | IGNORE_DISORDER | DELETE_RECALC | DELETE_OUTPUT_TABLE | FILL_HISTORY[(start_time)] | FILL_HISTORY_FIRST[(start_time)] | CALC_NOTIFY_ONLY | LOW_LATENCY_CALC | PRE_FILTER(expr) | FORCE_OUTPUT | MAX_DELAY(delay_time) | EVENT_TYPE(event_types) | IGNORE_NODATA_TRIGGER | IDLE_TIMEOUT(duration_time)}
```

Control options are used to manage trigger and computation behavior. Multiple options can be specified, but the same option cannot be specified more than once. The available options include:

- WATERMARK(duration_time) specifies the tolerance duration for out-of-order data. Data arriving later than this duration is treated as out-of-order and processed according to the out-of-order handling strategy of the trigger type and user configuration. Default: duration_time = 0 (no tolerance).
- EXPIRED_TIME(exp_time) specifies an expiration interval after which data is ignored. If not set, no data is considered expired. This option can be used when data writes or updates older than a certain time range are irrelevant. exp_time defines the expiration interval. Supported time units: milliseconds (a), seconds (s), minutes (m), hours (h), days (d).
- IGNORE_DISORDER ignores out-of-order data in the trigger table. By default, out-of-order data is not ignored. This option is useful in scenarios where timeliness of computation or notification is more important, and where out-of-order data does not affect the result. Out-of-order data includes both newly written late data and updates to previously written data. For count windows whose sliding step is 1, such as `COUNT_WINDOW(1)` and `COUNT_WINDOW(n, 1)`, out-of-order data and updates trigger automatic recalculation unless this option is specified; count windows whose sliding step is not 1 ignore out-of-order data and updates.
- DELETE_RECALC specifies that data deletions in the trigger table (including when a child table is dropped) should trigger automatic recomputation. This can only be set if the trigger type supports automatic recomputation for deletions. By default, deletions are ignored. This is only needed when data deletions in the trigger table may affect computation results. For count windows, only windows whose sliding step is 1, such as `COUNT_WINDOW(1)` and `COUNT_WINDOW(n, 1)`, support this option. Count windows whose sliding step is not 1 do not support it.
- DELETE_OUTPUT_TABLE ensures that when a subtable in the trigger table is deleted, its corresponding output subtable is also deleted. It applies only to streams grouped by subtable and does not apply to `PARTITION BY` tag grouping or `ROLLUP BY` rollup tag grouping. Default: If not specified, deleting a subtable does not delete its output subtable.
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
  - IDLE: Group idle event. Triggered once when a group has not received any new data for longer than the `IDLE_TIMEOUT` duration. Requires `IDLE_TIMEOUT` to be configured.
  - RESUME: Group resume event. Triggered immediately when an idle group receives new data again. Requires `IDLE_TIMEOUT` to be configured.
- IGNORE_NODATA_TRIGGER ignores triggers when the trigger table has no input data. Applicable for sliding (SLIDING), time window (INTERVAL), and periodic (PERIOD) triggers.
  - Sliding and periodic triggers: If there is no data between two trigger times, the trigger is ignored.
  - Time window triggers: If no data exists in the window, the trigger is ignored.
  - Default: If not specified, triggers will occur even when no input data is present.
- IDLE_TIMEOUT(duration_time) enables group idle detection and specifies the idle timeout duration. When a group has not received any new data for longer than this duration, it is considered idle and an IDLE event is triggered; when the idle group receives new data again, a RESUME event is triggered. Must be used together with `EVENT_TYPE(IDLE)` and/or `EVENT_TYPE(RESUME)`. Supported time units: milliseconds (a), seconds (s), minutes (m), hours (h), days (d). Valid range: `[1s, 10d]`. Idle detection is based on processing time (the time data arrives and is processed by the stream), using a monotonic clock to avoid being affected by system clock adjustments.

### Notification Mechanism in Stream Processing

Event notifications are optional actions executed after a stream is triggered. Notifications can be sent to applications over the WebSocket protocol. Users define notifications through a notification_definition, which specifies the events to be notified and the target address for receiving messages. The notification content may include the computation results, or, when no result is produced, only the event-related information.

```sql
[notification_definition]

notification_definition:
    NOTIFY(url [, ...]) [ON (event_types)] [WHERE condition] [NOTIFY_OPTIONS(notify_option[|notify_option])]

event_types:
    event_type [|event_type]    
    
event_type: {WINDOW_OPEN | WINDOW_CLOSE | IDLE | RESUME}
```

Details:

- url [, ...]: Specifies the target address(es) for notifications. Each URL must include the protocol, IP or domain, and port; it may also include a path and query parameters. Enclose the entire URL in quotes. Currently, only the WebSocket protocol is supported. Examples: `ws://localhost:8080`, `ws://localhost:8080/notify`, `ws://localhost:8080/notify?key=foo`.
- [ON (event_types)]: Specifies the event types to notify; separate multiple values with `|` (do not use strings or a comma-separated list). For SLIDING (without INTERVAL) and PERIOD triggers, this clause is not required; for other trigger types, it is mandatory. Supported event types:
  - WINDOW_OPEN: Window open event; sent when a group window in the trigger table opens.
  - WINDOW_CLOSE: Window close event; sent when a group window in the trigger table closes.
  - IDLE: Group idle event; sent when a group enters idle state. Requires `IDLE_TIMEOUT` to be configured in `STREAM_OPTIONS`.
  - RESUME: Group resume event; sent when an idle group receives data again. Requires `IDLE_TIMEOUT` to be configured in `STREAM_OPTIONS`.
  - For `PERIOD` and plain `SLIDING` triggers, the notification payload uses `ON_TIME` as its `eventType`. `ON_TIME` is a payload value and cannot be specified in the `ON (...)` list.
- [WHERE condition]: Specifies a condition that must be met for a notification to be sent. The condition may reference only columns from the computation result and/or constants.
- [NOTIFY_OPTIONS(notify_option[|notify_option])]: Optional. Specifies notification behavior. The currently supported option is:
  - NOTIFY_HISTORY: Send notifications during historical computation. Default: not sent.
  - `ON_FAILURE_PAUSE` is not currently supported; see [Rules and Limitations](./02-instructions.md#rules-and-limitations).

When a specified event is triggered, taosd sends a POST request to the configured URL. The message body is in JSON format. A single request may contain events from multiple streams, and the event types may vary.
The event information included depends on the window type:

- Time window: on open: start time; on close: start time, end time, computation result.
- State window: on open: start time, previous state key, current state key; on close: start time, end time, computation result, current state key, next state key. State keys are always encoded as arrays ordered the same way as the `STATE_WINDOW` arguments. A single-key state window uses a one-element array, while a multi-key state window uses one element per state key.
- Session window: on open: start time; on close: start time, end time, computation result.
- Event window: on open: start time, triggering data value(s), and condition ID(s); on close: start time, end time, computation result, closing data value(s), and condition ID(s).
- Count window: on open: start time; on close: start time, end time, computation result.

Examples of state-window notification payloads:

```json
{"prevState":[1],"curState":[2]}
```

```json
{"curState":[2, "a"],"nextState":[2, "b"]}
```

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
          }
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

#### Root-Level Fields

- messageId: String. A unique identifier for the notification message, used to ensure the message can be tracked and de-duplicated.
- timestamp: Long integer. The time the notification message was generated, in milliseconds since 00:00, Jan 1 1970 UTC.
- streams: Array of objects. Contains event information for one or more stream tasks. (See the next section for details.)

#### Fields of the stream Object

- streamName: String. The name of the stream task, used to identify which stream the event belongs to.
- events: Array of objects. The list of events under this stream task, containing one or more event objects. (See the next section for details.)

#### Fields of the event Object

##### Common Fields

These fields are shared by all event objects:

- tableName: String. The name of the target child table associated with the event. When there is no output, this field does not exist.
- eventType: String. The type of event. Supported values are ON_TIME, WINDOW_OPEN, WINDOW_CLOSE, WINDOW_INVALIDATION, IDLE, and RESUME.
- eventTime: Long integer. The time the event was generated, in milliseconds since 00:00, Jan 1 1970 UTC.
- triggerId: String. A unique identifier for the trigger event. Ensures that open and close events (if both exist) share the same ID, allowing external systems to correlate them. If taosd crashes and restarts, some events may be resent, but the same event will always retain the same triggerId.
- triggerType: String. The type of trigger. Supported values include the two non-window types Period and SLIDING, as well as the five window types INTERVAL, State, Session, Event, and Count.
- groupId: String. The unique identifier of the group to which the event belongs. If the grouping is by child table, this matches the UID of the corresponding table. When there is no grouping, this field is 0.

##### Fields for Scheduled Triggers

These fields apply when triggerType is Period.

- eventType: Always ON_TIME.
  - result: The computation result, expressed as key–value pairs containing the names of the result columns and their corresponding values.

##### Fields for Sliding Triggers

These fields apply when triggerType is Sliding.

- eventType: Always ON_TIME.
  - result: The computation result, expressed as key–value pairs containing the names of the result columns and their corresponding values.

##### Fields for Time Windows (Interval)

These fields apply when triggerType is Interval.

- If eventType = WINDOW_OPEN, the event object includes:
  - windowStart: Long integer timestamp indicating the window’s start time. Precision matches the time precision of the result table.
- If eventType = WINDOW_CLOSE, the event object includes:
  - windowStart: Long integer timestamp indicating the window’s start time. Precision matches the time precision of the result table.
  - windowEnd: Long integer timestamp indicating the window’s end time. Precision matches the time precision of the result table.

##### Fields for State Windows

These fields apply only when triggerType is State.

- If eventType = WINDOW_OPEN, the event object includes:
  - windowStart: Long integer timestamp indicating the window’s start time. Precision matches the time precision of the result table.
  - prevState: Represents the state key of the previous window, or JSON `NULL` if there is no previous window. When a previous window exists, this field is always a JSON array ordered the same way as the `STATE_WINDOW` arguments. For a single-key state window, the array contains one element. For a multi-key state window, the array contains one element per state key.
  - curState: Represents the state key of the current window. This field is always a JSON array ordered the same way as the `STATE_WINDOW` arguments. For a single-key state window, the array contains one element. For a multi-key state window, the array contains one element per state key.
- If eventType = WINDOW_CLOSE, the event object includes:
  - windowStart: Long integer timestamp indicating the window’s start time. Precision matches the time precision of the result table.
  - windowEnd: Long integer timestamp indicating the window’s end time. Precision matches the time precision of the result table.
  - curState: Represents the state key of the current window. This field is always a JSON array ordered the same way as the `STATE_WINDOW` arguments. For a single-key state window, the array contains one element. For a multi-key state window, the array contains one element per state key.
  - nextState: Represents the state key of the next window. This field is always a JSON array ordered the same way as the `STATE_WINDOW` arguments. For a single-key state window, the array contains one element. For a multi-key state window, the array contains one element per state key.
  - result: The computation result, expressed as key–value pairs containing the names of the result columns and their corresponding values.

##### Fields for Session Windows

These fields apply only when triggerType is Session.

- If eventType = WINDOW_OPEN, the event object includes:
  - windowStart: Long integer timestamp indicating the window’s start time. Precision matches the time precision of the result table.
- If eventType = WINDOW_CLOSE, the event object includes:
  - windowStart: Long integer timestamp indicating the window’s start time. Precision matches the time precision of the result table.
  - windowEnd: Long integer timestamp indicating the window’s end time. Precision matches the time precision of the result table.
  - result: The computation result, expressed as key–value pairs containing the names of the result columns and their corresponding values.

##### Fields for Event Windows

These fields apply only when triggerType is Event.

- If eventType = WINDOW_OPEN, the event object includes:
  - windowStart: Long integer timestamp indicating the window’s start time. Precision matches the time precision of the result table.
  - windowIndex: Integer. The index of a sub-event window within its parent window, starting from 0. For a regular event window or parent window, the value is -1.
  - triggerCondition: Information about the condition that opened the window, including:
    - conditionIndex: Integer. The index of the condition that triggered the window open, starting from 0.
    - fieldValue: Key–value pairs containing the condition column names and their corresponding values.
- If eventType = WINDOW_CLOSE, the event object includes:
  - windowStart: Long integer timestamp indicating the window’s start time. Precision matches the time precision of the result table.
  - windowEnd: Long integer timestamp indicating the window’s end time. Precision matches the time precision of the result table.
  - windowIndex: Integer. The index of a sub-event window within its parent window, starting from 0. For a regular event window or parent window, the value is -1.
  - triggerCondition: Information about the condition that closed the window, including:
    - conditionIndex: Integer. The index of the condition that triggered the window close, starting from 0.
    - fieldValue: Key–value pairs containing the condition column names and their corresponding values.
  - result: The computation result, expressed as key–value pairs containing the names of the result columns and their corresponding values.

##### Fields for Count Windows

These fields apply only when triggerType is Count.

- If eventType = WINDOW_OPEN, the event object includes:
  - windowStart: Long integer timestamp indicating the window’s start time. Precision matches the time precision of the result table.
- If eventType = WINDOW_CLOSE, the event object includes:
  - windowStart: Long integer timestamp indicating the window’s start time. Precision matches the time precision of the result table.
  - windowEnd: Long integer timestamp indicating the window’s end time. Precision matches the time precision of the result table.
  - result: The computation result, expressed as key–value pairs containing the names of the result columns and their corresponding values.

##### Fields for Idle Triggers

These fields apply only when eventType is IDLE or RESUME.

- If eventType = IDLE, the event object includes:
  - idleStart: Long integer. The time of the last data received by the group before it entered idle state. Nanosecond precision Unix epoch.
  - idleEnd: Long integer. The time the IDLE event was triggered. Nanosecond precision Unix epoch.
  - idleDurationMs: Long integer. The duration of idle time in milliseconds, calculated using the monotonic clock.
- If eventType = RESUME, the event object includes:
  - idleStart: Long integer. The timestamp when the idle period began (same as the idleStart of the corresponding IDLE event). Nanosecond precision Unix epoch.
  - idleEnd: Long integer. The time the RESUME event was triggered. Nanosecond precision Unix epoch.
  - idleDurationMs: Long integer. The total duration from idle start to resume in milliseconds, calculated using the monotonic clock.

The IDLE and RESUME events of the same idle cycle for a group share the same `triggerId`, allowing external systems to correlate the two events.

##### Fields for Window Invalidation

During stream processing, out-of-order data, updates, or deletions may cause an already generated window to be removed or require its results to be recalculated. In such cases, a WINDOW_INVALIDATION notification is sent to the target address to indicate which windows have been deleted.

These fields apply only when eventType is WINDOW_INVALIDATION.

- windowStart: Long integer timestamp indicating the window’s start time. Precision matches the time precision of the result table.
- windowEnd: Long integer timestamp indicating the window’s end time. Precision matches the time precision of the result table.

## Delete a Stream

This operation deletes only the stream processing task. Data written by the stream processing task will not be deleted.

```sql
DROP STREAM [IF EXISTS] [db_name.]stream_name [, [db_name.]stream_name] ...
```

## View Streams

### View Stream Information

Displays the stream processing tasks in the current database or in a specified database. You can use `LIKE` to match stream names. See [SHOW STREAMS](../05-tdengine-sql/09-system-info/03-show.md#show-streams) for the full syntax.

```sql
SHOW [db_name.]STREAMS [LIKE 'pattern'];
```

To display the statement used to create a stream (supported from `v3.4.1.13`):

```sql
SHOW CREATE STREAM [db_name.]stream_name;
```

For more detailed information, query [`INS_STREAMS`](../05-tdengine-sql/09-system-info/01-meta.md#ins_streams):

```sql
SELECT * from information_schema.`ins_streams`;
```

For recalculation records, see [`INS_STREAM_RECALCULATES`](../05-tdengine-sql/09-system-info/01-meta.md#ins_stream_recalculates).

### View Stream Tasks

When a stream is running, it is executed as multiple tasks. Detailed task information can be obtained from [`INS_STREAM_TASKS`](../05-tdengine-sql/09-system-info/01-meta.md#ins_stream_tasks):

```sql
SELECT * from information_schema.`ins_stream_tasks`;
```

## Start or Stop a Stream

### Start a Stream

```sql
START STREAM [IF EXISTS] [IGNORE UNTREATED] [db_name.]stream_name; 
```

Notes:

- If IF EXISTS is not specified and the stream does not exist, an error is returned; if the stream exists, the stream processing is started.
- If IF EXISTS is specified and the stream does not exist, the operation returns success; if the stream exists, the stream processing is started.
- After a stream is created, it starts automatically. Manual start is only required if the stream has been stopped and needs to be resumed.
- Without `IGNORE UNTREATED`, data written but not processed while the stream was stopped is processed as historical data after restart. With `IGNORE UNTREATED`, that untreated data is skipped.

### Stop a Stream

```sql
STOP STREAM [IF EXISTS] [db_name.]stream_name; 
```

Notes:

- If IF EXISTS is not specified and the stream does not exist, an error is returned; if the stream exists, the stream processing is stopped.
- If IF EXISTS is specified and the stream does not exist, the operation returns success; if the stream exists, the stream processing is stopped.
- The stop operation is persistent. The stream will not resume until the user explicitly restarts it.
