---
title: Time-Series Extensions
description: TDengine's unique querying capabilities for time series data
slug: /tdengine-reference/sql-manual/time-series-extensions
---

import Image from '@theme/IdealImage';
import imgStep01 from '../../assets/time-series-extensions-01.png';
import imgStep02 from '../../assets/time-series-extensions-02.png';
import imgStep03 from '../../assets/time-series-extensions-03.png';
import imgStep04 from '../../assets/time-series-extensions-04.png';
import imgStep05 from '../../assets/time-series-extensions-05.png';

On top of supporting standard SQL, TDengine also offers a series of unique query syntaxes that meet the needs of time series business scenarios, greatly facilitating the development of applications in these contexts.

The unique queries provided by TDengine include data partitioning queries and time window partitioning queries.

## Data Partitioning Queries

When there is a need to partition data by certain dimensions and then perform a series of calculations within the partitioned data space, the data partitioning clause is used. The syntax for data partitioning statements is as follows:

```sql
PARTITION BY part_list
```

`part_list` can be any scalar expression, including columns, constants, scalar functions, and their combinations. For example, to group data by the tag `location` and calculate the average voltage for each group:

```sql
select location, avg(voltage) from meters partition by location
```

TDengine handles data partitioning clauses as follows:

- The data partitioning clause is located after the WHERE clause.
- The data partitioning clause partitions table data by the specified dimensions, performing specified calculations on each partitioned fragment. The calculations are defined by subsequent clauses (window clauses, GROUP BY clauses, or SELECT clauses).
- The data partitioning clause can be used in conjunction with window partitioning clauses (or GROUP BY clauses), in which case the subsequent clauses operate on each partitioned fragment. For example, group data by the tag `location` and downsample each group by 10 minutes, taking the maximum value.

```sql
select _wstart, location, max(current) from meters partition by location interval(10m)
```

The most common usage of the data partitioning clause is in querying supertables, where subtable data is partitioned by tags and then calculated separately. Particularly, the use of `PARTITION BY TBNAME` isolates each subtable's data, forming independent time series, greatly facilitating statistical analysis in various time series scenarios. For instance, to calculate the average voltage for each meter every 10 minutes:

```sql
select _wstart, tbname, avg(voltage) from meters partition by tbname interval(10m)
```

## Window Partitioning Queries

TDengine supports aggregate result queries using time window partitioning, such as querying the average temperature every 10 minutes when temperature sensors collect data every second. In such scenarios, the window clause can be used to obtain the desired query results. The window clause is used to partition the queried data set into query subsets based on windows and perform aggregation, which includes five types of windows: time windows, state windows, session windows, event windows, and count windows. Among them, time windows can be further divided into sliding time windows and tumbling time windows.

The syntax for the window clause is as follows:

```sql
window_clause: {
    SESSION(ts_col, tol_val)
  | STATE_WINDOW(col)
  | INTERVAL(interval_val [, interval_offset]) [SLIDING (sliding_val)] [FILL(fill_mod_and_val)]
  | EVENT_WINDOW START WITH start_trigger_condition END WITH end_trigger_condition
  | COUNT_WINDOW(count_val[, sliding_val])
}
```

Here, `interval_val` and `sliding_val` both represent time periods, and `interval_offset` represents the window offset, which must be less than `interval_val`. The syntax supports three forms, illustrated as follows:

- `INTERVAL(1s, 500a) SLIDING(1s)`, a form with built-in time units, where the time unit is represented by a single character: a (milliseconds), b (nanoseconds), d (days), h (hours), m (minutes), n (months), s (seconds), u (microseconds), w (weeks), y (years).
- `INTERVAL(1000, 500) SLIDING(1000)`, a form without time units, which uses the query database's time precision as the default time unit, adopting the higher precision from multiple databases.
- `INTERVAL('1s', '500a') SLIDING('1s')`, a string form with built-in time units, where the string cannot contain any spaces or other characters.

### Rules for the Window Clause

- The window clause follows the data partitioning clause and cannot be used with GROUP BY clauses.
- The window clause partitions the data by windows and calculates expressions in the SELECT list for each window. Expressions in the SELECT list may include:
  - Constants.
  - `_wstart`, `_wend`, and `_wduration` pseudo-columns.
  - Aggregate functions (including selection functions and time series-specific functions that can determine output row numbers by parameters).
  - Expressions that include the above expressions.
  - At least one aggregate function must be included.
- The window clause cannot be used with GROUP BY clauses.
- The WHERE clause can specify the start and end time of the query and other filter conditions.

### FILL Clause

The FILL statement specifies the filling mode for cases where data is missing in a certain window range. The filling modes include the following:

1. No filling: NONE (default filling mode).
2. VALUE filling: Filling with a fixed value, where the filling value must be specified. For example: FILL(VALUE, 1.23). It is important to note that the final filling value is determined by the type of the corresponding column. For instance, if FILL(VALUE, 1.23) is used, and the corresponding column is of INT type, the filling value will be 1. If multiple columns in the query list require FILL, each FILL column must specify VALUE, such as `SELECT _wstart, min(c1), max(c1) FROM ... FILL(VALUE, 0, 0)`.
3. PREV filling: Filling data using the previous non-NULL value. For example: FILL(PREV).
4. NULL filling: Filling data with NULL. For example: FILL(NULL).
5. LINEAR filling: Filling data based on linear interpolation using the nearest non-NULL values on both sides. For example: FILL(LINEAR).
6. NEXT filling: Filling data with the next non-NULL value. For example: FILL(NEXT).
   Among the above filling modes, except for NONE mode, which defaults to no filling values, other modes will be ignored if there is no data within the entire query time range, resulting in no filling data and an empty query result. This behavior makes sense for some modes (PREV, NEXT, LINEAR) because having no data means no fill value can be produced. However, for other modes (NULL, VALUE), theoretically, fill values can be produced, and whether to output the fill values depends on application needs. To accommodate applications that require forced filling of data or NULL, while maintaining compatibility with existing filling behavior, two new filling modes have been added since version 3.0.3.0:
7. NULL_F: Force filling NULL values.
8. VALUE_F: Force filling VALUE values.

The distinctions between NULL, NULL_F, VALUE, and VALUE_F filling modes for different scenarios are as follows:

- INTERVAL clause: NULL_F and VALUE_F are forced filling modes; NULL and VALUE are non-forced modes, where their semantics match their names.
- Stream computing INTERVAL clause: NULL_F and NULL behave the same and are both non-forced modes; VALUE_F and VALUE behave the same and are both non-forced modes, meaning stream computing INTERVAL has no forced modes.
- INTERP clause: NULL and NULL_F behave the same and are both forced modes; VALUE and VALUE_F behave the same and are both forced modes, meaning there are no non-forced modes in INTERP.

:::info

1. Using the FILL statement may generate a large amount of filling output; always specify the time range for the query. The system can return no more than 10 million rows with interpolated results for each query.
2. In time dimension aggregation, the returned results have a strictly monotonically increasing time series.
3. If the query target is a supertable, the aggregate functions will act on all tables under the supertable that meet the value filtering criteria. If the query does not use a PARTITION BY clause, the results will be returned in a strictly monotonically increasing time series; if the query uses a PARTITION BY clause for grouping, the results within each PARTITION will be returned in a strictly monotonically increasing time series.

:::

### Time Windows

Time windows can be further divided into sliding time windows and tumbling time windows.

The INTERVAL clause is used to produce equal time period windows, while SLIDING specifies the time for the window to slide forward. Each executed query is a time window that moves forward in time. When defining continuous queries, it is necessary to specify the size of the time window and the forward sliding time for each execution. As shown in the figure, [t0s, t0e], [t1s, t1e], and [t2s, t2e] represent the time window ranges for executing three continuous queries, with the sliding time range indicating the forward sliding of the window. Query filtering, aggregation, and other operations are executed independently for each time window. When SLIDING is equal to INTERVAL, the sliding window is equivalent to the tumbling window.

<figure>
<Image img={imgStep01} alt=""/>
</figure>

The INTERVAL and SLIDING clauses need to be used in conjunction with aggregate and selection functions. The following SQL statement is invalid:

```sql
SELECT * FROM temp_tb_1 INTERVAL(1m);
```

The sliding time of SLIDING cannot exceed the time range of one window. The following statement is invalid:

```sql
SELECT COUNT(*) FROM temp_tb_1 INTERVAL(1m) SLIDING(2m);
```

When using time windows, the following should be noted:

- The width of the aggregation time window is specified by the INTERVAL keyword, with a minimum time interval of 10 milliseconds (10a); it also supports offsets (the offset must be less than the interval), i.e., the window division is compared to "UTC time 0". The SLIDING statement is used to specify the forward increment of the aggregation time period, i.e., how long the window slides forward each time.
- When using the INTERVAL statement, unless in very special circumstances, it is required that the `timezone` parameters in the taos.cfg configuration files of both the client and server be set to the same value to avoid severe performance impacts caused by frequent time zone conversions in time processing functions.
- The returned results have a strictly monotonically increasing time series.

### State Windows

State windows are defined using integers (boolean values) or strings to identify the state variable of the device when records are generated. Records with the same state variable value belong to the same state window, and the window closes when the value changes. As shown in the figure below, the state windows determined by the state variable are [2019-04-28 14:22:07, 2019-04-28 14:22:10] and [2019-04-28 14:22:11, 2019-04-28 14:22:12].

<figure>
<Image img={imgStep02} alt=""/>
</figure>

The `STATE_WINDOW` is used to determine the column that defines the state window. For example:

```sql
SELECT COUNT(*), FIRST(ts), status FROM temp_tb_1 STATE_WINDOW(status);
```

To focus only on the state window information where `status` is 2:

```sql
SELECT * FROM (SELECT COUNT(*) AS cnt, FIRST(ts) AS fst, status FROM temp_tb_1 STATE_WINDOW(status)) t WHERE status = 2;
```

TDengine also supports using the CASE expression in the state variable, which can express that the start of a certain state is triggered by meeting a certain condition, and the end of that state is triggered by meeting another condition. For example, the normal voltage range for a smart meter is 205V to 235V, and monitoring the voltage can determine whether the circuit is normal.

```sql
SELECT tbname, _wstart, CASE WHEN voltage >= 205 and voltage <= 235 THEN 1 ELSE 0 END status FROM meters PARTITION BY tbname STATE_WINDOW(CASE WHEN voltage >= 205 and voltage <= 235 THEN 1 ELSE 0 END);
```

### Session Windows

Session windows are determined based on the values of the timestamp primary key of records to determine whether they belong to the same session. As shown in the figure below, if the continuous interval of the timestamps is less than or equal to 12 seconds, the following 6 records form 2 session windows: [2019-04-28 14:22:10, 2019-04-28 14:22:30] and [2019-04-28 14:23:10, 2019-04-28 14:23:30]. This is because the interval between 2019-04-28 14:22:30 and 2019-04-28 14:23:10 is 40 seconds, exceeding the continuous interval (12 seconds).

<figure>
<Image img={imgStep03} alt=""/>
</figure>

Records within the `tol_value` time interval are considered to belong to the same window; if the timestamps of two consecutive records exceed `tol_val`, a new window is automatically opened.

```sql
SELECT COUNT(*), FIRST(ts) FROM temp_tb_1 SESSION(ts, tol_val);
```

### Event Windows

Event windows are defined based on start and end conditions. When `start_trigger_condition` is met, the window starts, and it closes when `end_trigger_condition` is met. Both `start_trigger_condition` and `end_trigger_condition` can be any TDengine-supported conditional expressions and can include different columns.

An event window can contain only one record. That is, when a record satisfies both `start_trigger_condition` and `end_trigger_condition` at the same time and is not currently in a window, that record forms its own window.

If an event window cannot close, it does not constitute a window and will not be output. For example, if data satisfies `start_trigger_condition` and the window opens, but subsequent data cannot satisfy `end_trigger_condition`, this window cannot close, and the corresponding data does not constitute a window and will not be output.

If an event window query is performed directly on a supertable, TDengine will aggregate the data of the supertable into a single time line and then perform event window calculations. If an event window query is needed on the result set of a subquery, the result set must meet the requirements of outputting in a time line and can output valid timestamp columns.

For example, the following SQL statement illustrates the event window partitioning:

```sql
select _wstart, _wend, count(*) from t event_window start with c1 > 0 end with c2 < 10 
```

<figure>
<Image img={imgStep04} alt=""/>
</figure>

### Count Windows

Count windows partition the data based on a fixed number of rows. By default, the data is sorted by timestamp, and the data is divided into multiple windows according to the value of `count_val`, followed by aggregation calculations. `count_val` represents the maximum number of data rows included in each count window; if the total number of data rows is not divisible by `count_val`, the last window will have fewer rows than `count_val`. `sliding_val` is a constant that represents the number of rows the window slides, similar to the SLIDING clause in INTERVAL.

The following SQL statement illustrates the count window partitioning:

```sql
select _wstart, _wend, count(*) from t count_window(4);
```

<figure>
<Image img={imgStep05} alt=""/>
</figure>

### Timestamp Pseudo Columns

In the results of window aggregate queries, if the SQL statement does not specify the timestamp columns to be output in the query results, the final results will not automatically include the time column information of the windows. If you need to output the time window information corresponding to the aggregate results in the results, you need to use timestamp-related pseudo-columns in the SELECT clause: the start time of the time window (\_WSTART), the end time of the time window (\_WEND), the duration of the time window (\_WDURATION), as well as the pseudo-columns related to the overall query window: the start time of the query window (\_QSTART) and the end time of the query window (\_QEND). It is important to note that both the start and end times of the time window are closed intervals, and the duration of the time window is the value under the current time resolution of the data. For example, if the current database's time resolution is milliseconds, then a result of 500 indicates that the current time window's duration is 500 milliseconds (500 ms).

### Example

The table creation statement for smart meters is as follows:

```sql
CREATE TABLE meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT);
```

For the data collected from smart meters, calculate the average, maximum, and median of current data over the past 24 hours, using 10 minutes as a phase. If no computed value is available, fill it with the previous non-NULL value. The query statement used is as follows:

```sql
SELECT _WSTART, _WEND, AVG(current), MAX(current), APERCENTILE(current, 50) FROM meters
  WHERE ts>=NOW-1d and ts<=now
  INTERVAL(10m)
  FILL(PREV);
```
