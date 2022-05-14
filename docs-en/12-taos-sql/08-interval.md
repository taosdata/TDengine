---
sidebar_label: Window
title: Aggregate by Window
---

Aggregate by time window is supported in TDengine. For example, each temperature sensor reports the temperature every second, the average temperature every 10 minutes can be retrieved by query with time window.
Window related clauses are used to divide the data set to be queried into subsets and then aggregate. There are three kinds of windows, time window, status window, and session window. There are two kinds of time windows, sliding window and flip time window.

## Time Window

`INTERVAL` claused is used to generate time windows of same time interval, `SLIDING` is used to specify the time step for which the time window moves forward. The query is performed on one time window each time, and the time window moves forward with time. When defining continuous query both the size of time window and the step of forward sliding time need to be specified. As shown in the figure blow, [t0s, t0e] ，[t1s , t1e]， [t2s, t2e] are respectively the time range of three time windows on which continuous queries are executed. The time step for which time window moves forward is marked by `sliding time`. Query, filter and aggregate operations are executed on each time window respectively. When the time step specified by `SLIDING` is same as the time interval specified by `INTERVAL`, the sliding time window is actually a flip time window.

![Time Window](/img/sql/timewindow-1.png)

`INTERVAL` and `SLIDING` should be used with aggregate functions and selection functions. Below SQL statement is illegal because no aggregate or selection function is used with `INTERVAL`.

```
SELECT * FROM temp_tb_1 INTERVAL(1m);
```

The time step specified by `SLIDING` can't exceed the time interval specified by `INTERVAL`. Below SQL statement is illegal because the time length specified by `SLIDING` exceeds that specified by `INTERVAL`.

```
SELECT COUNT(*) FROM temp_tb_1 INTERVAL(1m) SLIDING(2m);
```

When the time length specified by `SLIDING` is same as that specified by `INTERVAL`, sliding window is actually flip window. The minimum time range specified by `INTERVAL` is 10 milliseconds (10a) prior to version 2.1.5.0. From version 2.1.5.0, the minimum time range by `INTERVAL` can be 1 microsecond (1u). However, if the DB precision is millisecond, the minimum time range is 1 millisecond (1a). Please be noted that the `timezone` parameter should be configured to same value in the `taos.cfg` configuration file on client side and server side.

## Status Window

In case of using integer, bool, or string to represent the device status at a moment, the continuous rows with same status belong to same status window. Once the status changes, the status window closes. As shown in the following figure，there are two status windows according to status, [2019-04-28 14:22:07，2019-04-28 14:22:10] and [2019-04-28 14:22:11，2019-04-28 14:22:12]. Status window is not applicable to stable for now.

![Status Window](/img/sql/timewindow-3.png)

`STATE_WINDOW` is used to specify the column based on which to define status window, for example：

```
SELECT COUNT(*), FIRST(ts), status FROM temp_tb_1 STATE_WINDOW(status);
```

## Session Window

```sql
SELECT COUNT(*), FIRST(ts) FROM temp_tb_1 SESSION(ts, tol_val);
```

The primary key, i.e. timestamp, is used to determine which session window the row belongs to. If the time interval between two adjacent rows is within the time range specified by `tol_val`, they belong to same session window; otherwise they belong to two different time windows. As shown in the figure below, if the limit of time interval for session window is specified as 12 seconds, then the 6 rows in the figure constitutes 2 time windows, [2019-04-28 14:22:10，2019-04-28 14:22:30] and [2019-04-28 14:23:10，2019-04-28 14:23:30], because the time difference between 2019-04-28 14:22:30 and 2019-04-28 14:23:10 is 40 seconds, which exceeds the time interval limit of 12 seconds.

![Session Window](/img/sql/timewindow-2.png)

If the time interval between two continuous rows are withint the time interval specified by `tol_value` they belong to the same session window; otherwise a new session window is started automatically. Session window is not supported on stable for now.

## More On Window Aggregate

### Syntax

The full syntax of aggregate by window is as following:

```sql
SELECT function_list FROM tb_name
  [WHERE where_condition]
  [SESSION(ts_col, tol_val)]
  [STATE_WINDOW(col)]
  [INTERVAL(interval [, offset]) [SLIDING sliding]]
  [FILL({NONE | VALUE | PREV | NULL | LINEAR | NEXT})]

SELECT function_list FROM stb_name
  [WHERE where_condition]
  [INTERVAL(interval [, offset]) [SLIDING sliding]]
  [FILL({NONE | VALUE | PREV | NULL | LINEAR | NEXT})]
  [GROUP BY tags]
```

### Restrictions

- Aggregate functions and selection functions can be used in `function_list`, with each function having only one output, for example COUNT, AVG, SUM, STDDEV, LEASTSQUARES, PERCENTILE, MIN, MAX, FIRST, LAST. Functions having multiple ouput can't be used, for example DIFF or arithmetic operations.
- `LAST_ROW` can't be used together with window aggregate.
- Scalar functions, like CEIL/FLOOR, can't be used with window aggregate.
- `WHERE` clause can be used to specify the starting and ending time and other filter conditions
- `FILL` clause is used to specify how to fill when there is data missing in any window, including: \
  1. NONE: No fill (the default fill mode)
  2. VALUE：Fill with a fixed value, which should be specified together, for example `FILL(VALUE, 1.23)`
  3. PREV：Fill with the previous non-NULL value, `FILL(PREV)`
  4. NULL：Fill with NULL, `FILL(NULL)`
  5. LINEAR：Fill with the closest non-NULL value, `FILL(LINEAR)`
  6. NEXT：Fill with the next non-NULL value, `FILL(NEXT)`

:::info

1. Huge volume of interpolation output may be returned using `FILL`, so it's recommended to specify the time range when using `FILL`. The maximum interpolation values that can be returned in single query is 10,000,000.
2. The result set is in the ascending order of timestamp in aggregate by time window aggregate.
3. If aggregate by window is used on stable, the aggregate function is performed on all the rows matching the filter conditions. If `GROUP BY` is not used in the query, the result set will be returned in ascending order of timestamp; otherwise the result set is not exactly in the order of ascending timestamp in each group.
   :::

Aggregate by time window is also used in continuous query, please refer to [Continuous Query](/develop/continuous-query).

## Examples

The table of intelligent meters can be created like below SQL statement：

```sql
CREATE TABLE meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT);
```

The average current, maximum current and median of current in every 10 minutes of the past 24 hours can be calculated using below SQL statement, with missing value filled with the previous non-NULL value.

```
SELECT AVG(current), MAX(current), APERCENTILE(current, 50) FROM meters
  WHERE ts>=NOW-1d and ts<=now
  INTERVAL(10m)
  FILL(PREV);
```
