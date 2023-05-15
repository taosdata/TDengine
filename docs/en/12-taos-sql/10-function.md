---
title: Functions
sidebar_label: Functions
description: This document describes the standard SQL functions available in TDengine.
toc_max_heading_level: 4
---

## Scalar Functions

Scalar functions return one result for each row.

### Mathematical Functions

#### ABS

```sql
ABS(expr)
```

**Description**: The absolute value of a specific field.

**Return value type**: Same as the field being used

**Applicable data types**: Numeric

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Usage**: This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.

#### ACOS

```sql
ACOS(expr)
```

**Description**: The arc cosine of a specific field.

**Return value type**: Double

**Applicable data types**: Numeric

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Usage**: This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.

#### ASIN

```sql
ASIN(expr)
```

**Description**: The arc sine of a specific field.

**Return value type**: Double

**Applicable data types**: Numeric

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Usage**: This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.


#### ATAN

```sql
ATAN(expr)
```

**Description**: The arc tangent of a specific field.

**Return value type**: Double

**Applicable data types**: Numeric

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Usage**: This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.


#### CEIL

```sql
CEIL(expr)
```

**Description**: The rounded up value of a specific field

**Return value type**: Same as the field being used

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Usage**: This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.

#### COS

```sql
COS(expr)
```

**Description**: The cosine of a specific field.

**Return value type**: Double

**Applicable data types**: Numeric

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Usage**: This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.

#### FLOOR

```sql
FLOOR(expr)
```

**Description**: The rounded down value of a specific field
 **More explanations**: The restrictions are same as those of the `CEIL` function.

#### LOG

```sql
LOG(expr [, base])
```

**Description**: The logarithm of a specific field with `base` as the radix. If you do not enter a base, the natural logarithm of the field is returned.

**Return value type**: Double

**Applicable data types**: Numeric

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Usage**: This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.


#### POW

```sql
POW(expr, power)
```

**Description**: The power of a specific field with `power` as the exponent.

**Return value type**: Double

**Applicable data types**: Numeric

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Usage**: This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.


#### ROUND

```sql
ROUND(expr)
```

**Description**: The rounded value of a specific field.
 **More explanations**: The restrictions are same as those of the `CEIL` function.


#### SIN

```sql
SIN(expr)
```

**Description**: The sine of a specific field.

**Return value type**: Double

**Applicable data types**: Numeric

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Usage**: This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.

#### SQRT

```sql
SQRT(expr)
```

**Description**: The square root of a specific field.

**Return value type**: Double

**Applicable data types**: Numeric

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Usage**: This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.

#### TAN

```sql
TAN(expr)
```

**Description**: The tangent of a specific field.

**Return value type**: Double

**Applicable data types**: Numeric

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Usage**: This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.

### Concatenation Functions

Concatenation functions take strings as input and produce string or numeric values as output.

#### CHAR_LENGTH

```sql
CHAR_LENGTH(expr)
```

**Description**: The length in number of characters of a string

**Return value type**: Bigint

**Applicable data types**: VARCHAR and NCHAR

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

#### CONCAT

```sql
CONCAT(expr1, expr2 [, expr] ...)
```

**Description**: The concatenation result of two or more strings

**Return value type**: If the concatenated strings are VARCHARs, the result is a VARCHAR. If the concatenated strings are NCHARs, the result is an NCHAR. If an input value is null, the result is null.

**Applicable data types**: VARCHAR and NCHAR You can concatenate between 2 and 8 strings.

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables


#### CONCAT_WS

```sql
CONCAT_WS(separator_expr, expr1, expr2 [, expr] ...)
```

**Description**: The concatenation result of two or more strings with separator

**Return value type**: If the concatenated strings are VARCHARs, the result is a VARCHAR. If the concatenated strings are NCHARs, the result is an NCHAR. If an input value is null, the result is null.

**Applicable data types**: VARCHAR and NCHAR You can concatenate between 3 and 9 strings.

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables


#### LENGTH

```sql
LENGTH(expr)
```

**Description**: The length in bytes of a string

**Return value type**: Bigint

**Applicable data types**: VARCHAR and NCHAR fields or columns

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables


#### LOWER

```sql
LOWER(expr)
```

**Description**: Convert the input string to lower case

**Return value type**: Same as input

**Applicable data types**: VARCHAR and NCHAR

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables


#### LTRIM

```sql
LTRIM(expr)
```

**Description**: Remove the left leading blanks of a string

**Return value type**: Same as input

**Applicable data types**: VARCHAR and NCHAR

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables


#### RTRIM

```sql
LTRIM(expr)
```

**Description**: Remove the right tailing blanks of a string

**Return value type**: Same as input

**Applicable data types**: VARCHAR and NCHAR

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables


#### SUBSTR

```sql
SUBSTR(expr, pos [, len])
```

**Description**: The sub-string starting from `pos` with length of `len` from the original string `str` - If `len` is not specified, it means from `pos` to the end.

**Return value type**: Same as input

**Applicable data types**: VARCHAR and NCHAR Parameter `pos` can be an positive or negative integer; If it's positive, the starting position will be counted from the beginning of the string; if it's negative, the starting position will be counted from the end of the string.

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: table, STable


#### UPPER

```sql
UPPER(expr)
```

**Description**: Convert the input string to upper case

**Return value type**: Same as input

**Applicable data types**: VARCHAR and NCHAR

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: table, STable


### Conversion Functions

Conversion functions change the data type of a value.

#### CAST

```sql
CAST(expr AS type_name)
```

**Description**: Convert the input data `expr` into the type specified by `type_name`. This function can be used only in SELECT statements.

**Return value type**: The type specified by parameter `type_name`

**Applicable data types**: All data types except JSON

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**More explanations**:

- Error will be reported for unsupported type casting
- Some values of some supported data types may not be casted, below are known issues:
        1. Some strings cannot be converted to numeric values. For example, the string `a` may be converted to `0`. However, this does not produce an error.
        2. If a converted numeric value is larger than the maximum size for the specified type, an overflow will occur. However, this does not produce an error.
        3. If a converted string value is larger than the maximum size for the specified type, the output value will be truncated. However, this does not produce an error.

#### TO_ISO8601

```sql
TO_ISO8601(expr [, timezone])
```

**Description**: The ISO8601 date/time format converted from a UNIX timestamp, plus the timezone. You can specify any time zone with the timezone parameter. If you do not enter this parameter, the time zone on the client is used.

**Return value type**: VARCHAR

**Applicable data types**: Integers and timestamps

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**More explanations**:

- You can specify a time zone in the following format: [z/Z, +/-hhmm, +/-hh, +/-hh:mm]. For example, TO_ISO8601(1, "+00:00").
- If the input is a UNIX timestamp, the precision of the returned value is determined by the digits of the input timestamp
- If the input is a column of TIMESTAMP type, the precision of the returned value is same as the precision set for the current data base in use


#### TO_JSON

```sql
TO_JSON(str_literal)
```

**Description**: Converts a string into JSON.

**Return value type**: JSON

**Applicable data types**: JSON strings in the form `{"literal": literal}`. `{}` indicates a null value. The key must be a string literal. The value can be a numeric literal, string literal, Boolean literal, or null literal. str_literal cannot include escape characters.

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: table, STable


#### TO_UNIXTIMESTAMP

```sql
TO_UNIXTIMESTAMP(expr [, return_timestamp])

return_timestamp: {
    0
  | 1
}
```

**Description**: UNIX timestamp converted from a string of date/time format

**Return value type**: BIGINT, TIMESTAMP

**Applicable column types**: VARCHAR and NCHAR

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**More explanations**:

- The input string must be compatible with ISO8601/RFC3339 standard, NULL will be returned if the string can't be converted
- The precision of the returned timestamp is same as the precision set for the current data base in use
- return_timestamp indicates whether the returned value type is TIMESTAMP or not. If this parameter set to 1, function will return TIMESTAMP type. Otherwise function will return BIGINT type. If parameter is omitted, default return value type is BIGINT.


### Time and Date Functions

These functions perform operations on times and dates.

All functions that return the current time, such as `NOW`, `TODAY`, and `TIMEZONE`, are calculated only once per statement even if they appear multiple times.

#### NOW

```sql
NOW()
```

**Description**: The current time of the client side system

**Return value type**: TIMESTAMP

**Applicable column types**: TIMESTAMP only

**Applicable table types**: standard tables and supertables

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**More explanations**:

- Add and Subtract operation can be performed, for example NOW() + 1s, the time unit can be:
        b(nanosecond), u(microsecond), a(millisecond)), s(second), m(minute), h(hour), d(day), w(week)
- The precision of the returned timestamp is same as the precision set for the current data base in use


#### TIMEDIFF

```sql
TIMEDIFF(expr1, expr2 [, time_unit])
```

**Description**: The difference between two timestamps, and rounded to the time unit specified by `time_unit`

**Return value type**: BIGINT

**Applicable column types**: UNIX-style timestamps in BIGINT and TIMESTAMP format and other timestamps in VARCHAR and NCHAR format

**Applicable table types**: standard tables and supertables

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**More explanations**:
- Time unit specified by `time_unit` can be:
          1b (nanoseconds), 1u (microseconds), 1a (milliseconds), 1s (seconds), 1m (minutes), 1h (hours), 1d (days), or 1w (weeks)
- The precision of the returned timestamp is same as the precision set for the current data base in use
- If the input data is not formatted as a timestamp, the returned value is null.


#### TIMETRUNCATE

```sql
TIMETRUNCATE(expr, time_unit [, ignore_timezone])

ignore_timezone: {
    0
  | 1
}
```

**Description**: Truncate the input timestamp with unit specified by `time_unit`

**Return value type**: TIMESTAMP

**Applicable column types**: UNIX-style timestamps in BIGINT and TIMESTAMP format and other timestamps in VARCHAR and NCHAR format

**Applicable table types**: standard tables and supertables

**More explanations**:
- Time unit specified by `time_unit` can be:
          1b (nanoseconds), 1u (microseconds), 1a (milliseconds), 1s (seconds), 1m (minutes), 1h (hours), 1d (days), or 1w (weeks)
- The precision of the returned timestamp is same as the precision set for the current data base in use
- If the input data is not formatted as a timestamp, the returned value is null.
- If `1d` is used as `time_unit` to truncate the timestamp, `ignore_timezone` option can be set to indicate if the returned result is affected by client timezone or not.
  For example, if client timezone is set to UTC+0800, TIMETRUNCATE('2020-01-01 23:00:00', 1d, 0) will return '2020-01-01 08:00:00'.
  Otherwise, TIMETRUNCATE('2020-01-01 23:00:00', 1d, 1) will return '2020-01-01 00:00:00'.
  If `ignore_timezone` option is omitted, the default value is set to 1.

#### TIMEZONE

```sql
TIMEZONE()
```

**Description**: The timezone of the client side system

**Applicable data types**: VARCHAR

**Applicable column types**: None

**Applicable table types**: standard tables and supertables


#### TODAY

```sql
TODAY()
```

**Description**: The timestamp of 00:00:00 of the client side system

**Return value type**: TIMESTAMP

**Applicable column types**: TIMESTAMP only

**Applicable table types**: standard tables and supertables

**More explanations**:

- Add and Subtract operation can be performed, for example TODAY() + 1s, the time unit can be:
                b(nanosecond), u(microsecond), a(millisecond)), s(second), m(minute), h(hour), d(day), w(week)
- The precision of the returned timestamp is same as the precision set for the current data base in use


## Aggregate Functions

Aggregate functions return one row per group. You can use windows or GROUP BY to group data. Otherwise, the entire query is considered a single group.

TDengine supports the following aggregate functions:

### APERCENTILE

```sql
APERCENTILE(expr, p [, algo_type])

algo_type: {
    "default"
  | "t-digest"
}
```

**Description**: Similar to `PERCENTILE`, but a simulated result is returned

**Return value type**: DOUBLE

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables

**Explanations**:
- _p_ is in range [0,100], when _p_ is 0, the result is same as using function MIN; when _p_ is 100, the result is same as function MAX.
- `algo_type` can only be input as `default` or `t-digest` Enter `default` to use a histogram-based algorithm. Enter `t-digest` to use the t-digest algorithm to calculate the approximation of the quantile. `default` is used by default.
- The approximation result of `t-digest` algorithm is sensitive to input data order. For example, when querying STable with different input data order there might be minor differences in calculated results.

### AVG

```sql
AVG(expr)
```

**Description**: The average value of the specified fields.

**Return value type**: DOUBLE

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables


### COUNT

```sql
COUNT({* | expr})
```

**Description**: The number of records in the specified fields.

**Return value type**: BIGINT

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables

**More explanation**:

- Wildcard (\*) is used to represent all columns.
If you input a specific column, the number of non-null values in the column is returned.


### ELAPSED

```sql
ELAPSED(ts_primary_key [, time_unit])
```

**Description**: `elapsed` function can be used to calculate the continuous time length in which there is valid data. If it's used with `INTERVAL` clause, the returned result is the calculated time length within each time window. If it's used without `INTERVAL` caluse, the returned result is the calculated time length within the specified time range. Please be noted that the return value of `elapsed` is the number of `time_unit` in the calculated time length.

**Return value type**: Double if the input value is not NULL;

**Return value type**: TIMESTAMP

**Applicable tables**: table, STable, outer in nested query

**Explanations**:
- `ts_primary_key` parameter can only be the first column of a table, i.e. timestamp primary key.
- The minimum value of `time_unit` is the time precision of the database. If `time_unit` is not specified, the time precision of the database is used as the default time unit. Time unit specified by `time_unit` can be:
          1b (nanoseconds), 1u (microseconds), 1a (milliseconds), 1s (seconds), 1m (minutes), 1h (hours), 1d (days), or 1w (weeks)
- It can be used with `INTERVAL` to get the time valid time length of each time window. Please be noted that the return value is same as the time window for all time windows except for the first and the last time window.
- `order by asc/desc` has no effect on the result.
- `group by tbname` must be used together when `elapsed` is used against a STable.
- `group by` must NOT be used together when `elapsed` is used against a table or sub table.
- When used in nested query, it's only applicable when the inner query outputs an implicit timestamp column as the primary key. For example, `select elapsed(ts) from (select diff(value) from sub1)` is legal usage while `select elapsed(ts) from (select * from sub1)` is not. In addition, because elapsed has a strict dependency on the timeline, a statement like `select elapsed(ts) from (select diff(value) from st group by tbname) will return a meaningless result.
- It can't be used with `leastsquares`, `diff`, `derivative`, `top`, `bottom`, `last_row`, `interp`.


### LEASTSQUARES

```sql
LEASTSQUARES(expr, start_val, step_val)
```

**Description**: The linear regression function of the specified column and the timestamp column (primary key), `start_val` is the initial value and `step_val` is the step value.

**Return value type**: A string in the format of "(slope, intercept)"

**Applicable data types**: Numeric

**Applicable table types**: table only


### SPREAD

```sql
SPREAD(expr)
```

**Description**: The difference between the max and the min of a specific column

**Return value type**: DOUBLE

**Applicable data types**: Integers and timestamps

**Applicable table types**: standard tables and supertables


### STDDEV

```sql
STDDEV(expr)
```

**Description**: Standard deviation of a specific column in a table or STable

**Return value type**: DOUBLE

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables


### SUM

```sql
SUM(expr)
```

**Description**: The sum of a specific column in a table or STable

**Return value type**: DOUBLE or BIGINT

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables


### HYPERLOGLOG

```sql
HYPERLOGLOG(expr)
```

**Description**:
  The cardinal number of a specific column is returned by using hyperloglog algorithm. The benefit of using hyperloglog algorithm is that the memory usage is under control when the data volume is huge.
  However, when the data volume is very small, the result may be not accurate, it's recommended to use `select count(data) from (select unique(col) as data from table)` in this case.

**Return value type**: Integer

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables


### HISTOGRAM

```sql
HISTOGRAM(expr, bin_type, bin_description, normalized)
```

**Description**: Returns count of data points in user-specified ranges.

**Return value type** If normalized is set to 1, a DOUBLE is returned; otherwise a BIGINT is returned

**Applicable data types**: Numeric

**Applicable table types**: table, STable

**Explanations**:
- bin_type: parameter to indicate the bucket type, valid inputs are: "user_input", "linear_bin", "log_bin".
- bin_description: parameter to describe how to generate buckets can be in the following JSON formats for each bin_type respectively:
    - "user_input": "[1, 3, 5, 7]":
       User specified bin values.

    - "linear_bin": "{"start": 0.0, "width": 5.0, "count": 5, "infinity": true}"
       "start" - bin starting point.       "width" - bin offset.       "count" - number of bins generated.       "infinity" - whether to add (-inf, inf) as start/end point in generated set of bins.
       The above "linear_bin" descriptor generates a set of bins: [-inf, 0.0, 5.0, 10.0, 15.0, 20.0, +inf].

    - "log_bin": "{"start":1.0, "factor": 2.0, "count": 5, "infinity": true}"
       "start" - bin starting point.       "factor" - exponential factor of bin offset.       "count" - number of bins generated.       "infinity" - whether to add (-inf, inf) as start/end point in generated range of bins.
       The above "linear_bin" descriptor generates a set of bins: [-inf, 1.0, 2.0, 4.0, 8.0, 16.0, +inf].
- normalized: setting to 1/0 to turn on/off result normalization. Valid values are 0 or 1.


### PERCENTILE

```sql
PERCENTILE(expr, p [, p1] ...)
```

**Description**: The value whose rank in a specific column matches the specified percentage. If such a value matching the specified percentage doesn't exist in the column, an interpolation value will be returned.

**Return value type**: This function takes 2 minimum and 11 maximum parameters, and it can simultaneously return 10 percentiles at most. If 2 parameters are given, a single percentile is returned and the value type is DOUBLE.
                       If more than 2 parameters are given, the return value type is a VARCHAR string, the format of which is a JSON ARRAY containing all return values.

**Applicable column types**: Numeric

**Applicable table types**: table only

**More explanations**:

- _p_ is in range [0,100], when _p_ is 0, the result is same as using function MIN; when _p_ is 100, the result is same as function MAX.
- When calculating multiple percentiles of a specific column, a single PERCENTILE function with multiple parameters is advised, as this can largely reduce the query response time.
  For example, using SELECT percentile(col, 90, 95, 99) FROM table will perform better than SELECT percentile(col, 90), percentile(col, 95), percentile(col, 99) from table.

## Selection Functions

Selection functions return one or more results depending. You can specify the timestamp column, tbname pseudocolumn, or tag columns to show which rows contain the selected value.

### BOTTOM

```sql
BOTTOM(expr, k)
```

**Description**: The least _k_ values of a specific column in a table or STable. If a value has multiple occurrences in the column but counting all of them in will exceed the upper limit _k_, then a part of them will be returned randomly.

**Return value type**:Same as the data type of the column being operated upon

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables

**More explanation**:

- _k_ must be in range [1,100]
- The timestamp associated with the selected values are returned too
- Can't be used with `FILL`

### FIRST

```sql
FIRST(expr)
```

**Description**: The first non-null value of a specific column in a table or STable

**Return value type**:Same as the data type of the column being operated upon

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables

**More explanation**:

- FIRST(\*) can be used to get the first non-null value of all columns
- NULL will be returned if all the values of the specified column are all NULL
- A result will NOT be returned if all the columns in the result set are all NULL

### INTERP

```sql
INTERP(expr)
```

**Description**: The value that matches the specified timestamp range is returned, if existing; or an interpolation value is returned.

**Return value type**: Same as the column being operated upon

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables

**More explanations**

- `INTERP` is used to get the value that matches the specified time slice from a column. If no such value exists an interpolation value will be returned based on `FILL` parameter.
- The input data of `INTERP` is the value of the specified column and a `where` clause can be used to filter the original data. If no `where` condition is specified then all original data is the input.
- `INTERP` must be used along with `RANGE`, `EVERY`, `FILL` keywords.
- The output time range of `INTERP` is specified by `RANGE(timestamp1,timestamp2)` parameter, with timestamp1 <= timestamp2. timestamp1 is the starting point of the output time range and must be specified. timestamp2 is the ending point of the output time range and must be specified.
- The number of rows in the result set of `INTERP` is determined by the parameter `EVERY(time_unit)`. Starting from timestamp1, one interpolation is performed for every time interval specified `time_unit` parameter. The parameter `time_unit` must be an integer, with no quotes, with a time unit of: a(millisecond)), s(second), m(minute), h(hour), d(day), or w(week). For example, `EVERY(500a)` will interpolate every 500 milliseconds.
- Interpolation is performed based on `FILL` parameter. For more information about FILL clause, see [FILL Clause](../distinguished/#fill-clause).
- `INTERP` can be applied to supertable by interpolating primary key sorted data of all its childtables. It can also be used with `partition by tbname` when applied to supertable to generate interpolation on each single timeline.
- Pseudocolumn `_irowts` can be used along with `INTERP` to return the timestamps associated with interpolation points(support after version 3.0.2.0).
- Pseudocolumn `_isfilled` can be used along with `INTERP` to indicate whether the results are original records or data points generated by interpolation algorithm(support after version 3.0.3.0).

**Example**

- We use the smart meters example used in this documentation to illustrate how to use the INTERP function.
- We want to downsample every 1 hour and use a linear fill for missing values. Note the order in which the "partition by" clause and the "range", "every" and "fill" parameters are used.

```sql
SELECT _irowts,INTERP(current) FROM test.meters PARTITION BY TBNAME RANGE('2017-07-22 00:00:00','2017-07-24 12:25:00') EVERY(1h) FILL(LINEAR) 
```

### LAST

```sql
LAST(expr)
```

**Description**: The last non-NULL value of a specific column in a table or STable

**Return value type**:Same as the data type of the column being operated upon

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables

**More explanation**:

- LAST(\*) can be used to get the last non-NULL value of all columns
- If the values of a column in the result set are all NULL, NULL is returned for that column; if all columns in the result are all NULL, no result will be returned.
- When it's used on a STable, if there are multiple values with the timestamp in the result set, one of them will be returned randomly and it's not guaranteed that the same value is returned if the same query is run multiple times.


### LAST_ROW

```sql
LAST_ROW(expr)
```

**Description**: The last row of a table or STable

**Return value type**:Same as the data type of the column being operated upon

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables

**More explanations**:

- When it's used on a STable, if there are multiple values with the timestamp in the result set, one of them will be returned randomly and it's not guaranteed that the same value is returned if the same query is run multiple times.
- Can't be used with `INTERVAL`.

### MAX

```sql
MAX(expr)
```

**Description**: The maximum value of a specific column of a table or STable

**Return value type**:Same as the data type of the column being operated upon

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables


### MIN

```sql
MIN(expr)
```

**Description**: The minimum value of a specific column in a table or STable

**Return value type**:Same as the data type of the column being operated upon

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables


### MODE

```sql
MODE(expr)
```

**Description**:The value which has the highest frequency of occurrence. One random value is returned if there are multiple values which have highest frequency of occurrence.

**Return value type**: Same as the input data

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables


### SAMPLE

```sql
SAMPLE(expr, k)
```

**Description**: _k_ sampling values of a specific column. The applicable range of _k_ is [1,1000].

**Return value type**: Same as the column being operated plus the associated timestamp

**Applicable data types**: Any data type except for tags of STable

**Applicable nested query**: Inner query and Outer query

**Applicable table types**: standard tables and supertables

**More explanations**:

This function cannot be used in expression calculation.
- Must be used with `PARTITION BY tbname` when it's used on a STable to force the result on each single timeline


### TAIL

```sql
TAIL(expr, k, offset_val)
```

**Description**: The next _k_ rows are returned after skipping the last `offset_val` rows, NULL values are not ignored. `offset_val` is optional parameter. When it's not specified, the last _k_ rows are returned. When `offset_val` is used, the effect is same as `order by ts desc LIMIT k OFFSET offset_val`.

**Parameter value range**: k: [1,100] offset_val: [0,100]

**Return value type**:Same as the data type of the column being operated upon

**Applicable data types**: Any data type except for timestamp, i.e. the primary key

**Applicable table types**: standard tables and supertables


### TOP

```sql
TOP(expr, k)
```

**Description**: The greatest _k_ values of a specific column in a table or STable. If a value has multiple occurrences in the column but counting all of them in will exceed the upper limit _k_, then a part of them will be returned randomly.

**Return value type**:Same as the data type of the column being operated upon

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables

**More explanation**:

- _k_ must be in range [1,100]
- The timestamp associated with the selected values are returned too
- Can't be used with `FILL`

### UNIQUE

```sql
UNIQUE(expr)
```

**Description**: The values that occur the first time in the specified column. The effect is similar to `distinct` keyword, but it can also be used to match tags or timestamp. The first occurrence of a timestamp or tag is used.

**Return value type**:Same as the data type of the column being operated upon

**Applicable column types**: Any data types except for timestamp

**Applicable table types**: table, STable


## Time-Series Extensions

TDengine includes extensions to standard SQL that are intended specifically for time-series use cases. The functions enabled by these extensions require complex queries to implement in general-purpose databases. By offering them as built-in extensions, TDengine reduces user workload.

### CSUM

```sql
CSUM(expr)
```

**Description**: The cumulative sum of each row for a specific column. The number of output rows is same as that of the input rows.

**Return value type**: Long integer for integers; Double for floating points. uint64_t for unsigned integers

**Applicable data types**: Numeric

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**More explanations**:

- Arithmetic operation can't be performed on the result of `csum` function
- Can only be used with aggregate functions This function can be used with supertables and standard tables.
- Must be used with `PARTITION BY tbname` when it's used on a STable to force the result on each single timeline


### DERIVATIVE

```sql
DERIVATIVE(expr, time_inerval, ignore_negative)

ignore_negative: {
    0
  | 1
}
```

**Description**: The derivative of a specific column. The time rage can be specified by parameter `time_interval`, the minimum allowed time range is 1 second (1s); the value of `ignore_negative` can be 0 or 1, 1 means negative values are ignored.

**Return value type**: DOUBLE

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables

**More explanation**:

- It can be used together with `PARTITION BY tbname` against a STable.
- It can be used together with a selected column. For example: select \_rowts, DERIVATIVE() from.

### DIFF

```sql
DIFF(expr [, ignore_negative])

ignore_negative: {
    0
  | 1
}
```

**Description**: The different of each row with its previous row for a specific column. `ignore_negative` can be specified as 0 or 1, the default value is 1 if it's not specified. `1` means negative values are ignored.

**Return value type**:Same as the data type of the column being operated upon

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables

**More explanation**:

- The number of result rows is the number of rows subtracted by one, no output for the first row
- It can be used together with a selected column. For example: select \_rowts, DIFF() from.


### IRATE

```sql
IRATE(expr)
```

**Description**: instantaneous rate on a specific column. The last two samples in the specified time range are used to calculate instantaneous rate. If the last sample value is smaller, then only the last sample value is used instead of the difference between the last two sample values.

**Return value type**: DOUBLE

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables


### MAVG

```sql
MAVG(expr, k)
```

**Description**: The moving average of continuous _k_ values of a specific column. If the number of input rows is less than _k_, nothing is returned. The applicable range of _k_ is [1,1000].

**Return value type**: DOUBLE

**Applicable data types**: Numeric

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**More explanations**:

- Arithmetic operation can't be performed on the result of `MAVG`.
- Can only be used with data columns, can't be used with tags. - Can't be used with aggregate functions.
- Must be used with `PARTITION BY tbname` when it's used on a STable to force the result on each single timeline


### STATECOUNT

```sql
STATECOUNT(expr, oper, val)
```

**Description**: The number of continuous rows satisfying the specified conditions for a specific column. The result is shown as an extra column for each row. If the specified condition is evaluated as true, the number is increased by 1; otherwise the number is reset to -1. If the input value is NULL, then the corresponding row is skipped.

**Applicable parameter values**:

- oper : Can be one of `'LT'` (lower than), `'GT'` (greater than), `'LE'` (lower than or equal to), `'GE'` (greater than or equal to), `'NE'` (not equal to), `'EQ'` (equal to), the value is case insensitive, the value must be in quotes.
- val: Numeric types

**Return value type**: Integer

**Applicable data types**: Numeric

**Applicable nested query**: Outer query only

**Applicable table types**: standard tables and supertables

**More explanations**:

- Must be used together with `PARTITION BY tbname` when it's used on a STable to force the result into each single timeline]
- Can't be used with window operation, like interval/state_window/session_window


### STATEDURATION

```sql
STATEDURATION(expr, oper, val, unit)
```

**Description**: The length of time range in which all rows satisfy the specified condition for a specific column. The result is shown as an extra column for each row. The length for the first row that satisfies the condition is 0. Next, if the condition is evaluated as true for a row, the time interval between current row and its previous row is added up to the time range; otherwise the time range length is reset to -1. If the value of the column is NULL, the corresponding row is skipped.

**Applicable parameter values**:

- oper : Can be one of `'LT'` (lower than), `'GT'` (greater than), `'LE'` (lower than or equal to), `'GE'` (greater than or equal to), `'NE'` (not equal to), `'EQ'` (equal to), the value is case insensitive, the value must be in quotes.
- val: Numeric types
- unit: The unit of time interval. Enter one of the following options: 1b (nanoseconds), 1u (microseconds), 1a (milliseconds), 1s (seconds), 1m (minutes), 1h (hours), 1d (days), or 1w (weeks) If you do not enter a unit of time, the precision of the current database is used by default.

**Return value type**: Integer

**Applicable data types**: Numeric

**Applicable nested query**: Outer query only

**Applicable table types**: standard tables and supertables

**More explanations**:

- Must be used together with `PARTITION BY tbname` when it's used on a STable to force the result into each single timeline]
- Can't be used with window operation, like interval/state_window/session_window


### TWA

```sql
TWA(expr)
```

**Description**: Time weighted average on a specific column within a time range

**Return value type**: DOUBLE

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables

- Must be used together with `PARTITION BY tbname` to force the result into each single timeline.


## System Information Functions

### DATABASE

```sql
SELECT DATABASE();
```

**Description**: The current database. If no database is specified upon logging in and no database is specified with `USE` after login, NULL will be returned by `select database()`.


### CLIENT_VERSION

```sql
SELECT CLIENT_VERSION();
```

**Description**: The client version.

### SERVER_VERSION

```sql
SELECT SERVER_VERSION();
```

**Description**: The server version.

### SERVER_STATUS

```sql
SELECT SERVER_STATUS();
```

**Description**: The server status.
