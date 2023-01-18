---
sidebar_label: Functions
title: Functions
toc_max_heading_level: 4
description: TDengine Built-in Functions.
---

## Single Row Functions

Single row functions return a result for each row.

### Mathematical Functions

#### ABS

```sql
SELECT ABS(field_name) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: The absolute value of a specific field.

**Return value type**: Same as the field being used

**Applicable data types**: Numeric

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Usage**: This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.

#### ACOS

```sql
SELECT ACOS(field_name) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: The arc cosine of a specific field.

**Return value type**: Double

**Applicable data types**: Numeric

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Usage**: This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.

#### ASIN

```sql
SELECT ASIN(field_name) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: The arc sine of a specific field.

**Return value type**: Double

**Applicable data types**: Numeric

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Usage**: This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.


#### ATAN

```sql
SELECT ATAN(field_name) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: The arc tangent of a specific field.

**Return value type**: Double

**Applicable data types**: Numeric

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Usage**: This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.


#### CEIL

```sql
SELECT CEIL(field_name) FROM { tb_name | stb_name } [WHERE clause];
```

**Description**: The rounded up value of a specific field

**Return value type**: Same as the field being used

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Usage**: This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.

#### COS

```sql
SELECT COS(field_name) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: The cosine of a specific field.

**Return value type**: Double

**Applicable data types**: Numeric

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Usage**: This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.

#### FLOOR

```sql
SELECT FLOOR(field_name) FROM { tb_name | stb_name } [WHERE clause];
```

**Description**: The rounded down value of a specific field  
 **More explanations**: The restrictions are same as those of the `CEIL` function.

#### LOG

```sql
SELECT LOG(field_name[, base]) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: The logarithm of a specific field with `base` as the radix. If you do not enter a base, the natural logarithm of the field is returned.

**Return value type**: Double

**Applicable data types**: Numeric

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Usage**: This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.


#### POW

```sql
SELECT POW(field_name, power) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: The power of a specific field with `power` as the exponent.

**Return value type**: Double

**Applicable data types**: Numeric

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Usage**: This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.


#### ROUND

```sql
SELECT ROUND(field_name) FROM { tb_name | stb_name } [WHERE clause];
```

**Description**: The rounded value of a specific field.  
 **More explanations**: The restrictions are same as those of the `CEIL` function.


#### SIN

```sql
SELECT SIN(field_name) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: The sine of a specific field.

**Return value type**: Double

**Applicable data types**: Numeric

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Usage**: This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.

#### SQRT

```sql
SELECT SQRT(field_name) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: The square root of a specific field.

**Return value type**: Double

**Applicable data types**: Numeric

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Usage**: This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.

#### TAN

```sql
SELECT TAN(field_name) FROM { tb_name | stb_name } [WHERE clause]
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
SELECT CHAR_LENGTH(str|column) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: The length in number of characters of a string

**Return value type**: Bigint

**Applicable data types**: VARCHAR and NCHAR

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

#### CONCAT

```sql
SELECT CONCAT(str1|column1, str2|column2, ...) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: The concatenation result of two or more strings

**Return value type**: If the concatenated strings are VARCHARs, the result is a VARCHAR. If the concatenated strings are NCHARs, the result is an NCHAR. If an input value is null, the result is null.

**Applicable data types**: VARCHAR and NCHAR You can concatenate between 2 and 8 strings.

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables


#### CONCAT_WS

```sql
SELECT CONCAT_WS(separator, str1|column1, str2|column2, ...) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: The concatenation result of two or more strings with separator

**Return value type**: If the concatenated strings are VARCHARs, the result is a VARCHAR. If the concatenated strings are NCHARs, the result is an NCHAR. If an input value is null, the result is null.

**Applicable data types**: VARCHAR and NCHAR You can concatenate between 3 and 9 strings.

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables


#### LENGTH

```sql
SELECT LENGTH(str|column) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: The length in bytes of a string

**Return value type**: Bigint

**Applicable data types**: VARCHAR and NCHAR fields or columns

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables


#### LOWER

```sql
SELECT LOWER(str|column) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: Convert the input string to lower case

**Return value type**: Same as input

**Applicable data types**: VARCHAR and NCHAR

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables


#### LTRIM

```sql
SELECT LTRIM(str|column) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: Remove the left leading blanks of a string

**Return value type**: Same as input

**Applicable data types**: VARCHAR and NCHAR

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables


#### RTRIM

```sql
SELECT LTRIM(str|column) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: Remove the right tailing blanks of a string

**Return value type**: Same as input

**Applicable data types**: VARCHAR and NCHAR

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables


#### SUBSTR

```sql
SELECT SUBSTR(str,pos[,len]) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: The sub-string starting from `pos` with length of `len` from the original string `str` - If `len` is not specified, it means from `pos` to the end.

**Return value type**: Same as input

**Applicable data types**: VARCHAR and NCHAR Parameter `pos` can be an positive or negative integer; If it's positive, the starting position will be counted from the beginning of the string; if it's negative, the starting position will be counted from the end of the string.

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: table, STable


#### UPPER

```sql
SELECT UPPER(str|column) FROM { tb_name | stb_name } [WHERE clause]
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
SELECT CAST(expression AS type_name) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: Convert the input data `expression` into the type specified by `type_name`. This function can be used only in SELECT statements.

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
SELECT TO_ISO8601(ts[, timezone]) FROM { tb_name | stb_name } [WHERE clause];
```

**Description**: The ISO8601 date/time format converted from a UNIX timestamp, plus the timezone. You can specify any time zone with the timezone parameter. If you do not enter this parameter, the time zone on the client is used.

**Return value type**: VARCHAR

**Applicable data types**: Integers and timestamps

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**More explanations**:

- You can specify a time zone in the following format: [z/Z, +/-hhmm, +/-hh, +/-hh:mm]。 For example, TO_ISO8601(1, "+00:00").
- If the input is a UNIX timestamp, the precision of the returned value is determined by the digits of the input timestamp 
- If the input is a column of TIMESTAMP type, the precision of the returned value is same as the precision set for the current data base in use


#### TO_JSON

```sql
SELECT TO_JSON(str_literal) FROM { tb_name | stb_name } [WHERE clause];
```

**Description**: Converts a string into JSON.

**Return value type**: JSON

**Applicable data types**: JSON strings in the form `{"literal": literal}`. `{}` indicates a null value. The key must be a string literal. The value can be a numeric literal, string literal, Boolean literal, or null literal. str_literal cannot include escape characters.

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: table, STable


#### TO_UNIXTIMESTAMP

```sql
SELECT TO_UNIXTIMESTAMP(datetime_string) FROM { tb_name | stb_name } [WHERE clause];
```

**Description**: UNIX timestamp converted from a string of date/time format

**Return value type**: BIGINT

**Applicable column types**: VARCHAR and NCHAR

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**More explanations**:

- The input string must be compatible with ISO8601/RFC3339 standard, NULL will be returned if the string can't be converted
- The precision of the returned timestamp is same as the precision set for the current data base in use


### Time and Date Functions

These functions perform operations on times and dates.

All functions that return the current time, such as `NOW`, `TODAY`, and `TIMEZONE`, are calculated only once per statement even if they appear multiple times.

#### NOW

```sql
SELECT NOW() FROM { tb_name | stb_name } [WHERE clause];
SELECT select_expr FROM { tb_name | stb_name } WHERE ts_col cond_operator NOW();
INSERT INTO tb_name VALUES (NOW(), ...);
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
SELECT TIMEDIFF(ts | datetime_string1, ts | datetime_string2 [, time_unit]) FROM { tb_name | stb_name } [WHERE clause];
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
SELECT TIMETRUNCATE(ts | datetime_string , time_unit) FROM { tb_name | stb_name } [WHERE clause];
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


#### TIMEZONE

```sql
SELECT TIMEZONE() FROM { tb_name | stb_name } [WHERE clause];
```

**Description**: The timezone of the client side system

**Applicable data types**: VARCHAR

**Applicable column types**: None

**Applicable table types**: standard tables and supertables


#### TODAY

```sql
SELECT TODAY() FROM { tb_name | stb_name } [WHERE clause];
SELECT select_expr FROM { tb_name | stb_name } WHERE ts_col cond_operator TODAY()];
INSERT INTO tb_name VALUES (TODAY(), ...);
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
SELECT APERCENTILE(field_name, P[, algo_type]) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: Similar to `PERCENTILE`, but a simulated result is returned

**Return value type**: DOUBLE

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables

**Explanations**：
- _P_ is in range [0,100], when _P_ is 0, the result is same as using function MIN; when _P_ is 100, the result is same as function MAX.
- `algo_type` can only be input as `default` or `t-digest` Enter `default` to use a histogram-based algorithm. Enter `t-digest` to use the t-digest algorithm to calculate the approximation of the quantile. `default` is used by default.

### AVG

```sql
SELECT AVG(field_name) FROM tb_name [WHERE clause];
```

**Description**: The average value of the specified fields.

**Return value type**: DOUBLE

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables


### COUNT

```sql
SELECT COUNT([*|field_name]) FROM tb_name [WHERE clause];
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
SELECT ELAPSED(ts_primary_key [, time_unit]) FROM { tb_name | stb_name } [WHERE clause] [INTERVAL(interval [, offset]) [SLIDING sliding]];
```

**Description**：`elapsed` function can be used to calculate the continuous time length in which there is valid data. If it's used with `INTERVAL` clause, the returned result is the calculated time length within each time window. If it's used without `INTERVAL` caluse, the returned result is the calculated time length within the specified time range. Please be noted that the return value of `elapsed` is the number of `time_unit` in the calculated time length.

**Return value type**: Double if the input value is not NULL;

**Return value type**: TIMESTAMP

**Applicable tables**: table, STable, outer in nested query

**Explanations**：
- `field_name` parameter can only be the first column of a table, i.e. timestamp primary key.
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
SELECT LEASTSQUARES(field_name, start_val, step_val) FROM tb_name [WHERE clause];
```

**Description**: The linear regression function of the specified column and the timestamp column (primary key), `start_val` is the initial value and `step_val` is the step value.

**Return value type**: A string in the format of "(slope, intercept)"

**Applicable data types**: Numeric

**Applicable table types**: table only


### SPREAD

```sql
SELECT SPREAD(field_name) FROM { tb_name | stb_name } [WHERE clause];
```

**Description**: The difference between the max and the min of a specific column

**Return value type**: DOUBLE

**Applicable data types**: Integers and timestamps

**Applicable table types**: standard tables and supertables


### STDDEV

```sql
SELECT STDDEV(field_name) FROM tb_name [WHERE clause];
```

**Description**: Standard deviation of a specific column in a table or STable

**Return value type**: DOUBLE

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables


### SUM

```sql
SELECT SUM(field_name) FROM tb_name [WHERE clause];
```

**Description**: The sum of a specific column in a table or STable

**Return value type**: DOUBLE or BIGINT

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables


### HYPERLOGLOG

```sql
SELECT HYPERLOGLOG(field_name) FROM { tb_name | stb_name } [WHERE clause];
```

**Description**：
  The cardinal number of a specific column is returned by using hyperloglog algorithm. The benefit of using hyperloglog algorithm is that the memory usage is under control when the data volume is huge.
  However, when the data volume is very small, the result may be not accurate, it's recommended to use `select count(data) from (select unique(col) as data from table)` in this case.

**Return value type**: Integer

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables


### HISTOGRAM

```sql
SELECT HISTOGRAM(field_name，bin_type, bin_description, normalized) FROM tb_name [WHERE clause];
```

**Description**：Returns count of data points in user-specified ranges.

**Return value type** If normalized is set to 1, a DOUBLE is returned; otherwise a BIGINT is returned

**Applicable data types**: Numeric

**Applicable table types**: table, STable

**Explanations**：
- bin_type: parameter to indicate the bucket type, valid inputs are: "user_input", "linear_bin", "log_bin"。
- bin_description: parameter to describe how to generate buckets，can be in the following JSON formats for each bin_type respectively:       
    - "user_input": "[1, 3, 5, 7]": 
       User specified bin values.
       
    - "linear_bin": "{"start": 0.0, "width": 5.0, "count": 5, "infinity": true}"
       "start" - bin starting point.       "width" - bin offset.       "count" - number of bins generated.       "infinity" - whether to add（-inf, inf）as start/end point in generated set of bins.
       The above "linear_bin" descriptor generates a set of bins: [-inf, 0.0, 5.0, 10.0, 15.0, 20.0, +inf].
 
    - "log_bin": "{"start":1.0, "factor": 2.0, "count": 5, "infinity": true}"
       "start" - bin starting point.       "factor" - exponential factor of bin offset.       "count" - number of bins generated.       "infinity" - whether to add（-inf, inf）as start/end point in generated range of bins.
       The above "linear_bin" descriptor generates a set of bins: [-inf, 1.0, 2.0, 4.0, 8.0, 16.0, +inf].
- normalized: setting to 1/0 to turn on/off result normalization. Valid values are 0 or 1.


### PERCENTILE

```sql
SELECT PERCENTILE(field_name, P) FROM { tb_name } [WHERE clause];
```

**Description**: The value whose rank in a specific column matches the specified percentage. If such a value matching the specified percentage doesn't exist in the column, an interpolation value will be returned.

**Return value type**: DOUBLE

**Applicable column types**: Numeric

**Applicable table types**: table only

**More explanations**: _P_ is in range [0,100], when _P_ is 0, the result is same as using function MIN; when _P_ is 100, the result is same as function MAX.


## Selection Functions

Selection functions return one or more results depending. You can specify the timestamp column, tbname pseudocolumn, or tag columns to show which rows contain the selected value.

### BOTTOM

```sql
SELECT BOTTOM(field_name, K) FROM { tb_name | stb_name } [WHERE clause];
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
SELECT FIRST(field_name) FROM { tb_name | stb_name } [WHERE clause];
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
SELECT INTERP(field_name) FROM { tb_name | stb_name } [WHERE where_condition] RANGE(timestamp1,timestamp2) EVERY(interval) FILL({ VALUE | PREV | NULL | LINEAR | NEXT});
```

**Description**: The value that matches the specified timestamp range is returned, if existing; or an interpolation value is returned.

**Return value type**: Same as the column being operated upon

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables

**More explanations**

- `INTERP` is used to get the value that matches the specified time slice from a column. If no such value exists an interpolation value will be returned based on `FILL` parameter.
- The input data of `INTERP` is the value of the specified column and a `where` clause can be used to filter the original data. If no `where` condition is specified then all original data is the input.
- The output time range of `INTERP` is specified by `RANGE(timestamp1,timestamp2)` parameter, with timestamp1<=timestamp2. timestamp1 is the starting point of the output time range and must be specified. timestamp2 is the ending point of the output time range and must be specified. 
- The number of rows in the result set of `INTERP` is determined by the parameter `EVERY`. Starting from timestamp1, one interpolation is performed for every time interval specified `EVERY` parameter. 
- Interpolation is performed based on `FILL` parameter. 
- `INTERP` can only be used to interpolate in single timeline. So it must be used with `partition by tbname` when it's used on a STable.

### LAST

```sql
SELECT LAST(field_name) FROM { tb_name | stb_name } [WHERE clause];
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
SELECT LAST_ROW(field_name) FROM { tb_name | stb_name };
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
SELECT MAX(field_name) FROM { tb_name | stb_name } [WHERE clause];
```

**Description**: The maximum value of a specific column of a table or STable

**Return value type**:Same as the data type of the column being operated upon

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables


### MIN

```sql
SELECT MIN(field_name) FROM {tb_name | stb_name} [WHERE clause];
```

**Description**: The minimum value of a specific column in a table or STable

**Return value type**:Same as the data type of the column being operated upon

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables


### MODE

```sql
SELECT MODE(field_name) FROM tb_name [WHERE clause];
```

**Description**:The value which has the highest frequency of occurrence. NULL is returned if there are multiple values which have highest frequency of occurrence.

**Return value type**: Same as the input data

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables


### SAMPLE

```sql
SELECT SAMPLE(field_name, K) FROM { tb_name | stb_name } [WHERE clause]
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
SELECT TAIL(field_name, k, offset_val) FROM {tb_name | stb_name} [WHERE clause];
```

**Description**: The next _k_ rows are returned after skipping the last `offset_val` rows, NULL values are not ignored. `offset_val` is optional parameter. When it's not specified, the last _k_ rows are returned. When `offset_val` is used, the effect is same as `order by ts desc LIMIT k OFFSET offset_val`.

**Parameter value range**: k: [1,100] offset_val: [0,100]

**Return value type**:Same as the data type of the column being operated upon

**Applicable data types**: Any data type except for timestamp, i.e. the primary key

**Applicable table types**: standard tables and supertables


### TOP

```sql
SELECT TOP(field_name, K) FROM { tb_name | stb_name } [WHERE clause];
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
SELECT UNIQUE(field_name) FROM {tb_name | stb_name} [WHERE clause];
```

**Description**: The values that occur the first time in the specified column. The effect is similar to `distinct` keyword, but it can also be used to match tags or timestamp. The first occurrence of a timestamp or tag is used.

**Return value type**:Same as the data type of the column being operated upon

**Applicable column types**: Any data types except for timestamp

**Applicable table types**: table, STable


## Time-Series Extensions

TDengine includes extensions to standard SQL that are intended specifically for time-series use cases. The functions enabled by these extensions require complex queries to implement in general-purpose databases. By offering them as built-in extensions, TDengine reduces user workload.

### CSUM

```sql
SELECT CSUM(field_name) FROM { tb_name | stb_name } [WHERE clause]
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
SELECT DERIVATIVE(field_name, time_interval, ignore_negative) FROM tb_name [WHERE clause];
```

**Description**: The derivative of a specific column. The time rage can be specified by parameter `time_interval`, the minimum allowed time range is 1 second (1s); the value of `ignore_negative` can be 0 or 1, 1 means negative values are ignored.

**Return value type**: DOUBLE

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables

**More explanation**: 
  
- It can be used together with `PARTITION BY tbname` against a STable.
- It can be used together with a selected column. For example: select \_rowts, DERIVATIVE() from。

### DIFF

```sql
SELECT {DIFF(field_name, ignore_negative) | DIFF(field_name)} FROM tb_name [WHERE clause];
```

**Description**: The different of each row with its previous row for a specific column. `ignore_negative` can be specified as 0 or 1, the default value is 1 if it's not specified. `1` means negative values are ignored.

**Return value type**:Same as the data type of the column being operated upon

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables

**More explanation**: 

- The number of result rows is the number of rows subtracted by one, no output for the first row
- It can be used together with a selected column. For example: select \_rowts, DIFF() from。


### IRATE

```sql
SELECT IRATE(field_name) FROM tb_name WHERE clause;
```

**Description**: instantaneous rate on a specific column. The last two samples in the specified time range are used to calculate instantaneous rate. If the last sample value is smaller, then only the last sample value is used instead of the difference between the last two sample values.

**Return value type**: DOUBLE

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables


### MAVG

```sql
SELECT MAVG(field_name, K) FROM { tb_name | stb_name } [WHERE clause]
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
SELECT STATECOUNT(field_name, oper, val) FROM { tb_name | stb_name } [WHERE clause];
```

**Description**: The number of continuous rows satisfying the specified conditions for a specific column. The result is shown as an extra column for each row. If the specified condition is evaluated as true, the number is increased by 1; otherwise the number is reset to -1. If the input value is NULL, then the corresponding row is skipped.

**Applicable parameter values**:

- oper : Can be one of `LT` (lower than), `GT` (greater than), `LE` (lower than or equal to), `GE` (greater than or equal to), `NE` (not equal to), `EQ` (equal to), the value is case insensitive
- val ： Numeric types

**Return value type**: Integer

**Applicable data types**: Numeric

**Applicable nested query**: Outer query only

**Applicable table types**: standard tables and supertables

**More explanations**:

- Must be used together with `PARTITION BY tbname` when it's used on a STable to force the result into each single timeline]
- Can't be used with window operation, like interval/state_window/session_window


### STATEDURATION

```sql
SELECT stateDuration(field_name, oper, val, unit) FROM { tb_name | stb_name } [WHERE clause];
```

**Description**: The length of time range in which all rows satisfy the specified condition for a specific column. The result is shown as an extra column for each row. The length for the first row that satisfies the condition is 0. Next, if the condition is evaluated as true for a row, the time interval between current row and its previous row is added up to the time range; otherwise the time range length is reset to -1. If the value of the column is NULL, the corresponding row is skipped.

**Applicable parameter values**:

- oper : Can be one of `LT` (lower than), `GT` (greater than), `LE` (lower than or equal to), `GE` (greater than or equal to), `NE` (not equal to), `EQ` (equal to), the value is case insensitive
- val ： Numeric types
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
SELECT TWA(field_name) FROM tb_name WHERE clause;
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
