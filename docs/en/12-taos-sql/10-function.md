---
title: Functions
toc_max_heading_level: 4
---

## Single-Row Functions

Single-Row functions return a result row for each row in the query result.

### Numeric Functions

#### ABS

```sql
SELECT ABS(field_name) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: The absolute value of a specific field.

**Return value type**: Same as input type.

**Applicable data types**: Numeric types.

**Applicable table types**: table, STable.

**Applicable nested query**: Inner query and Outer query.

**More explanations**:
- Cannot be used with aggregate functions.

#### ACOS

```sql
SELECT ACOS(field_name) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: The anti-cosine of a specific field.

**Return value type**: DOUBLE.

**Applicable data types**: Numeric types.

**Applicable table types**: table, STable.

**Applicable nested query**: Inner query and Outer query.

**More explanations**:
- Cannot be used with aggregate functions.

#### ASIN

```sql
SELECT ASIN(field_name) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: The anti-sine of a specific field.

**Return value type**: DOUBLE.

**Applicable data types**: Numeric types.

**Applicable table types**: table, STable

**Applicable nested query**: Inner query and Outer query.

**More explanations**:
- Cannot be used with aggregate functions.

#### ATAN

```sql
SELECT ATAN(field_name) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: anti-tangent of a specific field.

**Return value type**: DOUBLE.

**Applicable data types**: Numeric types.

**Applicable table types**: table, STable

**Applicable nested query**: Inner query and Outer query.

**More explanations**:
- Cannot be used with aggregate functions.

#### CEIL

```
SELECT CEIL(field_name) FROM { tb_name | stb_name } [WHERE clause];
```

**Description**: The rounded up value of a specific field.

**Return value type**: Same as input type.

**Applicable data types**: Numeric types.

**Applicable table types**: table, STable

**Applicable nested query**: Inner query and outer query.

**More explanations**:
- Can't be used with aggregate functions.

#### COS

```sql
SELECT COS(field_name) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: The cosine of a specific field.

**Return value type**: DOUBLE.

**Applicable data types**: Numeric types.

**Applicable table types**: table, STable.

**Applicable nested query**: Inner query and Outer query.

**More explanations**:
- Can't be used with aggregate functions.

#### FLOOR

```
SELECT FLOOR(field_name) FROM { tb_name | stb_name } [WHERE clause];
```

**Description**: The rounded down value of a specific field.

**More explanations**: Refer to `CEIL` function for usage restrictions.

#### LOG

```sql
SELECT LOG(field_name, base) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: The logarithm of a specific field with `base` as the radix. If `base` parameter is ignored, natural logarithm of the field is returned.

**Return value type**: DOUBLE.

**Applicable data types**: Numeric types.

**Applicable table types**: table, STable.

**Applicable nested query**: Inner query and Outer query.

**More explanations**:
- Can't be used with aggregate functions

#### POW

```sql
SELECT POW(field_name, power) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: The power of a specific field with `power` as the index.

**Return value type**: DOUBLE.

**Applicable data types**: Numeric types.

**Applicable table types**: table, STable.

**Applicable nested query**: Inner query and Outer query.

**More explanations**:
- Can't be used with aggregate functions.

#### ROUND

```
SELECT ROUND(field_name) FROM { tb_name | stb_name } [WHERE clause];
```

**Description**: The rounded value of a specific field.

**More explanations**: Refer to `CEIL` function for usage restrictions.

#### SIN

```sql
SELECT SIN(field_name) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: The sine of a specific field.

**Description**: The anti-cosine of a specific field.

**Return value type**: DOUBLE.

**Applicable data types**: Numeric types.

**Applicable table types**: table, STable.

**Applicable nested query**: Inner query and Outer query.

**More explanations**:
- Can't be used with aggregate functions.

#### SQRT

```sql
SELECT SQRT(field_name) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: The square root of a specific field.

**Return value type**: DOUBLE.

**Applicable data types**: Numeric types.

**Applicable table types**: table, STable.

**Applicable nested query**: Inner query and Outer query.

**More explanations**:
- Can't be used with aggregate functions.

#### TAN

```sql
SELECT TAN(field_name) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: The tangent of a specific field.

**Description**: The anti-cosine of a specific field.

**Return value type**: DOUBLE.

**Applicable data types**: Numeric types.

**Applicable table types**: table, STable.

**Applicable nested query**: Inner query and Outer query.

**More explanations**:
- Can't be used with aggregate functions.

### String Functions

String functiosn take strings as input and output numbers or strings.

#### CHAR_LENGTH

```
SELECT CHAR_LENGTH(str|column) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: The mumber of characters of a string.

**Return value type**: INTEGER.

**Applicable data types**: VARCHAR, NCHAR.

**Applicable table types**: table, STable.

**Applicable nested query**: Inner query and Outer query.


#### CONCAT

```sql
SELECT CONCAT(str1|column1, str2|column2, ...) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: The concatenation result of two or more strings.

**Return value type**: If all input strings are VARCHAR type, the result is VARCHAR type too. If any one of input strings is NCHAR type, then the result is NCHAR. If input strings contain NULL value, the result is NULL. 

**Applicable data types**: VARCHAR, NCHAR.  At least 2 input strings are requird, and at most 8 input strings are allowed. 

**Applicable table types**: table, STable.

**Applicable nested query**: Inner query and Outer query.


#### CONCAT_WS

```
SELECT CONCAT_WS(separator, str1|column1, str2|column2, ...) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: The concatenation result of two or more strings with separator.

**Return value type**: If all input strings are VARCHAR type, the result is VARCHAR type too. If any one of input strings is NCHAR type, then the result is NCHAR. If input strings contain NULL value, the result is NULL. 

**Applicable data types**: VARCHAR, NCHAR. At least 3 input strings are requird, and at most 9 input strings are allowed. 

**Applicable table types**: table, STable.

**Applicable nested query**: Inner query and Outer query.


#### LENGTH

```
SELECT LENGTH(str|column) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: The length in bytes of a string.

**Return value type**: INTEGER.

**Applicable data types**: VARCHAR, NCHAR.

**Applicable table types**: table, STable.

**Applicable nested query**: Inner query and Outer query.


#### LOWER

```
SELECT LOWER(str|column) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: Convert the input string to lower case.

**Return value type**: Same as input type.

**Applicable data types**: VARCHAR, NCHAR.

**Applicable table types**: table, STable.

**Applicable nested query**: Inner query and Outer query.


#### LTRIM

```
SELECT LTRIM(str|column) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: Remove the left leading blanks of a string.

**Return value type**: Same as input type.

**Applicable data types**: VARCHAR, NCHAR.

**Applicable table types**: table, STable.

**Applicable nested query**: Inner query and Outer query.


#### RTRIM

```
SELECT RTRIM(str|column) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: Remove the right tailing blanks of a string.

**Return value type**: Same as input type.

**Applicable data types**: VARCHAR, NCHAR.

**Applicable table types**: table, STable.

**Applicable nested query**: Inner query and Outer query.


#### SUBSTR

```
SELECT SUBSTR(str,pos[,len]) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: The sub-string starting from `pos` with length of `len` from the original string `str`.

**Return value type**: Same as input type.

**Applicable data types**: VARCHAR, NCHAR.

**Applicable table types**: table, STable.

**Applicable nested query**: Inner query and Outer query.

**More explanations**:

- If the input is NULL, the output is NULL
- Parameter `pos` can be an positive or negative integer; If it's positive, the starting position will be counted from the beginning of the string; if it's negative, the starting position will be counted from the end of the string.
- If `len` is not specified, it means from `pos` to the end of string.

#### UPPER

```
SELECT UPPER(str|column) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: Convert the input string to upper case.

**Return value type**: Same as input type.

**Applicable data types**: VARCHAR, NCHAR.

**Applicable table types**: table, STable.

**Applicable nested query**: Inner query and Outer query.


### Conversion Functions

Conversion functions convert from one data type to another.

#### CAST

```sql
SELECT CAST(expression AS type_name) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: Used for type casting. Convert `expression` to the type specified by `type_name`.

**Return value type**: The type specified by parameter `type_name`.

**Applicable data types**: `expression` can be any data type except for JSON.

**More explanations**:

- Error will be reported for unsupported type casting.
- Some values of some supported data types may not be casted, below are known issues:
  1）When casting VARCHAR/NCHAR to BIGINT/BIGINT UNSIGNED, some characters may be treated as illegal, for example "a" may be converted to 0.
  2）When casting to numeric type, if converted result is out of range the destination data type can hold, overflow may occur and casting behavior is undefined.
  3) When casting to VARCHAR/NCHAR type, if converted string length exceeds the length specified in `type_name`, the result will be truncated. (e.g. CAST("abcd" as BINARY(2)) will return string "ab").

#### TO_ISO8601

```sql
SELECT TO_ISO8601(ts[, timezone]) FROM { tb_name | stb_name } [WHERE clause];
```

**Description**: The ISO8601 date/time format converted from a UNIX timestamp, with timezone attached. `timezone` parameter allows attaching any customized timezone string to the output format. If `timezone` parameter is not specified, the timezone information of client side system will be attached.

**Return value type**: VARCHAR.

**Applicable data types**: INTEGER, TIMESTAMP.

**Applicable table types**: table, STable.

**More explanations**:

- If the input is INTEGER represents UNIX timestamp, the precision of the returned value is determined by the digits of the input integer.
- If the input is of TIMESTAMP type, The precision of the returned value is same as the precision set for the current database in use.

#### TO_JSON

```sql
SELECT TO_JSON(str_literal) FROM { tb_name | stb_name } [WHERE clause];
```

**Description**: Convert a JSON string to a JSON body.

**Return value type**: JSON.

**Applicable data types**: JSON string, in the format like '{ "literal" : literal }'. '{}' is NULL value. keys in the string must be string constants, values can be constants of numeric types, bool, string or NULL. Escaping characters are not allowed in the JSON string.

**Applicable table types**: table, STable.

**Applicable nested query**: Inner query and Outer query.

#### TO_UNIXTIMESTAMP

```sql
SELECT TO_UNIXTIMESTAMP(datetime_string | ts_col) FROM { tb_name | stb_name } [WHERE clause];
```

**Description**: UNIX timestamp converted from a string of date/time format.

**Return value type**: BIGINT.

**Applicable data types**: VARCHAR, NCHAR.

**Applicable table types**: table, STable.

**More explanations**:

- The input string must be compatible with ISO8601/RFC3339 standard, NULL will be returned if the string cannot be converted.
- The precision of the returned timestamp is same as the precision set for the current database in use.

### DateTime Functions

DateTime functions applied to timestamp data. NOW(), TODAY() and TIMEZONE() are executed only once even though they may occur multiple times in a single SQL statement.

#### NOW

```sql
SELECT NOW() FROM { tb_name | stb_name } [WHERE clause];
SELECT select_expr FROM { tb_name | stb_name } WHERE ts_col cond_operatior NOW();
INSERT INTO tb_name VALUES (NOW(), ...);
```

**Description**: The current time of the client side system.

**Return value type**: TIMESTAMP.

**Applicable data types**: TIMESTAMP only if used in WHERE/INSERT clause.

**Applicable table types**: table, STable.

**More explanations**:

- Addition and Subtraction operation with time duration can be performed, for example NOW() + 1s, the time unit can be one of the followings:
  b(nanosecond), u(microsecond), a(millisecond)), s(second), m(minute), h(hour), d(day), w(week).
- The precision of the returned timestamp is same as the precision set for the current database in use.

#### TIMEDIFF

```sql
SELECT TIMEDIFF(ts1 | datetime_string1, ts2 | datetime_string2 [, time_unit]) FROM { tb_name | stb_name } [WHERE clause];
```

**Description**: The difference(duration) between two timestamps, and rounded to the time unit specified by `time_unit`.

**Return value type**: BIGINT.

**Applicable data types**: INTEGER/TIMESTAMP represents UNIX timestamp, or VARCHAR/NCHAR string in date/time format.

**Applicable table types**: table, STable.

**More explanations**:

- Time unit specified by `time_unit` can be:
  1b(nanosecond), 1u(microsecond),1a(millisecond),1s(second),1m(minute),1h(hour),1d(day),1w(week).
- If `time_unit` parameter is not specified, the precision of the returned time duration is same as the precision set for the current database in use.

#### TIMETRUNCATE

```sql
SELECT TIMETRUNCATE(ts_val | datetime_string | ts_col, time_unit) FROM { tb_name | stb_name } [WHERE clause];
```

**Description**: Truncate the input timestamp with unit specified by `time_unit`.

**Return value type**: TIMESTAMP.

**Applicable data types**: INTEGER/TIMESTAMP represents UNIX timestamp, or VARCHAR/NCHAR string in date/time format.

**Applicable table types**: table, STable.

**More explanations**:

- Time unit specified by `time_unit` can be:
  1b(nanosecond),1u(microsecond),1a(millisecond),1s(second),1m(minute),1h(hour),1d(day),1w(week).
- The precision of the returned timestamp is same as the precision set for the current database in use.

#### TIMEZONE

```sql
SELECT TIMEZONE() FROM { tb_name | stb_name } [WHERE clause];
```

**Description**: The timezone of the client side system.

**Return value type**: VARCHAR.

**Applicable data types**: None.

**Applicable table types**: table, STable.

#### TODAY

```sql
SELECT TODAY() FROM { tb_name | stb_name } [WHERE clause];
SELECT select_expr FROM { tb_name | stb_name } WHERE ts_col cond_operatior TODAY()];
INSERT INTO tb_name VALUES (TODAY(), ...);
```

**Description**: The timestamp of 00:00:00 of the client side system.

**Return value type**: TIMESTAMP.

**Applicable data types**: TIMESTAMP only if used in WHERE/INSERT clause.

**Applicable table types**: table, STable.

**More explanations**:

- Addition and Subtraction operation can be performed with time durations, for example NOW() + 1s, the time unit can be:
  b(nanosecond), u(microsecond), a(millisecond)), s(second), m(minute), h(hour), d(day), w(week).
- The precision of the returned timestamp is same as the precision set for the current database in use.

## Aggregate Functions

Aggregate functions return single result row for each group in the query result set. Groups are determined by `GROUP BY` clause or time window clause if they are used; or the whole result is considered a group if neither of them is used.

### APERCENTILE

```
SELECT APERCENTILE(field_name, P[, algo_type])
FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: Similar to `PERCENTILE`, but a approximated result is returned.

**Return value type**: DOUBLE.

**Applicable column types**: Numeric types.

**Applicable table types**: table, STable.

**More explanations**

- _P_ is in range [0,100], when _P_ is 0, the result is same as using function MIN; when _P_ is 100, the result is same as function MAX.
- **algo_type** can only be input as `default` or `t-digest`, if it's not specified `default` will be used, i.e. `apercentile(column_name, 50)` is same as `apercentile(column_name, 50, "default")`.
- If `default` is used, histogram based algorithm is used for calculation. If `t-digest` is used, `t-digest` sampling algorithm is used to calculate the result.

**Nested query**: It can be used in both the outer query and inner query in a nested query.

### AVG

```
SELECT AVG(field_name) FROM tb_name [WHERE clause];
```

**Description**: Get the average value of a column in a table or STable.

**Return value type**: DOUBLE.

**Applicable data types**: Numeric type.

**Applicable table types**: table, STable.

### COUNT

```
SELECT COUNT([*|field_name]) FROM tb_name [WHERE clause];
```

**Description**: Get the number of rows in a table or a super table.

**Return value type**: BIGINT.

**Applicable data types**: All data types.

**Applicable table types**: table, STable.

**More explanation**:

- Wildcard (\*) is used to represent all columns. If \* used `COUNT` function will get the total number of all rows.
- The number of non-NULL values will be returned if this function is used on a specific column.

### ELAPSED

```mysql
SELECT ELAPSED(field_name[, time_unit]) FROM { tb_name | stb_name } [WHERE clause] [INTERVAL(interval [, offset]) [SLIDING sliding]];
```

**Description**：`elapsed` function can be used to calculate the continuous time length in which there is valid data. If it's used with `INTERVAL` clause, the returned result is the calcualted time length within each time window. If it's used without `INTERVAL` caluse, the returned result is the calculated time duration within the specified time range. Please be noted that the calculated time duration is in the specified `time_unit`.

**Return value type**：DOUBLE.

**Applicable Column type**：TIMESTAMP.

**Applicable tables**: table, STable, outter in nested query.

**Explanations**：

- `field_name` parameter can only be the first column of a table, i.e. timestamp primary key.
- The minimum value of `time_unit` is the time precision of the database. If `time_unit` is not specified, the time precision of the database is used as the default ime unit.
- It can be used with `INTERVAL` to get the time valid time length of each time window. Please be noted that the return value is same as the time window for all time windows except for the first and the last time window.
- `order by asc/desc` has no effect on the result.
- `group by tbname` must be used together when `elapsed` is used against a STable.
- `group by` must NOT be used together when `elapsed` is used against a table or sub table.
- When used in nested query, it's only applicable when the inner query outputs an implicit timestamp column as the primary key. For example, `select elapsed(ts) from (select diff(value) from sub1)` is legal usage while `select elapsed(ts) from (select * from sub1)` is not.
- It cannot be used with `leastsquares`, `diff`, `derivative`, `top`, `bottom`, `last_row`, `interp`.

### LEASTSQUARES

```
SELECT LEASTSQUARES(field_name, start_val, step_val) FROM tb_name [WHERE clause];
```

**Description**: The linear regression function of the specified column and the timestamp column (primary key), `start_val` is the initial value and `step_val` is the step value.

**Return value type**: VARCHAR string in the format of "(slope, intercept)".

**Applicable data types**: Numeric types.

**Applicable table types**: table only.

### SPREAD

```
SELECT SPREAD(field_name) FROM { tb_name | stb_name } [WHERE clause];
```

**Description**: The difference between the max and the min value of a specific column.

**Return value type**: DOUBLE.

**Applicable column types**: Numeric types.

**Applicable table types**: table, STable.

**More explanations**: Can be used on a column of TIMESTAMP type, the result time unit precision is same as the current database in use.

### STDDEV

```
SELECT STDDEV(field_name) FROM tb_name [WHERE clause];
```

**Description**: Standard deviation of a specific column in a table or STable.

**Return value type**: DOUBLE.

**Applicable column types**: Numeric types.

**Applicable table types**: table, STable.

### SUM

```
SELECT SUM(field_name) FROM tb_name [WHERE clause];
```

**Description**: The summation of values of a specific column in a table or STable.

**Return value type**: DOUBLE.

**Applicable column types**: Numeric types.

**Applicable table types**: table, STable.

### HYPERLOGLOG

```
SELECT HYPERLOGLOG(field_name) FROM { tb_name | stb_name } [WHERE clause];
```

**Description**:The cardinal number of a specific column is returned by using hyperloglog algorithm.

**Return value type**: INTEGER.

**Applicable column types**: All data types.

**More explanations**: The benefit of using hyperloglog algorithm is that the memory usage is under control when the data volume is huge. However, when the data volume is very small, the result may be not accurate, it's recommented to use `select count(data) from (select unique(col) as data from table)` in this case.

### HISTOGRAM

```
SELECT HISTOGRAM(field_name，bin_type, bin_description, normalized) FROM tb_name [WHERE clause];
```

**Description**：Returns count of data points in user-specified ranges.

**Return value type**：DOUBLE or BIGINT, depends on normalized parameter settings.

**Applicable column type**：Numerical types.

**Applicable table types**: table, STable.

**Explanations**：

1. bin_type: parameter to indicate the bucket type, valid inputs are: "user_input", "linear_bin", "log_bin"。
2. bin_description: parameter to describe the rule to generate buckets，can be in the following JSON formats for each bin_type respectively:

   - "user_input": "[1, 3, 5, 7]": User specified bin values.

   - "linear_bin": "{"start": 0.0, "width": 5.0, "count": 5, "infinity": true}"
     "start" - bin starting point.
     "width" - bin offset.
     "count" - number of bins generated.
     "infinity" - whether to add（-inf, inf）as start/end point in generated set of bins.
     The above "linear_bin" descriptor generates a set of bins: [-inf, 0.0, 5.0, 10.0, 15.0, 20.0, +inf].

   - "log_bin": "{"start":1.0, "factor": 2.0, "count": 5, "infinity": true}"
     "start" - bin starting point.
     "factor" - exponential factor of bin offset.
     "count" - number of bins generated.
     "infinity" - whether to add（-inf, inf）as start/end point in generated range of bins.
     The above "log_bin" descriptor generates a set of bins:[-inf, 1.0, 2.0, 4.0, 8.0, 16.0, +inf].

3. normalized: setting to 1/0 to turn on/off result normalization.

### PERCENTILE

```
SELECT PERCENTILE(field_name, P) FROM { tb_name } [WHERE clause];
```

**Description**: The value whose rank in a specific column matches the specified percentage. If such a value matching the specified percentage doesn't exist in the column, an interpolation value will be returned.

**Return value type**: DOUBLE.

**Applicable column types**: Numeric types.

**Applicable table types**: table.

**More explanations**: _P_ is in range [0,100], when _P_ is 0, the result is same as using function MIN; when _P_ is 100, the result is same as function MAX.

## Selector Functions

Selector functiosn choose one or more rows in the query result according to the semantics. You can specify to output primary timestamp column and other columns including tbname and tags so that you can easily know which rows the selected values belong to.

### BOTTOM

```
SELECT BOTTOM(field_name, K) FROM { tb_name | stb_name } [WHERE clause];
```

**Description**: The least _k_ values of a specific column in a table or STable. If a value has multiple occurrences in the column but counting all of them in will exceed the upper limit _k_, then a part of them will be returned randomly.

**Return value type**: Same as the column being operated upon.

**Applicable column types**: Numeric types.

**Applicable table types**: table, STable.

**More explanations**:

- _k_ must be in range [1,100].
- The timestamp associated with the selected values are returned too.
- Can't be used with `FILL`.

### FIRST

```
SELECT FIRST(field_name) FROM { tb_name | stb_name } [WHERE clause];
```

**Description**: The first non-null value of a specific column in a table or STable.

**Return value type**: Same as the column being operated upon.

**Applicable column types**: All data types.

**Applicable table types**: table, STable.

**More explanations**:

- FIRST(\*) can be used to get the first non-null value of all columns
- NULL will be returned if all the values of the specified column are all NULL
- A result will NOT be returned if all the columns in the result set are all NULL

### INTERP

```
SELECT INTERP(field_name) FROM { tb_name | stb_name } [WHERE where_condition] [ RANGE(timestamp1,timestamp2) ] [EVERY(interval)] [FILL ({ VALUE | PREV | NULL | LINEAR | NEXT})];
```

**Description**: The value that matches the specified timestamp range is returned, if existing; or an interpolation value is returned.

**Return value type**: Same as the column being operated upon.

**Applicable column types**: Numeric data types.

**Applicable table types**: table, STable, nested query.

**More explanations**

- `INTERP` is used to get the value that matches the specified time slice from a column. If no such value exists an interpolation value will be returned based on `FILL` parameter.
- The input data of `INTERP` is the value of the specified column and a `where` clause can be used to filter the original data. If no `where` condition is specified then all original data is the input.
- The output time range of `INTERP` is specified by `RANGE(timestamp1,timestamp2)` parameter, with timestamp1<=timestamp2. timestamp1 is the starting point of the output time range and must be specified. timestamp2 is the ending point of the output time range and must be specified. If `RANGE` is not specified, then the timestamp of the first row that matches the filter condition is treated as timestamp1, the timestamp of the last row that matches the filter condition is treated as timestamp2.
- The number of rows in the result set of `INTERP` is determined by the parameter `EVERY`. Starting from timestamp1, one interpolation is performed for every time interval specified `EVERY` parameter. If `EVERY` parameter is not used, the time windows will be considered as no ending timestamp, i.e. there is only one time window from timestamp1.
- Interpolation is performed based on `FILL` parameter. No interpolation is performed if `FILL` is not used, that means either the original data that matches is returned or nothing is returned.
- `INTERP` can only be used to interpolate in single timeline. So it must be used with `group by tbname` when it's used on a STable. It can't be used with `GROUP BY` when it's used in the inner query of a nested query.
- The result of `INTERP` is not influenced by `ORDER BY TIMESTAMP`, which impacts the output order only..

### LAST

```
SELECT LAST(field_name) FROM { tb_name | stb_name } [WHERE clause];
```

**Description**: The last non-NULL value of a specific column in a table or STable.

**Return value type**: Same as the column being operated upon.

**Applicable column types**: All data types.

**Applicable table types**: table, STable.

**More explanations**:

- LAST(\*) can be used to get the last non-NULL value of all columns
- If the values of a column in the result set are all NULL, NULL is returned for that column; if all columns in the result are all NULL, no result will be returned.
- When it's used on a STable, if there are multiple values with the timestamp in the result set, one of them will be returned randomly and it's not guaranteed that the same value is returned if the same query is run multiple times.

### LAST_ROW

```
SELECT LAST_ROW(field_name) FROM { tb_name | stb_name };
```

**Description**: The last row of a table or STable.

**Return value type**: Same as the column being operated upon.

**Applicable column types**: All data type.

**Applicable table types**: table, STable.

**More explanations**:

- When it's used against a STable, multiple rows with the same and largest timestamp may exist, in this case one of them is returned randomly and it's not guaranteed that the result is same if the query is run multiple times.
- Cannot be used with `INTERVAL`.

### MAX

```
SELECT MAX(field_name) FROM { tb_name | stb_name } [WHERE clause];
```

**Description**: The maximum value of a specific column of a table or STable.

**Return value type**: Same as the data type of the column being operated upon.

**Applicable column types**: Numeric types.

**Applicable table types**: table, STable.

### MIN

```
SELECT MIN(field_name) FROM {tb_name | stb_name} [WHERE clause];
```

**Description**: The minimum value of a specific column in a table or STable.

**Return value type**: Same as the data type of the column being operated upon.

**Applicable column types**: Numeric types.

**Applicable table types**: table, STable.

### MODE

```
SELECT MODE(field_name) FROM tb_name [WHERE clause];
```

**Description**:The value which has the highest frequency of occurrence. NULL is returned if there are multiple values which have highest frequency of occurrence.

**Return value type**:Same as the data type of the column being operated upon.

**Applicable column types**: All data types.

**More explanations**:Considering the number of returned result set is unpredictable, it's suggested to limit the number of unique values to 100,000, otherwise error will be returned.

### SAMPLE

```sql
    SELECT SAMPLE(field_name, K) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: _k_ sampling values of a specific column. The applicable range of _k_ is [1,1000].

**Return value type**: Same as the column being operated.

**Applicable data types**: All data types.

**Applicable table types**: table, STable.

**Applicable nested query**: Inner query and Outer query.

**More explanations**:

- Arithmetic operation cannot be operated on the result of `SAMPLE` function
- Must be used with `Partition by tbname` when it's used on a STable to force the result on each single timeline.

### TAIL

```
SELECT TAIL(field_name, k, offset_val) FROM {tb_name | stb_name} [WHERE clause];
```

**Description**: The next _k_ rows are returned after skipping the last `offset_val` rows, NULL values are not ignored. `offset_val` is optional parameter. When it's not specified, the last _k_ rows are returned. When `offset_val` is used, the effect is same as `order by ts desc LIMIT k OFFSET offset_val`.

**Parameter value range**: k: [1,100] offset_val: [0,100].

**Return value type**: Same as the column being operated upon.

**Applicable column types**: All data types.

### TOP

```
SELECT TOP(field_name, K) FROM { tb_name | stb_name } [WHERE clause];
```

**Description**: The greatest _k_ values of a specific column in a table or STable. If a value has multiple occurrences in the column but counting all of them in will exceed the upper limit _k_, then a part of them will be returned randomly.

**Return value type**: Same as the column being operated upon.

**Applicable column types**: Numeric types.

**Applicable table types**: table, STable.

**More explanations**:

- _k_ must be in range [1,100].
- The timestamp associated with the selected values are returned too.
- Cannot be used with `FILL`.

### UNIQUE

```
SELECT UNIQUE(field_name) FROM {tb_name | stb_name} [WHERE clause];
```

**Description**: The values that occur the first time in the specified column. The effect is similar to `distinct` keyword, but it can also be used to match tags or timestamp.

**Return value type**: Same as the column or tag being operated upon.

**Applicable column types**: All data types.

**More explanations**:

- It can be used against table or STable, but can't be used together with time window, like `interval`, `state_window` or `session_window` .
- Considering the number of result sets is unpredictable, it's suggested to limit the distinct values under 100,000 to control the memory usage, otherwise error will be returned.

## Time-Series Specific Functions

TDengine provides a set of time-series specific functions to better meet the requirements in querying time-series data. In general databases, similar functionalities can only be achieved with much more complex syntax and much worse performance. TDengine provides these functionalities in builtin functions so that the burden on user side is minimized.

### CSUM

```sql
    SELECT CSUM(field_name) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: The cumulative sum of each row for a specific column. The number of output rows is same as that of the input rows.

**Return value type**: BIGINT for signed integer input types; UNSIGNED BIGINT for unsigned integer input types; DOUBLE for floating point input types. 

**Applicable data types**: Numeric types.

**Applicable table types**: table, STable.

**Applicable nested query**: Inner query and Outer query.

**More explanations**:
- Arithmetic operation cannot be performed on the result of `csum` function.
- Can only be used with aggregate functions.
- `Partition by tbname` must be used together on a STable to force the result on a single timeline.

### DERIVATIVE

```
SELECT DERIVATIVE(field_name, time_interval, ignore_negative) FROM tb_name [WHERE clause];
```

**Description**: The derivative of a specific column. The time rage can be specified by parameter `time_interval`, the minimum allowed time range is 1 second (1s); the value of `ignore_negative` can be 0 or 1, 1 means negative values are ignored.

**Return value type**: DOUBLE.

**Applicable column types**: Numeric types.

**Applicable table types**: table, STable.

**More explanations**:

- The number of result rows is the number of total rows in the time range subtracted by one, no output for the first row.
- It can be used together with `PARTITION BY tbname` against a STable.
- Can be used together with selection of relative columns. E.g. select \_rowts, DERIVATIVE() from.

### DIFF

```sql
SELECT {DIFF(field_name, ignore_negative) | DIFF(field_name)} FROM tb_name [WHERE clause];
```

**Description**: The different of each row with its previous row for a specific column. `ignore_negative` can be specified as 0 or 1, the default value is 1 if it's not specified. `1` means negative values are ignored.

**Return value type**: Same as the column being operated upon.

**Applicable column types**: Numeric types.

**Applicable table types**: table, STable.

**More explanations**:

- The number of result rows is the number of rows subtracted by one, no output for the first row.
- It can be used on STable with `PARTITION by tbname`.
- Can be used together with selection of relative columns. E.g. select \_rowts, DIFF() from.

### IRATE

```
SELECT IRATE(field_name) FROM tb_name WHERE clause;
```

**Description**: instantaneous rate on a specific column. The last two samples in the specified time range are used to calculate instantaneous rate. If the last sample value is smaller, then only the last sample value is used instead of the difference between the last two sample values.

**Return value type**: DOUBLE.

**Applicable column types**: Numeric types.

**Applicable table types**: table, STable.

**More explanations**:

- It can be used on stble with `PARTITION BY`, i.e. timelines generated by `PARTITION BY tbname` on a STable.

### MAVG

```sql
    SELECT MAVG(field_name, K) FROM { tb_name | stb_name } [WHERE clause]
```

**Description**: The moving average of continuous _k_ values of a specific column. If the number of input rows is less than _k_, nothing is returned. The applicable range of _k_ is [1,1000].

**Return value type**: DOUBLE.

**Applicable data types**: Numeric types.

**Applicable nested query**: Inner query and Outer query.

**Applicable table types**: table, STable.

**More explanations**:

- Arithmetic operation cannot be performed on the result of `MAVG`.
- Cannot be used with aggregate functions.
- Must be used with `PARTITION BY tbname` when it's used on a STable to force the result on each single timeline.

### STATECOUNT

```
SELECT STATECOUNT(field_name, oper, val) FROM { tb_name | stb_name } [WHERE clause];
```

**Description**: The number of continuous rows satisfying the specified conditions for a specific column. If the specified condition is evaluated as true, the number is increased by 1; otherwise the number is reset to -1. If the input value is NULL, then the corresponding row is skipped.

**Applicable parameter values**:

- oper : Can be one of "LT" (lower than), "GT" (greater than), "LE" (lower than or euqal to), "GE" (greater than or equal to), "NE" (not equal to), "EQ" (equal to).
- val ： Numeric types.

**Return value type**: INTEGER.

**Applicable data types**: Numeric types.

**Applicable table types**: table, STable.

**Applicable nested query**: Outer query only.

**More explanations**:

- Must be used together with `PARTITION BY tbname` when it's used on a STable to force the result into each single timeline.
- Cannot be used with window operation, like interval/state_window/session_window.

### STATEDURATION

```
SELECT stateDuration(field_name, oper, val, unit) FROM { tb_name | stb_name } [WHERE clause];
```

**Description**: The length of time range in which all rows satisfy the specified condition for a specific column. The length for the first row that satisfies the condition is 0. Next, if the condition is evaluated as true for a row, the time interval between current row and its previous row is added up to the time range; otherwise the time range length is reset to -1. If the value of the column is NULL, the corresponding row is skipped.

**Applicable parameter values**:

- oper : Can be one of "LT" (lower than), "GT" (greater than), "LE" (lower than or euqal to), "GE" (greater than or equal to), "NE" (not equal to), "EQ" (equal to).
- val : Numeric types.
- unit : The unit of time interval, can be: 1b(nanosecond), 1u(microsecond),1a(millisecond),1s(second),1m(minute),1h(hour),1d(day),1w(week). If not specified, default is same as the current database time precision in use.

**Return value type**: INTEGER.

**Applicable data types**: Numeric types.

**Applicable table types**: table, STable.

**Applicable nested query**: Outer query only.

**More explanations**:

- Must be used together with `PARTITION BY tbname` when it's used on a STable to force the result into each single timeline.
- Cannot be used with window operation, like interval/state_window/session_window.

### TWA

```
SELECT TWA(field_name) FROM tb_name WHERE clause;
```

**Description**: Time weighted average on a specific column within a time range.

**Return value type**: DOUBLE.

**Applicable column types**: Numeric types.

**Applicable table types**: table, STable.

**More explanations**:

- It can be used on stable with `PARTITION BY`, i.e. timelines generated by `PARTITION BY tbname` on a STable.

## System Information Functions

### DATABASE

```
SELECT DATABASE();
```

**Description**：Return the current database being used. If the user doesn't specify database when logon and doesn't use `USE` SQL command to switch the datbase, this function returns NULL.

### CLIENT_VERSION

```
SELECT CLIENT_VERSION();
```

**Description**：Return the client version.

### SERVER_VERSION

```
SELECT SERVER_VERSION();
```

**Description**：Returns the server version.

### SERVER_STATUS

```
SELECT SERVER_VERSION();
```

**Description**：Returns the server's status.
