---
title: Data Imputation
sidebar_label: Data Imputation
---

TDgpt provides data imputation based on time-series foundation models. It automatically detects missing time-series data points based on timestamps.

The timestamp intervals of the time-series data used for imputation must be strictly equal. In real-world environments, however, this is often not the case. To resolve this issue, perform the following:

- Large difference in timestamp intervals: Perform window aggregation and then apply imputation on the aggregated results.

   ```sql
   SELECT IMPUTATION(col_val)
   FROM (SELECT _wstart, MIN(val) col_val FROM foo INTERVAL(1s));
   ```

- Small difference in timestamp intervals: Use the `TIMETRUNCATE` function to truncate timestamps before performing imputation.

   ```sql
   SELECT IMPUTATION(col_val)
   FROM (SELECT TIMETRUNCATE(val) FROM foo);
   ```

## Syntax

```sql
IMPUTATION(column_name, option_expr)

option_expr: {
"algo=expr1
[,wncheck=1|0]
[,expr2]"
}
```

1. `column_name`: Input column for time-series imputation. Enter a single numeric column only. String types such as `VARCHAR` are not supported.  
2. `options`: A string specifying the anomaly detection algorithm and related parameters, formatted as comma-separated key=value pairs. Do not use quotes, escape characters, or non-ASCII characters. For example, `algo=moment` indicates using the `moment` time-series model with no additional parameters.  
3. You can use the results of imputation as subquery input for outer queries. Aggregation or scalar functions in the `SELECT` clause behave the same as in other window queries.  
4. White noise detection is enabled by default. If the input data is detected as white noise, an error will be returned.

## Parameters

| Parameter | Description                                                  | Default     |
| --------- | ------------------------------------------------------------ | ----------- |
| algo      | Algorithm used for automatic imputation                      | moment      |
| wncheck   | Whether to perform white noise detection (0 or 1)            | 1           |
| freq      | Standard frequency of the input time-series data (interval between records) | seconds (s) |

1. Only the `moment` time-series foundation model is supported for automatic imputation.  
2. Ensure that the `moment` model is properly deployed. Refer to the time-series foundation model deployment documentation.  
3. Optional `freq` values: d (days), h (hours), m (minutes), s (seconds), ms (milliseconds), us (microseconds). The default is `s` (seconds). Setting an incorrect `freq` may affect time-series integrity detection and produce incorrect results.  
4. The user-specified `freq` must not exceed the actual interval of the input time-series data; otherwise, incorrect results may occur.  
5. Timestamp resolution is automatically detected and matches the database time precision. Users do not need to specify it manually.  
6. The maximum number of imputed records per operation is 2048. For a single input time series, at most 2048 missing records can be filled at once. Excessive missing data will result in an error. Each input requires at least 10 records and supports up to 8192 records.

## Pseudocolumns

The pseudocolumn `_improwts` displays the corresponding timestamp for each imputed data point.

## Example

Use the moment model for automatic imputation on column `i32`, with frequency in seconds, outputting timestamps and values:

```sql
SELECT _improwts, imputation(i32, 'algo=moment, freq=s')
FROM foo;
```
