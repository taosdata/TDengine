---
title: Correlation Analysis
sidebar_label: Correlation Analysis
---

TDgpt provides the following correlation analysis capabilities for time-series data.

## CORR

```sql
CORR(expr)
```

`CORR` calculates the Pearson correlation coefficient between two time series, reflecting their linear correlation. For details on using the `corr` function, refer to the CORR function documentation.

:::note

The `CORR` function does not require TDgpt. You can use this function in TDengine TSDB directly.

:::

## DTW

`DTW` uses a dynamic programming approach to perform nonlinear temporal alignment between two time series, then calculates their similarity. A smaller result indicates higher similarity.

The `DTW` function uses Manhattan distance (ignoring the time dimension). Euclidean distance is not supported.

### Syntax

```sql
DTW(column1_name, column2_name, option_expr)

option_expr: {
"radius=expr
[,expr2]"
}
```

1. `column1_name` and `column2_name`: Two time series columns participating in DTW calculation.
2. `option_expr`: String specifying DTW parameters in comma-separated K=V format. Do not use quotation marks, escape characters, or non-ASCII characters.
3. `radius=2` indicates a neighborhood radius of 2, limiting the DTW path to adjacent values within the distance matrix.
4. White noise detection is not supported, and algorithm selection is not required.
5. Maximum supported input is 10,240 rows. Exceeding this limit triggers `Analysis failed since too many input rows` (0x80000446).

### Parameters

| Parameter | Description                                                  | Default |
| --------- | ------------------------------------------------------------ | ------- |
| radius    | Neighborhood radius limiting the DTW search space. Smaller radius improves speed but may reduce accuracy; larger radius increases accuracy at higher cost. | 1       |

1. Supports numeric column input
2. Returns a double-precision floating-point value

### Example

Calculate DTW between col1 and col2 without specifying radius:

```sql
SELECT dtw(col1, col2)
FROM foo;
```

Specify radius = 2:

```sql
SELECT dtw(col1, col2, 'radius=2')
FROM foo;
```

## DTW_PATH

`DTW_PATH` uses dynamic programming to align two time series nonlinearly for similarity calculation. Unlike `DTW`, `DTW_PATH` returns the matching index pairs used in the similarity calculation.

### Syntax

```sql
DTW_PATH(column1_name, column2_name, option_expr)

option_expr: {
"radius=expr
[,expr2]"
}
```

`DTW_PATH` has the same conditions and constraints as `DTW` but returns a string representing index mappings between the two series.

### Example

```sql
taos> select col1, col2 from foo;
taos> select dtw_path(col1, col2,'radius=1') res from foo;
```

## TLCC

`TLCC` returns correlation values between two time series under different time lags to evaluate dynamic relationships. This is commonly used to identify delayed effects between sequences and determine direction and magnitude.

### Syntax

```sql
TLCC(column1_name, column2_name, option_expr)

option_expr: {
"lag_start=expr,
lag_end=expr,
[,expr2]"
}
```

1. `column1_name` and `column2_name`: Two time series columns used for analysis.
2. `option_expr`: String parameters in comma-separated key=value format. Do not use quotation marks or non-ASCII characters.
3. White noise detection is not supported, and algorithm selection is not required.
4. Maximum supported input is 10,240 rows; exceeding this triggers `Analysis failed since too many input rows` (0x80000446).

### Parameters

| Parameter | Description        | Default |
| --------- | ------------------ | ------- |
| lag_start | Starting lag value | -1      |
| lag_end   | Ending lag value   | 1       |

1. Supports numeric columns only
2. Returns correlation values across different lag offsets

### Example

Calculate correlation across default lag range:

```sql
SELECT tlcc(col1, col2)
FROM foo;
```

Specify lag range:

```sql
SELECT tlcc(col1, col2, 'lag_start=-10, lag_end=10')
FROM foo;
```
