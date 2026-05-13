---
title: ARIMA
sidebar_label: ARIMA
---

This document describes how to generate autoregressive integrated moving average (ARIMA) models.

## Description

The ARIMA(*p*, *d*, *q*) model is one of the most common in time-series forecasting.
It is an autoregressive model that can predict future data from an independent variable. ARIMA requires that time-series data be stationary. Accurate results cannot be obtained from non-stationary data.

A stationary time series is one whose characteristics do not change based on the time at which it is observed. Time series that experience trends or seasonality are not stationary because they exhibit different characteristics at different times.

The following variables can be dynamically input to generate appropriate ARIMA models:

- *p* is the order of the autoregressive model
- *d* is the order of differencing
- *q* is the order of the moving-average model

### Parameters

Automated ARIMA modeling is performed in TDgpt. For this reason, the results for each input are automatically fitted to the most appropriate model. Forecasting is then performed based on the specified model.

|Parameter|Description|Required?|
|---|---|-----|
|period|The number of data points included in each period. If not specified or set to 0, non-seasonal ARIMA models are used.|No|
|start_p|The starting order of the autoregressive model. Enter an integer greater than or equal to 0. Values greater than 10 are not recommended.|No|
|max_p|The ending order of the autoregressive model. Enter an integer greater than or equal to 0. Values greater than 10 are not recommended.|No|
|start_q|The starting order of the moving-average model. Enter an integer greater than or equal to 0. Values greater than 10 are not recommended.|No|
|max_q|The ending order of the moving-average model. Enter an integer greater than or equal to 0. Values greater than 10 are not recommended.|No|
|d|The order of differencing.|No|

The `start_p`, `max_p`, `start_q`, and `max_q` parameters cause the model to find the optimal solution within the specified restrictions. Given the same input data, a larger range will result in higher resource consumption and slower response time.

### Example

In this example, forecasting is performed on the `i32` column. Each 10 data points in the column form a period. The values of `start_p` and `start_q` are both 1, and the corresponding ending values are both 5. The forecasting results are within a 95% confidence interval.

```sql
FORECAST(i32, "algo=arima,alpha=95,period=10,start_p=1,max_p=5,start_q=1,max_q=5")
```

The complete SQL statement is shown as follows:

```sql
SELECT _frowts, FORECAST(i32, "algo=arima,alpha=95,period=10,start_p=1,max_p=5,start_q=1,max_q=5") from foo
```

```json
{
"rows": fc_rows,  // Rows returned
"period": period, // Period of results (equivalent to input period)
"alpha": alpha,   // Confidence interval of results (equivalent to input confidence interval)
"algo": "arima",  // Algorithm
"mse": mse,       // Mean square error (MSE) of model generated for input time series
"res": res        // Results in column format
}
```

### References

- [Autoregressive moving-average model](https://en.wikipedia.org/wiki/Autoregressive_moving-average_model)
