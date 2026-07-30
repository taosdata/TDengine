---
title: Theta
sidebar_label: Theta
---

This document describes the usage of the Theta method for forecasting.

## Description

The Theta method transforms an input time series into theta lines with different curvatures. The lines are forecast separately and then combined into the final result. The method is suitable for univariate time series with trends and can decompose seasonal data before forecasting.

Theta provides confidence intervals for forecast results.

### Parameters

|Parameter|Description|Required?|
|---|---|---|
|period|The number of data points included in each period. If not specified or set to 0, a non-seasonal model is used.|No|
|decomposition_type|Seasonal decomposition type. Enter `additive` or `multiplicative`. The default value is `multiplicative`.|No|

Use `additive` when seasonal changes remain relatively constant. Use `multiplicative` when seasonal changes are proportional to the level of the time series. Multiplicative decomposition requires positive input data.

Input data must contain only finite numeric values. A non-seasonal forecast requires at least two input values. When `period` is greater than 0, the input data must contain at least two complete periods.

### Example

In this example, forecasting is performed on the `i32` column. Each 12 data points form a period, and multiplicative seasonal decomposition is used.

```sql
FORECAST(i32, "algo=theta,period=12,decomposition_type=multiplicative")
```

The complete SQL statement is shown as follows:

```sql
SELECT _frowts, FORECAST(i32, "algo=theta,period=12,decomposition_type=multiplicative") from foo
```

```json
{
"rows": rows,       // Rows returned
"period": period,   // Period of results
"algo": "theta",   // Algorithm
"mse": mse,         // Mean square error (MSE)
"res": res          // Forecast results and confidence intervals in column format
}
```

### References

- [Standard Theta Model](https://nixtlaverse.nixtla.io/statsforecast/docs/models/standardtheta.html)
- [The Theta model: a decomposition approach to forecasting](https://doi.org/10.1016/S0169-2070(00)00066-2)
