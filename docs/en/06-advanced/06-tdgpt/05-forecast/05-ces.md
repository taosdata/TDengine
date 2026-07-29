---
title: CES
sidebar_label: CES
---

This document describes the usage of complex exponential smoothing (CES) for forecasting.

## Description

CES uses complex-valued state-space equations to model level, trend, and seasonal changes in a time series. TDgpt uses automated CES modeling to select an appropriate model for the input time series and forecast future data.

CES provides confidence intervals for forecast results.

### Parameters

|Parameter|Description|Required?|
|---|---|---|
|period|The number of data points included in each period. If not specified or set to 0, a non-seasonal model is used.|No|
|model|CES model type. Enter `N`, `S`, `P`, `F`, or `Z`. The default value is `Z`, which automatically selects a model.|No|

`N` specifies a non-seasonal model, `S` specifies simple seasonality, `P` specifies partial seasonality, `F` specifies full seasonality, and `Z` automatically selects a model.

Input data must contain only finite numeric values. A non-seasonal forecast requires at least two input values. When `period` is greater than 0, the input data must contain at least two complete periods.

### Example

In this example, forecasting is performed on the `i32` column. Each 12 data points form a period, and CES automatically selects the model.

```sql
FORECAST(i32, "algo=ces,period=12,model=Z")
```

The complete SQL statement is shown as follows:

```sql
SELECT _frowts, FORECAST(i32, "algo=ces,period=12,model=Z") from foo
```

```json
{
"rows": rows,       // Rows returned
"period": period,   // Period of results
"algo": "ces",     // Algorithm
"mse": mse,         // Mean square error (MSE)
"res": res          // Forecast results and confidence intervals in column format
}
```

### References

- [AutoCES Model](https://nixtlaverse.nixtla.io/statsforecast/docs/models/autoces.html)
- [Complex Exponential Smoothing](https://doi.org/10.1002/nav.22074)
