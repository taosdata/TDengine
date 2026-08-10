---
title: ETS
sidebar_label: ETS
---

This document describes the usage of error, trend, and seasonal (ETS) models for forecasting.

## Description

ETS uses state-space models to describe the error, trend, and seasonal components of a time series. TDgpt uses automated ETS modeling to select an appropriate component combination for the input time series and forecast future data.

ETS provides confidence intervals for forecast results.

### Parameters

|Parameter|Description|Required?|
|---|---|---|
|period|The number of data points included in each period. If not specified or set to 0, a non-seasonal model is used.|No|
|model|Three-character ETS model. The characters specify the error, trend, and seasonal components. The default value is `ZZZ`, which automatically selects each component.|No|
|damped|Whether to use a damped trend. Enter `1` to enable or `0` to disable it. If not specified, the algorithm determines the setting based on the model.|No|

In the model string, `A` specifies an additive component, `M` specifies a multiplicative component, `N` omits the component, and `Z` automatically selects it. For example, `ANN` specifies additive errors without trend or seasonality.

Input data must contain only finite numeric values. A non-seasonal forecast requires at least two input values. When `period` is greater than 0, the input data must contain at least two complete periods. Multiplicative components require positive input data.

### Example

In this example, forecasting is performed on the `i32` column. Each 12 data points form a period. ETS automatically selects the error, trend, and seasonal components, and uses a damped trend.

```sql
FORECAST(i32, "algo=ets,period=12,model=ZZZ,damped=1")
```

The complete SQL statement is shown as follows:

```sql
SELECT _frowts, FORECAST(i32, "algo=ets,period=12,model=ZZZ,damped=1") from foo
```

```json
{
"rows": rows,       // Rows returned
"period": period,   // Period of results
"algo": "ets",     // Algorithm
"mse": mse,         // Mean square error (MSE)
"res": res          // Forecast results and confidence intervals in column format
}
```

### References

- [AutoETS](https://nixtlaverse.nixtla.io/statsforecast/src/core/models.html#autoets)
- [Forecasting with Exponential Smoothing: The State Space Approach](https://robjhyndman.com/expsmooth/)
