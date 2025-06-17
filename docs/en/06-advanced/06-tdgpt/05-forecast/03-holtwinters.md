---
title: Holt-Winters
sidebar_label: Holt-Winters
---

This document describes the usage of the Holt-Winters method for forecasting.

## Description

Holt-Winters, or exponential moving average (EMA), is used to forecast non-stationary time series that have linear trends or periodic fluctuations. This method uses exponential smoothing to constantly adapt the model parameters to the changes in the time series and perform short-term forecasting.
If seasonal variation remains mostly consistent within a time series, the additive Holt-Winters model is used, whereas if seasonal variation is proportional to the level of the time series, the multiplicative Holt-Winters model is used.
Holt-Winters does not provide results within a confidence interval. The forecast results are the same as those on the upper and lower thresholds of the confidence interval.

### Parameters

Automated Holt-Winters modeling is performed in TDgpt. For this reason, the results for each input are automatically fitted to the most appropriate model. Forecasting is then performed based on the specified model.

|Parameter|Description|Required?|
|---|---|---|
|period|The number of data points included in each period. If not specified or set to 0, exponential smoothing is applied for data fitting, and then future data is forecast.|No|
|trend|Use additive (`add`) or multiplicative (`mul`) Holt-Winters for the trend model.|No|
|seasonal|Use additive (`add`) or multiplicative (`mul`) Holt-Winters for seasonality.|No|

### Example

In this example, forecasting is performed on the `i32` column. Each 10 data points in the column form a period. Multiplicative Holt-Winters is used for trends and for seasonality.

```sql
FORECAST(i32, "algo=holtwinters,period=10,trend=mul,seasonal=mul")
```

The complete SQL statement is shown as follows:

```sql
SELECT _frowts, FORECAST(i32, "algo=holtwinters, period=10,trend=mul,seasonal=mul") from foo
```

```json
{
"rows": fc_rows,  // Rows returned
"period": period, // Period of results (equivalent to input period; set to 0 if no periodicity)
"algo": 'holtwinters' // Algorithm
"mse": mse,       // Mean square error (MSE)
"res": res        // Results in column format (typically returned as two columns, `timestamp` and `fc_results`.)
}
```

### References

- [Exponential smoothing](https://en.wikipedia.org/wiki/Exponential_smoothing)
- [Holt-Winters Forecasting Simplified](https://orangematter.solarwinds.com/2019/12/15/holt-winters-forecasting-simplified/)
