---
title: Prophet
sidebar_label: Prophet
---

Prophet is an open-source time-series forecasting algorithm developed by Facebook. It is suitable for series with clear seasonality, holiday effects, and trend changes. Prophet is robust to missing values and outliers, automatically detects periodic patterns, and supports custom seasonality and holiday parameters. Unlike ARIMA, it does not require a fully stationary series and can model yearly, weekly, and daily seasonalities.

The model consists of:

- A trend component with linear or saturating growth and changepoints.
- A seasonality component fitted with Fourier series.
- A holiday component for custom holidays or events.

## Parameters

| Parameter | Description | Required |
| --- | --- | --- |
| `growth` | Trend type: `linear` or `logistic`. Default: `linear` | No |
| `yearly_seasonality` | Enable yearly seasonality: `true`, `false`, or `auto`. Default: `auto` | No |
| `weekly_seasonality` | Enable weekly seasonality: `true`, `false`, or `auto`. Default: `auto` | No |
| `daily_seasonality` | Enable daily seasonality: `true`, `false`, or `auto`. Default: `auto` | No |
| `changepoint_prior_scale` | Trend-changepoint flexibility. Larger values allow more variation. Default: `0.05` | No |

## Example

Forecast the daily `passengers` series with yearly and weekly seasonality and a changepoint prior scale of `0.1`:

```sql
SELECT _frowts,
       FORECAST(passengers, "algo=prophet,growth=linear,yearly_seasonality=true,weekly_seasonality=true,changepoint_prior_scale=0.1")
FROM air;
```

The model result uses the following fields:

```json5
{
  "rows": fc_rows,
  "algo": "prophet",
  "growth": "linear",
  "yearly_seasonality": "auto",
  "weekly_seasonality": "auto",
  "daily_seasonality": "auto",
  "changepoint_prior_scale": 0.05,
  "mse": mse,
  "res": res
}
```

## References

1. [Prophet documentation](https://facebook.github.io/prophet/)
2. *Forecasting at Scale* (2017)
