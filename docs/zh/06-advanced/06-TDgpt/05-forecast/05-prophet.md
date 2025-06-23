---
title: "prophet"
sidebar_label: "prophet"
---

This section explains how to use the prophet time series data prediction analysis model.

## Functional Overview

Prophet is a time series forecasting tool open-sourced by Facebook (Meta), specifically designed to handle business data with significant trend changes, strong seasonality, and holiday effects. In TDengine's TD-GPT system, Prophet has been integrated as an optional prediction algorithm. Users can directly call this model through simple SQL queries without complex coding, enabling rapid predictive analysis of time series data.

## Available Parameter List

### The following are the general parameters supported by Prophet in TDengine:

| Parameter Name           | Default Value | Type     | Description                                                  |
|--------------------------|---------------|----------|--------------------------------------------------------------|
| p_cfg_tpl                | None          | string   | Custom parameter template name, not effective if not specified |


### Model Configuration
To meet diverse prediction needs, TDengine supports configuring Prophet model parameters through yaml files, allowing users to conveniently configure holidays, customize Prophet behavior, etc.
#### Configuration Directory
Configuration files are typically located in the cfg folder under the taosanode installation directory

For example, if the taosanode installation directory is ```/usr/local/taos/taosanode```, then the configuration file directory is ```/usr/local/taos/taosanode/cfg```

#### Configuration File
- ***File path determination rule***: If the p_cfg_tpl value is demo, and the taosanode installation directory is ```/usr/local/taos/taosanode```, then the complete path of the configuration file read by the model is ```/usr/local/taos/taosanode/cfg/prophet_demo.yaml```

- ***Default behavior***: If ```p_cfg_tpl``` is not provided, the model will use the default behavior of ```Prophet``` for prediction

#### Configuration File Example

```yaml
saturating_cap_max: false          # Whether to automatically use the maximum value of input data as the saturation upper limit (cap)
saturating_cap: null               # Manually specify the saturation upper limit value, higher priority than saturating_cap_max
saturating_cap_scale: null         # Scaling factor for the saturation upper limit, e.g., 1.2 means cap = maximum value * 1.2

saturating_floor_min: false        # Whether to automatically use the minimum value of input data as the saturation lower limit (floor)
saturating_floor: null             # Manually specify the saturation lower limit value, higher priority than saturating_floor_min
saturating_floor_scale: null       # Scaling factor for the saturation lower limit, e.g., 0.8 means floor = minimum value * 0.8

seasonality_mode: "additive"       # Seasonality model type, additive or multiplicative
seasonality_prior_scale: 10.0      # Controls seasonality flexibility, higher values make the model more flexible
holidays_prior_scale: 10.0         # Controls holiday effect strength, higher values have more significant impact
changepoint_prior_scale: 0.05      # Trend changepoint sensitivity, higher values make the model more sensitive to trend changes

daily_seasonality: true            # Whether to enable daily seasonality
weekly_seasonality: false           # Whether to enable weekly seasonality
yearly_seasonality: false           # Whether to enable yearly seasonality

holidays:
  - holiday: "New Year"           # Holiday name
    dates:
      - "2025-01-01"
      - "2025-01-02"
    lower_window: 0               # Number of days affected before the holiday (negative numbers indicate days before the holiday)
    upper_window: 1               # Number of days affected after the holiday

  - holiday: "Labor Day"
    dates:
      - "2025-05-01"
      - "2025-05-02"
      - "2025-05-03"
    lower_window: 0
    upper_window: 0

  - holiday: "Custom Holiday"
    dates:
      - "2025-08-15"
    lower_window: -1              # The day before the holiday is also considered as a holiday impact period
    upper_window: 2               # Two days after the holiday are considered as holiday impact periods
```

#### Complete Parameters

The following are the parameters supported by Prophet in TDengine, which affect the basic prediction behavior of the model
##### Initialization Parameters

| Parameter Name            | Default Value | Type     | Description                                                  |
|---------------------------|---------------|----------|--------------------------------------------------------------|
| growth                    | linear        | string   | Trend model type: `linear` or `logistic` (saturating)        |
| changepoint_prior_scale   | 0.05          | float    | Trend change sensitivity, higher values are more sensitive   |
| changepoint_range         | 0.8           | float    | Controls the proportion of the changepoint search range      |
| daily_seasonality         | auto          | bool     | Whether to enable daily seasonality modeling                 |
| weekly_seasonality        | auto          | bool     | Whether to enable weekly seasonality modeling                |
| yearly_seasonality        | auto          | bool     | Whether to enable yearly seasonality modeling                |
| seasonality_mode          | additive      | string   | Seasonality mode: `additive` or `multiplicative`             |
| seasonality_prior_scale   | 10.0          | float    | Controls seasonality complexity                              |
| holidays_prior_scale      | 10.0          | float    | Controls holiday effect strength                             |
| mcmc_samples              | 0             | int      | Number of MCMC samples, used when enabling Bayesian          |
| holidays                  | -             | string   | Custom holiday list (can be JSON encoded or registered template) |

##### Resampling Parameters

| Parameter Name    | Default Value | Type   | Description                                      |
|-------------------|--------------|--------|--------------------------------------------------|
| resample          | None         | string | Sampling frequency, such as `1S`, `1T`, `1H`, `1D`, etc. |
| resample_mode     | mean         | string | Sampling method: mean, max, min, sum, etc.       |

##### Saturation Parameters

| Parameter Name             | Default Value | Type   | Description                                           |
|----------------------------|--------------|--------|-------------------------------------------------------|
| saturating_cap_max         | false        | bool   | Automatically set the upper limit to the historical maximum value |
| saturating_cap             | -            | float  | Manually set the upper limit value                    |
| saturating_cap_scale       | -            | float  | Upper limit scaling factor                           |
| saturating_floor_min       | false        | bool   | Automatically set the lower limit to the historical minimum value |
| saturating_floor           | -            | float  | Manually set the lower limit value                   |
| saturating_floor_scale     | -            | float  | Lower limit scaling factor                          |

### Query Examples

For data prediction on the temperature column, starting from timestamp 1748361600000, outputting 10 prediction points with a 60-second interval; p_cfg_tpl is the model parameter template:

```
FORECAST(temperature,"algo=prophet,rows=10,start=1748361600000,every=60,p_cfg_tpl=demo")
```

The complete SQL query statement is as follows:

```SQL
SELECT _frowts, _fhigh, _frowts, FORECAST(temperature,"algo=prophet,rows=10,start=1748361600000,every=60,p_cfg_tpl=demo") FROM (SELECT * FROM test.boiler_temp WHERE ts >= '2025-05-21 00:00:00' AND ts < '2025-05-25 00:00:00' ORDER BY ts DESC LIMIT 10 ) foo;
```

### References

- https://facebook.github.io/prophet/docs/quick_start.html#python-api