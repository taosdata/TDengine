---
title: Model Evaluation Tools
sidebar_label: Model Evaluation Tools
description: Evaluate TDgpt forecasting and anomaly-detection models
---

TDgpt Enterprise includes the `analytics_compare` tool for backtesting forecasting and anomaly-detection models against time-series data stored in TDengine.

> This tool is not available in TDgpt OSS.

Configure the data range, models, model parameters, and optional charts in `analytics.ini`. First configure the TDengine connection and input data:

```ini
[taosd]
host = 127.0.0.1
user = root
password = taosdata
conf = /etc/taos/taos.cfg

[input_data]
db_name = test
table_name = passengers
column_name = val, _c0
```

Run `analytics_compare.py` from the `misc` directory in the TDgpt installation. Use the Python executable from the TDgpt virtual environment so that all dependencies are available.

## Evaluate Forecasting Models

1. Load the sample data included in the TDgpt `resource` directory:

   ```shell
   taos -f sample-fc.sql
   ```

2. Configure the forecast:

   ```ini
   [forecast]
   period = 12
   rows = 10
   start_time = 1949-01-01T00:00:00
   end_time = 1960-12-01T00:00:00
   res_start_time = 1730000000000
   gen_figure = true

   [forecast.algos]
   holtwinters={"trend":"add", "seasonal":"add"}
   arima={"time_step": 3600000, "start_p": 0, "max_p": 5, "start_q": 0, "max_q": 5}
   ```

3. Run the comparison:

   ```shell
   python3 ./analytics_compare.py forecast
   ```

The tool creates `fc_result.xlsx`. Its first sheet lists the algorithm, parameters, mean squared error (MSE), and elapsed time. If `gen_figure` is `true`, additional sheets contain a chart for each model. Support for MAPE and MAE is planned.

## Evaluate Anomaly-Detection Models

Anomaly-detection evaluation reports precision and recall.

1. Load the included sample data:

   ```shell
   taos -f sample-ad.sql
   ```

2. Configure the data range, expected anomaly indexes, and algorithms:

   ```ini
   [ad]
   start_time = 2021-01-01T01:01:01
   end_time = 2021-01-01T01:01:11
   gen_figure = true
   anno_res = [9]

   [ad.algos]
   ksigma={"k": 2}
   iqr={}
   grubbs={}
   lof={"algorithm":"auto", "neighbors": 3}
   ```

   Before running the comparison, manually label each expected anomaly in `anno_res` by its zero-based position. For example, use `[0, 9]` when the first and tenth points are anomalies.

3. Run the comparison:

   ```shell
   python3 ./analytics_compare.py anomaly-detection
   ```

The tool creates `ad_result.xlsx`. Its first sheet lists each algorithm, parameters, precision, recall, and elapsed time. If `gen_figure` is `true`, additional sheets visualize the detection results.
