---
title: Time-Series Foundation Model
sidebar_label: Time-Series Foundation Model
---

TDgpt includes two time-series foundation models, TDtsfm and Time-MoE, that can be used for forecasting.

## Description

Time-series foundation models are specifically trained to handle advanced time-series analysis tasks such as forecasting, anomaly detection, and imputation. These models inherit the strong generalization capabilities of large foundation models and can perform predictive analysis based on input data without the need for complex parameter configuration.

|Number| Parameter | Description|
|---|----------|------------------------|
|1  | tdtsfm_1 | TDtsfm v1.0|
|2  | time-moe | Time-MoE |

You can use these models in TDgpt without any configuration, only specifying the desired model and number of output rows in your SQL statement.

- The following statement forecasts 10 rows of data using TDtsfm:

```SQL
SELECT _frowts, FORECAST(i32, "algo=tdtsfm_1,rows=10") from foo
```

- The following statement forecasts 10 rows of data using Time-MoE:

```SQL
SELECT _frowts, FORECAST(i32, "algo=timemoe-fc,rows=10") from foo
```
