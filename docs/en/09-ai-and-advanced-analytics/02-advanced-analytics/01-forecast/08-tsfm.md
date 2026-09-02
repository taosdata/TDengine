---
title: Time-Series Foundation Model
sidebar_label: Time-Series Foundation Model
---

To use a time-series foundation model, deploy its service first. For instructions, see [Deploy a Time-Series Foundation Model](../../01-tdgpt/06-dev/03-tsfm/index.md).

## Description

Time-series foundation models are specifically trained to handle advanced time-series analysis tasks such as forecasting, anomaly detection, and imputation. These models inherit the strong generalization capabilities of large foundation models and can perform predictive analysis based on input data without the need for complex parameter configuration.

| Number | Parameter | Description |
| --- | --- | --- |
| 1 | `tdtsfm_1` | TDtsfm v1.0 |
| 2 | `timemoe-fc` | Time-MoE SQL model name |
| 3 | `moirai` | Salesforce time-series foundation model |
| 4 | `chronos` | Amazon time-series foundation model |
| 5 | `timesfm` | Google time-series foundation model |
| 6 | `moment` | CMU time-series foundation model |

After a model service is deployed and configured in TDgpt, invoke it through SQL by specifying its model name and the number of output rows.

- The following statement forecasts 10 rows of data using TDtsfm:

```sql
SELECT _frowts, FORECAST(i32, "algo=tdtsfm_1,rows=10") FROM foo;
```

- The following statement forecasts 10 rows of data using Time-MoE:

```sql
SELECT _frowts, FORECAST(i32, "algo=timemoe-fc,rows=10") FROM foo;
```

## References

1. [Time-MoE](https://github.com/Time-MoE/Time-MoE)
2. [Moirai](https://github.com/SalesforceAIResearch/uni2ts)
3. [Chronos](https://github.com/amazon-science/chronos-forecasting)
4. [TimesFM](https://github.com/google-research/timesfm/)
5. [MOMENT](https://github.com/moment-timeseries-foundation-model/moment)
