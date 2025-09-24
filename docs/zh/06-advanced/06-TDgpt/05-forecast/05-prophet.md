---
title: "PROPHET"
sidebar_label: "PROPHET"
---

本节说明 PROPHET 算法模型的使用方法。

## 功能概述

PROPHET：由 Facebook 开发的开源时间序列预测算法，记作 Prophet，适用于具有明显季节性、节假日效应以及趋势变化的时间序列数据。Prophet 模型对缺失值和异常值具有较强的鲁棒性，并且能够自动检测周期模式，支持自定义季节性和节假日参数，适用于商业预测、生态数据等多个领域。与 ARIMA 不同，Prophet 不需要时间序列完全平稳，且对多周期性（如年周期、周周期、日周期）具有良好的建模能力。

Prophet 模型的核心组成包括：

- 趋势部分：涵盖线性或饱和增长/下降趋势，并支持趋势变化点。
- 季节性部分：通过傅里叶级数拟合周期性模式。
- 节假日部分：支持注入自定义的节假日或事件效应。

以下参数可动态输入，用于调整 PROPHET 模型的预测行为。

## 参数

在分析平台中，PROPHET 模型支持自动化配置，但用户也可通过参数调整模型的拟合和预测过程。

| 参数                    | 说明                                                 | 必填项 |
| ----------------------- | ---------------------------------------------------- | ------ |
| growth                  | 趋势类型，可选 `linear` 或 `logistic`，默认 `linear` | 选填   |
| yearly_seasonality      | 是否启用年季节性（true/false/auto），默认 `auto`     | 选填   |
| weekly_seasonality      | 是否启用周季节性（true/false/auto），默认 `auto`     | 选填   |
| daily_seasonality       | 是否启用日季节性（true/false/auto），默认 `auto`     | 选填   |
| changepoint_prior_scale | 趋势转折点灵活度，数值越大越容易变化，默认 `0.05`    | 选填   |

### 示例及结果

针对列 `passengers`进行时间序列预测，数据按天记录，假设存在周周期性和年周期性，趋势转折点灵活度为 0.1，进行预测。

```text
FORECAST(passengers, "algo=prophet,growth=linear,yearly_seasonality=true,weekly_seasonality=true,changepoint_prior_scale=0.1")
````

完整的调用 SQL 语句如下：

```sql
SELECT _frowts,
       FORECAST(passengers, "algo=prophet,growth=linear,yearly_seasonality=true,weekly_seasonality=true,changepoint_prior_scale=0.1")
FROM air;
```

返回结果格式如下：

```json5
{
  "rows": fc_rows,                 // 返回的预测结果行数，即生成多少条预测数据
  "algo": "prophet",               // 使用的算法名称，这里是 Prophet
  "growth": "linear",              // 趋势类型，可选 linear 或 logistic，源码默认 linear
  "yearly_seasonality": "auto",    // 是否启用年季节性（true/false/auto），源码默认 auto
  "weekly_seasonality": "auto",    // 是否启用周季节性（true/false/auto），源码默认 auto
  "daily_seasonality": "auto",     // 是否启用日季节性（true/false/auto），源码默认 auto
  "changepoint_prior_scale": 0.05, // 趋势转折点灵活度，数值越大越容易发生趋势变化，源码默认 0.05
  "mse": mse,                      // 模型拟合误差指标：均方误差 (Mean Squared Error)
  "res": res                       // 预测结果列表，包含每个时间点的预测值及其区间（上下限）
}
```

### 参考文献

1. [Prophet 官方文档](https://facebook.github.io/prophet/)
2. Prophet 论文：Forecasting at Scale (2017)
