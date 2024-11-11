---
title: "HoltWinters"
sidebar_label: "HoltWinters"
---

本节讲述 HoltWinters 算法模型的使用方法。

## 功能概述
HoltWinters 模型又称为多次指数平滑模型（EMA）。适用于含有线性趋势和周期波动的非平稳序列，利用指数平滑法让模型参数不断适应非平稳序列的变化，并对未来趋势进行**短期**预测。
HoltWinters 有两种不同的季节性组成部分，当季节变化在该时间序列中大致保持不变时，通常选择**加法模型**；而当季节变化与时间序列的水平成比例变化时，通常选择**乘法模型**。
该模型对于返回数据不提供计算的置信区间范围结果，在 95% 置信区间的上下界结果与预测结果相同。


### 参数

分析平台中使用自动化的 HoltWinters 模型进行计算，因此每次计算的时候会根据输入的数据自动拟合最合适的模型，然后根据该模型进行预测输出结果。
|参数|说明|必填项|
|---|---|---|
|period|输入时间序列每个周期包含的数据点个数。如果不设置该参数或该参数设置为 0，将使用一次（简单）指数平滑方式进行数据拟合，并据此进行未来数据的预测|选填|
|trend|趋势模型使用加法模型还是乘法模型|选填|
|seasonal|季节性采用加法模型还是乘法模型|选填|

参数 `trend` 和 `seasonal`的均可以选择 `add` （加法模型）或 `mul`（乘法模型）。

### 示例及结果
针对 i32 列进行数据预测，输入列 i32 每 10 个点是一个周期，趋势采用乘法模型，季节采用乘法模型
```
FORECAST(i32, "algo=holtwinters,period=10,trend=mul,seasonal=mul")
```

```json5
{
"rows": rows,         // 返回结果的行数
"period": period,     // 返回结果的周期性，该结果与输入的周期性相同，如果没有周期性，该值为 0
"algo": 'holtwinters' // 返回结果使用的计算模型
"mse": mse,           // 最小均方误差（minmum square error）
"res": res            // 具体的结果，按照列形式返回的结果。一般意义上包含了两列 [timestamp][fc_results]。
}
```

### 参考文献
- https://en.wikipedia.org/wiki/Exponential_smoothing
- https://orangematter.solarwinds.com/2019/12/15/holt-winters-forecasting-simplified/
