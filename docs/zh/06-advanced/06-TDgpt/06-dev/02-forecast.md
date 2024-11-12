---
title: "预测算法"
sidebar_label: "预测算法"
---

对于预测算法，`AbstractForecastService` 的对象属性说明如下：

|属性名称|说明|默认值|
|---|---|---|
|period|输入时间序列的周期性，多少个数据点表示一个完整的周期。如果没有周期性，设置为 0 即可|	0|
|start_ts|预测结果的开始时间|	0|
|time_step|预测结果的两个数据点之间时间间隔|0	|
|fc_rows|预测结果的数量|	0	|
|return_conf|预测结果中是否包含置信区间范围，如果不包含置信区间，那么上界和下界与自身相同|	1|	
|conf|置信区间分位数 0.05|


预测返回结果如下：
```python
return {
    "rows": self.fc_rows,   # 预测数据行数
    "period": self.period,  # 数据周期性，同输入
    "algo": "holtwinters",  # 预测使用的算法
    "mse": mse,				# 预测算法的 mse
    "res": res              # 结果数组 [时间戳数组, 预测结果数组, 预测结果执行区间下界数组，预测结果执行区间上界数组]
}
```
