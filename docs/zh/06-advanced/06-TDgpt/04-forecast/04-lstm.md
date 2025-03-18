---
title: "LSTM"
sidebar_label: "LSTM"
---

本节说明 LSTM 模型的使用方法。

## 功能概述

LSTM模型即长短期记忆网络(Long Short Term Memory)，是一种特殊的循环神经网络，适用于处理时间序列数据、自然语言处理等任务，通过其独特的门控机制，能够有效捕捉长期依赖关系，
解决传统RNN的梯度消失问题，从而对序列数据进行准确预测，不过它不直接提供计算的置信区间范围结果。


完整的调用SQL语句如下：
```SQL
SELECT _frowts, FORECAST(i32, "algo=lstm,alpha=95,period=10,start_p=1,max_p=5,start_q=1,max_q=5") from foo
```

```json5
{
"rows": fc_rows,  // 返回结果的行数
"period": period, // 返回结果的周期性，同输入
"alpha": alpha,   // 返回结果的置信区间，同输入
"algo": "lstm",  // 返回结果使用的算法
"mse": mse,       // 拟合输入时间序列时候生成模型的最小均方误差(MSE)
"res": res        // 列模式的结果
}
```

### 参考文献
- [1] Hochreiter S. Long Short-term Memory[J]. Neural Computation MIT-Press, 1997.