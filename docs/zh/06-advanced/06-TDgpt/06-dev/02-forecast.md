---
title: "预测算法"
sidebar_label: "预测算法"
---

### 输入约定
`execute` 是预测算法处理的核心方法。框架调用该方法之前，在对象属性参数 `self.list` 中已经设置完毕用于预测的历史时间序列数据。

### 父类属性及输出约定
`execute` 方法执行完成后的返回值是长度与 `self.list` 相同的数组，数组位置 -1 的标识异常值点。

对于预测算法的父类 `AbstractForecastService` 包含的对象属性如下：

|属性名称|说明|默认值|
|---|---|---|
|period|输入时间序列的周期性，多少个数据点表示一个完整的周期。如果没有周期性，设置为 0 即可|	0|
|start_ts|预测结果的开始时间|	0|
|time_step|预测结果的两个数据点之间时间间隔|0	|
|fc_rows|预测结果的数量|	0	|
|return_conf|预测结果中是否包含置信区间范围，如果不包含置信区间，那么上界和下界与自身相同|	1|	
|conf|置信区间分位数|95|


预测返回结果如下：
```python
return {
    "rows": self.fc_rows,   # 预测数据行数
    "period": self.period,  # 数据周期性，同输入
    "algo": "holtwinters",  # 预测使用的算法
    "mse": mse,				# 预测算法的最小均方误差(minimum squared error)
    "res": res              # 结果数组 [时间戳数组, 预测结果数组, 预测结果执行区间下界数组，预测结果执行区间上界数组]
}
```

### 示例代码
下面我们开发一个示例预测算法，对于任何输入的时间序列数据，固定返回值 1 作为预测结果。

```python
import numpy as np
from service import AbstractForecastService

# 算法实现类名称 需要以下划线 "_" 开始，并以 Service 结束
class _MyForecastService(AbstractForecastService):
    """ 定义类，从 AbstractForecastService 继承，并实现 AbstractAnomalyDetectionService 类的抽象方法  """

    # 定义算法调用关键词，全小写ASCII码
    name = 'myad'

    # 该算法的描述信息(建议添加)
    desc = """return the last value as the anomaly data"""

    def __init__(self):
        """类初始化方法"""
        super().__init__()

    def execute(self):
	""" 算法逻辑的核心实现"""

        """创建一个长度为 len(self.list)，全部值为 1 的结果数组，然后将最后一个值设置为 -1，表示最后一个值是异常值"""
        res = [1] * len(self.list)
        res[-1] = -1

        """返回结果数组"""
        return res

	
    def set_params(self, params):
	"""该算法无需任何输入参数，直接重载父类该函数，不处理算法参数设置逻辑"""
        pass
```
