---
title: "addins"
sidebar_label: "addins"
---

本节说明如何将自己开发的新预测算法和异常检测算法整合到 TDengine 分析平台， 并能够通过 SQL 语句进行调用。

## 目录结构


|目录|说明|
|---|---|
|taos|Python 源代码目录，其下包含了算法具体保存目录 algo，放置杂项目录 misc， 单元测试和集成测试目录 test。 algo目录下 ad 放置异常检测算法代码， fc 放置预测算法代码|
|script|是安装脚本和发布脚本放置目录|
|model|放置针对数据集完成的训练模型|
|cfg|	配置文件目录|

## 约定与限制

定义异常检测算法的 Python 代码文件 需放在 /taos/algo/ad 目录中，预测算法 Python 代码文件需要放在 /taos/algo/fc 目录中，以确保系统启动的时候能够正常加载对应目录下的 Python 文件。


### 类命名规范

算法类的名称需要 以下划线开始，以 Service 结尾。例如：_KsigmaService 是  KSigma 异常检测算法的实现类。

### 类继承约定

异常检测算法需要从 `AbstractAnomalyDetectionService` 继承，并实现其核心抽象方法 `execute`.
预测算法需要从 `AbstractForecastService` 继承，同样需要实现其核心抽象方法 `execute`。

### 类属性初始化
每个算法实现的类需要静态初始化两个类属性，分别是

`name`: 的触发调用关键词，全小写英文字母。
`desc`：该算法的描述信息。

### 核心方法输入与输出约定

`execute` 是算法处理的核心方法。调用该方法的时候， self.list 已经设置好输入数组。
异常检测输出结果

`execute` 的返回值是长度与  self.list 相同的数组，数组位置为 -1 的即为异常值点。例如：输入数组是  [2, 2, 2, 2, 100]， 如果 100 是异常点，那么返回值是 [1, 1, 1, 1, -1]。
预测输出结果

对于预测算法， `AbstractForecastService` 的对象属性说明如下：

|属性名称|说明|默认值|
|---|---|---|
|period|输入时序数据的周期性，多少个数据点表示一个完整的周期。如果没有周期性，那么设置为 0 即可。|	0|
|start_ts|预测数据的开始时间|	0|
|time_step|预测结果的两个数据点之间时间间隔|0	|
|fc_rows|预测结果数量|	0	|
|return_conf|返回结果中是否包含执行区间范围，如果算法计算结果不包含置信区间，那么上界和下界与自身相同|	1|	
|conf|执行区间分位数	0.05|


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


### 示例代码

```python
import numpy as np
from service import AbstractAnomalyDetectionService

# 算法实现类名称 需要以下划线 "_" 开始，并以 Service 结束，如下 _IqrService 是 IQR 异常检测算法的实现类。
class _IqrService(AbstractAnomalyDetectionService):
    """ IQR algorithm 定义类，从 AbstractAnomalyDetectionService 继承，并实现 AbstractAnomalyDetectionService类的抽象函数  """

	# 定义算法调用关键词，全小写ASCII码(必须添加)
    name = 'iqr'

	# 该算法的描述信息(建议添加)
    desc = """found the anomaly data according to the inter-quartile range"""

    def __init__(self):
        super().__init__()

    def execute(self):
		""" execute 是算法实现逻辑的核心实现，直接修改该实现即可 """

		# self.list 是输入数值列，list 类型，例如：[1,2,3,4,5]。设置 self.list 的方法在父类中已经进行了定义。实现自己的算法，修改该文件即可，以下代码使用自己的实现替换即可。
        #lower = np.quantile(self.list, 0.25)
        #upper = np.quantile(self.list, 0.75)

        #min_val = lower - 1.5 * (upper - lower)
        #max_val = upper + 1.5 * (upper - lower)
        #threshold = [min_val, max_val]

		# 返回值是与输入数值列长度相同的数据列，异常值对应位置是 -1。例如上述输入数据列，返回数值列是 [1, 1, 1, 1, -1],表示 [5] 是异常值。
        return [-1 if k < threshold[0] or k > threshold[1] else 1 for k in self.list]

	
    def set_params(self, params):
		"""该算法无需任何输入参数，直接重载父类该函数，不处理算法参数设置逻辑"""
        pass
```


### 单元测试

在测试文件目录中的 anomaly_test.py 中增加单元测试用例。
```python
def test_iqr(self):
	""" 测试 _IqrService 类 """
    s = loader.get_service("iqr")

    # 设置需要进行检测的输入数据
    s.set_input_list(AnomalyDetectionTest.input_list)

	#  测试 set_params 的处理逻辑
    try:
        s.set_params({"k": 2})
    except ValueError as e:
        self.assertEqual(1, 0)

    r = s.execute()
	
	# 绘制异常检测结果
    draw_ad_results(AnomalyDetectionTest.input_list, r, "iqr")
	
	# 检查结果
    self.assertEqual(r[-1], -1)
    self.assertEqual(len(r), len(AnomalyDetectionTest.input_list))
```

### 需要模型的算法

针对特定数据集，进行模型训练的算法，在训练完成后。需要将训练得到的模型保存在  model 目录中。需要注意的是，针对每个算法，需要建立独立的文件夹。例如 auto_encoder 的训练算法在 model 目录下建立了， autoencoder的目录，使用该算法针对不同数据集训练得到的模型，均需要放置在该目录下。

训练完成后的模型，使用  joblib 进行保存。

并在 model 目录下建立对应的文件夹存放该模型。

保存模型的调用，可参考  encoder.py 的方式，用户通过调用  set_params 方法，并指定参数  {"model": "ad_encoder_keras"} 的方式，可以调用该模型进行计算。

具体的调用方式如下：

```python
def test_autoencoder_ad(self):
    # 获取特定的算法服务
    s = loader.get_service("ac")
    data = self.__load_remote_data_for_ad()
	
    # 设置异常检查的输入数据
    s.set_input_list(data)
    
    # 指定调用的模型，该模型是之前针对该数据集进行训练获得
    s.set_params({"model": "ad_encoder_keras"})
    # 执行检查动作，并返回结果
    r = s.execute()

    num_of_error = -(sum(filter(lambda x: x == -1, r)))
    self.assertEqual(num_of_error, 109)
```

