---
title: "算法开发者指南"
sidebar_label: "算法开发者指南"
---
TDgpt 是一个可扩展的时序数据高级分析平台，用户仅按照简易的步骤就能将新分析算法添加到分析平台中。将开发完成的算法代码文件放入对应的目录文件夹，然后重启 Anode 即可完成扩展升级。Anode 启动后会自动加载特定目录的分析算法。用户可以直接使用 SQL 语句调用添加到 TDgpt 系统中的分析算法。得益于 TDgpt 与 taosd 的松散耦合关系，分析平台升级对 taosd 完全没有影响。应用系统也不需要做任何更改就能够完成分析功能和分析算法的升级。

这种方式能够按需扩展新分析算法，极大地拓展了 TDgpt 适应的范围，用户可以将契合业务场景开发的（预测、异常检测）分析算法嵌入到 TDgpt，并通过 SQL 语句进行调用。在不更改或更改非常少的应用系统代码的前提下，就能够快速完成分析功能的平滑升级。

本节说明如何将预测算法和异常检测算法添加到 TDengine 分析平台。

## 目录结构
首先需要了解TDgpt的目录结构。其主体目录结构如下图：

```bash
.
├── cfg
├── model
│   └── ac_detection
├── release
├── script
└── taosanalytics
    ├── algo
    │   ├── ad
    │   └── fc
    ├── misc
    └── test

```

|目录|说明|
|---|---|
|taos|Python 源代码目录，其下包含了算法具体保存目录 algo，放置杂项目录 misc，单元测试和集成测试目录 test。 algo 目录下 ad 保存异常检测算法代码，fc 目录保存预测算法代码|
|script|是安装脚本和发布脚本放置目录|
|model|放置针对数据集完成的训练模型|
|cfg|配置文件目录|

## 约定与限制

- 异常检测算法的 Python 代码文件需放在 `./taos/algo/ad` 目录中
- 预测算法 Python 代码文件需要放在 `./taos/algo/fc` 目录中


### 类命名规范

由于算法采用自动加载，因此其只识别按照特定命名方式的类。算法类的名称需要以下划线开始，以 Service 结尾。例如：_KsigmaService 是  KSigma 异常检测算法类。

### 类继承约定

- 异常检测算法需要从 `AbstractAnomalyDetectionService` 继承，并实现其核心抽象方法 `execute`
- 预测算法需要从 `AbstractForecastService` 继承，同样需要实现其核心抽象方法 `execute`

### 类属性初始化
每个算法实现的类需要静态初始化两个类属性，分别是：

- `name`：触发调用的关键词，全小写英文字母。该名称也是通过 `SHOW` 命令查看可用分析算法是显示的名称。
- `desc`：算法的描述信息

```SQL
--- algo 后面的参数 algo_name 即为类名称 `name`
SELECT COUNT(*) FROM foo ANOMALY_DETECTION(col_name, 'algo=algo_name')
```
  
## 需要模型的算法

针对特定数据集，进行模型训练的算法，在训练完成后。需要将训练得到的模型保存在 model 目录中。需要注意的是，针对每个算法，需要建立独立的文件夹。例如 auto_encoder 的训练算法在 model 目录下建立 autoencoder 的目录，使用该算法针对不同数据集训练得到的模型，均需要放置在该目录下。

训练完成后的模型，使用 joblib 进行保存。

并在 model 目录下建立对应的文件夹存放该模型。

保存模型的调用，可参考  encoder.py 的方式，用户通过调用  set_params 方法，并指定参数 `{"model": "ad_encoder_keras"}` 的方式，可以调用该模型进行计算。

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

