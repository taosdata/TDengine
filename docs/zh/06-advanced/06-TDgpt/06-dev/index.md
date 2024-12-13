---
title: "算法开发者指南"
sidebar_label: "算法开发者指南"
---
TDgpt 是一个可扩展的时序数据高级分析平台，用户遵循简易的步骤就能将自己开发的分析算法添加到分析平台, 各种应用就可以通过SQL语句直接调用, 让高级分析算法的使用门槛降到几乎为零。目前 TDpgt 平台只支持使用 Python 语言开发的分析算法。
Anode 采用类动态加载模式，在启动的时候扫描特定目录内满足约定条件的所有代码文件，并将其加载到系统中。因此，开发者只需要遵循以下几步就能完成新算法的添加工作：
1. 开发完成符合要求的分析算法类
2. 将代码文件放入对应目录，然后重启 Anode
3. 使用SQL命令"CREATE ANODE"，将 Anode 添加到 TDengine
   
此时就完成了新算法的添加工作，之后应用就可以直接使用SQL语句调用新算法。得益于 TDgpt 与 TDengine主进程 `taosd` 的松散耦合，Anode算法升级对 `taosd` 完全没有影响。应用系统只需要调整对应的SQL语句调用新（升级的）算法，就能够快速完成分析功能和分析算法的升级。

这种方式能够按需扩展分析算法，极大地拓展 TDgpt 的适应范围，用户可以按需将更契合业务场景的、更准确的（预测、异常检测）分析算法动态嵌入到 TDgpt，并通过 SQL 语句进行调用。在基本不用更改应用系统代码的前提下，就能够快速完成分析功能的平滑升级。

以下内容将说明如何将分析算法添加到 Anode 中并能够通过SQL语句进行调用。

## 目录结构
Anode的主要目录结构如下图所示

```bash
.
├── bin
├── cfg
├── lib
│   └── taosanalytics
│       ├── algo
│       │   ├── ad
│       │   └── fc
│       ├── misc
│       └── test
├── log -> /var/log/taos/taosanode
├── model -> /var/lib/taos/taosanode/model
└── venv -> /var/lib/taos/taosanode/venv

```

|目录|说明|
|---|---|
|taosanalytics| 源代码目录，其下包含了算法具体保存目录 algo，放置杂项目录 misc，单元测试和集成测试目录 test。 algo 目录下 ad 保存异常检测算法代码，fc 目录保存预测算法代码|
|venv| Python 虚拟环境|
|model|放置针对数据集完成的训练模型|
|cfg|配置文件目录|

## 约定与限制

- 异常检测算法的 Python 代码文件需放在 `./taos/algo/ad` 目录中
- 预测算法 Python 代码文件需要放在 `./taos/algo/fc` 目录中


### 类命名规范

Anode采用算法自动加载模式，因此只识别符合命名约定的 Python 类。需要加载的算法类名称需要以下划线 `_` 开始并以 `Service` 结尾。例如：`_KsigmaService` 是  KSigma 异常检测算法类。

### 类继承约定

- 异常检测算法需要从 `AbstractAnomalyDetectionService` 继承，并实现其核心抽象方法 `execute`
- 预测算法需要从 `AbstractForecastService` 继承，同样需要实现其核心抽象方法 `execute`

### 类属性初始化
实现的类需要初始化以下两个类属性：

- `name`：识别该算法的关键词，全小写英文字母。通过 `SHOW` 命令查看可用算法显示的名称即为该名称。
- `desc`：算法的基础描述信息

```SQL
--- algo 后面的参数 name 即为类属性 `name`
SELECT COUNT(*)
FROM foo ANOMALY_WINDOW(col_name, 'algo=name')
```

## 添加具有模型的分析算法

基于统计学的分析算法可以直接针对输入时间序列数据进行分析，但是某些深度学习算法对于输入数据需要较长的时间训练，并且生成相应的模型。这种情况下，同一个分析算法对应不同的输入数据集有不同的分析模型。
将具有模型的分析算法添加到 Anode 中，首先需要在 `model` 目录中建立该算法对应的目录（目录名称可自拟），将采用该算法针对不同的输入时间序列数据生成的训练模型均需要保存在该目录下，同时目录名称要在分析算法中确定，以便能够固定加载该目录下的分析模型。为了确保模型能够正常读取加载，存储的模型使用`joblib`库进行序列化保存。

下面以自编码器（Autoencoder）为例，说明如何添加要预先训练的模型进行异常检测。
首先我们在 `model `目录中创建一个目录 -- `ad_autoencoder` (见上图目录结构)，该目录将用来保存所有使用自编码器训练的模型。然后，我们使用自编码器对 foo 表的时间序列数据进行训练，得到模型 针对 foo 表的模型，我们将其命名为 `ad_autoencoder_foo`，使用 `joblib`序列化该模型以后保存在 `ad_autoencoder` 目录中。如下图所示，ad_autoencoder_foo 由两个文件构成，分别是模型文件 (ad_autoencoder_foo.dat) 和模型文件描述文件 (ad_autoencoder_foo.info)。

```bash
.
└── model
    └── ad_autoencoder
        ├── ad_autoencoder_foo.dat
        └── ad_autoencoder_foo.info

```

接下来说明如何使用 SQL 调用该模型。
通过设置参数 `algo=ad_encoder` 告诉分析平台要调用自编码器算法训练的模型（自编码器算法在可用算法列表中），因此直接指定即可。此外还需要指定自编码器针对某数据集训练的确定的模型，此时我们需要使用已经保存的模型 `ad_autoencoder_foo` ，因此需要添加参数 `model=ad_autoencoder_foo` 以便能够调用该模型。

```SQL
--- 在 options 中增加 model 的名称，ad_autoencoder_foo， 针对 foo 数据集（表）训练的采用自编码器的异常检测模型进行异常检测
SELECT COUNT(*), _WSTART
FROM foo
ANOMALY_WINDOW(col1, 'algo=ad_encoder, model=ad_autoencoder_foo');
```
