---
title: "添加机器学习模型"
sidebar_label: "添加机器学习模型"
---

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
