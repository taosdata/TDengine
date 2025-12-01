---
title: "机器学习算法"
sidebar_label: "机器学习算法"
---

Autoencoder<sup>[1]</sup>: TDgpt 内置了一个使用自编码器（Autoencoder）构建的深度学习网络的异常检测模型。该异常检测模型基于 NAB 的 [art_daily_small_noise 数据集](https://raw.githubusercontent.com/numenta/NAB/master/data/artificialNoAnomaly/art_daily_small_noise.csv) 进行训练，该模型的详细信息请参见 添加机器学习模型 - [准备模型](../../dev/ml/#%E5%87%86%E5%A4%87%E6%A8%A1%E5%9E%8B) 部分。

我们并没有将该模型预置与 `model` 目录中。需要该模型正常运行需要下载模型文件，请点击此处[下载](https://github.com/taosdata/TDengine/blob/main/tools/tdgpt/model/sample-ad-autoencoder/)，并在 `/var/lib/taos/taosanode/model/` 目录中创建子目录 `sample-ad-autoencoder`，保存下载两个模型文件，然后需要重启 taosanode 服务。相关操作原理及方式请参考[添加机器学习模型](../../dev/ml) 的介绍。

此时 `model` 文件夹结构如下：

```bash
.
└── model
    └── sample-ad-autoencoder
        ├── sample-ad-autoencoder.keras
        └── sample-ad-autoencoder.info
```

```SQL
--- 在 options 中增加 model 参数 sample-ad-autoencoder， 采用自编码器的异常检测模型进行异常检测
SELECT _wstart, count(*) 
FROM foo anomaly_window(val, 'algo=sample_ad_model,model=sample-ad-autoencoder');
```

其中的 `algo` 设置为 `sample_ad_model` 为示例异常检测模型，`model` 指定加载模型文件的信息。需要注意的是，该模型只针对训练的数据集具有较好的检测效果，针对非训练相关数据集，可能无法得出合适的结果。

后续添加机器（深度）学习异常检测算法

- Isolation Forest
- One-Class Support Vector Machines (SVM)

### 参考文献

1. [Autoencoder - Wikipedia](https://en.wikipedia.org/wiki/Autoencoder)
