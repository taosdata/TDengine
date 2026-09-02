---
title: 机器学习算法
sidebar_label: 机器学习算法
---

Autoencoder<sup>[1]</sup>：TDgpt 提供了一个使用自编码器（Autoencoder）构建的深度学习异常检测示例。该模型基于 NAB 的 [art_daily_small_noise 数据集](https://raw.githubusercontent.com/numenta/NAB/master/data/artificialNoAnomaly/art_daily_small_noise.csv) 进行训练，详细步骤见 [添加机器学习模型](../../01-tdgpt/06-dev/02-ml/index.md)。

该示例模型默认既不在 `model` 目录中，也不会自动出现在算法列表里。要正常运行，需要：

1. 从 [sample-ad-autoencoder](https://github.com/taosdata/TDengine/tree/main/tools/tdgpt/model/sample-ad-autoencoder/) 下载模型文件，并放到 `/var/lib/taos/taosanode/model/sample-ad-autoencoder/`；
2. 将示例适配代码 [misc/autoencoder.py](https://github.com/taosdata/TDengine/blob/main/tools/tdgpt/taosanalytics/misc/autoencoder.py) 复制到 `algo/ad/`；
3. 重启 taosanode，并执行 `UPDATE ALL ANODES`。

相关操作原理及方式请参考 [添加机器学习模型](../../01-tdgpt/06-dev/02-ml/index.md)。

此时 `model` 文件夹结构如下：

```bash
.
└── model
    └── sample-ad-autoencoder
        ├── sample-ad-autoencoder.keras
        └── sample-ad-autoencoder.info
```

```sql
-- 在 options 中增加 model 参数 sample-ad-autoencoder，采用自编码器的异常检测模型进行异常检测
SELECT _wstart, count(*)
FROM foo ANOMALY_WINDOW(val, 'algo=sample_ad_model,model=sample-ad-autoencoder');
```

其中的 `algo` 设置为 `sample_ad_model` 为示例异常检测模型，`model` 指定加载模型文件的信息。需要注意的是，该模型只针对训练的数据集具有较好的检测效果，针对非训练相关数据集，可能无法得出合适的结果。

- IForest：Isolation Forest，基于隔离树的无监督集成学习异常检测算法。该算法通过随机选择特征和切分值构建多棵隔离树，并根据数据点被隔离所需的路径长度计算离群程度。数据点越容易被隔离，路径越短，越可能是异常点。IForest 不需要预训练模型文件，TDgpt 会在每次调用时基于输入数据拟合检测器。参数 `contamination` 用于指定预期异常点比例，默认值为 `0.1`；参数 `n_estimators` 用于指定隔离树数量，默认值为 `100`；参数 `random_state` 用于指定随机种子，默认值为 `42`。

| 参数 | 说明 | 是否必选 | 默认值 |
| --- | --- | --- | --- |
| contamination | 预期异常点比例，取值范围为 `(0, 0.5]` | 否 | `0.1` |
| n_estimators | 隔离树数量，必须大于 `0` | 否 | `100` |
| random_state | 随机种子 | 否 | `42` |

```SQL
--- 使用 IForest 进行多变量异常检测
SELECT _WSTART, COUNT(*)
FROM foo
ANOMALY_WINDOW(foo.i32, foo.i64, "algo=iforest,contamination=0.1,n_estimators=100,random_state=42")
```

后续添加机器（深度）学习异常检测算法

- One-Class Support Vector Machines (SVM)

### 参考文献

1. [Autoencoder - Wikipedia](https://en.wikipedia.org/wiki/Autoencoder)
