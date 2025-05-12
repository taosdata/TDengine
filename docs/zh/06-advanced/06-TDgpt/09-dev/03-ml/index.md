---
title: "添加机器学习模型"
sidebar_label: "添加机器学习模型"
---

机器/深度学习模型一般要求训练集样本量足够大，才能取得不错的预测效果。训练过程要消耗一定量的时间和计算资源，并需要根据输入的数据进行定期的训练以及更新模型。TDgpt 内置了 PyTorch 和 Keras 机器学习库。所有使用 PyTorch 或 Keras 开发的模型均可以驱动运行。

本章介绍将预训练完成的机器/深度学习分析模型添加到 TDgpt 中的方法。

## 准备模型

推荐将模型保存在默认的保存目录 `/usr/local/taos/taosanode/model/` 中，也可以在目录中建立下一级目录，用以保存模型。下面使用 Keras 开发的基于自编码器 `auto encoder` 的异常检测模型添加到 TDgpt 为例讲解整个流程。

该模型在 TDgpt 系统中名称为 'sample_ad_model'。
训练该模型的代码见：[training_ad_model.py](https://github.com/taosdata/TDengine/tree/main/tools/tdgpt/taosanalytics/misc/training_ad_model.py)。
该模型训练使用了 NAB 的 [art_daily_small_noise 数据集](https://raw.githubusercontent.com/numenta/NAB/master/data/artificialNoAnomaly/art_daily_small_noise.csv)。
训练完成得到的模型保存成为了两个文件，点击 [此处](https://github.com/taosdata/TDengine/blob/main/tools/tdgpt/model/sample-ad-autoencoder/) 下载该模型文件，模型文件说明如下。

```bash
sample-ad-autoencoder.keras  模型文件，默认 keras 模型文件格式
sample-ad-autoencoder.info   模型附加参数文件，采用了 joblib 格式保存
```

## 保存在合适位置

然后在 `/usr/local/taos/taosanode/model` 文件目录下建立子目录 `sample-ad-autoencoder`，用以保存下载两个模型文件。此时 `model` 文件夹结构如下：

```bash
.
└── model
    └── sample-ad-autoencoder
        ├── sample-ad-autoencoder.keras
        └── sample-ad-autoencoder.info
```

## 添加模型适配代码

需要在 taosanalytics 目录下添加加载该模型并进行适配的 Python 代码。适配并行运行的代码见 [autoencoder.py](https://github.com/taosdata/TDengine/blob/main/tools/tdgpt/taosanalytics/algo/ad/autoencoder.py)。
为了方便使用，我们已经将该文件保存在该目录，所以您在执行 `show anodes full` 命令时候，能够看见该算法模型。

下面详细说明该代码的逻辑。

```python
class _AutoEncoderDetectionService(AbstractAnomalyDetectionService):
    name = 'sample_ad_model'  # 通过 show 命令看到的算法的名称，在此处定义
    desc = "sample anomaly detection model based on auto encoder"

    def __init__(self):
        super().__init__()

        self.table_name = None
        self.mean = None
        self.std = None
        self.threshold = None
        self.time_interval = None
        self.model = None

        # 模型文件保存的文件夹名称，如果您更改了文件夹名称，在此处需要同步修改，以确保代码可以正确加载模型文件
        self.dir = 'sample-ad-autoencoder'  

        self.root_path = conf.get_model_directory()

        self.root_path = self.root_path + f'/{self.dir}/'

        # 检查代码中指定的模型文件路径是否存在
        if not os.path.exists(self.root_path):
            app_logger.log_inst.error(
                "%s ad algorithm failed to locate default module directory:"
                "%s, not active", self.__class__.__name__, self.root_path)
        else:
            app_logger.log_inst.info("%s ad algorithm root path is: %s", self.__class__.__name__,
                                     self.root_path)

    def execute(self):
        """异常检测主体执行函数"""
        if self.input_is_empty():
            return []

        if self.model is None:
            raise FileNotFoundError("not load autoencoder model yet, or load model failed")

        # 初始化输入进行异常检测的数据
        array_2d = np.reshape(self.list, (len(self.list), 1))
        df = pd.DataFrame(array_2d)

        # 使用 z-score 进行归一化处理
        normalized_list = (df - self.mean.value) / self.std.value
        seq = create_sequences(normalized_list.values, self.time_interval)

        # 进行模型推理
        pred_list = self.model.predict(seq)

        # 计算 MAE 损失值
        mae_loss = np.mean(np.abs(pred_list - seq), axis=1)
        mae = mae_loss.reshape((-1))

        # 大于阈值的设置为异常点
        anomalies = mae > self.threshold

        # data i is an anomaly if samples [(i - timesteps + 1) to (i)] are anomalies
        ad_indices = []
        for data_idx in range(self.time_interval - 1,
                              len(normalized_list) - self.time_interval + 1):
            if np.all(anomalies[data_idx - self.time_interval + 1: data_idx]):
                ad_indices.append(data_idx)

        # 变换结果，符合输出的约定，支持分析完成
        return [-1 if i in ad_indices else 1 for i in range(len(self.list))]

    def set_params(self, params):
        """在此函数中进行模型的加载操作，然后在 executor 中调用"""
        if "model" not in params:
            raise ValueError("model needs to be specified")

        name = params['model']

        # 拼装模型文件的全路径，此处有两个文件，一个模型主体文件，一个信息文件
        module_file_path = f'{self.root_path}/{name}.keras'
        module_info_path = f'{self.root_path}/{name}.info'

        app_logger.log_inst.info("try to load module:%s", module_file_path)

        # 因为保存成为 keras 格式，所以调用 keras API 加载模型文件
        if os.path.exists(module_file_path):
            self.model = keras.models.load_model(module_file_path)
        else:
            app_logger.log_inst.error("failed to load autoencoder model file: %s", module_file_path)
            raise FileNotFoundError(f"{module_file_path} not found")

        # 加载辅助信息文件
        if os.path.exists(module_info_path):
            info = joblib.load(module_info_path)
        else:
            app_logger.log_inst.error("failed to load autoencoder model file: %s", module_file_path)
            raise FileNotFoundError("%s not found", module_info_path)

        # 初始化模型推理的辅助信息到对象中
        if info is not None:
            self.mean = info["mean"]
            self.std = info["std"]
            self.threshold = info["threshold"]
            self.time_interval = info["timesteps"]

            app_logger.log_inst.info(
                "load ac module success, mean: %f, std: %f, threshold: %f, time_interval: %d",
                self.mean[0], self.std[0], self.threshold, self.time_interval
            )
        else:
            app_logger.log_inst.error("failed to load %s model", name)
            raise RuntimeError(f"failed to load model {name}")
```

## 使用 SQL 调用模型

该模型已经预置在系统中，您可通过 `show anodes full` 直接查看。一个新的算法适配完成以后，需要重新启动 taosanode，并执行命令 `update all anodes` 更新 mnode 的算法列表。

- 通过设置参数 `algo=sample_ad_model`，告诉 TDgpt 调用自编码器算法训练的模型（该算法模型在可用算法列表中）。
- 通过设置参数 `model=sample-ad-autoencoder`，告诉自编码器加载特定的模型。

```SQL
--- 在 options 中增加 model 参数 sample-ad-autoencoder， 针对 foo 数据集（表）训练的采用自编码器的异常检测模型进行异常检测
SELECT _wstart, count(*) 
FROM foo anomaly_window(val, 'algo=sample_ad_model,model=sample-ad-autoencoder');
```
