---
title: Add Machine Learning Models to TDgpt
sidebar_label: Add Machine Learning Models to TDgpt
---

Machine and deep learning models generally require a sufficiently large training dataset to achieve good prediction performance. The training process consumes time and computational resources and often requires regular retraining and model updates based on new input data. TDgpt has built-in support for the PyTorch and Keras machine learning libraries, allowing any model developed with Torch or Keras to be seamlessly integrated and executed within the TDgpt framework.

This document describes how to add trained machine or deep learning models to TDgpt.

## Prepare the Model

Save your model to the `/usr/local/taos/taosanode/model/` directory on the anode. If there are multiple files in your model, you can make a subdirectory for them. As an example, the following procedure adds an autoencoder-based anomaly detection model developed in Keras to TDgpt:

The name of the model in TDgpt is `sample_ad_model`.
For the code used to train the model, see [trainind_ad_model.py](https://github.com/taosdata/TDengine/tree/main/tools/tdgpt/taosanalytics/misc/training_ad_model.py).
For the data used to train the model, see the [art_daily_small_noise dataset](https://raw.githubusercontent.com/numenta/NAB/master/data/artificialNoAnomaly/art_daily_small_noise.csv) from NAB.
The trained model consists of two files, which can be downloaded from [our GitHub repository](https://github.com/taosdata/TDengine/blob/main/tools/tdgpt/model/sample-ad-autoencoder/).

```bash
sample-ad-autoencoder.keras: The model file in keras format
sample-ad-autoencoder.info: Additional parameters for the model in joblib format
```

## Save the Model

The `sample-ad-autoencoder` subdirectory is created in `/usr/local/taos/taosanode/model/`, and both model files are saved to the subdirectory. The `model` directory structure is as follows:

```bash
.
└── model
    └── sample-ad-autoencoder
        ├── sample-ad-autoencoder.keras
        └── sample-ad-autoencoder.info
```

## Load the Model into TDgpt

In the `taosanalytics` folder, save Python code that loads and adapts the model for TDgpt. For reference, see [autoencoder.py](https://github.com/taosdata/TDengine/blob/main/tools/tdgpt/taosanalytics/algo/ad/autoencoder.py).
The code for the sample model is installed by default, and the model is displayed in the output of the `SHOW ANODES FULL` statement.

The logic that you need to implement for your models is described as follows:

```python
class _AutoEncoderDetectionService(AbstractAnomalyDetectionService):
    name = 'sample_ad_model'  # the name of your model as displayed by the SHOW statement
    desc = "sample anomaly detection model based on auto encoder"

    def __init__(self):
        super().__init__()

        self.table_name = None
        self.mean = None
        self.std = None
        self.threshold = None
        self.time_interval = None
        self.model = None

        # the directory in which the model files are stored. if you move your model files, you must update this value.
        self.dir = 'sample-ad-autoencoder'  

        self.root_path = conf.get_model_directory()

        self.root_path = self.root_path + f'/{self.dir}/'

        # check whether the specified directory exists
        if not os.path.exists(self.root_path):
            app_logger.log_inst.error(
                "%s ad algorithm failed to locate default module directory:"
                "%s, not active", self.__class__.__name__, self.root_path)
        else:
            app_logger.log_inst.info("%s ad algorithm root path is: %s", self.__class__.__name__,
                                     self.root_path)

    def execute(self):
        """anomaly detection function"""
        if self.input_is_empty():
            return []

        if self.model is None:
            raise FileNotFoundError("not load autoencoder model yet, or load model failed")

        # initialize input data for anomaly detection
        array_2d = np.reshape(self.list, (len(self.list), 1))
        df = pd.DataFrame(array_2d)

        # use z-score to normalize the data
        normalized_list = (df - self.mean.value) / self.std.value
        seq = create_sequences(normalized_list.values, self.time_interval)

        # perform model inference
        pred_list = self.model.predict(seq)

        # calculate MAE loss
        mae_loss = np.mean(np.abs(pred_list - seq), axis=1)
        mae = mae_loss.reshape((-1))

        # set values greater than threshold as anomalies
        anomalies = mae > self.threshold

        # data i is an anomaly if samples [(i - timesteps + 1) to (i)] are anomalies
        ad_indices = []
        for data_idx in range(self.time_interval - 1,
                              len(normalized_list) - self.time_interval + 1):
            if np.all(anomalies[data_idx - self.time_interval + 1: data_idx]):
                ad_indices.append(data_idx)

        # transform results to meet output requirements and support analytics
        return [-1 if i in ad_indices else 1 for i in range(len(self.list))]

    def set_params(self, params):
        """load model in this function, to be called in executor"""
        if "model" not in params:
            raise ValueError("model needs to be specified")

        name = params['model']

        # concatenate full path of model and info files
        module_file_path = f'{self.root_path}/{name}.keras'
        module_info_path = f'{self.root_path}/{name}.info'

        app_logger.log_inst.info("try to load module:%s", module_file_path)

        # use keras api to load keras format file
        if os.path.exists(module_file_path):
            self.model = keras.models.load_model(module_file_path)
        else:
            app_logger.log_inst.error("failed to load autoencoder model file: %s", module_file_path)
            raise FileNotFoundError(f"{module_file_path} not found")

        # load info file
        if os.path.exists(module_info_path):
            info = joblib.load(module_info_path)
        else:
            app_logger.log_inst.error("failed to load autoencoder model file: %s", module_file_path)
            raise FileNotFoundError("%s not found", module_info_path)

        # initialize additional model inference information to an object
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

## Use the Model in SQL

The model has been preloaded into TDgpt and can be seen in the output of the `SHOW ANODES FULL` statement. Before you can use the model, restart the taosanode service, and then run `UPDATE ALL ANODES` to register the model in the mnode.

- Set the `algo` parameter in your queries to `sample_ad_model`  to instruct TDgpt to use the new algorithm.
- Also set the `model` parameter to `sample-ad-autoencoder` to load your pretrained model.

```SQL
--- Detect anomalies in the `foo` table using the `sample_ad_model` algorithm and `sample-ad-autoencoder` model.
SELECT _wstart, count(*) 
FROM foo anomaly_window(val, 'algo=sample_ad_model,model=sample-ad-autoencoder');
```
