# encoding:utf-8
# pylint: disable=c0103
""" auto encoder algorithms to detect anomaly for time series data"""
import os.path

import joblib
import keras
import numpy as np
import pandas as pd

from taosanalytics.conf import app_logger, conf
from taosanalytics.service import AbstractAnomalyDetectionService
from taosanalytics.util import create_sequences


class _AutoEncoderDetectionService(AbstractAnomalyDetectionService):
    name = 'sample_ad_model'
    desc = "sample anomaly detection model based on auto encoder"

    def __init__(self):
        super().__init__()

        self.table_name = None
        self.mean = None
        self.std = None
        self.threshold = None
        self.time_interval = None
        self.model = None
        self.dir = 'sample-ad-autoencoder'

        self.root_path = conf.get_model_directory()

        self.root_path = self.root_path + f'/{self.dir}/'

        if not os.path.exists(self.root_path):
            app_logger.log_inst.error(
                "%s ad algorithm failed to locate default module directory:"
                "%s, not active", self.__class__.__name__, self.root_path)
        else:
            app_logger.log_inst.info("%s ad algorithm root path is: %s", self.__class__.__name__,
                                     self.root_path)

    def execute(self):
        if self.input_is_empty():
            return []

        if self.model is None:
            raise FileNotFoundError("not load autoencoder model yet, or load model failed")

        array_2d = np.reshape(self.list, (len(self.list), 1))
        df = pd.DataFrame(array_2d)

        # normalize input data using z-score
        normalized_list = (df - self.mean.value) / self.std.value
        if len(normalized_list) < self.time_interval:
            raise ValueError("input data is too short")

        seq = create_sequences(normalized_list.values, self.time_interval)

        # Get test MAE loss.
        pred_list = self.model.predict(seq)
        mae_loss = np.mean(np.abs(pred_list - seq), axis=1)
        mae = mae_loss.reshape((-1))

        # Detect all the samples which are anomalies.
        anomalies = mae > self.threshold

        # data i is an anomaly if samples [(i - timesteps + 1) to (i)] are anomalies
        ad_indices = []
        for data_idx in range(self.time_interval - 1,
                              len(normalized_list) - self.time_interval + 1):
            if np.all(anomalies[data_idx - self.time_interval + 1: data_idx]):
                ad_indices.append(data_idx)

        return [-1 if i in ad_indices else 1 for i in range(len(self.list))]

    def set_params(self, params):

        if "model" not in params:
            raise ValueError("model needs to be specified")

        name = params['model']

        module_file_path = f'{self.root_path}/{name}.keras'
        module_info_path = f'{self.root_path}/{name}.info'

        app_logger.log_inst.info("try to load module:%s", module_file_path)

        if os.path.exists(module_file_path):
            self.model = keras.models.load_model(module_file_path)
        else:
            app_logger.log_inst.error("failed to load autoencoder model file: %s", module_file_path)
            raise FileNotFoundError(f"{module_file_path} not found")

        if os.path.exists(module_info_path):
            info = joblib.load(module_info_path)
        else:
            app_logger.log_inst.error("failed to load autoencoder model file: %s", module_file_path)
            raise FileNotFoundError("%s not found", module_info_path)

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

    def get_params(self):
        return {"dir": self.dir + '/*'}
