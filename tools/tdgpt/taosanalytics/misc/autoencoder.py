# encoding:utf-8
# pylint: disable=c0103
""" auto encoder algorithms to detect anomaly for time series data"""
import os.path
import time
from pathlib import Path

import joblib
import keras
import numpy as np
import pandas as pd

from taosanalytics.analytics_base import AbstractAnomalyDetectionService, AnalyticsService
from taosanalytics.conf import Configure
from taosanalytics.error import failed_load_model_except
from taosanalytics.log import AppLogger
from taosanalytics.model_file_mgt import ModelFileManager
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

        ModelFileManager.get_instance().load_model_file(
            _AutoEncoderDetectionService.name,
            os.path.join(Configure.get_instance().get_model_directory(),
                         'sample-ad-autoencoder/sample-ad-autoencoder'),
            _AutoEncoderDetectionService.do_load_model,
            _AutoEncoderDetectionService.name
        )

    def get_status(self) -> str:
        """return model status """
        info = ModelFileManager.get_instance().get_model(self.name)
        return AnalyticsService._toStatusName[
            AnalyticsService.UNAVAILABLE if info is None else AnalyticsService.READY]

    def execute(self):
        if self.input_is_empty():
            return []

        if self.model is None:
            failed_load_model_except(self.name)

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
        info = ModelFileManager.get_instance().get_model(self.name)
        if info is None:
            failed_load_model_except(self.name)

        self.mean = info["mean"]
        self.std = info["std"]
        self.threshold = info["threshold"]
        self.time_interval = info["timesteps"]
        self.model = info["model"]

        AppLogger.info(
            "load ac module success, mean: %f, std: %f, threshold: %f, time_interval: %d",
            self.mean[0], self.std[0], self.threshold, self.time_interval
        )

    def get_params(self):
        return {}

    @classmethod
    def do_load_model(cls, path):
        model_file_path = f'{path}.keras'
        model_info_path = f'{path}.info'

        AppLogger.info("try to load module:%s", model_file_path)

        if os.path.exists(model_file_path):
            model = keras.saving.load_model(model_file_path)
        else:
            AppLogger.error("failed to load autoencoder model file: %s", model_file_path)
            raise FileNotFoundError(f"{model_file_path} not found")

        if os.path.exists(model_info_path):
            info = joblib.load(model_info_path)
        else:
            AppLogger.error("failed to load autoencoder model file: %s", model_file_path)
            raise FileNotFoundError("%s not found", model_info_path)

        info["model"] = model

        stat_info = Path(model_file_path).stat()
        try:
            create_time = stat_info.st_birthtime
        except AttributeError:
            create_time = stat_info.st_mtime

        create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(create_time))
        return info, create_time
