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

from taosanalytics.conf import app_logger, conf
from taosanalytics.error import failed_load_model_except
from taosanalytics.model import model_manager
from taosanalytics.service import AbstractAnomalyDetectionService, AnalyticsService
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
        # self.dir = 'sample-ad-autoencoder'

    def get_status(self) -> str:
        """return model status """
        info = model_manager.get_model(self.name)
        return AnalyticsService._toStatusName[
            AnalyticsService.UNAVAILABLE if info is None else AnalyticsService.READY]

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
        info = model_manager.get_model(self.name)
        if info is None:
            failed_load_model_except(self.name)

        self.mean = info["mean"]
        self.std = info["std"]
        self.threshold = info["threshold"]
        self.time_interval = info["timesteps"]
        self.model = info["model"]

        app_logger.log_inst.info(
            "load ac module success, mean: %f, std: %f, threshold: %f, time_interval: %d",
            self.mean[0], self.std[0], self.threshold, self.time_interval
        )

    def get_params(self):
        return {}

    @classmethod
    def do_load_model(cls, path):
        model_file_path = f'{path}.keras'
        model_info_path = f'{path}.info'

        app_logger.log_inst.info("try to load module:%s", model_file_path)

        if os.path.exists(model_file_path):
            model = keras.models.load_model(model_file_path)
        else:
            app_logger.log_inst.error("failed to load autoencoder model file: %s", model_file_path)
            raise FileNotFoundError(f"{model_file_path} not found")

        if os.path.exists(model_info_path):
            info = joblib.load(model_info_path)
        else:
            app_logger.log_inst.error("failed to load autoencoder model file: %s", model_file_path)
            raise FileNotFoundError("%s not found", model_info_path)

        info["model"] = model

        stat_info = Path(model_file_path).stat()
        try:
            create_time = stat_info.st_birthtime
        except AttributeError:
            create_time = stat_info.st_mtime

        create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(create_time))
        return info, create_time


model_manager.load_model(_AutoEncoderDetectionService.name,
                         conf.get_model_directory() + 'sample-ad-autoencoder/sample-ad-autoencoder',
                         _AutoEncoderDetectionService.do_load_model,
                         _AutoEncoderDetectionService.name)
