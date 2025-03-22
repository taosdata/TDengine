# encoding:utf-8
# pylint: disable=c0103
""" auto encoder algorithms to detect anomaly for time series data"""
import os.path

import keras

from taosanalytics.algo.forecast import insert_ts_list
from taosanalytics.conf import app_logger, conf
from taosanalytics.service import AbstractForecastService


class _LSTMService(AbstractForecastService):
    name = 'sample_forecast_model'
    desc = "sample forecast model based on LSTM"

    def __init__(self):
        super().__init__()

        self.table_name = None
        self.mean = None
        self.std = None
        self.threshold = None
        self.time_interval = None
        self.model = None
        self.dir = 'sample-fc-lstm'

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

        res = self.model.predict(self.list)

        insert_ts_list(res, self.start_ts, self.time_step, self.fc_rows)

        if self.return_conf:
            res1 = [res.tolist(), res.tolist(), res.tolist()], None
        else:
            res1 = [res.tolist()], None

        # add the conf range if required
        return {
            "mse": None,
            "res": res1
        }

    def set_params(self, params):

        if "model" not in params:
            raise ValueError("model needs to be specified")

        name = params['model']

        module_file_path = f'{self.root_path}/{name}.keras'
        # module_info_path = f'{self.root_path}/{name}.info'

        app_logger.log_inst.info("try to load module:%s", module_file_path)

        if os.path.exists(module_file_path):
            self.model = keras.models.load_model(module_file_path)
        else:
            app_logger.log_inst.error("failed to load LSTM model file: %s", module_file_path)
            raise FileNotFoundError(f"{module_file_path} not found")

    def get_params(self):
        return {"dir": self.dir + '/*'}
