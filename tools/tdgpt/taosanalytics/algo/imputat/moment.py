# encoding:utf-8
# pylint: disable=c0103
""" auto encoder algorithms to detect anomaly for time series data"""
import json
import requests

from taosanalytics.base import AbstractImputationService, AnalyticsService
from taosanalytics.conf import Configure
from taosanalytics.log import AppLogger


class _MomentImputationService(AbstractImputationService):
    """moment imputation service class"""
    name = 'moment'
    desc = "Time-Series Foundation Model by CMU"
    is_builtins = True

    def __init__(self):
        super().__init__()
        self.headers = {'Content-Type': 'application/json'}
        self.service_host = Configure.get_instance().get_tsfm_service(self.name)

        # set the default frequency and time precision
        self.freq = 'H'
        self.precision = 'ms'

        if self.service_host is None:
            self.service_host = 'http://127.0.0.1:6062/imputation'

    def execute(self):
        # let's request the gpt service
        data = {
            "input": self.list,
            "ts": self.ts_list,
            'precision': self.precision,
            'freq':self.freq,
        }

        try:
            response = requests.post(self.service_host, data=json.dumps(data), headers=self.headers)
        except Exception as e:
            AppLogger.error("failed to connect the service: %s %s", self.service_host, str(e))
            raise

        if response.status_code == 404:
            AppLogger.error(f"failed to connect the service: {self.service_host}")
            raise ValueError("invalid host url")
        elif response.status_code != 200:
            AppLogger.error(f"failed to request the service: {self.service_host}, reason: {response.text}")
            raise ValueError(f"failed to request the service, {response.text}")

        resp_json = response.json()
        AppLogger.debug(f"recv rsp, {resp_json}")

        return resp_json

    def set_params(self, params):
        super().set_params(params)

        if "host" in params:
            self.service_host = params['host']

            if self.service_host.startswith("https://"):
                self.service_host = self.service_host.replace("https://", "http://")
            elif "http://" not in self.service_host:
                self.service_host = "http://" + self.service_host

        AppLogger.info("%s specify gpt host service: %s", self.__class__.__name__,
                                 self.service_host)

        if "freq" in params:
            self.freq = params["freq"]

        if "precision" in params:
            self.precision = params["precision"]

        AppLogger.info("%s specify freq: %s, precision: %s", self.__class__.__name__,
                                 self.freq, self.precision)

    def get_status(self) -> str:
        try:
            _ = requests.get(self.service_host, headers=self.headers, timeout=5)
        except Exception as e:
            AppLogger.error("failed to connect the service: %s %s", self.service_host, str(e))
            return AnalyticsService._toStatusName[AnalyticsService.UNAVAILABLE]

        return super().get_status()

