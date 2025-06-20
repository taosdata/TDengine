# encoding:utf-8
# pylint: disable=c0103
""" auto encoder algorithms to detect anomaly for time series data"""
import json
import requests

from taosanalytics.algo.forecast import insert_ts_list
from taosanalytics.conf import app_logger, conf
from taosanalytics.service import AbstractForecastService


class TsfmBaseService(AbstractForecastService):
    """tsfm base service class"""

    def __init__(self):
        super().__init__()
        self.headers = {'Content-Type': 'application/json'}
        self.service_host = conf.get_tsfm_service(self.name)


    def execute(self):
        if self.list is None or len(self.list) < self.period:
            raise ValueError("number of input data is less than the periods")

        if self.rows <= 0:
            raise ValueError("forecast rows is not specified yet")

        # let's request the gpt service
        data = {
            "input": self.list,
            "next_len": self.rows,
            "past_dynamic_real": self.past_dynamic_real,
            "dynamic_real":self.dynamic_real,
            "conf_interval": self.conf
        }

        try:
            response = requests.post(self.service_host, data=json.dumps(data), headers=self.headers)
        except Exception as e:
            app_logger.log_inst.error(f"failed to connect the service: {self.service_host} ", str(e))
            raise e

        if response.status_code == 404:
            app_logger.log_inst.error(f"failed to connect the service: {self.service_host} ")
            raise ValueError("invalid host url")
        elif response.status_code != 200:
            app_logger.log_inst.error(f"failed to request the service: {self.service_host}, reason: {response.text}")
            raise ValueError(f"failed to request the service, {response.text}")

        resp_json = response.json()
        app_logger.log_inst.debug(f"recv rsp, {resp_json}")

        if self.return_conf == 0:
            res = {
                "res": [resp_json["output"]],
                "conf": 0,
            }
        else:
            if resp_json.get('upper') is None:
                res =  {
                    "res": [resp_json["output"], resp_json["output"], resp_json["output"]],
                    "conf": self.conf,
                }
            else:  # return data with lower/upper interval value
                res = {
                    "res": [resp_json["output"], resp_json["lower"], resp_json["upper"]],
                    "conf": self.conf,
                }

        insert_ts_list(res["res"], self.start_ts, self.time_step, self.rows)
        return res


    def set_params(self, params):
        super().set_params(params)

        if "host" in params:
            self.service_host = params['host']

            if self.service_host.startswith("https://"):
                self.service_host = self.service_host.replace("https://", "http://")
            elif "http://" not in self.service_host:
                self.service_host = "http://" + self.service_host

        app_logger.log_inst.info("%s specify gpt host service: %s", self.__class__.__name__,
                                 self.service_host)

