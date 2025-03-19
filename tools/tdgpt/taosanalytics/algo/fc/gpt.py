# encoding:utf-8
# pylint: disable=c0103
""" auto encoder algorithms to detect anomaly for time series data"""
import json
import requests

from taosanalytics.algo.forecast import insert_ts_list
from taosanalytics.conf import app_logger, conf
from taosanalytics.service import AbstractForecastService


class _GPTService(AbstractForecastService):
    name = 'td_gpt_fc'
    desc = "internal gpt forecast model based on transformer"

    def __init__(self):
        super().__init__()

        self.table_name = None
        self.service_host = 'http://192.168.2.90:5000/ds_predict'
        self.headers = {'Content-Type': 'application/json'}

        self.std = None
        self.threshold = None
        self.time_interval = None
        self.dir = 'internal-gpt'


    def execute(self):
        if self.list is None or len(self.list) < self.period:
            raise ValueError("number of input data is less than the periods")

        if self.fc_rows <= 0:
            raise ValueError("fc rows is not specified yet")

        # let's request the gpt service
        data = {"input": self.list, 'next_len': self.fc_rows}
        try:
            response = requests.post(self.service_host, data=json.dumps(data), headers=self.headers)
        except Exception as e:
            app_logger.log_inst.error(f"failed to connect the service: {self.service_host} ", str(e))
            raise ValueError("error")

        # print(response)

        pred_y = response.json()['output']
        # print(f"pred_y len:{len(pred_y)}")
        # print(f"pred_y:{pred_y}")

        res =  {
            "res": [pred_y]
        }

        insert_ts_list(res["res"], self.start_ts, self.time_step, self.fc_rows)
        return res

        # insert_ts_list(res, self.start_ts, self.time_step, self.fc_rows)
        #
        # if self.return_conf:
        #     res1 = [res.tolist(), res.tolist(), res.tolist()], None
        # else:
        #     res1 = [res.tolist()], None
        #
        # # add the conf range if required
        # return {
        #     "mse": None,
        #     "res": res1
        # }

    def set_params(self, params):
        super().set_params(params)

        if "host" not in params:
            raise ValueError("gpt service host needs to be specified")

        self.service_host = params['host'].trim()

        if self.service_host.startswith("https://"):
            self.service_host = self.service_host.replace("https://", "http://")
        elif "http://" not in self.service_host:
            self.service_host = "http://" + self.service_host

        app_logger.log_inst.info("%s specify gpt host service: %s", self.__class__.__name__,
                                 self.service_host)

