# encoding:utf-8
# pylint: disable=c0103
""" auto encoder algorithms to detect anomaly for time series data"""
from taosanalytics.algo.tsfm import TsfmBaseService

class _GPTService(TsfmBaseService):
    name = 'tdtsfm_1'
    desc = "Time-Series Foundation Model based on transformer by TAOS DATA"

    def __init__(self):
        super().__init__()

        if  self.service_host is None:
            self.service_host = 'http://127.0.0.1:5000/tdtsfm'


    def execute(self):
        if len(self.past_dynamic_real):
            raise ValueError("covariate forecast is not supported yet")

        return super().execute()