# encoding:utf-8
# pylint: disable=c0103
""" auto encoder algorithms to detect anomaly for time series data"""
from taosanalytics.algo.tsfm import TsfmBaseService

class _TimesFmService(TsfmBaseService):
    name = 'timesfm'
    desc = "Time-Series Foundation Model by Google Inc."

    def __init__(self):
        super().__init__()

        if  self.service_host is None:
            self.service_host = 'http://127.0.0.1:6075/ds_predict'

    def execute(self):
        if len(self.past_dynamic_real):
            raise ValueError("covariate forecast is not supported yet")

        return super().execute()