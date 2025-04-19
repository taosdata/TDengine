# encoding:utf-8
# pylint: disable=c0103
""" auto encoder algorithms to detect anomaly for time series data"""
from taosanalytics.algo.tsfm import TsfmBaseService

class _ChronosService(TsfmBaseService):
    name = 'chronos'
    desc = "Time-Series Foundation Model by Amazon Inc."

    def __init__(self):
        super().__init__()

        if  self.service_host is None:
            self.service_host = 'http://127.0.0.1:5002/ds_predict'