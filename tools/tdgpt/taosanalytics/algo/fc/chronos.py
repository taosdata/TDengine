# encoding:utf-8
# pylint: disable=c0103
""" auto encoder algorithms to detect anomaly for time series data"""
from taosanalytics.algo.tsfm import TsfmBaseService

class _ChronosService(TsfmBaseService):
    name = 'chronos'
    desc = "Time-Series Foundation Model by Amazon Inc."
