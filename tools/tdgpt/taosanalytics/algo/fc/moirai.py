# encoding:utf-8
# pylint: disable=c0103
""" auto encoder algorithms to detect anomaly for time series data"""
from taosanalytics.algo.tsfm import TsfmBaseService

class _MoiraiService(TsfmBaseService):
    name = 'moirai'
    desc = "Time-Series Foundation Model by SalesForce Inc."
