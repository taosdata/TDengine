# encoding:utf-8
"""iqr class definition"""
import numpy as np
from taosanalytics.service import AbstractAnomalyDetectionService


class _IqrService(AbstractAnomalyDetectionService):
    """ IQR algorithm is to check the anomaly data in the input list """
    name = 'iqr'
    desc = """found the anomaly data according to the inter-quartile range"""

    def __init__(self):
        super().__init__()

    def execute(self):
        if self.input_is_empty():
            return []

        lower = np.quantile(self.list, 0.25)
        upper = np.quantile(self.list, 0.75)

        min_val = lower - 1.5 * (upper - lower)
        max_val = upper + 1.5 * (upper - lower)

        threshold = [min_val, max_val]
        return [-1 if k < threshold[0] or k > threshold[1] else 1 for k in self.list]

    def set_params(self, params):
        pass
