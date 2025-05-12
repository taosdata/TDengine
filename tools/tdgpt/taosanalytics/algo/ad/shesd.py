# encoding:utf-8
"""shesd algorithm class definition"""

from pandas import Series
from pyculiarity import detect_vec
from taosanalytics.service import AbstractAnomalyDetectionService


class _SHESDService(AbstractAnomalyDetectionService):
    """ s-h-esd algorithm is to check the anomaly data in the input list """
    name = 'shesd'
    desc = ""

    def __init__(self, n_period=0, direction="both", anoms=0.05):
        super().__init__()

        self.period = n_period
        self.direction = direction
        self.max_anoms = anoms

    def execute(self):
        """perform SHESD test and identify (if any) the outlier"""
        if self.input_is_empty():
            return []

        results = detect_vec(Series(self.list), max_anoms=self.max_anoms, direction=self.direction,
                             period=self.period)

        res_val = results['anoms']['anoms']

        return [-1 if k in set(res_val) else 1 for k in self.list]

    def set_params(self, params):
        super().set_params(params)

        if "period" in params:  # todo check value range
            self.period = int(params["period"])
        if "direction" in params:
            self.direction = params["direction"]
        if "max_anoms" in params:
            self.max_anoms = float(params["max_anoms"])

    def get_params(self):
        return {"period": self.period, "direction": self.direction, "max_anoms": self.max_anoms}
