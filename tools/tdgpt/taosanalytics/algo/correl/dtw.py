import numpy as np

from taosanalytics.service import AbstractCorrelationService
from fastdtw import fastdtw


class _DtwService(AbstractCorrelationService):
    """ dtw analysis """
    name = 'dtw'
    desc = """found the dynamic time wrap for two given time series data"""

    def __init__(self):
        super().__init__()
        self.radius = 1

    def execute(self):
        if self.list is None or self.list1 is None or len(self.list) == 0 or len(self.list1) == 0:
            raise ValueError("empty time-series data to perform dtw")

        dist, path = fastdtw(np.array(self.list), np.array(self.list1), radius=self.radius)
        return dist, path

    def set_params(self, params):
        super().set_params(params)

        if "radius" in params:
            k = int(params["radius"])

            if k < 1 or k > 10:
                raise ValueError("radius value out of range, valid range [1, 10]")

            self.radius = k
