import numpy as np
from taosanalytics.service import AbstractCorrelationService


class _TlccService(AbstractCorrelationService):
    """ Moving Time-Lagged Cross-Correlation (TLCC) analysis """
    name = 'tlcc'
    desc = """calculate the TLCC value for two time series data"""

    def __init__(self):
        super().__init__()
        self.lag_start = -1
        self.lag_end = 1

    def execute(self):
        if self.list is None or self.list1 is None or len(self.list) == 0 or len(self.list1) == 0:
            raise ValueError("empty time-series data to perform TLCC")

        if self.lag_start > self.lag_end:
            raise ValueError("lag_start should be less than or equal to lag_end value")

        dist, path = self.do_tlcc()
        return dist, path

    def set_params(self, params):
        super().set_params(params)

        if "lag_start" in params:
            lag_start = int(params["lag_start"])

            if abs(lag_start) >= len(self.list):
                raise ValueError("lag_start value out of range, should be less than list length")
            self.lag_start = lag_start

        if "lag_end" in params:
            lag_end = int(params["lag_end"])

            if abs(lag_end) >= len(self.list):
                raise ValueError("lag_end value out of range, should be less than list length")
            self.lag_end = lag_end


    def do_tlcc(self):
        ccf_vals = []
        if len(self.list) != len(self.list1) or len(self.list) == 0 or len(self.list1) == 0:
            raise ValueError("input lists are mismatch, tlcc values not return")

        n = len(self.list)
        for lag in np.arange(self.lag_start, self.lag_end + 1):
            x1 = self.list[max(0, -lag):n if lag < 0 else n - lag]
            y1 = self.list1[max(0, lag):n if lag > 0 else n + lag]

            ccf_vals.append(np.corrcoef(x1, y1)[0, 1])

        return [i for i in range(self.lag_start, self.lag_end + 1)], ccf_vals
