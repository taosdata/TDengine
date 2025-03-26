# encoding:utf-8
"""ksigma class definition"""

import numpy as np
from taosanalytics.service import AbstractAnomalyDetectionService


class _KSigmaService(AbstractAnomalyDetectionService):
    """ KSigma algorithm is to check the anomaly data in the input list """
    name = "ksigma"
    desc = """the k-sigma algorithm (or 3σ rule) expresses a conventional heuristic that nearly all
    values are taken to lie within k (usually three) standard deviations of the mean, and thus
    it is empirically useful to treat 99.7% probability as near certainty"""

    def __init__(self, k_val=3):
        super().__init__()
        self.k_val = k_val

    def execute(self):
        def get_k_sigma_range(vals, k_value):
            """ Return the k-sigma value range """
            avg = np.mean(vals)
            std = np.std(vals)

            upper = avg + k_value * std
            lower = avg - k_value * std
            return [float(lower), float(upper)]

        if self.input_is_empty():
            return []

        threshold = get_k_sigma_range(self.list, self.k_val)
        return [-1 if k < threshold[0] or k > threshold[1] else 1 for k in self.list]

    def set_params(self, params):
        super().set_params(params)

        if "k" in params:
            k = int(params["k"])

            if k < 1 or k > 3:
                raise ValueError("k value out of range, valid range [1, 3]")

            self.k_val = k

    def get_params(self):
        return {"k": self.k_val}
