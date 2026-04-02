# encoding:utf-8
"""local outlier factor class definition"""

import numpy as np
import sklearn.neighbors as neighbor

from taosanalytics.conf import app_logger
from taosanalytics.service import AbstractAnomalyDetectionService


class _LofService(AbstractAnomalyDetectionService):
    """ LOF(local outlier factor) algorithm is to check the anomaly data in the input list """
    name = 'lof'
    desc = """Local Outlier Factor, Ref: M. M. Breunig, H. P. Kriegel, R. T. Ng, J. Sander.
    LOF:Identifying Density-based Local Outliers. SIGMOD, 2000."""

    def __init__(self, n_neighbors=10, algo="auto"):
        super().__init__()

        self.neighbors = n_neighbors
        self.algorithm = algo

    def execute(self):
        """perform LOF(local outlier factor) test and identify (if any) the outlier"""
        if self.input_is_empty():
            return []

        checker = neighbor.LocalOutlierFactor(n_neighbors=self.neighbors, algorithm=self.algorithm)

        arr_2d = np.reshape(self.list, (len(self.list), 1))
        res = checker.fit_predict(arr_2d)

        app_logger.log_inst.debug(f"The negative outlier factor is:{checker.negative_outlier_factor_}")
        return res.tolist()

    def set_params(self, params):
        super().set_params(params)

        if "neighbors" in params:  # todo check value range
            self.neighbors = int(params["neighbors"])
        if "algorithm" in params:
            self.algorithm = params["algorithm"]

    def get_params(self):
        return {"neighbors": self.neighbors, "algorithm": self.algorithm}
