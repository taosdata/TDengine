# encoding:utf-8
""" grubbs algorithm class"""

from outliers import smirnov_grubbs as grubbs
from taosanalytics.service import AbstractAnomalyDetectionService


class _GrubbsService(AbstractAnomalyDetectionService):
    """ Grubbs algorithm is to check the anomaly data in the input list """
    name = 'grubbs'
    desc = """Grubbs' test is to detect the presence of one outlier in a data set that is normally
    distributed"""

    def __init__(self, alpha_val=0.95):
        super().__init__()

        if alpha_val <= 0 or alpha_val >= 1:
            raise ValueError("invalid alpha value, valid range is (0, 1)")
        self.alpha = 1 - alpha_val

    def execute(self):
        """perform Grubbs' test and identify (if any) the outlier"""
        if self.input_is_empty():
            return []

        res = grubbs.test(self.list, alpha=self.alpha)

        error_indicator = [1 if k in set(res) else -1 for k in self.list]
        return error_indicator

    def set_params(self, params):
        """ set the value of alpha """
        super().set_params(params)

        if "alpha".lower() in params:
            # raise ValueError("alpha parameter is missing for grubbs algorithm")
            alpha_val = float(params["alpha"])

            if alpha_val <= 0 or alpha_val >= 1:
                raise ValueError("invalid alpha value, valid range is (0, 1)")

            self.alpha = 1 - alpha_val
