# encoding:utf-8
"""DynamicRegressionService: a regression service driven by a parameter config file."""
from taosanalytics.algo.tool.regressioner import (
    LinearRegressioner,
    LassoRegressioner,
    RidgeRegressioner,
    ElasticNetRegressioner,
)
from taosanalytics.log import AppLogger
from taosanalytics.base import AbstractRegressionService

_SUPPORTED_ALGOS = {'linear', 'lasso', 'ridge', 'elasticnet'}
_DETECTOR_CLASSES = {
    'linear': LinearRegressioner,
    'lasso': LassoRegressioner,
    'ridge': RidgeRegressioner,
    'elasticnet': ElasticNetRegressioner,
}


class DynamicRegressionService(AbstractRegressionService):
    """
    A simple dynamic regression service driven by a JSON config file.
    The detector is constructed and executed when execute() is called.

    Currently supported algorithms: linear.
    """

    def __init__(self, name: str, desc: str, algo: str, path: str):
        super().__init__()

        self.name = name
        self.desc = desc

        self.config_file_path = path
        self.algo = algo

    def execute(self):
        """Construct the detector from the config file and run regression."""
        algo_name = self.algo.lower()
        AppLogger.info("execute dynamic regression service:%s, algo:%s", self.name, algo_name)

        if algo_name not in _SUPPORTED_ALGOS:
            raise ValueError(f"unsupported algorithm '{algo_name}' in dynamic regression service")

        detector_class = _DETECTOR_CLASSES.get(algo_name)
        if detector_class is None:
            raise ValueError(f"no detector class found for algorithm '{algo_name}'")

        detector = detector_class(
            path=self.config_file_path,
            input_data=self.input_data,
            schema=self.schema,
        )

        result = detector.predict()
        expected_size = len(self.input_data) if self.input_data is not None else None

        if result is None:
            raise ValueError("dynamic regression detector returned no result")

        if expected_size is not None and len(result) != expected_size:
            raise ValueError(
                "dynamic regression detector returned %d predictions for %d input samples"
                % (len(result), expected_size)
            )

        return result
