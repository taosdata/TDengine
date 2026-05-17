# encoding:utf-8
"""DynamicAnomalyService: an anomaly detection service driven by a parameter config file."""
from taosanalytics.algo.tool.detector import IsolationForestModelDetector
from taosanalytics.log import AppLogger
from taosanalytics.base import AbstractAnomalyDetectionService

_SUPPORTED_ALGOS = {'iforest'}


class DynamicAnomalyService(AbstractAnomalyDetectionService):
    """
    A simple dynamic anomaly detection service driven by a JSON config file.
    The detector is constructed and executed when execute() is called.

    Currently supported algorithms: iforest.
    """

    def __init__(self, name: str, desc: str, algo: str, path: str):
        super().__init__()

        self.name = name
        self.desc = desc

        self.config_file_path = path
        self.algo = algo

    def execute(self):
        """Construct the detector from the config file and run anomaly detection."""
        algo_name = self.algo.lower()
        AppLogger.info("execute dynamic anomaly service:%s, algo:%s", self.name, algo_name)

        if algo_name not in _SUPPORTED_ALGOS:
            raise ValueError(f"unsupported algorithm '{algo_name}' in dynamic anomaly service")

        detector = IsolationForestModelDetector(
            path=self.config_file_path,
            input_list=self.list,
            ts_list=self.ts_list,
            valid_code=self.valid_code,
            input_data_lists=self.input_data_lists,
        )

        result = detector.detect()
        expected_size = len(self.list) if self.list is not None else None

        if result is None:
            raise ValueError("dynamic anomaly detector returned no result")

        if expected_size is not None and len(result) != expected_size:
            raise ValueError(
                "dynamic anomaly detector returned %d codes for %d input points"
                % (len(result), expected_size)
            )

        return result
