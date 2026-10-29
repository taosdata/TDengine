# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

"""DynamicAnomalyService: an anomaly detection service driven by a parameter config file."""

from lib.base import AbstractAnomalyDetectionService
from lib.dynamic.algo.detector import (
    IsolationForestModelDetector,
    SVMModelDetector,
)
from lib.log import AppLogger

_DETECTOR_CLASSES = {
    "iforest": IsolationForestModelDetector,
    "svm": SVMModelDetector,
}


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
        AppLogger.info(
            "execute dynamic anomaly service:%s, algo:%s", self.name, algo_name
        )

        detector_class = _DETECTOR_CLASSES.get(algo_name)
        if detector_class is None:
            raise ValueError(
                f"unsupported algorithm '{algo_name}' in dynamic anomaly service"
            )

        detector = detector_class(
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
