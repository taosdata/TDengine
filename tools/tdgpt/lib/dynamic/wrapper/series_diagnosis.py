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

"""DynamicSeriesDiagnosisService: whole-series scoring driven by a JSON config file."""

from lib.base import AbstractSeriesDiagnosisService
from lib.dynamic.algo.series_diagnosis import (
    IsolationForestSeriesDiagnosisScorer,
    OCSVMSeriesDiagnosisScorer,
)
from lib.log import AppLogger

_SCORER_CLASSES = {
    "iforest_diagnosis": IsolationForestSeriesDiagnosisScorer,
    "svm_diagnosis": OCSVMSeriesDiagnosisScorer,
}


class DynamicSeriesDiagnosisService(AbstractSeriesDiagnosisService):
    """Dynamic service that scores an entire time series via catch22 + a trained model.

    Supported algorithms: isolation_forest, ocsvm.
    """

    def __init__(self, name: str, desc: str, algo: str, path: str):
        super().__init__()
        self.name = name
        self.desc = desc
        self.config_file_path = path
        self.algo = algo

    def execute(self) -> float:
        algo_name = self.algo.lower()
        AppLogger.info(
            "execute dynamic sequence diagnosis service: %s, algo: %s",
            self.name,
            algo_name,
        )
        scorer_class = _SCORER_CLASSES.get(algo_name)
        if scorer_class is None:
            raise ValueError(
                f"unsupported algorithm '{algo_name}' in dynamic sequence diagnosis service"
            )
        scorer = scorer_class(path=self.config_file_path, series=self.series)
        return scorer.predict()
