# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
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

"""shesd algorithm class definition"""

from pandas import Series
from pyculiarity import detect_vec

from taosanalytics.base import AbstractAnomalyDetectionService


class _SHESDService(AbstractAnomalyDetectionService):
    """s-h-esd algorithm is to check the anomaly data in the input list"""

    name = "shesd"
    desc = ""
    _builtins = True

    def __init__(self, n_period=0, direction="both", anoms=0.05):
        super().__init__()

        self.period = n_period
        self.direction = direction
        self.max_anoms = anoms

    def execute(self):
        """perform SHESD test and identify (if any) the outlier"""
        if self.input_is_empty():
            return []

        period = self.period if self.period > 0 else max(2, len(self.list) // 2)

        results = detect_vec(
            Series(self.list),
            max_anoms=self.max_anoms,
            direction=self.direction,
            period=period,
        )

        res_val = results["anoms"]["anoms"]

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
        return {
            "period": self.period,
            "direction": self.direction,
            "max_anoms": self.max_anoms,
        }
