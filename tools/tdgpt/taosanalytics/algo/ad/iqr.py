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

"""iqr class definition"""

import numpy as np

from taosanalytics.base import AbstractAnomalyDetectionService


class _IqrService(AbstractAnomalyDetectionService):
    """IQR algorithm is to check the anomaly data in the input list"""

    name = "iqr"
    desc = """found the anomaly data according to the inter-quartile range"""
    _builtins = True

    def __init__(self):
        super().__init__()

    def execute(self):
        if self.input_is_empty():
            return []

        lower = np.quantile(self.list, 0.25)
        upper = np.quantile(self.list, 0.75)

        min_val = lower - 1.5 * (upper - lower)
        max_val = upper + 1.5 * (upper - lower)

        threshold = [min_val, max_val]
        return [-1 if k < threshold[0] or k > threshold[1] else 1 for k in self.list]

    def set_params(self, params):
        pass
