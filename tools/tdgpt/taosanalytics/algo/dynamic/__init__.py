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

"""
Dynamic model loader classes.

This module provides base classes for loading and managing dynamically configured models
(anomaly detection, forecasting, regression) from JSON configuration files.
"""

from taosanalytics.algo.dynamic.classifier import BaseModelClassifier
from taosanalytics.algo.dynamic.detector import BaseModelAnomalyDetector
from taosanalytics.algo.dynamic.forecaster import BaseModelForecaster
from taosanalytics.algo.dynamic.regressioner import BaseModelRegressioner

__all__ = [
    "BaseModelAnomalyDetector",
    "BaseModelForecaster",
    "BaseModelRegressioner",
    "BaseModelClassifier",
]
