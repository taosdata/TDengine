#!/usr/bin/env python3

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

"""Example univariate anomaly detection algorithm for TDgpt.

Copy this file to ``<custom_algo_dir>/ad/<algorithm_name>.py``, set ``name`` to
the requested algorithm name, restart the TDgpt main service, then call it with
``POST /anomaly-detect`` or ``ANOMALY_WINDOW(column, "algo=<algorithm_name>")``.

Only ``numpy`` is required. Replace the loop inside ``execute()`` with the real
detection logic; keep the class name, base class, ``name`` attribute, the
zero-argument constructor, and the parameter handling contract.
"""

import math

import numpy as np

from lib.base import AbstractAnomalyDetectionService


class _CustomAnomalyDetector(AbstractAnomalyDetectionService):
    """Rolling z-score detector used as a starting point for custom algorithms.

    The first ``reference`` points build the baseline. From then on, every
    point is compared with the mean of the trailing ``window`` points. A point
    is marked anomalous (``-1``) when the standardized difference exceeds
    ``threshold``. Normal points stay at ``valid_code`` (default ``1``).
    """

    name = "custom_ad"
    desc = "Example rolling z-score anomaly detector"

    def __init__(self):
        super().__init__()
        self.reference = 30
        self.window = 10
        self.min_window = 5
        self.threshold = 3.0

    def execute(self):
        if self.input_is_empty():
            return []

        values = np.asarray(self.list, dtype=float)
        n = values.size
        if n < self.reference + self.min_window:
            return [1] * n

        baseline = values[: self.reference]
        base_mean = float(np.mean(baseline))
        base_std = float(np.std(baseline))
        if not math.isfinite(base_std) or base_std <= 0:
            base_std = max(abs(base_mean) * 1e-6, 1e-9)

        flags = np.ones(n, dtype=int)
        for i in range(self.reference + self.min_window - 1, n):
            recent = values[i - self.window + 1 : i + 1]
            score = abs(float(np.mean(recent)) - base_mean) / (
                base_std / math.sqrt(recent.size)
            )
            if score > self.threshold:
                flags[i] = -1

        return flags.tolist()

    def set_params(self, params):
        """Receive request parameters.

        ``option`` values arrive as strings, for example
        ``option="window=20,threshold=4.5"``, so every value is converted and
        validated here. Keep the conversion even if the algorithm is called
        programmatically with integers or floats.
        """
        super().set_params(params)
        if not params:
            return

        if "reference" in params:
            self.reference = self._positive_int("reference", params["reference"], 2)
        if "window" in params:
            self.window = self._positive_int("window", params["window"], 2)
        if "min_window" in params:
            self.min_window = self._positive_int("min_window", params["min_window"], 1)
        if "threshold" in params:
            self.threshold = self._positive_float("threshold", params["threshold"], 0.1)

        if self.window > self.reference:
            raise ValueError("window must not be greater than reference")

    def get_params(self):
        return {
            "reference": self.reference,
            "window": self.window,
            "min_window": self.min_window,
            "threshold": self.threshold,
        }

    @staticmethod
    def _positive_int(name, value, minimum):
        try:
            parsed = int(float(value))
        except (TypeError, ValueError):
            raise ValueError("parameter '%s' must be an integer" % name)
        if parsed < minimum:
            raise ValueError("parameter '%s' must be >= %d" % (name, minimum))
        return parsed

    @staticmethod
    def _positive_float(name, value, minimum):
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            raise ValueError("parameter '%s' must be a number" % name)
        if not math.isfinite(parsed) or parsed < minimum:
            raise ValueError("parameter '%s' must be >= %s" % (name, minimum))
        return parsed
