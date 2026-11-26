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

"""Example classification algorithm for TDgpt.

Copy this file to ``<custom_algo_dir>/classification/<algorithm_name>.py``, set
``name`` to the requested algorithm name, restart the TDgpt main service, then
call it with ``POST /classification``.

The input is a feature matrix: ``self.input_data`` is a list of rows. The
service must return one class label per input row.
"""

import numpy as np

from lib.base import AbstractClassificationService


class _CustomClassifier(AbstractClassificationService):
    """Predict a binary label from the first feature column."""

    name = "custom_cls"
    desc = "Example threshold classifier"

    def __init__(self):
        super().__init__()
        self.threshold = 0.5

    def execute(self):
        if not self.input_data:
            raise ValueError("input_data is required for classification")

        rows = np.asarray(self.input_data, dtype=float)
        if rows.ndim != 2 or rows.shape[1] < 1:
            raise ValueError("classification expects a feature matrix")

        # TODO: replace this rule with the user's model. The first column is
        # used here as the feature; the last column, when present, can be used
        # as the training label.
        features = rows[:, 0]
        labels = (features >= self.threshold).astype(int)
        return labels.tolist()

    def set_params(self, params):
        """Map option strings to typed parameters for the user model."""
        super().set_params(params)
        if params and "threshold" in params:
            try:
                threshold = float(params["threshold"])
            except (TypeError, ValueError):
                raise ValueError("parameter 'threshold' must be a number")
            self.threshold = threshold

    def get_params(self):
        return {"threshold": self.threshold}
