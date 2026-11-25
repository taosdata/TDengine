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

"""Adapter skeleton: expose a user model as a TDgpt anomaly detector.

Use this when the user's code does not inherit AbstractAnomalyDetectionService.
Keep this adapter class in the entry file and put the user's code in a helper
package next to it, for example ``my_algo_impl/``. Deploy both with
``--script my_algo.py --asset my_algo_impl``.
"""

import threading

import numpy as np

from lib.base import AbstractAnomalyDetectionService


class _UserModelAdapter(AbstractAnomalyDetectionService):
    name = "user_algo"  # TODO: set to the requested algorithm name
    desc = "Adapter for a user-provided anomaly detection model"

    def __init__(self):
        super().__init__()
        self._model = None
        self._lock = threading.Lock()
        # TODO: add typed parameters with defaults.

    def _load_model(self):
        """Build or load the user's model once, on first use."""
        if self._model is None:
            with self._lock:
                if self._model is None:
                    # TODO: import and construct the user's model here.
                    # Keep helper code in a subpackage so the loader does not
                    # import it as a separate algorithm:
                    # from .my_algo_impl.user_model import load_model
                    # self._model = load_model()
                    raise NotImplementedError(
                        "wire the user's model in _load_model()"
                    )
        return self._model

    def execute(self):
        if self.input_is_empty():
            return []

        values = np.asarray(self.list, dtype=float)
        model = self._load_model()

        # TODO: call the user's model and convert its output to one mark per
        # input point: 1 for normal, -1 for anomaly (or any value different
        # from self.valid_code).
        raw = model.predict(values.reshape(-1, 1))
        marks = np.asarray(raw).ravel()
        if marks.size != values.size:
            raise ValueError(
                "user model returned %d marks, expected %d"
                % (marks.size, values.size)
            )
        return np.where(marks == 0, 1, -1).tolist()

    def set_params(self, params):
        """Convert option strings to typed parameters for the user model."""
        super().set_params(params)
        # TODO: validate ranges and pass values to the model. Option values
        # arrive as strings, for example option="threshold=3.5".
        # if "threshold" in params:
        #     self.threshold = float(params["threshold"])

    def get_params(self):
        # TODO: return the parameters shown by GET /list.
        return {}
