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

"""Adapter skeleton: expose a user model as a TDgpt regression algorithm.

Use this when the user's code does not inherit AbstractRegressionService.
Keep this adapter class in the entry file and put the user's code in a helper
package next to it, for example ``my_algo_impl/``. Deploy both with
``--script my_algo.py --asset my_algo_impl``.
"""

import threading

import numpy as np

from lib.base import AbstractRegressionService


class _UserRegressionAdapter(AbstractRegressionService):
    name = "user_regression"  # TODO: set to the requested algorithm name
    desc = "Adapter for a user-provided regression model"

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
                    # from .my_algo_impl.user_model import load_model
                    # self._model = load_model()
                    raise NotImplementedError(
                        "wire the user's model in _load_model()"
                    )
        return self._model

    def execute(self):
        if not self.input_data:
            raise ValueError("input_data is required for regression")

        features = np.asarray(self.input_data, dtype=float)
        model = self._load_model()

        # TODO: call the user's model. It must return one prediction per row.
        predictions = np.asarray(model.predict(features), dtype=float).ravel()
        if predictions.size != features.shape[0]:
            raise ValueError(
                "user model returned %d predictions, expected %d"
                % (predictions.size, features.shape[0])
            )
        return predictions.tolist()

    def set_params(self, params):
        """Convert option strings to typed parameters for the user model."""
        super().set_params(params)
        # TODO: validate ranges and pass values to the model.

    def get_params(self):
        # TODO: return the parameters shown by GET /list.
        return {}
