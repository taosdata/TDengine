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

"""Example batch envelope algorithm for TDgpt.

Copy this file to ``<custom_algo_dir>/batch/<algorithm_name>.py``, set ``name``
to the requested algorithm name, restart the TDgpt main service, then call it
with ``POST /api/v1/analysis/batch``.

Batch plugins are standalone classes. Do not inherit a TDgpt service base class.
The loader scans one level of ``batch/*.py`` files, finds classes whose name
starts with ``_``, and registers ``name``. ``build()`` returns
``(center, lower, upper)`` with equal lengths and no NaN/Inf values.
"""

import numpy as np


class _CustomBatchEnv:
    """Build a constant-offset envelope around the input series."""

    name = "custom_batch"
    description = "Constant offset envelope around the input series"

    def build(self, ts, values, windows, params):
        center = np.asarray(values, dtype=float)
        offset = float(params.get("offset", 1.0))
        if offset < 0:
            raise ValueError("parameter 'offset' must be >= 0")
        return center, center - offset, center + offset
