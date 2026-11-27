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

"""series_diagnosis: request handler for whole-series anomaly scoring."""

from lib.errors import failed_load_model_except
from lib.log import AppLogger
from lib.service_registry import loader
from lib.util import do_initial_check, parse_options

_MAX_SERIES_LENGTH = 40000


def do_series_diagnosis(series, algo_name, params):
    s = loader.get_service(algo_name)
    if s is None:
        AppLogger.error("specified series diagnosis model not found: %s", algo_name)
        failed_load_model_except(algo_name)

    s.set_input_data(series)
    s.set_params(params)
    return s.execute()


def handle_series_diagnosis(request):
    """
    Score an entire time series with a deployed diagnosis model.

    The ``algo`` field is the **deployed model name** (the name given at
    ``/deploy`` time, matching the config file stem), not the algorithm type.
    Use ``/list`` to see which models are currently registered under
    ``type=series_diagnosis``.

    Request format:
    {
        "algo": "<deployed-model-name>",
        "data": [[v1, v2, v3, ...]],   # exactly one column: the series values
        "schema": [["ts", "TIMESTAMP", 8], ["val", "DOUBLE", 8]],
        "option": "..."
    }

    Response:
    {
        "algo": "<deployed-model-name>",
        "score": <float>,
        "rows": 1
    }
    """
    try:
        req_json = do_initial_check(request)
    except Exception as e:
        AppLogger.error("failed to parse series diagnosis request: %s", e)
        return {"msg": str(e), "rows": -1}

    algo = req_json.get("algo", "").lower()
    if not algo:
        return {"msg": "missing required field: algo", "rows": -1}

    payload = req_json.get("data")
    if not payload or not payload[0]:
        return {"msg": "missing required field: data", "rows": -1}

    if len(payload) != 1:
        return {
            "msg": f"series diagnosis expects exactly 1 column, got {len(payload)}",
            "rows": -1,
        }

    try:
        series = [float(v) for v in payload[0]]
    except (TypeError, ValueError) as e:
        return {"msg": f"invalid data values: {e}", "rows": -1}

    n = len(series)
    if n > _MAX_SERIES_LENGTH:
        return {
            "msg": f"series too long: got {n} points, max is {_MAX_SERIES_LENGTH}",
            "rows": -1,
        }

    options = req_json.get("option")
    params = parse_options(options)

    try:
        score = do_series_diagnosis(series, algo, params)
        return {"algo": algo, "score": score, "rows": 1}
    except Exception as e:
        AppLogger.error("series diagnosis failed: %s", e)
        return {"msg": str(e), "rows": -1}
