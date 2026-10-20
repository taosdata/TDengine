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

"""regression: algo logic and request handler"""

from lib.errors import failed_load_model_except
from lib.log import AppLogger
from lib.service_registry import loader
from lib.util import do_initial_check, parse_options


def do_regression(input_data, schema, algo_name, params):
    """Execute regression using the specified algorithm."""
    s = loader.get_service(algo_name)

    if s is None:
        AppLogger.error("specified regression model not found: %s", algo_name)
        failed_load_model_except(algo_name)

    s.set_input_data(input_data, schema)
    s.set_params(params)

    result = s.execute()

    AppLogger.debug(
        "regression result: %d predictions from %d input samples",
        len(result),
        len(input_data),
    )

    return result


def handle_regression(request):
    """
    Execute regression business logic.

    Parses the incoming request, validates parameters, runs the regression
    algorithm, and returns the result dict.

    Request format:
    {
        "algo": "linear",
        "data": [[x1, x2, ...], [x1, x2, ...], ...],  # feature columns
        "schema": [["ts", "TIMESTAMP", 8], ["x1", "DOUBLE", 8], ...],
        "option": "..."  # optional
    }

    :param request: Flask request object
    :return: dict with regression result or error information
    """
    try:
        req_json = do_initial_check(request)
    except Exception as e:
        AppLogger.error("failed to parse regression request: %s", str(e))
        return {"msg": str(e), "rows": -1}

    algo = req_json.get("algo", "").lower()
    if not algo:
        return {"msg": "missing required field: algo", "rows": -1}

    schema = req_json.get("schema")
    if not schema:
        return {"msg": "missing required field: schema", "rows": -1}

    payload = req_json.get("data")
    if not payload:
        return {"msg": "missing required field: data", "rows": -1}

    options = req_json.get("option")
    params = parse_options(options)

    # Transpose: payload is list of columns, regression expects list of rows
    try:
        n_rows = len(payload[0])
        input_data = [
            [payload[col][row] for col in range(len(payload))] for row in range(n_rows)
        ]
    except (IndexError, TypeError) as e:
        AppLogger.error("failed to transpose input data: %s", str(e))
        return {"msg": f"invalid data format: {e}", "rows": -1}

    try:
        predictions = do_regression(input_data, schema, algo, params)
        result = {
            "algo": algo,
            "option": options,
            "res": predictions,
            "rows": len(predictions),
        }
        AppLogger.debug("regression result: %s", result)
        return result
    except Exception as e:
        AppLogger.error("regression failed: %s", str(e))
        return {"msg": str(e), "rows": -1}
