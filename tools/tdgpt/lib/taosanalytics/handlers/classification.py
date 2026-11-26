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

"""classification: algo logic and request handler"""

from lib.errors import failed_load_model_except
from lib.log import AppLogger
from lib.service_registry import loader
from lib.util import (
    do_initial_check,
    get_algo,
    normalize_json_values,
    parse_options,
    validate_columns,
)


def do_classification(input_data, schema, algo_name, params):
    """Execute classification using the specified algorithm."""
    s = loader.get_service(algo_name)

    if s is None:
        AppLogger.error("specified classification model not found: %s", algo_name)
        failed_load_model_except(algo_name)

    s.set_input_data(input_data, schema)
    s.set_params(params)

    result = s.execute()

    AppLogger.debug(
        "classification result: %d predictions from %d input samples",
        len(result),
        len(input_data),
    )

    return result


def handle_classification(request):
    """
    Execute classification business logic.

    Parses the incoming request, validates parameters, runs the classification
    algorithm, and returns the result dict.

    Request format:
    {
        "algo": "user_classifier",
        "data": [[x1, x2, ...], [x1, x2, ...], ...],  # feature columns
        "schema": [["x1", "DOUBLE", 8], ...],
        "option": "..."  # optional
    }

    :param request: Flask request object
    :return: dict with classification result or error information
    """
    try:
        req_json = do_initial_check(request)
    except Exception as e:
        AppLogger.error("failed to parse classification request: %s", str(e))
        return {"msg": str(e), "rows": -1}

    if not isinstance(req_json, dict):
        return {"msg": "request body must be a JSON object", "rows": -1}

    try:
        algo = get_algo(req_json, "")
    except ValueError as e:
        return {"msg": str(e), "rows": -1}
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

    # Transpose: payload is list of columns, classification expects list of rows
    try:
        n_rows = validate_columns(payload)
    except ValueError as e:
        AppLogger.error("invalid classification input data: %s", str(e))
        return {"msg": str(e), "rows": -1}

    try:
        input_data = [
            [payload[col][row] for col in range(len(payload))] for row in range(n_rows)
        ]
    except (IndexError, KeyError, TypeError) as e:
        AppLogger.error("failed to transpose classification input data: %s", str(e))
        return {"msg": f"invalid data format: {e}", "rows": -1}

    try:
        predictions = do_classification(input_data, schema, algo, params)
        if not isinstance(predictions, list):
            predictions = list(predictions)
        if len(predictions) != n_rows:
            return {
                "msg": "classification returned %d predictions for %d input samples"
                % (len(predictions), n_rows),
                "rows": -1,
            }
        predictions = normalize_json_values(predictions)

        result = {
            "algo": algo,
            "option": options,
            "res": predictions,
            "rows": len(predictions),
        }
        AppLogger.debug("classification result: %s", result)
        return result
    except Exception as e:
        AppLogger.error("classification failed, %s", str(e))
        return {"msg": str(e), "rows": -1}
