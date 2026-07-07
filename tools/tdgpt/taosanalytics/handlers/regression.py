# encoding:utf-8
"""regression handlers: encapsulates regression business logic"""

from taosanalytics.algo.regression import do_regression
from taosanalytics.log import AppLogger
from taosanalytics.util import parse_options, do_initial_check


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
        input_data = [[payload[col][row] for col in range(len(payload))] for row in range(n_rows)]
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
