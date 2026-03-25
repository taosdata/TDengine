# encoding:utf-8
"""forecast service: encapsulates forecast business logic"""

from taosanalytics.algo.forecast import do_forecast, do_add_fc_params
from taosanalytics.conf import AppLogger
from taosanalytics.util import parse_options, get_past_dynamic_data, get_dynamic_data, do_check_before_exec


def handle_forecast(request):
    """
    Execute forecast business logic.

    Parses the incoming request, validates parameters, runs the forecast
    algorithm, and returns the result dict.

    :param request: Flask request object
    :return: dict with forecast result or error information
    """

    try:
        req_json, payload, options, data_index, ts_index = do_check_before_exec(request)
    except Exception as e:
        AppLogger.get_instance().log_inst.error("failed to parse forecast request: %s", str(e))
        return {"msg": str(e), "rows": -1}

    params = parse_options(options)

    try:
        do_add_fc_params(params, req_json)
    except ValueError as e:
        AppLogger.get_instance().log_inst.error("invalid fc params: %s", e)
        return {"msg": f"{e}", "rows": -1}

    # holt-winters by default
    algo = req_json['algo'].lower() if 'algo' in req_json else 'holtwinters'

    try:
        res1 = do_forecast(
            payload[data_index],
            payload[ts_index],
            algo,
            params,
            get_past_dynamic_data(payload, req_json["schema"]),
            get_dynamic_data(payload, req_json["schema"]),
        )

        res = {"option": options, "rows": params["rows"]}
        res.update(res1)

        AppLogger.get_instance().log_inst.debug("forecast result: %s", res)
        return res
    except Exception as e:
        AppLogger.get_instance().log_inst.error('forecast failed, %s', str(e))
        return {"msg": str(e), "rows": -1}
