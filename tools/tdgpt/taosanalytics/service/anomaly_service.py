# encoding:utf-8
"""anomaly service: encapsulates anomaly detection business logic"""

from taosanalytics.algo.anomaly import do_ad_check
from taosanalytics.conf import AppLogger
from taosanalytics.util import parse_options, do_check_before_exec


def handle_anomaly(request):
    """
    Execute anomaly detection business logic.

    Parses the incoming request, validates parameters, runs the anomaly
    detection algorithm, and returns the result dict.

    :param request: Flask request object
    :return: dict with anomaly detection result or error information
    """

    try:
        req_json, payload, options, data_index, ts_index = do_check_before_exec(request, True)
    except Exception as e:
        AppLogger.get_instance().error("failed to do anomaly-detection, %s", str(e))
        return {"msg": str(e), "rows": -1}

    algo = req_json["algo"].lower() if "algo" in req_json else "ksigma"

    try:
        ts_list = payload[ts_index].copy()
        payload.pop(ts_index)
    except ValueError as e:
        return {"msg": str(e), "rows": -1}

    params = parse_options(options)

    try:
        res_list, ano_window, mask_list = do_ad_check(payload, ts_list, algo, params)
        result = {"algo": algo, "option": options, "res": ano_window, "rows": len(ano_window), "mask": mask_list}

        AppLogger.get_instance().debug("anomaly-detection result: %s", str(result))
        return result
    except Exception as e:
        result = {"res": {}, "rows": -1, "msg": str(e)}
        AppLogger.get_instance().error("failed to do anomaly-detection, %s", str(e))
        return result
