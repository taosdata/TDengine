# encoding:utf-8
"""correlation service: encapsulates correlation business logic"""

from taosanalytics.algo.correlation import do_dtw, do_tlcc
from taosanalytics.conf import AppLogger
from taosanalytics.util import parse_options, get_more_data_list, do_check_before_exec


def handle_correlation(request):
    """
    Execute correlation business logic.

    :param request: Flask request object
    :return: dict with correlation result or error information
    """
    try:
        # check for rows limitation to reduce the dtw process time
        req_json, payload, options, data_index, ts_index = do_check_before_exec(request, False)
    except Exception as e:
        return {"msg": str(e), "rows": -1}

    params = parse_options(options)
    algo = req_json['algo'].lower()

    try:
        second_list = get_more_data_list(payload, req_json["schema"])

        if algo == 'dtw':
            dist, path = do_dtw(payload[data_index], second_list, params)
            res = {"option": options, "rows": len(path), "distance": dist, "path": path}
            AppLogger.get_instance().debug("dtw result: %s", res)
            return res
        elif algo == 'tlcc':
            lags, ccf_vals = do_tlcc(payload[data_index], second_list, params)
            res = {"option": options, "rows": len(lags), "lags": lags, "ccf_vals": ccf_vals}
            AppLogger.get_instance().debug("tlcc result: %s", res)
            return res
        else:
            raise ValueError(f"unsupported algo: {algo}")
    except Exception as e:
        AppLogger.get_instance().error('correlation failed, %s', str(e))
        return {"msg": str(e), "rows": -1}
