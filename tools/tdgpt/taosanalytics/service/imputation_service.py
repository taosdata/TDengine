# encoding:utf-8
"""imputation service: encapsulates imputation business logic"""

from taosanalytics.algo.imputation import do_imputation, do_set_imputation_params, check_freq_param
from taosanalytics.conf import AppLogger
from taosanalytics.util import parse_options, do_check_before_exec


def handle_imputation(request):
    """
    Execute imputation business logic.

    :param request: Flask request object
    :return: dict with imputation result or error information
    """

    try:
        req_json, payload, options, data_index, ts_index = do_check_before_exec(request)
    except Exception as e:
        return {"msg": str(e), "rows": -1}

    params = parse_options(options)

    try:
        do_set_imputation_params(params, req_json)
    except ValueError as e:
        AppLogger.get_instance().log_inst.error("invalid imputation params: %s", e)
        return {"msg": f"{e}", "rows": -1}

    algo = req_json['algo'].lower() if 'algo' in req_json else 'moment'

    try:
        freq = req_json["freq"] if 'freq' in req_json else '1ms'
        prec = req_json['prec'] if 'prec' in req_json else 'ms'
        check_freq_param(payload[ts_index], freq, prec)

        imputat_res = do_imputation(payload[data_index], payload[ts_index], algo, params)

        final_res = {"option": options, "rows": len(imputat_res["ts"])}
        final_res.update(imputat_res)

        AppLogger.get_instance().log_inst.debug("imputation result: %s", final_res)
        return final_res
    except Exception as e:
        AppLogger.get_instance().log_inst.error('imputation failed, %s', str(e))
        return {"msg": str(e), "rows": -1}
