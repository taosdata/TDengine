# encoding:utf-8
# pylint: disable=c0103
"""the main route definition for restful service"""
import os.path, sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../")

from flask import Flask, request

from taosanalytics.algo.imputation import (do_imputation, do_set_imputation_params, check_freq_param)
from taosanalytics.algo.anomaly import do_ad_check
from taosanalytics.algo.forecast import do_forecast, do_add_fc_params
from taosanalytics.algo.correlation import do_dtw, do_tlcc

from taosanalytics.conf import conf
from taosanalytics.model import get_avail_model
from taosanalytics.servicemgmt import loader
from taosanalytics.util import (app_logger, parse_options, get_past_dynamic_data, get_dynamic_data,
                                get_second_data_list,
                                do_check_before_exec)

app = Flask(__name__)

# load the all algos
app_logger.set_handler(conf.get_log_path())
app_logger.set_log_level(conf.get_log_level())
loader.load_all_service()

_ANODE_VER = 'TDgpt - TDengine TSDB© Time-Series Data Analytics Platform (ver 3.3.7.1)'

@app.route("/")
def start():
    """ default rsp """
    return _ANODE_VER


@app.route("/status")
def server_status():
    """ return server status """
    return {
        'protocol': 1.0,
        'status': 'ready'
    }


@app.route("/list")
def list_all_services():
    """
    API function to return all available services, including both fc and anomaly detection
    """
    return loader.get_service_list()


@app.route("/models")
def list_all_models():
    """ list all available models """
    return get_avail_model()


@app.route("/anomaly-detect", methods=['POST'])
def handle_ad_request():
    """handle the anomaly detection requests"""
    app_logger.log_inst.info('recv ad request from %s', request.remote_addr)

    try:
        req_json, payload, options, data_index, ts_index = do_check_before_exec(request, True)
    except Exception as e:
        return {"msg": str(e), "rows": -1}

    algo = req_json["algo"].lower() if "algo" in req_json else "ksigma"

    # 1. validate the input data in json format
    try:
        d = req_json["data"]
        if len(d) > 2:
            raise ValueError(f"invalid data format, too many columns for anomaly-detection, allowed:2, input:{len(d)}")
    except ValueError as e:
        return {"msg": str(e), "rows": -1}

    params = parse_options(options)

    # 4. do anomaly detection
    try:
        res_list, ano_window, mask_list = do_ad_check(payload[data_index], payload[ts_index], algo, params)
        result = {"algo": algo, "option": options, "res": ano_window, "rows": len(ano_window), "mask":mask_list}

        app_logger.log_inst.debug("anomaly-detection result: %s", str(result))
        return result

    except Exception as e:
        result = {"res": {}, "rows": 0, "msg": str(e)}
        app_logger.log_inst.error("failed to do anomaly-detection, %s", str(e))

        return result


@app.route("/forecast", methods=['POST'])
def handle_forecast_req():
    """handle the fc request """
    app_logger.log_inst.info('recv fc from %s', request.remote_addr)
    req_json, payload, options, data_index, ts_index = do_check_before_exec(request)

    params = parse_options(options)

    try:
        do_add_fc_params(params, req_json)
    except ValueError as e:
        app_logger.log_inst.error("invalid fc params: %s", e)
        return {"msg": f"{e}", "rows": -1}

    # holt-winters by default
    algo = req_json['algo'].lower() if 'algo' in req_json else 'holtwinters'

    try:
        res1 = do_forecast(payload[data_index], payload[ts_index], algo, params,
                           get_past_dynamic_data(payload, req_json["schema"]),
                           get_dynamic_data(payload, req_json["schema"]))

        res = {"option": options, "rows": params["rows"]}
        res.update(res1)

        app_logger.log_inst.debug("forecast result: %s", res)
        return res
    except Exception as e:
        app_logger.log_inst.error('forecast failed, %s', str(e))
        return {"msg": str(e), "rows": -1}


@app.route("/imputation", methods=['POST'])
def handle_imputation_req():
    """handle the imputation request """
    app_logger.log_inst.info('recv imputation from %s', request.remote_addr)
    try:
        req_json, payload, options, data_index, ts_index = do_check_before_exec(request)
    except Exception as e:
        return {"msg": str(e), "rows": -1}

    params = parse_options(options)

    try:
        do_set_imputation_params(params, req_json)
    except ValueError as e:
        app_logger.log_inst.error("invalid imputation params: %s", e)
        return {"msg": f"{e}", "rows": -1}

    algo = req_json['algo'].lower() if 'algo' in req_json else 'moment'

    try:
        freq = req_json["freq"] if 'freq' in req_json else '1ms'
        prec = req_json['prec'] if 'prec' in req_json else 'ms'
        check_freq_param(payload[ts_index], freq, prec)

        imputat_res = do_imputation(payload[data_index], payload[ts_index], algo, params)

        final_res = {"option": options, "rows": len(imputat_res["ts"])}
        final_res.update(imputat_res)

        app_logger.log_inst.debug("imputation result: %s", final_res)
        return final_res
    except Exception as e:
        app_logger.log_inst.error('imputation failed, %s', str(e))
        return {"msg": str(e), "rows": -1}


@app.route("/correlation", methods=['POST'])
def handle_correlation_req():
    """handle the correlation request """
    app_logger.log_inst.info('recv correlation from %s', request.remote_addr)
    try:
        # check for rows limitation to reduce the dtw process time
        req_json, payload, options, data_index, ts_index = do_check_before_exec(request, False)
    except Exception as e:
        return {"msg": str(e), "rows": -1}

    params = parse_options(options)

    algo = req_json['algo'].lower()

    try:
        second_list = get_second_data_list(payload, req_json["schema"])

        if algo == 'dtw':
            dist, path = do_dtw(payload[data_index], second_list, params)

            res = {"option": options, "rows": len(path), "distance": dist, "path": path}
            app_logger.log_inst.debug("dtw result: %s", res)

            return res
        elif algo == 'tlcc':
            lags, ccf_vals = do_tlcc(payload[data_index], second_list, params)

            res = {"option": options, "rows": len(lags), "lags": lags, "ccf_vals": ccf_vals}
            app_logger.log_inst.debug("tlcc result: %s", res)

            return res
        else:
            raise ValueError(f"unsupported algo: {algo}")
    except Exception as e:
        app_logger.log_inst.error('correlation failed, %s', str(e))
        return {"msg": str(e), "rows": -1}

if __name__ == '__main__':
    app.run(port=6035)
