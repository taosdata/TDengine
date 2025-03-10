# encoding:utf-8
# pylint: disable=c0103
"""the main route definition for restful service"""
import os.path, sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../")

from flask import Flask, request

from taosanalytics.algo.anomaly import do_ad_check
from taosanalytics.algo.forecast import do_forecast, do_add_fc_params
from taosanalytics.conf import conf
from taosanalytics.model import get_avail_model
from taosanalytics.servicemgmt import loader
from taosanalytics.util import app_logger, validate_pay_load, get_data_index, get_ts_index, is_white_noise, \
    parse_options, convert_results_to_windows

app = Flask(__name__)

# load the all algos
app_logger.set_handler(conf.get_log_path())
app_logger.set_log_level(conf.get_log_level())
loader.load_all_service()


@app.route("/")
def start():
    """ default rsp """
    return "TDengine© Time Series Data Analytics Platform (ver 1.0.1)"


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
    app_logger.log_inst.debug('req payload: %s', request.json)

    algo = request.json["algo"].lower() if "algo" in request.json else "ksigma"

    # 1. validate the input data in json format
    try:
        validate_pay_load(request.json)
    except ValueError as e:
        return {"msg": str(e), "rows": -1}

    payload = request.json["data"]

    # 2. white noise data check
    wn_check = request.json["wncheck"] if "wncheck" in request.json else 1

    data_index = get_data_index(request.json["schema"])
    ts_index = get_ts_index(request.json["schema"])

    if wn_check:
        try:
            data = payload[data_index]
            if is_white_noise(data):
                app_logger.log_inst.debug("wn data, not process")
                return {"msg": "white noise can not be check", "rows": -1}
        except Exception as e:
            return {"msg": str(e), "rows": -1}

    # 3. parse the options for different ad services
    # the default options is like following: "algo=ksigma,k=2,invalid_option=44"
    options = request.json["option"] if "option" in request.json else None
    params = parse_options(options)

    # 4. do anomaly detection
    try:
        res_list = do_ad_check(payload[data_index], payload[ts_index], algo, params)
        ano_window = convert_results_to_windows(res_list, payload[ts_index])

        result = {"algo": algo, "option": options, "res": ano_window, "rows": len(ano_window)}
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
    app_logger.log_inst.debug('req payload: %s', request.json)

    # holt-winters by default
    algo = request.json['algo'].lower() if 'algo' in request.json else 'holtwinters'

    # 1. validate the input data in json format
    try:
        validate_pay_load(request.json)
    except ValueError as e:
        app_logger.log_inst.error('validate req json failed, %s', e)
        return {"msg": str(e), "rows": -1}

    payload = request.json["data"]

    # 2. white noise data check
    wn_check = request.json["wncheck"] if "wncheck" in request.json else 1
    data_index = get_data_index(request.json["schema"])
    ts_index = get_ts_index(request.json["schema"])

    if wn_check:
        try:
            data = payload[data_index]
            if is_white_noise(data):
                app_logger.log_inst.debug("%s wn data, not process", data)
                return {"msg": "white noise can not be check", "rows": -1}
        except Exception as e:
            return {"msg": str(e), "rows": -1}

    options = request.json["option"] if "option" in request.json else None
    params = parse_options(options)

    try:
        do_add_fc_params(params, request.json)
    except ValueError as e:
        app_logger.log_inst.error("invalid fc params: %s", e)
        return {"msg": f"{e}", "rows": -1}

    try:
        res1 = do_forecast(payload[data_index], payload[ts_index], algo, params)
        res = {"option": options, "rows": params["fc_rows"]}
        res.update(res1)

        app_logger.log_inst.debug("forecast result: %s", res)

        return res
    except Exception as e:
        app_logger.log_inst.error('forecast failed, %s', str(e))
        return {"msg": str(e), "rows": -1}


if __name__ == '__main__':
    app.run()
