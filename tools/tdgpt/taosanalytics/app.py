# encoding:utf-8
# pylint: disable=c0103
"""the main route definition for restful service"""
import os.path, sys
import argparse

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../")

import taosanalytics
from flask import Flask, request
from taosanalytics.service.imputation_service import handle_imputation
from taosanalytics.service.anomaly_service import handle_anomaly
from taosanalytics.service.forecast_service import handle_forecast
from taosanalytics.service.correl_service import handle_correlation
from taosanalytics.service.misc_service import handle_batch

from taosanalytics.conf import Configure
from taosanalytics.model_mgmt import ModelManager
from taosanalytics.builtins import loader
from taosanalytics.log import AppLogger

app = Flask(__name__)
app.config["PROPAGATE_EXCEPTIONS"] = True


@app.route("/")
def default():
    """ default rsp """
    return taosanalytics._ANODE_VER


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
    return ModelManager.get_instance().get_model_list()


@app.route("/anomaly-detect", methods=['POST'])
def handle_ad_request():
    """handle the anomaly detection requests"""
    AppLogger.get_instance().info('recv ad request from %s', request.remote_addr)
    return handle_anomaly(request)


@app.route("/forecast", methods=['POST'])
def handle_forecast_req():
    """handle the fc request """
    AppLogger.get_instance().info('recv forecast request from %s', request.remote_addr)
    return handle_forecast(request)


@app.route("/imputation", methods=['POST'])
def handle_imputation_req():
    """handle the imputation request """
    return handle_imputation(request)


@app.route("/correlation", methods=['POST'])
def handle_correlation_req():
    """handle the correlation request """
    AppLogger.get_instance().info('recv correlation from %s', request.remote_addr)
    return handle_correlation(request)


@app.route("/tool/batch", methods=['POST'])
def handle_batch_req():
    """handle the batch request request """
    return handle_batch(request)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='TDgpt analytics service')
    parser.add_argument('-c', dest='conf_path', default=None,
                        help='path to configuration file')
    args = parser.parse_args()

    conf = Configure.init(args.conf_path)

    AppLogger.set_handler(conf.get_log_path())
    AppLogger.set_log_level(conf.get_log_level())
    loader.load_all_service()

    app.run(port=6035)

