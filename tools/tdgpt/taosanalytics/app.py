# encoding:utf-8
# pylint: disable=c0103
"""the main route definition for restful service"""
import os.path, sys
import argparse

from taosanalytics.model_file_mgt import ModelFileManager
from taosanalytics.service_registry import loader

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../")

import taosanalytics
from flask import Flask, request
from taosanalytics.service.imputation_service import handle_imputation
from taosanalytics.service.anomaly_service import handle_anomaly
from taosanalytics.service.forecast_service import handle_forecast
from taosanalytics.service.correl_service import handle_correlation
from taosanalytics.service.misc_service import handle_batch

from taosanalytics.conf import Configure
from taosanalytics.log import AppLogger

app = Flask(__name__)
app.config["PROPAGATE_EXCEPTIONS"] = True


@app.route("/")
def index():
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
    return ModelFileManager.get_instance().get_model_list()


@app.route("/anomaly-detect", methods=['POST'])
def handle_ad_request():
    """handle the anomaly detection requests"""
    AppLogger.info('recv ad request from %s', request.remote_addr)
    return handle_anomaly(request)


@app.route("/forecast", methods=['POST'])
def handle_forecast_req():
    """handle the fc request """
    AppLogger.info('recv forecast request from %s', request.remote_addr)
    return handle_forecast(request)


@app.route("/imputation", methods=['POST'])
def handle_imputation_req():
    """handle the imputation request """
    return handle_imputation(request)


@app.route("/correlation", methods=['POST'])
def handle_correlation_req():
    """handle the correlation request """
    AppLogger.info('recv correlation from %s', request.remote_addr)
    return handle_correlation(request)


@app.route("/tool/batch", methods=['POST'])
def handle_batch_req():
    """handle the batch request request """
    return handle_batch(request)


def parse_args():
    """parse command line arguments"""
    parser = argparse.ArgumentParser(description='TDgpt analytics service')
    parser.add_argument('-c', dest='conf_path', default=None,
                        help='path to configuration file')
    return parser.parse_args()


if __name__ == '__main__':
    arg = parse_args()

    # init configuration
    conf = Configure.init(arg.conf_path)

    # set log parameters
    AppLogger.set_handler(conf.get_log_path())
    AppLogger.set_log_level(conf.get_log_level())

    # register all services
    loader.load_all_service()

    # run HTTP server
    host, port = conf.get_server_bind()
    app.run(host=host, port=port)
