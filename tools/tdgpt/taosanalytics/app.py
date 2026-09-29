# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
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

"""the main route definition for restful service"""

import os
import os.path
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../")

from flask import Flask, request

import taosanalytics
from taosanalytics.conf import Configure
from taosanalytics.handlers.anomaly import handle_anomaly
from taosanalytics.handlers.clustering import handle_clustering
from taosanalytics.handlers.correlation import handle_correlation
from taosanalytics.handlers.forecast import handle_forecast
from taosanalytics.handlers.imputation import handle_imputation
from taosanalytics.handlers.misc import (
    do_profile_search,
    handle_association,
    handle_build_batch_env,
    handle_pearsonr,
)
from taosanalytics.handlers.model_mgmt import (
    do_deploy_dynamic_model,
    do_undeploy_dynamic_model,
)
from taosanalytics.handlers.regression import handle_regression
from taosanalytics.log import AppLogger
from taosanalytics.model_file_mgt import ModelFileManager
from taosanalytics.service_registry import loader
from taosanalytics.tsfm.foundation_model import (
    delete_foundation_model,
    delete_foundation_runtime,
    download_and_start_foundation_model,
    download_foundation_model,
    download_foundation_runtime,
    ensure_pending_deletes_resumed,
    get_model_info,
    init_models_db,
    list_all_tsfm,
    start_foundation_model,
    stop_foundation_model,
)
from taosanalytics.tsfm.response_event import (
    get_all_operations_stream,
    get_operation_status,
)


def _init_app():
    """Initialize configuration, logger, and load services. Called on module import."""
    # Read config path from environment variable or use default
    conf_path = os.environ.get("TDGPT_CONF")

    # Init configuration
    conf = Configure.init(conf_path)

    # Set log parameters
    AppLogger.set_handler(conf.get_log_path())
    AppLogger.set_log_level(conf.get_log_level())

    # Register all services
    loader.register_all_services()

    # Initialize foundation models catalog in database
    init_models_db()

    AppLogger.info("TDgpt service initialized (config: %s)", conf.path)


# Create Flask app
app = Flask(__name__)
app.config["PROPAGATE_EXCEPTIONS"] = True


@app.before_request
def _resume_interrupted_deletes():
    """Re-arm the deletes a service restart interrupted (once per process).

    Done here rather than in a server hook so the recovery stays inside the app,
    which is where the executor queues that run the tasks live and is the only
    place that works for both gunicorn and waitress. Every request calls in, but
    only the first one in each process does any work.
    """
    try:
        ensure_pending_deletes_resumed()
    except Exception as exc:  # recovery is best-effort; never break the request
        AppLogger.error("Failed to resume interrupted model deletes: %s", exc)


@app.route("/")
def index():
    """default rsp"""
    return taosanalytics._ANODE_VER


@app.route("/status")
def server_status():
    """return server status"""
    return {"protocol": 1.0, "status": "ready"}


@app.route("/list")
def list_all_services():
    """
    API function to return all available services, including both fc and anomaly detection
    """
    AppLogger.info("Received request to list all services, ip:%s", request.remote_addr)
    return loader.get_service_list()


@app.route("/models")
def list_all_models():
    """list all available models"""
    AppLogger.info("Received request to list all models, ip:%s", request.remote_addr)
    return ModelFileManager.get_instance().get_model_list()


@app.route("/anomaly-detect", methods=["POST"])
def handle_ad_request():
    """handle the anomaly detection requests"""
    AppLogger.info("recv ad request from %s", request.remote_addr)
    return handle_anomaly(request)


@app.route("/forecast", methods=["POST"])
def handle_forecast_req():
    """handle the fc request"""
    AppLogger.info("recv forecast request from %s", request.remote_addr)
    return handle_forecast(request)


@app.route("/imputation", methods=["POST"])
def handle_imputation_req():
    """handle the imputation request"""
    return handle_imputation(request)


@app.route("/correlation", methods=["POST"])
def handle_correlation_req():
    """handle the correlation request"""
    AppLogger.info("recv correlation from %s", request.remote_addr)
    return handle_correlation(request)


@app.route("/regression", methods=["POST"])
def handle_regression_req():
    """handle the regression request"""
    AppLogger.info("recv regression request from %s", request.remote_addr)
    return handle_regression(request)


@app.route("/clustering", methods=["POST"])
def handle_clustering_req():
    """handle the clustering request"""
    AppLogger.info("recv clustering request from %s", request.remote_addr)
    return handle_clustering(request)


# Keep both routes mapped to this handler so existing clients using the legacy endpoint continue to work, while
# the new integration uses the versioned one.
@app.route("/api/v1/analysis/batch", methods=["POST"])
@app.route("/tool/batch", methods=["POST"])
def handle_batch_req():
    """handle the batch request request"""
    return handle_build_batch_env(request)


@app.route("/deploy", methods=["POST"])
def deploy_model():
    """deploy model to production environment, e.g. load model to memory, etc."""
    return do_deploy_dynamic_model(request)


@app.route("/undeploy", methods=["POST"])
def undeploy_model():
    return do_undeploy_dynamic_model(request)


@app.route("/api/v1/analysis/pearsonr", methods=["POST"])
def handle_pearsonr_req():
    """handle the pearsonr correlation request"""
    AppLogger.info("recv pearsonr correlation request from %s", request.remote_addr)
    return handle_pearsonr(request, api_version="v1")


@app.route("/api/v1/analysis/profile-search", methods=["POST"])
def handle_profile_search_req():
    """handle the profile search request"""
    AppLogger.info("recv profile search request from %s", request.remote_addr)
    return do_profile_search(request, api_version="v1")


@app.route("/api/v1/analysis/association", methods=["POST"])
def handle_association_req():
    """handle the association rule mining request"""
    AppLogger.info("recv association analysis request from %s", request.remote_addr)
    return handle_association(request, api_version="v1")


# Foundation Model Management APIs
@app.route("/api/v1/foundation-models", methods=["GET"])
def handle_list_foundation_models_req():
    """Get list of foundation models with their status"""
    return list_all_tsfm(request)


@app.route("/api/v1/foundation-models/<model_name>", methods=["GET"])
def handle_get_model_info_req(model_name):
    """Get information about a specific foundation model"""
    return get_model_info(request, model_name)

@app.route("/api/v1/foundation-models/download", methods=["POST"])
def handle_download_foundation_model_req():
    """Download a foundation model (async operation)"""
    return download_foundation_model(request)


@app.route("/api/v1/foundation-models/download-start", methods=["POST"])
def handle_download_start_foundation_model_req():
    """Download a foundation model and auto-start it once the download completes (async operation)"""
    return download_and_start_foundation_model(request)


@app.route("/api/v1/foundation-models/start", methods=["POST"])
def handle_start_foundation_model_req():
    """Start a foundation model service (async operation)"""
    return start_foundation_model(request)


@app.route("/api/v1/foundation-models/stop", methods=["POST"])
def handle_stop_foundation_model_req():
    """Stop a foundation model service (async operation)"""
    return stop_foundation_model(request)


@app.route("/api/v1/foundation-models/delete", methods=["POST"])
def handle_delete_foundation_model_req():
    """Delete a foundation model (async operation)"""
    return delete_foundation_model(request)


@app.route("/api/v1/foundation-models/runtime/download", methods=["POST"])
def handle_download_foundation_runtime_req():
    """Install or refresh the shared TSFM runtime (venv) on its own (async operation)"""
    return download_foundation_runtime(request)


@app.route("/api/v1/foundation-models/runtime/delete", methods=["POST"])
def handle_delete_foundation_runtime_req():
    """Destroy the shared TSFM runtime (venv) environment (async operation)"""
    return delete_foundation_runtime(request)


@app.route("/api/v1/foundation-models/<task_id>/status", methods=["GET"])
def handle_get_operation_status_req(task_id):
    """Get status of async operation by task_id"""
    return get_operation_status(request, task_id)


@app.route("/api/v1/foundation-models/status/stream/all", methods=["GET"])
def handle_get_all_operations_stream_req():
    """Stream all foundation model operations status via SSE"""
    return get_all_operations_stream(request)


if __name__ == "__main__":
    # Parse args before initializing so the correct config file is used from
    # the start; services are loaded only once, with the final configuration.
    from taosanalytics.util import parse_args

    args = parse_args()

    if args.conf_path:
        os.environ["TDGPT_CONF"] = args.conf_path

    _init_app()

    # Run development server
    conf = Configure.get_instance()
    host, port = conf.get_server_bind()
    AppLogger.info("Starting development server on %s:%d", host, port)

    app.run(host=host, port=port)
else:
    # Initialize on module import when used as a WSGI module (e.g. gunicorn).
    # When running as __main__, initialization is deferred until after argument
    # parsing so that a -c/--config path is applied before services are loaded.
    _init_app()
