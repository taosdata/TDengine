import json
import os
import re
from pathlib import Path

from taosanalytics.conf import Configure
from taosanalytics.log import AppLogger
from taosanalytics.service_registry import loader


def _is_valid_model_name(model_name):
    MODEL_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
    # Allow a conservative filename subset and block traversal-like names.
    return isinstance(model_name, str) and model_name not in {".", ".."} and bool(MODEL_NAME_PATTERN.fullmatch(model_name))


def do_handle_dynamic_model(request):
    """
    Handle dynamic model operations, e.g. load model to memory, warm up model, etc.
    {
    "model_name": "sample_ad_model",
    "config": {
        "algo": "arima",
        "best_params": {
            "p": 3,
        }
    }

    Args:
        request: The incoming request object containing the model deployment information.

    Returns:
        A result based on the handling of the dynamic model.
    """

    payload = request.get_json(silent=True) or {}
    AppLogger.info('recv deploy request, payload:%s, ip:%s', payload, request.remote_addr)

    if not payload:
        AppLogger.error("deploy request missing JSON payload, ip:%s", request.remote_addr)
        return {
            'status': 'error',
            'error': "Missing JSON payload in request"
        }, 400

    if not {"model_name", "config"}.issubset(payload.keys()):
        AppLogger.error("deploy request missing required fields, payload:%s, ip:%s", payload, request.remote_addr)
        return {
            'status': 'error',
            'error': "Missing required fields in request payload, required: model_name and config"
        }, 400

    raw_model_name = payload.get("model_name")
    if not _is_valid_model_name(raw_model_name):
        AppLogger.error("deploy request invalid model_name, model_name:%s, ip:%s", raw_model_name, request.remote_addr)
        return {
            'status': 'error',
            'error': "Invalid model_name in request payload"
        }, 400

    model_file_name = raw_model_name + '.json'
    model_config = payload.get("config")

    AppLogger.debug("deploy model with name %s, config:%s", raw_model_name, model_config)

    model_dir = Configure.get_instance().get_model_directory()
    full_path = str(os.path.join(model_dir, model_file_name))

    # check for valid model name, e.g. check if model file exists, etc.
    if Path(full_path).exists():
        AppLogger.error("model with name %s already exists", raw_model_name)
        return {
            'status': 'error',
            'error': f"Model with name {raw_model_name} already exists"
        }, 400

    try:
        with open(full_path, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(model_config))
        AppLogger.info("Model %s saved to %s successfully", raw_model_name, full_path)
    except Exception as e:
        AppLogger.error("Error saving model %s configuration to file: %s", raw_model_name, str(e))
        return {
            'status': 'error',
            'error': f"Error saving model {raw_model_name} configuration: {str(e)}"
        }, 500

    try:
        loader.register_service_from_file(full_path)
        AppLogger.info("Model %s deployed successfully", raw_model_name)
    except Exception as e:
        AppLogger.error("Error deploying dynamic model %s: %s", raw_model_name, str(e))
        return {
            'status': 'error',
            'error': f"Error deploying model {raw_model_name}: {str(e)}"
        }, 400

    return {
        'status': 'success',
        'message': f"Model {raw_model_name} deployed successfully"
    }, 200


def do_handle_undeploy_model(request):
    """undeploy model from production environment, e.g. release model from memory, etc."""
    AppLogger.debug("recv undeploy request, ip:%s", request.remote_addr)
    model_dir = Configure.get_instance().get_model_directory()
    payload = request.get_json(silent=True) or {}
    if not payload:
        AppLogger.error("undeploy request missing JSON payload, ip:%s", request.remote_addr)
        return {
            'status': 'error',
            'error': "Missing JSON payload in request"
        }, 400
    if "model_name" not in payload:
        AppLogger.error("undeploy request missing model_name field, ip:%s, payload:%s", request.remote_addr, payload)
        return {
            'status': 'error',
            'error': "Missing required field model_name in request payload"
        }, 400

    model_name = payload.get("model_name")
    if not _is_valid_model_name(model_name):
        AppLogger.error("undeploy request invalid model_name, ip:%s, model_name:%s", request.remote_addr, model_name)
        return {
            'status': 'error',
            'error': "Invalid model_name in request payload"
        }, 400

    model_file_name = model_name + '.json'
    full_path = os.path.join(model_dir, model_file_name)

    try:
        if Path(str(full_path)).exists():
            os.remove(full_path)
            AppLogger.info("Model %s configuration file is removed successfully", model_name)

            loader.unregister_dynamic_service(model_name)

            return {
                'status': 'success',
                'message': f"Model {model_name} undeployed successfully"
            }, 200
        else:
            AppLogger.error("Model %s not found for undeploy", model_name)
            return {
                'status': 'error',
                'error': f"Model {model_name} not found"
            }, 404
    except Exception as e:
        AppLogger.error("Error undeploying model %s: %s", model_name, str(e))
        return {
            'status': 'error',
            'error': f"Error undeploying model {model_name}: {str(e)}"
        }, 500
