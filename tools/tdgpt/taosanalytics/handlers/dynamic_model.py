import os
from pathlib import Path

from taosanalytics.conf import Configure
from taosanalytics.log import AppLogger
from taosanalytics.service_registry import loader


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

    AppLogger.info('recv deploy request, ip:%s', request.remote_addr)
    model_dir = Configure.get_instance().get_model_directory()

    payload = request.get_json(silent=True) or {}
    if not payload:
        AppLogger.error("deploy request missing JSON payload, ip:%s", request.remote_addr)
        return {
            'status': 'error',
            'error': "Missing JSON payload in request"
        }, 400

    if not {"model_name", "config"}.issubset(payload.keys()):
        AppLogger.error("deploy request missing required fields, ip:%s, payload:%s", request.remote_addr, payload)
        return {
            'status': 'error',
            'error': "Missing required fields in request payload, required: model_name and config"
        }, 400

    model_name = payload.get("model_name")
    model_config = payload.get("config")

    AppLogger.debug("deploy model with name %s, config:%s", model_name, model_config)

    full_path = str(os.path.join(model_dir, model_name))

    # check for valid model name, e.g. check if model file exists, etc.
    if Path(full_path).exists():
        AppLogger.error("model with name %s already exists", model_name)
        return {
            'status': 'error',
            'error': f"Model with name {model_name} already exists"
        }, 400

    try:
        with open(full_path, "w", encoding="utf-8") as handle:
            handle.write(model_config)
        AppLogger.info("Model %s saved to %s successfully", model_name, full_path)

        loader.register_service_from_file(full_path)
        AppLogger.info("Model %s deployed successfully", model_name)
    except Exception as e:
        AppLogger.error("Error deploying dynamic model %s: %s", model_name, str(e))
        return {
            'status': 'error',
            'error': f"Error deploying model {model_name}: {str(e)}"
        }, 500

    return {
        'status': 'success',
        'message': f"Model {model_name} deployed successfully"
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
    full_path = os.path.join(model_dir, model_name)

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
