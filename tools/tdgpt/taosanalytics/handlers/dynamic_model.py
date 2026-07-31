import json
import os
import re
import joblib
from pathlib import Path
from taosanalytics.conf import Configure
from taosanalytics.exception import NotFoundDynamicModelError
from taosanalytics.log import AppLogger
from taosanalytics.service_registry import loader


MODEL_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")


def _is_valid_model_name(model_name):
    # Allow a conservative filename subset and block traversal-like names.
    return isinstance(model_name, str) and model_name not in {".", ".."} and bool(MODEL_NAME_PATTERN.fullmatch(model_name))


def _extract_request_payload(request):
    """Extract and validate payload from multipart or JSON request.

    Returns:
        (payload, model_file, model_config) tuple, or (None, None, None) on error with HTTP response
    """
    payload = None
    model_file = None

    if request.method == 'POST' and 'multipart/form-data' in request.content_type:
        # Handle multipart form data
        config_str = request.form.get('config')
        if config_str:
            try:
                payload = json.loads(config_str)
            except json.JSONDecodeError as e:
                AppLogger.error("deploy request invalid config JSON, ip:%s, error:%s", request.remote_addr, str(e))
                return None, None, None, ({
                    'status': 'error',
                    'error': "Invalid config JSON in multipart request"
                }, 400)

        model_file = request.files.get('model_binary')
        model_config = payload
    else:
        # Handle JSON body
        payload = request.get_json(silent=True) or {}

        if not payload:
            AppLogger.error("deploy request missing payload, ip:%s", request.remote_addr)
            return None, None, None, ({
                'status': 'error',
                'error': "Missing payload in request"
            }, 400)

        model_config = payload.get("config")
        if model_config is None:
            AppLogger.error("deploy request missing required fields, payload:%s, ip:%s", payload, request.remote_addr)
            return None, None, None, ({
                'status': 'error',
                'error': "Missing required fields in request payload, required: model_name and config"
            }, 400)

    return payload, model_file, model_config, None


def _validate_and_prepare_model_directory(raw_model_name, request_addr):
    """Validate model name and prepare directory paths.

    Returns:
        (config_file_path, pkl_file_path) or (None, None, error_response) on failure
    """
    if not _is_valid_model_name(raw_model_name):
        AppLogger.error("deploy request invalid model_name, model_name:%s, ip:%s", raw_model_name, request_addr)
        return None, None, ({
            'status': 'error',
            'error': "Invalid model_name in request payload"
        }, 400)

    model_dir = Configure.get_instance().get_dynamic_model_directory()
    os.makedirs(model_dir, exist_ok=True)

    config_file_name = raw_model_name + '.json'
    config_file_path = str(os.path.join(model_dir, config_file_name))

    # Check if model already exists
    if Path(config_file_path).exists():
        AppLogger.error("model with name %s already exists", raw_model_name)
        return None, None, ({
            'status': 'error',
            'error': f"Model with name {raw_model_name} already exists"
        }, 400)

    pkl_file_path = None
    return config_file_path, pkl_file_path, None


def _save_model_files_and_validate(raw_model_name, config_file_path, model_config, model_file):
    """Save configuration and pkl files, then validate pkl format.

    Returns:
        (pkl_file_path, error_response) where error_response is None on success
    """
    pkl_file_path = None

    # Update model_path in config if pkl file is provided
    if model_file:
        model_dir = os.path.dirname(config_file_path)
        pkl_file_name = raw_model_name + '.pkl'
        pkl_file_path = str(os.path.join(model_dir, pkl_file_name))
        model_config['model_path'] = pkl_file_path

    # Save configuration file
    try:
        with open(config_file_path, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(model_config))
        AppLogger.info("Model %s configuration saved to %s successfully", raw_model_name, config_file_path)
    except Exception as e:
        AppLogger.error("Error saving model %s configuration to file: %s", raw_model_name, str(e))
        try:
            if Path(config_file_path).exists():
                os.unlink(config_file_path)
        except Exception:
            pass
        return None, ({'status': 'error','error': f"Error saving model {raw_model_name} configuration: {str(e)}"}, 500)

    # Save pkl file if provided
    if model_file and pkl_file_path:
        try:
            model_file.save(pkl_file_path)
            AppLogger.info("Model %s pkl file saved to %s successfully", raw_model_name, pkl_file_path)
        except Exception as e:
            AppLogger.error("Error saving model %s pkl file: %s", raw_model_name, str(e))
            try:
                if Path(config_file_path).exists():
                    os.unlink(config_file_path)
                if Path(pkl_file_path).exists():
                    os.unlink(pkl_file_path)
            except Exception:
                pass
            return None, ({
                'status': 'error',
                'error': f"Error saving model {raw_model_name} pkl file: {str(e)}"
            }, 500)

        # Verify pkl file is readable and contains valid format
        try:
            data = joblib.load(pkl_file_path)

            # Validate format: either direct model or dict with 'model' key
            if isinstance(data, dict) and 'model' in data:
                model = data['model']
                # Accept any sklearn model (IsolationForest, OneClassSVM, etc.)
                if not hasattr(model, 'predict'):
                    raise ValueError(f"pkl file contains non-model object of type: {type(model).__name__}")
            elif hasattr(data, 'predict'):
                # Direct model object
                pass
            else:
                raise ValueError(f"pkl file must contain a model or dict with 'model' key, got: {type(data).__name__}")

            AppLogger.info("Model %s pkl file verified successfully", raw_model_name)
        except Exception as e:
            AppLogger.error("Error verifying model %s pkl file: %s", raw_model_name, str(e))
            try:
                if Path(config_file_path).exists():
                    os.unlink(config_file_path)
                if Path(pkl_file_path).exists():
                    os.unlink(pkl_file_path)
            except Exception:
                pass
            return None, ({
                'status': 'error',
                'error': f"Invalid model pkl file: {str(e)}"
            }, 400)

    return pkl_file_path, None


def do_handle_dynamic_model(request):
    """
    Handle dynamic model operations, e.g. load model to memory, warm up model, etc.
    Supports both JSON-only and multipart/form-data requests:
    
    JSON-only (best_params):
    {
        "model_name": "sample_ad_model_test",
        "config": {
            "algo": "iforest",
            "best_params": {"n_estimators": 10, "contamination": 0.05}
        }
    }

    Multipart (with pkl file):
    POST /deploy with:
      - config: JSON string with algo, best_params (optional), model_path
      - model: binary pkl file (optional)

    Args:
        request: The incoming request object containing the model deployment information.

    Returns:
        A result based on the handling of the dynamic model.
    """

    # Step 1: Extract and validate payload
    payload, model_file, model_config, error_response = _extract_request_payload(request)
    if error_response:
        return error_response

    AppLogger.info('recv deploy request, payload:%s, ip:%s', payload, request.remote_addr)

    if "model_name" not in payload:
        AppLogger.error("deploy request missing required fields, payload:%s, ip:%s", payload, request.remote_addr)
        return {
            'status': 'error',
            'error': "Missing required fields in request payload, required: model_name and config"
        }, 400

    raw_model_name = payload.get("model_name")

    # Step 2: Validate model name and prepare directories
    config_file_path, _, error_response = _validate_and_prepare_model_directory(raw_model_name, request.remote_addr)
    if error_response:
        return error_response

    AppLogger.debug("deploy model with name %s, config:%s", raw_model_name, model_config)

    # Step 3: Save model files and validate pkl
    pkl_file_path, error_response = _save_model_files_and_validate(raw_model_name, config_file_path, model_config, model_file)
    if error_response:
        return error_response

    # Step 4: Register service
    try:
        loader.register_service_from_file(config_file_path)
        AppLogger.info("Model %s deployed successfully", raw_model_name)
    except Exception as e:
        # Check if another worker already registered this model via sync_dynamic_services
        if raw_model_name in loader.services:
            AppLogger.info("Model %s was already registered by another worker, treating as success", raw_model_name)
        else:
            AppLogger.error("Error deploying dynamic model:%s, remove files, error:%s", raw_model_name, str(e))
            try:
                if Path(config_file_path).exists():
                    os.unlink(config_file_path)
                if pkl_file_path and Path(pkl_file_path).exists():
                    os.unlink(pkl_file_path)
            except Exception:
                pass

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

    model_dir = Configure.get_instance().get_dynamic_model_directory()
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

    config_file_name = model_name + '.json'
    config_file_path = os.path.join(model_dir, config_file_name)
    pkl_file_name = model_name + '.pkl'
    pkl_file_path = os.path.join(model_dir, pkl_file_name)

    try:
        loader.unregister_dynamic_service(model_name)

        # Remove config file
        if Path(str(config_file_path)).exists():
            try:
                os.remove(config_file_path)
            except FileNotFoundError:
                # Another worker removed the file between exists() and remove()
                AppLogger.warning("Model %s config file was already removed by another worker during undeploy", model_name)
        else:
            AppLogger.warning("Model configuration file for model %s not found during undeploy, maybe already removed", model_name)

        # Remove pkl file if it exists
        if Path(str(pkl_file_path)).exists():
            try:
                os.remove(pkl_file_path)
                AppLogger.info("Model %s pkl file removed successfully", model_name)
            except FileNotFoundError:
                # Another worker removed the file between exists() and remove()
                AppLogger.warning("Model %s pkl file was already removed by another worker during undeploy", model_name)
            except Exception as cleanup_error:
                AppLogger.error("Error removing model %s pkl file during undeploy: %s",
                              model_name, str(cleanup_error))

        AppLogger.info("Model %s configuration file is removed successfully", model_name)
        return {
            'status': 'success',
            'message': f"Model {model_name} undeployed successfully"
        }, 200
    except Exception as e:
        if isinstance(e, NotFoundDynamicModelError):
            if Path(str(config_file_path)).exists():
                try:
                    os.remove(config_file_path)
                    AppLogger.warning(
                        "Model %s not found in memory during undeploy, but config file existed and was removed",
                        model_name
                    )
                except FileNotFoundError:
                    # Another worker already removed the file between the exists() check
                    # and the remove() call — the model is already undeployed, treat as success.
                    AppLogger.warning(
                        "Model %s config file was already removed by another worker during undeploy",
                        model_name
                    )
                except Exception as cleanup_error:
                    AppLogger.error("Error removing model %s config file during undeploy: %s",
                                    model_name, str(cleanup_error))
                    return {
                        'status': 'error',
                        'error': f"Error undeploying model {model_name}: {str(cleanup_error)}"
                    }, 500

            # Remove pkl file if it exists
            if Path(str(pkl_file_path)).exists():
                try:
                    os.remove(pkl_file_path)
                    AppLogger.info("Model %s pkl file removed successfully during undeploy", model_name)
                except FileNotFoundError:
                    AppLogger.warning("Model %s pkl file was already removed by another worker during undeploy", model_name)
                except Exception as cleanup_error:
                    AppLogger.error("Error removing model %s pkl file during undeploy: %s",
                                  model_name, str(cleanup_error))

                return {
                    'status': 'success',
                    'message': f"Model {model_name} undeployed successfully"
                }, 200

            AppLogger.warning("Model %s not found during undeploy, maybe already undeployed", model_name)
            return {
                'status': 'error',
                'error': f"Model {model_name} not found for undeployment"
            }, 404

        AppLogger.error("Error undeploying model %s: %s", model_name, str(e))
        return {
            'status': 'error',
            'error': f"Error undeploying model {model_name}: {str(e)}"
        }, 500
