import json
import os
import re
from pathlib import Path

import joblib

from taosanalytics.conf import Configure
from taosanalytics.exception import NotFoundDynamicModelError
from taosanalytics.log import AppLogger
from taosanalytics.service_registry import loader
from taosanalytics.util import safely_remove_directory, safely_remove_file

MODEL_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")


def _is_valid_model_name(model_name):
    # Allow a conservative filename subset and block traversal-like names.
    return (
        isinstance(model_name, str)
        and model_name not in {".", ".."}
        and bool(MODEL_NAME_PATTERN.fullmatch(model_name))
    )


def _extract_request_payload(request):
    """Extract and validate payload from multipart or JSON request.

    Returns:
        (payload, model_file, model_config) tuple, or (None, None, None) on error with HTTP response
    """
    payload = None
    model_file = None

    if request.method == "POST" and "multipart/form-data" in request.content_type:
        # Handle multipart form data
        config_str = request.form.get("config")
        if config_str:
            try:
                payload = json.loads(config_str)
            except json.JSONDecodeError as e:
                AppLogger.error(
                    "deploy request invalid config JSON, ip:%s, error:%s",
                    request.remote_addr,
                    str(e),
                )
                return (
                    None,
                    None,
                    None,
                    (
                        {
                            "status": "error",
                            "error": "Invalid config JSON in multipart request",
                        },
                        400,
                    ),
                )

        model_file = request.files.get("model_binary")
        model_config = payload
    else:
        # Handle JSON body
        payload = request.get_json(silent=True) or {}

        if not payload:
            AppLogger.error(
                "deploy request missing payload, ip:%s", request.remote_addr
            )
            return (
                None,
                None,
                None,
                ({"status": "error", "error": "Missing payload in request"}, 400),
            )

        model_config = payload.get("config")
        if model_config is None:
            AppLogger.error(
                "deploy request missing required fields, payload:%s, ip:%s",
                payload,
                request.remote_addr,
            )
            return (
                None,
                None,
                None,
                (
                    {
                        "status": "error",
                        "error": "Missing required fields in request payload, required: model_name and config",
                    },
                    400,
                ),
            )

    return payload, model_file, model_config, None


def _validate_and_prepare_model_directory(raw_model_name, request_addr):
    """Validate model name and prepare directory paths.

    Returns:
        (model_subdir, config_file_path, pkl_file_path) or (None, None, None, error_response) on failure
    """
    if not _is_valid_model_name(raw_model_name):
        AppLogger.error(
            "deploy request invalid model_name, model_name:%s, ip:%s",
            raw_model_name,
            request_addr,
        )
        return (
            None,
            None,
            None,
            (
                {"status": "error", "error": "Invalid model_name in request payload"},
                400,
            ),
        )

    base_model_dir = Configure.get_instance().get_dynamic_model_directory()
    os.makedirs(base_model_dir, exist_ok=True)

    model_subdir = str(os.path.join(base_model_dir, raw_model_name))

    # Check if model already exists
    if Path(model_subdir).exists():
        AppLogger.error("model with name %s already exists", raw_model_name)
        return (
            None,
            None,
            None,
            (
                {
                    "status": "error",
                    "error": f"Model with name {raw_model_name} already exists",
                },
                400,
            ),
        )

    # Create model-specific subdirectory
    os.makedirs(model_subdir, exist_ok=True)

    config_file_name = raw_model_name + ".json"
    config_file_path = str(os.path.join(model_subdir, config_file_name))

    pkl_file_path = None
    return model_subdir, config_file_path, pkl_file_path, None


def _save_model_files_and_validate(
    raw_model_name, config_file_path, model_config, model_file
):
    """Save configuration and pkl files, then validate pkl format.

    Returns:
        (error_response) where error_response is None on success
    """
    pkl_file_path = None
    model_dir = os.path.dirname(config_file_path)

    # Update model_path in config if pkl file is provided
    if model_file:
        pkl_file_name = raw_model_name + ".pkl"
        pkl_file_path = str(os.path.join(model_dir, pkl_file_name))
        model_config["model_path"] = pkl_file_path

    # Save configuration file
    try:
        with open(config_file_path, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(model_config))
        AppLogger.info(
            "Model %s configuration saved to %s successfully",
            raw_model_name,
            config_file_path,
        )
    except Exception as e:
        AppLogger.error(
            "Error saving model %s configuration to file: %s", raw_model_name, str(e)
        )
        safely_remove_file(config_file_path, raw_model_name, "config file")
        return (
            {
                "status": "error",
                "error": f"Error saving model {raw_model_name} configuration: {e!s}",
            },
            500,
        )

    # Save pkl file if provided
    if model_file and pkl_file_path:
        try:
            model_file.save(pkl_file_path)
            AppLogger.info(
                "Model %s pkl file saved to %s successfully",
                raw_model_name,
                pkl_file_path,
            )
        except Exception as e:
            AppLogger.error(
                "Error saving model %s pkl file: %s", raw_model_name, str(e)
            )
            safely_remove_file(config_file_path, raw_model_name, "config file")
            safely_remove_file(pkl_file_path, raw_model_name, "pkl file")
            return (
                {
                    "status": "error",
                    "error": f"Error saving model {raw_model_name} pkl file: {e!s}",
                },
                500,
            )

        # Verify pkl file is readable and contains valid format
        try:
            data = joblib.load(pkl_file_path)

            # Validate format: either direct model or dict with 'model' key
            if isinstance(data, dict) and "model" in data:
                model = data["model"]
                # Accept any sklearn model (IsolationForest, OneClassSVM, etc.)
                if not hasattr(model, "predict"):
                    raise ValueError(
                        f"pkl file contains non-model object of type: {type(model).__name__}"
                    )
            elif hasattr(data, "predict"):
                # Direct model object
                pass
            else:
                raise ValueError(
                    f"pkl file must contain a model or dict with 'model' key, got: {type(data).__name__}"
                )

            AppLogger.info("Model %s pkl file verified successfully", raw_model_name)
        except Exception as e:
            AppLogger.error(
                "Error verifying model %s pkl file: %s", raw_model_name, str(e)
            )
            safely_remove_file(config_file_path, raw_model_name, "config file")
            safely_remove_file(pkl_file_path, raw_model_name, "pkl file")
            return (
                {"status": "error", "error": f"Invalid model pkl file: {e!s}"},
                400,
            )

    return None


def do_deploy_dynamic_model(request):
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

    AppLogger.info(
        "recv deploy request, payload:%s, ip:%s", payload, request.remote_addr
    )

    if "model_name" not in payload:
        AppLogger.error(
            "deploy request missing required fields, payload:%s, ip:%s",
            payload,
            request.remote_addr,
        )
        return {
            "status": "error",
            "error": "Missing required fields in request payload, required: model_name and config",
        }, 400

    raw_model_name = payload.get("model_name")

    # Step 2: Validate model name and prepare directories
    model_subdir, config_file_path, _, error_response = _validate_and_prepare_model_directory(
        raw_model_name, request.remote_addr
    )
    if error_response:
        return error_response

    AppLogger.debug(
        "deploy model with name %s, config:%s", raw_model_name, model_config
    )

    # Step 3: Save model files and validate pkl
    error_response = _save_model_files_and_validate(
        raw_model_name, config_file_path, model_config, model_file
    )
    if error_response:
        # Clean up the orphaned directory when file validation fails
        safely_remove_directory(model_subdir, raw_model_name)
        return error_response

    # Step 4: Register service
    try:
        loader.register_service_from_file(config_file_path)
        AppLogger.info("Model %s deployed successfully", raw_model_name)
    except Exception as e:
        # Check if another worker already registered this model via sync_dynamic_services
        if raw_model_name in loader.services:
            AppLogger.info(
                "Model %s was already registered by another worker, treating as success",
                raw_model_name,
            )
        else:
            AppLogger.error(
                "Error deploying dynamic model:%s, remove files, error:%s",
                raw_model_name,
                str(e),
            )
            safely_remove_directory(model_subdir, raw_model_name)

            return {
                "status": "error",
                "error": f"Error deploying model {raw_model_name}: {e!s}",
            }, 400

    return {
        "status": "success",
        "message": f"Model {raw_model_name} deployed successfully",
    }, 200


def do_undeploy_dynamic_model(request):
    """undeploy model from production environment, e.g. release model from memory, etc."""
    AppLogger.debug("recv undeploy request, ip:%s", request.remote_addr)

    base_model_dir = Configure.get_instance().get_dynamic_model_directory()
    payload = request.get_json(silent=True) or {}
    if not payload:
        AppLogger.error(
            "undeploy request missing JSON payload, ip:%s", request.remote_addr
        )
        return {"status": "error", "error": "Missing JSON payload in request"}, 400
    if "model_name" not in payload:
        AppLogger.error(
            "undeploy request missing model_name field, ip:%s, payload:%s",
            request.remote_addr,
            payload,
        )
        return {
            "status": "error",
            "error": "Missing required field model_name in request payload",
        }, 400

    model_name = payload.get("model_name")
    if not _is_valid_model_name(model_name):
        AppLogger.error(
            "undeploy request invalid model_name, ip:%s, model_name:%s",
            request.remote_addr,
            model_name,
        )
        return {
            "status": "error",
            "error": "Invalid model_name in request payload",
        }, 400

    model_subdir = str(os.path.join(base_model_dir, model_name))

    # For backward compatibility: also try legacy flat structure (model_name.json directly in model_dir)
    legacy_config_file_path = os.path.join(base_model_dir, model_name + ".json")
    legacy_pkl_file_path = os.path.join(base_model_dir, model_name + ".pkl")

    try:
        loader.unregister_dynamic_service(model_name)

        # Try to remove new subdirectory structure first
        dir_removed = safely_remove_directory(model_subdir, model_name)

        # Clean up legacy flat structure files if they exist (backward compatibility)
        safely_remove_file(legacy_config_file_path, model_name, "legacy config file")
        safely_remove_file(legacy_pkl_file_path, model_name, "legacy pkl file")

        # Check if anything needed to be cleaned up
        dir_existed = Path(model_subdir).exists()
        legacy_existed = Path(legacy_config_file_path).exists() or Path(legacy_pkl_file_path).exists()

        if (dir_existed or legacy_existed) and not dir_removed:
            # Directory existed but could not be removed (permission denied, etc.)
            AppLogger.error(
                "Model %s directory cleanup failed after unregister",
                model_name,
            )
            return {
                "status": "error",
                "error": f"Error undeploying model {model_name}: failed to remove directory",
            }, 500

        AppLogger.info("Model %s is removed successfully", model_name)
        return {
            "status": "success",
            "message": f"Model {model_name} undeployed successfully",
        }, 200
    except Exception as e:
        if isinstance(e, NotFoundDynamicModelError):
            # Model not found in memory, but check if directory exists on disk
            AppLogger.warning(
                "Model %s not found in memory during undeploy, attempting to clean up directory",
                model_name,
            )
            dir_existed = Path(model_subdir).exists()
            legacy_existed = Path(legacy_config_file_path).exists() or Path(legacy_pkl_file_path).exists()

            dir_removed = safely_remove_directory(model_subdir, model_name)

            # Clean up legacy flat structure files if they exist (backward compatibility)
            safely_remove_file(legacy_config_file_path, model_name, "legacy config file")
            safely_remove_file(legacy_pkl_file_path, model_name, "legacy pkl file")

            if dir_existed and not dir_removed:
                # Directory existed but could not be removed (permission denied, etc.)
                AppLogger.error(
                    "Model %s directory cleanup failed during undeploy",
                    model_name,
                )
                return {
                    "status": "error",
                    "error": f"Error undeploying model {model_name}: failed to remove directory",
                }, 500

            if dir_existed and dir_removed:
                # Directory was found and cleaned up; likely a race condition with another worker
                AppLogger.info(
                    "Model %s directory was cleaned up during undeploy; possibly undeployed by another worker",
                    model_name,
                )
                return {
                    "status": "success",
                    "message": f"Model {model_name} undeployed successfully",
                }, 200
            elif legacy_existed:
                # No directory found, but legacy flat-structure files were cleaned up
                AppLogger.info(
                    "Model %s legacy files were cleaned up during undeploy",
                    model_name,
                )
                return {
                    "status": "success",
                    "message": f"Model {model_name} undeployed successfully",
                }, 200
            else:
                # No directory or files found; model never existed
                AppLogger.warning(
                    "Model %s not found in memory or on disk during undeploy",
                    model_name,
                )
                return {
                    "status": "error",
                    "error": f"Model {model_name} not found",
                }, 404

        AppLogger.error("Error undeploying model %s: %s", model_name, str(e))
        return {
            "status": "error",
            "error": f"Error undeploying model {model_name}: {e!s}",
        }, 500
