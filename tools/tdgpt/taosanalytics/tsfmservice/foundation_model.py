"""Handlers for foundation model operations (list, download, start, delete).

Currently uses mock implementations. Can be extended to integrate with
ResourceManager for real downloads and taosanode_service for process management.
"""

import json
import subprocess
import sys
import time
import uuid
from enum import Enum
from multiprocessing import Pool
from pathlib import Path

from taosanalytics.conf import Configure
from taosanalytics.log import AppLogger
from taosanalytics.tsfmservice.foundation_models_db import (
    AsyncOperation,
    FoundationModel,
    get_dao,
)


class TaskStatus(Enum):
    """Task execution status states."""

    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    INTERRUPTED = "interrupted"


class OperationType(Enum):
    """Foundation model operation types."""

    DOWNLOAD = "download"
    START = "start"
    STOP = "stop"
    DELETE = "delete"


# Global process pools for async operations — lazily initialized to avoid
# re-creating Pool inside a forked worker process (gunicorn/multiprocessing).
# Separate pools prevent blocking between long-running downloads and responsive service operations.
_DOWNLOAD_POOL = None
_SERVICE_POOL = None
_MODELS_INITIALIZED = False


def init_models_db():
    """Initialize models database with built-in model catalog.

    Called once at startup to populate the database with default models.
    After this, all model operations go through DAO for multi-process consistency.
    """
    global _MODELS_INITIALIZED
    if _MODELS_INITIALIZED:
        return

    _DEFAULT_MODELS = [
        FoundationModel(
            name="chronos",
            model_id="amazon/chronos-bolt-base",
            package="tdengine-tdgpt-model-chronos-bolt-base.zip",
            type="forecast",
            description="Amazon Chronos Bolt Base forecasting model",
            size_gb=2.5,
        ),
        FoundationModel(
            name="moirai",
            model_id="Salesforce/moirai-moe-1.0-R-small",
            package="tdengine-tdgpt-model-moirai-moe-1-0-r-small.zip",
            type="forecast",
            description="Salesforce Moirai MoE 1.0 R-Small forecasting model",
            size_gb=1.8,
        ),
        FoundationModel(
            name="moment",
            model_id="AutonLab/MOMENT-1-base",
            package="tdengine-tdgpt-model-moment-1-base.zip",
            type="forecast",
            description="AutonLab MOMENT-1 Base foundation model",
            size_gb=1.2,
        ),
        FoundationModel(
            name="tdtsfm",
            model_id="tdengine/tdtsfm-v1.0",
            package="tdengine-tdgpt-model-tdtsfm-v1-0.zip",
            type="forecast",
            description="TDengine TSFM v1.0 time series forecasting model",
            size_gb=3.5,
        ),
        FoundationModel(
            name="timemoe",
            model_id="Maple728/TimeMoE-200M",
            package="tdengine-tdgpt-model-timemoe-200m.zip",
            type="forecast",
            description="Maple TimeMoE 200M time series model",
            size_gb=0.8,
        ),
        FoundationModel(
            name="timesfm",
            model_id="google/timesfm-2.5-200m-pytorch",
            package="tdengine-tdgpt-model-timesfm-2-5-200m-pytorch.zip",
            type="forecast",
            description="Google TimesFM 2.5 200M PyTorch time series model",
            size_gb=0.9,
        ),
    ]

    dao = get_dao()
    for model in _DEFAULT_MODELS:
        if not dao.get_model(model.name):
            dao.create_model(model)
            AppLogger.info("Initialized model in database: %s", model.name)

    # On startup, any task still in 'running' state means the service crashed mid-task
    dao.fail_stale_running_operations()

    cleaned = dao.cleanup_old_operations(days=1)
    if cleaned:
        AppLogger.info("Cleaned up %d stale async operations (>1 day)", cleaned)

    _MODELS_INITIALIZED = True
    AppLogger.info("Models database initialization complete")


def _get_download_pool():
    """Return the download process pool, creating it on first use."""
    global _DOWNLOAD_POOL
    if _DOWNLOAD_POOL is None:
        AppLogger.info("Initializing download pool with 1 worker")
        _DOWNLOAD_POOL = Pool(processes=1)
        AppLogger.info("Download pool initialized: %s", _DOWNLOAD_POOL)
    return _DOWNLOAD_POOL


def _get_service_pool():
    """Return the service process pool, creating it on first use.

    Service operations (start, stop, delete) are handled separately from downloads
    to prevent long download operations from blocking responsive service actions.
    """
    global _SERVICE_POOL
    if _SERVICE_POOL is None:
        AppLogger.info("Initializing service pool with 1 worker")
        _SERVICE_POOL = Pool(processes=1)
        AppLogger.info("Service pool initialized: %s", _SERVICE_POOL)
    return _SERVICE_POOL


def _update_model_state(model_name, downloaded=None, running=None):
    """Update model state in database.

    Args:
        model_name: Name of the model to update
        downloaded: Optional new value for downloaded flag
        running: Optional new value for running flag
    """
    dao = get_dao()
    success = dao.update_model(model_name, downloaded=downloaded, running=running)

    if success:
        AppLogger.info(
            "Updated model '%s' state: downloaded=%s, running=%s",
            model_name,
            downloaded,
            running,
        )
    else:
        AppLogger.warning("Model '%s' not found for state update", model_name)


def _complete_async_operation(task_id, model_name, operation, status, error=None):
    """Mark an async operation as completed or failed, and update model state.

    Args:
        task_id: Unique task identifier
        model_name: Name of the model being operated on
        operation: Operation type (download, start, stop, delete)
        status: Final status (TaskStatus enum value)
        error: Optional error message (for failed status)
    """
    dao = get_dao()
    operation_info = dao.get_async_operation(task_id)
    if not operation_info:
        AppLogger.warning("Task %s not found for completion update", task_id)
        return

    now = time.time()
    dao.update_async_operation(
        task_id,
        status=status,
        progress=100,
        completed_at=now,
        error=error if error else None
    )

    duration = now - operation_info.created_at
    AppLogger.info(
        "Task %s (%s on %s) completed with status=%s (duration=%.2fs)",
        task_id,
        operation,
        model_name,
        status,
        duration,
    )

    # Update model state based on operation and result
    if status == TaskStatus.COMPLETED.value:
        if operation == OperationType.DOWNLOAD.value:
            _update_model_state(model_name, downloaded=True)
        elif operation == OperationType.START.value:
            _update_model_state(model_name, running=True)
        elif operation == OperationType.STOP.value:
            _update_model_state(model_name, running=False)
        elif operation == OperationType.DELETE.value:
            _update_model_state(model_name, downloaded=False, running=False)

    dao.cleanup_old_operations(days=1)


def _get_taosanodectl_cmd():
    """Return the command prefix for invoking taosanodectl.

    Prefers the system-installed taosanodectl binary. Falls back to:
    1. Production layout: taosanode/bin/taosanodectl.py
    2. Development layout: tdgpt/script/taosanodectl.py

    Returns:
        list: Command prefix, e.g. ["taosanodectl"] or [sys.executable, "/path/to/taosanodectl.py"]

    Raises:
        FileNotFoundError: If neither the system binary nor the script is found
    """
    import shutil
    if shutil.which("taosanodectl"):
        return ["taosanodectl"]

    # Production layout: taosanode/bin/taosanodectl.py
    script_dir = Path(__file__).resolve().parent.parent.parent.parent / "bin"
    script = script_dir / "taosanodectl.py"
    if script.exists():
        return [sys.executable, str(script)]

    # Development layout: tdgpt/script/taosanodectl.py
    dev_script = Path(__file__).resolve().parent.parent.parent / "script" / "taosanodectl.py"
    if dev_script.exists():
        return [sys.executable, str(dev_script)]

    raise FileNotFoundError(
        "taosanodectl not found in PATH or script directory"
    )


def _run_download_task(task_id, model_name, release, resource_base_url, replace):
    """Worker function for process pool to download model via taosanodectl.

    This runs in a separate process, so it needs all necessary context passed in.
    Uses Popen to monitor download progress via resource manager task files.
    """
    try:
        # Mark task as running at the start of the worker process
        dao = get_dao()
        dao.update_async_operation(task_id, status=TaskStatus.RUNNING.value, started_at=time.time())

        taosanodectl = _get_taosanodectl_cmd()

        cmd = taosanodectl + [
            "resource",
            "apply",
            model_name,
            "--release",
            release,
            "--format",
            "json",
            "--resource-base-url",
            resource_base_url,
        ]

        if replace:
            cmd.append("--replace")

        AppLogger.info(
            "Starting download process for task_id: %s, model: %s, cmd: %s",
            task_id,
            model_name,
            " ".join(cmd),
        )

        # Get task_root for polling download progress
        config = Configure.get_instance()
        resource_dir = config._conf.get("resource_dir", "")
        task_root = Path(resource_dir) / "state" / "tasks" if resource_dir else None

        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Poll task files for real-time progress while download runs
        while proc.poll() is None:
            if task_root and task_root.is_dir():
                for task_file in task_root.glob("*.json"):
                    try:
                        with task_file.open() as f:
                            task_data = json.load(f)
                        if task_data.get("name") == model_name and task_data.get("status") in ("downloading", "verifying"):
                            bytes_done = task_data.get("bytes_done", 0)
                            bytes_total = task_data.get("bytes_total", 0)
                            if bytes_total > 0:
                                progress = min(99, int(bytes_done * 100 / bytes_total))
                                dao.update_async_operation(task_id, progress=progress)
                            break
                    except (OSError, json.JSONDecodeError):
                        pass
            time.sleep(1)

        stdout, stderr = proc.communicate()
        if proc.returncode == 0:
            AppLogger.info("Download completed for task_id: %s, model: %s", task_id, model_name)
            return {"status": "completed", "error": None}
        else:
            error_msg = stderr.decode(errors="replace") if stderr else "Download failed"
            AppLogger.error("Download failed for task_id: %s, model: %s, error: %s", task_id, model_name, error_msg)
            return {"status": "failed", "error": error_msg}


    except subprocess.TimeoutExpired:
        error_msg = "Download timeout (>1 hour)"
        AppLogger.error("Download timeout for task_id: %s, model: %s", task_id, model_name)
        return {"status": "failed", "error": error_msg}
    except Exception as e:
        error_msg = str(e)
        AppLogger.error(
            "Download error for task_id: %s, model: %s, error: %s",
            task_id,
            model_name,
            error_msg,
        )
        return {"status": "failed", "error": error_msg}


def _trigger_model_download_background(task_id, model_name, release, resource_base_url, replace):
    """Submit download task to process pool.

    Args:
        task_id: Unique identifier for tracking this operation
        model_name: Name of the model to download (e.g., 'moirai')
        release: Release version (e.g., '1.0.0')
        resource_base_url: Base URL for resource repository
        replace: Whether to replace existing model

    The process pool handles async execution; caller monitors progress via get_operation_status(task_id)
    """

    def callback(result):
        """Called in the main process when the worker finishes (success or failure)."""
        status = result.get("status", TaskStatus.FAILED.value) if result else TaskStatus.FAILED.value
        error = result.get("error") if result else "Worker process returned no result" if status == TaskStatus.FAILED.value else None
        _complete_async_operation(task_id, model_name, OperationType.DOWNLOAD.value, status, error)

    def error_callback(exc):
        """Called in the main process when the worker raises an unhandled exception."""
        AppLogger.error(
            "Download task raised exception: task_id=%s, model=%s, exc=%s",
            task_id, model_name, exc,
        )
        _complete_async_operation(task_id, model_name, "download", "failed", str(exc))

    pool = _get_download_pool()
    AppLogger.info(
        "Submitting download task to pool: task_id=%s, model=%s, pool=%s",
        task_id, model_name, pool,
    )
    pool.apply_async(
        _run_download_task,
        args=(task_id, model_name, release, resource_base_url, replace),
        callback=callback,
        error_callback=error_callback,
    )
    AppLogger.info("Download task submitted to pool: task_id=%s", task_id)


def list_all_tsfm(request):
    """Get list of available models from resource repository via taosanodectl.

    Executes: taosanodectl resource check --release <release> --resource-base-url <url>

    Query parameters:
        release: Release version (default: 1.0.0)
        resource_base_url: Base URL for resource repository (default: from config)

    Returns:
        JSON response with available models including:
        - kind: "model" or "runtime"
        - name: model name
        - model_id: model identifier
        - version: resource version
        - state: "installed", "missing", etc.
        - size: size in bytes
        - note: description

    This provides a real-time view of available resources from the repository.
    """
    AppLogger.debug("Received request to check available models, ip:%s", request.remote_addr)

    try:
        # Get parameters from query string
        release = request.args.get("release", "1.0.0")
        config = Configure.get_instance()
        default_resource_url = config._conf.get("resource_base_url", "https://download.taosdata.com")
        resource_base_url = request.args.get("resource_base_url", default_resource_url)

        # Construct taosanodectl command
        try:
            taosanodectl = _get_taosanodectl_cmd()
        except FileNotFoundError as e:
            AppLogger.error("taosanodectl not found: %s, ip:%s", str(e), request.remote_addr)
            return {
                "status": "error",
                "error": f"taosanodectl not found: {e!s}",
            }, 500

        cmd = taosanodectl + [
            "resource",
            "check",
            "--release",
            release,
            "--format",
            "json",
            "--resource-base-url",
            resource_base_url,
        ]

        AppLogger.info(
            "Executing resource check: %s, ip:%s",
            " ".join(cmd),
            request.remote_addr,
        )

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)

        if result.returncode == 0:
            try:
                # Parse JSON output from taosanodectl
                resources_data = json.loads(result.stdout)
                AppLogger.info(
                    "Resource check successful: %d resources found, ip:%s",
                    len(resources_data.get("resources", [])),
                    request.remote_addr,
                )

                # Sync repository data into database
                resources = resources_data.get("resources", [])
                models_from_repo = [r for r in resources if r.get("kind") == "model"]
                dao = get_dao()

                for repo_model in models_from_repo:
                    model_name = repo_model.get("name")
                    size_bytes = repo_model.get("size", 0)
                    size_gb = size_bytes / (1024**3) if size_bytes else 0
                    downloaded = repo_model.get("state") == "installed"

                    existing = dao.get_model(model_name)
                    if existing:
                        dao.update_model(
                            model_name,
                            model_id=repo_model.get("model_id", existing.model_id),
                            package=repo_model.get("name", existing.package),
                            size_gb=size_gb,
                            downloaded=downloaded,
                        )
                    else:
                        new_model = FoundationModel(
                            name=model_name,
                            model_id=repo_model.get("model_id", ""),
                            package=repo_model.get("name", ""),
                            type="forecast",
                            description=repo_model.get("note", ""),
                            size_gb=size_gb,
                            downloaded=downloaded,
                        )
                        dao.create_model(new_model)
                        AppLogger.info(
                            "Added new model to catalog: %s from repository, ip:%s",
                            model_name,
                            request.remote_addr,
                        )

                # Overlay operation_status from SQLite (authoritative) over task file status
                for resource in resources_data.get("resources", []):
                    if resource.get("kind") != "model":
                        continue
                    latest_op = dao.get_latest_operation(resource.get("name", ""))
                    if latest_op:
                        resource["operation_status"] = latest_op.status

                return {
                    "status": "success",
                    "data": resources_data,
                }, 200
            except json.JSONDecodeError as e:
                AppLogger.error(
                    "Failed to parse resource check output: %s, ip:%s",
                    str(e),
                    request.remote_addr,
                )
                return {
                    "status": "error",
                    "error": f"Failed to parse resource check output: {e!s}",
                    "raw_output": result.stdout[:500],  # First 500 chars for debugging
                }, 500
        else:
            error_msg = result.stderr or "Resource check failed"
            AppLogger.error(
                "Resource check failed: %s, ip:%s",
                error_msg,
                request.remote_addr,
            )
            return {
                "status": "error",
                "error": f"Resource check failed: {error_msg}",
            }, 500

    except subprocess.TimeoutExpired:
        AppLogger.error("Resource check timeout (>30s), ip:%s", request.remote_addr)
        return {
            "status": "error",
            "error": "Resource check timeout (exceeded 30 seconds)",
        }, 504
    except Exception as e:
        AppLogger.error("Error checking available models: %s, ip:%s", str(e), request.remote_addr)
        return {
            "status": "error",
            "error": f"Error checking available models: {e!s}",
        }, 500


def get_model_info(request, model_name):
    """Get information about a specific foundation model from resource repository.

    Executes: taosanodectl resource list --release <release> --resource-base-url <url>

    Args:
        request: Flask request object
        model_name: Name of the model to retrieve (e.g., 'moirai', 'chronos')

    Query parameters (optional):
        release: Release version (default: 1.0.0)
        resource_base_url: Base URL for resource repository (default: from config)

    Returns:
        JSON response with model details including real-time status:
        - name: model name
        - model_id: model identifier
        - model_version: model version
        - version: resource version
        - size: size in bytes
        - install_status: "installed" | "missing"
        - service_status: "running" | "stopped"
        - status: current status
        - deployed_at: deployment timestamp
        - path: installation path
        - port: service port

    HTTP Status Codes:
        200 — Model found
        404 — Model not found
        500 — Server error
    """
    AppLogger.debug("Received request for model info: %s, ip:%s", model_name, request.remote_addr)

    try:
        # Get parameters from query string
        release = request.args.get("release", "1.0.0")
        config = Configure.get_instance()
        default_resource_url = config._conf.get("resource_base_url", "https://download.taosdata.com")
        resource_base_url = request.args.get("resource_base_url", default_resource_url)

        # Execute taosanodectl resource list to get real-time model info
        try:
            taosanodectl = _get_taosanodectl_cmd()
        except FileNotFoundError as e:
            AppLogger.error("taosanodectl not found: %s, ip:%s", str(e), request.remote_addr)
            return {
                "status": "error",
                "error": f"taosanodectl not found: {e!s}",
            }, 500

        cmd = taosanodectl + [
            "resource",
            "list",
            "--release",
            release,
            "--format",
            "json",
            "--resource-base-url",
            resource_base_url,
        ]

        AppLogger.info(
            "Executing resource list to get model info: %s, cmd: %s, ip:%s",
            model_name,
            " ".join(cmd),
            request.remote_addr,
        )

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)

        if result.returncode == 0:
            try:
                # Parse JSON response from taosanodectl
                resource_data = json.loads(result.stdout)
                models = resource_data.get("models", [])

                # Find the requested model
                model = next((m for m in models if m.get("name") == model_name), None)

                if not model:
                    AppLogger.warning(
                        "Model not found in resource list: %s, ip:%s",
                        model_name,
                        request.remote_addr,
                    )
                    return {
                        "status": "error",
                        "error": f"Model '{model_name}' not found",
                    }, 404

                AppLogger.info(
                    "Returning model info for: %s, status: %s, ip:%s",
                    model_name,
                    model.get("service_status"),
                    request.remote_addr,
                )

                # Sync real-time status from repository back to database
                _update_model_state(
                    model_name,
                    running=(model.get("service_status") == "running"),
                    downloaded=(model.get("install_status") == "installed"),
                )

                # Overlay operation_status from SQLite (authoritative) over task file status
                dao = get_dao()
                latest_op = dao.get_latest_operation(model_name)
                if latest_op:
                    model["operation_status"] = latest_op.status

                return {
                    "status": "success",
                    "data": model,
                }, 200

            except json.JSONDecodeError as e:
                AppLogger.error(
                    "Failed to parse resource list output: %s, ip:%s",
                    str(e),
                    request.remote_addr,
                )
                return {
                    "status": "error",
                    "error": f"Failed to parse resource list output: {e!s}",
                }, 500
        else:
            error_msg = result.stderr or "Resource list failed"
            AppLogger.error(
                "Resource list failed: %s, ip:%s",
                error_msg,
                request.remote_addr,
            )
            return {
                "status": "error",
                "error": f"Resource list failed: {error_msg}",
            }, 500

    except subprocess.TimeoutExpired:
        AppLogger.error("Resource list timeout (>30s) for model %s, ip:%s", model_name, request.remote_addr)
        return {
            "status": "error",
            "error": "Resource list timeout (exceeded 30 seconds)",
        }, 504

    except Exception as e:
        AppLogger.error(
            "Error retrieving model info for %s: %s, ip:%s",
            model_name,
            str(e),
            request.remote_addr,
        )
        return {
            "status": "error",
            "error": f"Error retrieving model info: {e!s}",
        }, 500



def _validate_model_in_repository(model_name, release, resource_base_url):
    """Validate that model exists in the resource repository via taosanodectl.

    This ensures we don't reject valid new models that were added to the repository
    but not yet in the database (which may be empty on first run).

    Args:
        model_name: Name of model to validate
        release: Release version
        resource_base_url: Base URL for resource repository

    Returns:
        tuple: (is_valid, model_info, error_message)
        - is_valid: True if model exists and is valid
        - model_info: Full model info from repository (if valid)
        - error_message: Error description (if invalid)
    """
    try:
        try:
            taosanodectl = _get_taosanodectl_cmd()
        except FileNotFoundError as e:
            return False, None, str(e)

        cmd = taosanodectl + [
            "resource",
            "check",
            "--release",
            release,
            "--format",
            "json",
            "--resource-base-url",
            resource_base_url,
        ]

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)

        if result.returncode != 0:
            error_msg = result.stderr or "Failed to check resources"
            return False, None, error_msg

        try:
            resources_data = json.loads(result.stdout)
            resources = resources_data.get("resources", [])

            # Find model in repository (only models, not runtimes)
            model_info = next(
                (r for r in resources if r.get("name") == model_name and r.get("kind") == "model"),
                None,
            )

            if not model_info:
                return False, None, f"Model '{model_name}' not found in repository"

            return True, model_info, None

        except json.JSONDecodeError as e:
            return False, None, f"Failed to parse repository data: {e!s}"

    except subprocess.TimeoutExpired:
        return False, None, "Repository check timeout (>30s)"
    except Exception as e:
        return False, None, f"Error validating model: {e!s}"


def download_foundation_model(request):
    """Start downloading a foundation model from remote repository.

    This function integrates with the DynamicResourceManager to download
    and install foundation models from the TDengine GPT resource repository.
    It mirrors the behavior of: taosanodectl resource apply <model_name>

    Expected payload:
    {
        "model_name": "moirai",
        "release": "1.0.0",                    # optional, default: "1.0.0"
        "resource_base_url": "https://...",   # optional, uses config default
        "replace": false                       # optional, allow overwrite
    }

    Returns:
        JSON response with task_id for async tracking (HTTP 202)
        Downloads are triggered asynchronously via DynamicResourceManager.apply_release()

    Implementation details:
        1. Validates model exists in repository (real-time check)
        2. Checks if already downloaded (prevents duplicate downloads)
        3. Generates unique task_id for progress tracking
        4. Records async operation in _ASYNC_OPERATIONS
        5. Spawns background task to call DynamicResourceManager.apply_release()
        6. Returns task_id immediately (HTTP 202 Accepted)
        7. Caller uses /status endpoint to monitor download progress
    """
    AppLogger.debug("Received foundation model download request, ip:%s", request.remote_addr)

    payload = request.get_json(silent=True) or {}
    model_name = payload.get("model_name")
    release = payload.get("release", "1.0.0")

    # Use provided resource_base_url or fall back to configured default
    config = Configure.get_instance()
    default_resource_url = config._conf.get("resource_base_url", "https://download.taosdata.com")
    resource_base_url = payload.get("resource_base_url", default_resource_url)
    replace = payload.get("replace", False)

    if not model_name:
        AppLogger.error("Download request missing model_name, ip:%s", request.remote_addr)
        return {
            "status": "error",
            "error": "Missing required field: model_name",
        }, 400

    # Validate model exists in repository (real-time check)
    is_valid, model_info, error_msg = _validate_model_in_repository(
        model_name, release, resource_base_url
    )
    if not is_valid:
        AppLogger.error(
            "Download request for invalid model: %s, error: %s, ip:%s",
            model_name,
            error_msg,
            request.remote_addr,
        )
        return {
            "status": "error",
            "error": error_msg,
        }, 404

    # Check if already downloaded
    dao = get_dao()
    model = dao.get_model(model_name)

    # If model not in database yet, add it from repository info
    if not model:
        size_bytes = model_info.get("size", 0)
        model = FoundationModel(
            name=model_name,
            model_id=model_info.get("model_id", ""),
            package=model_info.get("name", ""),
            type="forecast",
            description=model_info.get("note", ""),
            size_gb=size_bytes / (1024**3) if size_bytes else 0,
        )
        dao.create_model(model)
        AppLogger.info("Added new model to database from repository: %s", model_name)

    if model.downloaded and not replace:
        AppLogger.warning(
            "Download request for already downloaded model: %s (use replace=true to force), ip:%s",
            model_name,
            request.remote_addr,
        )
        return {
            "status": "error",
            "error": f"Model '{model_name}' is already downloaded (use replace=true to force redownload)",
        }, 400

    # Check if model is running (cannot replace running models)
    if model and model.running and replace:
        AppLogger.error(
            "Cannot replace running model: %s, stop it first, ip:%s",
            model_name,
            request.remote_addr,
        )
        return {
            "status": "error",
            "error": f"Cannot replace running model '{model_name}'. Stop it first.",
        }, 400

    dao.cleanup_download_operations(model_name)

    # Generate unique task ID for async tracking
    # Format: download-{model_name}-{random_hex}
    task_id = f"download-{model_name}-{uuid.uuid4().hex[:8]}"

    # Record async operation for progress tracking
    # This mirrors taosanodectl resource apply <model_name> workflow:
    # 1. Load manifest from resource_base_url
    # 2. Select model from manifest
    # 3. Acquire lock
    # 4. Download resource package (.zip)
    # 5. Verify SHA256 checksum
    # 6. Extract and install to model directory
    # 7. Record model metadata
    # 8. Release lock
    dao = get_dao()
    operation = AsyncOperation(
        task_id=task_id,
        operation=OperationType.DOWNLOAD.value,
        model_name=model_name,
        status=TaskStatus.QUEUED.value,
        data={
            "model_id": model.model_id,
            "package": model.package,
            "release": release,
            "resource_base_url": resource_base_url,
            "replace": replace,
        }
    )
    dao.create_async_operation(operation)

    # Spawn background process to download via taosanodectl
    _trigger_model_download_background(task_id, model_name, release, resource_base_url, replace)

    AppLogger.info(
        "Download initiated for model '%s' (package: %s, release: %s, resource_url: %s, replace: %s), "
        "task_id: %s, ip:%s",
        model_name,
        model.package,
        release,
        resource_base_url,
        replace,
        task_id,
        request.remote_addr,
    )

    return {
        "status": "queued",
        "task_id": task_id,
        "message": f"Download initiated for model '{model_name}'",
        "model_name": model_name,
        "model_id": model.model_id,
        "package": model.package,
        "size_gb": model.size_gb,
        "release": release,
        "resource_base_url": resource_base_url,
        "next_action": f"Query progress: GET /api/v1/foundation-models/{task_id}/status",
    }, 202


def _trigger_model_start_background(task_id, model_name):
    """Submit model start task to process pool.

    Executes: python ./taosanodectl.py model start <model_name>

    Args:
        task_id: Unique identifier for tracking this operation
        model_name: Name of the model to start (e.g., 'moirai')

    The process pool handles async execution; caller monitors progress via get_operation_status(task_id)
    """

    def callback(result):
        """Called in the main process when the worker finishes."""
        status = result.get("status", "failed") if result else "failed"
        error = result.get("error") if result else "Worker process returned no result" if status == "failed" else None
        _complete_async_operation(task_id, model_name, "start", status, error)

    def error_callback(exc):
        """Called in the main process when the worker raises an unhandled exception."""
        AppLogger.error(
            "Start task raised exception: task_id=%s, model=%s, exc=%s",
            task_id, model_name, exc,
        )
        _complete_async_operation(task_id, model_name, "start", "failed", str(exc))

    pool = _get_service_pool()
    AppLogger.info(
        "Submitting start task to pool: task_id=%s, model=%s, pool=%s",
        task_id, model_name, pool,
    )
    pool.apply_async(
        _run_model_start_task,
        args=(task_id, model_name),
        callback=callback,
        error_callback=error_callback,
    )
    AppLogger.info("Start task submitted to pool: task_id=%s", task_id)


def _run_model_delete_task(task_id, model_name):
    """Worker function for process pool to delete foundation model via taosanodectl.

    This runs in a separate process, so it needs all necessary context passed in.
    Cannot access _ASYNC_OPERATIONS directly; updates are applied after completion.
    """
    try:
        dao = get_dao()
        dao.update_async_operation(task_id, status=TaskStatus.RUNNING.value, started_at=time.time())

        taosanodectl = _get_taosanodectl_cmd()

        cmd = taosanodectl + [
            "resource",
            "delete",
            model_name,
        ]

        AppLogger.info(
            "Starting model delete process for task_id: %s, model: %s, cmd: %s",
            task_id,
            model_name,
            " ".join(cmd),
        )

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)

        if result.returncode == 0:
            AppLogger.info(
                "Model delete completed for task_id: %s, model: %s",
                task_id,
                model_name,
            )
            return {"status": "completed", "error": None}
        else:
            error_msg = result.stderr or "Model delete failed"
            AppLogger.error(
                "Model delete failed for task_id: %s, model: %s, error: %s",
                task_id,
                model_name,
                error_msg,
            )
            return {"status": "failed", "error": error_msg}

    except subprocess.TimeoutExpired:
        error_msg = "Model delete timeout (>5 minutes)"
        AppLogger.error("Model delete timeout for task_id: %s, model: %s", task_id, model_name)
        return {"status": "failed", "error": error_msg}
    except Exception as e:
        error_msg = str(e)
        AppLogger.error(
            "Model delete error for task_id: %s, model: %s, error: %s",
            task_id,
            model_name,
            error_msg,
        )
        return {"status": "failed", "error": error_msg}


def _trigger_model_delete_background(task_id, model_name):
    """Submit model delete task to process pool.

    Executes: python ./taosanodectl.py resource delete <model_name>

    Args:
        task_id: Unique identifier for tracking this operation
        model_name: Name of the model to delete (e.g., 'moirai')

    The process pool handles async execution; caller monitors progress via get_operation_status(task_id)
    """

    def callback(result):
        """Called in the main process when the worker finishes."""
        status = result.get("status", "failed") if result else "failed"
        error = result.get("error") if result else "Worker process returned no result" if status == "failed" else None
        _complete_async_operation(task_id, model_name, "delete", status, error)

    def error_callback(exc):
        """Called in the main process when the worker raises an unhandled exception."""
        AppLogger.error(
            "Delete task raised exception: task_id=%s, model=%s, exc=%s",
            task_id, model_name, exc,
        )
        _complete_async_operation(task_id, model_name, "delete", "failed", str(exc))

    pool = _get_service_pool()
    AppLogger.info(
        "Submitting delete task to pool: task_id=%s, model=%s, pool=%s",
        task_id, model_name, pool,
    )
    pool.apply_async(
        _run_model_delete_task,
        args=(task_id, model_name),
        callback=callback,
        error_callback=error_callback,
    )
    AppLogger.info("Delete task submitted to pool: task_id=%s", task_id)


def _run_model_start_task(task_id, model_name):
    """Worker function for process pool to start foundation model via taosanodectl.

    This runs in a separate process, so it needs all necessary context passed in.
    Cannot access _ASYNC_OPERATIONS directly; updates are applied after completion.

    Uses Popen with start_new_session=True so the model service process is detached
    from the RESTful server's process group. This prevents the model service from
    being killed when the RESTful server stops (SIGHUP/SIGTERM to process group).
    """
    try:
        dao = get_dao()
        dao.update_async_operation(task_id, status=TaskStatus.RUNNING.value, started_at=time.time())

        taosanodectl = _get_taosanodectl_cmd()

        cmd = taosanodectl + [
            "model",
            "start",
            model_name,
        ]

        AppLogger.info(
            "Starting model process for task_id: %s, model: %s, cmd: %s",
            task_id,
            model_name,
            " ".join(cmd),
        )

        # Use Popen with start_new_session=True to detach the model service from
        # this process group. Without this, the model service would be killed when
        # the RESTful server exits (OS sends SIGHUP to the entire process group).
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )

        AppLogger.info(
            "Model start process spawned: task_id=%s, model=%s, pid=%d (new session, detached from server)",
            task_id,
            model_name,
            proc.pid,
        )

        # Wait briefly to detect immediate startup failures (bad args, missing files).
        # A long-running service will still be alive after this window.
        try:
            stdout, stderr = proc.communicate(timeout=10)
            returncode = proc.returncode
        except subprocess.TimeoutExpired:
            # Process is still running after 10s — this means it started successfully
            # as a long-running service. Do not kill it.
            AppLogger.info(
                "Model start process still running after 10s — treating as successful daemon: "
                "task_id=%s, model=%s, pid=%d",
                task_id,
                model_name,
                proc.pid,
            )
            return {"status": "completed", "error": None}

        if returncode == 0:
            AppLogger.info(
                "Model start completed: task_id=%s, model=%s, pid=%d",
                task_id,
                model_name,
                proc.pid,
            )
            return {"status": "completed", "error": None}
        else:
            error_msg = stderr.decode(errors="replace").strip() if stderr else "Model start failed"
            AppLogger.error(
                "Model start failed: task_id=%s, model=%s, returncode=%d, error=%s",
                task_id,
                model_name,
                returncode,
                error_msg,
            )
            return {"status": "failed", "error": error_msg}

    except Exception as e:
        error_msg = str(e)
        AppLogger.error(
            "Model start error: task_id=%s, model=%s, error=%s",
            task_id,
            model_name,
            error_msg,
        )
        return {"status": "failed", "error": error_msg}



def _create_async_operation_task(request, model_name, operation, operation_type, validate_fn, trigger_fn):
    """Generic handler for async model operations (start, stop, delete).

    Args:
        request: Flask request object
        model_name: Name of the model
        operation: Operation name (start, stop, delete)
        operation_type: Type of operation for task_id (start, stop, delete)
        validate_fn: Function to validate model state - returns (is_valid, error_msg)
        trigger_fn: Function to trigger the background task

    Returns:
        tuple: (response_dict, http_status_code)
    """
    # Get model
    dao = get_dao()
    model = dao.get_model(model_name)
    if not model:
        AppLogger.error(
            "%s request for non-existent model: %s, ip:%s",
            operation.capitalize(),
            model_name,
            request.remote_addr,
        )
        return {
            "status": "error",
            "error": f"Model '{model_name}' not found",
        }, 404

    # Validate model state
    is_valid, error_msg = validate_fn(model, model_name, operation.capitalize(), request.remote_addr)
    if not is_valid:
        return {
            "status": "error",
            "error": error_msg,
        }, 400

    # Generate task ID for async tracking
    task_id = f"{operation_type}-{model_name}-{uuid.uuid4().hex[:8]}"

    # Record async operation with timestamps
    dao = get_dao()
    op = AsyncOperation(
        task_id=task_id,
        operation=operation_type,
        model_name=model_name,
        status=TaskStatus.QUEUED.value
    )
    dao.create_async_operation(op)

    # Spawn background process
    trigger_fn(task_id, model_name)

    AppLogger.info(
        "%s initiated for model '%s', task_id: %s, ip:%s",
        operation.capitalize(),
        model_name,
        task_id,
        request.remote_addr,
    )

    return {
        "status": "queued",
        "task_id": task_id,
        "message": f"Model '{model_name}' {operation} initiated",
        "model_name": model_name,
        "next_action": f"Query progress: GET /api/v1/foundation-models/{task_id}/status",
    }, 202


def _validate_start_state(model, model_name, operation, ip):
    """Validate model state for start operation."""
    if not model.downloaded:
        AppLogger.error(
            "%s request for non-downloaded model: %s, ip:%s",
            operation,
            model_name,
            ip,
        )
        return False, f"Model '{model_name}' is not downloaded yet. Download it first."

    if model.running:
        AppLogger.warning(
            "%s request for already running model: %s, ip:%s",
            operation,
            model_name,
            ip,
        )
        return False, f"Model '{model_name}' is already running"

    return True, None


def _validate_stop_state(model, model_name, operation, ip):
    """Validate model state for stop operation."""
    if not model.running:
        AppLogger.warning(
            "%s request for non-running model: %s, ip:%s",
            operation,
            model_name,
            ip,
        )
        return False, f"Model '{model_name}' is not running"

    return True, None


def start_foundation_model(request):
    """Start a foundation model service.

    Expected payload:
    {
        "model_name": "moirai"
    }

    Returns:
        JSON response with task_id for async tracking (HTTP 202)
    """
    AppLogger.debug("Received foundation model start request, ip:%s", request.remote_addr)

    payload = request.get_json(silent=True) or {}
    model_name = payload.get("model_name")

    if not model_name:
        AppLogger.error("Start request missing model_name, ip:%s", request.remote_addr)
        return {
            "status": "error",
            "error": "Missing required field: model_name",
        }, 400

    return _create_async_operation_task(
        request,
        model_name,
        "start",
        "start",
        _validate_start_state,
        _trigger_model_start_background,
    )


def delete_foundation_model(request):
    """Delete a foundation model.

    Expected payload:
    {
        "model_name": "embedding-model-v1"
    }

    Returns:
        JSON response with task_id for async tracking (HTTP 202)
    """
    AppLogger.debug("Received foundation model delete request, ip:%s", request.remote_addr)

    payload = request.get_json(silent=True) or {}
    model_name = payload.get("model_name")

    if not model_name:
        AppLogger.error("Delete request missing model_name, ip:%s", request.remote_addr)
        return {
            "status": "error",
            "error": "Missing required field: model_name",
        }, 400

    # Validate model exists
    dao = get_dao()
    model = dao.get_model(model_name)
    if not model:
        AppLogger.error(
            "Delete request for non-existent model: %s, ip:%s",
            model_name,
            request.remote_addr,
        )
        return {
            "status": "error",
            "error": f"Model '{model_name}' not found",
        }, 404

    if model.running:
        AppLogger.error(
            "Delete request for running model: %s, ip:%s",
            model_name,
            request.remote_addr,
        )
        return {
            "status": "error",
            "error": f"Cannot delete running model '{model_name}'. Stop it first.",
        }, 400

    if not model.downloaded:
        AppLogger.warning(
            "Delete request for non-downloaded model: %s, ip:%s",
            model_name,
            request.remote_addr,
        )
        return {
            "status": "error",
            "error": f"Model '{model_name}' is not downloaded",
        }, 400

    # Generate task ID for async tracking
    task_id = f"delete-{model_name}-{uuid.uuid4().hex[:8]}"

    # Record async operation
    dao = get_dao()
    op = AsyncOperation(
        task_id=task_id,
        operation=OperationType.DELETE.value,
        model_name=model_name,
        status=TaskStatus.QUEUED.value
    )
    dao.create_async_operation(op)

    # Spawn background process to delete model via taosanodectl
    _trigger_model_delete_background(task_id, model_name)

    AppLogger.info(
        "Delete initiated for model '%s', task_id: %s, ip:%s",
        model_name,
        task_id,
        request.remote_addr,
    )

    return {
        "status": "queued",
        "task_id": task_id,
        "message": f"Model '{model_name}' deletion initiated",
        "model_name": model_name,
        "next_action": f"Query progress: GET /api/v1/foundation-models/{task_id}/status",
    }, 202


def _run_model_stop_task(task_id, model_name):
    """Worker function for process pool to stop foundation model via taosanodectl.

    This runs in a separate process, so it needs all necessary context passed in.
    Cannot access _ASYNC_OPERATIONS directly; updates are applied after completion.
    """
    try:
        dao = get_dao()
        dao.update_async_operation(task_id, status=TaskStatus.RUNNING.value, started_at=time.time())

        taosanodectl = _get_taosanodectl_cmd()

        cmd = taosanodectl + [
            "model",
            "stop",
            model_name,
        ]

        AppLogger.info(
            "Starting model stop process for task_id: %s, model: %s, cmd: %s",
            task_id,
            model_name,
            " ".join(cmd),
        )

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)

        if result.returncode == 0:
            AppLogger.info(
                "Model stop completed for task_id: %s, model: %s",
                task_id,
                model_name,
            )
            return {"status": "completed", "error": None}
        else:
            error_msg = result.stderr or "Model stop failed"
            AppLogger.error(
                "Model stop failed for task_id: %s, model: %s, error: %s",
                task_id,
                model_name,
                error_msg,
            )
            return {"status": "failed", "error": error_msg}

    except subprocess.TimeoutExpired:
        error_msg = "Model stop timeout (>5 minutes)"
        AppLogger.error("Model stop timeout for task_id: %s, model: %s", task_id, model_name)
        return {"status": "failed", "error": error_msg}
    except Exception as e:
        error_msg = str(e)
        AppLogger.error(
            "Model stop error for task_id: %s, model: %s, error: %s",
            task_id,
            model_name,
            error_msg,
        )
        return {"status": "failed", "error": error_msg}


def _trigger_model_stop_background(task_id, model_name):
    """Submit model stop task to process pool.

    Executes: python ./taosanodectl.py model stop <model_name>

    Args:
        task_id: Unique identifier for tracking this operation
        model_name: Name of the model to stop (e.g., 'moirai')

    The process pool handles async execution; caller monitors progress via get_operation_status(task_id)
    """

    def callback(result):
        """Called in the main process when the worker finishes."""
        status = result.get("status", "failed") if result else "failed"
        error = result.get("error") if result else "Worker process returned no result" if status == "failed" else None
        _complete_async_operation(task_id, model_name, "stop", status, error)

    def error_callback(exc):
        """Called in the main process when the worker raises an unhandled exception."""
        AppLogger.error(
            "Stop task raised exception: task_id=%s, model=%s, exc=%s",
            task_id, model_name, exc,
        )
        _complete_async_operation(task_id, model_name, "stop", "failed", str(exc))

    pool = _get_service_pool()
    AppLogger.info(
        "Submitting stop task to pool: task_id=%s, model=%s, pool=%s",
        task_id, model_name, pool,
    )
    pool.apply_async(
        _run_model_stop_task,
        args=(task_id, model_name),
        callback=callback,
        error_callback=error_callback,
    )
    AppLogger.info("Stop task submitted to pool: task_id=%s", task_id)


def stop_foundation_model(request):
    """Stop a running foundation model service.

    Expected payload:
    {
        "model_name": "moirai"
    }

    Returns:
        JSON response with task_id for async tracking (HTTP 202)
    """
    AppLogger.debug("Received foundation model stop request, ip:%s", request.remote_addr)

    payload = request.get_json(silent=True) or {}
    model_name = payload.get("model_name")

    if not model_name:
        AppLogger.error("Stop request missing model_name, ip:%s", request.remote_addr)
        return {
            "status": "error",
            "error": "Missing required field: model_name",
        }, 400

    return _create_async_operation_task(
        request,
        model_name,
        "stop",
        "stop",
        _validate_stop_state,
        _trigger_model_stop_background,
    )


def _build_operation_response(operation_info):
    """Build a detailed response dict for an async operation.

    Args:
        operation_info: AsyncOperation dataclass from DAO

    Returns:
        dict with all operation details (task_id, status, progress, errors, etc.)
    """
    task_id = operation_info.task_id
    operation = operation_info.operation
    model_name = operation_info.model_name

    response_data = {
        "task_id": task_id,
        "operation": operation,
        "model_name": model_name,
        "progress": operation_info.progress,
        "status": operation_info.status,
    }

    if operation == OperationType.DOWNLOAD.value:
        data = operation_info.data or {}
        response_data.update({
            "model_id": data.get("model_id", ""),
            "package": data.get("package", ""),
            "release": data.get("release", "1.0.0"),
            "resource_base_url": data.get("resource_base_url", ""),
            "replace": data.get("replace", False),
        })

    if operation_info.status == TaskStatus.FAILED.value:
        response_data["error"] = operation_info.error or "Unknown error"
        response_data["error_code"] = operation_info.error_code or ""

    return response_data


def get_operation_status(request, task_id):
    """Get status of an async operation by task_id.

    This endpoint allows clients to query the progress of long-running operations
    like model downloads. It mirrors the behavior of resource management operations.

    Mock implementation: async task execution via taosanodectl subprocess.
    In production, this would:
    1. Query the DynamicResourceManager's operation state
    2. Return real download progress (bytes downloaded / total size)
    3. Report current installation step
    4. Return detailed error information on failure

    Args:
        request: Flask request object
        task_id: Unique identifier for the async operation
                Format: {operation}-{model_name}-{random_hex}
                Example: download-moirai-abc123ef

    Returns:
        JSON response with operation status and detailed progress information

    Response states:
        queued     - Task created, waiting for pool worker to pick it up
        running    - Task is being executed by pool worker
        completed  - Task finished successfully
        failed     - Task execution failed with error
    """
    AppLogger.debug("Received operation status request for task_id: %s, ip:%s", task_id, request.remote_addr)

    dao = get_dao()
    operation_info = dao.get_async_operation(task_id)
    if not operation_info:
        AppLogger.warning("Status request for non-existent task_id: %s, ip:%s", task_id, request.remote_addr)
        return {
            "status": "error",
            "error": f"Task '{task_id}' not found",
        }, 404

    response_data = _build_operation_response(operation_info)
    return {
        "status": "success",
        "data": response_data,
    }, 200


def _emit_operation_status_updates(dao, all_ops, seen_states):
    """Check for operation state changes and yield SSE events.

    Updates seen_states dict and yields SSE events for:
    - Active operations that have changed state
    - Tasks that disappeared from active list (now completed/failed)

    Args:
        dao: Data access object for fetching operation details
        all_ops: List of active AsyncOperation objects
        seen_states: Dict tracking task_id -> (status, progress) seen so far

    Yields:
        SSE formatted strings ("data: {...}\n\n") or empty string if no changes
    """
    current_states = {}
    changed = False

    # Check each operation for state changes
    for op_info in all_ops:
        task_id = op_info.task_id
        current_state = (op_info.status, op_info.progress)
        current_states[task_id] = current_state
        last_state = seen_states.get(task_id)

        # Emit if state changed
        if last_state != current_state:
            response_data = _build_operation_response(op_info)
            yield f"data: {json.dumps(response_data)}\n\n"
            seen_states[task_id] = current_state
            changed = True

    # Check for tasks that were active but have now disappeared
    # They may have completed, failed, or been interrupted
    for task_id in list(seen_states.keys()):
        if task_id not in current_states:
            # Task is no longer in the active list — check if it has completed
            completed_op = dao.get_async_operation(task_id)
            if completed_op and completed_op.status in [TaskStatus.COMPLETED.value, TaskStatus.FAILED.value, TaskStatus.INTERRUPTED.value]:
                # Send the final completion/failure status
                response_data = _build_operation_response(completed_op)
                yield f"data: {json.dumps(response_data)}\n\n"
                del seen_states[task_id]
                changed = True

    if not changed:
        yield ""


def get_all_operations_stream(request):
    """Stream all foundation model operations status via Server-Sent Events (SSE).

    Single connection pushes updates for ALL running tasks.
    Clients subscribe once and receive all task status changes.

    Client usage:
        const sse = new EventSource(`/api/v1/foundation-models/status/stream/all`)
        sse.onmessage = (e) => {
            const op = JSON.parse(e.data)
            console.log(`Task ${op.task_id}: ${op.status} (${op.progress}%)`)
        }

    Returns:
        SSE stream of operation updates: {task_id, status, progress, ...}
    """
    from flask import Response

    def event_stream():
        """Generator yielding SSE events for all active operations."""
        dao = get_dao()
        seen_states = {}  # task_id -> (status, progress)
        no_change_count = 0
        max_no_change = 3600  # 1 hour of no change = timeout

        while True:
            try:
                # Get all active (non-completed) operations
                all_ops = dao.list_async_operations(active_only=True)

                # Emit updates for changed operations
                had_changes = False
                for event in _emit_operation_status_updates(dao, all_ops, seen_states):
                    if event:  # Skip empty strings (no changes)
                        yield event
                        had_changes = True

                # Send keep-alive every 15s if no changes
                no_change_count += 1
                if no_change_count % 15 == 0:
                    yield ":keepalive\n\n"

                if had_changes:
                    no_change_count = 0

                # Timeout if no changes for 60 minutes
                if no_change_count >= max_no_change:
                    yield f"data: {json.dumps({'error': 'No status changes for 60 minutes, stream closed'})}\n\n"
                    return

                time.sleep(1)

            except Exception as e:
                AppLogger.error("SSE all-ops stream error: %s", e)
                yield f"data: {json.dumps({'error': str(e)})}\n\n"
                return

    AppLogger.info("SSE stream opened for all operations, ip:%s", request.remote_addr)
    return Response(event_stream(), mimetype="text/event-stream")
