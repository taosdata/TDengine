# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

"""Handlers for foundation model operations (list, download, start, delete).

Currently uses mock implementations. Can be extended to integrate with
ResourceManager for real downloads and taosanode_service for process management.
"""

import json
import logging
import os
import subprocess
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

from taosanalytics.conf import Configure
from taosanalytics.log import AppLogger
from taosanalytics.tsfmservice.foundation_models_db import (
    AsyncOperation,
    FoundationModel,
    get_dao,
)

# Name the shared TSFM runtime (venv) goes by everywhere: in the release manifest
# (packaging/tdengine-tdgpt-tsfm-release-*.json), in `resource apply/delete` (see
# SHARED_RUNTIME_NAME in tdgpt/script/resource_manifest.py), as the `model_name`
# of a runtime operation and on the SSE stream. One resource, one name --
# `resource apply` selects by exactly this value.
_RUNTIME_RESOURCE_NAME = "tsfm_runtime"

# Whether this process has already looked for deletes to resume. The recovery
# runs from the request path (see app.before_request), not from a server hook:
# the executor queues that run these tasks are process-local and the preloading
# master owns none, and keeping it in the app is what makes it work under both
# gunicorn and waitress. One run per process is enough -- claiming is atomic, so
# a second process scanning adds nothing.
_PENDING_DELETES_RESUMED = False


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
    # Combined operation: download the model, then automatically start it once
    # the download finishes. Tracked as a single async operation so clients can
    # watch one task from "queued" all the way to the model being running.
    DOWNLOAD_START = "download_start"


# Global background executors for async operations — lazily initialized to
# avoid re-creating them inside a forked worker process (gunicorn/multiprocessing).
# Downloads and service operations both only orchestrate a short-lived
# taosanodectl subprocess (the heavy disk/network work runs in that child), so
# both run on dedicated single-thread executors and stay isolated from each
# other (a long download never blocks start/stop/delete and vice versa).
#
# Two different authorities gate a download, and this module only owns one:
#
#   * Whether an apply may run *at all* is the CLI's decision. `resource apply`
#     takes the system-wide lifecycle lock in taosanode_service.py (non-blocking,
#     shared with model start/stop, import and delete) and is refused with exit
#     75 while anyone else holds it. That lock has no queue: it either runs the
#     command or refuses it.
#   * *When* a refused download gets to run is what the flock below decides.
#     Each gunicorn worker has its own download executor (1 worker), so a request
#     submits its download into the local executor queue, and the runner waits on
#     the shared flock before marking the op running. That wait is exactly what
#     keeps a download arriving during another one `queued` in the shared
#     async_operations table (visible via /status & SSE) and runs it once the
#     turn is free, instead of letting it hit the CLI's refusal and fail. No
#     scheduler thread is needed — the executor queue + flock ordering handle
#     sequencing, and a restart simply drops queued tasks (startup cleanup).
#
# The shared TSFM runtime is downloaded through this very pool as one more
# operation: a single queue for every download, served in submission order. A
# runtime download therefore waits for the download already running ahead of it
# instead of jumping the queue, which is what keeps each worker process to one
# download pool.
_DOWNLOAD_EXECUTOR = None
_SERVICE_EXECUTOR = None
_MODELS_INITIALIZED = False


def _log_invocation(action, **params):
    """Log the parameters of a taosanodectl / model invocation at debug level.

    Records the *resolved* values rather than the raw request, so a failed call
    can be reproduced from the log alone (which base URL, which timeout, which
    task). Emitted at debug level so a normal run stays quiet; it is controlled
    by ``log_level`` in taosanode.config.py (shipped default: 'DEBUG').
    """
    if not params:
        AppLogger.debug("%s invoked without parameters", action)
        return
    AppLogger.debug(
        "%s invoked: %s",
        action,
        ", ".join(f"{key}={value!r}" for key, value in params.items()),
    )


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

    # A restart does not stop the model workers -- Models.start detaches them into
    # a session of their own, so they outlive this process -- which is why the
    # cached `running` flag has to be rebuilt from the pid files, not assumed.
    reconcile_model_running_state()

    cleaned = dao.cleanup_old_operations(days=1)
    if cleaned:
        AppLogger.info("Cleaned up %d stale async operations (>1 day)", cleaned)

    _MODELS_INITIALIZED = True
    AppLogger.info("Models database initialization complete")

def _get_download_executor():
    """Return the download thread executor, creating it on first use.

    A single worker keeps downloads serialized within this process. The heavy
    work runs in a taosanodectl subprocess, so a thread executor is enough here.
    Serialization *across* processes is not this pool's job: permission comes
    from the CLI's lifecycle lock and ordering from the flock the runner waits on
    (see the module-level note on the download executor).
    """
    global _DOWNLOAD_EXECUTOR
    if _DOWNLOAD_EXECUTOR is None:
        AppLogger.info("Initializing download executor with 1 worker")
        _DOWNLOAD_EXECUTOR = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="tsfm-download"
        )
        AppLogger.info("Download executor initialized: %s", _DOWNLOAD_EXECUTOR)
    return _DOWNLOAD_EXECUTOR


def _download_lock_path() -> str:
    """Return the lock file downloads take turns on before applying.

    It sequences this API's own downloads only. It is not the authority on
    whether an apply may run -- that is the CLI's lifecycle lock (see the
    module-level note on the download executor).
    """
    config = Configure.get_instance()
    resource_dir = config._conf.get("resource_dir", "")
    root = Path(resource_dir) if resource_dir else Path(config.get_model_directory())
    root.mkdir(parents=True, exist_ok=True)
    return str(root / ".tdgpt-download.lock")


def _try_acquire_download_lock():
    """Try to take this download's turn (non-blocking).

    Holding this lock means "it is my turn to apply", not "applying is allowed":
    the child that eventually applies can still be refused by the CLI's lifecycle
    lock. Its job is ordering, so a download that arrives while another one runs
    waits for its turn instead of failing outright.

    Returns:
        int: open fd holding the lock, or None if another download holds it.
    """
    path = _download_lock_path()
    fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o644)
    try:
        if os.name == "nt":
            # Windows: lock a single byte via msvcrt (advisory, cross-process).
            import msvcrt
            os.lseek(fd, 0, os.SEEK_SET)
            if os.fstat(fd).st_size == 0:
                os.write(fd, b"\x00")
            os.lseek(fd, 0, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fd
    except (OSError, ImportError):
        os.close(fd)
        return None


def _release_download_lock(fd):
    """Give up the download turn taken by _try_acquire_download_lock.

    Handing the turn over promptly is what lets the next queued download start,
    so every path that wins the lock releases it (including the ones that abort
    before the child is spawned).
    """
    try:
        if os.name == "nt":
            import msvcrt
            os.lseek(fd, 0, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        try:
            os.close(fd)
        except OSError:
            pass


def _submit_download_task(worker_fn, args, task_id, model_name, operation):
    """Submit one download-family task to the download thread executor.

    The executor's single worker acts as a local queue: downloads submitted to
    this process run one at a time. Ordering across workers comes from the flock
    the worker waits on before applying (see the module-level note on the
    download executor), so a download that loses the race simply stays queued and
    runs once the turn is free — it never fails because another worker is
    downloading. Permission to apply still belongs to the CLI's lifecycle lock.

    Args:
        worker_fn: Worker function taking *args and returning a result dict
        args: Positional args for worker_fn
        task_id: Unique identifier for tracking this operation
        model_name: Name of the model being operated on
        operation: Operation name (download, download_start)
    """
    # One pool for every download, the shared TSFM runtime included: the pool's
    # single worker is the local queue, so downloads are served in submission
    # order -- the runtime does not get a pool (or a queue position) of its own.
    executor = _get_download_executor()

    def _on_done(future):
        """Called when the runner finishes (success or failure)."""
        try:
            result = future.result()
        except Exception as exc:
            AppLogger.error(
                "%s task raised exception: task_id=%s, model=%s, exc=%s",
                operation, task_id, model_name, exc,
            )
            _complete_async_operation(task_id, model_name, operation, "failed", str(exc))
            return

        status = result.get("status", "failed") if result else "failed"
        error = result.get("message") if result else ("Worker returned no result" if status == "failed" else None)
        _complete_async_operation(task_id, model_name, operation, status, error)

    AppLogger.info(
        "Submitting %s task to download executor: task_id=%s, model=%s, executor=%s",
        operation, task_id, model_name, executor,
    )
    future = executor.submit(worker_fn, *args)
    future.add_done_callback(_on_done)
    AppLogger.info(
        "%s task submitted to download executor: task_id=%s",
        operation.capitalize(), task_id,
    )


def _get_service_executor():
    """Return the service thread executor, creating it on first use.

    Service operations (start, stop, delete) only coordinate a short-lived
    taosanodectl subprocess, so they run on a dedicated thread executor. Keeping
    them separate from the download executor prevents long downloads from
    blocking responsive service actions (and vice versa). A single worker keeps
    service operations serialized, matching the previous one-per-pool semantics.
    """
    global _SERVICE_EXECUTOR
    if _SERVICE_EXECUTOR is None:
        AppLogger.info("Initializing service executor with 1 worker")
        _SERVICE_EXECUTOR = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="tsfm-service"
        )
        AppLogger.info("Service executor initialized: %s", _SERVICE_EXECUTOR)
    return _SERVICE_EXECUTOR


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


def _reconcile_local_model_state(model_name, local_model):
    """Synchronize cached model state from the authoritative local inventory."""
    if local_model is None:
        _update_model_state(model_name, downloaded=False)
        return
    _update_model_state(
        model_name,
        downloaded=(local_model.get("install_status") == "installed"),
        running=(local_model.get("service_status") == "running"),
    )


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
    fields = {
        "status": status,
        "completed_at": now,
        "error": error if error else None,
    }
    # 100 only describes a successful finish. A failed operation keeps whatever
    # its last phase reported, which /status derives from the task file (see
    # _apply_download_phase); forcing 100 there would contradict the phase view.
    if status == TaskStatus.COMPLETED.value:
        fields["progress"] = 100
    dao.update_async_operation(task_id, **fields)

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
            # An explicitly downloaded runtime has no model row of its own, so
            # only a model download flips the downloaded flag.
            if model_name != _RUNTIME_RESOURCE_NAME:
                _update_model_state(model_name, downloaded=True)
        elif operation == OperationType.START.value:
            _update_model_state(model_name, running=True)
        elif operation == OperationType.DOWNLOAD_START.value:
            # Combined operation: a successful completion means the model was
            # downloaded AND its service started.
            _update_model_state(model_name, downloaded=True, running=True)
        elif operation == OperationType.STOP.value:
            _update_model_state(model_name, running=False)
        elif operation == OperationType.DELETE.value:
            # The shared runtime is a resource without a model row of its own;
            # there is nothing to update for it.
            if model_name != _RUNTIME_RESOURCE_NAME:
                _update_model_state(model_name, downloaded=False, running=False)

    dao.cleanup_old_operations(days=1)


def _taosanodectl_log_level() -> str:
    """Return the --log-level value forwarded to every taosanodectl child.

    The level follows the configured ``log_level`` so a child process logs with
    the same detail as this service rather than a level of its own. The shipped
    default is DEBUG, because a failed resource/model operation is usually not
    reproducible afterwards: the archive has been consumed and the staging
    directory removed, so the child's own trace is the only evidence left.

    Returns:
        str: Level name ("debug", "info", ...), or the numeric level verbatim
            when it is not a standard one — taosanode_service --log-level
            accepts both forms.
    """
    level = Configure.get_instance()._conf.get("log_level", logging.DEBUG)
    name = logging.getLevelName(level)
    if not isinstance(name, str) or name.startswith("Level "):
        return str(level)
    return name.lower()


def _get_taosanodectl_cmd():
    """Return the command prefix for invoking taosanodectl.

    Prefers the system-installed taosanodectl binary. Falls back to:
    1. Production layout: taosanode/bin/taosanodectl.py
    2. Development layout: tdgpt/script/taosanodectl.py

    The prefix carries the child log level as well (``--log-level``, taken from
    the configured log_level — see _taosanodectl_log_level), so every
    taosanodectl invocation started by the service logs with the same detail
    without having to touch each call site.

    Returns:
        list: Command prefix, e.g. ["taosanodectl", "--log-level", "debug"] or
            [sys.executable, "/path/to/taosanodectl.py", "--log-level", "debug"]

    Raises:
        FileNotFoundError: If neither the system binary nor the script is found
    """
    import shutil
    if shutil.which("taosanodectl"):
        prefix = ["taosanodectl"]
    else:
        # Production layout: taosanode/bin/taosanodectl.py
        script_dir = Path(__file__).resolve().parent.parent.parent.parent / "bin"
        script = script_dir / "taosanodectl.py"
        if script.exists():
            prefix = [sys.executable, str(script)]
        else:
            # Development layout: tdgpt/script/taosanodectl.py
            dev_script = Path(__file__).resolve().parent.parent.parent / "script" / "taosanodectl.py"
            if not dev_script.exists():
                raise FileNotFoundError(
                    "taosanodectl not found in PATH or script directory"
                )
            prefix = [sys.executable, str(dev_script)]

    return prefix + ["--log-level", _taosanodectl_log_level()]


def _run_download_task(task_id, model_name, release, resource_base_url, replace):
    """Worker function for the download executor: apply a model via taosanodectl.

    Runs in an executor thread. The task first waits for its turn on the shared
    download file lock (ordering between this API's downloads; see the
    module-level note on the download executor), then marks the operation running
    and drives a taosanodectl subprocess to completion. While it waits the op
    stays `queued` in the shared async_operations table — it never races the
    underlying resource-manager file lock, which would report an error.

    Holding the lock does not admit the apply: the child can still be refused by
    the CLI's lifecycle lock, which is shared with model start/stop, import and
    delete. Such a refusal (exit 75) is reported as a failure here; see the note
    on _CLI_EXIT_TEMPFAIL.
    """
    # Waiting for the download turn, not for permission to apply: the CLI decides
    # whether the apply may run, and this flock only decides the order in which
    # this API's downloads ask for it.

    # There is deliberately no wall-clock wait timeout: a slow network can make
    # a download (and thus the queue ahead of it) legitimately take hours, and
    # only a user action cancels a download — the system never force-kills one.
    # So while waiting for the lock the task is aborted only if it is no longer
    # valid: the row was deleted or its status changed away from 'queued'
    # (cancelled/cleaned up). The op stays `queued` in the shared table so the
    # client can still observe it via /status & SSE.

    dao = get_dao()
    lock_fd = None

    # 若为持久性错误（非互斥占用）应立即失败，避免无限等待
    while lock_fd is None:
        op = dao.get_async_operation(task_id)
        if op is None or op.status != TaskStatus.QUEUED.value:
            AppLogger.info(
                "Download task no longer active, aborting wait: task_id=%s, model=%s",
                task_id,
                model_name,
            )
            return {
                "status": TaskStatus.INTERRUPTED.value,
                "message": "Task no longer active (cancelled/removed); download aborted",
            }

        # Every download competes for the same lock, the runtime included: there
        # is no priority queue here. Yielding to a runtime download would not
        # even work any more -- the runtime runs on this same single-worker pool,
        # so a model task spinning in this loop would be blocking the very task
        # it is waiting for.
        lock_fd = _try_acquire_download_lock()
        if lock_fd is None:
            AppLogger.info(
                "Download queued behind another download (waiting for flock): task_id=%s, model=%s",
                task_id,
                model_name,
            )
            time.sleep(5)

    # The task may have been cancelled in the window between the last validity
    # check and actually winning the lock — re-check before running, and if it
    # is gone hand the lock to the next waiter instead of downloading.
    op = dao.get_async_operation(task_id)
    if op is None or op.status != TaskStatus.QUEUED.value:
        AppLogger.info(
            "Download task cancelled after lock acquired, releasing lock: task_id=%s, model=%s",
            task_id,
            model_name,
        )
        _release_download_lock(lock_fd)
        return {
            "status": TaskStatus.INTERRUPTED.value,
            "message": "Task no longer active (cancelled/removed); download aborted",
        }

    try:
        # Re-check before applying: the runtime may have been destroyed while this
        # task was queued behind another download. The runtime download itself is
        # the bootstrap, so it is never gated on the runtime being ready.
        if model_name != _RUNTIME_RESOURCE_NAME:
            blocker = _runtime_download_blocker()
            if blocker:
                AppLogger.error(
                    "Download aborted, shared TSFM runtime is not ready: task_id=%s, model=%s, reason=%s",
                    task_id,
                    model_name,
                    blocker,
                )
                return {
                    "status": "failed",
                    "message": f"Shared TSFM runtime is not ready: {blocker}",
                }

        # Mark task as running only once the download lock is actually held. The
        # first start time is kept (see mark_operation_running): the start phase
        # of a download_start op runs on this same row.
        dao.mark_operation_running(task_id)

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

        _log_invocation(
            "resource apply",
            task_id=task_id,
            model_name=model_name,
            release=release,
            resource_base_url=resource_base_url,
            replace=replace,
        )

        AppLogger.info(
            "Starting download process for task_id: %s, model: %s, cmd: %s",
            task_id,
            model_name,
            " ".join(cmd),
        )

        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # The child owns the phases: it writes them into
        # <resource_dir>/state/tasks/<sha256>.json, and both /status and the SSE
        # stream read the current phase back from there. Nothing is polled (or
        # written to the row) here any more -- the coarser progress scale this
        # loop used to maintain only had to be kept in step with those files by
        # hand, which is how it drifted from what the stream reports.
        stdout, stderr = proc.communicate()
        if proc.returncode == 0:
            AppLogger.info("Download completed for task_id: %s, model: %s", task_id, model_name)
            return {"status": "completed", "message": None}
        else:
            error_msg = stderr.decode(errors="replace") if stderr else "Download failed"
            AppLogger.error("Download failed for task_id: %s, model: %s, error: %s", task_id, model_name, error_msg)
            return {"status": "failed", "message": error_msg}

    except subprocess.TimeoutExpired:
        error_msg = "Download timeout (>1 hour)"
        AppLogger.error("Download timeout for task_id: %s, model: %s", task_id, model_name)
        return {"status": "failed", "message": error_msg}
    except Exception as e:
        error_msg = str(e)
        AppLogger.error(
            "Download error for task_id: %s, model: %s, error: %s",
            task_id,
            model_name,
            error_msg,
        )
        return {"status": "failed", "message": error_msg}
    finally:
        # Release the download lock so the next queued download can proceed.
        _release_download_lock(lock_fd)


def _trigger_model_download_background(task_id, model_name, release, resource_base_url, replace):
    """Dispatch a download to the download thread executor.

    Executes: taosanodectl resource apply <model_name> ... inside the executor.
    The worker waits for its turn on the shared download flock before applying,
    so it never races the resource-manager file lock (which would report an
    error); that wait is ordering between this API's downloads, not permission to
    apply. Requests that arrive while another download runs simply stay queued in
    the executor / async_operations table until the turn is free.

    Args:
        task_id: Unique identifier for tracking this operation
        model_name: Name of the model to download (e.g., 'moirai')
        release: Release version (e.g., '1.0.0')
        resource_base_url: Base URL for resource repository
        replace: Whether to replace existing model
    """
    _submit_download_task(
        _run_download_task,
        args=(task_id, model_name, release, resource_base_url, replace),
        task_id=task_id,
        model_name=model_name,
        operation=OperationType.DOWNLOAD.value,
    )


def _resolve_resource_settings(request=None, payload=None):
    """Resolve resource repository settings used for taosanodectl invocations.

    Reads the configured defaults (``resource_base_url`` and
    ``tsfm_resource_exec_timeout``) and allows a per-request override of the
    base URL — taken from the query string for GET handlers or from the JSON
    body for POST handlers.

    Args:
        request: Optional Flask request. ``resource_base_url`` may be supplied
            as a query argument.
        payload: Optional request JSON dict. ``resource_base_url`` may be
            supplied in the body.

    Returns:
        tuple: (resource_base_url, exec_timeout)
    """
    config = Configure.get_instance()
    default_resource_url = config._conf.get(
        "resource_base_url", "https://download.taosdata.com"
    )

    if payload is not None:
        source = "payload"
        resource_base_url = payload.get("resource_base_url", default_resource_url)
    elif request is not None:
        source = "query"
        resource_base_url = request.args.get("resource_base_url", default_resource_url)
    else:
        source = "config"
        resource_base_url = default_resource_url

    exec_timeout = config._conf.get("tsfm_resource_exec_timeout", 120)
    _log_invocation(
        "resource settings resolved",
        source=source,
        resource_base_url=resource_base_url,
        overridden=resource_base_url != default_resource_url,
        tsfm_resource_exec_timeout=exec_timeout,
    )
    return resource_base_url, exec_timeout


# Deadline (seconds) for a model to become ready after `taosanodectl model start`
# is invoked. Used both as the child's readiness deadline (`--timeout`) and as the
# basis of the outer wait in `_run_model_start_task`.
_DEFAULT_MODEL_START_TIMEOUT = 300

# Extra grace (seconds) added on top of the model start deadline for the outer
# wait, so the child process always has a chance to report its result and exit
# before the parent gives up on it.
_MODEL_START_GRACE_SECONDS = 30


def _resolve_model_start_timeout():
    """Resolve the model start timeout used by `taosanodectl model start`.

    Reads ``tsfm_model_start_timeout`` from configuration. The value must be
    greater than or equal to the model readiness deadline
    (``timeouts['model_startup']``, default 300s) so that a slow-starting model
    is not reported as started before it is actually ready.

    Returns:
        int: Deadline in seconds for the model readiness wait.
    """
    config = Configure.get_instance()
    try:
        start_timeout = int(
            config._conf.get("tsfm_model_start_timeout", _DEFAULT_MODEL_START_TIMEOUT)
        )
    except (TypeError, ValueError):
        AppLogger.warning(
            "Invalid tsfm_model_start_timeout %r, falling back to %d seconds",
            config._conf.get("tsfm_model_start_timeout"),
            _DEFAULT_MODEL_START_TIMEOUT,
        )
        return _DEFAULT_MODEL_START_TIMEOUT

    if start_timeout <= 0:
        AppLogger.warning(
            "Non-positive tsfm_model_start_timeout %d, falling back to %d seconds",
            start_timeout,
            _DEFAULT_MODEL_START_TIMEOUT,
        )
        return _DEFAULT_MODEL_START_TIMEOUT

    _log_invocation(
        "model start timeout resolved", tsfm_model_start_timeout=start_timeout
    )
    return start_timeout


# Facts that describe *this host's* install of a model rather than the release the
# repository publishes. `resource check` cannot know them, so they are merged in
# from the CLI's local view (see _merge_local_resource_state).
_LOCAL_RESOURCE_FIELDS = ("service_status", "deployed_at")


def _local_model_state(exec_timeout):
    """Return the CLI's local per-model view, keyed by model name.

    `resource list` is the local counterpart of `resource check`: it reports what
    is installed here -- `service_status` from the pid files, `deployed_at` from
    the per-model state file -- without contacting the repository. It is read
    through the CLI for the same reason `_runtime_download_blocker` is: the
    resource layout and the state schema belong to tdgpt/script, and this module
    is a separate deployment unit that cannot import them.

    Returns:
        dict: Model name -> local entry. Empty when the view cannot be read: the
            caller still has the repository listing to return, so a failure here
            degrades the response rather than failing it -- unlike get_model_info,
            where the local view *is* the answer and a failure is an error.
    """
    try:
        taosanodectl = _get_taosanodectl_cmd()
    except FileNotFoundError as exc:
        AppLogger.warning("Cannot read the local resource view: %s", exc)
        return {}

    cmd = taosanodectl + ["resource", "list", "--format", "json"]
    _log_invocation("local resource view request", cmd=" ".join(cmd))

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=exec_timeout)
    except subprocess.TimeoutExpired:
        AppLogger.warning("Local resource view timed out (>%ss)", exec_timeout)
        return {}
    except (OSError, subprocess.SubprocessError) as exc:
        AppLogger.warning("Local resource view failed: %s", exc)
        return {}

    if result.returncode != 0:
        AppLogger.warning(
            "Local resource view exited %s: %s",
            result.returncode,
            (result.stderr or "").strip()[:500],
        )
        return {}

    try:
        payload = json.loads(result.stdout)
    except (TypeError, ValueError):
        AppLogger.warning(
            "Cannot parse the local resource view: %s", (result.stdout or "")[:200]
        )
        return {}

    models = payload.get("models") if isinstance(payload, dict) else None
    if not isinstance(models, list):
        return {}

    return {
        entry["name"]: entry
        for entry in models
        if isinstance(entry, dict) and entry.get("name")
    }


def _merge_local_resource_state(resources, exec_timeout):
    """Copy this host's service state and deploy time into a repository listing.

    The list endpoint answers "what can I install, and what do I already have?",
    so its entries come from the release manifest; a client rendering that list
    also needs to know whether each model is running and when it was deployed.
    Those two facts exist only locally, so they are merged in here.

    An entry the local view does not know keeps neither field: the model is not
    installed on this host, which `state` already reports, and reporting a
    "stopped" service for a model that has no local record would be a statement
    about nothing. The shared runtime is skipped for the same kind of reason --
    it has no worker process, so it has no service state to report.
    """
    local = _local_model_state(exec_timeout)
    if not local:
        return

    enriched = 0
    for resource in resources:
        if not isinstance(resource, dict) or resource.get("kind") == "runtime":
            continue
        entry = local.get(resource.get("name"))
        if not entry:
            continue
        for field in _LOCAL_RESOURCE_FIELDS:
            if field in entry:
                resource[field] = entry[field]
        enriched += 1

    AppLogger.debug("Merged local service state into %d model entries", enriched)


def list_all_tsfm(request):
    """Get list of available models from resource repository via taosanodectl.

    Executes: taosanodectl resource check --release <release> --resource-base-url <url>
    and, for this host's own state, taosanodectl resource list --format json.

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
        - service_status: "running" or "stopped" -- this host's model worker,
          not the repository. Absent when the model has no local record here.
        - deployed_at: install timestamp from the local state file; null when the
          model has a local record but no completed install.

    This provides a real-time view of available resources from the repository,
    with the two local-only fields (service_status, deployed_at) merged in so a
    client can render the list without a second request per model. The repository
    call is the one that talks to the network; the local view is read through the
    CLI so the list and get_model_info cannot disagree about a model's state.
    """
    AppLogger.debug("Received request to check available models, ip:%s", request.remote_addr)

    try:
        # Get parameters from query string
        release = request.args.get("release", "1.0.0")
        resource_base_url, exec_timeout = _resolve_resource_settings(request=request)
        _log_invocation(
            "resource check request",
            release=release,
            ip=request.remote_addr,
        )

        # Construct taosanodectl command
        try:
            taosanodectl = _get_taosanodectl_cmd()
        except FileNotFoundError as e:
            AppLogger.error("taosanodectl not found: %s, ip:%s", str(e), request.remote_addr)
            return {
                "status": "error",
                "message": f"taosanodectl not found: {e!s}",
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

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=exec_timeout)

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

                # The repository listing says what the release offers, not what
                # this host has running or when it deployed it: merge those two
                # local facts in before the operation status overlay.
                _merge_local_resource_state(resources, exec_timeout)

                # Overlay operation_status from SQLite (authoritative) over task file status
                for resource in resources_data.get("resources", []):
                    if "task_status" in resource:
                        del resource["task_status"]  # Remove transient task file status

                    latest_op = dao.get_latest_operation(resource.get("name", ""))
                    if latest_op:
                        if resource.get("state") == "missing" and latest_op.status == "completed":
                            pass
                        else:
                            resource["operation_status"] = latest_op.status

                    # let the frontend know that a queued downloading is inprogress for a missing model
                    if resource.get("state") == "missing" and resource.get("operation_status") == TaskStatus.QUEUED.value:
                        resource["state"] = "downloading"

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
                    "message": f"Failed to parse resource check output: {e!s}",
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
                "message": f"Resource check failed: {error_msg}",
            }, 500

    except subprocess.TimeoutExpired:
        AppLogger.error("Resource check timeout (>%ds), ip:%s", exec_timeout, request.remote_addr)
        return {
            "status": "error",
            "message": f"Resource check timeout (exceeded {exec_timeout} seconds)",
        }, 504
    except Exception as e:
        AppLogger.error("Error checking available models: %s, ip:%s", str(e), request.remote_addr)
        return {
            "status": "error",
            "message": f"Error checking available models: {e!s}",
        }, 500


def get_model_info(request, model_name):
    """Get information about a specific foundation model from local resources.

    Executes: taosanodectl resource list --format json

    Args:
        request: Flask request object
        model_name: Name of the model to retrieve (e.g., 'moirai', 'chronos')

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
        resource_base_url, exec_timeout = _resolve_resource_settings(request=request)
        _log_invocation(
            "resource list request",
            model_name=model_name,
            release=release,
            ip=request.remote_addr,
        )

        # Execute taosanodectl resource list to get real-time model info
        try:
            taosanodectl = _get_taosanodectl_cmd()
        except FileNotFoundError as e:
            AppLogger.error("taosanodectl not found: %s, ip:%s", str(e), request.remote_addr)
            return {
                "status": "error",
                "message": f"taosanodectl not found: {e!s}",
            }, 500

        cmd = taosanodectl + [
            "resource",
            "list",
            "--format",
            "json",
        ]

        AppLogger.info(
            "Executing resource list to get model info: %s, cmd: %s, ip:%s",
            model_name,
            " ".join(cmd),
            request.remote_addr,
        )

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=exec_timeout)

        if result.returncode == 0:
            try:
                # Parse JSON response from taosanodectl
                resource_data = json.loads(result.stdout)
                models = resource_data.get("models", [])

                # Find the requested model
                model = next((m for m in models if m.get("name") == model_name), None)

                if not model:
                    _reconcile_local_model_state(model_name, None)
                    AppLogger.warning(
                        "Model not found in resource list: %s, server rsp: %s, ip:%s",
                        model_name,
                        models,
                        request.remote_addr,
                    )
                    return {
                        "status": "error",
                        "message": f"Model '{model_name}' not found",
                    }, 404

                AppLogger.info(
                    "Returning model info for: %s, status: %s, ip:%s",
                    model_name,
                    model.get("service_status"),
                    request.remote_addr,
                )

                _reconcile_local_model_state(model_name, model)

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
                    "message": f"Failed to parse resource list output: {e!s}",
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
                "message": f"Resource list failed: {error_msg}",
            }, 500

    except subprocess.TimeoutExpired:
        AppLogger.error("Resource list timeout (>30s) for model %s, ip:%s", model_name, request.remote_addr)
        return {
            "status": "error",
            "message": "Resource list timeout (exceeded 30 seconds)",
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
            "message": f"Error retrieving model info: {e!s}",
        }, 500



def _validate_model_in_repository(model_name, release, resource_base_url, kind="model"):
    """Validate that a resource exists in the resource repository via taosanodectl.

    This ensures we don't reject valid new models that were added to the repository
    but not yet in the database (which may be empty on first run).

    Args:
        model_name: Name of the resource to validate, as published in the release
            manifest (a model name, or "tsfm_runtime" for the shared runtime)
        release: Release version
        resource_base_url: Base URL for resource repository
        kind: Resource kind to match: "model" (default) or "runtime"

    Returns:
        tuple: (is_valid, model_info, error_message)
        - is_valid: True if model exists and is valid
        - model_info: Full model info from repository (if valid)
        - error_message: Error description (if invalid)
    """
    try:
        _, exec_timeout = _resolve_resource_settings()
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

        _log_invocation(
            "repository validation request",
            model_name=model_name,
            release=release,
            resource_base_url=resource_base_url,
            cmd=" ".join(cmd),
        )

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=exec_timeout)

        if result.returncode != 0:
            error_msg = result.stderr or "Failed to check resources"
            return False, None, error_msg

        try:
            resources_data = json.loads(result.stdout)
            resources = resources_data.get("resources", [])

            # Match the requested kind only: the release publishes the shared
            # runtime in the same list, so a model request must not be satisfied
            # by a runtime entry of the same name (or vice versa).
            resource_info = next(
                (
                    r for r in resources
                    if r.get("name") == model_name and r.get("kind", "model") == kind
                ),
                None,
            )

            if not resource_info:
                label = "Runtime" if kind == "runtime" else "Model"
                return False, None, f"{label} '{model_name}' not found in repository"

            return True, resource_info, None

        except json.JSONDecodeError as e:
            return False, None, f"Failed to parse repository data: {e!s}"

    except subprocess.TimeoutExpired:
        return False, None, f"Repository check timeout (>{exec_timeout})"
    except Exception as e:
        return False, None, f"Error validating model: {e!s}"


_DOWNLOAD_FAMILY = frozenset({OperationType.DOWNLOAD.value, OperationType.DOWNLOAD_START.value})


def _get_active_download_op(model_name):
    """Return the active (queued/running) download-family op for a model, if any.

    Matches both DOWNLOAD and DOWNLOAD_START so that a concurrent download of
    either variant is detected and reused, preventing the same model from being
    downloaded twice.

    Args:
        model_name: Name of the model

    Returns:
        AsyncOperation or None
    """
    dao = get_dao()
    for op in dao.list_async_operations(model_name=model_name, active_only=True):
        if op.operation in _DOWNLOAD_FAMILY and op.status in (
            TaskStatus.QUEUED.value,
            TaskStatus.RUNNING.value,
        ):
            return op
    return None


def _reuse_download_response(active_op, model_name):
    """Build a response that reuses an already in-progress download task."""
    detail = _build_operation_response(active_op)
    return {
        "status": "success",
        "reused": True,
        "message": (
            f"Model '{model_name}' download already in progress "
            f"(status={active_op.status}); reusing task '{active_op.task_id}'"
        ),
        "task_id": active_op.task_id,
        "operation": active_op.operation,
        "model_name": model_name,
        "download_status": active_op.status,
        "data": detail,
    }, 200


def _claim_download_operation(dao, operation, model_name):
    """Queue a download, or hand back the one already in flight.

    The insert is atomic (see create_download_operation_if_idle), so two requests
    arriving on two gunicorn workers cannot both queue a download of the same
    resource: the loser is answered with the winner's task instead of a second
    task_id for the same work.

    Returns:
        tuple: (response, status) when another request owns the download, else
            None when this caller created the row.
    """
    if dao.create_download_operation_if_idle(operation):
        return None

    active_op = _get_active_download_op(model_name)
    if active_op is None:
        # The winner finished between the insert and this lookup, so nobody owns
        # the download any more; one retry is enough to make this caller the
        # owner. If that loses too, the next lookup below reports it.
        if dao.create_download_operation_if_idle(operation):
            return None
        active_op = _get_active_download_op(model_name)

    if active_op is not None:
        return _reuse_download_response(active_op, model_name)

    return {
        "status": "error",
        "message": (
            f"Cannot queue a download for '{model_name}': another download is in flight"
        ),
    }, 409


def download_foundation_runtime(request):
    """Install or refresh the shared TSFM runtime (venv) on its own.

    The runtime is normally installed as a side effect of downloading a model:
    `resource apply <model>` installs the platform runtime plus that model. This
    entry point installs the runtime alone, which is what an operator needs to

      * prepare the venv before (or without ever) downloading a model,
      * reinstall it after an explicit runtime destroy, or
      * repair a runtime whose interpreter or manifest no longer validates.

    There is no runtime row in the models table, so "already installed" is not a
    client-visible flag here: the underlying `resource apply` is idempotent and
    reports ``unchanged`` when the installed runtime already matches the release.
    A runtime installed from a *different* release is refused unless ``replace``
    is true.

    Expected payload:
    {
        "release": "1.0.0",                    # optional, default: "1.0.0"
        "resource_base_url": "https://...",   # optional, uses config default
        "replace": false                       # optional, allow overwrite
    }

    Returns:
        JSON response with task_id for async tracking (HTTP 202). The operation
        streams under `model_name = "tsfm_runtime"` with the `runtime_download` /
        `runtime_import` phase events, exactly like an implicit runtime install.
    """
    AppLogger.debug(
        "Received shared TSFM runtime download request, ip:%s", request.remote_addr
    )

    payload = request.get_json(silent=True) or {}
    release = payload.get("release", "1.0.0")
    resource_base_url, _ = _resolve_resource_settings(payload=payload)
    replace = payload.get("replace", False)
    _log_invocation(
        "runtime download request parameters",
        resource=_RUNTIME_RESOURCE_NAME,
        release=release,
        resource_base_url=resource_base_url,
        replace=replace,
        ip=request.remote_addr,
    )

    is_valid, runtime_info, error_msg = _validate_model_in_repository(
        _RUNTIME_RESOURCE_NAME, release, resource_base_url, kind="runtime"
    )
    if not is_valid:
        AppLogger.error(
            "Runtime download request rejected: release=%s, error: %s, ip:%s",
            release,
            error_msg,
            request.remote_addr,
        )
        return {
            "status": "error",
            "message": error_msg,
        }, 404

    # Deduplicate against a runtime download that is already queued/running.
    active_op = _get_active_download_op(_RUNTIME_RESOURCE_NAME)
    if active_op:
        return _reuse_download_response(active_op, _RUNTIME_RESOURCE_NAME)

    dao = get_dao()
    dao.cleanup_download_operations(_RUNTIME_RESOURCE_NAME)

    task_id = f"download-{_RUNTIME_RESOURCE_NAME}-{uuid.uuid4().hex[:8]}"
    rejection = _claim_download_operation(dao, AsyncOperation(
        task_id=task_id,
        operation=OperationType.DOWNLOAD.value,
        model_name=_RUNTIME_RESOURCE_NAME,
        status=TaskStatus.QUEUED.value,
        data={
            "package": runtime_info.get("file", ""),
            "release": release,
            "resource_base_url": resource_base_url,
            "replace": replace,
        },
    ), _RUNTIME_RESOURCE_NAME)
    if rejection:
        return rejection

    # The worker is driven with the resource name itself: that is what
    # `resource apply` matches on and what the resource task file records as its
    # `name`, which is how the progress poller finds it again.
    _trigger_model_download_background(
        task_id, _RUNTIME_RESOURCE_NAME, release, resource_base_url, replace
    )

    AppLogger.info(
        "Runtime download initiated for resource '%s' (release: %s, resource_url: %s, "
        "replace: %s), task_id: %s, ip:%s",
        _RUNTIME_RESOURCE_NAME,
        release,
        resource_base_url,
        replace,
        task_id,
        request.remote_addr,
    )

    return {
        "status": "queued",
        "task_id": task_id,
        "message": "Shared TSFM runtime download initiated",
        "model_name": _RUNTIME_RESOURCE_NAME,
        "package": runtime_info.get("file", ""),
        "size_gb": (runtime_info.get("size") or 0) / (1024**3),
        "release": release,
        "resource_base_url": resource_base_url,
        "next_action": f"Query progress: GET /api/v1/foundation-models/{task_id}/status",
    }, 202


def download_foundation_model(request):
    """Start downloading a foundation model from remote repository.

    This function integrates with the DynamicResourceManager to download
    and install foundation models from the TDengine GPT resource repository.
    It mirrors the behavior of: taosanodectl resource apply <model_name>

    ``model_name = "tsfm_runtime"`` installs the shared TSFM runtime instead of a
    model (see download_foundation_runtime); that is the same operation as
    POST /api/v1/foundation-models/runtime/download.

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
    resource_base_url, _ = _resolve_resource_settings(payload=payload)
    replace = payload.get("replace", False)
    _log_invocation(
        "download request parameters",
        model_name=model_name,
        release=release,
        resource_base_url=resource_base_url,
        replace=replace,
        ip=request.remote_addr,
    )

    if not model_name:
        AppLogger.error("Download request missing model_name, ip:%s", request.remote_addr)
        return {
            "status": "error",
            "message": "Missing required field: model_name",
        }, 400

    # model_name = "tsfm_runtime" installs the shared TSFM runtime instead of a model
    # (see download_foundation_runtime); same operation as
    # POST /api/v1/foundation-models/runtime/download.
    if model_name == _RUNTIME_RESOURCE_NAME:
        return download_foundation_runtime(request)

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
            "message": error_msg,
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

    # Deduplicate: if this model already has a download queued/running, return
    # the existing task instead of enqueuing another (only one download applies
    # system-wide anyway).
    active_op = _get_active_download_op(model_name)
    if active_op:
        return _reuse_download_response(active_op, model_name)

    if model.downloaded and not replace:
        AppLogger.warning(
            "Download request for already downloaded model: %s (use replace=true to force), ip:%s",
            model_name,
            request.remote_addr,
        )
        return {
            "status": "error",
            "message": f"Model '{model_name}' is already downloaded (use replace=true to force redownload)",
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
            "message": f"Cannot replace running model '{model_name}'. Stop it first.",
        }, 400

    dao.cleanup_download_operations(model_name)

    rejection = _reject_download_without_runtime(model_name, request.remote_addr)
    if rejection:
        return rejection

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
    rejection = _claim_download_operation(dao, operation, model_name)
    if rejection:
        return rejection

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


# Exit codes of the taosanode_service CLI (see script/taosanode_service.py).
# Resource operations are serialized system-wide by that process's lifecycle
# lock, so its refusals are classified instead of being reported as failures.
#
# Only the delete worker classifies 75 today. A download rarely meets it -- the
# worker holds the download flock before applying, so no *download* is usually in
# flight -- but the lifecycle lock is shared with model start/stop and import, so
# an apply can still be refused. That case is reported as a failure verbatim (see
# _run_download_task); teaching the download path to defer would be the fix, not
# widening this note.
_CLI_EXIT_TEMPFAIL = 75  # another resource operation holds the lifecycle lock
_CLI_EXIT_CONFLICT = 3  # permanent: a live worker, an unsafe target, ...

# A delete that finds the resource busy is retried, not failed: the lock it lost
# is held by the download it has to wait for. The waiting is deliberately not
# done inside the worker -- the service pool has a single worker, so sleeping
# there would starve every start/stop behind a download that can run for an hour.
# The task is released back to `queued` and resubmitted after a growing delay.
_DELETE_RETRY_BASE_SECONDS = 5
_DELETE_RETRY_MAX_DELAY_SECONDS = 60
_DELETE_RETRY_MAX_WAIT_SECONDS = 3600


def _operation_data(dao, task_id):
    """Return a mutable copy of an operation's data payload."""
    op = dao.get_async_operation(task_id)
    data = op.data if op and isinstance(op.data, dict) else {}
    return dict(data)


def _cli_reason(result):
    """Return the CLI's own one-line explanation of a refusal."""
    text = (result.stderr or "").strip() or (result.stdout or "").strip()
    return text.splitlines()[-1].strip() if text else ""


def _schedule_service_retry(worker_fn, args, task_id, model_name, operation, result):
    """Re-arm a deferred service task after the delay it asked for.

    A timer thread per deferred task keeps the wait outside the executor: the
    worker thread is released immediately, so a long download behind the retry
    cannot pin the single service worker.
    """
    delay = max(1, int(result.get("delay") or _DELETE_RETRY_BASE_SECONDS))
    timer = threading.Timer(
        delay,
        _resubmit_service_task,
        args=(worker_fn, args, task_id, model_name, operation),
    )
    timer.daemon = True
    timer.start()
    AppLogger.info(
        "Deferred %s task re-armed in %ss: task_id=%s, model=%s, waiting_for=%s",
        operation,
        delay,
        task_id,
        model_name,
        result.get("waiting_for") or "unknown",
    )


def _resubmit_service_task(worker_fn, args, task_id, model_name, operation):
    """Resubmit a deferred service task, unless it was cancelled meanwhile."""
    try:
        op = get_dao().get_async_operation(task_id)
    except Exception as exc:
        AppLogger.error(
            "Cannot re-check deferred %s task %s: %s", operation, task_id, exc
        )
        return

    if op is None or op.status != TaskStatus.QUEUED.value:
        AppLogger.info(
            "Deferred %s task is no longer queued, dropping it: task_id=%s, status=%s",
            operation,
            task_id,
            getattr(op, "status", None),
        )
        return

    _submit_service_task(
        worker_fn,
        args=args,
        task_id=task_id,
        model_name=model_name,
        operation=operation,
    )


def _submit_service_task(worker_fn, args, task_id, model_name, operation):
    """Submit a service operation (start/stop/delete) to the service executor.

    Service workers return a dict like {"status": "completed"|"failed", "message": ...}.
    Completion (including unhandled worker exceptions) is applied via
    _complete_async_operation, running in the executor thread. A single service
    executor keeps service operations serialized and independent of downloads.

    Args:
        worker_fn: Worker function taking *args and returning a result dict
        args: Positional args for worker_fn
        task_id: Unique identifier for tracking this operation
        model_name: Name of the model being operated on
        operation: Operation name (start, stop, delete)
    """

    def _on_done(future):
        """Called in the executor thread when the worker finishes."""
        try:
            result = future.result()
        except Exception as exc:
            AppLogger.error(
                "%s task raised exception: task_id=%s, model=%s, exc=%s",
                operation, task_id, model_name, exc,
            )
            _complete_async_operation(task_id, model_name, operation, "failed", str(exc))
            return

        status = result.get("status", "failed") if result else "failed"
        if status == "deferred":
            # Not terminal: the worker already returned the task to `queued`, so
            # completing it here would report an outcome that has not happened.
            # Re-arm it after its backoff instead of writing a status.
            _schedule_service_retry(
                worker_fn, args, task_id, model_name, operation, result or {}
            )
            return

        error = result.get("message") if result else ("Worker returned no result" if status == "failed" else None)
        _complete_async_operation(task_id, model_name, operation, status, error)

    executor = _get_service_executor()
    AppLogger.info(
        "Submitting %s task to service executor: task_id=%s, model=%s, executor=%s",
        operation, task_id, model_name, executor,
    )
    future = executor.submit(worker_fn, *args)
    future.add_done_callback(_on_done)
    AppLogger.info(
        "%s task submitted to service executor: task_id=%s",
        operation.capitalize(), task_id,
    )


def _trigger_model_start_background(task_id, model_name):
    """Submit model start task to the service thread executor.

    Executes: python ./taosanodectl.py model start <model_name>

    Args:
        task_id: Unique identifier for tracking this operation
        model_name: Name of the model to start (e.g., 'moirai')

    The executor handles async execution; caller monitors progress via get_operation_status(task_id)
    """
    _submit_service_task(
        _run_model_start_task,
        args=(task_id, model_name),
        task_id=task_id,
        model_name=model_name,
        operation="start",
    )


def _defer_delete(dao, task_id, model_name, state, result):
    """Release a delete back to the queue after a transient refusal.

    The refusal means another resource operation holds the lifecycle lock --
    usually the download this delete has to wait for. Rather than sleeping in
    the worker (which would pin the single service worker) the task goes back to
    `queued` and is resubmitted after a growing delay, with the wait recorded so
    /status and SSE can show what it is waiting for.

    Returns:
        dict: A ``deferred`` result for the submitter's callback, or a ``failed``
            one once the total wait budget is spent.
    """
    now = time.time()
    attempt = int(state.get("attempt") or 0) + 1
    wait_since = float(state.get("wait_since") or now)
    waited = now - wait_since
    waiting_for = _cli_reason(result)

    if waited >= _DELETE_RETRY_MAX_WAIT_SECONDS:
        message = (
            f"Timed out after {int(waited)}s waiting for an in-flight resource "
            f"operation to finish before deleting '{model_name}'"
            + (f": {waiting_for}" if waiting_for else "")
        )
        AppLogger.error(
            "Delete gave up waiting for the resource operation: task_id=%s, model=%s, "
            "attempt=%d, waited=%ds, reason=%s",
            task_id,
            model_name,
            attempt,
            int(waited),
            waiting_for or "unknown",
        )
        return {"status": "failed", "message": message}

    delay = min(
        _DELETE_RETRY_BASE_SECONDS * (2 ** (attempt - 1)),
        _DELETE_RETRY_MAX_DELAY_SECONDS,
    )
    state.update({
        "attempt": attempt,
        "wait_since": wait_since,
        "waiting_for": waiting_for,
        "retry_after": delay,
    })
    dao.update_async_operation(
        task_id,
        status=TaskStatus.QUEUED.value,
        progress=0,
        data=state,
        error=None,
    )
    AppLogger.info(
        "Delete deferred behind an in-flight resource operation: task_id=%s, model=%s, "
        "attempt=%d, waited=%ds, retry_in=%ds, held_by=%s",
        task_id,
        model_name,
        attempt,
        int(waited),
        delay,
        waiting_for or "unknown",
    )
    return {"status": "deferred", "delay": delay, "waiting_for": waiting_for}


def _run_model_delete_task(task_id, model_name):
    """Worker function for the service thread executor to delete a resource.

    This runs in an executor thread; completion is applied by the caller via
    _complete_async_operation after this returns its result dict.

    A transient refusal (the lifecycle lock is held by another resource
    operation -- typically the download this delete must wait for) defers the
    task instead of failing it (see _defer_delete). A permanent refusal still
    fails it, carrying the CLI's own explanation.
    """
    exec_timeout = None
    try:
        dao = get_dao()
        state = _operation_data(dao, task_id)
        dao.mark_operation_running(task_id)

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

        _, exec_timeout = _resolve_resource_settings()
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=exec_timeout)

        if result.returncode == 0:
            AppLogger.info(
                "Model delete completed for task_id: %s, model: %s",
                task_id,
                model_name,
            )
            return {"status": "completed", "message": None}

        if result.returncode == _CLI_EXIT_TEMPFAIL:
            return _defer_delete(dao, task_id, model_name, state, result)

        error_msg = result.stderr or (
            f"stdout: {result.stdout.strip()}" if result.stdout.strip() else "Model delete failed"
        )
        AppLogger.error(
            "Model delete failed for task_id: %s, model: %s, error: %s",
            task_id,
            model_name,
            error_msg,
        )
        return {"status": "failed", "message": error_msg}

    except subprocess.TimeoutExpired:
        error_msg = f"Model delete timeout {exec_timeout}sec"
        AppLogger.error("Model delete timeout for task_id: %s, model: %s", task_id, model_name)
        return {"status": "failed", "message": error_msg}
    except Exception as e:
        error_msg = str(e)
        AppLogger.error(
            "Model delete error for task_id: %s, model: %s, error: %s",
            task_id,
            model_name,
            error_msg,
        )
        return {"status": "failed", "message": error_msg}


def _trigger_model_delete_background(task_id, model_name):
    """Submit model delete task to the service thread executor.

    Executes: python ./taosanodectl.py resource delete <model_name>

    Args:
        task_id: Unique identifier for tracking this operation
        model_name: Name of the model to delete (e.g., 'moirai')

    The executor handles async execution; caller monitors progress via get_operation_status(task_id)
    """
    _submit_service_task(
        _run_model_delete_task,
        args=(task_id, model_name),
        task_id=task_id,
        model_name=model_name,
        operation="delete",
    )


def _run_model_start_task(task_id, model_name):
    """Worker function for the service thread executor to start a model via taosanodectl.

    This runs in an executor thread; completion is applied by the caller via
    _complete_async_operation after this returns its result dict.

    Uses Popen with start_new_session=True so the model service process is detached
    from the RESTful server's process group. This prevents the model service from
    being killed when the RESTful server stops (SIGHUP/SIGTERM to process group).

    The readiness deadline is read from the ``tsfm_model_start_timeout`` config
    setting and forwarded to ``taosanodectl model start --timeout``, so the
    parent's wait and the child's own readiness check share one value.
    """
    try:
        dao = get_dao()
        dao.mark_operation_running(task_id)

        taosanodectl = _get_taosanodectl_cmd()
        start_timeout = _resolve_model_start_timeout()

        cmd = taosanodectl + [
            "model",
            "start",
            model_name,
            "--timeout",
            str(start_timeout),
        ]

        _log_invocation(
            "model start",
            task_id=task_id,
            model_name=model_name,
            start_timeout=start_timeout,
            outer_timeout=start_timeout + _MODEL_START_GRACE_SECONDS,
        )

        AppLogger.info(
            "Starting model process for task_id: %s, model: %s, timeout: %ds, cmd: %s",
            task_id,
            model_name,
            start_timeout,
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

        # `taosanodectl model start` performs the real readiness check (the model
        # service must accept TCP connections) and exits with 0/1, so a successful
        # start returns as soon as the model is ready rather than blocking for the
        # whole window. The outer deadline is slightly larger than the child's own
        # readiness deadline so the child can always report its verdict first.
        outer_timeout = start_timeout + _MODEL_START_GRACE_SECONDS
        try:
            _, stderr = proc.communicate(timeout=outer_timeout)
            returncode = proc.returncode
        except subprocess.TimeoutExpired:
            # The child did not report an outcome within the deadline, so the real
            # startup state is unknown. Leave the detached process running (do not
            # kill it) but never report success for an unknown result.
            error_msg = (
                f"Model start timed out after {outer_timeout}sec "
                f"(start timeout {start_timeout}sec)"
            )
            AppLogger.error(
                "Model start timeout: task_id=%s, model=%s, pid=%d, timeout=%dsec",
                task_id,
                model_name,
                proc.pid,
                outer_timeout,
            )
            return {"status": "failed", "message": error_msg}

        if returncode == 0:
            AppLogger.info(
                "Model start completed: task_id=%s, model=%s, pid=%d",
                task_id,
                model_name,
                proc.pid,
            )
            return {"status": "completed", "message": None}
        else:
            error_msg = stderr.decode(errors="replace").strip() if stderr else "Model start failed"
            AppLogger.error(
                "Model start failed: task_id=%s, model=%s, returncode=%d, error=%s",
                task_id,
                model_name,
                returncode,
                error_msg,
            )
            return {"status": "failed", "message": error_msg}

    except Exception as e:
        error_msg = str(e)
        AppLogger.error(
            "Model start error: task_id=%s, model=%s, error=%s",
            task_id,
            model_name,
            error_msg,
        )
        return {"status": "failed", "message": error_msg}



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
            "message": f"Model '{model_name}' not found",
        }, 404

    # Validate model state
    is_valid, error_msg = validate_fn(model, model_name, operation.capitalize(), request.remote_addr)
    if not is_valid:
        return {
            "status": "error",
            "message": error_msg,
        }, 400

    # Generate task ID for async tracking
    task_id = f"{operation_type}-{model_name}-{uuid.uuid4().hex[:8]}"
    _log_invocation(
        f"{operation} operation requested",
        model_name=model_name,
        task_id=task_id,
        ip=request.remote_addr,
    )

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
            "message": "Missing required field: model_name",
        }, 400

    return _create_async_operation_task(
        request,
        model_name,
        "start",
        "start",
        _validate_start_state,
        _trigger_model_start_background,
    )


def _run_download_and_start_task(task_id, model_name, release, resource_base_url, replace):
    """Worker function for the download executor: download then auto-start a model.

    This runs in an executor thread. It composes the existing download and start
    workers: the model is downloaded first (waiting for its turn on the shared
    download flock like any download) and, only on a successful download, the
    model service is started.

    Args:
        task_id: Unique identifier for tracking this operation
        model_name: Name of the model to download and start (e.g., 'moirai')
        release: Release version (e.g., '1.0.0')
        resource_base_url: Base URL for resource repository
        replace: Whether to replace an existing (downloaded) model

    Returns:
        dict: Result with {"status": "completed"|"failed", "message": ...}
    """
    # Phase 1: download the model
    download_result = _run_download_task(task_id, model_name, release, resource_base_url, replace)
    if not download_result or download_result.get("status") != TaskStatus.COMPLETED.value:
        # If the whole download_start op was cancelled/removed while waiting for
        # the download lock, keep the interrupted semantics instead of masking
        # it as a generic download failure.
        if download_result and download_result.get("status") == TaskStatus.INTERRUPTED.value:
            return download_result

        error = (download_result or {}).get("message") or "Download phase failed"
        AppLogger.error(
            "download_start: download phase failed, task_id: %s, model: %s, error: %s",
            task_id,
            model_name,
            error,
        )
        return {"status": TaskStatus.FAILED.value, "message": error}

    # Mark model as downloaded in the database before starting it, so that a
    # start-phase failure still leaves the model downloaded and usable.
    _update_model_state(model_name, downloaded=True)
    AppLogger.info(
        "download_start: download completed for task_id: %s, model: %s, auto-starting model service",
        task_id,
        model_name,
    )

    # Phase 2: start the model service via the service executor so that this
    # start is serialized with any concurrent stop/delete on the same model.
    # Submitting here and blocking (future.result()) keeps the download executor
    # thread occupied only during the brief start window, without any deadlock
    # risk (both executors are independent single-thread pools).
    service_executor = _get_service_executor()
    future = service_executor.submit(_run_model_start_task, task_id, model_name)
    return future.result()


def _trigger_model_download_start_background(task_id, model_name, release, resource_base_url, replace):
    """Dispatch a combined download-then-start task to the download executor.

    Executes: taosanodectl resource apply <model_name> then model start, inside
    the executor. The download phase waits for its turn on the shared download
    flock like any plain download; the model is auto-started only after the
    download succeeds.

    Args:
        task_id: Unique identifier for tracking this operation
        model_name: Name of the model to download and start (e.g., 'moirai')
        release: Release version (e.g., '1.0.0')
        resource_base_url: Base URL for resource repository
        replace: Whether to replace existing model
    """
    _submit_download_task(
        _run_download_and_start_task,
        args=(task_id, model_name, release, resource_base_url, replace),
        task_id=task_id,
        model_name=model_name,
        operation=OperationType.DOWNLOAD_START.value,
    )


def _runtime_verify_reason(stdout):
    """Extract the runtime's failure reason from `resource verify` JSON output."""
    try:
        payload = json.loads(stdout)
    except (TypeError, ValueError):
        return ""
    if not isinstance(payload, dict):
        return ""
    for entry in payload.get("resources", []):
        if isinstance(entry, dict) and entry.get("kind") == "runtime" and entry.get("status") != "valid":
            return str(entry.get("reason") or "")
    return ""


def _runtime_download_blocker():
    """Return why a model download cannot start yet, or None when the runtime is ready.

    Model workers execute from the shared TSFM runtime, so a model must not be
    fetched while that runtime is missing or unusable.

    The check runs `resource verify <runtime>` in a child process instead of
    validating the venv here: the layout and manifest schema are owned by
    tdgpt/script, and this module is a separate deployment unit that cannot
    import it. It is a local check -- no repository access -- and it is the same
    command an operator would run by hand.

    Returns:
        str: Reason the runtime is unusable, or None when it is ready.
    """
    try:
        taosanodectl = _get_taosanodectl_cmd()
    except FileNotFoundError as exc:
        return str(exc)

    _, exec_timeout = _resolve_resource_settings()
    cmd = taosanodectl + [
        "resource",
        "verify",
        _RUNTIME_RESOURCE_NAME,
        "--format",
        "json",
    ]
    _log_invocation("runtime readiness probe", cmd=" ".join(cmd))

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=exec_timeout)
    except subprocess.TimeoutExpired:
        return f"runtime readiness check timed out (>{exec_timeout}s)"
    except (OSError, subprocess.SubprocessError) as exc:
        return f"unable to check the shared TSFM runtime: {exc}"

    if result.returncode == 0:
        return None

    reason = _runtime_verify_reason(result.stdout)
    if not reason:
        AppLogger.warning(
            "Cannot determine why the shared TSFM runtime is not ready (exit=%s): %s",
            result.returncode,
            (result.stderr or "").strip()[:500],
        )
        return "shared TSFM runtime is not installed or invalid"
    return reason


def _reject_download_without_runtime(model_name, ip):
    """Build the 400 response for a model download whose runtime is not ready.

    A runtime download that is already queued or running counts as "will be
    ready": the model task is allowed to queue behind it instead of being
    rejected, even though the runtime directory is still missing. That is what
    makes asking for the runtime and then for a model work back to back without
    waiting for the first to finish -- the download lock serializes the two, and
    the worker re-checks readiness before applying the model.

    Returns:
        tuple: (response_dict, 400) when the runtime is unusable, else None.
    """
    runtime_download = _get_active_download_op(_RUNTIME_RESOURCE_NAME)
    if runtime_download:
        AppLogger.info(
            "Model download allowed while the shared TSFM runtime is being downloaded: "
            "model=%s, runtime_task=%s, runtime_status=%s, ip:%s",
            model_name,
            runtime_download.task_id,
            runtime_download.status,
            ip,
        )
        return None

    blocker = _runtime_download_blocker()
    if not blocker:
        return None

    AppLogger.error(
        "Model download rejected, shared TSFM runtime is not ready: model=%s, reason=%s, ip:%s",
        model_name,
        blocker,
        ip,
    )
    return {
        "status": "error",
        "message": (
            f"Shared TSFM runtime is not ready: {blocker}. "
            "Install it first: POST /api/v1/foundation-models/runtime/download"
        ),
    }, 400


def download_and_start_foundation_model(request):
    """Download a foundation model and automatically start it once downloaded.

    Combines the download (download_foundation_model) and start
    (start_foundation_model) flows into a single async operation. If the model
    is already downloaded (and replace is not requested), only the start step is
    performed and the download step is skipped.

    ``model_name = "tsfm_runtime"`` has no model worker to start, so it degrades to a
    plain runtime download instead of failing.

    Expected payload:
    {
        "model_name": "moirai",
        "release": "1.0.0",                    # optional, default: "1.0.0"
        "resource_base_url": "https://...",   # optional, uses config default
        "replace": false                       # optional, allow overwrite + redownload
    }

    Returns:
        JSON response with task_id for async tracking (HTTP 202)
    """
    AppLogger.debug("Received foundation model download-and-start request, ip:%s", request.remote_addr)

    payload = request.get_json(silent=True) or {}
    model_name = payload.get("model_name")
    release = payload.get("release", "1.0.0")

    # Use provided resource_base_url or fall back to configured default
    resource_base_url, _ = _resolve_resource_settings(payload=payload)
    replace = payload.get("replace", False)
    _log_invocation(
        "download-and-start request parameters",
        model_name=model_name,
        release=release,
        resource_base_url=resource_base_url,
        replace=replace,
        ip=request.remote_addr,
    )

    if not model_name:
        AppLogger.error("Download-and-start request missing model_name, ip:%s", request.remote_addr)
        return {
            "status": "error",
            "message": "Missing required field: model_name",
        }, 400

    # The shared runtime has no model worker to start, so a download-start
    # request for it degrades to a plain runtime download.
    if model_name == _RUNTIME_RESOURCE_NAME:
        response, code = download_foundation_runtime(request)
        if isinstance(response, dict) and response.get("status") == "queued":
            response["message"] = (
                "Shared TSFM runtime download initiated (no model worker to start)"
            )
        return response, code

    dao = get_dao()
    model = dao.get_model(model_name)

    # A running model cannot be replaced or started again.
    if model and model.running:
        AppLogger.error(
            "Download-and-start request for already running model: %s, ip:%s",
            model_name,
            request.remote_addr,
        )
        return {
            "status": "error",
            "message": f"Model '{model_name}' is already running. Stop it first.",
        }, 400

    # Already downloaded and no forced redownload requested: skip the download
    # step and simply start the model service.
    if model and model.downloaded and not replace:
        AppLogger.info(
            "Download-and-start request for already downloaded model: %s — download skipped, starting service, ip:%s",
            model_name,
            request.remote_addr,
        )
        response, code = start_foundation_model(request)
        if isinstance(response, dict) and response.get("status") == "queued":
            response["message"] = (
                f"Model '{model_name}' is already downloaded; start initiated (download skipped)"
            )
        return response, code

    # Validate the model exists in the repository (real-time check). Only needed
    # when a download is actually going to run.
    is_valid, model_info, error_msg = _validate_model_in_repository(model_name, release, resource_base_url)
    if not is_valid:
        AppLogger.error(
            "Download-and-start request for invalid model: %s, error: %s, ip:%s",
            model_name,
            error_msg,
            request.remote_addr,
        )
        return {
            "status": "error",
            "message": error_msg,
        }, 404

    # If the model is not in the database yet, add it from the repository info.
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

    # Deduplicate: if this model already has a download-and-start queued/running,
    # reuse that task instead of enqueuing a duplicate.
    active_op = _get_active_download_op(model_name)
    if active_op:
        return _reuse_download_response(active_op, model_name)

    dao.cleanup_download_operations(model_name)

    rejection = _reject_download_without_runtime(model_name, request.remote_addr)
    if rejection:
        return rejection

    # Generate a unique task ID for async tracking.
    task_id = f"download-start-{model_name}-{uuid.uuid4().hex[:8]}"

    # Record the combined async operation. The worker downloads the model and,
    # only on success, starts the model service.
    operation = AsyncOperation(
        task_id=task_id,
        operation=OperationType.DOWNLOAD_START.value,
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
    rejection = _claim_download_operation(dao, operation, model_name)
    if rejection:
        return rejection

    # Spawn the background download-then-start task.
    _trigger_model_download_start_background(
        task_id, model_name, release, resource_base_url, replace
    )

    AppLogger.info(
        "Download-and-start initiated for model '%s' (package: %s, release: %s, resource_url: %s, "
        "replace: %s), task_id: %s, ip:%s",
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
        "message": f"Download initiated for model '{model_name}'; it will start automatically once the download completes",
        "model_name": model_name,
        "model_id": model.model_id,
        "package": model.package,
        "size_gb": model.size_gb,
        "release": release,
        "resource_base_url": resource_base_url,
        "auto_start": True,
        "next_action": f"Query progress: GET /api/v1/foundation-models/{task_id}/status",
    }, 202


def _live_worker_names():
    """Return the model workers whose process is actually alive, or None if unknown.

    Reads the pid files through `taosanodectl model status` -- the same source
    taosanode_service.py uses to decide whether a worker exists -- instead of the
    cached `models.running` column. That column is only written when *this* API
    reports a successful start, so it goes stale in both directions: a crashed
    worker keeps looking running, and a worker started from the CLI looks stopped.

    Returns:
        list[str]: Names of models with a live worker process (sorted).
        None: Liveness could not be determined -- callers must not read that as
            "nothing is running"; the resource-delete guard in
            taosanode_service.py checks the same pids again before removing
            anything, so an unknown answer is safe to pass through.
    """
    try:
        taosanodectl = _get_taosanodectl_cmd()
    except FileNotFoundError as exc:
        AppLogger.warning("Cannot probe model worker liveness: %s", exc)
        return None

    _, exec_timeout = _resolve_resource_settings()
    cmd = taosanodectl + ["model", "status", "--format", "json"]
    _log_invocation("model worker liveness probe", cmd=" ".join(cmd))

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=exec_timeout)
    except subprocess.TimeoutExpired:
        AppLogger.warning("Model worker liveness probe timed out (>%ss)", exec_timeout)
        return None
    except (OSError, subprocess.SubprocessError) as exc:
        AppLogger.warning("Model worker liveness probe failed: %s", exc)
        return None

    if result.returncode != 0:
        AppLogger.warning(
            "Model worker liveness probe exited %s: %s",
            result.returncode,
            (result.stderr or "").strip()[:500],
        )
        return None

    try:
        statuses = json.loads(result.stdout)
    except (TypeError, ValueError):
        AppLogger.warning(
            "Cannot parse model worker liveness output: %s", (result.stdout or "")[:200]
        )
        return None

    if not isinstance(statuses, list):
        return None

    return sorted(
        str(entry.get("model"))
        for entry in statuses
        if isinstance(entry, dict) and entry.get("running") and entry.get("model")
    )


def reconcile_model_running_state():
    """Rebuild the cached `running` flags from the workers that are actually alive.

    A model worker is not a child of this service: Models.start detaches it into a
    session of its own, so it survives a restart of the API. The cached column,
    however, is only written when *this* API observes a start or a stop, so after
    a restart it can hold either answer wrongly -- and both directions are
    user-visible. A dead worker that still looks running refuses every start; a
    live worker that looks stopped refuses every stop, and a forced replace is
    allowed to overwrite the files it has loaded.

    So the pid files are asked instead (the same source the delete path trusts)
    and their answer is written. Liveness that could not be probed changes
    nothing: an unknown answer is not evidence that a model is stopped.

    Returns:
        int: Number of model rows whose flag was corrected.
    """
    live = _live_worker_names()
    if live is None:
        AppLogger.warning(
            "Skipping model running-state reconciliation: worker liveness is unknown"
        )
        return 0

    running_names = set(live)
    dao = get_dao()
    corrected = 0
    for model in dao.list_models():
        is_running = model.name in running_names
        if model.running == is_running:
            continue
        if dao.update_model(model.name, running=is_running):
            corrected += 1
            AppLogger.info(
                "Reconciled model '%s' running state: %s -> %s",
                model.name,
                model.running,
                is_running,
            )

    AppLogger.info(
        "Model running-state reconciliation complete: %d corrected (live workers: %s)",
        corrected,
        ", ".join(live) or "none",
    )
    return corrected


def _runtime_delete_blocker(dao):
    """Return why the shared TSFM runtime must not be destroyed, if at all.

    Exactly two conditions refuse the request:

    1. a model worker process that is *actually alive* (pid-file liveness, not the
       cached `models.running` column);
    2. a model start that has not finished yet (queued/running). That window has
       no pid to observe -- the worker process does not exist until the start
       spawns it -- and it is about to create one.

    Nothing else blocks. An `interrupted` operation row is history and is never
    reclaimed, so refusing on it used to lock this endpoint permanently (a single
    restart during a start or download was enough); and an in-flight download is
    not a refusal reason either -- the lifecycle lock in taosanode_service.py
    serializes it against this delete, which is why the delete is retried until
    that lock is free instead of failing fast here.

    Returns:
        str: Error message, or None when deleting the runtime is safe.
    """
    live = _live_worker_names()
    if live:
        return (
            "Cannot delete shared TSFM runtime while model workers are running: "
            + ", ".join(live)
        )

    # Filter on queued/running explicitly: active_only also returns interrupted
    # rows, and those must never be a reason to refuse.
    starting = sorted(
        {
            op.model_name
            for op in dao.list_async_operations(active_only=True)
            if op.operation == OperationType.START.value
            and op.status in (TaskStatus.QUEUED.value, TaskStatus.RUNNING.value)
        }
    )
    if starting:
        return (
            "Cannot delete shared TSFM runtime while model workers are starting: "
            + ", ".join(starting)
        )

    return None


def _start_delete_operation(model_name, message):
    """Queue the async delete of one resource and build its 202 response.

    Shared by the model-delete and runtime-destroy entry points: both record the
    same kind of operation row and hand it to the same background worker, so
    only the validation performed before this call differs.

    Args:
        model_name: Resource to delete; ``_RUNTIME_RESOURCE_NAME`` for the
            shared TSFM runtime, a model name otherwise.
        message: Human readable message for the response body.

    Returns:
        tuple: (response_dict, 202)
    """
    dao = get_dao()
    task_id = f"delete-{model_name}-{uuid.uuid4().hex[:8]}"
    _log_invocation("delete operation queued", model_name=model_name, task_id=task_id)
    dao.create_async_operation(AsyncOperation(
        task_id=task_id,
        operation=OperationType.DELETE.value,
        model_name=model_name,
        status=TaskStatus.QUEUED.value,
    ))
    _trigger_model_delete_background(task_id, model_name)
    return {
        "status": "queued",
        "task_id": task_id,
        "message": message,
        "model_name": model_name,
        "next_action": f"Query progress: GET /api/v1/foundation-models/{task_id}/status",
    }, 202


def delete_foundation_runtime(request):
    """Destroy the shared TSFM runtime (venv) environment.

    The runtime is the shared Python environment every dynamically installed
    TSFM model worker executes from. Destroying it removes that directory tree
    from disk; installed model directories are left untouched, but they cannot
    start again until a runtime is installed (downloaded or imported) anew.

    Refused while a model worker is running or still starting, because the
    runtime it is executing from would be removed underneath it.

    Also reachable through the model delete endpoint by passing
    ``model_name = "tsfm_runtime"``, which is how that endpoint routes here.

    Returns:
        JSON response with task_id for async tracking (HTTP 202), or HTTP 400
        when a model worker is running or starting.
    """
    AppLogger.debug(
        "Received shared TSFM runtime delete request, ip:%s", request.remote_addr
    )

    dao = get_dao()
    blocker = _runtime_delete_blocker(dao)
    if blocker:
        AppLogger.error("%s, ip:%s", blocker, request.remote_addr)
        return {
            "status": "error",
            "message": blocker,
        }, 400

    response, code = _start_delete_operation(
        _RUNTIME_RESOURCE_NAME, "Shared TSFM runtime deletion initiated"
    )
    AppLogger.info(
        "Shared TSFM runtime deletion initiated, task_id: %s, ip:%s",
        response["task_id"],
        request.remote_addr,
    )
    return response, code


def ensure_pending_deletes_resumed():
    """Re-arm the deletes a restart interrupted; safe to call on every request.

    The first request a worker process handles is what re-arms them: a client
    that asked for a delete keeps polling its task, so that request reliably
    arrives. The flag is set before the work is attempted, because a failure
    here must not be retried on every following request; a task that still could
    not be re-armed stays `queued` and is dropped by the next restart.
    """
    global _PENDING_DELETES_RESUMED
    if _PENDING_DELETES_RESUMED:
        return
    _PENDING_DELETES_RESUMED = True
    resume_pending_delete_operations()


def resume_pending_delete_operations():
    """Resubmit the deletes a service restart asked to resume.

    Runs once per worker process (see ensure_pending_deletes_resumed): the
    executor queues are process-local, so neither the preloading master nor a
    module import can run these tasks. Claiming is atomic (see
    FoundationModelsDAO.claim_requeued_delete), so exactly one worker picks each
    task up and the others see the flag already taken.
    """
    try:
        dao = get_dao()
        pending = dao.list_requeued_deletes()
    except Exception as exc:
        AppLogger.error("Cannot look up resumable deletes: %s", exc)
        return

    for op in pending:
        if not dao.claim_requeued_delete(op):
            continue
        AppLogger.info(
            "Resuming delete after service restart: task_id=%s, resource=%s",
            op.task_id,
            op.model_name,
        )
        _submit_service_task(
            _run_model_delete_task,
            args=(op.task_id, op.model_name),
            task_id=op.task_id,
            model_name=op.model_name,
            operation=OperationType.DELETE.value,
        )


def delete_foundation_model(request):
    """Delete a foundation model.

    Expected payload:
    {
        "model_name": "embedding-model-v1"
    }

    ``model_name = "tsfm_runtime"`` destroys the shared TSFM runtime instead of a
    model (see delete_foundation_runtime); that is the same operation as
    POST /api/v1/foundation-models/runtime/delete.

    Returns:
        JSON response with task_id for async tracking (HTTP 202)
    """
    AppLogger.debug("Received foundation model delete request, ip:%s", request.remote_addr)

    payload = request.get_json(silent=True) or {}
    model_name = payload.get("model_name")

    if not isinstance(model_name, str) or not model_name:
        AppLogger.error("Delete request missing model_name, ip:%s", request.remote_addr)
        return {
            "status": "error",
            "message": "Missing required field: model_name",
        }, 400

    if model_name == _RUNTIME_RESOURCE_NAME:
        return delete_foundation_runtime(request)

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
            "message": f"Model '{model_name}' not found",
        }, 404

    # Ask the pid files rather than the cached `models.running` column: a worker
    # started from the CLI, or one that outlived a restart, is not reflected
    # there. When liveness cannot be determined the request is let through -- the
    # resource-delete command checks the same pids again and refuses with a clear
    # reason, so an unknown answer never deletes a live worker's directory.
    live = _live_worker_names()
    if live and model_name in live:
        return {
            "status": "error",
            "message": f"Cannot delete running model '{model_name}'. Stop it first.",
        }, 400

    response, code = _start_delete_operation(
        model_name, f"Model '{model_name}' deletion initiated"
    )
    AppLogger.info(
        "Delete initiated for model '%s', task_id: %s, ip:%s",
        model_name,
        response["task_id"],
        request.remote_addr,
    )
    return response, code


def _run_model_stop_task(task_id, model_name):
    """Worker function for the service thread executor to stop a model via taosanodectl.

    This runs in an executor thread; completion is applied by the caller via
    _complete_async_operation after this returns its result dict.
    """
    try:
        dao = get_dao()
        dao.mark_operation_running(task_id)

        taosanodectl = _get_taosanodectl_cmd()
        _, exec_timeout = _resolve_resource_settings()

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

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=exec_timeout)

        if result.returncode == 0:
            AppLogger.info(
                "Model stop completed for task_id: %s, model: %s",
                task_id,
                model_name,
            )
            return {"status": "completed", "message": None}
        else:
            error_msg = result.stderr or "Model stop failed"
            AppLogger.error(
                "Model stop failed for task_id: %s, model: %s, error: %s",
                task_id,
                model_name,
                error_msg,
            )
            return {"status": "failed", "message": error_msg}

    except subprocess.TimeoutExpired:
        error_msg = f"Model stop timeout {exec_timeout}sec"
        AppLogger.error("Model stop timeout for task_id: %s, model: %s", task_id, model_name)
        return {"status": "failed", "message": error_msg}
    except Exception as e:
        error_msg = str(e)
        AppLogger.error(
            "Model stop error for task_id: %s, model: %s, error: %s",
            task_id,
            model_name,
            error_msg,
        )
        return {"status": "failed", "message": error_msg}


def _trigger_model_stop_background(task_id, model_name):
    """Submit model stop task to the service thread executor.

    Executes: python ./taosanodectl.py model stop <model_name>

    Args:
        task_id: Unique identifier for tracking this operation
        model_name: Name of the model to stop (e.g., 'moirai')

    The executor handles async execution; caller monitors progress via get_operation_status(task_id)
    """
    _submit_service_task(
        _run_model_stop_task,
        args=(task_id, model_name),
        task_id=task_id,
        model_name=model_name,
        operation="stop",
    )


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
            "message": "Missing required field: model_name",
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

    if operation in (OperationType.DOWNLOAD.value, OperationType.DOWNLOAD_START.value):
        data = operation_info.data or {}
        response_data.update({
            "model_id": data.get("model_id", ""),
            "package": data.get("package", ""),
            "release": data.get("release", "1.0.0"),
            "resource_base_url": data.get("resource_base_url", ""),
            "replace": data.get("replace", False),
        })

    if operation == OperationType.DOWNLOAD_START.value:
        response_data["auto_start"] = True

    # A download is reported in the same per-phase terms the SSE stream uses: the
    # phase it is in, and that phase's own 0-100 progress. There is deliberately
    # no monotonic whole-operation value any more, so a client that falls back to
    # polling renders exactly the bars a streaming client does.
    _apply_download_phase(response_data, operation_info)

    # A delete that is waiting for an in-flight resource operation is still
    # `queued`: say what it waits for, so a client can tell waiting from stuck.
    if operation_info.status == TaskStatus.QUEUED.value:
        data = operation_info.data if isinstance(operation_info.data, dict) else {}
        if data.get("waiting_for"):
            response_data["waiting_for"] = data["waiting_for"]
            response_data["attempt"] = data.get("attempt", 0)
            response_data["retry_after"] = data.get("retry_after")

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
            "message": f"Task '{task_id}' not found",
        }, 404

    response_data = _build_operation_response(operation_info)
    return {
        "status": "success",
        "data": response_data,
    }, 200


# A download operation installs exactly one resource -- the shared TSFM runtime
# (venv) or a model -- and that resource goes through two sequential phases:
# download the archive, then unpack/install it. The whole request stays ONE row
# in async_operations.
#
# So that clients can label and render each phase separately, every phase is
# pushed as its own synthetic operation name with its own 0-100 progress, and the
# operation's own row is never streamed as `download`. These names are never
# written to async_operations and cannot be queried through /status; only the real
# operation row can. The operation lifecycle (single-flight de-duplication,
# cancellation, terminal state) stays exactly as it is.
#
# A download_start request has one step more than a plain download: after the
# install it starts the model worker. That step is reported too, as `start` --
# the same name an explicit start operation streams under, so a client renders the
# auto-start with the code path and label it already has for a start it asked for.
# See _auto_start_phase_events: it is derived from the operation row, because
# there is no archive or byte counter behind it.
_RUNTIME_DOWNLOAD = "runtime_download"
_RUNTIME_IMPORT = "runtime_import"
_MODEL_DOWNLOAD = "model_download"
_MODEL_IMPORT = "model_import"
_MODEL_START = "start"

# How long after a download-family operation finished its phases are replayed to
# a client that subscribes to the stream. A reconnected EventSource or a reloaded
# page has no phase history of its own, and the stream only closes out streams it
# reported itself -- without this the client would never learn how the operation
# ended. The window is deliberately short: it covers a reconnect around the
# completion without handing a long-lived client the history of everything that
# finished while it was away.
_FINISHED_DOWNLOAD_REPLAY_SECONDS = 60

# Per-resource task states (written by taosanodectl into
# <resource_dir>/state/tasks/<sha256>.json), grouped by phase.
_DOWNLOAD_PHASE_STATES = frozenset({"downloading", "verifying"})
_IMPORT_PHASE_STATES = frozenset({"importing"})

# `downloaded` means the archive is complete and verified on disk: the download
# phase is finished, but the install phase has not reported a state of its own yet.
_DOWNLOAD_COMPLETE_STATES = frozenset({"downloaded"})

# Terminal statuses of the operation row itself, as opposed to the task-file
# status of one resource.
_TERMINAL_OPERATION_STATES = (
    TaskStatus.COMPLETED.value,
    TaskStatus.FAILED.value,
    TaskStatus.INTERRUPTED.value,
)


def _parse_resource_timestamp(value):
    """Parse the `updated_at` field written into a resource task file."""
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _read_resource_task_states():
    """Read the per-resource task files written by taosanodectl.

    Returns an empty list when ``resource_dir`` is not configured or the task
    directory does not exist yet, so the SSE stream degrades to model-only
    updates instead of failing.
    """
    resource_dir = Configure.get_instance()._conf.get("resource_dir", "")
    if not resource_dir:
        return []

    task_root = Path(resource_dir) / "state" / "tasks"
    if not task_root.is_dir():
        return []

    states = []
    for task_file in task_root.glob("*.json"):
        try:
            with task_file.open() as file_obj:
                value = json.load(file_obj)
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(value, dict):
            states.append(value)
    return states


def _find_resource_state(op_info, task_states, kind, name=None):
    """Return the task state written for one resource during this operation.

    Task files are keyed by archive SHA-256 and persist across runs, so a file
    from an earlier attempt must not be mistaken for the current one: only
    entries updated after the operation started (or was created, while it is
    still queued behind another download) are considered.

    Args:
        kind: "runtime" or "model", matching the task file's `kind` field
        name: Model name to match; ignored for the runtime, which is shared

    Returns:
        dict: Newest matching task state, or None when this operation is not
            touching that resource (e.g. it is already installed, in which case
            no task file is written).
    """
    since = op_info.started_at or op_info.created_at
    newest_state = None
    newest_at = None
    for state in task_states:
        if state.get("kind") != kind:
            continue
        if name is not None and state.get("name") != name:
            continue
        updated_at = _parse_resource_timestamp(state.get("updated_at"))
        if updated_at is None or updated_at.timestamp() < since:
            continue
        if newest_at is None or updated_at > newest_at:
            newest_state, newest_at = state, updated_at
    return newest_state


def _phase_progress(done, total):
    """Scale a phase's byte counters to 0-99 (100 means the phase finished)."""
    if total <= 0:
        return 0
    ratio = min(1.0, max(0.0, done / total))
    return min(99, int(ratio * 100))


def _resource_phase_events(download_op, import_op, state, import_seen):
    """Build the (operation, progress, status) tuples for one resource.

    The download phase is reported as finished as soon as the archive is on
    disk; the install phase owns the terminal status of the resource.

    Args:
        download_op: Synthetic operation name of the download phase
        import_op: Synthetic operation name of the install phase
        state: Resource task state (see _find_resource_state)
        import_seen: Whether the install phase was already reported to clients

    Returns:
        list: Phase events for this resource, empty while it has nothing to
            report (already installed, or not started yet).
    """
    if state is None:
        return []

    status = state.get("status")
    done = state.get("bytes_done") or 0
    total = state.get("bytes_total") or 0
    progress = _phase_progress(done, total)

    if status in _DOWNLOAD_PHASE_STATES:
        return [(download_op, progress, TaskStatus.RUNNING.value)]

    # `downloaded` and everything after it mean the archive is fully on disk.
    download_done = (download_op, 100, TaskStatus.COMPLETED.value)

    if status in _DOWNLOAD_COMPLETE_STATES:
        return [download_done]

    if status in _IMPORT_PHASE_STATES:
        return [download_done, (import_op, progress, TaskStatus.RUNNING.value)]

    if status == "completed":
        return [download_done, (import_op, 100, TaskStatus.COMPLETED.value)]

    if status == "failed":
        # The failure belongs to whichever phase was in flight: the install
        # phase when it had already been reported, otherwise the download phase.
        if import_seen:
            return [download_done, (import_op, progress, TaskStatus.FAILED.value)]
        return [(download_op, progress, TaskStatus.FAILED.value)]

    return []


def _download_phase_snapshot(op_info):
    """Return the phase a download is in, in the terms the SSE stream reports.

    The phase names, the per-phase progress and the task-file selection all come
    from the helpers the SSE emitter uses, so a client that falls back to polling
    /status renders the same bars as one that streams. That is also why the
    operation row no longer carries a monotonic value of its own: the task file
    already holds the truth.

    Returns:
        tuple: (phase_name, progress), where progress is None when the operation
            has no resource task file yet -- it is still queued, or the resource
            was already installed so `resource apply` wrote none.
    """
    task_states = _read_resource_task_states()

    if op_info.model_name == _RUNTIME_RESOURCE_NAME:
        download_op, import_op = _RUNTIME_DOWNLOAD, _RUNTIME_IMPORT
        state = _find_resource_state(op_info, task_states, "runtime")
    else:
        download_op, import_op = _MODEL_DOWNLOAD, _MODEL_IMPORT
        state = _find_resource_state(op_info, task_states, "model", op_info.model_name)

    if state is None:
        # No task file: the download phase is the one this operation will start
        # in, which is the identity the SSE stand-in row carries as well.
        return download_op, None

    # A task file records that the resource failed, not which phase it failed in.
    # The SSE stream knows, because it saw the install phase reported first; a
    # poll cannot, so the failure is attributed to the download phase here and the
    # status is what tells the client the operation did not succeed.
    events = _resource_phase_events(download_op, import_op, state, import_seen=False)
    if not events:
        return download_op, None

    # The auto-start step follows the install; report it in the same terms the
    # stream uses, so a polling client sees the same sequence of phases.
    installed = any(
        operation == import_op and status == TaskStatus.COMPLETED.value
        for operation, _, status in events
    )
    start_events = _auto_start_phase_events(op_info, installed)
    if start_events:
        phase, progress, _ = start_events[-1]
        return phase, progress

    phase, progress, _ = events[-1]
    return phase, progress


def _apply_download_phase(response_data, op_info):
    """Attach the per-phase view of a download to an operation response."""
    if op_info.operation not in _DOWNLOAD_FAMILY:
        return

    phase, progress = _download_phase_snapshot(op_info)
    response_data["phase"] = phase
    if progress is not None:
        response_data["progress"] = progress


def _build_phase_response(op_info, operation, model_name, progress, status, state):
    """Wrap one phase event into the standard SSE payload.

    The payload reuses the exact field set of the model-level response so the
    stream stays compatible; only `operation`, `model_name`, `progress` and
    `status` are substituted. `model_id`/`package`/`release` are copied from the
    operation row, so a phase event carries the same metadata as its envelope.
    """
    response_data = _build_operation_response(op_info)
    response_data["operation"] = operation
    response_data["model_name"] = model_name
    response_data["progress"] = progress
    response_data["status"] = status
    if status == TaskStatus.FAILED.value and state is not None:
        # A phase backed by a task file fails for the resource's own reason; one
        # without a task file (the auto-start step) keeps the operation's error,
        # which _build_operation_response already put in the payload.
        response_data["error"] = state.get("message") or "Resource install failed"
    return response_data


def _auto_start_phase_events(op_info, installed):
    """Build the events for the auto-start step of a download_start operation.

    A download_start request runs two jobs on one row: install the resource, then
    start its worker. The install is reported from the resource task file; the
    start has no such file, and it used to be invisible -- the stream went quiet
    once `model_import` reached 100 and, both install phase streams having already
    reported their terminal state, said nothing when the model was up either.
    Deriving it from the row is also what lets a client that connects late still
    learn that the step ran.

    Args:
        op_info: Operation row
        installed: Whether the install phase has been reported finished

    Returns:
        list: One event while the start runs, one terminal event once it ends;
            empty for any other operation, and before the install has finished.
    """
    if op_info.operation != OperationType.DOWNLOAD_START.value or not installed:
        return []

    status = op_info.status
    if status == TaskStatus.COMPLETED.value:
        return [(_MODEL_START, 100, TaskStatus.COMPLETED.value)]
    if status in (TaskStatus.FAILED.value, TaskStatus.INTERRUPTED.value):
        return [(_MODEL_START, 0, status)]
    if status == TaskStatus.RUNNING.value:
        return [(_MODEL_START, 0, TaskStatus.RUNNING.value)]
    return []


def _build_resource_phase_responses(op_info, task_states, seen_states):
    """Build the per-phase payloads for the resource this operation installs.

    An operation installs exactly one resource: ``tsfm_runtime`` installs the
    shared runtime, any other name installs that model. Matching a task file by
    kind alone is no longer enough -- the runtime can be downloaded on its own
    now, so a runtime task file belonging to a *concurrent* runtime download must
    not be reported as this model operation's runtime phase.
    """
    if op_info.operation not in _DOWNLOAD_FAMILY:
        return []

    if op_info.model_name == _RUNTIME_RESOURCE_NAME:
        resources = (
            (
                _RUNTIME_DOWNLOAD,
                _RUNTIME_IMPORT,
                _RUNTIME_RESOURCE_NAME,
                _find_resource_state(op_info, task_states, "runtime"),
            ),
        )
    else:
        resources = (
            (
                _MODEL_DOWNLOAD,
                _MODEL_IMPORT,
                op_info.model_name,
                _find_resource_state(op_info, task_states, "model", op_info.model_name),
            ),
        )

    responses = []
    for download_op, import_op, resource_name, state in resources:
        import_seen = (op_info.task_id, import_op) in seen_states
        events = _resource_phase_events(download_op, import_op, state, import_seen)
        for operation, progress, status in events:
            responses.append(
                _build_phase_response(
                    op_info, operation, resource_name, progress, status, state
                )
            )

        # The auto-start step only exists once the install it follows is done, so
        # it is derived from that outcome rather than from a file of its own.
        installed = any(
            operation == import_op and status == TaskStatus.COMPLETED.value
            for operation, _, status in events
        )
        for operation, progress, status in _auto_start_phase_events(op_info, installed):
            responses.append(
                _build_phase_response(
                    op_info, operation, resource_name, progress, status, None
                )
            )
    return responses


def _emit_operation_status_updates(dao, all_ops, seen_states):
    """Check for operation state changes and yield SSE events.

    Updates seen_states dict and yields SSE events for:
    - Active operations that have changed state
    - Tasks that disappeared from active list (now completed/failed)

    A download operation is reported entirely by its phase streams (see
    _RUNTIME_DOWNLOAD and friends): the operation's own row is never streamed as
    `download`, it only stands in for the download phase while that phase has
    nothing to report yet -- still queued, or failed before a resource task file
    was written -- and then under the phase's own name. Operations without phases
    (start/stop/delete) stream their row as before. Phase streams report their
    own 0-100 progress and their own terminal status, so a client can label each
    phase instead of inferring it from the progress value.

    Args:
        dao: Data access object for fetching operation details
        all_ops: List of active AsyncOperation objects
        seen_states: Dict tracking (task_id, operation) -> (status, progress)

    Yields:
        SSE formatted strings ("data: {...}\n\n") or empty string if no changes
    """
    changed = False
    task_states = _read_resource_task_states()

    # Check each operation for state changes
    for op_info in all_ops:
        if op_info.operation in _DOWNLOAD_FAMILY:
            # The generic `download` row would only repeat the download phase
            # (same task, same resource, same moment), so a download operation is
            # streamed through its phase names only. The row stands in for the
            # download phase only while that phase has nothing to report yet, so
            # the task is never silent.
            phases = _build_resource_phase_responses(op_info, task_states, seen_states)
            if phases:
                responses = phases
            else:
                row = _build_operation_response(op_info)
                row["operation"] = (
                    _RUNTIME_DOWNLOAD
                    if op_info.model_name == _RUNTIME_RESOURCE_NAME
                    else _MODEL_DOWNLOAD
                )
                responses = [row]
        else:
            # start/stop/delete have no phases: their row is the whole story.
            responses = [_build_operation_response(op_info)]

        for response_data in responses:
            key = (op_info.task_id, response_data.get("operation"))
            current_state = (response_data.get("status"), response_data.get("progress"))

            # Emit if state changed
            if seen_states.get(key) == current_state:
                continue
            seen_states[key] = current_state

            yield f"data: {json.dumps(response_data)}\n\n"
            changed = True

    # Check for tasks that were active but have now disappeared
    # They may have completed, failed, or been interrupted
    active_task_ids = {op.task_id for op in all_ops}

    # The tail phase of an operation can start and finish between two polls: the
    # row is retired as soon as taosanodectl exits, which happens right after the
    # last import. A phase that was never observed as running would then never be
    # reported at all, because the loop below only closes out streams it already
    # knows about. Synthesize those payloads from the resource task files so a
    # client always learns how every phase ended.
    for retired_task_id in {
        task_id for task_id, _ in seen_states.keys() if task_id not in active_task_ids
    }:
        retired_op = dao.get_async_operation(retired_task_id)
        if not retired_op or retired_op.status not in _TERMINAL_OPERATION_STATES:
            continue
        for response_data in _build_resource_phase_responses(
            retired_op, task_states, seen_states
        ):
            key = (retired_task_id, response_data.get("operation"))
            if key in seen_states:
                # Already tracked: the loop below reports its terminal state
                # (and deletes the entry) instead of emitting a duplicate here.
                continue
            seen_states[key] = (
                response_data.get("status"),
                response_data.get("progress"),
            )
            yield f"data: {json.dumps(response_data)}\n\n"
            changed = True

    # A client that subscribes around the end of a download -- a reconnected
    # EventSource, a reloaded page -- has no phase stream of its own here, so the
    # loops below would tell it nothing at all. Replay what finished moments ago so
    # the outcome is never lost; the window keeps this from becoming a history
    # dump on every reconnect.
    for finished_op in dao.list_recently_finished_downloads(
        time.time() - _FINISHED_DOWNLOAD_REPLAY_SECONDS
    ):
        if finished_op.task_id in active_task_ids:
            continue
        for response_data in _build_resource_phase_responses(
            finished_op, task_states, seen_states
        ):
            key = (finished_op.task_id, response_data.get("operation"))
            if key in seen_states:
                continue
            seen_states[key] = (
                response_data.get("status"),
                response_data.get("progress"),
            )
            yield f"data: {json.dumps(response_data)}\n\n"
            changed = True

    for key in list(seen_states.keys()):
        task_id, operation = key
        if task_id in active_task_ids:
            continue

        # Task is no longer in the active list — check if it has completed
        completed_op = dao.get_async_operation(task_id)
        if not completed_op:
            # The row was removed (e.g. cancelled); nothing left to report.
            del seen_states[key]
            continue
        if completed_op.status not in _TERMINAL_OPERATION_STATES:
            continue

        # Send the final completion/failure status
        last_seen_state = seen_states[key]
        if last_seen_state[0] in _TERMINAL_OPERATION_STATES:
            # This stream already reported its own terminal state (e.g. the
            # runtime finished while the operation row stayed alive) — the
            # operation's outcome says nothing new about it.
            continue

        response_data = _build_operation_response(completed_op)
        if operation != completed_op.operation:
            # A synthetic phase stream: keep its identity instead of replacing
            # it with the operation's.
            response_data["operation"] = operation
            if completed_op.status == TaskStatus.COMPLETED.value:
                # The operation only completes once every phase has finished, so
                # a phase still marked running did reach 100.
                response_data["progress"] = 100
            else:
                # Keep the progress this phase last reported.
                response_data["progress"] = last_seen_state[1]
            if operation.startswith("runtime_"):
                response_data["model_name"] = _RUNTIME_RESOURCE_NAME

        # Record the terminal state instead of deleting the entry so the
        # replay loop does not re-emit it within its window.
        seen_states[key] = (
            response_data.get("status"),
            response_data.get("progress"),
        )
        yield f"data: {json.dumps(response_data)}\n\n"
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

    Downloads are streamed through their phase events instead of the generic
    `download` row: `operation: "model_download"` then `"model_import"` for a
    model, and `"runtime_download"` then `"runtime_import"` (with
    `model_name: "tsfm_runtime"`) for the shared runtime. A download_start request
    adds `"start"` once the install has finished, because it goes on to start the
    worker -- under the name an explicit start streams under, so a client renders
    it with one code path. Phase operations are stream-only and are not queryable
    via the /status endpoint. An operation that finished moments ago is replayed
    to a client that subscribes after the fact (see
    _FINISHED_DOWNLOAD_REPLAY_SECONDS), so a reconnected stream cannot miss the
    outcome.

    Returns:
        SSE stream of operation updates: {task_id, status, progress, ...}
    """
    from flask import Response

    def event_stream():
        """Generator yielding SSE events for all active operations."""
        dao = get_dao()
        seen_states = {}  # (task_id, operation) -> (status, progress)
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
