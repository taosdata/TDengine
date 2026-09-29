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

import threading
from concurrent.futures import ThreadPoolExecutor

from taosanalytics.log import AppLogger

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
_EXECUTOR_INIT_LOCK = threading.Lock()

def get_service_executor():
    """Return the service thread executor, creating it on first use.

    Service operations (start, stop, delete) only coordinate a short-lived
    taosanodectl subprocess, so they run on a dedicated thread executor. Keeping
    them separate from the download executor prevents long downloads from
    blocking responsive service actions (and vice versa). A single worker keeps
    service operations serialized, matching the previous one-per-pool semantics.
    """
    global _SERVICE_EXECUTOR

    # double-checked locking: avoid acquiring the lock if the executor is already initialized
    if _SERVICE_EXECUTOR is None:

        with _EXECUTOR_INIT_LOCK:
            if _SERVICE_EXECUTOR is None:
                AppLogger.info("Initializing service executor with 1 worker")
                _SERVICE_EXECUTOR = ThreadPoolExecutor(
                    max_workers=1, thread_name_prefix="tsfm-service"
                )
                AppLogger.info("Service executor initialized: %s", _SERVICE_EXECUTOR)
    return _SERVICE_EXECUTOR


def get_download_executor():
    """Return the download thread executor, creating it on first use.

    A single worker keeps downloads serialized within this process. The heavy
    work runs in a taosanodectl subprocess, so a thread executor is enough here.
    Serialization *across* processes is not this pool's job: permission comes
    from the CLI's lifecycle lock and ordering from the flock the runner waits on
    (see the module-level note on the download executor).
    """
    global _DOWNLOAD_EXECUTOR
    # double-checked locking
    if _DOWNLOAD_EXECUTOR is None:

        with _EXECUTOR_INIT_LOCK:
            if _DOWNLOAD_EXECUTOR is None:
                AppLogger.info("Initializing download executor with 1 worker")
                _DOWNLOAD_EXECUTOR = ThreadPoolExecutor(
                    max_workers=1, thread_name_prefix="tsfm-download"
                )
                AppLogger.info("Download executor initialized: %s", _DOWNLOAD_EXECUTOR)

    return _DOWNLOAD_EXECUTOR


def _worker_outcome(future, task_id, model_name, operation):
    """Return (status, error, result) for one finished worker.

    Every way a worker can end becomes the same triple here, so the caller has
    exactly one path that writes an outcome:

    - it returned a result dict with a valid status: that status and message;
    - it returned a result dict with status=None or missing: ``failed``;
    - it returned None or an empty dict: ``failed``;
    - it returned something that is not a dict: ``failed``, naming the type;
    - it raised any exception (Exception or BaseException like KeyboardInterrupt):
      ``failed`` with the exception text.

    In all cases, a row is closed (status written) and will not stay ``running``
    after the worker exits.
    """
    try:
        result = future.result()
    except Exception as exc:
        AppLogger.error(
            "%s task raised exception: task_id=%s, model=%s, exc=%s",
            operation, task_id, model_name, exc, exc_info=True,
        )
        return "failed", str(exc), None
    except BaseException as exc:
        AppLogger.error(
            "%s task aborted: task_id=%s, model=%s, exc=%s",
            operation, task_id, model_name, type(exc).__name__, exc_info=True,
        )
        return "failed", f"Worker aborted: {type(exc).__name__}", None

    if not isinstance(result, dict):
        if result is None:
            return "failed", "Worker returned no result", None
        AppLogger.error(
            "%s task returned %s instead of a result dict: task_id=%s, model=%s",
            operation, type(result).__name__, task_id, model_name,
        )
        return "failed", f"Worker returned {type(result).__name__}, not a result dict", None

    if not result:
        # An empty dict carries no status either.
        return "failed", "Worker returned no result", None

    # Normalize None status to "failed" (result.get only returns the default for
    # missing keys, not for None values). This ensures exactly one valid TaskStatus
    # value is written, never NULL.
    status = result.get("status") or "failed"
    return status, result.get("message", None), result


def _handle_operation_result(future, task_id, model_name, operation, complete_fn, deferred_fn=None):
    """Handle worker result and exceptions with common logic.

    Extracts status and error from the result dict, distinguishing between
    exceptions, None results, successful completions, and deferred tasks.
    Both download and service task callbacks use this to reduce duplication.

    Whatever the worker did -- returned, raised, or aborted -- ends in exactly one
    write here (see _worker_outcome). A row nobody closes stays ``running`` until
    the next restart reconciles it, and a ``running`` start row refuses shared
    runtime deletion while it lasts.

    Args:
        future: ThreadPoolExecutor future object
        task_id: Unique task identifier
        model_name: Name of the model being operated on
        operation: Operation name (download, download_start, start, stop, delete)
        complete_fn: Completion callback (task_id, model_name, operation, status, error)
        deferred_fn: Optional callback for deferred tasks (status, result_dict)
    """
    try:
        status, error, result = _worker_outcome(future, task_id, model_name, operation)

        if status == "deferred" and deferred_fn is not None:
            deferred_fn(status, result)
            return

        if status == "deferred":
            # Nothing to re-arm without a handler, and `deferred` is not a
            # TaskStatus: writing it would leave a value in the table that no
            # active-only query excludes and no cleanup sweep removes.
            AppLogger.error(
                "%s task was deferred but this operation has no retry handler, "
                "reporting it as failed: task_id=%s, model=%s",
                operation, task_id, model_name,
            )
            status, error = "failed", "Task was deferred but cannot be retried"
        elif status == "async_delegated":
            # For composite operations like download_start that delegate to another
            # executor, the next executor owns the outcome write. Skip complete_fn here.
            AppLogger.info(
                "%s task delegated to next executor: task_id=%s, model=%s",
                operation, task_id, model_name,
            )
            return
        elif result is not None:
            AppLogger.info(
                "%s task completed: task_id=%s, model=%s, status=%s, result=%s",
                operation, task_id, model_name, status, result,
            )

        complete_fn(task_id, model_name, operation, status, error)

    except Exception as exc:
        # Last resort: an outcome that cannot be written must not escape into the
        # `concurrent.futures` logger, where nothing would close the row. Only
        # Exception is caught -- an abort raised in this thread is a genuine one
        # and stays the caller's business.
        AppLogger.error(
            "Cannot record the outcome of %s task: task_id=%s, model=%s, exc=%s",
            operation, task_id, model_name, exc, exc_info=True,
        )


def submit_download_task(worker_fn, args, task_id, model_name, operation:str, complete_async_operation_fn, deferred_fn=None):
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
        deferred_fn: Handler (status, result) for a deferred result. A worker that
            cannot defer leaves it out, and its status is then written as the
            operation's outcome; the combined download_start passes one, because a
            start the lifecycle lock refuses is deferred rather than finished (see
            _run_download_and_start_task).
    """
    # One pool for every download, the shared TSFM runtime included: the pool's
    # single worker is the local queue, so downloads are served in submission
    # order -- the runtime does not get a pool (or a queue position) of its own.
    executor = get_download_executor()

    def _on_done(future):
        _handle_operation_result(
            future, task_id, model_name, operation, complete_async_operation_fn,
            deferred_fn=deferred_fn,
        )

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


def submit_service_task(worker_fn, args, task_id, model_name, operation, complete_async_operation_fn, retry_scheduler_fn):
    """Submit a service operation (start/stop/delete) to the service executor.

    Service workers return a dict like {"status": "completed"|"failed", "message": ...}.
    Completion (including unhandled worker exceptions) is applied via the
    injected complete_async_operation_fn, running in the executor thread. A
    single service executor keeps service operations serialized and independent
    of downloads.


    Args:
        worker_fn: Worker function taking *args and returning a result dict
        args: Positional args for worker_fn
        task_id: Unique identifier for tracking this operation
        model_name: Name of the model being operated on
        operation: Operation name (start, stop, delete)
        complete_async_operation_fn: Callback invoked with the terminal status
            (task_id, model_name, operation, status, error) once the task ends
        retry_scheduler_fn: Callback invoked to re-arm a deferred task after its
            backoff (worker_fn, args, task_id, model_name, operation, result)

    Returns:
        Future: the submitted task, so a caller that has to wait for its verdict
            (the auto-start phase of a download_start) can do so without giving up
            the completion and retry callbacks.
    """

    def _on_done(future):
        def _deferred_handler(status, result):
            retry_scheduler_fn(worker_fn, args, task_id, model_name, operation, result)

        _handle_operation_result(
            future, task_id, model_name, operation,
            complete_async_operation_fn,
            deferred_fn=_deferred_handler
        )

    executor = get_service_executor()
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
    return future
