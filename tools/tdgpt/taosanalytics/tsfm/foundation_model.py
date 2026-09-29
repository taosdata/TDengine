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
import os
import subprocess
import threading
import time
from pathlib import Path

from taosanalytics.log import AppLogger

# Every helper is called through the module (foundation_util.x()), never imported
# by name: a `from ... import` would bind a second reference at import time, so a
# test patching a helper would have to patch each consumer instead of the single
# place it is defined.
from taosanalytics.tsfm import foundation_util
from taosanalytics.tsfm.executor import (
    submit_download_task,
    submit_service_task,
)
from taosanalytics.tsfm.foundation_models_db import (
    AsyncOperation,
    FoundationModel,
    get_dao,
    get_resource_dir,
)
from taosanalytics.tsfm.model_state_machine import ModelStateManager, ModelStateTransitionError
from taosanalytics.tsfm.response_event import (
    RUNTIME_RESOURCE_NAME,
    OperationType,
    TaskStatus,
    do_build_operation_response,
)

# Whether this process has already looked for deletes to resume. The recovery
# runs from the request path (see app.before_request), not from a server hook:
# the executor queues that run these tasks are process-local and the preloading
# master owns none, and keeping it in the app is what makes it work under both
# gunicorn and waitress. One run per process is enough -- claiming is atomic, so
# a second process scanning adds nothing.
_PENDING_DELETES_RESUMED = False
_MODELS_INITIALIZED = False


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
    live = foundation_util.live_worker_names()
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
        if model.running != is_running:
            if dao.update_model(model.name, running=is_running):
                corrected += 1
                AppLogger.info(
                    "Reconciled model '%s' running state: %s -> %s",
                    model.name,
                    model.running,
                    is_running,
                )

        # Reconcile model_state for transient states left behind by an interrupted
        # operation. Deleting is excluded: its re-queue mechanism drives the final
        # state. Downloading is excluded: fail_stale_running_operations marks it
        # interrupted and _complete_async_operation drives the state from there.
        stale_state = model.model_state
        if stale_state == "starting":
            # The start was interrupted; no worker launched or it was killed.
            dao.update_model(model.name, model_state="idle")
            AppLogger.info("Reconciled model '%s' model_state: starting -> idle", model.name)
        elif stale_state == "stopping":
            # If the worker is still alive the stop never completed; revert to started.
            # If the worker is gone the model is effectively stopped; treat as idle.
            next_state = "started" if is_running else "idle"
            dao.update_model(model.name, model_state=next_state)
            AppLogger.info(
                "Reconciled model '%s' model_state: stopping -> %s", model.name, next_state
            )

    AppLogger.info(
        "Model running-state reconciliation complete: %d corrected (live workers: %s)",
        corrected,
        ", ".join(live) or "none",
    )
    return corrected

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


def _download_lock_path() -> str:
    """Return the lock file downloads take turns on before applying.

    It sequences this API's own downloads only. It is not the authority on
    whether an apply may run -- that is the CLI's lifecycle lock (see the
    module-level note on the download executor).
    """
    resource_dir = get_resource_dir()
    root = Path(resource_dir)
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


def _reconcile_local_model_state(model_name, local_model):
    """Synchronize cached model state from the authoritative local inventory."""
    if local_model is None:
        foundation_util.update_model_state(model_name, downloaded=False)
        return
    foundation_util.update_model_state(
        model_name,
        downloaded=(local_model.get("install_status") == "installed"),
        running=(local_model.get("service_status") == "running"),
    )


def _advance_model_state(dao, model_name, operation, operation_status):
    """Update model_state in the DB based on an operation/status transition.

    Called both when an operation is first queued and when it completes, so
    model_state tracks the full lifecycle rather than only terminal outcomes.
    Silently skips models not in the catalog (e.g. the shared runtime).

    Returns None on success, or an error message string if the transition is invalid.
    """
    model = dao.get_model(model_name)
    if not model:
        return None
    try:
        next_state = ModelStateManager.calculate_next_state(
            operation=operation,
            operation_status=operation_status,
            current_state=model.model_state or "missing",
        )
        dao.update_model(model_name, model_state=next_state)
        return None
    except (ValueError, ModelStateTransitionError) as e:
        AppLogger.error("Failed to advance model_state for %s: %s", model_name, e)
        return str(e)


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

    # Update unified model_state using state machine
    _advance_model_state(dao, model_name, operation, status)

    # Update legacy model state fields (for backward compatibility)
    if status == TaskStatus.COMPLETED.value:
        if operation == OperationType.DOWNLOAD.value:
            # An explicitly downloaded runtime has no model row of its own, so
            # only a model download flips the downloaded flag.
            if model_name != RUNTIME_RESOURCE_NAME:
                foundation_util.update_model_state(model_name, downloaded=True)
        elif operation == OperationType.START.value:
            foundation_util.update_model_state(model_name, running=True)
        elif operation == OperationType.DOWNLOAD_START.value:
            # Combined operation: a successful completion means the model was
            # downloaded AND its service started.
            foundation_util.update_model_state(model_name, downloaded=True, running=True)
        elif operation == OperationType.STOP.value:
            foundation_util.update_model_state(model_name, running=False)
        elif operation == OperationType.DELETE.value:
            # The shared runtime is a resource without a model row of its own;
            # there is nothing to update for it.
            if model_name != RUNTIME_RESOURCE_NAME:
                foundation_util.update_model_state(model_name, downloaded=False, running=False)

    dao.cleanup_old_operations(days=1)


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
        if model_name != RUNTIME_RESOURCE_NAME:
            blocker = _runtime_model_download_blocker()
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

        taosanodectl = foundation_util.get_taosanodectl_cmd()

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

        foundation_util.log_invocation(
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
    submit_download_task(
        _run_download_task,
        args=(task_id, model_name, release, resource_base_url, replace),
        task_id=task_id,
        model_name=model_name,
        operation=OperationType.DOWNLOAD.value,
        complete_async_operation_fn=_complete_async_operation,
    )



# Extra grace (seconds) added on top of the model start deadline for the outer
# wait, so the child process always has a chance to report its result and exit
# before the parent gives up on it.
_MODEL_START_GRACE_SECONDS = 30


# Facts that describe *this host's* install of a model rather than the release the
# repository publishes. `resource check` cannot know them, so they are merged in
# from the CLI's local view (see _merge_local_resource_state).
_LOCAL_RESOURCE_FIELDS = ("service_status", "deployed_at")


def _local_model_state(exec_timeout):
    """Return the CLI's local per-model view, keyed by model name.

    `resource list` is the local counterpart of `resource check`: it reports what
    is installed here -- `service_status` from the pid files, `deployed_at` from
    the per-model state file -- without contacting the repository. It is read
    through the CLI for the same reason `_runtime_model_download_blocker` is: the
    resource layout and the state schema belong to tdgpt/script, and this module
    is a separate deployment unit that cannot import them.

    Returns:
        dict: Model name -> local entry. Empty when the view cannot be read: the
            caller still has the repository listing to return, so a failure here
            degrades the response rather than failing it -- unlike get_model_info,
            where the local view *is* the answer and a failure is an error.
    """
    try:
        taosanodectl = foundation_util.get_taosanodectl_cmd()
    except FileNotFoundError as exc:
        AppLogger.warning("Cannot read the local resource view: %s", exc)
        return {}

    cmd = taosanodectl + ["resource", "list", "--format", "json"]
    foundation_util.log_invocation("local resource view request", cmd=" ".join(cmd))

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
        resource_base_url, exec_timeout = foundation_util.resolve_resource_settings(request=request)
        foundation_util.log_invocation(
            "resource check request",
            release=release,
            ip=request.remote_addr,
        )

        # Construct taosanodectl command
        try:
            taosanodectl = foundation_util.get_taosanodectl_cmd()
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

                    model_name = resource.get("name", "")
                    latest_op = dao.get_latest_operation(model_name)
                    if latest_op:
                        if resource.get("state") == "missing" and latest_op.status == "completed":
                            pass
                        else:
                            resource["operation_status"] = latest_op.status

                    # let the frontend know that a queued downloading is inprogress for a missing model
                    if resource.get("state") == "missing" and resource.get("operation_status") == TaskStatus.QUEUED.value:
                        resource["state"] = "downloading"

                    db_model = dao.get_model(model_name)
                    resource["model_state"] = db_model.model_state if db_model else "missing"

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
        resource_base_url, exec_timeout = foundation_util.resolve_resource_settings(request=request)
        foundation_util.log_invocation(
            "resource list request",
            model_name=model_name,
            release=release,
            ip=request.remote_addr,
        )

        # Execute taosanodectl resource list to get real-time model info
        try:
            taosanodectl = foundation_util.get_taosanodectl_cmd()
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



def _extract_model_status_in_repository(model_name, release, resource_base_url, kind="model"):
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
        _, exec_timeout = foundation_util.resolve_resource_settings()
        try:
            taosanodectl = foundation_util.get_taosanodectl_cmd()
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

        foundation_util.log_invocation(
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


def _reuse_download_response(active_op, model_name):
    """Build a response that reuses an already in-progress download task."""
    detail = do_build_operation_response(active_op)
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

    active_op = foundation_util.get_active_download_op(model_name)
    if active_op is None:
        # The winner finished between the insert and this lookup, so nobody owns
        # the download any more; one retry is enough to make this caller the
        # owner. If that loses too, the next lookup below reports it.
        if dao.create_download_operation_if_idle(operation):
            return None
        active_op = foundation_util.get_active_download_op(model_name)

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
    resource_base_url, _ = foundation_util.resolve_resource_settings(payload=payload)
    replace = payload.get("replace", False)

    foundation_util.log_invocation(
        "runtime download request parameters",
        resource=RUNTIME_RESOURCE_NAME,
        release=release,
        resource_base_url=resource_base_url,
        replace=replace,
        ip=request.remote_addr,
    )

    is_valid, runtime_info, error_msg = _extract_model_status_in_repository(
        RUNTIME_RESOURCE_NAME, release, resource_base_url, kind="runtime"
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
    active_op = foundation_util.get_active_download_op(RUNTIME_RESOURCE_NAME)
    if active_op:
        return _reuse_download_response(active_op, RUNTIME_RESOURCE_NAME)

    dao = get_dao()
    dao.cleanup_download_operations(RUNTIME_RESOURCE_NAME)

    task_id = foundation_util.make_task_id("download", RUNTIME_RESOURCE_NAME)
    rejection = _claim_download_operation(dao, AsyncOperation(
        task_id=task_id,
        operation=OperationType.DOWNLOAD.value,
        model_name=RUNTIME_RESOURCE_NAME,
        status=TaskStatus.QUEUED.value,
        data={
            "package": runtime_info.get("file", ""),
            "release": release,
            "resource_base_url": resource_base_url,
            "replace": replace,
        },
    ), RUNTIME_RESOURCE_NAME)

    if rejection:
        return rejection

    # The worker is driven with the resource name itself: that is what
    # `resource apply` matches on and what the resource task file records as its
    # `name`, which is how the progress poller finds it again.
    _trigger_model_download_background(
        task_id, RUNTIME_RESOURCE_NAME, release, resource_base_url, replace
    )

    AppLogger.info(
        "Runtime download initiated for resource '%s' (release: %s, resource_url: %s, "
        "replace: %s), task_id: %s, ip:%s",
        RUNTIME_RESOURCE_NAME,
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
        "model_name": RUNTIME_RESOURCE_NAME,
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
    replace = payload.get("replace", False)

    # Use provided resource_base_url or fall back to configured default
    resource_base_url, _ = foundation_util.resolve_resource_settings(payload=payload)

    foundation_util.log_invocation(
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
    if model_name == RUNTIME_RESOURCE_NAME:
        return download_foundation_runtime(request)

    # Validate model exists in repository (real-time check)
    is_valid, model_info, error_msg = _extract_model_status_in_repository(
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
    active_op = foundation_util.get_active_download_op(model_name)
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
    task_id = foundation_util.make_task_id("download", model_name)

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

    err = _advance_model_state(dao, model_name, OperationType.DOWNLOAD.value, TaskStatus.QUEUED.value)
    if err:
        return {"error": err, "model_name": model_name}, 500
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

# Retry configuration for all deferrable operations (delete, start, stop).
# All operations share the same budget: exponential backoff for up to 1 hour.
_SHARED_RETRY_CONFIG = {
    "base_seconds": 5,
    "max_delay_seconds": 60,
    "max_wait_seconds": 3600,
}


def _operation_data(dao, task_id):
    """Return a mutable copy of an operation's data payload."""
    op = dao.get_async_operation(task_id)
    data = op.data if op and isinstance(op.data, dict) else {}
    return dict(data)


def _cli_reason(*streams):
    """Return the CLI's own one-line explanation of a refusal, as text.

    Takes the child's captured streams in preference order (stderr first, then
    stdout) in either form the two invocation styles produce: ``str`` from
    ``subprocess.run(text=True)`` and ``bytes`` from ``Popen.communicate()``.
    Decoding happens here, once, because the value is stored in the operation's
    JSON ``data`` -- bytes would not survive json.dumps.

    Returns:
        str: The last non-empty line, or "" when the child said nothing.
    """
    for stream in streams:
        if not stream:
            continue
        text = stream.decode("utf-8", errors="replace") if isinstance(stream, bytes) else stream
        text = text.strip()
        if text:
            return text.splitlines()[-1].strip()
    return ""


def _schedule_service_retry(worker_fn, args, task_id, model_name, operation, result):
    """Re-arm a deferred service task after the delay it asked for.

    A timer thread per deferred task keeps the wait outside the executor: the
    worker thread is released immediately, so a long download behind the retry
    cannot pin the single service worker.
    """
    delay = max(1, int(result.get("delay") or _SHARED_RETRY_CONFIG["base_seconds"]))
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

    submit_service_task(
        worker_fn,
        args=args,
        task_id=task_id,
        model_name=model_name,
        operation=operation,
        complete_async_operation_fn=_complete_async_operation,
        retry_scheduler_fn=_schedule_service_retry,
    )


def _trigger_model_start_background(task_id, model_name):
    """Submit model start task to the service thread executor.

    Executes: python ./taosanodectl.py model start <model_name>

    Args:
        task_id: Unique identifier for tracking this operation
        model_name: Name of the model to start (e.g., 'moirai')

    The executor handles async execution; caller monitors progress via get_operation_status(task_id)
    """
    submit_service_task(
        _run_model_start_task,
        args=(task_id, model_name),
        task_id=task_id,
        model_name=model_name,
        operation=OperationType.START.value,
        complete_async_operation_fn=_complete_async_operation,
        retry_scheduler_fn=_schedule_service_retry,
    )


def _defer_operation(dao, task_id, model_name, operation, state, reason):
    """Generic defer handler for operations hit by transient conflict (exit 75).

    When an operation encounters the lifecycle lock held by another in-flight
    operation (usually a download), this function defers the task instead of
    failing. The task is put back to `queued` and resubmitted after exponential
    backoff, with the wait budget tracked so it eventually fails if the lock
    is held too long.

    Args:
        dao: FoundationModelsDAO instance
        task_id: Unique task identifier
        model_name: Name of the model being operated on
        operation: Operation name ("delete", "start", or "stop")
        state: Mutable dict of operation.data (retry state)
        reason: The CLI's one-line explanation, already decoded by the caller
            with _cli_reason -- this function has no opinion on which stream it
            came from, or whether the child produced text or bytes

    Returns:
        dict: {"status": "deferred", "delay": N, "waiting_for": "reason"}
              or {"status": "failed", "message": "..."}
    """
    if operation not in ("delete", "start", "stop"):
        return {"status": "failed", "message": f"Unknown operation: {operation}"}

    now = time.time()
    attempt = int(state.get("attempt") or 0) + 1
    wait_since = float(state.get("wait_since") or now)
    waited = now - wait_since
    waiting_for = reason or ""

    if waited >= _SHARED_RETRY_CONFIG["max_wait_seconds"]:
        operation_description = {
            "stop": "stopping",
            "start": "starting",
            "delete": "deleting",
        }
        message = (
            f"Timed out after {int(waited)}s waiting for an in-flight resource "
            f"operation to finish before {operation_description.get(operation)} '{model_name}'"
            + (f": {waiting_for}" if waiting_for else "")
        )
        AppLogger.error(
            "%s gave up waiting for the resource operation: task_id=%s, model=%s, "
            "attempt=%d, waited=%ds, reason=%s",
            operation.capitalize(), task_id, model_name, attempt, int(waited),
            waiting_for,
        )
        return {"status": "failed", "message": message}

    delay = min(
        _SHARED_RETRY_CONFIG["base_seconds"] * (2 ** (attempt - 1)),
        _SHARED_RETRY_CONFIG["max_delay_seconds"],
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
        "%s deferred behind an in-flight resource operation: task_id=%s, model=%s, "
        "attempt=%d, waited=%ds, retry_in=%ds, held_by=%s",
        operation.capitalize(), task_id, model_name, attempt, int(waited), delay,
        str(waiting_for) or "unknown",
    )

    return {
        "status": "deferred",
        "delay": delay,
        "waiting_for": waiting_for,
    }


def _run_model_delete_task(task_id, model_name):
    """Worker function for the service thread executor to delete a resource.

    This runs in an executor thread; completion is applied by the caller via
    _complete_async_operation after this returns its result dict.

    A transient refusal (the lifecycle lock is held by another resource
    operation -- typically the download this delete must wait for) defers the
    task instead of failing it (see _defer_operation). A permanent refusal still
    fails it, carrying the CLI's own explanation.
    """
    exec_timeout = None
    try:
        dao = get_dao()
        state = _operation_data(dao, task_id)
        dao.mark_operation_running(task_id)

        taosanodectl = foundation_util.get_taosanodectl_cmd()

        cmd = taosanodectl + ["resource", "delete", model_name]

        AppLogger.info(
            "Starting model delete process for task_id: %s, model: %s, cmd: %s",
            task_id,
            model_name,
            " ".join(cmd),
        )

        _, exec_timeout = foundation_util.resolve_resource_settings()
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=exec_timeout)

        if result.returncode == 0:
            AppLogger.info(
                "Model delete completed for task_id: %s, model: %s",
                task_id,
                model_name,
            )
            return {"status": "completed", "message": None}

        if result.returncode == _CLI_EXIT_TEMPFAIL:
            return _defer_operation(
                dao, task_id, model_name, "delete", state,
                _cli_reason(result.stderr, result.stdout),
            )

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
    submit_service_task(
        _run_model_delete_task,
        args=(task_id, model_name),
        task_id=task_id,
        model_name=model_name,
        operation="delete",
        complete_async_operation_fn=_complete_async_operation,
        retry_scheduler_fn=_schedule_service_retry,
    )


def _run_model_start_task(task_id, model_name):
    """Worker function for the service thread executor to start a model via taosanodectl.

    This runs in an executor thread; completion is applied by the caller via
    _complete_async_operation after this returns its result dict.

    Uses Popen with start_new_session=True so the model service process is detached
    from the RESTful server's process group. This prevents the model service from
    being killed when the RESTful server stops (SIGHUP/SIGTERM to process group).

    A transient refusal (the lifecycle lock is held by another resource operation
    -- typically a download this start must wait for) defers the task instead of
    failing it (see _defer_operation). A permanent refusal still fails it.

    The readiness deadline is read from the ``tsfm_model_start_timeout`` config
    setting and forwarded to ``taosanodectl model start --timeout``, so the
    parent's wait and the child's own readiness check share one value.
    """
    try:
        dao = get_dao()
        state = _operation_data(dao, task_id)
        dao.mark_operation_running(task_id)

        taosanodectl = foundation_util.get_taosanodectl_cmd()
        start_timeout = foundation_util.resolve_model_start_timeout()

        cmd = taosanodectl + [
            "model",
            "start",
            model_name,
            "--timeout",
            str(start_timeout),
        ]

        foundation_util.log_invocation(
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

        if returncode == _CLI_EXIT_TEMPFAIL:
            return _defer_operation(
                dao, task_id, model_name, "start", state, _cli_reason(stderr)
            )

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
    task_id = foundation_util.make_task_id(operation_type, model_name)
    foundation_util.log_invocation(
        f"{operation} operation requested",
        model_name=model_name,
        task_id=task_id,
        ip=request.remote_addr,
    )

    # Record async operation with timestamps
    op = AsyncOperation(
        task_id=task_id,
        operation=operation_type,
        model_name=model_name,
        status=TaskStatus.QUEUED.value
    )

    dao.create_async_operation(op)
    err = _advance_model_state(dao, model_name, operation_type, TaskStatus.QUEUED.value)
    if err:
        return {"error": err, "model_name": model_name}, 500

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


def _validate_delete_state(model, model_name, operation, ip):
    """Validate model state for delete operation.

    Ask the pid files rather than the cached ``models.running`` column: a worker
    started from the CLI, or one that outlived a restart, is not reflected there.
    When liveness cannot be determined the request is let through -- the
    resource-delete command checks the same pids again and refuses with a clear
    reason, so an unknown answer never deletes a live worker's directory.

    A model is deletable whether or not it is downloaded: deleting a model that
    never finished downloading is how a half-installed model is cleaned up.
    """
    live = foundation_util.live_worker_names()
    if live and model_name in live:
        AppLogger.error(
            "%s request for running model: %s, ip:%s",
            operation,
            model_name,
            ip,
        )
        return False, f"Cannot delete running model '{model_name}'. Stop it first."

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
        OperationType.START.value,
        OperationType.START.value,
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
    foundation_util.update_model_state(model_name, downloaded=True)
    AppLogger.info(
        "download_start: download completed for task_id: %s, model: %s, auto-starting model service",
        task_id,
        model_name,
    )

    # Phase 2: start the model service through the service submission, so the
    # start stays serialized with any concurrent stop/delete on the same model and
    # keeps the retry wiring a refusal needs. A start the lifecycle lock refuses
    # (exit 75) is deferred: _defer_operation puts the row back to `queued` and the
    # retry scheduler re-arms this start phase alone -- never the download phase
    # again. Submit the start task asynchronously: the start completes in the service
    # executor, and the download executor releases its worker immediately to process
    # queued downloads. Both executors are independent single-thread pools, so no
    # deadlock risk. Return async_delegated so _handle_operation_result knows the
    # service executor owns the outcome write.
    submit_service_task(
        _run_model_start_task,
        args=(task_id, model_name),
        task_id=task_id,
        model_name=model_name,
        operation=OperationType.DOWNLOAD_START.value,
        complete_async_operation_fn=_complete_async_operation,
        retry_scheduler_fn=_schedule_service_retry,
    )
    # Signal that outcome is owned by the next executor; this prevents double-write
    # of terminal status (download executor reports to API, service executor updates DB).
    return {"status": "async_delegated", "message": "Download completed; starting model service in background"}


def _deferred_start_stays_queued(_status, result):
    """Decline to write an outcome for a deferred download_start.

    The refused start is not the operation's end: _defer_operation already put the
    row back to `queued`, and the retry that will run the start phase again was
    armed by the service submission that ran it (see _run_download_and_start_task).
    Writing a status here would both fake an outcome and make _resubmit_service_task
    drop the retry, because a resubmit only happens while the task is still queued.
    """
    AppLogger.info(
        "download_start deferred behind an in-flight resource operation: "
        "start phase re-armed in %ss (held_by=%s), row stays queued",
        result.get("delay"),
        result.get("waiting_for") or "unknown",
    )


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
    submit_download_task(
        _run_download_and_start_task,
        args=(task_id, model_name, release, resource_base_url, replace),
        task_id=task_id,
        model_name=model_name,
        operation=OperationType.DOWNLOAD_START.value,
        complete_async_operation_fn=_complete_async_operation,
        deferred_fn=_deferred_start_stays_queued,
    )


def _runtime_model_download_blocker():
    """Return why a model download cannot start yet, or None when the runtime is ready.

    Model workers execute from the shared TSFM runtime, so a model must not be
    fetched while that runtime is missing or unusable.

    The check runs `resource verify tsfm_runtime` in a child process instead of
    validating the venv here: the layout and manifest schema are owned by
    tdgpt/script, and this module is a separate deployment unit that cannot
    import it. It is a local check -- no repository access -- and it is the same
    command an operator would run by hand.

    Returns:
        str: Reason the runtime is unusable, or None when it is ready.
    """

    def extract_reason(stdout):
        """Extract the runtime environment's failure reason from `resource verify` JSON output."""
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


    try:
        taosanodectl = foundation_util.get_taosanodectl_cmd()
    except FileNotFoundError as exc:
        return str(exc)

    _, exec_timeout = foundation_util.resolve_resource_settings()
    cmd = taosanodectl + ["resource", "verify", RUNTIME_RESOURCE_NAME, "--format", "json"]

    foundation_util.log_invocation("runtime readiness probe", cmd=" ".join(cmd))

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=exec_timeout)
    except subprocess.TimeoutExpired:
        return f"runtime readiness check timed out (>{exec_timeout}s)"
    except (OSError, subprocess.SubprocessError) as exc:
        return f"unable to check the shared TSFM runtime: {exc}"

    if result.returncode == 0:
        return None

    reason = extract_reason(result.stdout)
    if reason:
        return reason
    else:
        AppLogger.warning(
            "Cannot determine why the shared TSFM runtime is not ready (exit=%s): %s",
            result.returncode,
            (result.stderr or "").strip()[:500],
        )
        return "shared TSFM runtime is not installed or invalid"


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
    runtime_download = foundation_util.get_active_download_op(RUNTIME_RESOURCE_NAME)
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

    blocker = _runtime_model_download_blocker()
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
    resource_base_url, _ = foundation_util.resolve_resource_settings(payload=payload)
    replace = payload.get("replace", False)

    foundation_util.log_invocation(
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
    if model_name == RUNTIME_RESOURCE_NAME:
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
    is_valid, model_info, error_msg = _extract_model_status_in_repository(model_name, release, resource_base_url)
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
    active_op = foundation_util.get_active_download_op(model_name)
    if active_op:
        return _reuse_download_response(active_op, model_name)

    dao.cleanup_download_operations(model_name)

    rejection = _reject_download_without_runtime(model_name, request.remote_addr)
    if rejection:
        return rejection

    # Generate a unique task ID for async tracking.
    task_id = foundation_util.make_task_id("download-start", model_name)

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

    err = _advance_model_state(dao, model_name, OperationType.DOWNLOAD_START.value, TaskStatus.QUEUED.value)
    if err:
        return {"error": err, "model_name": model_name}, 500
    # Spawn the background download-then-start task.
    _trigger_model_download_start_background(task_id, model_name, release, resource_base_url, replace)

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
    live = foundation_util.live_worker_names()
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
    """Queue the async delete of the shared TSFM runtime and build its 202 response.

    Only the runtime-destroy entry point uses this now: a model delete goes
    through the generic ``_create_async_operation_task``, because it shares the
    model lookup and state validation that start/stop already define. The
    runtime has no model row to look up, so it keeps its own queue path.

    Args:
        model_name: Resource to delete; ``RUNTIME_RESOURCE_NAME`` for the
            shared TSFM runtime.
        message: Human readable message for the response body.

    Returns:
        tuple: (response_dict, 202)
    """
    dao = get_dao()
    task_id = foundation_util.make_task_id("delete", model_name)

    foundation_util.log_invocation("delete operation queued", model_name=model_name, task_id=task_id)
    dao.create_async_operation(AsyncOperation(
        task_id=task_id,
        operation=OperationType.DELETE.value,
        model_name=model_name,
        status=TaskStatus.QUEUED.value,
    ))

    err = _advance_model_state(dao, model_name, OperationType.DELETE.value, TaskStatus.QUEUED.value)
    if err:
        return {"error": err, "model_name": model_name}, 500

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
        RUNTIME_RESOURCE_NAME, "Shared TSFM runtime deletion initiated"
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

        submit_service_task(
            _run_model_delete_task,
            args=(op.task_id, op.model_name),
            task_id=op.task_id,
            model_name=op.model_name,
            operation=OperationType.DELETE.value,
            complete_async_operation_fn=_complete_async_operation,
            retry_scheduler_fn=_schedule_service_retry,
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

    if model_name == RUNTIME_RESOURCE_NAME:
        return delete_foundation_runtime(request)

    return _create_async_operation_task(
        request,
        model_name,
        OperationType.DELETE.value,
        OperationType.DELETE.value,
        _validate_delete_state,
        _trigger_model_delete_background,
    )


def _run_model_stop_task(task_id, model_name):
    """Worker function for the service thread executor to stop a model via taosanodectl.

    This runs in an executor thread; completion is applied by the caller via
    _complete_async_operation after this returns its result dict.

    A transient refusal (the lifecycle lock is held by another resource operation
    -- typically a download this stop must wait for) defers the task instead of
    failing it (see _defer_operation). A permanent refusal still fails it.
    """
    try:
        dao = get_dao()
        state = _operation_data(dao, task_id)
        dao.mark_operation_running(task_id)

        taosanodectl = foundation_util.get_taosanodectl_cmd()
        _, exec_timeout = foundation_util.resolve_resource_settings()

        cmd = taosanodectl + ["model", "stop", model_name]

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

        if result.returncode == _CLI_EXIT_TEMPFAIL:
            return _defer_operation(
                dao, task_id, model_name, "stop", state,
                _cli_reason(result.stderr, result.stdout),
            )

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
    submit_service_task(
        _run_model_stop_task,
        args=(task_id, model_name),
        task_id=task_id,
        model_name=model_name,
        operation=OperationType.STOP.value,
        complete_async_operation_fn=_complete_async_operation,
        retry_scheduler_fn=_schedule_service_retry,
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
        OperationType.STOP.value,
        OperationType.STOP.value,
        _validate_stop_state,
        _trigger_model_stop_background,
    )
