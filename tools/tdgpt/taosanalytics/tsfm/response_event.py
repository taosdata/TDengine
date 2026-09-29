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

import json
import time
from datetime import datetime
from enum import Enum
from pathlib import Path

from flask import Response

from taosanalytics.log import AppLogger
from taosanalytics.tsfm.foundation_models_db import (
    get_dao,
    get_resource_dir,
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
    # Combined operation: download the model, then automatically start it once
    # the download finishes. Tracked as a single async operation so clients can
    # watch one task from "queued" all the way to the model being running.
    DOWNLOAD_START = "download_start"


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

DOWNLOAD_FAMILY = frozenset({OperationType.DOWNLOAD.value, OperationType.DOWNLOAD_START.value})

# Name the shared TSFM runtime (venv) goes by everywhere: in the release manifest
# (packaging/tdengine-tdgpt-tsfm-release-*.json), in `resource apply/delete` (see
# SHARED_RUNTIME_NAME in tdgpt/script/resource_manifest.py), as the `model_name`
# of a runtime operation and on the SSE stream. One resource, one name --
# `resource apply` selects by exactly this value.
RUNTIME_RESOURCE_NAME = "tsfm_runtime"


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
    # One snapshot per poll: every payload built below -- the phase rows, the
    # retired rows, the replay and the close-out -- is derived from this list, so
    # a poll costs one directory scan instead of one per operation and phase.
    task_states = _read_resource_task_states()

    # Check each operation for state changes
    for op_info in all_ops:
        if op_info.operation in DOWNLOAD_FAMILY:
            # The generic `download` row would only repeat the download phase
            # (same task, same resource, same moment), so a download operation is
            # streamed through its phase names only. The row stands in for the
            # download phase only while that phase has nothing to report yet, so
            # the task is never silent.
            phases = _build_resource_phase_responses(op_info, task_states, seen_states)
            if phases:
                responses = phases
            else:
                row = do_build_operation_response(op_info, task_states)
                row["operation"] = (
                    _RUNTIME_DOWNLOAD
                    if op_info.model_name == RUNTIME_RESOURCE_NAME
                    else _MODEL_DOWNLOAD
                )
                responses = [row]
        else:
            # start/stop/delete have no phases: their row is the whole story.
            responses = [do_build_operation_response(op_info, task_states)]

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

        response_data = do_build_operation_response(completed_op, task_states)
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
                response_data["model_name"] = RUNTIME_RESOURCE_NAME

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

def _read_resource_task_states():
    """Read the per-resource task files written by taosanodectl.

    Returns an empty list when ``resource_dir`` is not configured or the task
    directory does not exist yet, so the SSE stream degrades to model-only
    updates instead of failing.

    Each call opens and parses every file in the directory, so the SSE emitter
    reads it once per poll and passes that snapshot down to the payloads it
    builds (see _emit_operation_status_updates). Callers that build a single
    payload let it read for itself by omitting ``task_states``.
    """
    resource_dir = get_resource_dir()

    task_root = Path(resource_dir) / "state" / "tasks"
    if not task_root.is_dir():
        AppLogger.warning("Failed to read task state, resource task directory does not exist: %s", task_root)
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


def _build_resource_phase_responses(op_info, task_states, seen_states):
    """Build the per-phase payloads for the resource this operation installs.

    An operation installs exactly one resource: ``tsfm_runtime`` installs the
    shared runtime, any other name installs that model. Matching a task file by
    kind alone is no longer enough -- the runtime can be downloaded on its own
    now, so a runtime task file belonging to a *concurrent* runtime download must
    not be reported as this model operation's runtime phase.
    """
    if op_info.operation not in DOWNLOAD_FAMILY:
        return []

    if op_info.model_name == RUNTIME_RESOURCE_NAME:
        resources = (
            (
                _RUNTIME_DOWNLOAD,
                _RUNTIME_IMPORT,
                RUNTIME_RESOURCE_NAME,
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
                    op_info,
                    operation,
                    resource_name,
                    progress,
                    status,
                    state,
                    task_states,
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
                    op_info, operation, resource_name, progress, status, None, task_states
                )
            )
    return responses


def _build_phase_response(
    op_info, operation, model_name, progress, status, state, task_states=None
):
    """Wrap one phase event into the standard SSE payload.

    The payload reuses the exact field set of the model-level response so the
    stream stays compatible; only `operation`, `model_name`, `progress` and
    `status` are substituted. `model_id`/`package`/`release` are copied from the
    operation row, so a phase event carries the same metadata as its envelope.

    ``task_states`` is the snapshot the caller already read: passing it in keeps a
    poll that reports several phases from re-reading the task directory once per
    phase, and keeps the envelope's `phase` field deriving from the same snapshot
    as the phase events beside it.
    """
    response_data = do_build_operation_response(op_info, task_states)

    response_data["operation"] = operation
    response_data["model_name"] = model_name
    response_data["progress"] = progress
    response_data["status"] = status

    if status == TaskStatus.FAILED.value and state is not None:
        # A phase backed by a task file fails for the resource's own reason; one
        # without a task file (the auto-start step) keeps the operation's error,
        # which do_build_operation_response already put in the payload.
        response_data["error"] = state.get("error") or "Resource install failed"

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

    response_data = do_build_operation_response(operation_info)
    return {
        "status": "success",
        "data": response_data,
    }, 200


def do_build_operation_response(operation_info, task_states=None):
    """Build a detailed response dict for an async operation.

    Args:
        operation_info: AsyncOperation dataclass from DAO
        task_states: Resource task files the caller already read (see
            _read_resource_task_states). Omitted by the single-payload callers --
            the /status poll and the download-reuse response -- which let it be
            read here; the SSE loop passes its per-poll snapshot in, which is what
            keeps a poll from re-reading the task directory once per operation and
            phase.

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

    # append additional metadata for download operations
    if operation in (OperationType.DOWNLOAD.value, OperationType.DOWNLOAD_START.value):
        data = operation_info.data if isinstance(operation_info.data, dict) else {}
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
    _apply_download_phase(response_data, operation_info, task_states)

    # A delete that is waiting for an in-flight resource operation is still
    # `queued`: say what it waits for, so a client can tell waiting from stuck.
    if operation_info.status == TaskStatus.QUEUED.value:
        data = operation_info.data if isinstance(operation_info.data, dict) else {}
        if data.get("waiting_for"):
            response_data["waiting_for"] = data["waiting_for"]
            response_data["attempt"] = data.get("attempt", 0)
            response_data["retry_after"] = data.get("retry_after")

    # append the error information for failed operations
    if operation_info.status == TaskStatus.FAILED.value:
        response_data["error"] = operation_info.error or "Unknown error"
        response_data["error_code"] = operation_info.error_code or ""

    return response_data


def _apply_download_phase(response_data, op_info, task_states=None):
    """Attach the per-phase view of a download to an operation response.

    ``task_states`` is forwarded to _download_phase_snapshot, so a caller that
    already read the task directory does not pay for a second scan.
    """
    if op_info.operation not in DOWNLOAD_FAMILY:
        return

    phase, progress = _download_phase_snapshot(op_info, task_states)
    response_data["phase"] = phase
    if progress is not None:
        response_data["progress"] = progress

def _download_phase_snapshot(op_info, task_states=None):
    """Return the phase a download is in, in the terms the SSE stream reports.

    The phase names, the per-phase progress and the task-file selection all come
    from the helpers the SSE emitter uses, so a client that falls back to polling
    /status renders the same bars as one that streams. That is also why the
    operation row no longer carries a monotonic value of its own: the task file
    already holds the truth.

    Args:
        op_info: Operation row
        task_states: Resource task files the caller already read; read here when
            omitted.

    Returns:
        tuple: (phase_name, progress), where progress is None when the operation
            has no resource task file yet -- it is still queued, or the resource
            was already installed so `resource apply` wrote none.
    """
    if task_states is None:
        task_states = _read_resource_task_states()

    if op_info.model_name == RUNTIME_RESOURCE_NAME:
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


def _parse_resource_timestamp(value):
    """Parse the `updated_at` field written into a resource task file."""
    if not isinstance(value, str):
        return None

    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.timezone.utc)
    return parsed


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
