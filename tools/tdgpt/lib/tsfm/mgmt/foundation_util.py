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

"""Shared utilities for foundation model operations."""

import json
import logging
import subprocess
import sys
import uuid
from pathlib import Path

from lib.conf import Configure
from lib.log import AppLogger
from lib.tsfm.mgmt.foundation_models_db import get_dao
from lib.tsfm.mgmt.response_event import DOWNLOAD_FAMILY, TaskStatus

_DEFAULT_MODEL_START_TIMEOUT = 300


def make_task_id(operation: str, model_name: str) -> str:
    """Generate a unique task identifier for an async operation.
       Format: {operation}-{model_name}-{random_hex}
    """
    return f"{operation}-{model_name}-{uuid.uuid4().hex[:8]}"


def log_invocation(action, **params):
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


def taosanodectl_log_level() -> str:
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


def get_taosanodectl_cmd():
    """Return the command prefix for invoking taosanodectl.

    Prefers the system-installed taosanodectl binary. Falls back to:
    1. Production layout: taosanode/bin/taosanodectl.py
    2. Development layout: tdgpt/script/taosanodectl.py

    The prefix carries the child log level as well (``--log-level``, taken from
    the configured log_level — see taosanodectl_log_level), so every
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
            dev_script = Path(__file__).resolve().parent.parent.parent.parent / "script" / "taosanodectl.py"
            if not dev_script.exists():
                raise FileNotFoundError(
                    "taosanodectl not found in PATH or script directory"
                )
            prefix = [sys.executable, str(dev_script)]

    return prefix + ["--log-level", taosanodectl_log_level()]


def resolve_resource_settings(request=None, payload=None):
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
    log_invocation(
        "resource settings resolved",
        source=source,
        resource_base_url=resource_base_url,
        overridden=resource_base_url != default_resource_url,
        tsfm_resource_exec_timeout=exec_timeout,
    )
    return resource_base_url, exec_timeout


def live_worker_names():
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
        taosanodectl = get_taosanodectl_cmd()
    except FileNotFoundError as exc:
        AppLogger.warning("Cannot probe model worker liveness: %s", exc)
        return None

    _, exec_timeout = resolve_resource_settings()
    cmd = taosanodectl + ["model", "status", "--format", "json"]

    log_invocation("model worker liveness probe", cmd=" ".join(cmd))

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


# Default for how long a resource task file keeps witnessing a live CLI child.
# Overridable through `resource_task_stale_seconds` (see the getter below).
DEFAULT_RESOURCE_TASK_STALE_SECONDS = 300


def resource_task_stale_seconds():
    """How old a resource task file may be and still witness a live CLI child.

    The resource manager rewrites its task file on every progress callback, so a
    file that has not moved for longer than this belongs to a download whose
    process is gone. The window therefore has to be wider than the progress
    granularity of the slowest link in use.

    Getting it wrong is not symmetric: guessing "gone" too early only sends the
    delete worker back to its deferral (it retries while the lifecycle lock is
    held), while guessing "still alive" too long keeps refusing a delete the
    caller is entitled to.

    Returns:
        int: The window in seconds. Never zero: a zero window would treat every
            download as finished.
    """
    config = Configure.get_instance()
    try:
        value = int(
            config._conf.get(
                "resource_task_stale_seconds", DEFAULT_RESOURCE_TASK_STALE_SECONDS
            )
        )
    except (TypeError, ValueError, AttributeError):
        return DEFAULT_RESOURCE_TASK_STALE_SECONDS

    return value if value > 0 else DEFAULT_RESOURCE_TASK_STALE_SECONDS


def installed_resource_names():
    """Return the models whose files are present on disk, or None if unknown.

    Reads `taosanodectl resource list --format json`, the inventory the CLI builds
    from the installed directories themselves -- the same source `get_model_info`
    reports. `models.downloaded` is only a cache of this answer, written when this
    API finishes a download, so a directory installed or removed from the CLI
    leaves it describing a lifecycle that no longer holds.

    Includes both ``installed`` and ``invalid`` entries: a directory that failed
    manifest validation still exists on disk. Treating it as absent would cause
    callers to set ``downloaded=False`` and ``model_state=missing``, potentially
    triggering delete logic against a directory that is present. Corruption is a
    separate concern from absence.

    Returns:
        list[str]: Model names whose directory is present (sorted).
        None: The inventory could not be read. Callers must not read that as
            "nothing is installed".
    """
    try:
        taosanodectl = get_taosanodectl_cmd()
    except FileNotFoundError as exc:
        AppLogger.warning("Cannot probe the installed resources: %s", exc)
        return None

    _, exec_timeout = resolve_resource_settings()
    cmd = taosanodectl + ["resource", "list", "--format", "json"]

    log_invocation("installed resource probe", cmd=" ".join(cmd))

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=exec_timeout)
    except subprocess.TimeoutExpired:
        AppLogger.warning("Installed resource probe timed out (>%ss)", exec_timeout)
        return None
    except (OSError, subprocess.SubprocessError) as exc:
        AppLogger.warning("Installed resource probe failed: %s", exc)
        return None

    if result.returncode != 0:
        AppLogger.warning(
            "Installed resource probe exited %s: %s",
            result.returncode,
            (result.stderr or "").strip()[:500],
        )
        return None

    try:
        inventory = json.loads(result.stdout)
    except (TypeError, ValueError):
        AppLogger.warning(
            "Cannot parse installed resource output: %s", (result.stdout or "")[:200]
        )
        return None

    models = inventory.get("models") if isinstance(inventory, dict) else None
    if not isinstance(models, list):
        return None

    return sorted(
        str(entry.get("name"))
        for entry in models
        if isinstance(entry, dict)
        and entry.get("install_status") in ("installed", "invalid")
        and entry.get("name")
    )


def update_model_state(model_name, downloaded=None, running=None):
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


def resolve_model_start_timeout():
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

    log_invocation(
        "model start timeout resolved", tsfm_model_start_timeout=start_timeout
    )
    return start_timeout

def get_active_download_op(model_name):
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
        if op.operation in DOWNLOAD_FAMILY and op.status in (
            TaskStatus.QUEUED.value,
            TaskStatus.RUNNING.value,
        ):
            return op
    return None
