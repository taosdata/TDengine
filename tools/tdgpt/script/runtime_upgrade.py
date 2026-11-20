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

"""Persistent maintenance operations for the shared TSFM environment."""

import re
import shutil
import time
import uuid
from pathlib import Path

from resource_manager import DynamicResourceManager, ResourceBusyError, ResourceImportError, lifecycle_lock
from resource_manifest import COMPLETION_FILE, SHARED_RUNTIME_NAME, load_manifest, validate_tsfm_runtime

TERMINAL = frozenset({"completed", "failed"})
WORKER_START_TIMEOUT = 30


class RuntimeUpgrade:
    """Coordinate a runtime replacement while the caller holds the lifecycle lock."""

    def __init__(self, config, models=None):
        self.manager = DynamicResourceManager(config)
        self.models = models
        self.root = self.manager.resource_root / "state" / "upgrades"
        self.current = self.root / "current.json"

    def task_path(self, task_id):
        if not re.fullmatch(r"upgrade-[0-9a-f]{32}", task_id or ""):
            raise ResourceImportError("invalid runtime upgrade task ID")
        return self.root / f"{task_id}.json"

    def read(self, task_id):
        path = self.task_path(task_id)
        if not path.is_file():
            raise ResourceImportError("runtime upgrade task not found")
        try:
            task = load_manifest(path)
        except (OSError, ValueError) as exc:
            raise ResourceImportError(f"invalid runtime upgrade task: {exc}") from exc
        if not isinstance(task, dict) or task.get("task_id") != task_id:
            raise ResourceImportError("invalid runtime upgrade task")
        required = ("status", "phase", "action", "backup")
        if any(not isinstance(task.get(field), str) or not task[field] for field in required):
            raise ResourceImportError("invalid runtime upgrade task")
        if any(not isinstance(task.get(field), dict) for field in ("target", "previous")):
            raise ResourceImportError("invalid runtime upgrade task")
        if not isinstance(task.get("running_models"), list):
            raise ResourceImportError("invalid runtime upgrade task")
        return task

    def latest(self):
        if not self.current.exists():
            return None
        try:
            pointer = load_manifest(self.current)
            task_id = pointer.get("task_id") if isinstance(pointer, dict) else None
            if not task_id:
                raise ResourceImportError("invalid current runtime upgrade pointer")
            return self.read(task_id)
        except (OSError, ValueError, KeyError, ResourceImportError) as exc:
            raise ResourceImportError(f"invalid current runtime upgrade state: {exc}") from exc

    def save(self, task, **fields):
        task.update(fields, updated_at=time.time())
        self.manager._write_json(self.task_path(task["task_id"]), task)
        return task

    def guard(self):
        task = self.latest()
        if task and task["status"] not in TERMINAL:
            raise ResourceBusyError(f"runtime maintenance requires attention: {task['task_id']}")

    def status(self, task_id):
        task = self.read(task_id)
        if task["status"] not in TERMINAL and task["phase"] != "manual_intervention":
            try:
                with lifecycle_lock(self.manager.resource_root):
                    task = self.read(task_id)
                    queued_expired = time.time() - task["updated_at"] >= WORKER_START_TIMEOUT
                    if task["status"] not in TERMINAL and (task["phase"] != "queued" or queued_expired):
                        self.save(task, status="interrupted", phase="manual_intervention",
                                  error="Upgrade worker stopped; explicit recovery is required")
            except ResourceBusyError:
                pass
        return task

    def inventory(self, release="", base_url=""):
        installed = None
        error = None
        try:
            manifest, _ = validate_tsfm_runtime(self.manager.runtime_root)
            completion = load_manifest(self.manager.runtime_root / COMPLETION_FILE)
            installed = {**manifest, "archive_sha256": completion.get("archive_sha256", "")}
        except (OSError, ValueError) as exc:
            error = str(exc)
        target = None
        target_error = None
        if release is not None:
            try:
                if release == "latest":
                    channel = self.manager.load_release("latest", base_url)
                    release = channel.get("release")
                    if not isinstance(release, str) or release == "latest" or not re.fullmatch(
                        r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", release
                    ):
                        raise ResourceImportError("latest manifest must identify a concrete release")
                manifest = self.manager.load_release(release, base_url)
                if (release and manifest.get("release") != release and not Path(release).is_file()
                        and not release.startswith(("http://", "https://"))):
                    raise ResourceImportError("release manifest does not match the requested version")
                items = self.manager._select_resources(manifest, SHARED_RUNTIME_NAME)
                if len(items) != 1:
                    raise ResourceImportError("release must select exactly one platform runtime")
                digest = items[0].get("sha256")
                if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
                    raise ResourceImportError("target runtime must have a valid SHA-256")
                target = {**items[0], "release": manifest.get("release"), "source": manifest["_source_url"]}
            except (OSError, ValueError, ResourceImportError) as exc:
                target_error = str(exc)
        latest = self.latest()
        if latest:
            latest = self.status(latest["task_id"])
        return {
            "installed": installed,
            "installed_error": error,
            "target": target,
            "target_error": target_error,
            "upgrade_available": bool(installed and target and
                                      installed.get("archive_sha256") != target.get("sha256")),
            "task": latest,
        }

    def queue(self, release, base_url, expected_sha256):
        self.guard()
        if not release or release == "latest":
            raise ResourceImportError("query latest first and submit its concrete release and SHA-256")
        inventory = self.inventory("", base_url)
        bundled_target = inventory["target"]
        if not bundled_target or (bundled_target.get("release"), bundled_target.get("sha256")) != (release, expected_sha256):
            inventory = self.inventory(release, base_url)
        if not inventory["installed"]:
            raise ResourceImportError("A valid installed runtime is required; use runtime/download for installation")
        if not inventory["target"]:
            raise ResourceImportError(inventory["target_error"] or "target runtime unavailable")
        if inventory["target"]["sha256"] != expected_sha256:
            raise ResourceImportError("target digest changed; query runtime inventory again")
        previous_task = inventory["task"]
        if previous_task and previous_task["status"] == "completed" and (
            previous_task.get("target", {}).get("sha256") == expected_sha256
            and inventory["installed"].get("archive_sha256") == expected_sha256
        ):
            return previous_task
        task_id = f"upgrade-{uuid.uuid4().hex}"
        task = {
            "task_id": task_id, "operation": "runtime_upgrade", "model_name": SHARED_RUNTIME_NAME,
            "status": "queued", "phase": "queued", "progress": 0, "release": release,
            "resource_base_url": base_url, "target": inventory["target"],
            "previous": inventory["installed"], "running_models": [], "created_at": time.time(),
            "backup": str(self.manager.runtime_root.parent / f".tsfm-rollback-{task_id}"),
            "action": "upgrade", "runtime_touched": False, "workers_touched": False,
            "backup_complete": False,
        }
        self.save(task)
        self.manager._write_json(self.current, {"task_id": task_id})
        return task

    def queue_recovery(self, task_id):
        task = self.read(task_id)
        latest = self.latest()
        if not latest or latest["task_id"] != task_id:
            raise ResourceImportError("only the latest runtime upgrade can be recovered")
        if task["status"] not in {"interrupted", "failed", "completed"}:
            raise ResourceBusyError("upgrade is still active; query its status before recovery")
        if task["status"] in TERMINAL and task.get("workers_touched"):
            if task["status"] == "completed":
                expected = task.get("target", {})
                if not re.fullmatch(r"[0-9a-f]{64}", str(expected.get("sha256", "")).lower()):
                    raise ResourceImportError("cannot recover task without target runtime digest")
            else:
                digest = task.get("previous", {}).get("archive_sha256", "")
                if not re.fullmatch(r"[0-9a-f]{64}", str(digest).lower()):
                    raise ResourceImportError("cannot recover task without previous runtime digest")
                expected = {"kind": "runtime", "sha256": digest}
            if not self.manager._installed_matches(expected):
                raise ResourceImportError("runtime changed after this task; refusing stale recovery")
        if not task.get("workers_touched"):
            if task["status"] == "completed":
                return task
            self._cleanup_preparation(task)
            return self.save(task, status="failed", phase="aborted", action="rollback", error=None)
        if task.get("runtime_touched"):
            validate_tsfm_runtime(Path(task["backup"]))
        return self.save(task, status="queued", phase="queued", action="rollback", error=None,
                         previous_error=task.get("error"), rollback_error=None, rollback_status=None)

    def _restore_workers(self, names):
        for name in names:
            if not self.models.start(name):
                raise ResourceImportError(f"model service failed to start: {name}")
        running = {item["model"] for item in self.models.status() if item["running"]}
        if not set(names).issubset(running):
            raise ResourceImportError("not all previously running models restarted")

    def _stop_workers(self, names):
        for name in names:
            if not self.models.stop(name):
                raise ResourceImportError(f"model service failed to stop: {name}")
        if any(item["running"] for item in self.models.status()):
            raise ResourceImportError("model workers are still running")

    def rollback(self, task):
        self.save(task, status="running", phase="rolling_back", progress=80)
        running = [item["model"] for item in self.models.status() if item["running"]]
        self._stop_workers(running)
        previous = {"kind": "runtime", "sha256": task["previous"].get("archive_sha256", "")}
        if task.get("runtime_touched") and not self.manager._installed_matches(previous):
            backup = Path(task["backup"])
            validate_tsfm_runtime(backup)
            restored = self.manager.runtime_root.parent / f".tsfm-restore-{uuid.uuid4().hex}"
            try:
                shutil.copytree(backup, restored, symlinks=True)
                self.manager._activate(restored, self.manager.runtime_root)
            finally:
                shutil.rmtree(restored, ignore_errors=True)
        if not self.manager.verify_resources(SHARED_RUNTIME_NAME)["valid"]:
            raise ResourceImportError("restored runtime failed integrity verification")
        self._restore_workers(task["running_models"])
        self.save(task, status="failed", phase="rolled_back", rollback_status="completed", progress=100)

    def _staging_path(self, task):
        return self.manager.runtime_root.parent / f".tsfm-upgrade-{task['task_id']}"

    def _cleanup_preparation(self, task):
        shutil.rmtree(self._staging_path(task), ignore_errors=True)
        if not task.get("backup_complete") and not task.get("workers_touched"):
            shutil.rmtree(task["backup"], ignore_errors=True)

    def run(self, task_id):
        self.manager._acquire_lock()
        try:
            return self._run(task_id)
        finally:
            self.manager._release_lock()

    def _run(self, task_id):
        task = self.read(task_id)
        if task["phase"] != "queued" or self.latest()["task_id"] != task_id:
            raise ResourceImportError("upgrade task is no longer queued")
        try:
            if task["action"] == "rollback":
                self.rollback(task)
                return task
            self.save(task, status="running", phase="preparing", progress=5)
            target = task["target"]
            if self.manager._installed_matches(target):
                return self.save(task, status="completed", phase="unchanged", progress=100)
            archive = self.manager._download(target, target["source"])
            if self.manager._classify(archive) != ("runtime", SHARED_RUNTIME_NAME):
                raise ResourceImportError("target archive is not a shared runtime")
            staging = self._staging_path(task)
            self.manager._extract(archive, "model_venvs/tsfm", staging)
            self.manager._relocate_runtime(staging)
            self.manager._runtime_complete(staging, target["sha256"])
            backup = Path(task["backup"])
            shutil.copytree(self.manager.runtime_root, backup, symlinks=True)
            validate_tsfm_runtime(backup)
            self.save(task, backup_complete=True)
            running = [item["model"] for item in self.models.status() if item["running"]]
            self.save(task, phase="stopping", progress=35, running_models=running, workers_touched=True)
            self._stop_workers(running)
            self.save(task, phase="installing", progress=50, runtime_touched=True)
            self.manager._activate(staging, self.manager.runtime_root)
            self.save(task, phase="verifying", progress=75)
            if not self.manager._installed_matches(target):
                raise ResourceImportError("new runtime failed integrity verification")
            self.save(task, phase="resuming", progress=90)
            self._restore_workers(running)
            self.manager._cleanup_cached_archive(archive)
            self.manager._update_task(self.manager._task_path(target["sha256"]), target, "completed",
                                      target.get("size", 0), target.get("size", 0))
            return self.save(task, status="completed", phase="completed", progress=100)
        except Exception as exc:
            self.save(task, error=str(exc))
            target = task.get("target", {})
            digest = target.get("sha256", "")
            if task["action"] == "upgrade" and re.fullmatch(r"[0-9a-f]{64}", digest):
                self.manager._update_task(self.manager._task_path(digest), target, "failed", error=str(exc))
            if task["action"] == "upgrade" and task.get("workers_touched"):
                try:
                    self.rollback(task)
                    return task
                except Exception as rollback_error:
                    self.save(task, rollback_error=str(rollback_error))
            if task.get("workers_touched") or task["action"] == "rollback":
                return self.save(task, status="interrupted", phase="manual_intervention")
            return self.save(task, status="failed", phase="failed")
        finally:
            self._cleanup_preparation(task)


def run_upgrade_task(config, task_id, models):
    """Run one queued upgrade transaction while holding the shared lifecycle lock."""
    deadline = time.monotonic() + WORKER_START_TIMEOUT
    while True:
        try:
            with lifecycle_lock(DynamicResourceManager(config).resource_root):
                return RuntimeUpgrade(config, models=models).run(task_id)
        except ResourceBusyError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.1)
