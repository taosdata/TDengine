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

import hashlib
import json
import platform
import shutil
import subprocess
import sys
import tarfile
import time
from pathlib import Path

import pytest
from resource_manager import ResourceBusyError, ResourceImportError, lifecycle_lock
from runtime_upgrade import RuntimeUpgrade

from tests.resource_manager_test import _release_manifest, _runtime_archive


class Workers:
    def __init__(self):
        self.running = {"moirai"}
        self.calls = []
        self.fail_next_start = False

    def status(self):
        return [{"model": name, "running": name in self.running} for name in ("moirai", "moment")]

    def stop(self, name):
        self.calls.append(("stop", name))
        self.running.discard(name)
        return True

    def start(self, name):
        self.calls.append(("start", name))
        if self.fail_next_start:
            self.fail_next_start = False
            return False
        self.running.add(name)
        return True


@pytest.fixture
def upgrade(tmp_path, mock_config, monkeypatch):
    monkeypatch.setattr("resource_manifest.normalized_platform", lambda: "linux")
    monkeypatch.setattr("resource_manifest.normalized_arch", lambda: "x64")
    monkeypatch.setattr("resource_manager.normalized_platform", lambda: "linux")
    monkeypatch.setattr("resource_manager.normalized_arch", lambda: "x64")
    runtime = tmp_path / "python-runtime" / "bin"
    runtime.mkdir(parents=True)
    executable = runtime / "python3"
    executable.write_text("#!/bin/sh\necho 3.12.12\n")
    executable.chmod(0o755)
    mock_config.python_runtime_dir = str(runtime.parent)
    service = RuntimeUpgrade(mock_config, Workers())
    archive = _runtime_archive(tmp_path)
    service.manager.import_package(str(archive))
    source = tmp_path / "source" / "model_venvs" / "tsfm"
    manifest = json.loads((source / "manifest.json").read_text())
    manifest["runtime_id"] = "tsfm-runtime-1.0.1"
    (source / "manifest.json").write_text(json.dumps(manifest))
    new_archive = tmp_path / "new-runtime.tar.xz"
    with tarfile.open(new_archive, "w:xz") as output:
        output.add(source, arcname="model_venvs/tsfm")
    release = _release_manifest(tmp_path, new_archive, {})
    digest = hashlib.sha256(new_archive.read_bytes()).hexdigest()
    return service, release, digest


def queue(upgrade):
    service, release, digest = upgrade
    with lifecycle_lock(service.manager.resource_root):
        return service.queue(str(release), "", digest)


@pytest.mark.parametrize("schema_version", [1, 2])
def test_latest_resolves_concrete_release_for_current_platform(upgrade, monkeypatch, schema_version):
    service, release, digest = upgrade
    manifest = json.loads(release.read_text())
    manifest.update(release="1.0.1", schema_version=schema_version, _source_url="https://example/1.0.1/release.json")
    calls = []
    def load(version, base_url):
        calls.append((version, base_url))
        return {"release": "1.0.1"} if version == "latest" else manifest
    monkeypatch.setattr(service.manager, "load_release", load)
    result = service.inventory("latest", "https://example")
    assert calls == [("latest", "https://example"), ("1.0.1", "https://example")]
    assert result["target"]["release"] == "1.0.1"
    assert result["target"]["sha256"] == digest
    assert result["upgrade_available"] is True
    assert result["target_error"] is None
    assert service.models.calls == []


@pytest.mark.parametrize("schema_version", [1, 2])
def test_runtime_inventory_reads_release_with_independent_model_version(upgrade, schema_version):
    service, release, digest = upgrade
    manifest = json.loads(release.read_text())
    manifest["schema_version"] = schema_version
    model = {"kind": "model", "name": "moirai", "version": "1.0.0", "file": "weights.zip"}
    if schema_version == 2:
        model["path"] = "models/weights.zip"
    manifest["models"] = [model]
    release.write_text(json.dumps(manifest))
    inventory = service.inventory(str(release), "")
    assert inventory["target_error"] is None
    assert inventory["target"]["sha256"] == digest
    assert inventory["target"]["kind"] == "runtime"
    assert inventory["upgrade_available"]


@pytest.mark.parametrize("version", [None, "latest", "../bad", "https://example/release.json"])
def test_invalid_latest_preserves_installed_inventory(upgrade, monkeypatch, version):
    service, _, _ = upgrade
    monkeypatch.setattr(service.manager, "load_release", lambda *args: {"release": version})
    result = service.inventory("latest", "https://example")
    assert result["installed"]["runtime_id"] == "tsfm-runtime-1.0.0"
    assert result["target"] is None
    assert result["target_error"]
    assert result["upgrade_available"] is False


def test_latest_network_error_preserves_installed_inventory(upgrade, monkeypatch):
    service, _, _ = upgrade
    def unavailable(*args):
        raise ResourceImportError("manifest unavailable")
    monkeypatch.setattr(service.manager, "load_release", unavailable)
    result = service.inventory("latest", "https://example")
    assert result["installed"]
    assert result["target_error"] == "manifest unavailable"
    assert not result["upgrade_available"]


@pytest.mark.parametrize("schema_version", [1, 2])
def test_bundled_inventory_and_cached_upgrade_need_no_network(upgrade, tmp_path, monkeypatch, schema_version):
    service, release, digest = upgrade
    manifest = json.loads(release.read_text())
    manifest["schema_version"] = schema_version
    script = tmp_path / "install" / "bin" / "resource_manager.py"
    script.parent.mkdir(parents=True)
    metadata = script.parent.parent / "share" / "tsfm" / "tsfm-release.json"
    metadata.parent.mkdir(parents=True)
    metadata.write_text(json.dumps(manifest))
    monkeypatch.setattr("resource_manager.__file__", str(script))
    def no_network(*args, **kwargs):
        pytest.fail("bundled metadata and cached package must not access the network")
    monkeypatch.setattr("resource_manager.urllib.request.urlopen", no_network)
    base_url = "https://example.test"
    inventory = service.inventory("", base_url)
    assert inventory["target"]["sha256"] == digest
    assert inventory["target_error"] is None
    assert inventory["target"]["source"].startswith(base_url)
    archive = release.parent / manifest["runtimes"][0]["file"]
    service.manager.cache_root.mkdir(parents=True, exist_ok=True)
    shutil.copy2(archive, service.manager.cache_root / f"{digest}.archive")
    with lifecycle_lock(service.manager.resource_root):
        task = service.queue(manifest["release"], base_url, digest)
        assert service.run(task["task_id"])["status"] == "completed"
    assert service.inventory(None)["installed"]["runtime_id"] == "tsfm-runtime-1.0.1"


@pytest.mark.parametrize("schema_version", [1, 2])
def test_bundled_manifest_rejects_shared_path_on_runtime(upgrade, tmp_path, monkeypatch, schema_version):
    service, release, _ = upgrade
    manifest = json.loads(release.read_text())
    manifest["schema_version"] = schema_version
    manifest["runtimes"][0]["path"] = "models/runtime.tar.xz"
    script = tmp_path / "install" / "bin" / "resource_manager.py"
    metadata = script.parent.parent / "share" / "tsfm" / "tsfm-release.json"
    metadata.parent.mkdir(parents=True)
    metadata.write_text(json.dumps(manifest))
    monkeypatch.setattr("resource_manager.__file__", str(script))
    inventory = service.inventory("", "https://example.test")
    assert inventory["target"] is None
    assert inventory["target_error"]
    assert inventory["installed"]


def test_missing_bundled_manifest_preserves_installed_state_without_network(upgrade, tmp_path, monkeypatch):
    service, _, _ = upgrade
    monkeypatch.setattr("resource_manager.__file__", str(tmp_path / "bin" / "resource_manager.py"))
    def no_network(*args, **kwargs):
        pytest.fail("default inventory must not access the network")
    monkeypatch.setattr("resource_manager.urllib.request.urlopen", no_network)
    inventory = service.inventory()
    assert inventory["installed"]
    assert inventory["target"] is None
    assert "bundled TSFM release manifest unavailable" in inventory["target_error"]


def test_corrupt_current_pointer_fails_closed(upgrade):
    service, _, _ = upgrade
    service.current.parent.mkdir(parents=True, exist_ok=True)
    service.current.write_text("{broken", encoding="utf-8")
    with pytest.raises(ResourceImportError, match="invalid current runtime upgrade state"):
        service.latest()


def test_status_does_not_interrupt_recently_queued_worker(upgrade):
    service, _, _ = upgrade
    task = queue(upgrade)
    result = service.status(task["task_id"])
    assert result["status"] == "queued"
    assert result["phase"] == "queued"


def test_stale_queued_worker_requires_recovery_and_cannot_start(upgrade):
    service, _, _ = upgrade
    task = queue(upgrade)
    task["updated_at"] = 0
    service.manager._write_json(service.task_path(task["task_id"]), task)
    result = service.status(task["task_id"])
    assert result["status"] == "interrupted"
    assert result["phase"] == "manual_intervention"
    with lifecycle_lock(service.manager.resource_root):
        with pytest.raises(ResourceImportError, match="no longer queued"):
            service.run(task["task_id"])
        recovered = service.queue_recovery(task["task_id"])
        assert recovered["phase"] == "aborted"
        service.guard()
    assert service.models.calls == []


def test_stale_queued_status_does_not_write_while_lifecycle_locked(upgrade):
    service, _, _ = upgrade
    task = queue(upgrade)
    task["updated_at"] = 0
    service.manager._write_json(service.task_path(task["task_id"]), task)
    with lifecycle_lock(service.manager.resource_root):
        assert service.status(task["task_id"])["status"] == "queued"
    assert service.read(task["task_id"])["status"] == "queued"


def test_worker_lock_timeout_does_not_modify_task(upgrade, mock_config, monkeypatch):
    from runtime_upgrade import run_upgrade_task
    service, _, _ = upgrade
    task = queue(upgrade)
    clock = iter([0, 31])
    monkeypatch.setattr("runtime_upgrade.time.monotonic", lambda: next(clock))
    with lifecycle_lock(service.manager.resource_root):
        with pytest.raises(ResourceBusyError):
            run_upgrade_task(mock_config, task["task_id"], service.models)
    assert service.read(task["task_id"]) == task


def test_recovery_rejects_missing_previous_digest(upgrade):
    service, _, _ = upgrade
    task = queue(upgrade)
    service.save(
        task,
        status="failed",
        phase="failed",
        workers_touched=True,
        runtime_touched=True,
        previous={"kind": "runtime", "archive_sha256": ""},
    )
    with lifecycle_lock(service.manager.resource_root):
        with pytest.raises(ResourceImportError, match="previous runtime digest"):
            service.queue_recovery(task["task_id"])


@pytest.mark.parametrize("problem", ["wrong_release", "wrong_platform", "invalid_digest"])
def test_latest_rejects_unusable_fixed_manifest(upgrade, monkeypatch, problem):
    service, release, _ = upgrade
    manifest = json.loads(release.read_text())
    manifest.update(release="1.0.1", _source_url="https://example/1.0.1/release.json")
    if problem == "wrong_release":
        manifest["release"] = "1.0.0"
    elif problem == "wrong_platform":
        manifest["runtimes"][0]["platform"] = "windows"
    else:
        manifest["runtimes"][0]["sha256"] = None
    monkeypatch.setattr(service.manager, "load_release", lambda version, base_url:
                        {"release": "1.0.1"} if version == "latest" else manifest)
    result = service.inventory("latest", "https://example")
    assert result["installed"]
    assert result["target"] is None
    assert result["target_error"]
    assert not result["upgrade_available"]


def test_upgrade_rejects_mutable_latest_target(upgrade):
    service, _, digest = upgrade
    with pytest.raises(ResourceImportError, match="concrete release"):
        service.queue("latest", "https://example", digest)


@pytest.mark.parametrize("terminal", ["completed", "rolled_back"])
def test_recovery_rejects_runtime_replaced_outside_upgrade(upgrade, terminal):
    service, release, _ = upgrade
    task = queue(upgrade)
    run(service, task)
    if terminal == "rolled_back":
        with lifecycle_lock(service.manager.resource_root):
            service.queue_recovery(task["task_id"])
        run(service, task)
    source = release.parent / "source" / "model_venvs" / "tsfm"
    manifest_path = source / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["runtime_id"] = "independently-installed-runtime"
    manifest_path.write_text(json.dumps(manifest))
    archive = release.parent / "independent-runtime.tar.xz"
    with tarfile.open(archive, "w:xz") as output:
        output.add(source, arcname="model_venvs/tsfm")
    service.models.stop("moirai")
    with lifecycle_lock(service.manager.resource_root):
        service.manager.import_packages([str(archive)], replace=True)
    with lifecycle_lock(service.manager.resource_root):
        with pytest.raises(ResourceImportError, match="stale recovery"):
            service.queue_recovery(task["task_id"])
    assert service.inventory()["installed"]["runtime_id"] == "independently-installed-runtime"


def test_partial_backup_is_removed_before_retry(upgrade, monkeypatch):
    service, _, _ = upgrade
    task = queue(upgrade)
    def partial_copy(source, destination, **kwargs):
        Path(destination).mkdir()
        (Path(destination) / "partial").write_bytes(b"partial backup")
        raise OSError("snapshot copy failed")
    monkeypatch.setattr("runtime_upgrade.shutil.copytree", partial_copy)
    result = run(service, task)
    assert result["phase"] == "failed"
    assert not Path(task["backup"]).exists()
    assert not service._staging_path(task).exists()
    assert service.models.calls == []


def test_extract_space_failure_never_stops_models(upgrade, monkeypatch):
    service, _, _ = upgrade
    task = queue(upgrade)
    def no_space(archive, prefix, destination):
        destination.mkdir()
        (destination / "partial").write_bytes(b"unfinished new runtime")
        raise OSError("No space left on device")
    monkeypatch.setattr(service.manager, "_extract", no_space)
    result = run(service, task)
    assert result["phase"] == "failed"
    assert service.models.running == {"moirai"}
    assert service.models.calls == []
    assert not service._staging_path(task).exists()
    assert not Path(task["backup"]).exists()


def test_invalid_snapshot_is_removed(upgrade, monkeypatch):
    service, _, _ = upgrade
    task = queue(upgrade)
    from runtime_upgrade import validate_tsfm_runtime
    def validate(path):
        if path == Path(task["backup"]):
            raise ValueError("snapshot invalid")
        return validate_tsfm_runtime(path)
    monkeypatch.setattr("runtime_upgrade.validate_tsfm_runtime", validate)
    result = run(service, task)
    assert result["phase"] == "failed"
    assert not Path(task["backup"]).exists()
    assert service.models.calls == []


def test_pre_stop_interruption_cleans_partial_preparation(upgrade):
    service, _, _ = upgrade
    task = queue(upgrade)
    Path(task["backup"]).mkdir()
    service._staging_path(task).mkdir()
    service.save(task, status="interrupted", phase="manual_intervention")
    with lifecycle_lock(service.manager.resource_root):
        service.queue_recovery(task["task_id"])
    assert not Path(task["backup"]).exists()
    assert not service._staging_path(task).exists()
    assert service.models.calls == []


def test_interrupted_activation_with_missing_live_path_recovers_snapshot(upgrade):
    service, _, _ = upgrade
    task = queue(upgrade)
    backup = Path(task["backup"])
    shutil.copytree(service.manager.runtime_root, backup, symlinks=True)
    shutil.rmtree(service.manager.runtime_root)
    service.models.running.clear()
    service.save(task, status="interrupted", phase="manual_intervention", backup_complete=True,
                 workers_touched=True, runtime_touched=True, running_models=["moirai"])
    with lifecycle_lock(service.manager.resource_root):
        service.queue_recovery(task["task_id"])
    result = run(service, task)
    assert result["phase"] == "rolled_back"
    assert service.models.running == {"moirai"}
    assert service.inventory()["installed"]["runtime_id"] == "tsfm-runtime-1.0.0"


def test_failed_activation_resumes_intact_runtime_without_copying(upgrade, monkeypatch):
    service, _, _ = upgrade
    task = queue(upgrade)
    copytree = shutil.copytree
    copies = []
    def copy_once(source, destination, *arguments, **kwargs):
        if Path(source) == service.manager.runtime_root or Path(source) == Path(task["backup"]):
            copies.append(destination)
            if len(copies) > 1:
                raise OSError("no space for another runtime copy")
        return copytree(source, destination, *arguments, **kwargs)
    def failed_activation(*args):
        raise OSError("activation failed before replacing runtime")
    monkeypatch.setattr("runtime_upgrade.shutil.copytree", copy_once)
    monkeypatch.setattr(service.manager, "_activate", failed_activation)
    result = run(service, task)
    assert result["phase"] == "rolled_back", result
    assert result["rollback_status"] == "completed"
    assert len(copies) == 1
    assert service.models.running == {"moirai"}
    assert service.manager._installed_matches({"kind": "runtime", "sha256": task["previous"]["archive_sha256"]})


@pytest.mark.parametrize("problem", ["python_version", "requirements_sha256"])
def test_inner_manifest_invalidity_never_stops_models(upgrade, problem):
    service, release, _ = upgrade
    root = release.parent / "source" / "model_venvs" / "tsfm"
    manifest_path = root / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest[problem] = "3.12.16" if problem == "python_version" else "0" * 64
    manifest_path.write_text(json.dumps(manifest))
    archive = release.parent / "incompatible.tar.xz"
    with tarfile.open(archive, "w:xz") as output:
        output.add(root, arcname="model_venvs/tsfm")
    release = _release_manifest(release.parent, archive, {})
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    with lifecycle_lock(service.manager.resource_root):
        task = service.queue(str(release), "", digest)
    result = run(service, task)
    assert result["phase"] == "failed"
    assert service.models.calls == []
    assert service.models.running == {"moirai"}
    assert not service._staging_path(task).exists()


def run(service, task):
    with lifecycle_lock(service.manager.resource_root):
        return service.run(task["task_id"])


def test_upgrade_switches_runtime_and_restarts_only_running_models(upgrade):
    service, _, _ = upgrade
    task = queue(upgrade)
    result = run(service, task)
    assert result["status"] == "completed"
    assert service.inventory()["installed"]["runtime_id"] == "tsfm-runtime-1.0.1"
    assert service.models.calls == [("stop", "moirai"), ("start", "moirai")]
    assert Path(result["backup"]).is_dir()
    assert service.models.running == {"moirai"}


def test_bad_archive_fails_before_stopping_models(upgrade, monkeypatch):
    service, _, _ = upgrade
    task = queue(upgrade)
    def fail_download(*args):
        raise ResourceImportError("download SHA-256 mismatch")
    monkeypatch.setattr(service.manager, "_download", fail_download)
    result = run(service, task)
    assert result["phase"] == "failed"
    assert service.models.calls == []
    service.guard()


def test_model_archive_cannot_be_installed_as_runtime(upgrade, monkeypatch):
    service, _, _ = upgrade
    task = queue(upgrade)
    monkeypatch.setattr(service.manager, "_classify", lambda archive: ("model", "moment"))
    result = run(service, task)
    assert result["phase"] == "failed"
    assert "not a shared runtime" in result["error"]
    assert service.models.calls == []


def test_worker_stop_keeps_pid_when_process_survives_force_kill(mock_config, monkeypatch):
    from unittest.mock import Mock

    from taosanode_service import ModelService
    processes = Mock()
    processes.is_running.return_value = True
    processes.read_pid.return_value = 123
    service = ModelService(mock_config, processes)
    monkeypatch.setattr("taosanode_service.time.sleep", lambda duration: None)
    clock = iter([0, 6])
    monkeypatch.setattr("taosanode_service.time.monotonic", lambda: next(clock))
    assert service.stop("moirai") is False
    processes.remove_pid.assert_not_called()


def test_failed_service_start_restores_previous_runtime(upgrade):
    service, _, _ = upgrade
    task = queue(upgrade)
    service.models.fail_next_start = True
    result = run(service, task)
    assert result["status"] == "failed"
    assert result["phase"] == "rolled_back"
    assert result["rollback_status"] == "completed"
    assert "failed to start" in result["error"]
    assert service.inventory()["installed"]["runtime_id"] == "tsfm-runtime-1.0.0"
    assert service.models.running == {"moirai"}


def test_queue_pins_target_digest_and_excludes_other_mutations(upgrade):
    service, release, _ = upgrade
    with pytest.raises(ResourceImportError, match="digest changed"):
        service.queue(str(release), "", "0" * 64)
    task = queue(upgrade)
    with pytest.raises(ResourceBusyError, match=task["task_id"]):
        service.guard()
    with pytest.raises(ResourceBusyError):
        queue(upgrade)


def test_interruption_is_persistent_and_recovery_before_stop_leaves_models_alone(upgrade):
    service, _, _ = upgrade
    task = queue(upgrade)
    service.save(task, phase="preparing", status="running")
    restarted = RuntimeUpgrade(type("Config", (), {
        "tsfm_model_dir": str(service.manager.model_root),
        "model_venvs_dir": str(service.manager.model_venvs_root),
        "tsfm_venv": str(service.manager.runtime_root),
        "resource_dir": str(service.manager.resource_root),
        "data_dir": str(service.manager.resource_root.parent),
        "venv_dir": str(service.manager.resource_root.parent / "venv"),
    })(), service.models)
    result = restarted.status(task["task_id"])
    assert result["phase"] == "manual_intervention"
    with pytest.raises(ResourceBusyError):
        restarted.guard()
    with lifecycle_lock(service.manager.resource_root):
        assert restarted.queue_recovery(task["task_id"])["phase"] == "aborted"
    assert service.models.calls == []
    restarted.guard()


def test_status_does_not_interrupt_a_live_locked_upgrade(upgrade):
    service, _, _ = upgrade
    task = queue(upgrade)
    service.save(task, phase="installing", status="running")
    with lifecycle_lock(service.manager.resource_root):
        assert service.status(task["task_id"])["status"] == "running"


def test_explicit_rollback_retains_backup_and_never_restarts_idle_models(upgrade):
    service, _, _ = upgrade
    task = queue(upgrade)
    run(service, task)
    with lifecycle_lock(service.manager.resource_root):
        task = service.queue_recovery(task["task_id"])
    result = run(service, task)
    assert result["phase"] == "rolled_back"
    assert service.inventory()["installed"]["runtime_id"] == "tsfm-runtime-1.0.0"
    assert service.models.running == {"moirai"}


def test_repeated_upgrade_preserves_previous_recovery_snapshot(upgrade):
    service, release, digest = upgrade
    task = queue(upgrade)
    run(service, task)
    with lifecycle_lock(service.manager.resource_root):
        repeated = service.queue(str(release), "", digest)
    assert repeated["task_id"] == task["task_id"]
    assert repeated["status"] == "completed"
    assert Path(repeated["backup"]).is_dir()


def test_invalid_task_id_cannot_escape_state_directory(upgrade):
    service, _, _ = upgrade
    with pytest.raises(ResourceImportError, match="invalid"):
        service.status("../../etc/passwd")


def test_failed_rollback_keeps_maintenance_gate(upgrade, monkeypatch):
    service, _, _ = upgrade
    task = queue(upgrade)
    monkeypatch.setattr(service.models, "start", lambda name: False)
    result = run(service, task)
    assert result["phase"] == "manual_intervention"
    assert result["rollback_error"]
    with pytest.raises(ResourceBusyError):
        service.guard()


def test_recover_interrupted_replacement_uses_saved_snapshot_and_model_set(upgrade):
    service, _, _ = upgrade
    task = queue(upgrade)
    task = run(service, task)
    service.save(task, status="running", phase="installing")
    assert service.status(task["task_id"])["status"] == "interrupted"
    with lifecycle_lock(service.manager.resource_root):
        service.queue_recovery(task["task_id"])
    result = run(service, task)
    assert result["phase"] == "rolled_back"
    assert service.models.running == {"moirai"}
    assert service.inventory()["installed"]["runtime_id"] == "tsfm-runtime-1.0.0"


def test_model_weights_are_preserved_by_upgrade_and_rollback(upgrade):
    service, _, _ = upgrade
    weights = service.manager.model_root / "moirai" / "weights.bin"
    weights.parent.mkdir(parents=True, exist_ok=True)
    weights.write_bytes(b"persistent weights")
    task = queue(upgrade)
    run(service, task)
    assert weights.read_bytes() == b"persistent weights"
    with lifecycle_lock(service.manager.resource_root):
        service.queue_recovery(task["task_id"])
    run(service, task)
    assert weights.read_bytes() == b"persistent weights"


@pytest.mark.skipif(sys.platform != "linux", reason="native Linux CLI lifecycle validation")
@pytest.mark.parametrize("schema_version", [1, 2])
def test_detached_cli_worker_persists_upgrade_and_rollback(tmp_path, schema_version):
    from resource_manifest import normalized_arch, normalized_platform
    root = tmp_path / "install"
    scripts = root / "bin"
    scripts.mkdir(parents=True)
    source_scripts = Path(__file__).resolve().parents[1] / "script"
    import shutil
    for filename in ("taosanodectl.py", "taosanode_service.py", "resource_manifest.py",
                     "resource_manager.py", "runtime_upgrade.py"):
        shutil.copy2(source_scripts / filename, scripts / filename)
    venv = root / "model_venvs" / "tsfm"
    executable = venv / "bin" / "python3"
    executable.parent.mkdir(parents=True)
    executable.symlink_to(sys.executable)
    lock = b"test-package==1\n"
    (venv / "requirements.lock").write_bytes(lock)
    manifest = {
        "schema_version": 1, "kind": "tsfm-runtime", "runtime_id": "old-runtime",
        "python_version": platform.python_version(), "platform": normalized_platform(),
        "arch": normalized_arch(), "requirements_sha256": hashlib.sha256(lock).hexdigest(),
    }
    (venv / "manifest.json").write_text(json.dumps(manifest))
    (venv / ".tdgpt-install-complete.json").write_text(json.dumps({
        "kind": "tsfm-runtime-install", "runtime_id": "old-runtime", "archive_sha256": "1" * 64,
    }))
    archive_root = tmp_path / "archive-source" / "model_venvs" / "tsfm"
    shutil.copytree(venv, archive_root, symlinks=True)
    manifest["runtime_id"] = "new-runtime"
    (archive_root / "manifest.json").write_text(json.dumps(manifest))
    archive = tmp_path / "new-runtime.tar.xz"
    with tarfile.open(archive, "w:xz") as output:
        output.add(archive_root, arcname="model_venvs/tsfm")
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    release = tmp_path / "release.json"
    release.write_text(json.dumps({"schema_version": 1, "release": "test", "models": [], "runtimes": [{
        "kind": "runtime", "name": "tsfm_runtime", "version": "test", "platform": normalized_platform(),
        "arch": normalized_arch(), "file": archive.name, "sha256": digest, "size": archive.stat().st_size,
    }]}))
    metadata_dir = root / "share" / "tsfm"
    metadata_dir.mkdir(parents=True)
    shutil.copy2(release, metadata_dir / "tsfm-release.json")
    config = root / "config.py"
    config.write_text("\n".join(f"{key} = {str(value)!r}" for key, value in {
        "tsfm_model_dir": root / "models", "model_venvs_dir": venv.parent, "tsfm_venv": venv,
        "resource_dir": root / "resources", "python_runtime_dir": sys.base_prefix,
    }.items()) + "\nmodels = {}\n")
    def invoke(*arguments):
        result = subprocess.run([sys.executable, str(scripts / "taosanodectl.py"), *arguments,
                                 "--config", str(config)], capture_output=True, text=True, timeout=20)
        assert result.returncode == 0, result.stderr
        return json.loads(result.stdout)
    task = invoke("resource", "upgrade", "--release", str(release), "--expected-sha256", digest)
    def poll():
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            status = invoke("resource", "upgrade-status", task["task_id"])
            if status["status"] in {"completed", "failed", "interrupted"}:
                return status
            time.sleep(0.1)
        pytest.fail("detached upgrade worker did not finish")
    assert poll()["status"] == "completed"
    assert invoke("resource", "maintenance-status")["task"]["task_id"] == task["task_id"]
    assert invoke("resource", "runtime-status")["installed"]["runtime_id"] == "new-runtime"
    invoke("resource", "upgrade-recover", task["task_id"])
    assert poll()["phase"] == "rolled_back"
    assert invoke("resource", "runtime-status")["installed"]["runtime_id"] == "old-runtime"

    from functools import partial
    from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
    from threading import Thread
    from unittest.mock import patch

    from flask import Flask, request

    from lib.conf import Configure
    from lib.tsfm.mgmt import foundation_model, foundation_util
    from lib.tsfm.mgmt.foundation_models_db import init_dao

    published = tmp_path / "tdengine-tdgpt-enterprise" / "tsfm" / "test"
    published.mkdir(parents=True)
    shutil.copy2(archive, published / archive.name)
    shutil.copy2(release, published / "tdengine-tdgpt-tsfm-release-test.json")
    published_manifest = published / "tdengine-tdgpt-tsfm-release-test.json"
    published_value = json.loads(published_manifest.read_text())
    published_value["schema_version"] = schema_version
    published_manifest.write_text(json.dumps(published_value))
    latest = published.parent / "latest"
    latest.mkdir()
    (latest / "tdengine-tdgpt-tsfm-release-latest.json").write_text(json.dumps({
        "schema_version": 1, "release": "test",
    }))
    server = ThreadingHTTPServer(("127.0.0.1", 0), partial(SimpleHTTPRequestHandler, directory=str(tmp_path)))
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    init_dao(str(root / "http-test.db"))
    app = Flask(__name__)
    app.add_url_rule("/runtime", endpoint="runtime", view_func=lambda: foundation_model.get_foundation_runtime(request))
    app.add_url_rule("/runtime/upgrade", endpoint="runtime_upgrade",
                     view_func=lambda: foundation_model.upgrade_foundation_runtime(request), methods=["POST"])
    app.add_url_rule("/runtime/upgrades/<task_id>", endpoint="runtime_upgrade_status",
                     view_func=lambda task_id: foundation_model.get_runtime_upgrade(request, task_id))
    app.add_url_rule("/runtime/upgrades/<task_id>/recover", endpoint="runtime_upgrade_recover",
                     view_func=lambda task_id: foundation_model.recover_runtime_upgrade(request, task_id), methods=["POST"])
    try:
        cli = [sys.executable, str(scripts / "taosanodectl.py")]
        with patch.object(foundation_util, "get_taosanodectl_cmd", return_value=cli), patch.object(
            foundation_util, "resolve_resource_settings", return_value=(f"http://127.0.0.1:{server.server_port}", 20),
        ), patch.object(Configure, "get_instance", return_value=type("Config", (), {"path": str(config)})()):
            client = app.test_client()
            inventory = client.get("/runtime").get_json()["data"]
            assert inventory["target"]["sha256"] == digest
            assert inventory["target"]["release"] == "test"
            latest_inventory = client.get("/runtime?release=latest").get_json()["data"]
            assert latest_inventory["target_error"] is None
            assert latest_inventory["target"]["release"] == "test"
            assert latest_inventory["target"]["sha256"] == digest
            response = client.post("/runtime/upgrade", json={
                "release": inventory["target"]["release"], "expected_sha256": inventory["target"]["sha256"],
            })
            assert response.status_code == 202, response.get_json()
            task_id = response.get_json()["task_id"]
            def http_poll():
                deadline = time.monotonic() + 20
                while time.monotonic() < deadline:
                    status = client.get(f"/runtime/upgrades/{task_id}").get_json()["data"]
                    if status["status"] in {"completed", "failed", "interrupted"}:
                        return status
                    time.sleep(0.1)
                pytest.fail("HTTP upgrade did not finish")
            assert http_poll()["status"] == "completed"
            assert client.get("/runtime").get_json()["data"]["installed"]["runtime_id"] == "new-runtime"
            assert client.post(f"/runtime/upgrades/{task_id}/recover").status_code == 202
            assert http_poll()["phase"] == "rolled_back"
            assert client.get("/runtime").get_json()["data"]["installed"]["runtime_id"] == "old-runtime"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
