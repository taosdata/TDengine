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
import subprocess
from pathlib import Path

import pytest
from resource_manifest import COMPLETION_FILE, release_resource_file, validate_tsfm_model, validate_tsfm_runtime


@pytest.mark.parametrize("schema_version,item,expected", [
    (1, {"file": "model.zip"}, "model.zip"),
    (1, {"file": "sub/model.zip"}, "sub/model.zip"),
    (2, {"file": "model.zip", "path": "models/model.zip"}, "../models/model.zip"),
    (2, {"kind": "runtime", "file": "runtime.tar.xz"}, "runtime.tar.xz"),
])
def test_release_resource_path_contract(schema_version, item, expected):
    assert release_resource_file(item, schema_version) == expected


@pytest.mark.parametrize("schema_version,item,error", [
    (1, {"file": "model.zip", "path": "models/model.zip"}, "require release schema_version 2"),
    (2, {"kind": "runtime", "file": "runtime.zip", "path": "models/runtime.zip"}, "only model resources"),
    (2, {"file": "runtime.zip"}, "model path"),
    (True, {"file": "model.zip"}, "unsupported"),
    ("2", {"file": "model.zip"}, "unsupported"),
    (3, {"file": "model.zip"}, "unsupported"),
])
def test_release_resource_path_rejects_incompatible_metadata(schema_version, item, error):
    with pytest.raises(ValueError, match=error):
        release_resource_file(item, schema_version)


def test_runtime_manifest_requires_completed_import(tmp_path, mocker):
    runtime_dir = Path(tmp_path) / "tsfm"
    python_exe = runtime_dir / "bin" / "python3"
    python_exe.parent.mkdir(parents=True)
    python_exe.touch()
    lock_contents = b"torch==2.4.1\n"
    (runtime_dir / "requirements.lock").write_bytes(lock_contents)
    manifest = {
        "schema_version": 1,
        "kind": "tsfm-runtime",
        "runtime_id": "tsfm-runtime-1.0.0",
        "python_version": "3.12.12",
        "platform": "linux",
        "arch": "x64",
        "requirements_sha256": hashlib.sha256(lock_contents).hexdigest(),
    }
    (runtime_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    mocker.patch("resource_manifest.normalized_platform", return_value="linux")
    mocker.patch("resource_manifest.normalized_arch", return_value="x64")
    run = mocker.patch("resource_manifest.subprocess.run")
    run.return_value.returncode = 0
    run.return_value.stdout = "3.12.12\n"

    with pytest.raises(ValueError, match="incomplete"):
        validate_tsfm_runtime(runtime_dir)

    (runtime_dir / COMPLETION_FILE).write_text(
        json.dumps({"kind": "tsfm-runtime-install"}), encoding="utf-8"
    )
    actual_manifest, actual_python = validate_tsfm_runtime(runtime_dir)
    assert actual_manifest == manifest
    assert actual_python == python_exe


def test_runtime_probe_timeout_is_reported_as_validation_failure(tmp_path, mocker):
    runtime_dir = Path(tmp_path) / "tsfm"
    python_exe = runtime_dir / "bin" / "python3"
    python_exe.parent.mkdir(parents=True)
    python_exe.touch()
    lock_contents = b"torch==2.4.1\n"
    (runtime_dir / "requirements.lock").write_bytes(lock_contents)
    manifest = {
        "schema_version": 1,
        "kind": "tsfm-runtime",
        "runtime_id": "tsfm-runtime-1.0.0",
        "python_version": "3.12.12",
        "platform": "linux",
        "arch": "x64",
        "requirements_sha256": hashlib.sha256(lock_contents).hexdigest(),
    }
    (runtime_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (runtime_dir / COMPLETION_FILE).write_text(
        json.dumps({"kind": "tsfm-runtime-install"}), encoding="utf-8"
    )
    mocker.patch("resource_manifest.normalized_platform", return_value="linux")
    mocker.patch("resource_manifest.normalized_arch", return_value="x64")
    mocker.patch(
        "resource_manifest.subprocess.run",
        side_effect=subprocess.TimeoutExpired([str(python_exe)], 60),
    )

    with pytest.raises(ValueError, match="probe timed out"):
        validate_tsfm_runtime(runtime_dir)


def test_model_validation_accepts_original_model_files(tmp_path):
    model_dir = Path(tmp_path) / "moirai"
    model_dir.mkdir()
    (model_dir / "config.json").write_text("{}", encoding="utf-8")
    (model_dir / "model.safetensors").write_bytes(b"weights")
    assert validate_tsfm_model(model_dir, "moirai", {})["model_name"] == "moirai"


def test_model_validation_rejects_invalid_or_empty_directory(tmp_path):
    with pytest.raises(ValueError, match="invalid model"):
        validate_tsfm_model(Path(tmp_path), "../moirai", {})
    with pytest.raises(ValueError, match="missing"):
        validate_tsfm_model(Path(tmp_path) / "empty", "moirai", {})


@pytest.mark.parametrize("actual,accepted", [
    ("3.12.12", True), ("3.12.16", True), ("3.13.0", False),
    ("3.11.16", False), ("3.12.11", False), ("3.12.16rc1", False),
    ("", False), ("garbage", False),
])
def test_runtime_python_patch_compatibility(tmp_path, mocker, actual, accepted):
    root = tmp_path / "tsfm"
    python = root / "bin" / "python3"
    python.parent.mkdir(parents=True)
    python.touch()
    lock = b"torch==2.4.1\n"
    (root / "requirements.lock").write_bytes(lock)
    manifest = {"schema_version": 1, "kind": "tsfm-runtime", "runtime_id": "tsfm-runtime-1.0.0",
                "python_version": "3.12.12", "platform": "linux", "arch": "x64",
                "requirements_sha256": hashlib.sha256(lock).hexdigest()}
    (root / "manifest.json").write_text(json.dumps(manifest))
    (root / COMPLETION_FILE).write_text(json.dumps({"kind": "tsfm-runtime-install"}))
    mocker.patch("resource_manifest.normalized_platform", return_value="linux")
    mocker.patch("resource_manifest.normalized_arch", return_value="x64")
    probe = mocker.patch("resource_manifest.subprocess.run")
    probe.return_value = subprocess.CompletedProcess([], 0, actual + "\n", "")
    if accepted:
        assert validate_tsfm_runtime(root)[0] == manifest
    else:
        with pytest.raises(ValueError, match="explicitly run taosanodectl resource import"):
            validate_tsfm_runtime(root)


def test_runtime_failed_python_probe_is_rejected(tmp_path, mocker):
    # A matching version printed by a failing process must not bypass validation.
    test_runtime_python_patch_compatibility(tmp_path, mocker, "3.12.12", True)
    mocker.patch("resource_manifest.subprocess.run", return_value=subprocess.CompletedProcess([], 1, "3.12.12\n", ""))
    with pytest.raises(ValueError, match="incompatible"):
        validate_tsfm_runtime(tmp_path / "tsfm")
