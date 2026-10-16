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
from resource_manifest import COMPLETION_FILE, validate_tsfm_model, validate_tsfm_runtime


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
