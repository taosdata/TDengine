#!/usr/bin/env python3

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

"""Validation helpers for dynamically installed TDgpt TSFM resources."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import platform
import re
import subprocess
from pathlib import Path
from typing import Any, Dict, Tuple

# Module-level logger only. This module is imported by resource_manager.py and
# taosanode_service.py, so where these records go (CLI stderr vs service log)
# stays a decision of whichever entry point is running.
logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1
COMPLETION_FILE = ".tdgpt-install-complete.json"
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
# Name of the shared TSFM runtime (the venv every official TSFM model runs on).
# It is the same string in the release manifest, in `resource apply/delete`, in
# the models API and on the SSE stream -- one resource, one name.
SHARED_RUNTIME_NAME = "tsfm_runtime"
MODEL_REQUIRED_FILES = {
    "tdtsfm": ("taos.pth",),
    "timemoe": ("model.safetensors", "config.json"),
    "moirai": ("model.safetensors", "config.json"),
    "chronos": ("model.safetensors", "config.json"),
    "timesfm": ("model.safetensors", "config.json"),
    "moment": ("model.safetensors", "config.json"),
}

# Default timeout for probing a venv's Python version. Under a debugger
# (debugpy/pydevd) interpreter startup is far slower, so allow overriding this
# per-invocation via the TDGPT_PYTHON_PROBE_TIMEOUT env var (inherited by all
# spawned subprocesses). A value <= 0 falls back to the default below.
DEFAULT_PYTHON_PROBE_TIMEOUT = 60


def python_probe_timeout() -> int:
    """Return the timeout (seconds) for probing a venv Python version."""
    raw = os.environ.get("TDGPT_PYTHON_PROBE_TIMEOUT", "")
    try:
        timeout = int(raw)
    except (TypeError, ValueError):
        # Only report a value that was actually set: the variable is normally
        # absent, and that is not worth a line on every probe.
        if raw.strip():
            logger.warning(
                "Ignoring invalid TDGPT_PYTHON_PROBE_TIMEOUT=%r; using %ss",
                raw,
                DEFAULT_PYTHON_PROBE_TIMEOUT,
            )
        timeout = DEFAULT_PYTHON_PROBE_TIMEOUT
    if timeout <= 0:
        logger.warning(
            "Ignoring non-positive TDGPT_PYTHON_PROBE_TIMEOUT=%r; using %ss",
            raw,
            DEFAULT_PYTHON_PROBE_TIMEOUT,
        )
        timeout = DEFAULT_PYTHON_PROBE_TIMEOUT
    return timeout


def normalized_platform() -> str:
    return "windows" if platform.system().lower() == "windows" else platform.system().lower()


def normalized_arch() -> str:
    machine = platform.machine().lower()
    if machine in {"x86_64", "amd64"}:
        return "x64"
    if machine in {"aarch64", "arm64"}:
        return "arm64"
    return machine


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_obj:
        for chunk in iter(lambda: file_obj.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_manifest(path: Path) -> Dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as file_obj:
            value = json.load(file_obj)
    except json.JSONDecodeError as exc:
        # The decoder error carries no filename, and callers surface it as a bare
        # "invalid resource", so name the offending file while it is still known.
        logger.warning("Corrupt JSON in manifest %s: %s", path, exc)
        raise
    except OSError as exc:
        # A missing manifest usually just means "resource not installed yet".
        logger.debug("Unable to read manifest %s: %s", path, exc)
        raise
    if not isinstance(value, dict):
        logger.warning(
            "Manifest %s must contain a JSON object, got %s", path, type(value).__name__
        )
        raise ValueError("manifest root must be a JSON object")
    return value


def _required_string(manifest: Dict[str, Any], field: str) -> str:
    value = manifest.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"manifest field {field!r} must be a non-empty string")
    return value.strip()


def _venv_python(venv_dir: Path) -> Path:
    return venv_dir / "Scripts" / "python.exe" if normalized_platform() == "windows" else venv_dir / "bin" / "python3"


def validate_tsfm_runtime(
    runtime_dir: Path, require_completion: bool = True
) -> Tuple[Dict[str, Any], Path]:
    logger.debug(
        "Validating TSFM runtime: dir=%s require_completion=%s", runtime_dir, require_completion
    )
    completion = runtime_dir / COMPLETION_FILE
    if require_completion and (
        not completion.is_file()
        or load_manifest(completion).get("kind") != "tsfm-runtime-install"
    ):
        if completion.is_file():
            logger.warning(
                "TSFM runtime completion marker %s has an unexpected kind (expected %r)",
                completion,
                "tsfm-runtime-install",
            )
        else:
            # Expected before the first successful import.
            logger.debug("TSFM runtime is not marked complete yet: %s", completion)
        raise ValueError("runtime import is incomplete")
    manifest = load_manifest(runtime_dir / "manifest.json")
    if manifest.get("schema_version") != SCHEMA_VERSION or manifest.get("kind") != "tsfm-runtime":
        logger.warning(
            "Unsupported TSFM runtime manifest %s: schema_version=%r (expected %s), "
            "kind=%r (expected 'tsfm-runtime')",
            runtime_dir / "manifest.json",
            manifest.get("schema_version"),
            SCHEMA_VERSION,
            manifest.get("kind"),
        )
        raise ValueError("unsupported TSFM runtime manifest")

    platform_matches = _required_string(manifest, "platform").lower() == normalized_platform()
    arch_matches = _required_string(manifest, "arch").lower() == normalized_arch()
    if not platform_matches or not arch_matches:
        logger.warning(
            "TSFM runtime platform mismatch in %s: manifest=%s/%s host=%s/%s",
            runtime_dir,
            manifest.get("platform"),
            manifest.get("arch"),
            normalized_platform(),
            normalized_arch(),
        )
        raise ValueError("runtime platform or architecture does not match this host")

    lock_file = runtime_dir / "requirements.lock"
    expected_hash = _required_string(manifest, "requirements_sha256").lower()
    actual_hash = sha256_file(lock_file) if lock_file.is_file() else "<missing>"

    if not SHA256_PATTERN.fullmatch(expected_hash) or not lock_file.is_file() or actual_hash != expected_hash:
        # Report expected vs actual: the bare exception cannot tell whether the
        # lock file is missing, truncated or simply from another release.
        logger.warning(
            "TSFM runtime requirements.lock mismatch in %s: expected=%s actual=%s",
            runtime_dir,
            expected_hash,
            actual_hash,
        )
        raise ValueError("runtime requirements.lock does not match its manifest")

    python_exe = _venv_python(runtime_dir)
    # The interpreter is a symlink into the host Python runtime on Linux, so it
    # can be missing or dangling even though the rest of the venv looks fine
    # (host runtime moved/removed, venv copied without relocating). Check it
    # explicitly instead of relying on the launch below to fail.
    if not python_exe.is_file():
        logger.warning("TSFM runtime Python interpreter is missing: %s", python_exe)
        raise ValueError(f"runtime Python interpreter is missing: {python_exe}")

    try:
        result = subprocess.run(
            [str(python_exe), "-c", "import platform; print(platform.python_version())"],
            capture_output=True,
            text=True,
            check=False,
            timeout=python_probe_timeout(),
        )
    except subprocess.TimeoutExpired as exc:
        logger.warning(
            "TSFM runtime Python interpreter probe timed out after %ss: %s",
            python_probe_timeout(),
            python_exe,
        )
        raise ValueError(
            f"runtime Python interpreter probe timed out: {python_exe}"
        ) from exc
    except OSError as exc:
        # Present on disk but not launchable (missing execute permission, broken
        # link target, missing shared libraries, ...). Surface it as a validation
        # failure with context rather than letting a bare Errno escape.
        logger.warning(
            "TSFM runtime Python interpreter is not runnable: %s: %s", python_exe, exc
        )
        raise ValueError(
            f"runtime Python interpreter is not runnable: {python_exe}: {exc}"
        ) from exc

    # Record what the probe actually returned: while the comparison below stays
    # disabled this is the only evidence of interpreter/manifest version drift.
    logger.debug(
        "TSFM runtime Python probe: %s exit=%s reported=%r manifest=%r",
        python_exe,
        result.returncode,
        (result.stdout or "").strip(),
        manifest.get("python_version"),
    )

    if result.returncode or result.stdout.strip() != _required_string(manifest, "python_version"):
        logger.error(
            "TSFM runtime Python version mismatch in %s: manifest=%r reported=%r",
            runtime_dir,
            manifest.get("python_version"),
            (result.stdout or "").strip(),
        )
        raise ValueError("runtime Python version does not match its manifest")

    logger.debug(
        "TSFM runtime validated: %s (runtime_id=%s)", runtime_dir, manifest.get("runtime_id")
    )
    return manifest, python_exe


def validate_tsfm_model(model_dir: Path, model_name: str, _: Dict[str, Any]) -> Dict[str, Any]:
    logger.debug("Validating TSFM model %s: dir=%s", model_name, model_dir)
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", model_name):
        logger.warning("Rejecting TSFM model resource with invalid name: %r", model_name)
        raise ValueError("invalid model name")
    if not model_dir.is_dir():
        # Optional models are simply absent until they are imported.
        logger.debug("TSFM model %s is not installed: %s", model_name, model_dir)
        raise ValueError(f"model resource is missing: {model_dir}")
    required_files = MODEL_REQUIRED_FILES.get(model_name)
    if required_files is None:
        # Unknown model: not yet registered in MODEL_REQUIRED_FILES.
        # Warn so gaps are visible; fall back to a non-empty-directory check so
        # future models aren't hard-rejected by this validator.
        logger.warning(
            "TSFM model %s is not registered in MODEL_REQUIRED_FILES; "
            "falling back to directory check — add it to enable full validation",
            model_name,
        )
        if not any(path.is_file() for path in model_dir.rglob("*")):
            logger.warning(
                "TSFM model resource directory is empty: %s (model=%s)", model_dir, model_name
            )
            raise ValueError(f"model resource is incomplete: {model_dir}; directory is empty")
        logger.debug(
            "TSFM model %s passed the unknown-model directory check: %s", model_name, model_dir
        )
        return {"model_name": model_name, "path": str(model_dir)}
    missing = [
        name for name in required_files
        if not ((model_dir / name).is_file() and (model_dir / name).stat().st_size > 0)
    ]
    if missing:
        logger.warning(
            "TSFM model %s is incomplete in %s: missing or empty %s",
            model_name,
            model_dir,
            ", ".join(missing),
        )
        raise ValueError(
            f"model resource is incomplete: {model_dir}; missing {', '.join(missing)}"
        )
    logger.debug("TSFM model %s validated: %s", model_name, model_dir)
    return {"model_name": model_name, "path": str(model_dir)}
