#!/usr/bin/env python3
"""Validation helpers for dynamically installed TDgpt TSFM resources."""

from __future__ import annotations

import hashlib
import json
import platform
import re
import subprocess
from pathlib import Path
from typing import Any, Dict, Tuple

SCHEMA_VERSION = 1
COMPLETION_FILE = ".tdgpt-install-complete.json"
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


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
    with path.open("r", encoding="utf-8") as file_obj:
        value = json.load(file_obj)
    if not isinstance(value, dict):
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
    completion = runtime_dir / COMPLETION_FILE
    if require_completion and (
        not completion.is_file()
        or load_manifest(completion).get("kind") != "tsfm-runtime-install"
    ):
        raise ValueError("runtime import is incomplete")
    manifest = load_manifest(runtime_dir / "manifest.json")
    if manifest.get("schema_version") != SCHEMA_VERSION or manifest.get("kind") != "tsfm-runtime":
        raise ValueError("unsupported TSFM runtime manifest")
    platform_matches = _required_string(manifest, "platform").lower() == normalized_platform()
    arch_matches = _required_string(manifest, "arch").lower() == normalized_arch()
    if not platform_matches or not arch_matches:
        raise ValueError("runtime platform or architecture does not match this host")
    lock_file = runtime_dir / "requirements.lock"
    expected_hash = _required_string(manifest, "requirements_sha256").lower()
    if not SHA256_PATTERN.fullmatch(expected_hash) or not lock_file.is_file() or sha256_file(lock_file) != expected_hash:
        raise ValueError("runtime requirements.lock does not match its manifest")
    python_exe = _venv_python(runtime_dir)
    result = subprocess.run(
        [str(python_exe), "-c", "import platform; print(platform.python_version())"],
        capture_output=True,
        text=True,
        check=False,
        timeout=15,
    )
    if result.returncode or result.stdout.strip() != _required_string(manifest, "python_version"):
        raise ValueError("runtime Python version does not match its manifest")
    return manifest, python_exe


def validate_tsfm_model(model_dir: Path, model_name: str, _: Dict[str, Any]) -> Dict[str, Any]:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", model_name):
        raise ValueError("invalid model name")
    if not model_dir.is_dir() or not any(path.is_file() for path in model_dir.rglob("*")):
        raise ValueError(f"model resource is empty: {model_dir}")
    return {"model_name": model_name, "path": str(model_dir)}
