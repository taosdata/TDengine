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

"""Build the TDgpt Lite main virtual-environment package for Windows x64."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_obj:
        for chunk in iter(lambda: file_obj.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def run(command: list[str]) -> None:
    result = subprocess.run(command, check=False)
    if result.returncode:
        raise RuntimeError(
            f"command failed ({result.returncode}): {' '.join(command)}"
        )


def runtime_python(runtime_dir: Path) -> Path:
    candidates = [runtime_dir / "python.exe", runtime_dir / "bin" / "python.exe"]
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(f"Python runtime not found under {runtime_dir}")


def python_version(python: Path) -> str:
    result = subprocess.run(
        [str(python), "-c", "import platform; print(platform.python_version())"],
        capture_output=True,
        check=False,
        text=True,
    )
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or "unable to read Python version")
    return result.stdout.strip()


def ensure_windows_x64() -> None:
    if os.name != "nt":
        raise RuntimeError("Windows Lite venv packages must be built on Windows")
    machine = platform.machine().lower()
    if machine not in {"amd64", "x86_64"}:
        raise RuntimeError(f"Windows Lite venv requires x64, got {machine}")


def prepare_work_dir(work_dir: Path) -> None:
    if work_dir.exists() and any(work_dir.iterdir()):
        raise RuntimeError(f"work directory must be empty: {work_dir}")
    work_dir.mkdir(parents=True, exist_ok=True)


def package_venv(venv_dir: Path, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        prefix=f".{output_path.name}.",
        suffix=".tmp",
        dir=output_path.parent,
        delete=False,
    ) as temp_file:
        temp_path = Path(temp_file.name)
    try:
        with zipfile.ZipFile(
            temp_path, "w", zipfile.ZIP_DEFLATED, allowZip64=True
        ) as archive:
            for path in sorted(venv_dir.rglob("*")):
                relative = path.relative_to(venv_dir).as_posix()
                archive.write(path, f"venvs/venv/{relative}")
        os.replace(temp_path, output_path)
    finally:
        temp_path.unlink(missing_ok=True)


def prune_runtime(venv_dir: Path) -> None:
    """Remove non-runtime files while retaining pip for offline repair."""
    for path in list(venv_dir.rglob("__pycache__")):
        shutil.rmtree(path, ignore_errors=True)
    # Test directories are pruned only with package/runtime regression coverage.
    for name in ("test", "tests"):
        for path in list(venv_dir.rglob(name)):
            if (path.is_dir() and not path.is_symlink()
                    and not any(part.casefold() == "include"
                                for part in path.relative_to(venv_dir).parts)):
                shutil.rmtree(path)
    # Keep .a libraries (e.g. NumPy libnpymath.a) for user C/Cython linking.
    # Import checks cannot prove they are unused; removal requires a
    # package-specific audit and a compilation/linking regression test.
    # Keep .pyi stubs for type checking and user model development.
    # Drop documentation but retain legal notices at their original paths.
    docs = venv_dir / "share" / "doc"
    legal_names = ("license", "licence", "copyright", "copying", "notice")
    if docs.is_dir():
        for path in docs.rglob("*"):
            if (path.is_file() or path.is_symlink()) and not any(
                name in str(path.relative_to(docs)).lower() for name in legal_names
            ):
                path.unlink()
    shutil.rmtree(venv_dir / "share" / "man", ignore_errors=True)


def pip_options(args: argparse.Namespace) -> list[str]:
    options: list[str] = []
    if args.index_url:
        options.extend(["--index-url", args.index_url])
    if args.extra_index_url:
        options.extend(["--extra-index-url", args.extra_index_url])
    if args.trusted_host:
        options.extend(["--trusted-host", args.trusted_host])
    return options


def parse_args() -> argparse.Namespace:
    tdgpt_root = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-dir", required=True, type=Path)
    parser.add_argument("--work-dir", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--version", default="1.0.2")
    parser.add_argument(
        "--requirements",
        type=Path,
        default=tdgpt_root / "requirements_docker.txt",
    )
    parser.add_argument("--index-url", default="")
    parser.add_argument("--extra-index-url", default="")
    parser.add_argument("--trusted-host", default="")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    ensure_windows_x64()

    runtime_dir = args.runtime_dir.resolve()
    requirements = args.requirements.resolve()
    work_dir = args.work_dir.resolve()
    output_dir = args.output_dir.resolve()
    if not requirements.is_file():
        raise FileNotFoundError(f"requirements file not found: {requirements}")
    prepare_work_dir(work_dir)

    base_python = runtime_python(runtime_dir)
    actual_version = python_version(base_python)
    if not actual_version.startswith("3.12."):
        raise RuntimeError(
            f"TDgpt Lite main venv requires Python 3.12, got {actual_version}"
        )

    venv_dir = work_dir / "venvs" / "venv"
    run([str(base_python), "-m", "venv", "--copies", str(venv_dir)])
    python = venv_dir / "Scripts" / "python.exe"
    options = pip_options(args)
    run(
        [
            str(python),
            "-m",
            "pip",
            "install",
            "--upgrade",
            "pip",
            "setuptools",
            "wheel",
            *options,
        ]
    )
    run(
        [
            str(python),
            "-m",
            "pip",
            "install",
            "-r",
            str(requirements),
            *options,
        ]
    )
    prune_runtime(venv_dir)
    run([str(python), "-B", "-m", "pip", "check"])
    run(
        [
            str(python),
            "-B",
            "-c",
            "import flask, pyarrow, requests, statsforecast, waitress; "
            "import numpy, pandas, scipy, statsmodels, sklearn, Cython, setuptools, wheel",
        ]
    )

    freeze = subprocess.run(
        [str(python), "-m", "pip", "freeze", "--all"],
        capture_output=True,
        check=False,
        text=True,
    )
    if freeze.returncode:
        raise RuntimeError(freeze.stderr.strip() or "pip freeze failed")
    lock_file = venv_dir / "requirements.lock"
    lock_file.write_text(freeze.stdout, encoding="utf-8")
    manifest = {
        "schema_version": 1,
        "kind": "tdgpt-lite-main-venv",
        "version": args.version,
        "python_version": actual_version,
        "platform": "windows",
        "arch": "x64",
        "requirements_sha256": sha256_file(requirements),
        "lock_sha256": sha256_file(lock_file),
    }
    (venv_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )

    output_path = output_dir / (
        f"tdengine-tdgpt-lite-venv-{args.version}-windows-x64.zip"
    )
    # Validation and pip metadata commands can regenerate bytecode.
    prune_runtime(venv_dir)
    package_venv(venv_dir, output_path)
    print(f"built {output_path}")
    print(f"sha256 {sha256_file(output_path)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
