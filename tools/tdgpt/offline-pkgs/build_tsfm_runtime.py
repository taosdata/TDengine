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

"""Build a platform-specific shared TSFM virtual-environment resource package."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import tarfile
import tempfile
import zipfile
from pathlib import Path

PYARROW_WHEELS = {
    ("linux", "x64"): "https://files.pythonhosted.org/packages/6e/f6/19360dae44200e35753c5c2889dc478154cd78e61b1f738514c9f131734d/pyarrow-18.1.0-cp312-cp312-manylinux_2_17_x86_64.manylinux2014_x86_64.whl",
    ("linux", "arm64"): "https://files.pythonhosted.org/packages/68/f9/29fb659b390312a7345aeb858a9d9c157552a8852522f2c8bad437c29c0a/pyarrow-18.1.0-cp312-cp312-manylinux_2_17_aarch64.manylinux2014_aarch64.whl",
    ("windows", "x64"): "https://files.pythonhosted.org/packages/76/52/f8da04195000099d394012b8d42c503d7041b79f778d854f410e5f05049a/pyarrow-18.1.0-cp312-cp312-win_amd64.whl",
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_obj:
        for chunk in iter(lambda: file_obj.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def run(command: list[str]) -> None:
    result = subprocess.run(command, check=False)
    if result.returncode:
        raise RuntimeError(f"command failed ({result.returncode}): {' '.join(command)}")


def resolve_uv(configured: str) -> str:
    uv = shutil.which(configured)
    if not uv:
        raise RuntimeError(f"uv executable not found: {configured}")
    version = subprocess.run([uv, "--version"], capture_output=True, check=False, text=True)
    if version.returncode:
        raise RuntimeError(
            f"unable to query uv version ({uv}, exit {version.returncode}): "
            f"{version.stderr.strip() or version.stdout.strip()}"
        )
    match = re.fullmatch(r"uv (\d+)\.(\d+)\.(\d+)(?: .*)?", version.stdout.strip())
    if not match:
        raise RuntimeError(f"unable to parse uv version ({uv}): {version.stdout.strip()!r}")
    if tuple(map(int, match.groups())) < (0, 13, 9):
        raise RuntimeError(f"uv >= 0.13.9 is required; found {version.stdout.strip()}")
    return uv


def runtime_python(runtime_dir: Path, target_platform: str) -> Path:
    filename = "python.exe" if target_platform == "windows" else "python3"
    candidates = [runtime_dir / filename, runtime_dir / "bin" / filename]
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(f"Python runtime not found under {runtime_dir}")


def venv_python(venv_dir: Path, target_platform: str) -> Path:
    if target_platform == "windows":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python3"


def validate_python_version(value: str) -> None:
    if re.fullmatch(r"3\.12\.\d+", value) is None:
        raise RuntimeError(
            f"TSFM runtime requires an exact Python 3.12.x version, got {value!r}"
        )


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


def patch_arm64_jaxlib(venv_dir: Path, target_platform: str, target_arch: str) -> None:
    """Fix the ARM64 jaxlib clock_gettime symbol binding to libc."""
    if target_platform != "linux" or target_arch != "arm64":
        return
    patchelf = shutil.which("patchelf")
    if not patchelf:
        raise RuntimeError("patchelf 0.14.0 or newer is required for ARM64 jaxlib")
    version_result = subprocess.run(
        [patchelf, "--version"], capture_output=True, check=False, text=True
    )
    version = version_result.stdout.strip().split()[-1] if version_result.stdout.strip() else ""
    try:
        version_ok = tuple(int(part) for part in version.split(".")[:2]) >= (0, 14)
    except ValueError:
        version_ok = False
    if not version_ok:
        raise RuntimeError(f"patchelf 0.14.0 or newer is required; found {version or 'unknown'}")
    candidates = list(venv_dir.glob("lib/python*/site-packages/jaxlib/libjax_common.so"))
    if not candidates:
        raise RuntimeError(f"jaxlib/libjax_common.so not found under {venv_dir}")
    libjax_common = candidates[0]
    needed = subprocess.run(
        [patchelf, "--print-needed", str(libjax_common)],
        capture_output=True,
        check=True,
        text=True,
    ).stdout.splitlines()
    if "librt.so.1" in needed:
        run([patchelf, "--replace-needed", "librt.so.1", "libc.so.6", str(libjax_common)])
    if "librt.so.1" in subprocess.run(
        [patchelf, "--print-needed", str(libjax_common)],
        capture_output=True,
        check=True,
        text=True,
    ).stdout.splitlines():
        raise RuntimeError(f"failed to patch jaxlib dependency: {libjax_common}")


def validate_cpu_torch(python: Path, venv_dir: Path, target_arch: str) -> None:
    if target_arch != "arm64":
        return
    result = subprocess.run(
        [str(python), "-B", "-c", "import torch; print(torch.version.cuda or '')"],
        capture_output=True,
        check=False,
        text=True,
    )
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or "unable to inspect ARM64 torch build")
    if result.stdout.strip():
        raise RuntimeError(f"ARM64 TSFM requires CPU torch, found CUDA {result.stdout.strip()}")
    forbidden = ("libtorch_cuda", "libcudnn", "libcublas", "libcusparse", "libcufft")
    library_dir = venv_dir / "lib"
    if not library_dir.is_dir():
        raise RuntimeError(f"ARM64 TSFM library directory not found: {library_dir}")
    for path in library_dir.rglob("*"):
        if any(token in path.name for token in forbidden) and path.is_file():
            raise RuntimeError(f"ARM64 TSFM contains CUDA torch library: {path}")


def package_runtime(source_dir: Path, output_path: Path, target_platform: str) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        prefix=f".{output_path.name}.", suffix=".tmp", dir=output_path.parent, delete=False
    ) as temp_file:
        temp_path = Path(temp_file.name)
    try:
        archive_root = "model_venvs/tsfm"
        if target_platform == "windows":
            with zipfile.ZipFile(temp_path, "w", zipfile.ZIP_DEFLATED, allowZip64=True) as archive:
                for path in sorted(source_dir.rglob("*")):
                    relative = path.relative_to(source_dir).as_posix()
                    archive.write(path, f"{archive_root}/{relative}")
        else:
            with tarfile.open(temp_path, "w:xz", preset=6) as archive:
                archive.add(source_dir, arcname=archive_root, recursive=True)
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
                    and "include" not in path.relative_to(venv_dir).parts):
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


def parse_args() -> argparse.Namespace:
    source_root = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-dir", required=True, type=Path)
    parser.add_argument("--platform", required=True, choices=["linux", "windows"])
    parser.add_argument("--arch", required=True, choices=["x64", "arm64"])
    parser.add_argument("--version", required=True)
    parser.add_argument(
        "--python-version",
        default="3.12.16",
        help="Exact CPython version (3.12.x) required by the runtime.",
    )
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--work-dir", required=True, type=Path)
    parser.add_argument(
        "--requirements", type=Path, default=source_root / "requirements_tsfm.txt"
    )
    parser.add_argument("--moment-package", default="momentfm==0.1.4")
    parser.add_argument("--pyarrow-wheel", default="")
    parser.add_argument("--uv", default="uv", help="uv executable used for venv and dependency installation")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    validate_python_version(args.python_version)
    runtime_dir = args.runtime_dir.resolve()
    requirements = args.requirements.resolve()
    work_dir = args.work_dir.resolve()
    if not requirements.is_file():
        raise FileNotFoundError(f"requirements file not found: {requirements}")
    if work_dir.exists() and any(work_dir.iterdir()):
        raise RuntimeError(f"work directory must be empty: {work_dir}")
    work_dir.mkdir(parents=True, exist_ok=True)

    base_python = runtime_python(runtime_dir, args.platform)
    uv = resolve_uv(args.uv)
    actual_version = python_version(base_python)
    if actual_version != args.python_version:
        raise RuntimeError(
            "TSFM runtime Python version mismatch: "
            f"expected {args.python_version}, got {actual_version}; "
            "pass --python-version matching the runtime"
        )

    venv_dir = work_dir / "model_venvs" / "tsfm"
    run([uv, "venv", "--python", str(base_python), str(venv_dir)])
    python = venv_python(venv_dir, args.platform)
    # Managed CPython includes ensurepip's bundled wheels. Bootstrap without a
    # seed download and retain pip for freeze and the installer's offline repair.
    run([str(python), "-m", "ensurepip", "--upgrade"])
    uv_pip = [uv, "pip", "install", "--python", str(python)]
    pyarrow_wheel = args.pyarrow_wheel or PYARROW_WHEELS[(args.platform, args.arch)]
    run([*uv_pip, "--no-deps", pyarrow_wheel])
    run([*uv_pip, "-r", str(requirements)])
    # MOMENT's upstream metadata pins older shared dependencies. Keep the
    # unified TSFM versions instead of downgrading them here; see the MOMENT
    # note in requirements_tsfm.txt for expected pip check conflicts and the
    # required model-loading/inference validation when versions change.
    run([*uv_pip, "--no-deps", args.moment_package])
    validate_cpu_torch(python, venv_dir, args.arch)
    patch_arm64_jaxlib(venv_dir, args.platform, args.arch)

    lock_file = venv_dir / "requirements.lock"
    freeze = subprocess.run(
        [str(python), "-m", "pip", "freeze", "--all"],
        capture_output=True,
        check=False,
        text=True,
    )
    if freeze.returncode:
        raise RuntimeError(freeze.stderr.strip() or "pip freeze failed")
    lock_file.write_text(freeze.stdout, encoding="utf-8")
    manifest = {
        "schema_version": 1,
        "kind": "tsfm-runtime",
        "runtime_id": f"tsfm-runtime-{args.version}",
        "python_version": actual_version,
        "platform": args.platform,
        "arch": args.arch,
        "requirements_sha256": sha256_file(lock_file),
    }
    (venv_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )
    prune_runtime(venv_dir)
    # Check the cleaned runtime before publishing; no model weights are loaded.
    run([str(python), "-B", "-c",
         "import numpy, pandas, scipy, torch, transformers, jax, jaxlib, timesfm; "
         "from uni2ts.model.moirai_moe import MoiraiMoEForecast; "
         "from chronos import BaseChronosPipeline; from momentfm import MOMENTPipeline"])
    # Final cleanup follows validation so no regenerated bytecode is packaged.
    prune_runtime(venv_dir)
    suffix = "zip" if args.platform == "windows" else "tar.xz"
    output_path = args.output_dir.resolve() / (
        f"tdengine-tdgpt-tsfm-runtime-{args.version}-{args.platform}-{args.arch}.{suffix}"
    )
    package_runtime(venv_dir, output_path, args.platform)
    print(f"built {output_path}")
    print(f"sha256 {sha256_file(output_path)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
