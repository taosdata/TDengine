#!/usr/bin/env python3
"""Build TDgpt Windows offline asset bundles."""

from __future__ import annotations

import argparse
import io
import os
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path
from typing import Iterable, Optional


DEFAULT_UV_EXE = Path(__file__).resolve().parent / "bin" / "uv.exe"


def info(message: str) -> None:
    print(f"[INFO] {message}")


def ok(message: str) -> None:
    print(f"[OK] {message}")


def fail(message: str) -> RuntimeError:
    return RuntimeError(message)


def run_command(cmd: list[str], env: Optional[dict[str, str]] = None) -> None:
    result = subprocess.run(cmd, capture_output=True, text=True, env=env, check=False)
    if result.returncode == 0:
        return
    if result.stdout:
        print(result.stdout, file=sys.stderr, end="")
    if result.stderr:
        print(result.stderr, file=sys.stderr, end="")
    raise fail(f"Command failed with exit code {result.returncode}: {' '.join(cmd)}")


def resolve_uv_exe(cli_value: str) -> Path:
    if cli_value:
        candidate = Path(cli_value).resolve()
        if candidate.exists():
            return candidate
        raise FileNotFoundError(f"uv.exe not found: {candidate}")

    if DEFAULT_UV_EXE.exists():
        return DEFAULT_UV_EXE.resolve()

    result = subprocess.run(["where", "uv"], capture_output=True, text=True, check=False)
    if result.returncode == 0:
        first = result.stdout.splitlines()[0].strip()
        if first:
            return Path(first).resolve()

    raise FileNotFoundError(
        "uv.exe was not found. Provide --uv-exe or place uv.exe under packaging/bin/."
    )


def find_python_runtime_home(root_dir: Path) -> Path:
    candidates: list[Path] = []
    for python_exe in root_dir.rglob("python.exe"):
        parent = python_exe.parent
        if parent.name.lower() == "scripts":
            continue
        if (parent / "Lib").exists():
            candidates.append(parent)
    if not candidates:
        raise FileNotFoundError(f"Unable to locate a Python runtime under {root_dir}")
    candidates.sort(key=lambda item: (len(item.parts), len(str(item))))
    return candidates[0]


def prepare_runtime_source(
    python_runtime_dir: Optional[Path],
    uv_exe: str,
    python_version: str,
    work_dir: Path,
) -> Path:
    if python_runtime_dir is not None:
        runtime_dir = python_runtime_dir.resolve()
        if not (runtime_dir / "python.exe").exists():
            raise FileNotFoundError(f"python.exe not found under --python-runtime-dir: {runtime_dir}")
        return runtime_dir

    uv_path = resolve_uv_exe(uv_exe)
    uv_store = work_dir / "uv-python-store"
    env = os.environ.copy()
    env["UV_PYTHON_INSTALL_DIR"] = str(uv_store)
    info(f"Installing Python {python_version} with uv into {uv_store}")
    run_command([str(uv_path), "python", "install", python_version], env=env)
    runtime_dir = find_python_runtime_home(uv_store)
    info(f"Resolved Python runtime: {runtime_dir}")
    return runtime_dir


def ensure_directory(path: Path, label: str) -> Path:
    resolved = path.resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"{label} not found: {resolved}")
    if not resolved.is_dir():
        raise NotADirectoryError(f"{label} is not a directory: {resolved}")
    return resolved


def add_directory_to_tar(tar_obj: tarfile.TarFile, source_dir: Path, arcname: str) -> None:
    tar_obj.add(source_dir, arcname=arcname)


def copy_tar_members(
    source_tar: Path,
    dest_tar: tarfile.TarFile,
    watched_prefixes: Optional[Iterable[str]] = None,
) -> tuple[int, set[str]]:
    count = 0
    found_prefixes: set[str] = set()
    normalized_prefixes = [item.strip("/").replace("\\", "/") + "/" for item in (watched_prefixes or [])]
    with tarfile.open(source_tar, "r:*") as src:
        for member in src:
            member_name = member.name.lstrip("./").replace("\\", "/")
            for prefix in normalized_prefixes:
                if member_name.startswith(prefix):
                    found_prefixes.add(prefix)
            fileobj = src.extractfile(member) if member.isfile() else None
            try:
                dest_tar.addfile(member, fileobj)
            finally:
                if fileobj is not None:
                    fileobj.close()
            count += 1
    return count, found_prefixes


def create_manifest_lines(
    seed_package: Optional[Path],
    runtime_dir: Path,
    main_venv_dir: Path,
    extra_venvs: Iterable[Path],
) -> str:
    lines = [
        "TDgpt offline bundle manifest",
        "",
        f"runtime={runtime_dir}",
        f"main_venv={main_venv_dir}",
    ]
    if seed_package is not None:
        lines.append(f"seed_package={seed_package}")
    extra_list = list(extra_venvs)
    if extra_list:
        lines.append("extra_venvs=" + ",".join(str(item) for item in extra_list))
    lines.append("")
    return "\n".join(lines)


def build_bundle(
    output_file: Path,
    seed_package: Optional[Path],
    runtime_dir: Path,
    main_venv_dir: Path,
    extra_venvs: list[Path],
) -> Path:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    if output_file.exists():
        output_file.unlink()

    with tarfile.open(output_file, "w") as tar_obj:
        existing_prefixes: set[str] = set()
        if seed_package is not None:
            info(f"Streaming seed package into bundle: {seed_package}")
            copied, existing_prefixes = copy_tar_members(
                seed_package,
                tar_obj,
                watched_prefixes=[
                    "python/runtime",
                    "venvs/venv",
                    *[f"venvs/{item.name}" for item in extra_venvs],
                ],
            )
            info(f"Copied {copied} members from seed package")

        if "python/runtime/" in existing_prefixes:
            info("Skipping Python runtime because seed package already contains python/runtime")
        else:
            info(f"Adding Python runtime from {runtime_dir}")
            add_directory_to_tar(tar_obj, runtime_dir, "python/runtime")

        if "venvs/venv/" in existing_prefixes:
            info("Skipping main venv because seed package already contains venvs/venv")
        else:
            info(f"Adding main venv from {main_venv_dir}")
            add_directory_to_tar(tar_obj, main_venv_dir, "venvs/venv")

        for extra_venv in extra_venvs:
            prefix = f"venvs/{extra_venv.name}/"
            if prefix in existing_prefixes:
                info(f"Skipping extra venv because seed package already contains {prefix[:-1]}")
                continue
            info(f"Adding extra venv from {extra_venv}")
            add_directory_to_tar(tar_obj, extra_venv, f"venvs/{extra_venv.name}")

        manifest = create_manifest_lines(seed_package, runtime_dir, main_venv_dir, extra_venvs)
        data = manifest.encode("utf-8")
        info("Adding bundle manifest")
        manifest_info = tarfile.TarInfo(name="offline-assets-manifest.txt")
        manifest_info.size = len(data)
        tar_obj.addfile(manifest_info, io.BytesIO(data))

    return output_file


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build TDgpt Windows offline asset bundles."
    )
    parser.add_argument(
        "--output-file",
        required=True,
        help="Output tar path, for example D:\\tdgpt-pkg-test\\deliverables\\full-package\\tdgpt-offline-full-bundle-win-x64.tar",
    )
    parser.add_argument(
        "--seed-package",
        help="Existing tar/tar.gz package that already contains offline model payloads and optional model venvs.",
    )
    parser.add_argument(
        "--python-runtime-dir",
        help="Existing Python runtime directory that contains python.exe. If omitted, uv will download Python.",
    )
    parser.add_argument(
        "--main-venv-dir",
        required=True,
        help="Main taosanode virtual environment directory to add as venvs/venv.",
    )
    parser.add_argument(
        "--extra-venv-dir",
        action="append",
        default=[],
        help="Additional virtual environment directory to add under venvs/. Repeat as needed.",
    )
    parser.add_argument(
        "--uv-exe",
        default="",
        help="Path to uv.exe used when --python-runtime-dir is omitted.",
    )
    parser.add_argument(
        "--python-version",
        default="3.11",
        help="Python version used by uv when --python-runtime-dir is omitted.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    output_file = Path(args.output_file).resolve()
    seed_package = Path(args.seed_package).resolve() if args.seed_package else None
    if seed_package is not None and not seed_package.exists():
        raise FileNotFoundError(f"Seed package not found: {seed_package}")

    main_venv_dir = ensure_directory(Path(args.main_venv_dir), "Main venv directory")
    extra_venvs = [ensure_directory(Path(item), "Extra venv directory") for item in args.extra_venv_dir]

    with tempfile.TemporaryDirectory(prefix="tdgpt-offline-assets-") as temp_name:
        temp_root = Path(temp_name)
        runtime_dir = prepare_runtime_source(
            Path(args.python_runtime_dir).resolve() if args.python_runtime_dir else None,
            args.uv_exe,
            args.python_version,
            temp_root,
        )
        runtime_dir = ensure_directory(runtime_dir, "Python runtime directory")

        bundle_path = build_bundle(output_file, seed_package, runtime_dir, main_venv_dir, extra_venvs)

    ok(f"Offline bundle created: {bundle_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
