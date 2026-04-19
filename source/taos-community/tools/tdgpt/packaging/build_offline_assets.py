#!/usr/bin/env python3
"""Build TDgpt Windows offline asset bundles."""

from __future__ import annotations

import argparse
import copy
import datetime
import io
import os
import subprocess
import sys
import tarfile
import tempfile
import time
from pathlib import Path
from typing import Iterable, Optional


DEFAULT_UV_EXE = Path(__file__).resolve().parent / "bin" / "uv.exe"
MODEL_NAME_ALIASES = {
    "tdtsfm": "tdtsfm",
    "timemoe": "timemoe",
    "moirai": "moirai",
    "chronos": "chronos",
    "timesfm": "timesfm",
    "moment": "moment",
    "moment-large": "moment",
    "momentfm": "moment",
}


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


def normalize_member_name(name: str) -> str:
    normalized = name.lstrip("./").replace("\\", "/")
    while normalized.startswith("/"):
        normalized = normalized[1:]
    return normalized


def remap_seed_member_name(member_name: str) -> str:
    normalized = normalize_member_name(member_name)
    if not normalized:
        return normalized

    top_level, _, remainder = normalized.partition("/")
    canonical_model = MODEL_NAME_ALIASES.get(top_level.lower())
    if canonical_model is None:
        return normalized
    if top_level.lower() in {"python", "venvs", "model"}:
        return normalized
    if remainder:
        return f"model/{canonical_model}/{remainder}"
    return f"model/{canonical_model}"


def normalize_prefixes(prefixes: Optional[Iterable[str]]) -> list[str]:
    normalized: list[str] = []
    for item in prefixes or []:
        pattern = item.strip("/").replace("\\", "/")
        if not pattern:
            continue
        if pattern not in normalized:
            normalized.append(pattern)
    return normalized


def matches_pattern(member_name: str, pattern: str) -> bool:
    return member_name == pattern or member_name.startswith(pattern + "/")


def inspect_tar_prefixes(source_tar: Path, watched_prefixes: Iterable[str]) -> set[str]:
    found_prefixes: set[str] = set()
    normalized_prefixes = normalize_prefixes(watched_prefixes)
    with tarfile.open(source_tar, "r:*") as src:
        for member in src:
            member_name = remap_seed_member_name(member.name)
            if not member_name:
                continue
            for prefix in normalized_prefixes:
                if matches_pattern(member_name, prefix):
                    found_prefixes.add(prefix + "/")
    return found_prefixes


def collect_seed_models(source_tar: Path) -> list[str]:
    models: set[str] = set()
    with tarfile.open(source_tar, "r:*") as src:
        for member in src:
            member_name = remap_seed_member_name(member.name)
            if not member_name:
                continue
            if not member_name.startswith("model/"):
                continue
            remainder = member_name[len("model/"):]
            model_name, _, _ = remainder.partition("/")
            if model_name:
                models.add(model_name)
    return sorted(models)


def copy_tar_members(
    source_tar: Path,
    dest_tar: tarfile.TarFile,
    include_prefixes: Optional[Iterable[str]] = None,
    exclude_prefixes: Optional[Iterable[str]] = None,
) -> int:
    count = 0
    include_normalized = normalize_prefixes(include_prefixes)
    exclude_normalized = normalize_prefixes(exclude_prefixes)
    with tarfile.open(source_tar, "r:*") as src:
        for member in src:
            member_name = remap_seed_member_name(member.name)
            if not member_name:
                continue
            if include_normalized and not any(matches_pattern(member_name, prefix) for prefix in include_normalized):
                continue
            if exclude_normalized and any(matches_pattern(member_name, prefix) for prefix in exclude_normalized):
                continue
            member = copy.copy(member)
            member.name = member_name
            fileobj = src.extractfile(member) if member.isfile() else None
            try:
                dest_tar.addfile(member, fileobj)
            finally:
                if fileobj is not None:
                    fileobj.close()
            count += 1
    return count


def create_manifest_lines(
    seed_package: Optional[Path],
    main_venv_dir: Optional[Path],
    extra_venvs: Iterable[Path],
    bundle_kind: str,
) -> str:
    lines = [
        "TDgpt offline assets manifest",
        "",
        "generated_at=" + datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
        f"bundle_kind={bundle_kind}",
        f"main_venv={main_venv_dir or ''}",
        "runtime=provided-by-installer",
    ]
    if seed_package is not None:
        lines.append(f"seed_package={seed_package}")
        model_list = collect_seed_models(seed_package)
        if model_list:
            lines.append("models=" + ",".join(model_list))
    extra_list = list(extra_venvs)
    if extra_list:
        lines.append("extra_venvs=" + ",".join(str(item) for item in extra_list))
    lines.append("")
    return "\n".join(lines)


def resolve_tar_write_mode(output_file: Path) -> str:
    suffixes = "".join(output_file.suffixes).lower()
    if suffixes.endswith(".tar.gz") or suffixes.endswith(".tgz"):
        return "w:gz"
    return "w"


def build_bundle(
    output_file: Path,
    seed_package: Optional[Path],
    main_venv_dir: Optional[Path],
    extra_venvs: list[Path],
    bundle_kind: str,
) -> Path:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    if output_file.exists():
        output_file.unlink()

    if bundle_kind in {"venv", "combined"} and main_venv_dir is None:
        raise fail("--main-venv-dir is required when --bundle-kind is venv or combined")

    with tarfile.open(output_file, resolve_tar_write_mode(output_file)) as tar_obj:
        existing_prefixes: set[str] = set()
        seed_handled_prefixes: list[str] = []
        if bundle_kind in {"venv", "combined"}:
            seed_handled_prefixes.append("venvs/venv")
        if bundle_kind in {"model", "combined"}:
            seed_handled_prefixes.extend(f"venvs/{item.name}" for item in extra_venvs)

        if seed_package is not None and seed_handled_prefixes:
            info(f"Inspecting seed package prefixes: {seed_package}")
            existing_prefixes = inspect_tar_prefixes(
                seed_package,
                watched_prefixes=seed_handled_prefixes,
            )

        if bundle_kind in {"venv", "combined"}:
            if "venvs/venv/" in existing_prefixes:
                info("Adding main venv from seed package")
                copied = copy_tar_members(seed_package, tar_obj, include_prefixes=["venvs/venv"])
                info(f"Copied {copied} main venv members from seed package")
            else:
                info(f"Adding main venv from {main_venv_dir}")
                add_directory_to_tar(tar_obj, main_venv_dir, "venvs/venv")

        if bundle_kind in {"model", "combined"}:
            for extra_venv in extra_venvs:
                prefix = f"venvs/{extra_venv.name}/"
                if prefix in existing_prefixes:
                    info(f"Adding extra venv from seed package: {prefix[:-1]}")
                    copied = copy_tar_members(seed_package, tar_obj, include_prefixes=[prefix[:-1]])
                    info(f"Copied {copied} members for {prefix[:-1]} from seed package")
                    continue
                info(f"Adding extra venv from {extra_venv}")
                add_directory_to_tar(tar_obj, extra_venv, f"venvs/{extra_venv.name}")

        if seed_package is not None and bundle_kind in {"model", "combined"}:
            exclude_prefixes = ["python", "offline-assets-manifest.txt"]
            if bundle_kind == "combined":
                exclude_prefixes.extend(seed_handled_prefixes)
            else:
                exclude_prefixes.extend(["venvs/venv", *seed_handled_prefixes])
            info(f"Streaming seed package model payloads after runtime and venvs: {seed_package}")
            copied = copy_tar_members(
                seed_package,
                tar_obj,
                exclude_prefixes=exclude_prefixes,
            )
            info(f"Copied {copied} members from seed package")

        manifest = create_manifest_lines(seed_package, main_venv_dir, extra_venvs, bundle_kind)
        data = manifest.encode("utf-8")
        info("Adding offline assets manifest")
        manifest_info = tarfile.TarInfo(name="offline-assets-manifest.txt")
        manifest_info.size = len(data)
        manifest_info.mtime = int(time.time())
        tar_obj.addfile(manifest_info, io.BytesIO(data))

    return output_file


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build TDgpt Windows offline asset bundles."
    )
    parser.add_argument(
        "--bundle-kind",
        choices=["venv", "model", "combined"],
        default="combined",
        help="Bundle type: venv = only venvs/venv, model = model payloads and optional model venvs, combined = legacy combined bundle.",
    )
    parser.add_argument(
        "--output-file",
        required=True,
        help="Output tar path, for example D:\\tdgpt-pkg-test\\deliverables\\full-package\\tdengine-tdgpt-offline-assets-3.4.1.0.0325-windows-x64.tar",
    )
    parser.add_argument(
        "--seed-package",
        help="Existing tar/tar.gz package that already contains offline model payloads and optional model venvs.",
    )
    parser.add_argument(
        "--main-venv-dir",
        help="Main taosanode virtual environment directory to add as venvs/venv.",
    )
    parser.add_argument(
        "--extra-venv-dir",
        action="append",
        default=[],
        help="Additional virtual environment directory to add under venvs/. Repeat as needed.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    output_file = Path(args.output_file).resolve()
    seed_package = Path(args.seed_package).resolve() if args.seed_package else None
    if seed_package is not None and not seed_package.exists():
        raise FileNotFoundError(f"Seed package not found: {seed_package}")

    main_venv_dir = None
    if args.main_venv_dir:
        main_venv_dir = ensure_directory(Path(args.main_venv_dir), "Main venv directory")
    extra_venvs = [ensure_directory(Path(item), "Extra venv directory") for item in args.extra_venv_dir]

    bundle_path = build_bundle(output_file, seed_package, main_venv_dir, extra_venvs, args.bundle_kind)

    ok(f"Offline assets package created: {bundle_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
