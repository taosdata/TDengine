#!/usr/bin/env python3
"""Split a TDgpt full model resource ZIP into raw per-model ZIP archives."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_obj:
        for chunk in iter(lambda: file_obj.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def model_members(archive: zipfile.ZipFile) -> dict[str, list[zipfile.ZipInfo]]:
    result: dict[str, list[zipfile.ZipInfo]] = {}
    for member in archive.infolist():
        parts = Path(member.filename).parts
        if len(parts) < 2 or parts[0] != "model" or not parts[1]:
            continue
        if member.filename.startswith("/") or ".." in parts:
            raise ValueError(f"unsafe archive member: {member.filename}")
        result.setdefault(parts[1], []).append(member)
    if not result:
        raise ValueError("source archive does not contain model/<name>/ entries")
    return result


def write_model_archive(
    source: zipfile.ZipFile, members: list[zipfile.ZipInfo], target: Path
) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        prefix=f".{target.name}.", suffix=".tmp", dir=target.parent, delete=False
    ) as temp_file:
        temp_path = Path(temp_file.name)
    try:
        with zipfile.ZipFile(temp_path, "w", compression=zipfile.ZIP_DEFLATED) as output:
            for member in members:
                if member.is_dir():
                    output.writestr(member, b"")
                    continue
                with source.open(member, "r") as input_file, output.open(
                    member, "w", force_zip64=True
                ) as output_file:
                    shutil.copyfileobj(input_file, output_file, length=1024 * 1024)
        os.replace(temp_path, target)
    finally:
        temp_path.unlink(missing_ok=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, type=Path, help="full resource ZIP")
    parser.add_argument("--output-dir", required=True, type=Path, help="output directory")
    parser.add_argument("--version", required=True, help="model resource version")
    parser.add_argument(
        "--catalog",
        type=Path,
        default=Path(__file__).with_name("tsfm_model_catalog.json"),
        help="model metadata catalog",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    source_path = args.source.resolve()
    output_dir = args.output_dir.resolve()
    if not source_path.is_file():
        raise FileNotFoundError(f"source ZIP not found: {source_path}")
    catalog = json.loads(args.catalog.resolve().read_text(encoding="utf-8"))
    if not isinstance(catalog, dict):
        raise ValueError("model catalog must be a JSON object")

    manifest_models: dict[str, dict[str, object]] = {}
    with zipfile.ZipFile(source_path, "r") as source:
        for model_name, members in sorted(model_members(source).items()):
            metadata = catalog.get(model_name)
            if not isinstance(metadata, dict):
                raise ValueError(f"model catalog is missing metadata for {model_name}")
            required_fields = ("model_id", "model_version", "artifact_slug")
            missing_fields = [field for field in required_fields if not metadata.get(field)]
            if missing_fields:
                raise ValueError(
                    f"model catalog is missing {', '.join(missing_fields)} for {model_name}"
                )
            artifact_slug = str(metadata["artifact_slug"])
            filename = f"tdengine-tdgpt-model-{artifact_slug}.zip"
            target = output_dir / filename
            write_model_archive(source, members, target)
            manifest_models[model_name] = {
                "version": args.version,
                "filename": filename,
                "model_id": metadata.get("model_id"),
                "model_version": metadata.get("model_version"),
                "note": metadata.get("note", ""),
                "size": target.stat().st_size,
                "sha256": sha256_file(target),
                "archive_root": f"model/{model_name}",
            }
            print(f"built {target} ({len(members)} entries)")

    manifest = {
        "schema_version": 1,
        "kind": "tdgpt-tsfm-model-release",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": {"filename": source_path.name, "sha256": sha256_file(source_path)},
        "models": manifest_models,
    }
    manifest_path = output_dir / "tdengine-tdgpt-tsfm-models-manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
