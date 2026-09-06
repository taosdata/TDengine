import json
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

SCRIPT = Path(__file__).parents[1] / "offline-pkgs" / "split_model_resource.py"


def _source_archive(tmp_path: Path) -> Path:
    archive = tmp_path / "full.zip"
    with zipfile.ZipFile(archive, "w") as output:
        output.writestr("model/moirai/config.json", "{}")
    return archive


def _catalog(tmp_path: Path, metadata: dict[str, str]) -> Path:
    catalog = tmp_path / "catalog.json"
    catalog.write_text(json.dumps({"moirai": metadata}), encoding="utf-8")
    return catalog


def _run_split(tmp_path: Path, catalog: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--source",
            str(_source_archive(tmp_path)),
            "--output-dir",
            str(tmp_path / "output"),
            "--version",
            "1.0.0",
            "--catalog",
            str(catalog),
        ],
        check=False,
        capture_output=True,
        text=True,
    )


def test_split_uses_model_artifact_name_and_records_metadata(tmp_path):
    result = _run_split(tmp_path, _catalog(tmp_path, {
        "model_id": "Salesforce/moirai-moe-1.0-R-small",
        "model_version": "moirai-moe-1.0-r-small",
        "artifact_slug": "moirai-moe-1-0-r-small",
        "note": "Salesforce Moirai-MoE 1.0 R small model",
    }))

    assert result.returncode == 0, result.stderr
    output_dir = tmp_path / "output"
    archive = output_dir / "tdengine-tdgpt-model-moirai-moe-1-0-r-small.zip"
    manifest = json.loads(
        (output_dir / "tdengine-tdgpt-tsfm-models-manifest.json").read_text(
            encoding="utf-8"
        )
    )
    model = manifest["models"]["moirai"]

    assert archive.is_file()
    assert model["filename"] == archive.name
    assert model["model_id"] == "Salesforce/moirai-moe-1.0-R-small"
    assert model["model_version"] == "moirai-moe-1.0-r-small"
    assert model["size"] == archive.stat().st_size
    assert model["size"] > 0


@pytest.mark.parametrize("missing_field", ["model_id", "model_version", "artifact_slug"])
def test_split_rejects_incomplete_catalog_metadata(tmp_path, missing_field):
    metadata = {
        "model_id": "Salesforce/moirai-moe-1.0-R-small",
        "model_version": "moirai-moe-1.0-r-small",
        "artifact_slug": "moirai-moe-1-0-r-small",
    }
    del metadata[missing_field]

    result = _run_split(tmp_path, _catalog(tmp_path, metadata))

    assert result.returncode != 0
    assert missing_field in result.stderr
