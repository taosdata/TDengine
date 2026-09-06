from __future__ import annotations

import argparse
import importlib.util
import subprocess
import zipfile
from pathlib import Path

SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "packaging"
    / "build_windows_lite_venv.py"
)


def load_module():
    spec = importlib.util.spec_from_file_location(
        "build_windows_lite_venv_test_module", SCRIPT_PATH
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_runtime_python_accepts_runtime_root(tmp_path):
    module = load_module()
    python = tmp_path / "python.exe"
    python.write_bytes(b"python")

    assert module.runtime_python(tmp_path) == python


def test_prepare_work_dir_rejects_existing_content(tmp_path):
    module = load_module()
    (tmp_path / "existing.txt").write_text("content", encoding="utf-8")

    try:
        module.prepare_work_dir(tmp_path)
    except RuntimeError as error:
        assert "work directory must be empty" in str(error)
    else:
        raise AssertionError("non-empty work directory was accepted")


def test_package_venv_uses_direct_offline_layout(tmp_path):
    module = load_module()
    venv_dir = tmp_path / "source"
    (venv_dir / "Scripts").mkdir(parents=True)
    (venv_dir / "Scripts" / "python.exe").write_bytes(b"python")
    (venv_dir / "pyvenv.cfg").write_text("home = C:\\Python\n", encoding="utf-8")
    output = tmp_path / "output" / "venv.zip"

    module.package_venv(venv_dir, output)

    with zipfile.ZipFile(output) as archive:
        names = set(archive.namelist())
    assert "venvs/venv/Scripts/python.exe" in names
    assert "venvs/venv/pyvenv.cfg" in names


def test_main_builds_versioned_windows_lite_zip(tmp_path, monkeypatch):
    module = load_module()
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()
    (runtime_dir / "python.exe").write_bytes(b"python")
    requirements = tmp_path / "requirements_docker.txt"
    requirements.write_text("requests==2.32.3\n", encoding="utf-8")
    work_dir = tmp_path / "work"
    output_dir = tmp_path / "output"
    args = argparse.Namespace(
        runtime_dir=runtime_dir,
        requirements=requirements,
        work_dir=work_dir,
        output_dir=output_dir,
        version="1.0.1",
        index_url="",
        extra_index_url="",
        trusted_host="",
    )

    def fake_run(command):
        if command[1:4] == ["-m", "venv", "--copies"]:
            venv_dir = Path(command[-1])
            (venv_dir / "Scripts").mkdir(parents=True)
            (venv_dir / "Scripts" / "python.exe").write_bytes(b"python")
            (venv_dir / "pyvenv.cfg").write_text(
                "home = C:\\Python\n", encoding="utf-8"
            )

    monkeypatch.setattr(module, "parse_args", lambda: args)
    monkeypatch.setattr(module, "ensure_windows_x64", lambda: None)
    monkeypatch.setattr(module, "python_version", lambda _python: "3.12.12")
    monkeypatch.setattr(module, "run", fake_run)
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess(
            args=[], returncode=0, stdout="requests==2.32.3\n", stderr=""
        ),
    )

    assert module.main() == 0

    output = output_dir / "tdengine-tdgpt-lite-venv-1.0.1-windows-x64.zip"
    with zipfile.ZipFile(output) as archive:
        names = set(archive.namelist())
        manifest = archive.read("venvs/venv/manifest.json").decode("utf-8")
    assert "venvs/venv/requirements.lock" in names
    assert '"kind": "tdgpt-lite-main-venv"' in manifest
    assert '"python_version": "3.12.12"' in manifest
