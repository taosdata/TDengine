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

import importlib.util
import shutil
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[4]
TDGPT = ROOT / "source/taos-community/tools/tdgpt"


def load_module(path):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("entry", ["python", "windows", "shell"])
def test_pruning_preserves_libraries_needed_for_user_model_linking(tmp_path, entry):
    cc, ar = shutil.which("cc"), shutil.which("ar")
    if not cc or not ar:
        pytest.skip("C compiler and ar are required for the linking regression")
    venv = tmp_path / "venv"
    lib = venv / "lib"
    lib.mkdir(parents=True)
    source = tmp_path / "model.c"
    source.write_text("int model_value(void) { return 42; }\n")
    obj = tmp_path / "model.o"
    archive = lib / "libmodel.a"
    subprocess.run([cc, "-c", str(source), "-o", str(obj)], check=True)
    subprocess.run([ar, "rcs", str(archive), str(obj)], check=True)
    original = archive.read_bytes()
    (lib / "model.pyi").write_text("def model_value() -> int: ...")
    (lib / "tests").mkdir()
    (lib / "tests" / "unused.py").write_text("unused = True")
    (lib / "test").mkdir()
    (lib / "test" / "unused.py").write_text("unused = True")
    module = lib / "runtime_model.py"
    module.write_text("value = 42\n")
    subprocess.run([sys.executable, "-m", "py_compile", str(module)], check=True)
    cache = lib / "__pycache__"
    assert cache.is_dir()
    headers = lib / "include/test"
    headers.mkdir(parents=True)
    (headers / "model.h").write_text("/* user extension header */")
    docs = venv / "share/doc"
    docs.mkdir(parents=True)
    (docs / "LICENSE").write_text("license must survive")
    (docs / "README").write_text("removable documentation")
    man = venv / "share/man"
    man.mkdir()
    (man / "tool.1").write_text("manual")
    if entry in {"python", "windows"}:
        builder = (TDGPT / "offline-pkgs/build_tsfm_runtime.py" if entry == "python"
                   else TDGPT / "packaging/build_windows_lite_venv.py")
        load_module(builder).prune_runtime(venv)
    else:
        script = (TDGPT / "packaging/build_linux_offline_assets.sh").read_text()
        function = script.split("prune_venv() {", 1)[1].split("\n}\n", 1)[0]
        subprocess.run(["bash", "-c", 'prune_venv() {' + function + '\n}\nprune_venv "$1"', "test", str(venv)], check=True)
    assert not cache.exists()
    subprocess.run([sys.executable, "-B", "-c",
                    ("import sys; sys.path.insert(0, sys.argv[1]); "
                     "import runtime_model; assert runtime_model.value == 42"), str(lib)], check=True)
    assert not cache.exists()
    assert (docs / "LICENSE").read_text() == "license must survive"
    assert not (docs / "README").exists()
    assert not man.exists()
    assert (headers / "model.h").is_file()
    assert archive.read_bytes() == original
    assert (lib / "model.pyi").is_file()
    assert not (lib / "tests").exists()
    assert not (lib / "test").exists()
    main = tmp_path / "main.c"
    main.write_text("int model_value(void); int main(void) { return model_value() != 42; }\n")
    executable = tmp_path / "model"
    subprocess.run([cc, str(main), str(archive), "-o", str(executable)], check=True)
    subprocess.run([str(executable)], check=True)


@pytest.mark.parametrize("suffix,mode", [("tar.gz", "w:gz"), ("tar.xz", "w:xz"), ("txz", "w:xz")])
def test_archive_audit_accepts_static_libraries(tmp_path, suffix, mode):
    scanner = load_module(ROOT / ".agents/skills/tsdb-tdgpt-image-audit/scripts/scan_tdgpt_image.py")
    library = tmp_path / "libnpymath.a"
    library.write_bytes(b"static library fixture")
    archive = tmp_path / f"runtime.{suffix}"
    with tarfile.open(archive, mode) as output:
        output.add(library, arcname="venv/lib/numpy/core/lib/libnpymath.a")
        output.add(library, arcname="venv/lib/torch/include/test/model.h")
        output.add(library, arcname="venv/lib/model.pyi")
    _, failures = scanner.archive_entries(archive)
    assert failures == []
    with tarfile.open(archive, mode) as output:
        output.add(library, arcname="venv/lib/__pycache__/module.pyc")
        output.add(library, arcname="venv/lib/scipy/tests/test_model.py")
    _, failures = scanner.archive_entries(archive)
    assert len(failures) == 2
    assert any("__pycache__" in failure for failure in failures)
    assert any("scipy/tests" in failure for failure in failures)


def test_lite_cleanup_does_not_hide_install_failure(tmp_path):
    dockerfile = Path(__file__).with_name("DockerfileTDgpt.lite.base").read_text()
    command = dockerfile.split("RUN ", 1)[1].replace("\\\n", " ")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for name in ("apt-get", "pip3", "python3.12", "find", "rm", "gcc"):
        script = bin_dir / name
        script.write_text("#!/bin/sh\nexit " + ("23" if name == "pip3" else "0") + "\n")
        script.chmod(0o755)
    result = subprocess.run(["/bin/sh", "-c", command], check=False, env={"PATH": str(bin_dir), "PIP_INDEX_URL": ""})
    assert result.returncode == 23


def test_runtime_package_uses_xz_and_preserves_links(tmp_path):
    builder = load_module(TDGPT / "offline-pkgs/build_tsfm_runtime.py")
    source = tmp_path / "runtime"
    source.mkdir()
    (source / "module.py").write_text("value = 42\n")
    (source / "python").symlink_to("/usr/local/bin/python3")
    archive = tmp_path / "runtime.tar.xz"
    builder.package_runtime(source, archive, "linux")
    assert archive.read_bytes().startswith(bytes.fromhex("fd377a585a00"))
    with tarfile.open(archive) as output:
        assert output.extractfile("model_venvs/tsfm/module.py").read() == b"value = 42\n"
        assert output.getmember("model_venvs/tsfm/python").issym()
