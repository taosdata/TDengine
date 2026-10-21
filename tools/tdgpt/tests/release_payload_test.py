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

import platform
import shutil
import subprocess
import tarfile
from pathlib import Path

import pytest


@pytest.mark.parametrize("lite", [False, True])
@pytest.mark.parametrize("legacy_resources", [False, True])
def test_release_payload_preserves_lifecycle_and_full_models(tmp_path, lite, legacy_resources):
    source = Path(__file__).resolve().parents[1]
    root = tmp_path / "tdgpt"
    shutil.copytree(source / "script", root / "script", ignore=shutil.ignore_patterns("__pycache__"))
    shutil.copytree(source / "cfg", root / "cfg")
    for requirement in source.glob("requirements*.txt"):
        shutil.copyfile(requirement, root / requirement.name)
    analytics = root / "lib" / "taosanalytics"
    analytics.mkdir(parents=True)
    (analytics / "__init__.py").write_text("__version__ = 'dev'\n")
    model_service = root / "lib" / "tsfm" / "service"
    model_service.mkdir(parents=True)
    (model_service / "tdtsfm-server.py").write_text("# model server\n")
    # A development helper matching the former glob must not enter the package.
    (root / "script" / "start-development.sh").write_text("exit 99\n")
    if legacy_resources:
        (root / "resource").mkdir()
        (root / "resource" / "legacy.sql").write_text("select 1;\n")
    models = tmp_path / "models"
    models.mkdir()
    for model in ("tdtsfm", "timemoe", "chronos", "moment-large", "moirai", "timesfm"):
        (models / f"{model}.tar.gz").write_bytes(b"model payload")
    args = ["bash", str(root / "script" / "release.sh"), "-e", "community", "-v", "3.4.2.0", "-m", str(models), "-a"]
    if lite:
        args.append("-l")
    result = subprocess.run(args, cwd=tmp_path, capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
    # This lifecycle test also runs with the separate XZ/flat-layout MR.
    # Compression and layout contracts have their own packaging regressions.
    archives = list((root / "release").glob("*.tar.gz"))
    archives += list((root / "release").glob("*.tar.xz"))
    assert len(archives) == 1
    variant = "-lite" if lite else ""
    assert archives[0].name.startswith(f"tdengine-tdgpt-oss{variant}-3.4.2.0-{platform.system().lower()}-")
    with tarfile.open(archives[0]) as package:
        members = {member.name.split("/", 1)[1] for member in package.getmembers() if "/" in member.name}
        for script in (
            "start-model.sh",
            "stop-model.sh",
            "taosanode_service.py",
            "taosanodectl.py",
            "resource_manager.py",
            "resource_manifest.py",
            "uninstall.sh",
        ):
            assert f"bin/{script}" in members
        assert "bin/start-development.sh" not in members
        assert "bin/start-with-allmd.sh" not in members
        for removed_script in (
            "start.sh",
            "stop.sh",
            "start-tdtsfm.sh",
            "stop-tdtsfm.sh",
            "start-time-moe.sh",
            "stop-time-moe.sh",
            "start_model_from_remote.sh",
        ):
            assert f"bin/{removed_script}" not in members
        assert "install.sh" in members
        assert "cfg/taosanoded.service" in members
        assert ("resource/legacy.sql" in members) == legacy_resources
        assert (".tdgpt-lite" in members) == lite
        assert ("model/tdtsfm.tar.gz" in members) == (not lite)
        assert ("model/moment-large.tar.gz" in members) == (not lite)
        assert "requirements_docker.txt" in members
        legacy_library = next(
            (member for member in package.getmembers() if member.name.endswith("/package.tar.gz")),
            None,
        )
        if legacy_library is not None:
            library = package.extractfile(legacy_library)
            assert library is not None
            with tarfile.open(fileobj=library) as code:
                assert "./lib/tsfm/service/tdtsfm-server.py" in code.getnames()
        else:
            assert "lib/tsfm/service/tdtsfm-server.py" in members


@pytest.mark.parametrize("legacy_resources", [False, True])
def test_install_resource_accepts_model_free_payload(tmp_path, legacy_resources):
    source = Path(__file__).resolve().parents[1] / "script" / "install.sh"
    text = source.read_text()
    start = text.index("function install_resource() {")
    function = text[start : text.index("\nfunction ", start + 1)]
    package = tmp_path / "package"
    package.mkdir()
    if legacy_resources:
        (package / "resource").mkdir()
        (package / "resource" / "legacy.sql").write_text("select 1;\n")
    destination = tmp_path / "installed-resources"
    # Execute the real installation function in isolation; never touch system paths.
    result = subprocess.run(
        [
            "bash",
            "-ec",
            function + '\ncsudo=""\ncustom_dir_set=1\nscript_dir="$1"\nresourceDir="$2"\ninstall_resource',
            "test",
            str(package),
            str(destination),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert destination.is_dir()
    assert (destination / "legacy.sql").exists() == legacy_resources


def test_uninstall_removes_current_and_legacy_model_links(tmp_path):
    source = Path(__file__).resolve().parents[1] / "script" / "uninstall.sh"
    text = source.read_text()
    start = text.index("remove_model_service() {")
    function = text[start : text.index("\nremove_service()", start)]
    commands = ("start-model", "stop-model", "start-tdtsfm", "stop-tdtsfm", "start-time-moe", "stop-time-moe", "start-model-from-remote")
    for command in commands:
        (tmp_path / command).symlink_to(tmp_path / "missing-script")
    (tmp_path / "unrelated-command").write_text("preserve me")
    result = subprocess.run(
        ["bash", "-ec", function + '\ncsudo=""\nbin_link_dir="$1"\nremove_model_service', "test", str(tmp_path)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert all(not (tmp_path / command).is_symlink() for command in commands)
    assert (tmp_path / "unrelated-command").read_text() == "preserve me"
