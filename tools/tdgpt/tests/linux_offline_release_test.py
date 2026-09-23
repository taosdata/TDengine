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

import hashlib
import shutil
import subprocess
from pathlib import Path

import pytest


@pytest.mark.parametrize("arch", ["x64", "arm64"])
def test_lite_release_builds_only_runtime_main_venv_and_installer(tmp_path, arch):
    packaging = tmp_path / "packaging"
    packaging.mkdir()
    source = Path(__file__).resolve().parents[1] / "packaging" / "build_linux_offline_release.sh"
    driver = packaging / source.name
    shutil.copyfile(source, driver)
    builder = '''set -eu
output=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --output-dir) output="$2"; shift 2 ;;
    --arch) arch="$2"; shift 2 ;;
    --bundle) test "$2" = main; shift 2 ;;
    --tdmodel-requirements) exit 99 ;;
    --lite) shift ;;
    *) shift 2 ;;
  esac
done
printf payload > "$output/ARTIFACT-linux-$arch.tar.gz"
'''
    for filename, artifact in [
        ("build_python_runtime_linux.sh", "tdengine-python-runtime-3.12.12"),
        ("build_linux_offline_assets.sh", "tdengine-tdgpt-lite-venv-1.0.1"),
    ]:
        stub = packaging / filename
        stub.write_text(builder.replace("ARTIFACT", artifact))
        stub.chmod(0o755)
    script = tmp_path / "script"
    script.mkdir()
    release_sh = script / "release.sh"
    release_sh.write_text(
        'set -eu\ntest "$5" = -l\nmkdir -p ../release\n'
        f'printf installer > ../release/tdengine-tdgpt-enterprise-lite-3.4.2.9.0909-linux-{arch}.tar.gz\n'
    )
    release_sh.chmod(0o755)
    result = subprocess.run(
        ["bash", str(driver), "--lite", "--version", "3.4.2.9.0909",
         "--arch", arch, "--work-dir", "work dir", "--output-dir", "output dir"],
        cwd=tmp_path, capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
    output = tmp_path / "output dir"
    assert len(list(output.glob("*.tar.gz"))) == 3
    assert len((output / "SHA256SUMS").read_text().splitlines()) == 3
    for line in (output / "SHA256SUMS").read_text().splitlines():
        expected, filename = line.split(maxsplit=1)
        digest = hashlib.sha256((output / filename).read_bytes()).hexdigest()
        assert digest == expected, filename


def test_lite_release_rejects_tdmodel_requirements(tmp_path):
    script = Path(__file__).resolve().parents[1] / "packaging" / "build_linux_offline_release.sh"

    result = subprocess.run(
        [
            "bash",
            str(script),
            "--lite",
            "--version",
            "3.4.2.9.0909",
            "--tdmodel-requirements",
            str(tmp_path / "requirements.txt"),
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    assert "cannot be used with --lite" in result.stderr
