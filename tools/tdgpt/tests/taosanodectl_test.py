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


import ast
import io
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from taosanodectl import build_parser, get_version_banner, translate_args


def test_version_banner_uses_packaged_tdgpt_version(tmp_path):
    version_file = tmp_path / "lib" / "taosanalytics" / "__init__.py"
    version_file.parent.mkdir(parents=True)
    version_file.write_text('__version__ = "9.8.7"\n', encoding="utf-8")

    script_path = tmp_path / "bin" / "taosanodectl.py"
    assert get_version_banner(script_path) == (
        "TDgpt - TDengine TSDB(c) Time-Series Data Analytics Platform (ver 9.8.7)"
    )


@pytest.mark.parametrize("flag", ["-V", "--version"])
def test_version_flag(flag, capsys):
    with pytest.raises(SystemExit) as exc_info:
        build_parser().parse_args([flag])

    assert exc_info.value.code == 0
    assert capsys.readouterr().out == get_version_banner() + "\n"


@pytest.mark.parametrize("flag", ["-V", "--version"])
def test_version_flag_redirected_to_gbk(flag, monkeypatch):
    # capsys captures Unicode and misses Windows redirected-output encoding errors.
    output = io.BytesIO()
    stream = io.TextIOWrapper(output, encoding="gbk", errors="strict")
    monkeypatch.setattr(sys, "stdout", stream)
    with pytest.raises(SystemExit) as exc_info:
        build_parser().parse_args([flag])
    stream.flush()
    assert exc_info.value.code == 0
    assert output.getvalue().decode("gbk") == get_version_banner() + "\n"


def test_model_start_translation():
    args = build_parser().parse_args(["model", "start", "moirai"])
    assert translate_args(args) == ["model-start", "moirai"]


def test_model_start_timeout_translation():
    args = build_parser().parse_args(["model", "start", "moment", "--timeout", "300"])
    assert translate_args(args) == ["model-start", "moment", "--timeout", "300"]


def test_resource_import_translation():
    args = build_parser().parse_args([
        "resource", "import", "--package", "runtime.tar.gz",
        "--expected-sha256", "a" * 64, "--replace",
    ])
    assert translate_args(args) == [
        "resource-import", "--package", "runtime.tar.gz",
        "--expected-sha256", "a" * 64, "--replace",
    ]


def test_service_wait_ready_translation():
    args = build_parser().parse_args(["service", "wait-ready", "--timeout", "45"])
    assert translate_args(args) == ["wait-ready", "--timeout", "45"]


def test_service_wait_ready_uses_configured_default():
    args = build_parser().parse_args(["service", "wait-ready"])
    assert translate_args(args) == ["wait-ready"]


def test_resource_apply_translation():
    args = build_parser().parse_args([
        "resource", "apply", "moirai", "--release", "1.0.0",
        "--resource-base-url", "https://download.taosdata.com",
    ])
    assert translate_args(args) == [
        "resource-apply", "moirai", "--release", "1.0.0",
        "--resource-base-url", "https://download.taosdata.com",
    ]


def test_resource_verify_translation():
    args = build_parser().parse_args(["resource", "verify", "all"])
    assert translate_args(args) == ["resource-verify", "all"]


def test_model_status_json_translation():
    """The RESTful service reads worker liveness from this output."""
    args = build_parser().parse_args(["model", "status", "--format", "json"])
    assert translate_args(args) == ["model-status", "--format", "json"]


def test_model_status_stays_table_by_default():
    args = build_parser().parse_args(["model", "status"])
    assert translate_args(args) == ["model-status"]


@pytest.mark.parametrize("release", [None, "1.0.0"])
def test_resource_release_default_and_explicit_rollback(release):
    argv = ["resource", "check"]
    if release is not None:
        argv.extend(["--release", release])
    args = build_parser().parse_args(argv)
    translated = translate_args(args)
    assert translated[translated.index("--release") + 1] == (release or "1.0.1")


def test_resource_release_contract():
    """Keep separately packaged CLI/HTTP defaults and every recovery path aligned."""
    root = Path(__file__).resolve().parents[1]
    constants = {}
    for relative in ("script/resource_manifest.py", "lib/tsfm/mgmt/response_event.py"):
        tree = ast.parse((root / relative).read_text(encoding="utf-8"))
        constants[relative] = next(
            ast.literal_eval(node.value)
            for node in tree.body
            if isinstance(node, ast.Assign)
            and any(isinstance(target, ast.Name) and target.id == "DEFAULT_RESOURCE_RELEASE" for target in node.targets)
        )
    assert set(constants.values()) == {"1.0.1"}

    # Cover all fallbacks, including async resubmission and SSE replay, without
    # importing the runtime-heavy HTTP application into the standalone CLI tests.
    expected_counts = {
        "script/taosanodectl.py": 1,
        "script/taosanode_service.py": 1,
        "lib/tsfm/mgmt/foundation_model.py": 6,
        "lib/tsfm/mgmt/response_event.py": 1,
    }
    for relative, expected_count in expected_counts.items():
        tree = ast.parse((root / relative).read_text(encoding="utf-8"))
        defaults = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            if node.func.attr == "get" and node.args and isinstance(node.args[0], ast.Constant):
                if node.args[0].value == "release" and len(node.args) > 1:
                    # A display placeholder is not a resource selection default.
                    if isinstance(node.args[1], ast.Constant) and node.args[1].value == "-":
                        continue
                    defaults.append(node.args[1])
            elif node.func.attr == "add_argument" and any(
                isinstance(arg, ast.Constant) and arg.value == "--release" for arg in node.args
            ):
                defaults.extend(kw.value for kw in node.keywords if kw.arg == "default")
        assert len(defaults) == expected_count, relative
        assert all(isinstance(value, ast.Name) and value.id == "DEFAULT_RESOURCE_RELEASE" for value in defaults), relative


@pytest.mark.parametrize("flag", ["--help", "--version"])
def test_packaged_cli_starts_without_application_imports(tmp_path, flag):
    root = Path(__file__).resolve().parents[1]
    scripts = tmp_path / "bin"
    scripts.mkdir()
    for name in ("taosanodectl.py", "resource_manifest.py"):
        shutil.copyfile(root / "script" / name, scripts / name)
    library = tmp_path / "lib" / "taosanalytics"
    library.mkdir(parents=True)
    (library / "__init__.py").write_text('__version__ = "9.8.7"\n')
    result = subprocess.run(
        [sys.executable, "-E", str(scripts / "taosanodectl.py"), flag],
        cwd=tmp_path, capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
    assert "usage:" in result.stdout if flag == "--help" else "9.8.7" in result.stdout
