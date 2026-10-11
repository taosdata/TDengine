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


import pytest
from taosanodectl import build_parser, get_version_banner, translate_args


def test_version_banner_uses_packaged_tdgpt_version(tmp_path):
    version_file = tmp_path / "lib" / "taosanalytics" / "__init__.py"
    version_file.parent.mkdir(parents=True)
    version_file.write_text('__version__ = "9.8.7"\n', encoding="utf-8")

    script_path = tmp_path / "bin" / "taosanodectl.py"
    assert get_version_banner(script_path) == (
        "TDgpt - TDengine TSDB© Time-Series Data Analytics Platform (ver 9.8.7)"
    )


@pytest.mark.parametrize("flag", ["-V", "--version"])
def test_version_flag(flag, capsys):
    with pytest.raises(SystemExit) as exc_info:
        build_parser().parse_args([flag])

    assert exc_info.value.code == 0
    assert capsys.readouterr().out == get_version_banner() + "\n"


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
