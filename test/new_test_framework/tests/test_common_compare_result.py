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
import sys
import types
from pathlib import Path


def _load_common_module():
    module_name = "test.new_test_framework.utils.common"
    module_path = Path(__file__).resolve().parents[1] / "utils" / "common.py"

    package_names = [
        "test",
        "test.new_test_framework",
        "test.new_test_framework.utils",
        "test.new_test_framework.utils.server",
    ]
    for name in package_names:
        if name not in sys.modules:
            package = types.ModuleType(name)
            package.__path__ = []
            sys.modules[name] = package

    boundary_module = types.ModuleType("test.new_test_framework.utils.boundary")
    boundary_module.DataBoundary = object
    sys.modules[boundary_module.__name__] = boundary_module

    class _DummyLog:
        def info(self, *_args, **_kwargs):
            pass

        def debug(self, *_args, **_kwargs):
            pass

        def notice(self, *_args, **_kwargs):
            pass

        def exit(self, message):
            raise AssertionError(message)

    log_module = types.ModuleType("test.new_test_framework.utils.log")
    log_module.tdLog = _DummyLog()
    sys.modules[log_module.__name__] = log_module

    for name in [
        "test.new_test_framework.utils.sql",
        "test.new_test_framework.utils.constant",
        "test.new_test_framework.utils.epath",
        "test.new_test_framework.utils.server.dnodes",
        "requests",
        "toml",
        "taos",
    ]:
        if name not in sys.modules:
            sys.modules[name] = types.ModuleType(name)

    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_compare_result_files_ignores_database_changed_prompt(tmp_path):
    common_module = _load_common_module()
    expected_file = tmp_path / "expected.ans"
    actual_file = tmp_path / "actual.result"

    expected_file.write_text(
        "taos> use db_asof_pushdown;\n"
        "Database changed.\n"
        "\n"
        "taos> select 1;\n"
        "1\n",
        encoding="utf-8",
    )
    actual_file.write_text(
        "taos> use db_asof_pushdown;\n"
        "\n"
        "taos> select 1;\n"
        "1\n",
        encoding="utf-8",
    )

    assert common_module.TDCom().compare_result_files(
        str(expected_file), str(actual_file)
    )


def test_generate_query_result_filters_without_shell(tmp_path, monkeypatch):
    common_module = _load_common_module()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(common_module.platform, "system", lambda: "Linux")
    sql_file = tmp_path / "input.sql"
    sql_file.write_text("explain select 1;\n", encoding="utf-8")
    common = common_module.TDCom()
    monkeypatch.setattr(common, "getClientCfgPath", lambda: "/test/config")
    monkeypatch.setattr(common, "_taos_cli_path", lambda: "/test/bin/taos")
    transcript = (
        "Welcome to the TDengine TSDB Command Line Interface\n"
        "taos> explain select 1;\r\n"
        "QUERY_PLAN:    InitPlan 1\r\n"
        "QUERY_PLAN:    InitPlan 2\r\n"
        "Query OK, 2 row(s) in set (0.01s)\n"
        "DB error: scalar subquery returned more than one row\n"
    )

    def run_cli(args, **kwargs):
        assert args == ["/test/bin/taos", "-c", "/test/config", "-f", str(sql_file)]
        assert "shell" not in kwargs
        return types.SimpleNamespace(stdout=transcript.encode(), stderr=b"", returncode=0)

    monkeypatch.setattr(common_module.subprocess, "run", run_cli)
    result = common.generate_query_result(str(sql_file), "output")
    assert Path(result).read_text() == (
        "taos> explain select 1;\n"
        "QUERY_PLAN:    InitPlan 1\n"
        "QUERY_PLAN:    InitPlan 2\n"
        "DB error: scalar subquery returned more than one row\n"
    )


def test_generate_query_result_rejects_failed_cli(tmp_path, monkeypatch):
    import pytest

    common_module = _load_common_module()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(common_module.platform, "system", lambda: "Linux")
    sql_file = tmp_path / "input.sql"
    sql_file.write_text("select 1;\n", encoding="utf-8")
    common = common_module.TDCom()
    monkeypatch.setattr(common, "getClientCfgPath", lambda: "/test/config")
    monkeypatch.setattr(common, "_taos_cli_path", lambda: "/test/bin/taos")
    monkeypatch.setattr(
        common_module.subprocess, "run",
        lambda *args, **kwargs: types.SimpleNamespace(
            stdout=b"partial output\n", stderr=b"ERROR: AddressSanitizer: heap-use-after-free\n", returncode=1
        ),
    )
    with pytest.raises(common_module.subprocess.CalledProcessError) as exc:
        common.generate_query_result(str(sql_file), "output")
    assert exc.value.returncode == 1
    assert "AddressSanitizer" in exc.value.stderr
    assert exc.value.output == "partial output\n"


def test_generate_query_result_preserves_windows_prompt_rules(tmp_path, monkeypatch):
    common_module = _load_common_module()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(common_module.platform, "system", lambda: "Windows")
    sql_file = tmp_path / "input.sql"
    sql_file.write_text("select 1;\n", encoding="utf-8")
    common = common_module.TDCom()
    monkeypatch.setattr(common, "getClientCfgPath", lambda: "/test/config")
    monkeypatch.setattr(
        common, "_run_taos_cli",
        lambda *args: ("taos>   \r\ntaos> select 1;   \r\n1\r\n", 0, ""),
    )
    result = common.generate_query_result(str(sql_file), "output")
    assert Path(result).read_text() == "taos> select 1;\n1\n"
