# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
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
from unittest.mock import Mock

import pytest


@pytest.fixture(params=["directory", "file"])
def etool(monkeypatch, tmp_path, request):
    # Load the real module with only its native/framework dependencies stubbed.
    package = types.ModuleType("benchmark_log_test")
    package.__path__ = []
    monkeypatch.setitem(sys.modules, package.__name__, package)
    for name in ("epath", "eos", "log", "sql", "srvCtl"):
        monkeypatch.setitem(sys.modules, f"benchmark_log_test.{name}",
                            types.ModuleType(f"benchmark_log_test.{name}"))
    sql = Mock()
    sys.modules["benchmark_log_test.sql"].tdSql = sql
    cfg = tmp_path / "taos.cfg"
    cfg.write_text("debugFlag 135\ncDebugFlag 143\nlogDir /tmp/test-logs\nfirstEp localhost:6030\n")
    sys.modules["benchmark_log_test.srvCtl"].sc = types.SimpleNamespace(
        dnodeCfgPath=lambda _: str(tmp_path if request.param == "directory" else cfg))
    path = Path(__file__).resolve().parents[1] / "utils" / "etool.py"
    spec = importlib.util.spec_from_file_location("benchmark_log_test.etool", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, sql, cfg


@pytest.mark.parametrize("failure", [None, RuntimeError, SystemExit])
def test_restore_after_writer_success_or_failure(etool, failure):
    module, sql, original = etool
    before = original.read_bytes()
    def run():
        with module.benchmark_write_log() as cfg_dir:
            private = Path(cfg_dir) / "taos.cfg"
            text = private.read_text()
            assert "debugFlag 131" in text and "cDebugFlag 131" in text
            assert "debugFlag 135" not in text and "cDebugFlag 143" not in text
            assert "firstEp localhost:6030" in text
            assert "logDir /tmp/test-logs" in text
            assert all("131" in c.args[0] for c in sql.execute.call_args_list)
            if failure:
                raise failure("writer failed")
        return cfg_dir
    if failure:
        with pytest.raises(failure, match="writer failed"):
            run()
    else:
        assert not Path(run()).exists()
    assert original.read_bytes() == before
    resets = [c.args[0] for c in sql.execute.call_args_list if "135" in c.args[0]]
    assert len(resets) == len(module._BENCHMARK_SERVER_DEBUG_FLAGS)
    assert not module._benchmark_log_lock.locked()


def test_partial_entry_failure_restores_server(etool):
    module, sql, _ = etool
    def execute(statement):
        if "qDebugFlag 131" in statement:
            raise RuntimeError("server unavailable")
    sql.execute.side_effect = execute
    with pytest.raises(RuntimeError, match="server unavailable"):
        with module.benchmark_write_log():
            pytest.fail("writer must not start")
    assert any("wDebugFlag 135" in c.args[0] for c in sql.execute.call_args_list)
    assert not module._benchmark_log_lock.locked()


def test_overlapping_scope_is_rejected_without_restoring_early(etool):
    module, sql, _ = etool
    with module.benchmark_write_log():
        with pytest.raises(RuntimeError, match="overlap"):
            with module.benchmark_write_log():
                pytest.fail("overlapping writer must not start")
        assert not any("135" in c.args[0] for c in sql.execute.call_args_list)


def test_restore_failure_attempts_remaining_modules(etool):
    module, sql, _ = etool
    def execute(statement):
        if "dDebugFlag 135" in statement:
            raise RuntimeError("reset rejected")
    sql.execute.side_effect = execute
    with pytest.raises(RuntimeError, match="restore"):
        with module.benchmark_write_log():
            pass
    assert "wDebugFlag 135" in sql.execute.call_args_list[-1].args[0]
    assert not module._benchmark_log_lock.locked()


def test_legacy_benchmark_default_does_not_change_logs(etool, monkeypatch):
    module, sql, _ = etool
    monkeypatch.setattr(module, "benchMarkFile", lambda: "taosBenchmark")
    execute = Mock(return_value=0)
    monkeypatch.setattr(module, "exe", execute, raising=False)
    module.benchMark(json="data.json")
    execute.assert_called_once_with("taosBenchmark -f data.json")
    sql.execute.assert_not_called()


def test_low_log_json_benchmark_uses_private_config(etool, monkeypatch):
    module, sql, _ = etool
    monkeypatch.setattr(module, "benchMarkFile", lambda: "taosBenchmark")
    commands = []
    def execute(command):
        commands.append(command)
        assert ' -c "' in command and command.endswith(" -f data.json")
        assert all("131" in c.args[0] for c in sql.execute.call_args_list)
        return 0
    monkeypatch.setattr(module, "exe", execute, raising=False)
    module.benchMark(json="data.json", low_log=True)
    assert len(commands) == 1
    assert "wDebugFlag 135" in sql.execute.call_args_list[-1].args[0]


def test_shell_writer_preserves_failure_status(etool, monkeypatch):
    module, sql, _ = etool
    monkeypatch.setattr(module.os, "system", lambda _: 256)
    assert module.benchmark_write("taosBenchmark -t 100 -n 10000") == 256
    assert "wDebugFlag 135" in sql.execute.call_args_list[-1].args[0]


@pytest.mark.parametrize("failure", [False, True])
def test_python_data_load_restores_without_copying_client_config(etool, failure):
    module, sql, original = etool
    original.unlink()
    def run():
        with module.data_write_log():
            assert all("131" in c.args[0] for c in sql.execute.call_args_list)
            if failure:
                raise RuntimeError("python writer failed")
    if failure:
        with pytest.raises(RuntimeError, match="python writer failed"):
            run()
    else:
        run()
    assert "wDebugFlag 135" in sql.execute.call_args_list[-1].args[0]
    assert not module._benchmark_log_lock.locked()
