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
