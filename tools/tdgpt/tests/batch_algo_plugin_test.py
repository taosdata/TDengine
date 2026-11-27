# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

"""Discovery, parameter contract and routing of user batch algorithms."""

import hashlib
import json
import logging
import os
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/..")

import numpy as np

from lib.custom_algorithms import migrate, module_prefix
from lib.service_registry import ServiceRegistry, loader
from lib.taosanalytics.algo.envelop.batch_algorithms import (
    DEFAULT_ALGO,
    BatchAlgoParamsError,
    discover_plugins,
    resolve_params,
)
from lib.taosanalytics.handlers.misc import handle_build_batch_env, handle_list_batch_algos

PLUGIN_SOURCE = '''
import numpy as np

from .helpers import scale


class _PercentileEnv:
    name = "percentile_env"
    description = "declared in code"

    def build(self, ts, values, windows, params):
        base = scale(values, params.get("factor", 1.0))
        return base, base * 0.9, base * 1.1
'''

HELPERS_SOURCE = '''
def scale(values, factor):
    return values * factor
'''


@pytest.fixture()
def custom_root(tmp_path):
    """A writable custom algorithm root with an empty ``batch`` category."""
    root = tmp_path / "custom"
    (root / "batch").mkdir(parents=True)
    return root


def write_plugin(root, name="percentile_env", source=PLUGIN_SOURCE, sidecar=None):
    batch = Path(root) / "batch"
    batch.mkdir(parents=True, exist_ok=True)
    (batch / "helpers.py").write_text(HELPERS_SOURCE, encoding="utf-8")
    entry = batch / f"{name}.py"
    entry.write_text(source, encoding="utf-8")
    if sidecar is not None:
        (batch / f"{name}.json").write_text(json.dumps(sidecar), encoding="utf-8")
    return entry


def discover(root, reserved=(DEFAULT_ALGO,)):
    return discover_plugins(Path(root) / "batch", module_prefix(root), reserved=reserved)


def test_plugin_is_registered_with_helper_submodule(custom_root):
    entry = write_plugin(custom_root)

    plugins = discover(custom_root)

    assert list(plugins) == ["percentile_env"]
    plugin = plugins["percentile_env"]
    assert plugin.cls.__name__ == "_PercentileEnv"
    assert plugin.entry_sha256 == hashlib.sha256(entry.read_bytes()).hexdigest()
    assert plugin.version is None
    assert plugin.params == {}


def test_class_without_leading_underscore_is_not_registered(custom_root):
    write_plugin(
        custom_root,
        source="""
class PercentileEnv:
    name = "visible_env"

    def build(self, ts, values, windows, params):
        return values, values, values
""",
    )

    assert discover(custom_root) == {}


def test_class_without_build_is_not_registered(custom_root):
    write_plugin(
        custom_root,
        source="""
class _NoBuild:
    name = "no_build"
""",
    )

    assert discover(custom_root) == {}


def test_plugin_name_colliding_with_builtin_is_skipped(custom_root):
    write_plugin(
        custom_root,
        source="""
class _Shadow:
    name = "golden"

    def build(self, ts, values, windows, params):
        return values, values, values
""",
    )

    assert discover(custom_root) == {}


def test_broken_module_does_not_stop_the_others(custom_root):
    write_plugin(custom_root, name="broken", source="raise RuntimeError('boom')\n")
    write_plugin(custom_root)

    assert list(discover(custom_root)) == ["percentile_env"]


def test_sidecar_declares_parameters(custom_root):
    write_plugin(
        custom_root,
        sidecar={
            "version": "1.2.0",
            "description": "declared in sidecar",
            "params": {
                "factor": {"type": "number", "minimum": 0.5, "maximum": 2.0, "default": 1.5},
                "mode": {"type": "string", "enum": ["a", "b"], "default": "a"},
                "strict": {"type": "boolean", "default": False},
            },
        },
    )

    plugin = discover(custom_root)["percentile_env"]

    assert plugin.version == "1.2.0"
    assert plugin.description == "declared in sidecar"
    assert set(plugin.params) == {"factor", "mode", "strict"}
    assert resolve_params(plugin, None) == {"factor": 1.5, "mode": "a", "strict": False}
    assert resolve_params(plugin, {"factor": 2}) == {"factor": 2, "mode": "a", "strict": False}


def test_sidecar_digest_mismatch_keeps_the_computed_value(custom_root):
    write_plugin(custom_root, sidecar={"entry_sha256": "0" * 64})

    plugin = discover(custom_root)["percentile_env"]
    actual = hashlib.sha256((Path(custom_root) / "batch/percentile_env.py").read_bytes()).hexdigest()

    assert plugin.entry_sha256 == actual


def test_broken_sidecar_disables_validation_without_hiding_the_plugin(custom_root):
    entry = write_plugin(custom_root)
    entry.with_suffix(".json").write_text("{not json", encoding="utf-8")

    plugin = discover(custom_root)["percentile_env"]

    assert plugin.params == {}
    # Without a schema the request parameters are forwarded unchanged.
    assert resolve_params(plugin, {"anything": 1}) == {"anything": 1}


def test_param_spec_attribute_is_used_when_no_sidecar(custom_root):
    write_plugin(
        custom_root,
        source="""
class _PercentileEnv:
    name = "percentile_env"
    param_spec = {"factor": {"type": "number", "default": 1.0}}

    def build(self, ts, values, windows, params):
        return values, values, values
""",
    )

    plugin = discover(custom_root)["percentile_env"]

    assert resolve_params(plugin, {}) == {"factor": 1.0}


@pytest.mark.parametrize(
    "params, message",
    [
        ({"unknown": 1}, "unknown parameter"),
        ({"factor": "high"}, "must be number"),
        ({"factor": 9}, "must be <= 2.0"),
        ({"factor": 0.1}, "must be >= 0.5"),
        ({"mode": "c"}, "must be one of"),
        ({"strict": 1}, "must be boolean"),
    ],
)
def test_invalid_parameter_is_refused(custom_root, params, message):
    write_plugin(
        custom_root,
        sidecar={
            "params": {
                "factor": {"type": "number", "minimum": 0.5, "maximum": 2.0, "default": 1.0},
                "mode": {"type": "string", "enum": ["a", "b"], "default": "a"},
                "strict": {"type": "boolean", "default": False},
            }
        },
    )
    plugin = discover(custom_root)["percentile_env"]

    with pytest.raises(BatchAlgoParamsError) as excinfo:
        resolve_params(plugin, params)

    assert message in str(excinfo.value)


def test_missing_required_parameter_is_refused(custom_root):
    write_plugin(
        custom_root,
        sidecar={"params": {"factor": {"type": "number", "required": True}}},
    )
    plugin = discover(custom_root)["percentile_env"]

    with pytest.raises(BatchAlgoParamsError) as excinfo:
        resolve_params(plugin, {})

    assert "missing required parameter 'factor'" in str(excinfo.value)


@pytest.mark.parametrize(
    "params",
    [
        # A bound in quotes is the usual sidecar typo.
        {"factor": {"type": "number", "minimum": "0", "maximum": 10, "default": 5}},
        {"factor": {"type": "number", "minimum": 0, "maximum": "10", "default": 5}},
        {"factor": {"type": "number", "minimum": True, "default": 1}},
        # Reversed bounds cannot be satisfied by any value.
        {"factor": {"type": "number", "minimum": 5, "maximum": 1, "default": 2}},
        # A default that violates its own declaration.
        {"factor": {"type": "number", "default": "not-a-number"}},
        {"factor": {"enum": ["a", "b"], "default": "z"}},
    ],
)
def test_invalid_schema_disables_validation(custom_root, params):
    """A schema that cannot be checked is dropped; the plugin still loads."""
    write_plugin(custom_root, sidecar={"params": params})

    plugin = discover(custom_root)["percentile_env"]

    assert plugin.params == {}
    # Without a schema the request parameters are forwarded unchanged.
    assert resolve_params(plugin, {"factor": 5}) == {"factor": 5}


def test_unserialisable_default_disables_validation(custom_root):
    """A default the discovery payload cannot encode drops the schema."""
    write_plugin(
        custom_root,
        source="""
class _PercentileEnv:
    name = "percentile_env"
    param_spec = {"factor": {"default": {1, 2}}}

    def build(self, ts, values, windows, params):
        return values, values, values
""",
    )

    plugin = discover(custom_root)["percentile_env"]

    assert plugin.params == {}
    json.dumps(plugin.describe())


def test_plugin_with_relative_import_builds_an_envelope(custom_root):
    write_plugin(
        custom_root,
        sidecar={"version": "0.1.0", "params": {"factor": {"type": "number", "default": 2.0}}},
    )
    plugin = discover(custom_root)["percentile_env"]

    params = resolve_params(plugin, {})
    center, lower, upper = plugin.cls().build(None, np.array([1.0, 2.0]), None, params)

    assert list(center) == [2.0, 4.0]
    assert list(lower) == [1.8, 3.6]
    assert list(upper) == [2.2, 4.4]


def batch_request(**overrides):
    payload = {
        "data": [float(index) for index in range(10)],
        "ts": list(range(10)),
        "window": [(0, 9)],
        "prec": "ms",
        "protocol": 1.0,
    }
    payload.update(overrides)
    return SimpleNamespace(is_json=True, json=payload)


def plugin_registry(custom_root):
    return discover(custom_root)


def test_request_without_algo_keeps_using_the_builtin_pipeline():
    response = handle_build_batch_env(batch_request())

    assert response["rows"] == 1000
    assert response["algo"] == DEFAULT_ALGO
    assert response["version"] is None


def test_request_routes_to_the_registered_plugin(custom_root):
    write_plugin(custom_root, sidecar={"version": "0.1.0"})

    with patch.dict(loader.batch_services, plugin_registry(custom_root)):
        response = handle_build_batch_env(
            batch_request(algo="percentile_env", config={"factor": 3.0})
        )

    assert response["rows"] == 10
    assert response["algo"] == "percentile_env"
    assert response["version"] == "0.1.0"
    assert response["lower"] == [index * 3.0 * 0.9 for index in range(10)]


def test_unknown_algo_lists_the_available_ones(custom_root):
    with patch.dict(loader.batch_services, plugin_registry(custom_root)):
        response = handle_build_batch_env(batch_request(algo="missing"))

    assert response["rows"] == -1
    assert "unknown batch algorithm 'missing'" in response["msg"]
    assert DEFAULT_ALGO in response["msg"]


def test_plugin_parameter_error_is_reported_to_the_caller(custom_root):
    write_plugin(
        custom_root,
        sidecar={"params": {"factor": {"type": "number", "default": 1.0}}},
    )

    with patch.dict(loader.batch_services, plugin_registry(custom_root)):
        response = handle_build_batch_env(
            batch_request(algo="percentile_env", config={"bogus": 1})
        )

    assert response["rows"] == -1
    assert "unknown parameter" in response["msg"]


def test_string_bound_schema_does_not_fail_the_request(custom_root):
    """A schema typo degrades to no validation instead of a 500."""
    write_plugin(
        custom_root,
        sidecar={
            "params": {
                "factor": {
                    "type": "number",
                    "minimum": "0",
                    "maximum": "10",
                    "default": 1.0,
                }
            }
        },
    )

    with patch.dict(loader.batch_services, plugin_registry(custom_root)):
        response = handle_build_batch_env(
            batch_request(algo="percentile_env", config={"factor": 5})
        )

    assert response["rows"] == 10


def test_plugin_returning_inconsistent_curves_is_rejected(custom_root):
    write_plugin(
        custom_root,
        source="""
class _PercentileEnv:
    name = "percentile_env"

    def build(self, ts, values, windows, params):
        return values, values[:-1], values
""",
    )

    with patch.dict(loader.batch_services, plugin_registry(custom_root)):
        response = handle_build_batch_env(batch_request(algo="percentile_env"))

    assert response["rows"] == -1
    assert "same length" in response["msg"]


def test_plugin_returning_nan_is_rejected(custom_root):
    write_plugin(
        custom_root,
        source="""
import numpy as np


class _PercentileEnv:
    name = "percentile_env"

    def build(self, ts, values, windows, params):
        bad = np.full(values.shape, np.nan)
        return bad, bad, bad
""",
    )

    with patch.dict(loader.batch_services, plugin_registry(custom_root)):
        response = handle_build_batch_env(batch_request(algo="percentile_env"))

    assert response["rows"] == -1
    assert "NaN" in response["msg"]


def test_discovery_endpoint_reports_builtin_and_plugins(custom_root):
    write_plugin(custom_root, sidecar={"version": "0.1.0", "params": {"factor": {"type": "number"}}})

    with patch.dict(loader.batch_services, plugin_registry(custom_root)):
        response = handle_list_batch_algos()

    names = [item["name"] for item in response["algo"]]
    assert names == [DEFAULT_ALGO, "percentile_env"]
    assert response["algo"][0]["builtin"] is True
    # The built-in pipeline reports the nested pipeline configuration under the
    # request field name; only plugins carry a flat parameter schema.
    assert "params" not in response["algo"][0]
    assert response["algo"][0]["config"]["golden"]["method"] == "median_percentile"
    custom = response["algo"][1]
    assert custom["version"] == "0.1.0"
    assert custom["params"]["factor"]["type"] == "number"
    # Discovery must not leak the on-disk location of user code.
    assert str(custom_root) not in json.dumps(response)


def test_registry_wires_plugins_without_touching_the_service_list(custom_root, monkeypatch):
    """`load_custom_services()` is the production entry point for discovery."""
    write_plugin(custom_root, sidecar={"version": "2.0.0"})
    registry = ServiceRegistry()
    config = SimpleNamespace(
        get_custom_algo_directory=lambda: str(custom_root), custom_algo_explicit=False
    )

    monkeypatch.delenv("TDGPT_CUSTOM_ALGO_DIR", raising=False)
    with patch("lib.service_registry.Configure.get_instance", return_value=config):
        registry.load_custom_services()

    plugin = registry.get_batch_algo("percentile_env")
    assert plugin is not None
    assert plugin.version == "2.0.0"
    assert [item["name"] for item in registry.get_batch_algo_list()] == ["percentile_env"]
    # `/list` iterates `services` and reads `.type` before its try block, so a
    # batch plugin must never be registered there.
    assert "percentile_env" not in registry.services


def test_plugin_declaring_a_service_type_stays_out_of_the_service_list(custom_root, monkeypatch):
    """A plugin that inherits a service base class must not reach `/list`."""
    write_plugin(
        custom_root,
        source="""
class _PercentileEnv:
    name = "percentile_env"
    type = "anomaly-detection"

    def build(self, ts, values, windows, params):
        return values, values, values
""",
    )
    registry = ServiceRegistry()
    config = SimpleNamespace(
        get_custom_algo_directory=lambda: str(custom_root), custom_algo_explicit=False
    )

    monkeypatch.delenv("TDGPT_CUSTOM_ALGO_DIR", raising=False)
    with patch("lib.service_registry.Configure.get_instance", return_value=config):
        registry.load_custom_services()

    assert registry.get_batch_algo("percentile_env") is not None
    # `get_typed_services()` reads `.type` outside its try block, so a member
    # without the full service interface would turn `/list` into a 500.
    assert "percentile_env" not in registry.services
    assert registry.get_typed_services("anomaly-detection") == []


def test_world_writable_root_is_reported_but_still_loaded(custom_root, monkeypatch, caplog):
    os.chmod(custom_root, 0o777)
    write_plugin(custom_root)
    registry = ServiceRegistry()
    config = SimpleNamespace(
        get_custom_algo_directory=lambda: str(custom_root), custom_algo_explicit=False
    )

    monkeypatch.delenv("TDGPT_CUSTOM_ALGO_DIR", raising=False)
    with caplog.at_level(logging.WARNING), patch(
        "lib.service_registry.Configure.get_instance", return_value=config
    ):
        registry.load_custom_services()

    # The warning is advisory: the plugin is still loaded, but the exposure has
    # to be visible in the log.
    assert registry.get_batch_algo("percentile_env") is not None
    reported = [record.getMessage() for record in caplog.records if "world-writable" in record.getMessage()]
    assert len(reported) == 1
    assert str(custom_root) in reported[0]


def test_world_writable_category_directory_is_reported(custom_root):
    """A 0755 root does not clear ``batch/``, and that is what gets imported."""
    os.chmod(custom_root, 0o755)
    os.chmod(custom_root / "batch", 0o777)

    with patch("lib.service_registry.AppLogger.warning") as warning:
        ServiceRegistry._warn_if_world_writable(custom_root)

    reported = [call.args[-1] for call in warning.call_args_list]
    assert reported == [custom_root / "batch"]


def test_private_directories_are_not_reported(custom_root):
    os.chmod(custom_root, 0o750)
    os.chmod(custom_root / "batch", 0o750)

    with patch("lib.service_registry.AppLogger.warning") as warning:
        ServiceRegistry._warn_if_world_writable(custom_root)

    warning.assert_not_called()


def test_windows_skips_the_check(custom_root):
    """CPython synthesises 0o40777 for directories on Windows, whatever the ACLs."""
    os.chmod(custom_root, 0o777)

    with patch("lib.service_registry.os.name", "nt"), patch(
        "lib.service_registry.AppLogger.warning"
    ) as warning:
        ServiceRegistry._warn_if_world_writable(custom_root)

    warning.assert_not_called()


def test_migration_creates_and_copies_the_batch_category(tmp_path):
    install = tmp_path / "install"
    legacy = install / "lib" / "taosanalytics" / "algo" / "custom" / "batch"
    legacy.mkdir(parents=True)
    (legacy / "my_env.py").write_text("VALUE = 1\n", encoding="utf-8")
    (legacy / "my_env.json").write_text('{"version": "1.0"}', encoding="utf-8")
    config = install / "cfg" / "taosanode.config.py"
    config.parent.mkdir(parents=True)
    config.write_text("bind = '127.0.0.1:6035'\n", encoding="utf-8")
    default = tmp_path / "data" / "custom"

    result = migrate(install, default, config, tmp_path / "data" / "custom-backups")

    target = Path(result["directory"])
    assert (target / "batch" / "my_env.py").is_file()
    assert (target / "batch" / "my_env.json").is_file()
    assert (target / "ad").is_dir()
    assert (target / "fc").is_dir()
    assert result["copied"] == 2
