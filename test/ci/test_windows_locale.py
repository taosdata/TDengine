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

"""Exercise generated node/client configs without loading native TDengine bindings."""

import ast
import copy
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock


UTILS = Path(__file__).resolve().parents[1] / "new_test_framework" / "utils"


def load_methods(path, class_name, names, namespace):
    namespace["__file__"] = str(path)
    tree = ast.parse(path.read_text(encoding="utf-8"))
    cls = next(node for node in tree.body
               if isinstance(node, ast.ClassDef) and node.name == class_name)
    cls.body = [node for node in cls.body
                if isinstance(node, ast.FunctionDef) and node.name in names]
    exec(compile(ast.Module(body=[cls], type_ignores=[]), str(path), "exec"), namespace)
    return namespace[class_name]


class WindowsLocaleTests(unittest.TestCase):
    def generated(self, system, template_locale="en_US.UTF-8", overrides=None):
        with tempfile.TemporaryDirectory() as directory:
            template = {"locale": template_locale, "charset": "UTF-8", "port": 6030}
            session = SimpleNamespace(
                work_dir=directory, taos_bin_path=directory, denodes_num=6,
                level=1, disk=1, independentMnode=False, mnodes_num=3,
                asan=False, start_taosadapter=False, set_taoskeeper=False)
            request = SimpleNamespace(session=session)
            namespace = {
                "os": SimpleNamespace(path=os.path, makedirs=os.makedirs, environ={}),
                "sys": SimpleNamespace(platform="win32" if system == "Windows" else "linux"),
                "platform": SimpleNamespace(system=lambda: system),
                "copy": copy, "tdLog": Mock(),
                "yaml": SimpleNamespace(dump=json.dump),
                "load_yaml_config": lambda path: copy.deepcopy(template)
                    if os.path.basename(path) == "taos_config.yaml" else {},
            }
            cls = load_methods(UTILS / "before_test.py", "BeforeTest",
                               {"get_config_from_param", "_dnode_cfg_baseline", "update_cfg"}, namespace)
            helper = cls()
            helper.root_dir = directory
            helper.request = request
            helper.get_config_from_param(request)
            if overrides is not None:
                helper.update_cfg(overrides)
            with open(Path(directory) / "env" / "ci_default.yaml") as file:
                config = json.load(file)
            # Check the configuration passed to taostest for every cluster node.
            return [node["config"] for node in config["settings"][0]["spec"]["dnodes"]]

    def test_windows_cluster_uses_engine_default_locale(self):
        nodes = self.generated("Windows")
        self.assertEqual(len(nodes), 6)
        for node in nodes:
            self.assertNotIn("locale", node)
            self.assertEqual(node["charset"], "UTF-8")

    def test_linux_keeps_posix_locale(self):
        for node in self.generated("Linux"):
            self.assertEqual(node["locale"], "en_US.UTF-8")

    def test_windows_preserves_custom_template_locale(self):
        for node in self.generated("Windows", "English_United States.1252"):
            self.assertEqual(node["locale"], "English_United States.1252")

    def test_windows_preserves_explicit_case_locale(self):
        # Even an intentionally invalid locale must reach the server when a
        # case explicitly requests it, rather than being silently normalized.
        for node in self.generated("Windows", overrides={"locale": "invalid-test-locale"}):
            self.assertEqual(node["locale"], "invalid-test-locale")

    def test_legacy_node_and_client_defaults_and_overrides(self):
        for filename, name, args in (("dnode.py", "TDDnode", ()),
                                      ("simClient.py", "TDSimClient", ("fixture",))):
            for system in ("Windows", "Linux"):
                with self.subTest(component=name, system=system):
                    namespace = {"platform": SimpleNamespace(system=lambda: system), "os": os}
                    cls = load_methods(UTILS / "server" / filename, name,
                                       {"__init__", "addExtraCfg"}, namespace)
                    instance = cls(*args)
                    if system == "Windows":
                        self.assertNotIn("locale", instance.cfgDict)
                    else:
                        self.assertEqual(instance.cfgDict["locale"], "en_US.UTF-8")
                    self.assertEqual(instance.cfgDict["charset"], "UTF-8")
                    instance.addExtraCfg("locale", "invalid-test-locale")
                    self.assertEqual(instance.cfgDict["locale"], "invalid-test-locale")


if __name__ == "__main__":
    unittest.main()
