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

"""Exercise the real registry with persistent plugins and existing configuration."""

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from lib.conf import Configure
from lib.service_registry import ServiceRegistry


class PersistentRegistryTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.custom = self.root / "custom"
        (self.custom / "ad").mkdir(parents=True)
        (self.custom / "fc").mkdir()
        self.cfg = self.root / "taosanode.config.py"
        self.cfg.write_text(f"custom_algo_dir = {str(self.custom)!r}\n")
        old = Configure._instance
        self.addCleanup(setattr, Configure, "_instance", old)
        self.env = patch.dict(os.environ, {}, clear=False)
        self.env.start()
        self.addCleanup(self.env.stop)
        os.environ.pop("TDGPT_CUSTOM_ALGO_DIR", None)
        Configure.init(str(self.cfg))
        self.registry = ServiceRegistry()

    def plugin(self, filename, name="user_forecast", code="return VALUE"):
        (self.custom / "fc" / filename).write_text(
            "from lib.base import AbstractForecastService\n"
            "from .helper import VALUE\n"
            "class _Plugin(AbstractForecastService):\n"
            f"    name = {name!r}\n"
            f"    def execute(self): {code}\n"
        )
        (self.custom / "fc/helper.py").write_text("VALUE = 42\n")

    def test_relative_import_and_actual_algorithm_execution(self):
        self.plugin("forecast.py")
        self.registry.load_custom_services()
        self.assertEqual(self.registry.services["user_forecast"].execute(), 42)

    def test_root_initializer_exports_helpers_and_runs_once(self):
        (self.custom / "__init__.py").write_text(
            "from pathlib import Path\n"
            "marker = Path(__file__).with_name('init-count.txt')\n"
            "marker.write_text(marker.read_text() + 'x' if marker.exists() else 'x')\n"
            "def adjust(value): return value + 10\n"
        )
        self.plugin("forecast.py", code="return adjust(VALUE)")
        path = self.custom / "fc/forecast.py"
        path.write_text("from .. import adjust\n" + path.read_text())
        self.registry.load_custom_services()
        self.registry.load_custom_services()
        self.assertEqual(self.registry.services["user_forecast"].execute(), 52)
        self.assertEqual((self.custom / "init-count.txt").read_text(), "x")

    def test_broken_root_initializer_preserves_builtins_and_can_retry(self):
        initializer = self.custom / "__init__.py"
        initializer.write_text("from . import helper\nraise RuntimeError('broken root')\n")
        (self.custom / "helper.py").write_text("VALUE = 1\n")
        builtin = object()
        self.registry.services["builtin"] = builtin
        self.registry.load_custom_services()
        self.assertIs(self.registry.services["builtin"], builtin)
        initializer.write_text("from .helper import VALUE\n")
        (self.custom / "helper.py").write_text("VALUE = 12345\n")
        self.plugin("forecast.py", code="return ROOT_VALUE")
        path = self.custom / "fc/forecast.py"
        path.write_text("from .. import VALUE as ROOT_VALUE\n" + path.read_text())
        self.registry.load_custom_services()
        self.assertEqual(self.registry.services["user_forecast"].execute(), 12345)

    def test_bad_module_does_not_hide_working_plugin(self):
        (self.custom / "fc/aaa_broken.py").write_text("raise ImportError('missing dependency')\n")
        self.plugin("forecast.py")
        self.registry.load_custom_services()
        self.assertEqual(self.registry.services["user_forecast"].execute(), 42)

    def test_duplicate_cannot_override_builtin_or_first_plugin(self):
        builtin = object()
        self.registry.services["builtin"] = builtin
        self.plugin("aaa.py", "builtin")
        self.plugin("first.py", code="return 1")
        self.plugin("second.py", code="return 2")
        self.registry.load_custom_services()
        self.assertIs(self.registry.services["builtin"], builtin)
        self.assertEqual(self.registry.services["user_forecast"].execute(), 1)

    def test_explicit_missing_directory_is_an_actionable_error(self):
        os.environ["TDGPT_CUSTOM_ALGO_DIR"] = str(self.root / "missing")
        with self.assertRaisesRegex(FileNotFoundError, "custom algorithm directory"):
            self.registry.load_custom_services()

    def test_old_configuration_without_new_key_keeps_defaults(self):
        self.cfg.write_text("bind = '127.0.0.1:6035'\n")
        conf = Configure.init(str(self.cfg))
        self.assertFalse(conf.custom_algo_explicit)
        self.assertTrue(Path(conf.get_custom_algo_directory()).is_absolute())

    def test_windows_current_and_legacy_layout_derive_actual_install_directory(self):
        for relative in ("lib/conf.py", "lib/taosanalytics/conf.py"):
            with self.subTest(layout=relative):
                install = self.root / "D drive" / "TDengine" / "taosanode"
                with patch("lib.conf.__file__", str(install / relative)), \
                        patch("lib.conf.platform.system", return_value="Windows"):
                    defaults = Configure.get_instance()._get_default_conf()
                self.assertEqual(Path(defaults["conf_path"]), install / "cfg/taosanode.config.py")
                self.assertEqual(Path(defaults["data_dir"]), install / "data")

    def test_configured_data_dir_controls_default_algorithm_location(self):
        self.cfg.write_text(f"data_dir = {str(self.root)!r}\n")
        conf = Configure.init(str(self.cfg))
        self.assertFalse(conf.custom_algo_explicit)
        self.assertEqual(Path(conf.get_custom_algo_directory()), self.custom)
        self.plugin("forecast.py")
        self.registry.load_custom_services()
        self.assertEqual(self.registry.services["user_forecast"].execute(), 42)
        self.cfg.write_text(f"data_dir = {str(self.root / 'relocated')!r}\n")
        conf = Configure.init(str(self.cfg))
        self.assertEqual(Path(conf.get_custom_algo_directory()), self.root / "relocated/custom")

    def test_invalid_configured_directory_fails_before_import(self):
        self.cfg.write_text("custom_algo_dir = 'relative'\n")
        Configure.init(str(self.cfg))
        with self.assertRaisesRegex(ValueError, "absolute"):
            self.registry.load_custom_services()


if __name__ == "__main__":
    unittest.main()
