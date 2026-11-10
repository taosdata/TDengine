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

"""Regression coverage for persistent plugins and non-destructive upgrades."""

import errno
import importlib
import json
import os
import runpy
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from lib.custom_algorithms import custom_directory, migrate, module_prefix


class CustomAlgorithmsTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.install = self.root / "install"
        self.default = self.root / "data" / "custom"
        self.config = self.install / "cfg" / "taosanode.config.py"
        self.config.parent.mkdir(parents=True)
        self.config.write_text("bind = '127.0.0.1:6035'\n", encoding="utf-8")
        self.legacy = self.install / "lib" / "taosanalytics" / "algo" / "custom"
        (self.legacy / "ad").mkdir(parents=True)
        (self.legacy / "ad" / "example.py").write_text("VALUE = 42\n")
        self.backups = self.root / "data" / "custom-backups"
        self.env = patch.dict(os.environ, {}, clear=False)
        self.env.start()
        self.addCleanup(self.env.stop)
        os.environ.pop("TDGPT_CUSTOM_ALGO_DIR", None)

    def upgrade(self):
        return migrate(self.install, self.default, self.config, self.backups)

    @unittest.skipIf(os.name == "nt", "Linux shell installer path")
    def test_linux_cleanup_after_migration_preserves_persistent_data(self):
        import subprocess

        script = Path(__file__).resolve().parents[1] / "script/install.sh"
        text = script.read_text()
        start = text.index("function install_main_path() {")
        function = text[start : text.index("\nfunction ", start + 1)]
        self.upgrade()
        model = self.install / "data/model/tsfm/example"
        runtime = self.install / "data/model_venvs/tsfm"
        model.mkdir(parents=True)
        runtime.mkdir(parents=True)
        (model / "weights.bin").write_bytes(b"weights")
        (runtime / "manifest.json").write_text('{"runtime_id":"tsfm-runtime-1.0.0"}')
        result = subprocess.run(
            [
                "bash",
                "-ec",
                function + '\ncsudo=""\ninstall_main_dir="$1"\nglobal_conf_dir="$2"\ninstall_main_path',
                "test",
                str(self.install),
                str(self.config.parent),
            ],
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((model / "weights.bin").read_bytes(), b"weights")
        self.assertIn("1.0.0", (runtime / "manifest.json").read_text())
        self.assertEqual((self.default / "ad/example.py").read_text(), "VALUE = 42\n")

    def test_upgrade_preserves_code_across_program_replacement(self):
        result = self.upgrade()
        self.assertEqual(result["copied"], 1)
        self.assertEqual((Path(result["backup"]) / "ad/example.py").read_text(), "VALUE = 42\n")
        shutil.rmtree(self.install / "lib")
        second = self.upgrade()
        self.assertEqual(second["copied"], 0)
        self.assertEqual((self.default / "ad/example.py").read_text(), "VALUE = 42\n")
        self.assertTrue((self.default / "fc").is_dir())
        self.assertNotIn("custom_algo_dir =", self.config.read_text())
        self.assertEqual(self.config.read_text().count("data_dir ="), 1)

    def test_retirement_preserves_backup_and_records_verified_files(self):
        result = self.upgrade()
        self.assertFalse(self.legacy.exists())
        record = json.loads(Path(result["record"]).read_text())
        self.assertEqual(record["state"], "retired")
        self.assertEqual(record["backup"], result["backup"])
        self.assertEqual(record["directory"], str(self.default))
        self.assertEqual(set(record["files"]), {"ad/example.py"})
        self.assertEqual((Path(result["backup"]) / "ad/example.py").read_text(), "VALUE = 42\n")

    def test_later_upgrade_preserves_edits_and_does_not_restore_deleted_plugins(self):
        (self.legacy / "ad/deleted.py").write_text("VALUE = 1\n")
        self.upgrade()
        (self.default / "ad/example.py").write_text("USER_EDIT = True\n")
        (self.default / "ad/deleted.py").unlink()
        # Windows retains the rest of lib rather than replacing the whole tree.
        result = self.upgrade()
        self.assertEqual(result["copied"], 0)
        self.assertIsNone(result["backup"])
        self.assertFalse((self.default / "ad/deleted.py").exists())
        self.assertEqual((self.default / "ad/example.py").read_text(), "USER_EDIT = True\n")

    def test_identical_existing_target_still_gets_backup_before_retirement(self):
        shutil.copytree(self.legacy, self.default)
        result = self.upgrade()
        self.assertEqual(result["copied"], 0)
        self.assertFalse(self.legacy.exists())
        self.assertEqual((Path(result["backup"]) / "ad/example.py").read_text(), "VALUE = 42\n")

    def test_interrupted_copy_keeps_source_and_can_be_retried(self):
        with patch("lib.custom_algorithms.os.link", side_effect=OSError("interrupted")):
            with self.assertRaisesRegex(OSError, "interrupted"):
                self.upgrade()
        self.assertTrue((self.legacy / "ad/example.py").exists())
        self.upgrade()
        self.assertFalse(self.legacy.exists())
        self.assertEqual((self.default / "ad/example.py").read_text(), "VALUE = 42\n")

    def test_failed_backup_verification_keeps_legacy_source(self):
        copytree = shutil.copytree

        def corrupt_backup(source, target, *args, **kwargs):
            result = copytree(source, target, *args, **kwargs)
            candidate = Path(target) / "ad/example.py"
            if candidate.is_file():
                candidate.write_text("CORRUPT = True\n")
            return result

        with patch("lib.custom_algorithms.shutil.copytree", side_effect=corrupt_backup):
            with self.assertRaisesRegex(OSError, "verification failed"):
                self.upgrade()
        self.assertEqual((self.legacy / "ad/example.py").read_text(), "VALUE = 42\n")

    def test_cleanup_interruption_cannot_reintroduce_retired_plugins(self):
        with patch("lib.custom_algorithms.shutil.rmtree", side_effect=OSError("cleanup interrupted")):
            with self.assertRaisesRegex(OSError, "cleanup interrupted"):
                self.upgrade()
        self.assertFalse(self.legacy.exists())
        (self.default / "ad/example.py").unlink()
        self.upgrade()
        self.assertFalse((self.default / "ad/example.py").exists())

    def test_conflicts_refuse_without_copying_any_files(self):
        (self.default / "ad").mkdir(parents=True)
        (self.default / "ad/example.py").write_text("USER_EDIT = True\n")
        (self.legacy / "ad/aaa.py").write_text("VALUE = 1\n")
        with self.assertRaisesRegex(FileExistsError, "conflict"):
            self.upgrade()
        self.assertFalse((self.default / "ad/aaa.py").exists())
        self.assertNotIn("custom_algo_dir", self.config.read_text())
        self.assertEqual((self.default / "ad/example.py").read_text(), "USER_EDIT = True\n")

    def test_relative_imports_and_namespaces_do_not_shadow_builtins(self):
        (self.default / "ad").mkdir(parents=True)
        (self.default / "ad/helper.py").write_text("VALUE = 42\n")
        (self.default / "ad/plugin.py").write_text("from .helper import VALUE\n")
        before = list(__import__("sys").path)
        prefix = module_prefix(self.default)
        self.assertEqual(importlib.import_module(prefix + ".ad.plugin").VALUE, 42)
        self.assertEqual(before, __import__("sys").path)
        self.assertNotEqual(prefix, module_prefix(self.root / "other"))

    def test_installer_ignores_transient_environment_override(self):
        target = self.root / "external"
        os.environ["TDGPT_CUSTOM_ALGO_DIR"] = str(target)
        result = self.upgrade()
        self.assertEqual(result["directory"], str(self.default))
        self.assertFalse(target.exists())
        self.assertIn("data_dir =", self.config.read_text())
        self.assertNotIn("custom_algo_dir", self.config.read_text())

    def test_check_only_leaves_source_config_and_destination_untouched(self):
        original = self.config.read_bytes()
        result = migrate(self.install, self.default, self.config, self.backups, check_only=True)
        self.assertEqual(result["pending"], 1)
        self.assertTrue(self.legacy.is_dir())
        self.assertFalse(self.default.exists())
        self.assertFalse(self.backups.exists())
        self.assertEqual(self.config.read_bytes(), original)

    def test_all_conflicts_reported_before_any_copy(self):
        (self.legacy / "fc").mkdir()
        (self.legacy / "fc/second.py").write_text("old\n")
        (self.default / "ad").mkdir(parents=True)
        (self.default / "fc").mkdir()
        (self.default / "ad/example.py").write_text("modified\n")
        (self.default / "fc/second.py").write_text("modified\n")
        with self.assertRaises(FileExistsError) as caught:
            self.upgrade()
        self.assertIn(str(self.legacy / "ad/example.py"), str(caught.exception))
        self.assertIn(str(self.legacy / "fc/second.py"), str(caught.exception))
        self.assertFalse(self.backups.exists())

    def test_legacy_config_encoding_and_unicode_data_directory_are_preserved(self):
        for encoding, prefix in (("gbk", "# coding: gbk\n# 用户配置\n"), ("utf-8-sig", "# 用户配置\n")):
            with self.subTest(encoding=encoding):
                self.default = self.root / "数据" / encoding / "custom"
                original = prefix.encode(encoding)
                self.config.write_bytes(original)
                self.upgrade()
                self.assertTrue(self.config.read_bytes().startswith(original))
                self.assertEqual(runpy.run_path(str(self.config))["data_dir"], str(self.default.parent))

    def test_filesystems_without_hardlinks_can_migrate(self):
        for code in (errno.EPERM, errno.ENOTSUP):
            with self.subTest(errno=code), tempfile.TemporaryDirectory() as temp:
                target = Path(temp) / "custom"
                self.config.write_text(f"custom_algo_dir = {str(target)!r}\n")
                with patch("lib.custom_algorithms.os.link", side_effect=OSError(code, "unsupported")):
                    self.upgrade()
                self.assertEqual((target / "ad/example.py").read_text(), "VALUE = 42\n")
                # Prepare another independent legacy source for the next case.
                (self.legacy / "ad").mkdir(parents=True)
                (self.legacy / "ad/example.py").write_text("VALUE = 42\n")

    def test_fallback_copy_failure_removes_partial_file_and_preserves_source(self):
        def interrupted(source, dest):
            dest.write(b"partial")
            raise OSError("copy interrupted")

        with patch("lib.custom_algorithms.os.link", side_effect=OSError(errno.ENOTSUP, "unsupported")), \
                patch("lib.custom_algorithms.shutil.copyfileobj", side_effect=interrupted):
            with self.assertRaisesRegex(OSError, "copy interrupted"):
                self.upgrade()
        self.assertTrue((self.legacy / "ad/example.py").is_file())
        self.assertFalse((self.default / "ad/example.py").exists())
        self.upgrade()
        self.assertEqual((self.default / "ad/example.py").read_text(), "VALUE = 42\n")

    def test_fallback_refuses_concurrent_destination(self):
        def concurrent_copy(temp, dest):
            dest.write_text("administrator edit\n")
            raise OSError(errno.ENOTSUP, "unsupported")

        with patch("lib.custom_algorithms.os.link", side_effect=concurrent_copy):
            with self.assertRaises(FileExistsError):
                self.upgrade()
        self.assertEqual((self.default / "ad/example.py").read_text(), "administrator edit\n")
        self.assertTrue(self.legacy.is_dir())

    @unittest.skipIf(os.name == "nt", "Linux shell installer path")
    def test_linux_stop_failure_keeps_legacy_and_does_not_replace_program(self):
        script = Path(__file__).resolve().parents[1] / "script/install.sh"
        text = script.read_text()
        start = text.index("function installProduct() {")
        function = text[start:text.index("\n# check for python version", start)]
        stage = self.root / "package"
        (stage / "lib").mkdir(parents=True)
        shutil.copy2(Path(__file__).resolve().parents[1] / "lib/custom_algorithms.py", stage / "lib")
        runtime = self.install / "data/venv/bin"
        runtime.mkdir(parents=True)
        (runtime / "python3").symlink_to(sys.executable)
        (self.install / "bin").mkdir()
        controller = self.install / "bin/taosanodectl.py"
        original = self.config.read_bytes()
        for failed_command in ("model", "service"):
            controller.write_text(f"import sys\nsys.exit(1 if sys.argv[1] == {failed_command!r} else 0)\n")
            setup = '''
csudo=""
script_dir="$1"
install_main_dir="$2"
dataDir="$3"
global_conf_dir="$4"
configFile=taosanode.config.py
venvDir="$install_main_dir/data/venv"
migration_test_python="$5"
validate_offline_packages() { :; }
resolve_repair_python_bin() { printf '%s\\n' "$migration_test_python"; }
install_main_path() { touch "$install_main_dir/replaced"; }
'''
            result = subprocess.run(
                ["bash", "-ec", setup + function + "\ninstallProduct\n", "test",
                 str(stage), str(self.install), str(self.default.parent), str(self.config.parent), sys.executable],
                cwd=self.install, capture_output=True, text=True,
            )
            with self.subTest(command=failed_command):
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertIn("legacy algorithms preserved", result.stderr)
                self.assertTrue((self.legacy / "ad/example.py").exists())
                self.assertFalse((self.install / "replaced").exists())
                self.assertFalse(self.default.exists())
                self.assertEqual(self.config.read_bytes(), original)

    def test_configured_external_directory_and_idempotency(self):
        target = self.root / "external"
        self.config.write_text(f"custom_algo_dir = {str(target)!r}\n")
        self.assertEqual(self.upgrade()["directory"], str(target))
        self.assertEqual(self.upgrade()["copied"], 0)

    def test_default_follows_data_dir_and_does_not_pin_custom_path(self):
        data = self.root / "relocated-data"
        self.config.write_text(f"data_dir = {str(data)!r}\n")
        result = self.upgrade()
        self.assertEqual(result["directory"], str(data / "custom"))
        self.assertEqual((data / "custom/ad/example.py").read_text(), "VALUE = 42\n")
        self.assertNotIn("custom_algo_dir", self.config.read_text())
        relocated = self.root / "another-data"
        self.assertEqual(
            custom_directory({"data_dir": str(relocated)}, self.default, self.install), relocated / "custom",
        )

    def test_explicit_custom_path_and_environment_override_data_dir_default(self):
        config = {"data_dir": str(self.root / "data"), "custom_algo_dir": str(self.root / "explicit")}
        self.assertEqual(custom_directory(config, self.default, self.install), self.root / "explicit")
        os.environ["TDGPT_CUSTOM_ALGO_DIR"] = str(self.root / "env")
        self.assertEqual(custom_directory(config, self.default, self.install), self.root / "env")

    def test_bytecode_excluded_and_helpers_retained(self):
        (self.legacy / "ad/__pycache__").mkdir()
        (self.legacy / "ad/__pycache__/bad.pyc").write_bytes(b"cache")
        (self.legacy / "ad/weights.bin").write_bytes(b"weights")
        self.upgrade()
        self.assertFalse((self.default / "ad/__pycache__").exists())
        self.assertEqual((self.default / "ad/weights.bin").read_bytes(), b"weights")

    def test_reject_managed_or_relative_paths(self):
        for value in [
            "relative",
            "",
            str(self.install / "lib/custom"),
            str(self.default.parent / "model/custom"),
            str(self.default.parent),
        ]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                custom_directory({"custom_algo_dir": value}, self.default, self.install)

    def test_windows_rejects_directories_cleaned_by_uninstaller(self):
        windows = SimpleNamespace(name="nt", environ=os.environ, PathLike=os.PathLike)
        with patch("lib.custom_algorithms.os", windows):
            for directory in ("bin", "lib", "resource", "requirements", "python", "log", "venvs"):
                with self.subTest(directory=directory), self.assertRaisesRegex(ValueError, "overlaps"):
                    custom_directory(
                        {"custom_algo_dir": str(self.install / directory / "custom")}, self.default, self.install,
                    )
            target = self.install / "custom"
            self.assertEqual(custom_directory({}, target, self.install), target)

    @unittest.skipIf(os.name == "nt", "symlink creation requires Windows privileges")
    def test_symlink_in_destination_cannot_redirect_migration(self):
        self.default.mkdir(parents=True)
        external = self.root / "external"
        external.mkdir()
        (self.default / "ad").symlink_to(external, target_is_directory=True)
        with self.assertRaisesRegex(FileExistsError, "migration conflicts"):
            self.upgrade()
        self.assertEqual(list(external.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
