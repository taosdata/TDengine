#!/usr/bin/env python3

# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Affero General Public
# License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import importlib.util
import tempfile
import unittest
from pathlib import Path


MODULE_SPEC = importlib.util.spec_from_file_location(
    "hot_upgrade_task", Path(__file__).with_name("hot_upgrade_task.py"))
HOT_UPGRADE = importlib.util.module_from_spec(MODULE_SPEC)
MODULE_SPEC.loader.exec_module(HOT_UPGRADE)


class HotUpgradeVersionTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.task = HOT_UPGRADE.HotUpgradeTask()
        self.task.greenVersionsPath = self.directory.name

    def test_ignores_download_staging_and_invalid_names(self):
        for name in (
                "3.4.3.0", "3.4.3.0.tmp.1072001", "3.4.3.2.partial",
                "3.4.3", "3.4.3.2.", "3.4.3.2..1014"):
            (self.root / name).mkdir()
        (self.root / "3.4.3.1").write_text("not a directory")
        self.assertEqual(self.task.findBaseVersions("3.4.3.0"), ["3.4.3.0"])

    def test_keeps_numeric_suffixes_and_oldest_baseline(self):
        for name in (
                "3.4.3.10", "3.4.3.2.1014", "3.4.3.2", "3.4.3.0.0928",
                "3.4.3.0", "3.4.30.0", "3.4.2.0"):
            (self.root / name).mkdir()
        self.assertEqual(self.task.findBaseVersions("3.4.3.0"), [
            "3.4.3.0", "3.4.3.0.0928", "3.4.3.2", "3.4.3.2.1014",
            "3.4.3.10"])

    def test_missing_cache_returns_no_versions(self):
        self.task.greenVersionsPath = str(self.root / "missing")
        self.assertEqual(self.task.findBaseVersions("3.4.3.0"), [])


if __name__ == "__main__":
    unittest.main()
